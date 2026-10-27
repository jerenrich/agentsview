package db

import (
	"bytes"
	"crypto/sha256"
	"encoding"
	"encoding/hex"
	"encoding/json/v2"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/agentsview/internal/parser"
)

func writeTimingSnapshot(t *testing.T, s Session, content []byte) Session {
	t.Helper()
	require.NoError(t, os.WriteFile(*s.FilePath, content, 0o600))
	info, err := os.Stat(*s.FilePath)
	require.NoError(t, err)
	sum := sha256.Sum256(content)
	s.FileHash = new(hex.EncodeToString(sum[:]))
	s.FileSize = new(info.Size())
	s.FileMtime = new(info.ModTime().UnixNano())
	s.TimingCapture = nil
	return s
}

func requireTimingCaptureMatchesFull(t *testing.T, d *DB, s Session, content []byte) Session {
	t.Helper()
	prepared := d.PrepareSessionTimingEvidence(t.Context(), s)
	require.NotNil(t, prepared.TimingCapture)
	full, err := parser.ReadSessionTiming(t.Context(), bytes.NewReader(content), s.Agent, *s.FilePath)
	require.NoError(t, err)
	require.Equal(t, full, prepared.TimingCapture.Evidence)
	return prepared
}

func TestTimingCaptureCheckpointRestartReadsOnlyAppend(t *testing.T) {
	d := testDB(t)
	s := archivedTimingFixture(t, d)
	full, err := os.ReadFile(*s.FilePath)
	require.NoError(t, err)
	cut := bytes.Index(full, []byte(`{"timestamp":"2026-04-26T10:00:05Z"`))
	require.Positive(t, cut)
	s = writeTimingSnapshot(t, s, full[:cut])
	first := requireTimingCaptureMatchesFull(t, d, s, full[:cut])
	assert.False(t, first.TimingCapture.Resumed)
	require.NoError(t, d.UpsertSession(t.Context(), first))
	require.NoError(t, d.Close())
	reopened, err := OpenIsolated(t.Context(), d.Path())
	require.NoError(t, err)
	defer reopened.Close()
	// The completed result must replace the provisional EOF bound after restart.
	s = writeTimingSnapshot(t, s, full)
	finished := requireTimingCaptureMatchesFull(t, reopened, s, full)
	assert.True(t, finished.TimingCapture.Resumed)
	assert.LessOrEqual(t, finished.TimingCapture.SourceBytesRead, int64(len(full)-cut+63))
	require.Len(t, finished.TimingCapture.Evidence.Turns, 1)
	assert.Equal(t, int64(6000), finished.TimingCapture.Evidence.Turns[0].End-finished.TimingCapture.Evidence.Turns[0].Start)
	require.Len(t, finished.TimingCapture.Evidence.Calls, 1)
	require.NoError(t, reopened.UpsertSession(t.Context(), finished))
	// Large opaque bodies increase source size without increasing timing state.
	large := append(bytes.Clone(full), []byte(`{"opaque":"`+strings.Repeat("x", 2<<20)+`private-checkpoint-secret-body-suffix"}`+"\n")...)
	s = writeTimingSnapshot(t, s, large)
	grown := requireTimingCaptureMatchesFull(t, reopened, s, large)
	require.True(t, grown.TimingCapture.Resumed)
	require.NoError(t, reopened.UpsertSession(t.Context(), grown))
	tail := []byte(`{"opaque":"small append"}` + "\n")
	next := append(bytes.Clone(large), tail...)
	s = writeTimingSnapshot(t, s, next)
	small := requireTimingCaptureMatchesFull(t, reopened, s, next)
	assert.True(t, small.TimingCapture.Resumed)
	assert.LessOrEqual(t, small.TimingCapture.SourceBytesRead, int64(len(tail)+63))
	assert.Less(t, small.TimingCapture.SourceBytesRead, int64(128))
	require.NoError(t, reopened.UpsertSession(t.Context(), small))
	var state, cursor []byte
	require.NoError(t, reopened.getReader().QueryRowContext(t.Context(), "SELECT hash_state,cursor FROM session_timing_checkpoints WHERE session_id=?", s.ID).Scan(&state, &cursor))
	assert.NotContains(t, string(state), "private-checkpoint-secret-body-suffix")
	assert.NotContains(t, string(cursor), "private-checkpoint-secret-body-suffix")
	require.NoError(t, os.Remove(*s.FilePath))
	timing, err := reopened.GetSessionTiming(t.Context(), s.ID)
	require.NoError(t, err)
	assert.Equal(t, "persisted", timing.Coverage.Evidence)
}

func TestTimingCaptureCheckpointPartialLine(t *testing.T) {
	d := testDB(t)
	s := archivedTimingFixture(t, d)
	full, err := os.ReadFile(*s.FilePath)
	require.NoError(t, err)
	split := bytes.Index(full, []byte(`"call_id":"call"`)) + 7
	require.Positive(t, split)
	prefix := full[:split]
	s = writeTimingSnapshot(t, s, prefix)
	first := requireTimingCaptureMatchesFull(t, d, s, prefix)
	require.NotNil(t, first.TimingCapture.Checkpoint)
	safe := first.TimingCapture.Checkpoint.Offset
	require.Equal(t, int64(bytes.LastIndexByte(prefix, '\n')+1), safe)
	require.NoError(t, d.UpsertSession(t.Context(), first))
	s = writeTimingSnapshot(t, s, full)
	next := requireTimingCaptureMatchesFull(t, d, s, full)
	assert.True(t, next.TimingCapture.Resumed)
	assert.LessOrEqual(t, next.TimingCapture.SourceBytesRead, int64(len(full))-safe+63)
}

func TestTimingCaptureCheckpointFallback(t *testing.T) {
	for _, mode := range []string{"rewrite", "truncate", "version", "cursor", "hash state", "path"} {
		t.Run(mode, func(t *testing.T) {
			d := testDB(t)
			s := archivedTimingFixture(t, d)
			full, err := os.ReadFile(*s.FilePath)
			require.NoError(t, err)
			full = append(full, []byte(`{"opaque":"before"}`+"\n")...)
			s = writeTimingSnapshot(t, s, full)
			first := requireTimingCaptureMatchesFull(t, d, s, full)
			require.NoError(t, d.UpsertSession(t.Context(), first))
			switch mode {
			case "rewrite":
				full = bytes.Replace(full, []byte(`"opaque":"before"`), []byte(`"opaque":"afterx"`), 1)
				// Put the changed bytes in an already-hashed block, before the 63-byte
				// reconstruction window, while preserving the source length.
				full = bytes.Replace(full, []byte(`"type":"task_started"`), []byte(`"type":"task_starteX"`), 1)
			case "truncate":
				full = full[:bytes.IndexByte(full, '\n')+1]
			case "version":
				_, err = d.getWriter().Exec(t.Context(), "UPDATE session_timing_checkpoints SET version=999")
				require.NoError(t, err)
			case "cursor":
				_, err = d.getWriter().Exec(t.Context(), "UPDATE session_timing_checkpoints SET cursor='not json'")
				require.NoError(t, err)
			case "hash state":
				_, err = d.getWriter().Exec(t.Context(), "UPDATE session_timing_checkpoints SET hash_state=X'00'")
				require.NoError(t, err)
			case "path":
				p := filepath.Join(t.TempDir(), "moved.jsonl")
				s.FilePath = &p
			}
			s = writeTimingSnapshot(t, s, full)
			next := requireTimingCaptureMatchesFull(t, d, s, full)
			assert.False(t, next.TimingCapture.Resumed)
			assert.GreaterOrEqual(t, next.TimingCapture.SourceBytesRead, int64(len(full)))
		})
	}
}

func TestTimingCheckpointRollsBackWithEvidence(t *testing.T) {
	d := testDB(t)
	s := archivedTimingFixture(t, d)
	full, err := os.ReadFile(*s.FilePath)
	require.NoError(t, err)
	require.NoError(t, d.UpsertSession(t.Context(), s))
	_, err = d.getWriter().Exec(t.Context(), `CREATE TRIGGER fail_timing_checkpoint BEFORE INSERT ON session_timing_checkpoints BEGIN SELECT RAISE(ABORT,'injected checkpoint failure'); END`)
	require.NoError(t, err)
	newer := slices.Concat(full, []byte(`{"opaque":"new bytes"}`+"\n"))
	changed := writeTimingSnapshot(t, s, newer)
	require.ErrorContains(t, d.UpsertSession(t.Context(), changed), "injected checkpoint failure")
	var evidenceHash, checkpointHash, sessionHash string
	require.NoError(t, d.getReader().QueryRowContext(t.Context(), "SELECT file_hash FROM session_timing_evidence WHERE session_id=?", s.ID).Scan(&evidenceHash))
	require.NoError(t, d.getReader().QueryRowContext(t.Context(), "SELECT file_hash FROM session_timing_checkpoints WHERE session_id=?", s.ID).Scan(&checkpointHash))
	require.NoError(t, d.getReader().QueryRowContext(t.Context(), "SELECT file_hash FROM sessions WHERE id=?", s.ID).Scan(&sessionHash))
	assert.Equal(t, *s.FileHash, evidenceHash)
	assert.Equal(t, evidenceHash, checkpointHash)
	assert.Equal(t, evidenceHash, sessionHash)
}

func TestTimingHashCheckpointDropsPartialBlockBytes(t *testing.T) {
	for size := range 256 {
		data := []byte(strings.Repeat("private-buffer-data-", 14))[:size]
		h, err := newAlignedTimingHash(nil)
		require.NoError(t, err)
		// Exercise writes that repeatedly cross SHA block boundaries.
		for offset := 0; offset < len(data); offset += 7 {
			h.Write(data[offset:min(offset+7, len(data))])
		}
		want := sha256.Sum256(data)
		assert.Equal(t, hex.EncodeToString(want[:]), h.sum())
		standard := sha256.New()
		_, err = standard.Write(data[:size/64*64])
		require.NoError(t, err)
		aligned, err := standard.(encoding.BinaryMarshaler).MarshalBinary()
		require.NoError(t, err)
		assert.Equal(t, aligned, h.state(), "saved state must exclude buffered transcript bytes")
	}
}

func TestTimingCheckpointWorkDoesNotGrowWithArchive(t *testing.T) {
	for _, extra := range []int{0, 1000} {
		t.Run(fmt.Sprintf("unrelated_sessions=%d", extra), func(t *testing.T) {
			d := testDB(t)
			s := archivedTimingFixture(t, d)
			require.NoError(t, d.UpsertSession(t.Context(), s))
			tx, err := d.getWriter().Begin(t.Context())
			require.NoError(t, err)
			for i := range extra {
				_, err := tx.ExecContext(t.Context(), "INSERT INTO sessions(id,project,machine,agent) VALUES(?,?,?,?)", fmt.Sprintf("unrelated-%d", i), "project", "local", "codex")
				require.NoError(t, err)
			}
			require.NoError(t, tx.Commit())
			source, err := os.ReadFile(*s.FilePath)
			require.NoError(t, err)
			tail := []byte(`{"opaque":"one update"}` + "\n")
			source = append(source, tail...)
			s = writeTimingSnapshot(t, s, source)
			prepared := requireTimingCaptureMatchesFull(t, d, s, source)
			assert.True(t, prepared.TimingCapture.Resumed)
			assert.LessOrEqual(t, prepared.TimingCapture.SourceBytesRead, int64(len(tail)+63))
			plan, err := d.getReader().QueryContext(t.Context(), "EXPLAIN QUERY PLAN "+loadTimingCheckpointSQL, s.ID, s.Agent, timingSourceID(&s), timingEvidenceVersion, timingCheckpointVersion, *s.FilePath, *s.FileSize, maxArchivedTimingBytes, maxArchivedTimingBytes)
			require.NoError(t, err)
			defer plan.Close()
			for plan.Next() {
				var id, parent, unused int
				var detail string
				require.NoError(t, plan.Scan(&id, &parent, &unused, &detail))
				assert.NotContains(t, detail, "SCAN ", "checkpoint lookup must seek the selected session")
			}
			require.NoError(t, plan.Err())
		})
	}
}

func TestTimingCheckpointFiltersInvalidRowsBeforePayloadRead(t *testing.T) {
	for _, tc := range []struct {
		name, mutate string
		limit        int
	}{
		{"unknown version", "UPDATE session_timing_checkpoints SET version=999", maxArchivedTimingBytes},
		{"offset past imported prefix", "UPDATE session_timing_checkpoints SET offset=file_size+1", maxArchivedTimingBytes},
		{"empty hash state", "UPDATE session_timing_checkpoints SET hash_state=X''", maxArchivedTimingBytes},
		{"oversized hash state", "UPDATE session_timing_checkpoints SET hash_state=zeroblob(4097)", maxArchivedTimingBytes},
		{"hash byte limit", "UPDATE session_timing_checkpoints SET hash_state=replace(hex(zeroblob(1024)),'0','界')", maxArchivedTimingBytes},
		// 80 characters occupy 240 UTF-8 bytes. A character-count limit would
		// mistakenly return this payload instead of rejecting it in SQLite.
		{"cursor byte limit", "UPDATE session_timing_checkpoints SET cursor=replace(hex(zeroblob(40)),'0','界')", 100},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := testDB(t)
			s := archivedTimingFixture(t, d)
			require.NoError(t, d.UpsertSession(t.Context(), s))
			_, err := d.getWriter().Exec(t.Context(), tc.mutate)
			require.NoError(t, err)
			rows, err := d.getReader().QueryContext(t.Context(), loadTimingCheckpointSQL, s.ID, s.Agent, timingSourceID(&s), timingEvidenceVersion, timingCheckpointVersion, *s.FilePath, *s.FileSize+1024, tc.limit, maxArchivedTimingBytes)
			require.NoError(t, err)
			defer rows.Close()
			assert.False(t, rows.Next(), "invalid metadata must not reach the driver as a result row")
			require.NoError(t, rows.Err())
		})
	}
}

func TestTimingCheckpointUpgradeDoesNotRequireResync(t *testing.T) {
	for _, archiveOnly := range []bool{false, true} {
		t.Run(fmt.Sprintf("archive_only=%t", archiveOnly), func(t *testing.T) {
			d := testDB(t)
			s := archivedTimingFixture(t, d)
			source, err := os.ReadFile(*s.FilePath)
			require.NoError(t, err)
			require.NoError(t, d.UpsertSession(t.Context(), s))
			before, err := d.GetSessionTiming(t.Context(), s.ID)
			require.NoError(t, err)
			var messageID int64
			require.NoError(t, d.getReader().QueryRowContext(t.Context(), "SELECT id FROM messages WHERE session_id=? AND ordinal=0", s.ID).Scan(&messageID))
			// Recreate a pre-checkpoint archive with retained evidence and no logs.
			_, err = d.getWriter().Exec(t.Context(), "DROP TABLE session_timing_checkpoints")
			require.NoError(t, err)
			require.NoError(t, os.Remove(*s.FilePath))
			if archiveOnly {
				require.NoError(t, d.EnableArchiveOnly(t.Context()))
			}
			path := d.Path()
			require.NoError(t, d.Close())
			needs, full, err := ArchiveResyncState(t.Context(), path)
			require.NoError(t, err)
			assert.False(t, needs)
			assert.False(t, full)
			ro, err := OpenReadOnly(t.Context(), path)
			require.NoError(t, err)
			timing, err := ro.GetSessionTiming(t.Context(), s.ID)
			require.NoError(t, err)
			assert.Equal(t, before, timing)
			var present int
			require.NoError(t, ro.getReader().QueryRowContext(t.Context(), "SELECT count(*) FROM sqlite_master WHERE name='session_timing_checkpoints'").Scan(&present))
			assert.Zero(t, present, "read-only opens must not migrate an archive")
			require.NoError(t, ro.Close())
			upgraded, err := OpenIsolated(t.Context(), path)
			require.NoError(t, err)
			defer upgraded.Close()
			timing, err = upgraded.GetSessionTiming(t.Context(), s.ID)
			require.NoError(t, err)
			assert.Equal(t, before, timing)
			require.NoError(t, upgraded.getReader().QueryRowContext(t.Context(), "SELECT count(*) FROM session_timing_checkpoints").Scan(&present))
			assert.Zero(t, present)
			var preservedID int64
			require.NoError(t, upgraded.getReader().QueryRowContext(t.Context(), "SELECT id FROM messages WHERE session_id=? AND ordinal=0", s.ID).Scan(&preservedID))
			assert.Equal(t, messageID, preservedID)
			version, only, err := readUserVersion(t.Context(), upgraded.getReader())
			require.NoError(t, err)
			assert.Equal(t, CurrentDataVersion(), version)
			assert.Equal(t, archiveOnly, only)
			// Plain local capture lazily warms a new checkpoint when logs return.
			if !archiveOnly {
				s = writeTimingSnapshot(t, s, source)
				cold := requireTimingCaptureMatchesFull(t, upgraded, s, source)
				assert.False(t, cold.TimingCapture.Resumed)
				require.NoError(t, upgraded.UpsertSession(t.Context(), cold))
				warm := requireTimingCaptureMatchesFull(t, upgraded, s, source)
				assert.True(t, warm.TimingCapture.Resumed)
			}
		})
	}
}

func TestTimingCheckpointIgnoresStalePreDowngradeCursor(t *testing.T) {
	d := testDB(t)
	s := archivedTimingFixture(t, d)
	source, err := os.ReadFile(*s.FilePath)
	require.NoError(t, err)
	require.NoError(t, d.UpsertSession(t.Context(), s))
	// A pre-checkpoint binary updates evidence and session metadata without
	// touching the optional cursor table. Its stale row must not be resumed.
	newer := slices.Concat(source, []byte(`{"opaque":"written by older binary"}`+"\n"))
	s = writeTimingSnapshot(t, s, newer)
	r, _ := scanTimingCapture(t.Context(), &s, nil)
	require.NotNil(t, r)
	payload, err := json.Marshal(r.Evidence)
	require.NoError(t, err)
	_, err = d.getWriter().Exec(t.Context(), "UPDATE session_timing_evidence SET file_size=?,file_mtime=?,file_hash=?,evidence=? WHERE session_id=?", r.Size, r.Mtime, r.Hash, string(payload), s.ID)
	require.NoError(t, err)
	_, err = d.getWriter().Exec(t.Context(), "UPDATE sessions SET file_size=?,file_mtime=?,file_hash=? WHERE id=?", r.Size, r.Mtime, r.Hash, s.ID)
	require.NoError(t, err)
	assert.Nil(t, d.loadTimingCheckpoint(t.Context(), &s))
	prepared := requireTimingCaptureMatchesFull(t, d, s, newer)
	assert.False(t, prepared.TimingCapture.Resumed)
	require.NoError(t, d.UpsertSession(t.Context(), prepared))
	var hash string
	err = d.getReader().QueryRowContext(t.Context(), "SELECT file_hash FROM session_timing_checkpoints WHERE session_id=?", s.ID).Scan(&hash)
	require.NoError(t, err)
	assert.Equal(t, *s.FileHash, hash)
}

func TestTimingCaptureCheckpointResumesAfterOversizedPartialRecord(t *testing.T) {
	d := testDB(t)
	s := archivedTimingFixture(t, d)
	base, err := os.ReadFile(*s.FilePath)
	require.NoError(t, err)
	// The record exceeds the provider line limit and is still being written.
	partial := slices.Concat(base, []byte(`{"opaque":"`+strings.Repeat("x", 64<<20)))
	s = writeTimingSnapshot(t, s, partial)
	first := requireTimingCaptureMatchesFull(t, d, s, partial)
	require.NotNil(t, first.TimingCapture.Checkpoint)
	assert.Equal(t, int64(len(base)), first.TimingCapture.Checkpoint.Offset, "resume starts at the oversized record")
	require.NoError(t, d.UpsertSession(t.Context(), first))
	finished := slices.Concat(partial, []byte(`"}`+"\n"+`{"opaque":"after"}`+"\n"))
	s = writeTimingSnapshot(t, s, finished)
	resumed := requireTimingCaptureMatchesFull(t, d, s, finished)
	assert.True(t, resumed.TimingCapture.Resumed, "the saved hash must match the resume boundary")
	assert.Equal(t, int64(len(finished)), resumed.TimingCapture.Checkpoint.Offset)
}
