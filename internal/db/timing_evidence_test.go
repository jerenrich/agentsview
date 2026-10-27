package db

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func archivedTimingFixture(t *testing.T, d *DB) Session {
	t.Helper()
	source := strings.Join([]string{
		`{"timestamp":"2026-04-26T10:00:00Z","type":"session_meta","payload":{"id":"retained"}}`,
		`{"timestamp":"2026-04-26T10:00:00Z","type":"event_msg","payload":{"type":"task_started","turn_id":"first"}}`,
		`{"timestamp":"2026-04-26T10:00:02Z","type":"event_msg","payload":{"type":"item_completed","item":{"type":"Reasoning"},"started_at_ms":1777197600000,"completed_at_ms":1777197602000}}`,
		`{"timestamp":"2026-04-26T10:00:02Z","type":"response_item","payload":{"type":"function_call","call_id":"call"}}`,
		`{"timestamp":"2026-04-26T10:00:05Z","type":"response_item","payload":{"type":"function_call_output","call_id":"call"}}`,
		`{"timestamp":"2026-04-26T10:00:06Z","type":"event_msg","payload":{"type":"task_complete","turn_id":"first"}}`,
	}, "\n") + "\n"
	path := filepath.Join(t.TempDir(), "rollout.jsonl")
	require.NoError(t, os.WriteFile(path, []byte(source), 0600))
	info, err := os.Stat(path)
	require.NoError(t, err)
	digest := sha256.Sum256([]byte(source))
	hash := hex.EncodeToString(digest[:])
	s := Session{ID: "codex:retained", Agent: "codex", Project: "project", Machine: "local", FilePath: &path, FileSize: new(int64(len(source))), FileMtime: new(info.ModTime().UnixNano()), FileHash: &hash, StartedAt: new("2026-04-26T10:00:00Z"), EndedAt: new("2026-04-26T10:00:06Z"), MessageCount: 3, UserMessageCount: 1}
	// Seed through SQL so each test exercises its selected real ingestion writer.
	timingInsertSession(t, d, s.ID, *s.StartedAt, *s.EndedAt)
	_, err = d.getWriter().Exec(t.Context(), `UPDATE sessions SET agent=?,file_path=?,file_size=?,file_mtime=?,file_hash=? WHERE id=?`, s.Agent, s.FilePath, s.FileSize, s.FileMtime, s.FileHash, s.ID)
	require.NoError(t, err)
	timingInsertMessage(t, d, s.ID, 0, "user", "start", *s.StartedAt, false)
	timingInsertMessage(t, d, s.ID, 1, "assistant", "tool", "2026-04-26T10:00:02Z", true)
	timingInsertToolCall(t, d, s.ID, timingMsgID(t, d, s.ID, 1), "call", "exec", "Bash", "")
	timingInsertMessage(t, d, s.ID, 2, "assistant", "done", *s.EndedAt, false)
	return s
}

func TestArchivedTimingSurvivesDeletionAndReopen(t *testing.T) {
	for _, mode := range []string{"upsert", "batch", "atomic_batch", "incremental", "incremental_content"} {
		t.Run(mode, func(t *testing.T) {
			d := testDB(t)
			s := archivedTimingFixture(t, d)
			switch mode {
			case "upsert":
				require.NoError(t, d.UpsertSession(t.Context(), s))
			case "batch":
				_, err := d.WriteSessionBatchContext(t.Context(), []SessionBatchWrite{{Session: s, SkipSignalUpdates: true}})
				require.NoError(t, err)
			case "atomic_batch":
				_, err := d.WriteSessionBatchAtomic(t.Context(), []SessionBatchWrite{{Session: s, SkipSignalUpdates: true}})
				require.NoError(t, err)
			case "incremental", "incremental_content":
				u := IncrementalSessionUpdate{FileSize: *s.FileSize, FileMtime: *s.FileMtime, FileHash: s.FileHash, MsgCount: 3, UserMsgCount: 1, NextOrdinal: 3, EndedAt: s.EndedAt}
				if mode == "incremental" {
					require.NoError(t, d.UpdateSessionIncremental(t.Context(), s.ID, u))
				} else {
					_, err := d.WriteSessionIncremental(t.Context(), s.ID, nil, u)
					require.NoError(t, err)
				}
			}
			before, err := d.GetSessionTiming(t.Context(), s.ID)
			require.NoError(t, err)
			require.NotNil(t, before)
			require.Equal(t, "persisted", before.Coverage.Evidence)
			require.Equal(t, int64(6000), before.ActiveDurationMs)
			assert.Equal(t, int64(2000), before.ActivityTotals.ModelMs)
			assert.Equal(t, int64(3000), before.ActivityTotals.ToolMs)
			assert.Equal(t, int64(1000), before.Coverage.UnattributedMs)
			assert.Equal(t, 1, before.Coverage.RecordedTurns)
			assert.Equal(t, 1, before.Coverage.ModelIntervals)
			assert.Equal(t, 1, before.Coverage.TimedToolCalls)
			assert.Equal(t, 1, before.Coverage.CallerWaitCalls)
			require.NoError(t, os.Remove(*s.FilePath))
			path := d.Path()
			require.NoError(t, d.Close())
			reopened, err := OpenIsolated(t.Context(), path)
			require.NoError(t, err)
			defer reopened.Close()
			after, err := reopened.GetSessionTiming(t.Context(), s.ID)
			require.NoError(t, err)
			assert.Equal(t, before, after)
			// A later metadata upsert with no source cannot erase the retained evidence.
			require.NoError(t, reopened.UpsertSession(t.Context(), s))
			after, err = reopened.GetSessionTiming(t.Context(), s.ID)
			require.NoError(t, err)
			assert.Equal(t, before, after)
		})
	}
}

func TestArchivedTimingCopiesOrphans(t *testing.T) {
	source := testDB(t)
	s := archivedTimingFixture(t, source)
	require.NoError(t, source.UpsertSession(t.Context(), s))
	before, err := source.GetSessionTiming(t.Context(), s.ID)
	require.NoError(t, err)
	require.NoError(t, os.Remove(*s.FilePath))
	path := source.Path()
	require.NoError(t, source.Close())
	destination := testDB(t)
	n, err := destination.CopyOrphanedDataFrom(path)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	after, err := destination.GetSessionTiming(t.Context(), s.ID)
	require.NoError(t, err)
	// Message row IDs can change during archive reconstruction.
	assert.Equal(t, before.ActivityTotals, after.ActivityTotals)
	assert.Equal(t, before.Coverage, after.Coverage)
	assert.Equal(t, before.ActiveDurationMs, after.ActiveDurationMs)
}

func TestArchivedTimingRejectsChangedBindingAndWrongHash(t *testing.T) {
	d := testDB(t)
	s := archivedTimingFixture(t, d)
	wrong := s
	wrong.FileHash = new(strings.Repeat("0", 64))
	require.NoError(t, d.UpsertSession(t.Context(), wrong))
	got, err := d.GetSessionTiming(t.Context(), s.ID)
	require.NoError(t, err)
	assert.Equal(t, "unavailable", got.Coverage.Evidence)
	require.NoError(t, d.UpsertSession(t.Context(), s))
	require.NoError(t, os.Remove(*s.FilePath))
	wrong.FileSize = new(*s.FileSize + 1)
	require.NoError(t, d.UpsertSession(t.Context(), wrong))
	got, err = d.GetSessionTiming(t.Context(), s.ID)
	require.NoError(t, err)
	assert.Equal(t, "unavailable", got.Coverage.Evidence)
	assert.Equal(t, "estimated", got.Coverage.TurnBoundaries)
	assert.Zero(t, got.Coverage.ModelIntervals)
	// The older evidence remains present, but cannot be applied to this revision.
	var n int
	require.NoError(t, d.getReader().QueryRowContext(t.Context(), "SELECT COUNT(*) FROM session_timing_evidence").Scan(&n))
	assert.Equal(t, 1, n)
}

func TestArchivedTimingBatchRollback(t *testing.T) {
	d := testDB(t)
	s := archivedTimingFixture(t, d)
	_, err := d.WriteSessionBatchAtomic(t.Context(), []SessionBatchWrite{{Session: s, SkipSignalUpdates: true}}, func() error { return assert.AnError })
	require.ErrorIs(t, err, assert.AnError)
	var n int
	require.NoError(t, d.getReader().QueryRowContext(t.Context(), "SELECT COUNT(*) FROM session_timing_evidence").Scan(&n))
	assert.Zero(t, n)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.Error(t, d.UpsertSession(ctx, s))
	require.NoError(t, d.getReader().QueryRowContext(t.Context(), "SELECT COUNT(*) FROM session_timing_evidence").Scan(&n))
	assert.Zero(t, n)
}

func TestArchivedTimingIncrementalAppend(t *testing.T) {
	d := testDB(t)
	s := archivedTimingFixture(t, d)
	require.NoError(t, d.UpsertSession(t.Context(), s))
	before, err := d.GetSessionTiming(t.Context(), s.ID)
	require.NoError(t, err)
	tail := strings.Join([]string{
		`{"timestamp":"2026-04-26T11:00:00Z","type":"event_msg","payload":{"type":"task_started","turn_id":"second"}}`,
		`{"timestamp":"2026-04-26T11:00:02Z","type":"event_msg","payload":{"type":"item_completed","item":{"type":"Reasoning"},"started_at_ms":1777201200000,"completed_at_ms":1777201202000}}`,
		`{"timestamp":"2026-04-26T11:00:04Z","type":"event_msg","payload":{"type":"task_complete","turn_id":"second"}}`,
	}, "\n") + "\n"
	f, err := os.OpenFile(*s.FilePath, os.O_APPEND|os.O_WRONLY, 0600)
	require.NoError(t, err)
	_, err = f.WriteString(tail)
	require.NoError(t, err)
	require.NoError(t, f.Close())
	// Before the append is imported, the saved snapshot remains authoritative.
	unchanged, err := d.GetSessionTiming(t.Context(), s.ID)
	require.NoError(t, err)
	assert.Equal(t, before, unchanged)
	bytes, err := os.ReadFile(*s.FilePath)
	require.NoError(t, err)
	digest := sha256.Sum256(bytes)
	hash := hex.EncodeToString(digest[:])
	info, err := os.Stat(*s.FilePath)
	require.NoError(t, err)
	require.NoError(t, d.UpdateSessionIncremental(t.Context(), s.ID, IncrementalSessionUpdate{FileSize: info.Size(), FileMtime: info.ModTime().UnixNano(), FileHash: &hash, EndedAt: new("2026-04-26T11:00:04Z"), MsgCount: 3, UserMsgCount: 1, NextOrdinal: 3}))
	require.NoError(t, os.Remove(*s.FilePath))
	after, err := d.GetSessionTiming(t.Context(), s.ID)
	require.NoError(t, err)
	assert.Equal(t, "persisted", after.Coverage.Evidence)
	assert.Equal(t, 2, after.Coverage.RecordedTurns)
	assert.Equal(t, 2, after.Coverage.ModelIntervals)
	assert.Equal(t, int64(10000), after.ActiveDurationMs)
	assert.Equal(t, int64(3594000), after.IdleDurationMs)
	assert.Equal(t, int64(4000), after.ActivityTotals.ModelMs)
}

func TestArchivedTimingReplacementCopyKeepsOriginalEvidence(t *testing.T) {
	d := testDB(t)
	s := archivedTimingFixture(t, d)
	require.NoError(t, d.UpsertSession(t.Context(), s))
	before, err := d.GetSessionTiming(t.Context(), s.ID)
	require.NoError(t, err)
	// Replace the source with a different valid snapshot under the same native ID.
	replacement := `{"timestamp":"2026-04-26T12:00:00Z","type":"session_meta","payload":{"id":"retained"}}` + "\n"
	require.NoError(t, os.WriteFile(*s.FilePath, []byte(replacement), 0600))
	digest := sha256.Sum256([]byte(replacement))
	hash := hex.EncodeToString(digest[:])
	info, err := os.Stat(*s.FilePath)
	require.NoError(t, err)
	changed := s
	changed.FileSize = new(info.Size())
	changed.FileMtime = new(info.ModTime().UnixNano())
	changed.FileHash = &hash
	copyID, err := d.ReplaceSessionKeepingTrashedCopy(t.Context(), SessionBatchWrite{Session: changed, Messages: []Message{{SessionID: s.ID, Ordinal: 0, Role: "user", Content: "replacement", Timestamp: *s.StartedAt}}, SkipSignalUpdates: true})
	require.NoError(t, err)
	require.NotEmpty(t, copyID)
	require.NoError(t, os.Remove(*s.FilePath))
	_, err = d.RestoreSession(t.Context(), copyID)
	require.NoError(t, err)
	copied, err := d.GetSessionTiming(t.Context(), copyID)
	require.NoError(t, err)
	require.NotNil(t, copied)
	assert.Equal(t, before.Coverage, copied.Coverage)
	assert.Equal(t, before.ActivityTotals, copied.ActivityTotals)
}

func TestArchivedTimingContinuationPage(t *testing.T) {
	for _, compressed := range []bool{false, true} {
		t.Run(fmt.Sprintf("compressed=%t", compressed), func(t *testing.T) {
			d := testDB(t)
			s := archivedTimingFixture(t, d)
			thread := "11111111-1111-4111-8111-111111111111"
			rollout := "22222222-2222-4222-8222-222222222222"
			key := thread + "_" + rollout
			bytes, err := os.ReadFile(*s.FilePath)
			require.NoError(t, err)
			bytes = []byte(strings.ReplaceAll(string(bytes), `"id":"retained"`, `"id":"`+thread+`"`))
			path := filepath.Join(t.TempDir(), "rollout-2026-04-26T10-00-00-"+key+".jsonl")
			if compressed {
				encoder, err := zstd.NewWriter(nil)
				require.NoError(t, err)
				bytes = encoder.EncodeAll(bytes, nil)
				encoder.Close()
				path += ".zst"
			}
			require.NoError(t, os.WriteFile(path, bytes, 0600))
			info, err := os.Stat(path)
			require.NoError(t, err)
			digest := sha256.Sum256(bytes)
			hash := hex.EncodeToString(digest[:])
			s.ID = "codex:" + key
			s.FilePath = &path
			s.FileSize = new(info.Size())
			s.FileMtime = new(info.ModTime().UnixNano())
			s.FileHash = &hash
			require.NoError(t, d.UpsertSession(t.Context(), s))
			before, err := d.GetSessionTiming(t.Context(), s.ID)
			require.NoError(t, err)
			require.Equal(t, "persisted", before.Coverage.Evidence)
			assert.Equal(t, 1, before.Coverage.RecordedTurns)
			assert.Equal(t, int64(6000), before.ActiveDurationMs)
			require.NoError(t, os.Remove(path))
			after, err := d.GetSessionTiming(t.Context(), s.ID)
			require.NoError(t, err)
			assert.Equal(t, before, after)
		})
	}
}

func TestArchivedTimingSurvivesForcedResyncWithoutSource(t *testing.T) {
	d := testDB(t)
	s := archivedTimingFixture(t, d)
	require.NoError(t, d.UpsertSession(t.Context(), s))
	before, err := d.GetSessionTiming(t.Context(), s.ID)
	require.NoError(t, err)
	require.Equal(t, "persisted", before.Coverage.Evidence)
	require.NoError(t, os.Remove(*s.FilePath))
	require.NoError(t, d.ResetAllMtimes(t.Context()))
	after, err := d.GetSessionTiming(t.Context(), s.ID)
	require.NoError(t, err)
	assert.Equal(t, before, after)
	// Resetting sync bookkeeping cannot make evidence for different bytes valid.
	_, err = d.getWriter().Exec(t.Context(), "UPDATE sessions SET file_hash=? WHERE id=?", strings.Repeat("0", 64), s.ID)
	require.NoError(t, err)
	require.NoError(t, d.ResetAllMtimes(t.Context()))
	stale, err := d.GetSessionTiming(t.Context(), s.ID)
	require.NoError(t, err)
	assert.Equal(t, "unavailable", stale.Coverage.Evidence)
}

func TestArchivedTimingReadsOneSnapshotDuringReplacement(t *testing.T) {
	d := testDB(t)
	s := archivedTimingFixture(t, d)
	require.NoError(t, d.UpsertSession(t.Context(), s))
	require.NoError(t, os.Remove(*s.FilePath))
	before, err := d.GetSessionTiming(t.Context(), s.ID)
	require.NoError(t, err)
	changed := s
	changed.FileSize = new(*s.FileSize + 1)
	changed.FileHash = new(strings.Repeat("0", 64))
	changed.EndedAt = new("2026-04-26T10:00:30Z")
	snapshot, err := d.getSessionTiming(t.Context(), s.ID, func() {
		_, err := d.WriteSessionBatchAtomic(t.Context(), []SessionBatchWrite{{Session: changed, ReplaceMessages: true, SkipSignalUpdates: true, Messages: []Message{
			{SessionID: s.ID, Ordinal: 0, Role: "user", Content: "changed", ContentLength: 7, Timestamp: *s.StartedAt},
			{SessionID: s.ID, Ordinal: 1, Role: "assistant", Content: "finished", ContentLength: 8, Timestamp: *changed.EndedAt},
		}}})
		require.NoError(t, err)
	})
	require.NoError(t, err)
	assert.Equal(t, before, snapshot)
	latest, err := d.GetSessionTiming(t.Context(), s.ID)
	require.NoError(t, err)
	assert.Equal(t, "unavailable", latest.Coverage.Evidence)
	assert.Zero(t, latest.ToolCallCount)
	assert.Equal(t, int64(30000), latest.ActiveDurationMs)
}

func TestResetAllMtimesDoesNotPromoteStaleZeroTiming(t *testing.T) {
	d := testDB(t)
	s := archivedTimingFixture(t, d)
	require.NoError(t, d.UpsertSession(t.Context(), s))
	require.NoError(t, os.Remove(*s.FilePath))
	// A legacy hashless source changed after an earlier reset. Its old zero
	// timestamp must remain stale even when all source mtimes are reset again.
	_, err := d.getWriter().Exec(t.Context(), "UPDATE session_timing_evidence SET file_mtime=0,file_hash='' WHERE session_id=?", s.ID)
	require.NoError(t, err)
	_, err = d.getWriter().Exec(t.Context(), "UPDATE sessions SET file_hash=NULL WHERE id=?", s.ID)
	require.NoError(t, err)
	before, err := d.GetSessionTiming(t.Context(), s.ID)
	require.NoError(t, err)
	assert.Equal(t, "unavailable", before.Coverage.Evidence)
	require.NoError(t, d.ResetAllMtimes(t.Context()))
	after, err := d.GetSessionTiming(t.Context(), s.ID)
	require.NoError(t, err)
	assert.Equal(t, "unavailable", after.Coverage.Evidence)
	var version int
	var body string
	require.NoError(t, d.getReader().QueryRowContext(t.Context(), "SELECT version,evidence FROM session_timing_evidence WHERE session_id=?", s.ID).Scan(&version, &body))
	assert.Zero(t, version)
	assert.NotEmpty(t, body, "invalidating a binding must retain archived metadata")
}
