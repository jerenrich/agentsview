package db

import (
	"context"
	"encoding/json/v2"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/agentsview/internal/parser"
)

func timingEvidencePayload(t *testing.T, d *DB, id string) (string, int64) {
	t.Helper()
	var payload string
	var size int64
	require.NoError(t, d.getReader().QueryRowContext(t.Context(),
		"SELECT evidence, file_size FROM session_timing_evidence WHERE session_id=?", id).Scan(&payload, &size))
	return payload, size
}

func timingCheckpointCount(t *testing.T, d *DB, id string) int {
	t.Helper()
	var n int
	require.NoError(t, d.getReader().QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM session_timing_checkpoints WHERE session_id=?", id).Scan(&n))
	return n
}

// forkedRolloutFixture is a plaintext Codex rollout the timing reader always
// rejects: forked sources mix parent and child timelines.
func forkedRolloutFixture(t *testing.T, d *DB) (Session, []byte) {
	t.Helper()
	s := archivedTimingFixture(t, d)
	content := []byte(strings.Join([]string{
		`{"timestamp":"2026-04-26T10:00:00Z","type":"session_meta","payload":{"id":"retained","forked_from_id":"parent"}}`,
		`{"timestamp":"2026-04-26T10:00:00Z","type":"event_msg","payload":{"type":"task_started","turn_id":"first"}}`,
		`{"timestamp":"2026-04-26T10:00:06Z","type":"event_msg","payload":{"type":"task_complete","turn_id":"first"}}`,
	}, "\n") + "\n")
	return writeTimingSnapshot(t, s, content), content
}

func TestTimingRejectionIsSavedAndCarriedForward(t *testing.T) {
	d := testDB(t)
	s, content := forkedRolloutFixture(t, d)
	prepared := d.PrepareSessionTimingEvidence(t.Context(), s)
	require.NotNil(t, prepared.TimingCapture)
	require.True(t, prepared.TimingCapture.Rejected, "a proven fork rejection is saved instead of retried")
	require.NoError(t, d.UpsertSession(t.Context(), prepared))
	payload, _ := timingEvidencePayload(t, d, s.ID)
	assert.Empty(t, payload)
	assert.Zero(t, timingCheckpointCount(t, d, s.ID))

	// Remove the source before the next incremental write: the rejection is
	// carried to the new binding without reading the appended file.
	require.NoError(t, os.Remove(*s.FilePath))
	grown := int64(len(content) + 128)
	require.NoError(t, d.UpdateSessionIncremental(t.Context(), s.ID, IncrementalSessionUpdate{
		FileSize: grown, FileMtime: *s.FileMtime + 1, FileHash: new("appended-hash"),
		MsgCount: 3, UserMsgCount: 1, NextOrdinal: 3, EndedAt: s.EndedAt,
	}))
	payload, size := timingEvidencePayload(t, d, s.ID)
	assert.Empty(t, payload)
	assert.Equal(t, grown, size, "the rejection must follow the appended source binding")

	// A matching rejection suppresses source fallbacks at read time.
	timing, err := d.GetSessionTiming(t.Context(), s.ID)
	require.NoError(t, err)
	require.NotNil(t, timing)
	assert.Equal(t, "unavailable", timing.Coverage.Evidence)
	assert.Empty(t, timing.TimingSource)
}

func TestTimingRejectionRequiresImportedHash(t *testing.T) {
	d := testDB(t)
	s, _ := forkedRolloutFixture(t, d)
	s.FileHash = new("hash-of-different-content")
	assert.Nil(t, d.PrepareSessionTimingEvidence(t.Context(), s).TimingCapture,
		"bytes that do not match the import cannot prove a lasting rejection")
}

func TestTimingRejectionReadFallbackSkipsRejectedSource(t *testing.T) {
	d := testDB(t)
	s := archivedTimingFixture(t, d)
	require.NoError(t, d.UpsertSession(t.Context(), ExcludeSessionTimingEvidence(s)))
	// The source itself is valid; only the saved decision prevents its use.
	timing, err := d.GetSessionTiming(t.Context(), s.ID)
	require.NoError(t, err)
	require.NotNil(t, timing)
	assert.Equal(t, "unavailable", timing.Coverage.Evidence)
	assert.Empty(t, timing.TimingSource)
}

func TestTimingClaudeSubagentTranscriptRejectedWithoutRead(t *testing.T) {
	d := testDB(t)
	// The path does not exist: a rejection must come from the path alone.
	path := filepath.Join(t.TempDir(), "parent", "subagents", "agent-abc.jsonl")
	s := Session{ID: "agent-abc", Agent: "claude", FilePath: &path, FileSize: new(int64(100)), FileMtime: new(int64(1)), FileHash: new("hash")}
	prepared := d.PrepareSessionTimingEvidence(t.Context(), s)
	require.NotNil(t, prepared.TimingCapture)
	assert.True(t, prepared.TimingCapture.Rejected)
}

func TestTimingExclusionSurvivesRebinding(t *testing.T) {
	d := testDB(t)
	s := archivedTimingFixture(t, d)
	excluded := ExcludeSessionTimingEvidence(s)
	// A later writer may refresh source metadata before preparing timing.
	excluded.FileSize = new(*s.FileSize + 1)
	prepared := d.PrepareSessionTimingEvidence(t.Context(), excluded)
	require.NotNil(t, prepared.TimingCapture)
	assert.True(t, prepared.TimingCapture.Rejected)
	assert.Equal(t, *excluded.FileSize, prepared.TimingCapture.Size)
}

func TestTimingIncrementalClaudeWithoutSavedDecisionSkipsCapture(t *testing.T) {
	d := testDB(t)
	path := filepath.Join(t.TempDir(), "claude.jsonl")
	content := `{"timestamp":"2026-04-26T10:00:00Z","type":"user","sessionId":"c1","message":{"content":"go"}}` + "\n"
	s := writeTimingSnapshot(t, Session{ID: "c1", Agent: "claude", Project: "project", Machine: "local", FilePath: &path}, []byte(content))
	timingInsertSession(t, d, s.ID, "2026-04-26T10:00:00Z", "2026-04-26T10:00:00Z")
	_, err := d.getWriter().Exec(t.Context(), `UPDATE sessions SET agent='claude', file_path=? WHERE id=?`, path, s.ID)
	require.NoError(t, err)
	update, err := d.prepareIncrementalTiming(t.Context(), s.ID, IncrementalSessionUpdate{FileSize: *s.FileSize, FileMtime: *s.FileMtime, FileHash: s.FileHash})
	require.NoError(t, err)
	assert.Nil(t, update.TimingCapture, "only a full Claude parse can prove the file is not shared")
}

func TestReadOwnedTimingSourceSkipsSharedClaudeFiles(t *testing.T) {
	path := filepath.Join(t.TempDir(), "main.jsonl")
	content := strings.Join([]string{
		`{"timestamp":"2026-04-26T10:00:00Z","type":"user","sessionId":"main","message":{"content":"go"}}`,
		`{"timestamp":"2026-04-26T10:00:05Z","type":"assistant","sessionId":"main","message":{"content":[{"type":"text","text":"done"}]}}`,
	}, "\n") + "\n"
	base := writeTimingSnapshot(t, Session{ID: "main", Agent: "claude", FilePath: &path}, []byte(content))
	noChildren := func(context.Context, string) ([]Session, error) { return nil, nil }
	forkChild := func(context.Context, string) ([]Session, error) {
		return []Session{{ID: "main-fu1", RelationshipType: string(parser.RelFork), FilePath: &path}}, nil
	}
	for _, tc := range []struct {
		name     string
		relation string
		children ChildSessionsFunc
		owned    bool
	}{
		{"single session", "", noChildren, true},
		{"fork session", string(parser.RelFork), noChildren, false},
		{"replayed continuation", string(parser.RelContinuation), noChildren, false},
		{"parent of in-file fork", "", forkChild, false},
		{"unknown children", "", nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := base
			s.RelationshipType = tc.relation
			evidence, err := readOwnedTimingSource(t.Context(), &s, tc.children)
			require.NoError(t, err)
			assert.Equal(t, tc.owned, evidence != nil)
		})
	}
}

func TestSourceTimingOpenTurnRunsUntilNow(t *testing.T) {
	src := strings.Join([]string{
		`{"timestamp":"2026-04-26T10:00:00Z","type":"user","sessionId":"s","message":{"content":"first"}}`,
		`{"timestamp":"2026-04-26T10:00:05Z","type":"assistant","sessionId":"s","message":{"content":[{"type":"text","text":"done"}]}}`,
		`{"timestamp":"2026-04-26T10:10:00Z","type":"user","sessionId":"s","message":{"content":"second, still thinking"}}`,
	}, "\n") + "\n"
	evidence, err := parser.ReadSessionTiming(t.Context(), strings.NewReader(src), "claude")
	require.NoError(t, err)
	turns := []TurnRow{
		{MessageID: 1, Ordinal: 0, Timestamp: "2026-04-26T10:00:00Z", Role: "user", ContentLength: 5},
		{MessageID: 2, Ordinal: 1, Timestamp: "2026-04-26T10:00:05Z", Role: "assistant", ContentLength: 4},
		{MessageID: 3, Ordinal: 2, Timestamp: "2026-04-26T10:10:00Z", Role: "user", ContentLength: 22},
	}
	now := time.Date(2026, 4, 26, 10, 12, 0, 0, time.UTC)
	for _, tc := range []struct {
		name    string
		endedAt *string
		running bool
		active  int64
	}{
		{"running session", nil, true, 5000 + 120000},
		{"ended session", new("2026-04-26T10:10:00Z"), false, 5000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sess := &Session{ID: "s", Agent: "claude", StartedAt: new("2026-04-26T10:00:00Z"), EndedAt: tc.endedAt}
			got := AssembleTiming(sess, turns, nil, now, evidence)
			require.Equal(t, "source", got.TimingSource)
			require.Len(t, got.Activity, 2, "an unanswered prompt keeps its activity row")
			assert.False(t, got.Activity[0].Running)
			assert.Equal(t, tc.running, got.Activity[1].Running)
			assert.Equal(t, tc.active, got.ActiveDurationMs)
			if tc.running {
				// 12 minutes elapsed; only the gap between the two turns is idle.
				assert.Equal(t, int64(10*60*1000-5000), got.IdleDurationMs, "in-progress time is active, not idle")
			}
		})
	}
}

func TestTimingCheckpointCursorOmitsClosedEvidence(t *testing.T) {
	d := testDB(t)
	s := archivedTimingFixture(t, d)
	prepared := requireTimingCaptureMatchesFullFromFile(t, d, s)
	require.NoError(t, d.UpsertSession(t.Context(), prepared))
	var cursor []byte
	require.NoError(t, d.getReader().QueryRowContext(t.Context(),
		"SELECT cursor FROM session_timing_checkpoints WHERE session_id=?", s.ID).Scan(&cursor))
	var stored timingCursor
	require.NoError(t, json.Unmarshal(cursor, &stored))
	assert.Empty(t, stored.State.Evidence.Turns, "closed turns live only in the evidence row")
	assert.Empty(t, stored.State.Evidence.Model)
	assert.Empty(t, stored.State.Evidence.Calls)
	assert.Equal(t, 1, stored.Turns)
	assert.Equal(t, 1, stored.Model)
	assert.Equal(t, 1, stored.Calls)
	loaded := d.loadTimingCheckpoint(t.Context(), &s)
	require.NotNil(t, loaded)
	assert.Equal(t, prepared.TimingCapture.Checkpoint.State.Evidence, loaded.State.Evidence,
		"the closed prefix is rebuilt from the committed evidence row")
}

func requireTimingCaptureMatchesFullFromFile(t *testing.T, d *DB, s Session) Session {
	t.Helper()
	content, err := os.ReadFile(*s.FilePath)
	require.NoError(t, err)
	return requireTimingCaptureMatchesFull(t, d, s, content)
}
