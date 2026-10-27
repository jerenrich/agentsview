package db

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTimingFallbackParentOfSameFileForkCannotUseWholeFile(t *testing.T) {
	d := testDB(t)
	content := strings.Join([]string{
		`{"timestamp":"2026-04-26T10:00:00Z","type":"user","sessionId":"main","message":{"content":"main prompt"}}`,
		`{"timestamp":"2026-04-26T10:00:02Z","type":"assistant","sessionId":"main","message":{"content":"main answer"}}`,
		`{"timestamp":"2026-04-26T11:00:00Z","type":"user","sessionId":"main","message":{"content":"fork prompt"}}`,
		`{"timestamp":"2026-04-26T11:00:03Z","type":"assistant","sessionId":"main","message":{"content":"fork answer"}}`,
	}, "\n") + "\n"
	path := filepath.Join(t.TempDir(), "main.jsonl")
	s := writeTimingSnapshot(t, Session{ID: "main", Agent: "claude", FilePath: &path}, []byte(content))
	// Seed an archive from before durable timing evidence was introduced.
	timingInsertSession(t, d, "main", "2026-04-26T10:00:00Z", "2026-04-26T10:00:02Z")
	_, err := d.getWriter().ExecContext(t.Context(), "UPDATE sessions SET agent='claude',file_path=?,file_size=?,file_mtime=?,file_hash=? WHERE id='main'", path, s.FileSize, s.FileMtime, s.FileHash)
	require.NoError(t, err)
	timingInsertMessage(t, d, "main", 0, "user", "main prompt", "2026-04-26T10:00:00Z", false)
	timingInsertMessage(t, d, "main", 1, "assistant", "main answer", "2026-04-26T10:00:02Z", false)
	timingInsertSession(t, d, "fork", "2026-04-26T11:00:00Z", "2026-04-26T11:00:03Z")
	_, err = d.getWriter().ExecContext(t.Context(), "UPDATE sessions SET agent='claude',file_path=?,parent_session_id='main',relationship_type='fork' WHERE id='fork'", path)
	require.NoError(t, err)
	timing, err := d.GetSessionTiming(t.Context(), "main")
	require.NoError(t, err)
	assert.Empty(t, timing.TimingSource, "known parent of a same-file fork must use archive bounds")
	assert.Equal(t, int64(2000), timing.ActiveDurationMs, "parent must not receive fork's 3 seconds")
	require.NoError(t, d.SoftDeleteSession(t.Context(), "fork"))
	timing, err = d.GetSessionTiming(t.Context(), "main")
	require.NoError(t, err)
	assert.Empty(t, timing.TimingSource, "trashing a fork does not remove its source records")
	assert.Equal(t, int64(2000), timing.ActiveDurationMs)
}

func TestTimingFailedContentRetainsCommittedRead(t *testing.T) {
	d := testDB(t)
	original := archivedTimingFixture(t, d)
	require.NoError(t, d.UpsertSession(t.Context(), original))
	before, err := d.GetSessionTiming(t.Context(), original.ID)
	require.NoError(t, err)
	require.Equal(t, int64(6000), before.ActiveDurationMs)
	content, err := os.ReadFile(*original.FilePath)
	require.NoError(t, err)
	content = append(content, []byte(`{"timestamp":"2026-04-26T11:00:00Z","type":"event_msg","payload":{"type":"task_started","turn_id":"second"}}`+"\n"+`{"timestamp":"2026-04-26T11:00:04Z","type":"event_msg","payload":{"type":"task_complete","turn_id":"second"}}`+"\n")...)
	incoming := writeTimingSnapshot(t, original, content)
	incoming.EndedAt = new("2026-04-26T11:00:04Z")
	incoming.MessageCount = 4
	incoming = d.PrepareSessionTimingEvidence(t.Context(), incoming)
	require.NotNil(t, incoming.TimingCapture)
	_, err = d.UpsertSessionPendingContent(t.Context(), incoming)
	require.NoError(t, err)
	_, err = d.getWriter().ExecContext(t.Context(), `CREATE TRIGGER reject_pending_timing_content BEFORE INSERT ON messages WHEN NEW.ordinal=3 BEGIN SELECT RAISE(ABORT,'injected failure'); END`)
	require.NoError(t, err)
	msgs, err := d.GetAllMessages(t.Context(), original.ID)
	require.NoError(t, err)
	msgs = append(msgs, Message{SessionID: original.ID, Ordinal: 3, Role: "assistant", Content: "not committed", Timestamp: *incoming.EndedAt})
	require.ErrorContains(t, d.ReplaceSessionMessages(t.Context(), incoming.ID, msgs), "injected failure")
	after, err := d.GetSessionTiming(t.Context(), original.ID)
	require.NoError(t, err)
	assert.Equal(t, before.ActiveDurationMs, after.ActiveDurationMs, "uncommitted source activity must not appear before content publication")
	assert.Equal(t, "persisted", after.Coverage.Evidence)
	require.NoError(t, os.Remove(*original.FilePath))
	deleted, err := d.GetSessionTiming(t.Context(), original.ID)
	require.NoError(t, err)
	assert.Equal(t, before.ActiveDurationMs, deleted.ActiveDurationMs)
	assert.Equal(t, before.ActivityTotals, deleted.ActivityTotals, "source loss must not hide already-archived model/tool evidence after a failed write")
	assert.Equal(t, "persisted", deleted.Coverage.Evidence, "retained old evidence must remain usable when failed content never replaced the old messages")
	archivePath := d.Path()
	require.NoError(t, d.Close())
	reopened, err := OpenIsolated(t.Context(), archivePath)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })
	retained, err := reopened.GetSessionTiming(t.Context(), original.ID)
	require.NoError(t, err)
	assert.Equal(t, before, retained, "the committed timing binding must survive restart without the log")
	_, err = reopened.getWriter().Exec(t.Context(), "DROP TRIGGER reject_pending_timing_content")
	require.NoError(t, err)
	incoming = writeTimingSnapshot(t, incoming, content)
	incoming = reopened.PrepareSessionTimingEvidence(t.Context(), incoming)
	_, err = reopened.UpsertSessionPendingContent(t.Context(), incoming)
	require.NoError(t, err)
	require.NoError(t, reopened.ReplaceSessionMessages(t.Context(), incoming.ID, msgs))
	require.NoError(t, reopened.PublishSessionTimingEvidence(t.Context(), incoming))
	timing, err := reopened.GetSessionTiming(t.Context(), incoming.ID)
	require.NoError(t, err)
	assert.Equal(t, "persisted", timing.Coverage.Evidence)
	assert.Equal(t, int64(10000), timing.ActiveDurationMs)
	var pending int
	require.NoError(t, reopened.getReader().QueryRowContext(t.Context(), "SELECT COUNT(*) FROM session_timing_pending WHERE session_id=?", incoming.ID).Scan(&pending))
	assert.Zero(t, pending)
}

func TestTimingFirstPendingImportHasNoPublishedSource(t *testing.T) {
	d := testDB(t)
	s := archivedTimingFixture(t, d)
	s.ID, s.SourceSessionID = "codex:new-import", "retained"
	s = d.PrepareSessionTimingEvidence(t.Context(), s)
	require.NotNil(t, s.TimingCapture)
	_, err := d.UpsertSessionPendingContent(t.Context(), s)
	require.NoError(t, err)
	timing, err := d.GetSessionTiming(t.Context(), s.ID)
	require.NoError(t, err)
	assert.Equal(t, "unavailable", timing.Coverage.Evidence)
	assert.Zero(t, timing.TotalDurationMs)
	assert.Zero(t, timing.ActiveDurationMs)
	assert.False(t, timing.Running)
}
