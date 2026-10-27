package db

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/agentsview/internal/export"
)

func TestPendingSessionTimingPublishesAfterContent(t *testing.T) {
	for _, identity := range []bool{false, true} {
		t.Run(map[bool]string{false: "plain upsert", true: "project identity upsert"}[identity], func(t *testing.T) {
			d := testDB(t)
			s := archivedTimingFixture(t, d)
			s = d.PrepareSessionTimingEvidence(t.Context(), s)
			require.NotNil(t, s.TimingCapture)
			if identity {
				_, err := d.UpsertSessionPendingContentWithProjectIdentity(t.Context(), s, export.ProjectIdentityObservation{SessionID: s.ID, Project: s.Project, Machine: s.Machine, RootPath: "/workspace/project"}, s.Project)
				require.NoError(t, err)
			} else {
				_, err := d.UpsertSessionPendingContent(t.Context(), s)
				require.NoError(t, err)
			}
			var records int
			require.NoError(t, d.getReader().QueryRowContext(t.Context(), "SELECT COUNT(*) FROM session_timing_evidence").Scan(&records))
			assert.Zero(t, records, "pending source metadata is not committed content")
			msgs, err := d.GetAllMessages(t.Context(), s.ID)
			require.NoError(t, err)
			// The capture must survive loss of the source while normalized content is written.
			require.NoError(t, os.Remove(*s.FilePath))
			require.NoError(t, d.ReplaceSessionMessages(t.Context(), s.ID, msgs))
			require.NoError(t, d.PublishSessionTimingEvidence(t.Context(), s))
			timing, err := d.GetSessionTiming(t.Context(), s.ID)
			require.NoError(t, err)
			assert.Equal(t, "persisted", timing.Coverage.Evidence)
			assert.Equal(t, int64(6000), timing.ActiveDurationMs)
		})
	}
}

func TestPendingSessionContentFailureRetainsPreviousTiming(t *testing.T) {
	d := testDB(t)
	original := archivedTimingFixture(t, d)
	require.NoError(t, d.UpsertSession(t.Context(), original))
	var previous string
	require.NoError(t, d.getReader().QueryRowContext(t.Context(), "SELECT evidence FROM session_timing_evidence WHERE session_id=?", original.ID).Scan(&previous))
	bytes, err := os.ReadFile(*original.FilePath)
	require.NoError(t, err)
	bytes = append(bytes, []byte(`{"timestamp":"2026-04-26T11:00:00Z","type":"event_msg","payload":{"type":"task_started","turn_id":"second"}}`+"\n"+`{"timestamp":"2026-04-26T11:00:04Z","type":"event_msg","payload":{"type":"task_complete","turn_id":"second"}}`+"\n")...)
	require.NoError(t, os.WriteFile(*original.FilePath, bytes, 0600))
	info, err := os.Stat(*original.FilePath)
	require.NoError(t, err)
	digest := sha256.Sum256(bytes)
	hash := hex.EncodeToString(digest[:])
	incoming := original
	incoming.FileSize = new(info.Size())
	incoming.FileMtime = new(info.ModTime().UnixNano())
	incoming.FileHash = &hash
	incoming.EndedAt = new("2026-04-26T11:00:04Z")
	incoming.MessageCount = 4
	incoming = d.PrepareSessionTimingEvidence(t.Context(), incoming)
	require.NotNil(t, incoming.TimingCapture)
	_, err = d.UpsertSessionPendingContent(t.Context(), incoming)
	require.NoError(t, err)
	_, err = d.getWriter().ExecContext(t.Context(), `CREATE TRIGGER reject_timing_content BEFORE INSERT ON messages WHEN NEW.ordinal=3 BEGIN SELECT RAISE(ABORT,'injected content failure'); END`)
	require.NoError(t, err)
	msgs, err := d.GetAllMessages(t.Context(), original.ID)
	require.NoError(t, err)
	msgs = append(msgs, Message{SessionID: original.ID, Ordinal: 3, Role: "assistant", Content: "not committed", Timestamp: *incoming.EndedAt})
	require.ErrorContains(t, d.ReplaceSessionMessages(t.Context(), incoming.ID, msgs), "injected content failure")
	var after string
	require.NoError(t, d.getReader().QueryRowContext(t.Context(), "SELECT evidence FROM session_timing_evidence WHERE session_id=?", original.ID).Scan(&after))
	assert.Equal(t, previous, after, "failed content cannot advance archived timing")
	stored, err := d.GetAllMessages(t.Context(), original.ID)
	require.NoError(t, err)
	assert.Len(t, stored, 3)
}

func TestTimingPublicationRejectsChangedSourceOrCancellation(t *testing.T) {
	for _, cancelled := range []bool{false, true} {
		t.Run(map[bool]string{false: "changed source", true: "cancelled publication"}[cancelled], func(t *testing.T) {
			d := testDB(t)
			s := archivedTimingFixture(t, d)
			prepared := d.PrepareSessionTimingEvidence(t.Context(), s)
			require.NotNil(t, prepared.TimingCapture)
			_, err := d.UpsertSessionPendingContent(t.Context(), prepared)
			require.NoError(t, err)
			if cancelled {
				ctx, cancel := context.WithCancel(t.Context())
				cancel()
				require.ErrorIs(t, d.PublishSessionTimingEvidence(ctx, prepared), context.Canceled)
			} else {
				_, err = d.getWriter().ExecContext(t.Context(), "UPDATE sessions SET file_hash='different imported source' WHERE id=?", s.ID)
				require.NoError(t, err)
				require.ErrorIs(t, d.PublishSessionTimingEvidence(t.Context(), prepared), ErrSessionChanged)
			}
			var n int
			require.NoError(t, d.getReader().QueryRowContext(t.Context(), "SELECT COUNT(*) FROM session_timing_evidence").Scan(&n))
			assert.Zero(t, n)
		})
	}
}

func TestPreparedTimingCaptureCanSurviveDeletionBeforeUpsert(t *testing.T) {
	d := testDB(t)
	s := archivedTimingFixture(t, d)
	prepared := d.PrepareSessionTimingEvidence(t.Context(), s)
	require.NotNil(t, prepared.TimingCapture)
	require.NoError(t, os.Remove(*s.FilePath))
	same := d.PrepareSessionTimingEvidence(t.Context(), prepared)
	assert.Same(t, prepared.TimingCapture, same.TimingCapture)
	require.NoError(t, d.UpsertSession(t.Context(), prepared))
	got, err := d.GetSessionTiming(t.Context(), s.ID)
	require.NoError(t, err)
	assert.Equal(t, "persisted", got.Coverage.Evidence)
	changed := prepared
	changed.FileHash = new("different imported source")
	changed = d.PrepareSessionTimingEvidence(t.Context(), changed)
	assert.Nil(t, changed.TimingCapture, "reuse requires the complete source binding, even after the source disappears")
}
