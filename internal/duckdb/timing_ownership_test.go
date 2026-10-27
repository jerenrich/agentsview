//go:build !(windows && arm64)

package duckdb

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/agentsview/internal/db"
	"go.kenn.io/agentsview/internal/storage"
)

func TestTimingSourceOnlyMirrorRejectsSharedClaudeFile(t *testing.T) {
	local := newLocalDB(t)
	path := filepath.Join(t.TempDir(), "main.jsonl")
	source := strings.Join([]string{
		`{"timestamp":"2026-04-26T10:00:00Z","type":"user","sessionId":"main","message":{"content":"main prompt"}}`,
		`{"timestamp":"2026-04-26T10:00:02Z","type":"assistant","sessionId":"main","message":{"content":"main answer"}}`,
		`{"timestamp":"2026-04-26T11:00:00Z","type":"user","sessionId":"main","message":{"content":"fork prompt"}}`,
		`{"timestamp":"2026-04-26T11:00:03Z","type":"assistant","sessionId":"main","message":{"content":"fork answer"}}`,
	}, "\n") + "\n"
	require.NoError(t, os.WriteFile(path, []byte(source), 0o600))
	info, err := os.Stat(path)
	require.NoError(t, err)
	for _, id := range []string{"main", "fork"} {
		start, end := "2026-04-26T10:00:00Z", "2026-04-26T10:00:02Z"
		if id == "fork" {
			start, end = "2026-04-26T11:00:00Z", "2026-04-26T11:00:03Z"
		}
		s := syncSession(id, "project", "question", start, 2)
		s.Agent, s.SourceSessionID = "claude", "main"
		s.EndedAt, s.FilePath, s.FileSize, s.FileMtime = &end, &path, new(info.Size()), new(info.ModTime().UnixNano())
		if id == "fork" {
			s.ParentSessionID, s.RelationshipType = new("main"), "fork"
		}
		_, err := local.WriteSessionBatchAtomic(t.Context(), []db.SessionBatchWrite{{
			Session: s, ReplaceMessages: true, DataVersion: db.CurrentDataVersion(),
			Messages: []db.Message{syncMessage(id, 0, "user", "question", start), syncMessage(id, 1, "assistant", "answer", end)},
		}})
		require.NoError(t, err)
	}
	syncer := newInMemoryTestSync(t, local, storage.MirrorPushOptions{})
	require.NoError(t, createSchema(t.Context(), syncer.DB()))
	store := NewStoreFromDB(syncer.DB())
	parent, err := local.GetSessionFull(t.Context(), "main")
	require.NoError(t, err)
	require.NotNil(t, parent)
	for _, trashed := range []bool{false, true} {
		if trashed {
			require.NoError(t, local.SoftDeleteSession(t.Context(), "fork"))
		}
		_, err := syncer.pushEverything(t.Context(), nil)
		require.NoError(t, err)
		turns, err := store.queryTurnRows(t.Context(), parent)
		require.NoError(t, err)
		control, err := db.AssembleTimingContext(t.Context(), parent, turns, nil, time.Now(), func(_ context.Context, _ string) ([]db.Session, error) { return nil, nil })
		require.NoError(t, err)
		require.Equal(t, "source", control.TimingSource, "the control proves the readable file would mix both branches")
		require.Equal(t, int64(5000), control.ActiveDurationMs)
		children, err := store.sessions().GetTimingSourceChildren(t.Context(), "main")
		require.NoError(t, err)
		require.Len(t, children, 1)
		require.Equal(t, &path, children[0].FilePath)
		timing, err := db.AssembleTimingContext(t.Context(), parent, turns, nil, time.Now(), store.sessions().GetTimingSourceChildren)
		require.NoError(t, err)
		assert.Empty(t, timing.TimingSource)
		assert.Equal(t, int64(2000), timing.ActiveDurationMs)
	}
}
