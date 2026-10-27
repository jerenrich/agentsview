package sync

import (
	"database/sql"
	"encoding/json/v2"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/agentsview/internal/db"
	"go.kenn.io/agentsview/internal/parser"
)

func TestTimingRepairUpgradeReplacesPreviewEvidence(t *testing.T) {
	const sessionID = "11111111-2222-4333-8444-555555555555"
	root := t.TempDir()
	path := filepath.Join(root, "project", sessionID+".jsonl")
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o700))
	source := strings.Join([]string{
		`{"timestamp":"2026-04-26T10:00:00Z","type":"user","sessionId":"` + sessionID + `","message":{"content":"first"}}`,
		`{"timestamp":"2026-04-26T10:00:02Z","type":"assistant","sessionId":"` + sessionID + `","message":{"content":"done"}}`,
		`{"timestamp":"2026-04-26T11:00:00Z","type":"attachment","sessionId":"` + sessionID + `","attachment":{"type":"queued_command","commandMode":"prompt","prompt":"next"}}`,
		`{"timestamp":"2026-04-26T11:00:03Z","type":"assistant","sessionId":"` + sessionID + `","message":{"content":"done"}}`,
	}, "\n") + "\n"
	require.NoError(t, os.WriteFile(path, []byte(source), 0o600))
	database := openTestDB(t)
	cfg := EngineConfig{
		AgentDirs: map[parser.AgentType][]string{parser.AgentClaude: {root}},
		Machine:   "local", DisableFilesystemProjectDiscovery: true,
	}
	engine := NewEngine(t.Context(), database, cfg)
	require.NoError(t, engine.SyncPathsContext(t.Context(), []string{path}))
	before, err := database.GetSessionFull(t.Context(), sessionID)
	require.NoError(t, err)
	require.NotNil(t, before)
	messages, err := database.GetAllMessages(t.Context(), sessionID)
	require.NoError(t, err)
	require.Len(t, messages, 4)
	engine.Close()

	// Version 152 dropped queued prompts from the timing reducer and persisted
	// a single window spanning the idle gap. Its checkpoint is incompatible.
	start := time.Date(2026, 4, 26, 10, 0, 0, 0, time.UTC).UnixMilli()
	oldEvidence, err := json.Marshal(parser.SessionTimingEvidence{
		SourceID: sessionID,
		Turns:    []parser.TimingInterval{{Start: start, End: start + 3603000}},
		Open:     true,
	})
	require.NoError(t, err)
	raw, err := sql.Open("sqlite3", database.Path())
	require.NoError(t, err)
	_, err = raw.ExecContext(t.Context(), "UPDATE session_timing_evidence SET evidence=? WHERE session_id=?", string(oldEvidence), sessionID)
	require.NoError(t, err)
	_, err = raw.ExecContext(t.Context(), "UPDATE session_timing_checkpoints SET version=2 WHERE session_id=?", sessionID)
	require.NoError(t, err)
	_, err = raw.ExecContext(t.Context(), "UPDATE sessions SET data_version=152 WHERE id=?", sessionID)
	require.NoError(t, err)
	_, err = raw.ExecContext(t.Context(), "PRAGMA user_version=152")
	require.NoError(t, err)
	require.NoError(t, raw.Close())
	oldTiming, err := database.GetSessionTiming(t.Context(), sessionID)
	require.NoError(t, err)
	require.Equal(t, int64(3603000), oldTiming.ActiveDurationMs)
	archivePath := database.Path()
	require.NoError(t, database.Close())

	reopened, err := db.OpenIsolated(t.Context(), archivePath)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })
	upgraded := NewEngine(t.Context(), reopened, cfg)
	t.Cleanup(upgraded.Close)
	require.Equal(t, 1, upgraded.SyncAll(t.Context(), nil).Synced)
	after, err := reopened.GetSessionFull(t.Context(), sessionID)
	require.NoError(t, err)
	require.NotNil(t, after)
	assert.Equal(t, before.ID, after.ID)
	assert.Equal(t, before.SourceSessionID, after.SourceSessionID)
	assert.Equal(t, before.FileHash, after.FileHash)
	afterMessages, err := reopened.GetAllMessages(t.Context(), sessionID)
	require.NoError(t, err)
	require.Len(t, afterMessages, len(messages))
	for i := range messages {
		assert.Equal(t, messages[i].ID, afterMessages[i].ID)
	}
	require.NoError(t, os.Remove(path))
	timing, err := reopened.GetSessionTiming(t.Context(), sessionID)
	require.NoError(t, err)
	assert.Equal(t, "persisted", timing.Coverage.Evidence)
	assert.Equal(t, int64(5000), timing.ActiveDurationMs)
	assert.Len(t, timing.Activity, 2)
}

func TestTimingUpgradeRepairsRepeatedQueuesWithoutChangingIdentities(t *testing.T) {
	const id = "11111111-2222-4333-8444-555555555555"
	root := t.TempDir()
	path := filepath.Join(root, "project", id+".jsonl")
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o700))
	source := strings.Join([]string{
		`{"timestamp":"2026-04-26T10:00:00Z","type":"user","sessionId":"` + id + `","message":{"content":"first"}}`,
		`{"timestamp":"2026-04-26T10:00:01Z","type":"assistant","sessionId":"` + id + `","message":{"content":[{"type":"tool_use","id":"call","name":"Bash","input":{}}]}}`,
		`{"timestamp":"2026-04-26T10:00:05Z","type":"attachment","sessionId":"` + id + `","attachment":{"type":"queued_command","prompt":"second"}}`,
		`{"timestamp":"2026-04-26T10:00:07Z","type":"attachment","sessionId":"` + id + `","attachment":{"type":"queued_command","prompt":"third"}}`,
		`{"timestamp":"2026-04-26T10:00:10Z","type":"user","sessionId":"` + id + `","message":{"content":[{"type":"tool_result","tool_use_id":"call","content":"done"}]}}`,
		`{"timestamp":"2026-04-26T10:00:12Z","type":"assistant","sessionId":"` + id + `","message":{"content":"done","stop_reason":"end_turn"}}`,
	}, "\n") + "\n"
	require.NoError(t, os.WriteFile(path, []byte(source), 0o600))
	database := openTestDB(t)
	cfg := EngineConfig{AgentDirs: map[parser.AgentType][]string{parser.AgentClaude: {root}}, Machine: "local", DisableFilesystemProjectDiscovery: true}
	engine := NewEngine(t.Context(), database, cfg)
	require.NoError(t, engine.SyncPathsContext(t.Context(), []string{path}))
	before, err := database.GetSessionFull(t.Context(), id)
	require.NoError(t, err)
	require.NotNil(t, before)
	messages, err := database.GetAllMessages(t.Context(), id)
	require.NoError(t, err)
	require.Len(t, messages, 5)
	engine.Close()
	start := time.Date(2026, 4, 26, 10, 0, 0, 0, time.UTC).UnixMilli()
	evidence, err := parser.ReadSessionTiming(t.Context(), strings.NewReader(source), "claude")
	require.NoError(t, err)
	// Version 154 omitted the active interval between the two queued prompts.
	evidence.Turns = []parser.TimingInterval{{Start: start, End: start + 5000}, {Start: start + 7000, End: start + 12000}}
	encoded, err := json.Marshal(evidence)
	require.NoError(t, err)
	raw, err := sql.Open("sqlite3", database.Path())
	require.NoError(t, err)
	_, err = raw.ExecContext(t.Context(), "UPDATE session_timing_evidence SET evidence=? WHERE session_id=?", string(encoded), id)
	require.NoError(t, err)
	_, err = raw.ExecContext(t.Context(), "UPDATE session_timing_checkpoints SET version=4 WHERE session_id=?", id)
	require.NoError(t, err)
	_, err = raw.ExecContext(t.Context(), "UPDATE sessions SET data_version=154 WHERE id=?", id)
	require.NoError(t, err)
	_, err = raw.ExecContext(t.Context(), "PRAGMA user_version=154")
	require.NoError(t, err)
	require.NoError(t, raw.Close())
	old, err := database.GetSessionTiming(t.Context(), id)
	require.NoError(t, err)
	require.Equal(t, int64(10000), old.ActiveDurationMs)
	archivePath := database.Path()
	require.NoError(t, database.Close())
	reopened, err := db.OpenIsolated(t.Context(), archivePath)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })
	upgraded := NewEngine(t.Context(), reopened, cfg)
	t.Cleanup(upgraded.Close)
	require.Equal(t, 1, upgraded.SyncAll(t.Context(), nil).Synced)
	after, err := reopened.GetSessionFull(t.Context(), id)
	require.NoError(t, err)
	require.NotNil(t, after)
	assert.Equal(t, before.ID, after.ID)
	assert.Equal(t, before.SourceSessionID, after.SourceSessionID)
	assert.Equal(t, before.FileHash, after.FileHash)
	afterMessages, err := reopened.GetAllMessages(t.Context(), id)
	require.NoError(t, err)
	require.Len(t, afterMessages, len(messages))
	for i := range messages {
		assert.Equal(t, messages[i].ID, afterMessages[i].ID)
	}
	require.NoError(t, os.Remove(path))
	timing, err := reopened.GetSessionTiming(t.Context(), id)
	require.NoError(t, err)
	assert.Equal(t, "persisted", timing.Coverage.Evidence)
	assert.Equal(t, int64(12000), timing.ActiveDurationMs)
	assert.Equal(t, int64(9000), timing.ActivityTotals.ToolMs)
}
