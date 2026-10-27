package sync

import (
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/agentsview/internal/db"
	"go.kenn.io/agentsview/internal/parser"
	"go.kenn.io/agentsview/internal/testjsonl"
)

const codexTimingID = "11111111-2222-7333-8444-555555555555"

// Both outputs precede the completed-item records. A late incremental import
// therefore has to update calls already persisted before its checkpoint.
func codexTimingRollout() (prefix, tail string) {
	prefix = testjsonl.JoinJSONL(
		testjsonl.CodexSessionMetaJSON(codexTimingID, "/work/project", "codex_cli_rs", "2026-10-01T00:00:00Z"),
		testjsonl.CodexMsgJSON("user", "Run both commands", "2026-10-01T00:00:00Z"),
		`{"type":"response_item","timestamp":"2026-10-01T00:00:00.100Z","payload":{"type":"function_call","name":"exec_command","call_id":"call_first","arguments":"{\"cmd\":\"pwd\"}"}}`,
		`{"type":"response_item","timestamp":"2026-10-01T00:00:00.200Z","payload":{"type":"function_call","name":"exec_command","call_id":"call_second","arguments":"{\"cmd\":\"false\"}"}}`,
		`{"type":"response_item","timestamp":"2026-10-01T00:00:04Z","payload":{"type":"function_call_output","call_id":"call_first","output":"first output"}}`,
		`{"type":"response_item","timestamp":"2026-10-01T00:00:05Z","payload":{"type":"function_call_output","call_id":"call_second","output":"second output"}}`,
	)
	tail = testjsonl.JoinJSONL(
		`{"type":"event_msg","timestamp":"2026-10-01T00:00:06Z","payload":{"type":"item_completed","started_at_ms":1790812801000,"completed_at_ms":1790812804000,"item":{"type":"CommandExecution","id":"call_first","status":"completed","aggregated_output":"first output"}}}`,
		`{"type":"event_msg","timestamp":"2026-10-01T00:00:06Z","payload":{"type":"item_completed","started_at_ms":1790812802000,"completed_at_ms":1790812805000,"item":{"type":"CommandExecution","id":"call_second","status":"failed","aggregated_output":"second output"}}}`,
		testjsonl.CodexMsgJSON("assistant", "Finished", "2026-10-01T00:00:10Z"),
	)
	return prefix, tail
}

func TestCodexExecutionTimingSurvivesSync(t *testing.T) {
	for _, mode := range []string{"full", "late completion", "unknown nested IDs", "blocked output"} {
		t.Run(mode, func(t *testing.T) {
			root := t.TempDir()
			path := filepath.Join(root, "rollout-2026-10-01T00-00-00-"+codexTimingID+".jsonl")
			prefix, tail := codexTimingRollout()
			if mode == "unknown nested IDs" {
				tail = strings.NewReplacer("call_first", "nested_first", "call_second", "nested_second").Replace(tail)
			}
			content := prefix + tail
			if mode == "late completion" || mode == "unknown nested IDs" {
				content = prefix
			}
			require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
			database := openTestDB(t)
			cfg := EngineConfig{AgentDirs: map[parser.AgentType][]string{parser.AgentCodex: {root}}, Machine: "local"}
			if mode == "blocked output" {
				cfg.BlockedResultCategories = []string{"Bash"}
			}
			engine := NewEngine(t.Context(), database, cfg)
			t.Cleanup(engine.Close)
			require.NoError(t, engine.SyncPathsContext(t.Context(), []string{path}))
			if mode == "late completion" || mode == "unknown nested IDs" {
				before, err := database.GetSessionTiming(t.Context(), "codex:"+codexTimingID)
				require.NoError(t, err)
				require.Len(t, before.Turns, 2)
				require.Len(t, before.Turns[0].Calls, 1)
				assert.Equal(t, new(int64(3900)), before.Turns[0].Calls[0].DurationMs, "paired caller wait is available before execution telemetry")
				engine.Close()
				file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
				require.NoError(t, err)
				_, err = file.WriteString(tail)
				require.NoError(t, err)
				require.NoError(t, file.Close())
				engine = NewEngine(t.Context(), database, cfg)
				t.Cleanup(engine.Close)
				require.NoError(t, engine.SyncPathsContext(t.Context(), []string{path}))
				session, err := database.GetSessionFull(t.Context(), "codex:"+codexTimingID)
				require.NoError(t, err)
				require.True(t, session.LastWriteIncremental, "unique persisted call IDs must resolve without a full parse")
			}
			timing, err := database.GetSessionTiming(t.Context(), "codex:"+codexTimingID)
			require.NoError(t, err)
			require.Len(t, timing.Turns, 2)
			assert.Equal(t, int64(4900), timing.ToolDurationMs, "overlapping request/result waits count once")
			assert.Equal(t, int64(5100), timing.ActivityTotals.UnattributedMs)
			for i, turn := range timing.Turns {
				require.Len(t, turn.Calls, 1)
				require.NotNil(t, turn.Calls[0].DurationMs)
				if mode == "unknown nested IDs" {
					assert.Equal(t, []int64{3900, 4800}[i], *turn.Calls[0].DurationMs, "unmatched nested items leave paired latency intact")
				} else {
					assert.Equal(t, int64(3000), *turn.Calls[0].DurationMs, "explicit execution still takes precedence for individual calls")
				}
			}
			messages, err := database.GetAllMessages(t.Context(), "codex:"+codexTimingID)
			require.NoError(t, err)
			var outputs []string
			executionEvents := 0
			for _, message := range messages {
				for _, call := range message.ToolCalls {
					outputs = append(outputs, call.ResultContent)
					for _, event := range call.ResultEvents {
						if event.Source == "tool_execution" {
							executionEvents++
						}
					}
				}
			}
			if mode == "unknown nested IDs" {
				assert.Zero(t, executionEvents, "nested IDs must never create outer execution evidence")
			} else {
				assert.Equal(t, 4, executionEvents)
			}
			if mode == "blocked output" {
				assert.Equal(t, []string{"", ""}, outputs)
			} else {
				assert.Equal(t, []string{"first output", "second output"}, outputs, "timing events must not duplicate tool output")
			}
		})
	}
}

func TestCodexExecutionTimingUpgradePreservesIdentity(t *testing.T) {
	const previousVersion = 147
	root := t.TempDir()
	path := filepath.Join(root, "rollout-2026-10-01T00-00-00-"+codexTimingID+".jsonl")
	prefix, tail := codexTimingRollout()
	require.NoError(t, os.WriteFile(path, []byte(prefix+tail), 0o600))
	database := openTestDB(t)
	cfg := EngineConfig{AgentDirs: map[parser.AgentType][]string{parser.AgentCodex: {root}}, Machine: "local"}
	engine := NewEngine(t.Context(), database, cfg)
	require.Equal(t, 1, engine.SyncAll(t.Context(), nil).Synced)
	engine.Close()
	before, err := database.GetSession(t.Context(), "codex:"+codexTimingID)
	require.NoError(t, err)
	hashBefore, ok := database.GetSessionFileHash(t.Context(), before.ID)
	require.True(t, ok)
	archivePath := database.Path()
	require.NoError(t, database.Close())

	// Model an older parser's archive without changing any source fingerprints.
	archive, err := sql.Open("sqlite3", archivePath)
	require.NoError(t, err)
	_, err = archive.ExecContext(t.Context(), `DELETE FROM tool_result_events WHERE session_id=? AND source='tool_execution'`, before.ID)
	require.NoError(t, err)
	_, err = archive.ExecContext(t.Context(), `UPDATE sessions SET data_version=? WHERE id=?`, previousVersion, before.ID)
	require.NoError(t, err)
	_, err = archive.ExecContext(t.Context(), "PRAGMA user_version = 147")
	require.NoError(t, err)
	require.NoError(t, archive.Close())
	assert.ElementsMatch(t, []parser.AgentType{parser.AgentCodex, parser.AgentTraeX, parser.AgentAugureCode},
		db.ScopedDataVersionProviders(previousVersion, previousVersion+1))
	reopened, err := db.OpenIsolated(t.Context(), archivePath)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })
	upgraded := NewEngine(t.Context(), reopened, cfg)
	t.Cleanup(upgraded.Close)
	require.Equal(t, 1, upgraded.SyncAll(t.Context(), nil).Synced, "unchanged sources must be reparsed")
	after, err := reopened.GetSession(t.Context(), before.ID)
	require.NoError(t, err)
	assert.Equal(t, before.ID, after.ID)
	assert.Equal(t, before.FilePath, after.FilePath)
	hashAfter, ok := reopened.GetSessionFileHash(t.Context(), after.ID)
	require.True(t, ok)
	assert.Equal(t, hashBefore, hashAfter)
	assert.Equal(t, db.CurrentDataVersion(), reopened.GetSessionDataVersion(t.Context(), after.ID))
	timing, err := reopened.GetSessionTiming(t.Context(), after.ID)
	require.NoError(t, err)
	assert.Equal(t, int64(4900), timing.ToolDurationMs)
	page, err := reopened.ListSessions(t.Context(), db.SessionFilter{Limit: 10})
	require.NoError(t, err)
	assert.Len(t, page.Sessions, 1, "upgrade must not create duplicate sessions")
}

func TestCodexTimingUpdateResolution(t *testing.T) {
	database := openTestDB(t)
	require.NoError(t, database.UpsertSession(t.Context(), db.Session{
		ID: "timing", Agent: "codex", Project: "project", Machine: "local",
	}))
	require.NoError(t, database.InsertMessages(t.Context(), []db.Message{
		{SessionID: "timing", Ordinal: 1, Role: "assistant", ToolCalls: []db.ToolCall{{ToolUseID: "unique", ToolName: "exec_command", Category: "Bash"}}},
		{SessionID: "timing", Ordinal: 2, Role: "assistant", ToolCalls: []db.ToolCall{{ToolUseID: "reused", ToolName: "exec_command", Category: "Bash"}}},
		{SessionID: "timing", Ordinal: 3, Role: "assistant", ToolCalls: []db.ToolCall{{ToolUseID: "reused", ToolName: "exec_command", Category: "Bash"}}},
	}))
	engine := NewEngine(t.Context(), database, EngineConfig{Machine: "local"})
	t.Cleanup(engine.Close)
	for _, tc := range []struct {
		name, callID, source string
		safe                 bool
		want                 int
	}{
		{"unique target", "unique", "tool_execution", true, 1},
		{"unknown nested ID", "nested", "tool_execution", true, 0},
		{"reused ID requires full parse", "reused", "tool_execution", false, 0},
		{"other results require full parse", "unique", "function_call_output", false, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			updates, safe, err := engine.resolveCodexTimingUpdates(t.Context(), "timing", []parser.ParsedToolCallUpdate{{
				ToolUseID: tc.callID,
				ResultEvents: []parser.ParsedToolResultEvent{{
					Source: tc.source, Status: "started",
				}},
			}})
			require.NoError(t, err)
			assert.Equal(t, tc.safe, safe)
			require.Len(t, updates, tc.want)
			if tc.want == 1 {
				assert.True(t, updates[0].TargetKnown)
				assert.Equal(t, 1, updates[0].MessageOrdinal)
				assert.Zero(t, updates[0].CallIndex)
			}
		})
	}
}
