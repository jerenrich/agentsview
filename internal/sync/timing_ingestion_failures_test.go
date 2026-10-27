package sync

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/agentsview/internal/db"
	"go.kenn.io/agentsview/internal/parser"
)

func TestInitialTimingPublicationFailureKeepsCommittedBounds(t *testing.T) {
	for _, mode := range []struct {
		name         string
		agent        parser.AgentType
		staged, bulk bool
	}{
		{name: "claude", agent: parser.AgentClaude},
		{name: "codex", agent: parser.AgentCodex},
		{name: "codex staged", agent: parser.AgentCodex, staged: true},
		{name: "codex bulk staged", agent: parser.AgentCodex, staged: true, bulk: true},
	} {
		t.Run(mode.name, func(t *testing.T) {
			const id = "11111111-2222-4333-8444-555555555555"
			root := t.TempDir()
			path := filepath.Join(root, "project", id+".jsonl")
			sessionID := id
			source := `{"timestamp":"2026-04-26T10:00:00Z","type":"user","sessionId":"` + id + `","message":{"content":"request"}}` + "\n" +
				`{"timestamp":"2026-04-26T10:00:02Z","type":"assistant","sessionId":"` + id + `","message":{"content":[{"type":"tool_use","id":"call","name":"Bash","input":{}}]}}` + "\n"
			if mode.agent == parser.AgentCodex {
				path = filepath.Join(root, "rollout-2026-04-26T10-00-00-"+id+".jsonl")
				sessionID = "codex:" + id
				source = `{"timestamp":"2026-04-26T10:00:00Z","type":"session_meta","payload":{"id":"` + id + `","cwd":"/work/project","source":"cli"}}
{"timestamp":"2026-04-26T10:00:00Z","type":"event_msg","payload":{"type":"task_started","turn_id":"pending"}}
{"timestamp":"2026-04-26T10:00:00Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"request"}]}}
{"timestamp":"2026-04-26T10:00:02Z","type":"response_item","payload":{"type":"function_call","name":"exec_command","call_id":"call","arguments":"{}"}}
`
			}
			require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o700))
			require.NoError(t, os.WriteFile(path, []byte(source), 0o600))
			database := openTestDB(t)
			raw, err := sql.Open("sqlite3", database.Path())
			require.NoError(t, err)
			defer raw.Close()
			_, err = raw.ExecContext(t.Context(), `CREATE TRIGGER reject_initial_timing BEFORE INSERT ON session_timing_evidence BEGIN SELECT RAISE(ABORT,'injected timing publication failure'); END`)
			require.NoError(t, err)
			cfg := EngineConfig{AgentDirs: map[parser.AgentType][]string{mode.agent: {root}}, Machine: "local", DisableFilesystemProjectDiscovery: true, DiscardPendingWritesOnCancel: mode.bulk}
			if mode.staged {
				cfg.StagedCodexParseMinBytes = 1
				cfg.CodexStagingDir = t.TempDir()
			}
			engine := NewEngine(t.Context(), database, cfg)
			t.Cleanup(engine.Close)
			require.ErrorContains(t, engine.SyncPathsContext(t.Context(), []string{path}), "failures")
			messages, err := database.GetAllMessages(t.Context(), sessionID)
			require.NoError(t, err)
			require.Len(t, messages, 2, "normalized content committed before the injected publication failure")
			timing, err := database.GetSessionTiming(t.Context(), sessionID)
			require.NoError(t, err)
			require.NotNil(t, timing)
			assert.False(t, timing.Running)
			assert.Equal(t, int64(2000), timing.TotalDurationMs)
			assert.Equal(t, int64(2000), timing.ActiveDurationMs)
			assert.Equal(t, "unavailable", timing.Coverage.Evidence)
			require.Len(t, timing.Activity, 1)
			assert.False(t, timing.Activity[0].Running)
			engine.Close()
			archivePath := database.Path()
			require.NoError(t, database.Close())
			database, err = db.OpenIsolated(t.Context(), archivePath)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, database.Close()) })
			engine = NewEngine(t.Context(), database, cfg)
			t.Cleanup(engine.Close)
			restarted, err := database.GetSessionTiming(t.Context(), sessionID)
			require.NoError(t, err)
			assert.Equal(t, timing, restarted, "pending bounds survive restart before retry")
			require.NoError(t, os.Remove(path))
			missing, err := database.GetSessionTiming(t.Context(), sessionID)
			require.NoError(t, err)
			assert.Equal(t, timing, missing, "a failed publication must not depend on a readable source")
			require.NoError(t, os.WriteFile(path, []byte(source), 0o600))
			_, err = raw.ExecContext(t.Context(), "DROP TRIGGER reject_initial_timing")
			require.NoError(t, err)
			require.NoError(t, engine.SyncPathsContext(t.Context(), []string{path}))
			assert.Equal(t, db.CurrentDataVersion(), database.GetSessionDataVersion(t.Context(), sessionID))
			retry, err := database.GetSessionTiming(t.Context(), sessionID)
			require.NoError(t, err)
			assert.Equal(t, "persisted", retry.Coverage.Evidence)
			assert.Equal(t, int64(2000), retry.ActiveDurationMs)
			assert.False(t, retry.Running)
		})
	}
}

func TestIngestedClaudeInterruptionStopsLiveTiming(t *testing.T) {
	for _, marker := range []string{"[Request interrupted by user]", "[Request interrupted by user for tool use]"} {
		for _, appendOnly := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/append=%t", marker, appendOnly), func(t *testing.T) {
				const id = "11111111-2222-4333-8444-555555555555"
				root := t.TempDir()
				path := filepath.Join(root, "project", id+".jsonl")
				require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o700))
				start := time.Now().UTC().Add(-30 * time.Second)
				prefix := fmt.Sprintf(`{"timestamp":%q,"type":"user","sessionId":%q,"message":{"content":"request"}}
{"timestamp":%q,"type":"assistant","sessionId":%q,"message":{"content":[{"type":"tool_use","id":"call","name":"Bash","input":{}}]}}
`, start.Format(time.RFC3339Nano), id, start.Add(time.Second).Format(time.RFC3339Nano), id)
				interruption := fmt.Sprintf(`{"timestamp":%q,"type":"user","sessionId":%q,"message":{"content":%q}}`+"\n", start.Add(2*time.Second).Format(time.RFC3339Nano), id, marker)
				// Native cancellation can acknowledge the tool after the interruption.
				cancellation := fmt.Sprintf(`{"timestamp":%q,"type":"user","sessionId":%q,"message":{"content":[{"type":"tool_result","tool_use_id":"call","is_error":true,"content":"Tool execution interrupted by user"}]}}`+"\n", start.Add(3*time.Second).Format(time.RFC3339Nano), id)
				content := prefix + interruption + cancellation
				initial := content
				if appendOnly {
					initial = prefix
				}
				require.NoError(t, os.WriteFile(path, []byte(initial), 0o600))
				database := openTestDB(t)
				engine := NewEngine(t.Context(), database, EngineConfig{AgentDirs: map[parser.AgentType][]string{parser.AgentClaude: {root}}, Machine: "local", DisableFilesystemProjectDiscovery: true})
				t.Cleanup(engine.Close)
				require.NoError(t, engine.SyncPathsContext(t.Context(), []string{path}))
				if appendOnly {
					before, err := database.GetSessionTiming(t.Context(), id)
					require.NoError(t, err)
					require.True(t, before.Running)
					require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
					require.NoError(t, engine.SyncPathsContext(t.Context(), []string{path}))
				}
				messages, err := database.GetAllMessages(t.Context(), id)
				require.NoError(t, err)
				require.Len(t, messages, 3)
				assert.True(t, messages[2].IsSystem)
				assert.Equal(t, "interrupted", messages[2].SourceSubtype)
				timing, err := database.GetSessionTiming(t.Context(), id)
				require.NoError(t, err)
				require.NotNil(t, timing)
				assert.False(t, timing.Running)
				assert.Equal(t, int64(2000), timing.ActiveDurationMs)
				assert.Equal(t, "persisted", timing.Coverage.Evidence)
				require.Len(t, timing.Activity, 1)
				assert.False(t, timing.Activity[0].Running)
				require.NoError(t, os.Remove(path))
				retained, err := database.GetSessionTiming(t.Context(), id)
				require.NoError(t, err)
				assert.Equal(t, timing, retained)
			})
		}
	}
}
