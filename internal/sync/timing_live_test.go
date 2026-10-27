package sync

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/agentsview/internal/parser"
)

func TestIngestedTimingLiveness(t *testing.T) {
	for _, agent := range []parser.AgentType{parser.AgentClaude, parser.AgentCodex} {
		for _, mode := range []string{"unfinished", "completion append", "stale", "missing source"} {
			t.Run(string(agent)+"/"+mode, func(t *testing.T) {
				const id = "11111111-2222-4333-8444-555555555555"
				root := t.TempDir()
				path := filepath.Join(root, "project", id+".jsonl")
				if agent == parser.AgentCodex {
					path = filepath.Join(root, "rollout-2026-10-27T21-00-00-"+id+".jsonl")
				}
				require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o700))
				start := time.Now().UTC().Add(-5 * time.Second)
				if mode == "stale" {
					start = start.Add(-time.Hour)
				}
				stamp := start.Format(time.RFC3339Nano)
				sessionID := id
				content := fmt.Sprintf(`{"timestamp":%q,"type":"user","sessionId":%q,"message":{"content":"Please do the task"}}`+"\n", stamp, id)
				completion := fmt.Sprintf(`{"timestamp":%q,"type":"assistant","sessionId":%q,"message":{"content":"Done","stop_reason":"end_turn"}}`+"\n", start.Add(2*time.Second).Format(time.RFC3339Nano), id)
				if agent == parser.AgentCodex {
					sessionID = "codex:" + id
					content = fmt.Sprintf(`{"timestamp":%q,"type":"session_meta","payload":{"id":%q,"cwd":"/work/project","source":"cli"}}
{"timestamp":%q,"type":"event_msg","payload":{"type":"task_started","turn_id":"pending"}}
{"timestamp":%q,"type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"Please do the task"}]}}
`, stamp, id, stamp, stamp)
					completion = fmt.Sprintf(`{"timestamp":%q,"type":"event_msg","payload":{"type":"task_complete","turn_id":"pending"}}`+"\n", start.Add(2*time.Second).Format(time.RFC3339Nano))
				}
				require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
				database := openTestDB(t)
				engine := NewEngine(t.Context(), database, EngineConfig{
					AgentDirs: map[parser.AgentType][]string{agent: {root}},
					Machine:   "local", DisableFilesystemProjectDiscovery: true,
				})
				t.Cleanup(engine.Close)
				require.NoError(t, engine.SyncPathsContext(t.Context(), []string{path}))
				if mode == "completion append" {
					before, err := database.GetSessionTiming(t.Context(), sessionID)
					require.NoError(t, err)
					require.True(t, before.Running)
					require.NoError(t, os.WriteFile(path, []byte(content+completion), 0o600))
					require.NoError(t, engine.SyncPathsContext(t.Context(), []string{path}))
				}
				if mode == "missing source" {
					require.NoError(t, os.Remove(path))
				}
				timing, err := database.GetSessionTiming(t.Context(), sessionID)
				require.NoError(t, err)
				require.NotNil(t, timing)
				require.Len(t, timing.Activity, 1)
				assert.Equal(t, "persisted", timing.Coverage.Evidence)
				if mode == "unfinished" {
					assert.True(t, timing.Running)
					assert.True(t, timing.Activity[0].Running)
					assert.GreaterOrEqual(t, timing.ActiveDurationMs, int64(5000))
				} else {
					assert.False(t, timing.Running)
					assert.False(t, timing.Activity[0].Running)
					expected := int64(0)
					if mode == "completion append" {
						expected = 2000
					}
					assert.Equal(t, expected, timing.ActiveDurationMs)
				}
			})
		}
	}
}
