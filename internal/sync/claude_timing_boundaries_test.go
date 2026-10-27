package sync

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/agentsview/internal/parser"
)

func TestClaudeTimingIngestedQueuedWaitAndIgnoredUsers(t *testing.T) {
	for _, mode := range []string{"queued full", "queued append", "command noise", "blank user"} {
		t.Run(mode, func(t *testing.T) {
			root := t.TempDir()
			path := filepath.Join(root, "project", "timing.jsonl")
			require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o700))
			lines := []string{
				`{"timestamp":"2026-04-26T10:00:00Z","type":"user","sessionId":"timing","message":{"content":"first"}}`,
				`{"timestamp":"2026-04-26T10:00:01Z","type":"assistant","sessionId":"timing","message":{"id":"response","content":[{"type":"tool_use","id":"call","name":"Bash","input":{}}]}}`,
				`{"timestamp":"2026-04-26T10:00:05Z","type":"attachment","sessionId":"timing","attachment":{"type":"queued_command","prompt":"also check this"}}`,
				`{"timestamp":"2026-04-26T10:00:10Z","type":"user","sessionId":"timing","message":{"content":[{"type":"tool_result","tool_use_id":"call","content":"done"}]}}`,
				`{"timestamp":"2026-04-26T10:00:12Z","type":"assistant","sessionId":"timing","message":{"content":"done","stop_reason":"end_turn"}}`,
			}
			active, tool := int64(12000), int64(9000)
			if mode == "command noise" || mode == "blank user" {
				noise := "<local-command-stdout>output</local-command-stdout>"
				if mode == "blank user" {
					noise = "   "
				}
				lines = []string{lines[0],
					`{"timestamp":"2026-04-26T10:00:01Z","type":"assistant","sessionId":"timing","message":{"content":"working"}}`,
					`{"timestamp":"2026-04-26T10:00:02Z","type":"user","sessionId":"timing","message":{"content":"` + noise + `"}}`,
					`{"timestamp":"2026-04-26T10:00:03Z","type":"assistant","sessionId":"timing","message":{"content":"done","stop_reason":"end_turn"}}`,
				}
				active, tool = 3000, 0
			}
			content := strings.Join(lines, "\n") + "\n"
			initial := content
			if mode == "queued append" {
				initial = strings.Join(lines[:2], "\n") + "\n"
			}
			require.NoError(t, os.WriteFile(path, []byte(initial), 0o600))
			database := openTestDB(t)
			engine := NewEngine(t.Context(), database, EngineConfig{
				AgentDirs: map[parser.AgentType][]string{parser.AgentClaude: {root}},
				Machine:   "local", DisableFilesystemProjectDiscovery: true,
			})
			t.Cleanup(engine.Close)
			require.NoError(t, engine.SyncPathsContext(t.Context(), []string{path}))
			if mode == "queued append" {
				require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
				require.NoError(t, engine.SyncPathsContext(t.Context(), []string{path}))
			}
			timing, err := database.GetSessionTiming(t.Context(), "timing")
			require.NoError(t, err)
			require.NotNil(t, timing)
			assert.Equal(t, "persisted", timing.Coverage.Evidence)
			assert.Equal(t, active, timing.ActiveDurationMs)
			assert.Equal(t, tool, timing.ToolDurationMs)
			assert.Equal(t, tool, timing.ActivityTotals.ToolMs)
			require.NoError(t, os.Remove(path))
			retained, err := database.GetSessionTiming(t.Context(), "timing")
			require.NoError(t, err)
			assert.Equal(t, timing, retained)
		})
	}
}
