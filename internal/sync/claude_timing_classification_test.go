package sync

import (
	"database/sql"
	"encoding/json/v2"
	"fmt"
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

type claudeTimingPromptCase struct {
	name              string
	lines             []string
	users, activities int
}

func claudeTimingPromptCases() []claudeTimingPromptCase {
	return []claudeTimingPromptCase{
		{name: "prompt beside tool result", users: 2, activities: 2, lines: []string{
			`{"timestamp":"2026-04-26T10:00:00Z","type":"user","sessionId":"timing","message":{"content":"first"}}`,
			`{"timestamp":"2026-04-26T10:00:01Z","type":"assistant","sessionId":"timing","message":{"content":[{"type":"tool_use","id":"call","name":"Bash","input":{}}]}}`,
			`{"timestamp":"2026-04-26T10:00:02Z","type":"user","sessionId":"timing","message":{"content":[{"type":"tool_result","tool_use_id":"call","content":"done"},{"type":"text","text":"Also check the tests"}]}}`,
			`{"timestamp":"2026-04-26T10:00:03Z","type":"assistant","sessionId":"timing","message":{"content":"done","stop_reason":"end_turn"}}`,
		}},
		{name: "IDE wrapped command noise", users: 1, activities: 1, lines: []string{
			`{"timestamp":"2026-04-26T10:00:00Z","type":"user","sessionId":"timing","message":{"content":"first"}}`,
			`{"timestamp":"2026-04-26T10:00:01Z","type":"assistant","sessionId":"timing","message":{"content":"working"}}`,
			`{"timestamp":"2026-04-26T10:00:02Z","type":"user","sessionId":"timing","message":{"content":"<ide_selection>The user selected package main.</ide_selection>\n<command-message></command-message>"}}`,
			`{"timestamp":"2026-04-26T10:00:03Z","type":"assistant","sessionId":"timing","message":{"content":"done","stop_reason":"end_turn"}}`,
		}},
	}
}

func TestClaudeTimingCanonicalPromptIngestion(t *testing.T) {
	for _, tc := range claudeTimingPromptCases() {
		for _, appendOnly := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/append=%t", tc.name, appendOnly), func(t *testing.T) {
				root := t.TempDir()
				path := filepath.Join(root, "project", "timing.jsonl")
				require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o700))
				source := strings.Join(tc.lines, "\n") + "\n"
				initial := source
				if appendOnly {
					initial = strings.Join(tc.lines[:2], "\n") + "\n"
				}
				require.NoError(t, os.WriteFile(path, []byte(initial), 0o600))
				database := openTestDB(t)
				engine := NewEngine(t.Context(), database, EngineConfig{AgentDirs: map[parser.AgentType][]string{parser.AgentClaude: {root}}, Machine: "local", DisableFilesystemProjectDiscovery: true})
				t.Cleanup(engine.Close)
				require.NoError(t, engine.SyncPathsContext(t.Context(), []string{path}))
				if appendOnly {
					require.NoError(t, os.WriteFile(path, []byte(source), 0o600))
					require.NoError(t, engine.SyncPathsContext(t.Context(), []string{path}))
				}
				session, err := database.GetSessionFull(t.Context(), "timing")
				require.NoError(t, err)
				require.NotNil(t, session)
				assert.Equal(t, tc.users, session.UserMessageCount)
				timing, err := database.GetSessionTiming(t.Context(), "timing")
				require.NoError(t, err)
				require.NotNil(t, timing)
				assert.Equal(t, "persisted", timing.Coverage.Evidence)
				assert.Equal(t, tc.activities, timing.Coverage.RecordedTurns)
				require.Len(t, timing.Activity, tc.activities)
				assert.Equal(t, int64(3000), timing.ActiveDurationMs)
				assert.False(t, timing.Running)
				if tc.users == 2 {
					assert.Equal(t, int64(2000), timing.Activity[0].DurationMs)
					assert.Equal(t, int64(1000), timing.Activity[1].DurationMs)
					assert.Equal(t, int64(1000), timing.Activity[0].ToolMs, "the result closes the preceding prompt's wait")
					assert.Zero(t, timing.Activity[1].ToolMs)
					assert.NotEqual(t, timing.Activity[0].MessageID, timing.Activity[1].MessageID, "the mixed prompt keeps its navigation anchor")
				}
				require.NoError(t, os.Remove(path))
				retained, err := database.GetSessionTiming(t.Context(), "timing")
				require.NoError(t, err)
				assert.Equal(t, timing, retained)
			})
		}
	}
}

func TestClaudePromptTimingUpgradePreservesIdentities(t *testing.T) {
	for _, tc := range claudeTimingPromptCases() {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			path := filepath.Join(root, "project", "timing.jsonl")
			require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o700))
			source := strings.Join(tc.lines, "\n") + "\n"
			require.NoError(t, os.WriteFile(path, []byte(source), 0o600))
			database := openTestDB(t)
			cfg := EngineConfig{AgentDirs: map[parser.AgentType][]string{parser.AgentClaude: {root}}, Machine: "local", DisableFilesystemProjectDiscovery: true}
			engine := NewEngine(t.Context(), database, cfg)
			require.NoError(t, engine.SyncPathsContext(t.Context(), []string{path}))
			before, err := database.GetSessionFull(t.Context(), "timing")
			require.NoError(t, err)
			messages, err := database.GetAllMessages(t.Context(), "timing")
			require.NoError(t, err)
			engine.Close()
			evidence, err := parser.ReadSessionTiming(t.Context(), strings.NewReader(source), "claude")
			require.NoError(t, err)
			start := time.Date(2026, 4, 26, 10, 0, 0, 0, time.UTC).UnixMilli()
			if tc.users == 2 {
				evidence.Turns = []parser.TimingInterval{{Start: start, End: start + 3000}}
			} else {
				evidence.Turns = []parser.TimingInterval{{Start: start, End: start + 1000}, {Start: start + 2000, End: start + 3000}}
			}
			encoded, err := json.Marshal(evidence)
			require.NoError(t, err)
			raw, err := sql.Open("sqlite3", database.Path())
			require.NoError(t, err)
			_, err = raw.ExecContext(t.Context(), "UPDATE session_timing_evidence SET evidence=? WHERE session_id='timing'", string(encoded))
			require.NoError(t, err)
			_, err = raw.ExecContext(t.Context(), "UPDATE session_timing_checkpoints SET version=5 WHERE session_id='timing'")
			require.NoError(t, err)
			_, err = raw.ExecContext(t.Context(), "UPDATE sessions SET data_version=155 WHERE id='timing'")
			require.NoError(t, err)
			_, err = raw.ExecContext(t.Context(), "PRAGMA user_version=155")
			require.NoError(t, err)
			require.NoError(t, raw.Close())
			archivePath := database.Path()
			require.NoError(t, database.Close())
			reopened, err := db.OpenIsolated(t.Context(), archivePath)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, reopened.Close()) })
			upgraded := NewEngine(t.Context(), reopened, cfg)
			t.Cleanup(upgraded.Close)
			require.Equal(t, 1, upgraded.SyncAll(t.Context(), nil).Synced)
			after, err := reopened.GetSessionFull(t.Context(), "timing")
			require.NoError(t, err)
			require.NotNil(t, after)
			assert.Equal(t, before.ID, after.ID)
			assert.Equal(t, before.SourceSessionID, after.SourceSessionID)
			assert.Equal(t, before.FileHash, after.FileHash)
			actual, err := reopened.GetAllMessages(t.Context(), "timing")
			require.NoError(t, err)
			require.Len(t, actual, len(messages))
			for i := range messages {
				assert.Equal(t, messages[i].ID, actual[i].ID)
			}
			require.NoError(t, os.Remove(path))
			timing, err := reopened.GetSessionTiming(t.Context(), "timing")
			require.NoError(t, err)
			assert.Equal(t, "persisted", timing.Coverage.Evidence)
			assert.Len(t, timing.Activity, tc.activities)
			assert.Equal(t, int64(3000), timing.ActiveDurationMs)
		})
	}
}
