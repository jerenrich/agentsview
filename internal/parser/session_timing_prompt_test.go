package parser

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClaudeTimingCanonicalUserPromptBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name, content string
		users         int
	}{
		{"mixed prompt and result", `[{"type":"tool_result","tool_use_id":"call","content":"done"},{"type":"text","text":"Also check the tests"}]`, 2},
		{"IDE skipped command", `"<ide_selection>package main</ide_selection>\n<command-message></command-message>"`, 1},
		{"IDE local command noise", `"<ide_opened_file>main.go</ide_opened_file>\n<local-command-stdout>output</local-command-stdout>"`, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := strings.Join([]string{
				`{"timestamp":"2026-04-26T10:00:00Z","type":"user","sessionId":"timing","message":{"content":"first"}}`,
				`{"timestamp":"2026-04-26T10:00:01Z","type":"assistant","sessionId":"timing","message":{"content":[{"type":"tool_use","id":"call","name":"Bash","input":{}}]}}`,
				`{"timestamp":"2026-04-26T10:00:02Z","type":"user","sessionId":"timing","message":{"content":` + tc.content + `}}`,
				`{"timestamp":"2026-04-26T10:00:03Z","type":"assistant","sessionId":"timing","message":{"content":"done","stop_reason":"end_turn"}}`,
			}, "\n") + "\n"
			sess, _ := runClaudeParserTest(t, "timing.jsonl", source)
			require.False(t, sess.SourceTimingShared)
			require.Equal(t, tc.users, sess.UserMessageCount)
			evidence, err := ReadSessionTiming(t.Context(), strings.NewReader(source), "claude")
			require.NoError(t, err)
			require.Len(t, evidence.Turns, tc.users)
			assert.False(t, evidence.Open)
			var active int64
			for _, interval := range evidence.Turns {
				active += interval.End - interval.Start
			}
			assert.Equal(t, int64(3000), active)
			if tc.users == 2 {
				assert.Equal(t, int64(2000), evidence.Turns[0].End-evidence.Turns[0].Start)
				require.Len(t, evidence.Calls, 1)
				assert.Equal(t, evidence.Turns[0].End, evidence.Calls[0].End)
			}
			assertTimingCheckpointSplitsMatch(t, "claude", source)
		})
	}
}
