package parser

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestClaudeTimingMultipleQueuedPromptsDuringTool(t *testing.T) {
	source := strings.Join([]string{
		`{"timestamp":"2026-04-26T10:00:00Z","type":"user","sessionId":"timing","message":{"content":"first"}}`,
		`{"timestamp":"2026-04-26T10:00:01Z","type":"assistant","sessionId":"timing","message":{"id":"response","content":[{"type":"tool_use","id":"call","name":"Bash","input":{}}]}}`,
		`{"timestamp":"2026-04-26T10:00:05Z","type":"attachment","sessionId":"timing","attachment":{"type":"queued_command","prompt":"also check this"}}`,
		`{"timestamp":"2026-04-26T10:00:07Z","type":"attachment","sessionId":"timing","attachment":{"type":"queued_command","prompt":"and this"}}`,
		`{"timestamp":"2026-04-26T10:00:10Z","type":"user","sessionId":"timing","message":{"content":[{"type":"tool_result","tool_use_id":"call","content":"done"}]}}`,
		`{"timestamp":"2026-04-26T10:00:12Z","type":"assistant","sessionId":"timing","message":{"content":"done"}}`,
	}, "\n")
	sess, _ := runClaudeParserTest(t, "timing.jsonl", source)
	require.False(t, sess.SourceTimingShared)
	require.Equal(t, 3, sess.UserMessageCount)
	evidence, err := ReadSessionTiming(t.Context(), strings.NewReader(source), "claude")
	require.NoError(t, err)
	var active int64
	for _, interval := range evidence.Turns {
		active += interval.End - interval.Start
	}
	t.Logf("evidence=%+v", evidence)
	assert.Equal(t, int64(12000), active)
	assertTimingCheckpointSplitsMatch(t, "claude", source)
}

func TestClaudeTimingCompletedPromptCannotCarryOldWaitIntoQueues(t *testing.T) {
	source := strings.Join([]string{
		`{"timestamp":"2026-04-26T10:00:00Z","type":"user","sessionId":"timing","message":{"content":"first"}}`,
		`{"timestamp":"2026-04-26T10:00:01Z","type":"assistant","sessionId":"timing","message":{"content":[{"type":"tool_use","id":"old","name":"Bash","input":{}}]}}`,
		`{"timestamp":"2026-04-26T10:00:02Z","type":"assistant","sessionId":"timing","message":{"content":"done","stop_reason":"end_turn"}}`,
		`{"timestamp":"2026-04-26T10:00:30Z","type":"attachment","sessionId":"timing","attachment":{"type":"queued_command","prompt":"second"}}`,
		`{"timestamp":"2026-04-26T10:00:31Z","type":"attachment","sessionId":"timing","attachment":{"type":"queued_command","prompt":"third"}}`,
		`{"timestamp":"2026-04-26T10:00:32Z","type":"assistant","sessionId":"timing","message":{"content":"done","stop_reason":"end_turn"}}`,
	}, "\n")
	evidence, err := ReadSessionTiming(t.Context(), strings.NewReader(source), "claude")
	require.NoError(t, err)
	require.Len(t, evidence.Turns, 2)
	assert.Equal(t, int64(2000), evidence.Turns[0].End-evidence.Turns[0].Start)
	assert.Equal(t, int64(1000), evidence.Turns[1].End-evidence.Turns[1].Start)
	assert.False(t, evidence.Open)
	assertTimingCheckpointSplitsMatch(t, "claude", source)
}
