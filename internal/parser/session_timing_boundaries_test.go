package parser

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestClaudeTimingQueuedPromptPreservesToolWait(t *testing.T) {
	source := strings.Join([]string{
		`{"timestamp":"2026-04-26T10:00:00Z","type":"user","sessionId":"timing","message":{"content":"first"}}`,
		`{"timestamp":"2026-04-26T10:00:01Z","type":"assistant","sessionId":"timing","message":{"id":"response","content":[{"type":"tool_use","id":"call","name":"Bash","input":{}}]}}`,
		`{"timestamp":"2026-04-26T10:00:05Z","type":"attachment","sessionId":"timing","attachment":{"type":"queued_command","prompt":"also check this"}}`,
		`{"timestamp":"2026-04-26T10:00:10Z","type":"user","sessionId":"timing","message":{"content":[{"type":"tool_result","tool_use_id":"call","content":"done"}]}}`,
		`{"timestamp":"2026-04-26T10:00:12Z","type":"assistant","sessionId":"timing","message":{"content":"done"}}`,
	}, "\n")
	sess, messages := runClaudeParserTest(t, "timing.jsonl", source)
	require.False(t, sess.SourceTimingShared)
	require.Equal(t, 2, sess.UserMessageCount)
	require.Len(t, messages, 5)
	evidence, err := ReadSessionTiming(t.Context(), strings.NewReader(source), "claude")
	require.NoError(t, err)
	require.Len(t, evidence.Calls, 1)
	require.Len(t, evidence.Turns, 2)
	var active int64
	for _, interval := range evidence.Turns {
		active += interval.End - interval.Start
	}
	t.Logf("evidence=%+v; active=%d; wait=%d", evidence, active, evidence.Calls[0].End-evidence.Calls[0].Start)
	assert.Equal(t, int64(12000), active, "a queued prompt cannot create idle time while an observed tool call is waiting")
	assertTimingCheckpointSplitsMatch(t, "claude", source)
}

func TestClaudeTimingIgnoredUserNoiseDoesNotSplitTurn(t *testing.T) {
	for _, content := range []string{`<local-command-stdout>foo</local-command-stdout>`, `   `} {
		t.Run(content, func(t *testing.T) {
			source := strings.Join([]string{
				`{"timestamp":"2026-04-26T10:00:00Z","type":"user","sessionId":"timing","message":{"content":"request"}}`,
				`{"timestamp":"2026-04-26T10:00:01Z","type":"assistant","sessionId":"timing","message":{"content":"working"}}`,
				`{"timestamp":"2026-04-26T10:00:02Z","type":"user","sessionId":"timing","message":{"content":"` + content + `"}}`,
				`{"timestamp":"2026-04-26T10:00:03Z","type":"assistant","sessionId":"timing","message":{"content":"done"}}`,
			}, "\n")
			sess, messages := runClaudeParserTest(t, "timing.jsonl", source)
			require.False(t, sess.SourceTimingShared)
			require.Equal(t, 1, sess.UserMessageCount)
			require.Len(t, messages, 3)
			evidence, err := ReadSessionTiming(t.Context(), strings.NewReader(source), "claude")
			require.NoError(t, err)
			t.Logf("evidence=%+v", evidence)
			assert.Len(t, evidence.Turns, 1, "noise ignored by normal ingestion must not create a timing prompt")
			assertTimingCheckpointSplitsMatch(t, "claude", source)
		})
	}
}

func TestClaudeTimingInterruptionClosesTurn(t *testing.T) {
	for _, marker := range []string{"[Request interrupted by user]", "[Request interrupted by user for tool use]"} {
		t.Run(marker, func(t *testing.T) {
			source := strings.Join([]string{
				`{"timestamp":"2026-04-26T10:00:00Z","type":"user","sessionId":"timing","message":{"content":"request"}}`,
				`{"timestamp":"2026-04-26T10:00:01Z","type":"assistant","sessionId":"timing","message":{"content":[{"type":"tool_use","id":"cancelled","name":"Bash","input":{}}]}}`,
				`{"timestamp":"2026-04-26T10:00:02Z","type":"user","sessionId":"timing","message":{"content":"` + marker + `"}}`,
			}, "\n") + "\n"
			evidence, err := ReadSessionTiming(t.Context(), strings.NewReader(source), "claude")
			require.NoError(t, err)
			require.Len(t, evidence.Turns, 1)
			assert.False(t, evidence.Open)
			assert.Equal(t, int64(2000), evidence.Turns[0].End-evidence.Turns[0].Start)
			assertTimingCheckpointSplitsMatch(t, "claude", source)
			source += `{"timestamp":"2026-04-26T10:00:03Z","type":"user","sessionId":"timing","message":{"content":[{"type":"tool_result","tool_use_id":"cancelled","is_error":true,"content":"Tool execution interrupted by user"}]}}` + "\n"
			acknowledged, err := ReadSessionTiming(t.Context(), strings.NewReader(source), "claude")
			require.NoError(t, err)
			assert.Equal(t, evidence, acknowledged, "a cancellation acknowledgement cannot reopen the interrupted turn")
			assertTimingCheckpointSplitsMatch(t, "claude", source)
			// A later prompt starts fresh; the cancelled call cannot bridge the idle gap.
			source += `{"timestamp":"2026-04-26T10:00:30Z","type":"user","sessionId":"timing","message":{"content":"new request"}}` + "\n" +
				`{"timestamp":"2026-04-26T10:00:31Z","type":"attachment","sessionId":"timing","attachment":{"type":"queued_command","prompt":"another request"}}` + "\n" +
				`{"timestamp":"2026-04-26T10:00:32Z","type":"assistant","sessionId":"timing","message":{"content":"done","stop_reason":"end_turn"}}` + "\n"
			evidence, err = ReadSessionTiming(t.Context(), strings.NewReader(source), "claude")
			require.NoError(t, err)
			require.Len(t, evidence.Turns, 2)
			assert.Equal(t, int64(1000), evidence.Turns[1].End-evidence.Turns[1].Start)
			assert.False(t, evidence.Open)
			assertTimingCheckpointSplitsMatch(t, "claude", source)
		})
	}
}
