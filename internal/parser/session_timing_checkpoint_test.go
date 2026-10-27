package parser

import (
	"bytes"
	"encoding/json/v2"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTimingCheckpointEverySplitMatchesFullRead(t *testing.T) {
	for _, tc := range []struct{ agent, source string }{
		{"codex", strings.Join([]string{
			`{"type":"session_meta","payload":{"id":"timing"}}`,
			`{"timestamp":"2026-04-26T10:00:00Z","type":"event_msg","payload":{"type":"task_started","turn_id":"first"}}`,
			`{"timestamp":"2026-04-26T10:00:01Z","type":"response_item","payload":{"type":"function_call","call_id":"call"}}`,
			`{"timestamp":"2026-04-26T10:00:02Z","type":"event_msg","payload":{"type":"item_completed","item":{"type":"Reasoning"},"started_at_ms":1777197600000,"completed_at_ms":1777197602000}}`,
			`{"timestamp":"2026-04-26T10:00:03Z","type":"response_item","payload":{"type":"function_call_output","call_id":"call","output":"private-checkpoint-body-must-not-be-retained"}}`,
			`{"timestamp":"2026-04-26T11:00:00Z","type":"event_msg","payload":{"type":"task_started","turn_id":"second"}}`,
			`{"timestamp":"2026-04-26T11:00:01Z","type":"response_item","payload":{"type":"function_call","call_id":"ambiguous"}}`,
			`{"timestamp":"2026-04-26T11:00:02Z","type":"response_item","payload":{"type":"function_call","call_id":"ambiguous"}}`,
			`{"timestamp":"2026-04-26T11:00:03Z","type":"response_item","payload":{"type":"function_call_output","call_id":"ambiguous"}}`,
			`{"timestamp":"2026-04-26T11:00:04Z","type":"event_msg","payload":{"type":"turn_aborted"}}`,
		}, "\r\n")},
		{"claude", strings.Join([]string{
			`{"timestamp":"2026-04-26T10:00:00Z","type":"user","sessionId":"timing","message":{"content":"private-checkpoint-body-must-not-be-retained"}}`,
			`{"timestamp":"2026-04-26T10:00:02Z","type":"assistant","sessionId":"timing","thinkingDurationMs":1000,"message":{"content":[{"type":"tool_use","id":"call"}]}}`,
			`{"timestamp":"2026-04-26T10:00:03Z","type":"user","sessionId":"timing","message":{"content":"[Request interrupted by user]"}}`,
			`{"timestamp":"2026-04-26T10:00:30Z","type":"user","sessionId":"timing","message":{"content":[{"type":"tool_result","tool_use_id":"call","content":"private-checkpoint-body-must-not-be-retained"}]}}`,
			`{"timestamp":"2026-04-26T11:00:00Z","type":"user","sessionId":"timing","message":{"content":"next"}}`,
			`{"timestamp":"2026-04-26T11:00:01Z","type":"assistant","sessionId":"timing","message":{"content":"answer"}}`,
		}, "\n")},
	} {
		t.Run(tc.agent, func(t *testing.T) {
			assertTimingCheckpointSplitsMatch(t, tc.agent, tc.source)
		})
	}
}

func TestTimingCheckpointEmptyEvidenceRoundTrip(t *testing.T) {
	source := `{"type":"session_meta","payload":{"id":"timing"}}` + "\n" + `{"timestamp":"2026-04-26T10:00:00Z","type":"event_msg","payload":{"type":"task_started","turn_id":"turn"}}` + "\n"
	expected, state, _, err := ReadSessionTimingChunk(t.Context(), strings.NewReader(source), "codex", "", nil, nil)
	require.NoError(t, err)
	encoded, err := json.Marshal(state)
	require.NoError(t, err)
	var restored SessionTimingState
	require.NoError(t, json.Unmarshal(encoded, &restored))
	actual, _, _, err := ReadSessionTimingChunk(t.Context(), strings.NewReader(""), "codex", "", &restored, nil)
	require.NoError(t, err)
	assert.Equal(t, expected, actual)
}

func assertTimingCheckpointSplitsMatch(t *testing.T, agent, source string) {
	t.Helper()
	expected, err := ReadSessionTiming(t.Context(), strings.NewReader(source), agent)
	require.NoError(t, err)
	// Every byte split covers complete records, CRLF, partial JSON and valid
	// JSON without a final newline. Restart through the actual JSON codec.
	for split := 0; split <= len(source); split++ {
		prefix := source[:split]
		_, state, offset, err := ReadSessionTimingChunk(t.Context(), strings.NewReader(prefix), agent, "", nil, nil)
		require.NoError(t, err)
		require.Equal(t, int64(strings.LastIndexByte(prefix, '\n')+1), offset)
		encoded, err := json.Marshal(state)
		require.NoError(t, err)
		assert.NotContains(t, string(encoded), "private-checkpoint-body-must-not-be-retained")
		var restart SessionTimingState
		require.NoError(t, json.Unmarshal(encoded, &restart))
		var observed bytes.Buffer
		actual, _, _, err := ReadSessionTimingChunk(t.Context(), strings.NewReader(source[offset:]), agent, "", &restart, func(b []byte, _ bool) error { _, err := observed.Write(b); return err })
		require.NoError(t, err)
		require.Equal(t, expected, actual, "split=%d", split)
		require.Equal(t, source[offset:], observed.String())
	}
}

func TestTimingCheckpointClaudePromptAndSnapshotParity(t *testing.T) {
	for _, tc := range []struct {
		name, source         string
		wantTurns, wantCalls int
	}{
		{
			name: "queued prompts and compact summaries",
			source: strings.Join([]string{
				`{"timestamp":"2026-04-26T10:00:00Z","type":"user","sessionId":"timing","message":{"content":"private-checkpoint-body-must-not-be-retained"}}`,
				`{"timestamp":"2026-04-26T10:00:02Z","type":"assistant","sessionId":"timing","message":{"content":"done"}}`,
				`{"timestamp":"2026-04-26T11:00:00Z","type":"attachment","sessionId":"timing","attachment":{"type":"queued_command","prompt":"private-checkpoint-body-must-not-be-retained"}}`,
				`{"timestamp":"2026-04-26T11:00:01Z","type":"assistant","sessionId":"timing","message":{"content":"working"}}`,
				`{"timestamp":"2026-04-26T11:00:02Z","type":"user","isCompactSummary":true,"sessionId":"timing","message":{"content":"private-checkpoint-body-must-not-be-retained"}}`,
				`{"timestamp":"2026-04-26T11:00:03Z","type":"assistant","isCompactSummary":true,"sessionId":"timing","message":{"content":"private-checkpoint-body-must-not-be-retained"}}`,
				`{"timestamp":"2026-04-26T11:00:04Z","type":"assistant","sessionId":"timing","message":{"content":"done"}}`,
				`{"timestamp":"2026-04-26T12:00:00Z","type":"attachment","sessionId":"timing","attachment":{"type":"queued_command","prompt":"<task-notification>private-checkpoint-body-must-not-be-retained</task-notification>"}}`,
				`{"timestamp":"2026-04-26T13:00:00Z","type":"attachment","sessionId":"timing","attachment":{"type":"queued_command","prompt":"next request"}}`,
			}, "\n"),
			wantTurns: 3,
		},
		{
			name: "same response snapshots and genuinely reused calls",
			source: strings.Join([]string{
				`{"timestamp":"2026-04-26T10:00:00Z","type":"user","sessionId":"timing","message":{"content":"private-checkpoint-body-must-not-be-retained"}}`,
				`{"timestamp":"2026-04-26T10:00:01Z","type":"assistant","sessionId":"timing","message":{"id":"response","content":[{"type":"tool_use","id":"call","name":"Read","input":{"path":"private-checkpoint-body-must-not-be-retained"}}]}}`,
				`{"timestamp":"2026-04-26T10:00:01.5Z","type":"system","sessionId":"timing","content":"private-checkpoint-body-must-not-be-retained"}`,
				`{"timestamp":"2026-04-26T10:00:02Z","type":"assistant","sessionId":"timing","message":{"id":"response","content":[{"type":"tool_use","id":"call","name":"Read","input":{"path":"private-checkpoint-body-must-not-be-retained"}},{"type":"text","text":"private-checkpoint-body-must-not-be-retained"}]}}`,
				`{"timestamp":"2026-04-26T10:00:03Z","type":"user","sessionId":"timing","message":{"content":[{"type":"tool_result","tool_use_id":"call","content":"private-checkpoint-body-must-not-be-retained"}]}}`,
				`{"timestamp":"2026-04-26T10:00:04Z","type":"assistant","sessionId":"timing","message":{"id":"response","content":[{"type":"tool_use","id":"call","name":"Read","input":{}}]}}`,
				`{"timestamp":"2026-04-26T10:00:05Z","type":"user","sessionId":"timing","message":{"content":[{"type":"tool_result","tool_use_id":"call","content":"private-checkpoint-body-must-not-be-retained"}]}}`,
				`{"timestamp":"2026-04-26T10:00:06Z","type":"assistant","sessionId":"timing","message":{"id":"other-one","content":[{"type":"tool_use","id":"reused","name":"Read","input":{}}]}}`,
				`{"timestamp":"2026-04-26T10:00:06.5Z","type":"user","isMeta":true,"sessionId":"timing","message":{"content":"private-checkpoint-body-must-not-be-retained"}}`,
				`{"timestamp":"2026-04-26T10:00:07Z","type":"assistant","sessionId":"timing","message":{"id":"other-one","content":[{"type":"tool_use","id":"reused","name":"Read","input":{}}]}}`,
				`{"timestamp":"2026-04-26T10:00:08Z","type":"user","sessionId":"timing","message":{"content":[{"type":"tool_result","tool_use_id":"reused","content":"private-checkpoint-body-must-not-be-retained"}]}}`,
			}, "\n"),
			wantTurns: 1, wantCalls: 2,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			evidence, err := ReadSessionTiming(t.Context(), strings.NewReader(tc.source), "claude")
			require.NoError(t, err)
			require.Len(t, evidence.Turns, tc.wantTurns)
			require.Len(t, evidence.Calls, tc.wantCalls)
			assertTimingCheckpointSplitsMatch(t, "claude", tc.source)
		})
	}
}

func TestTimingCheckpointBoundsClaudeSnapshotIdentifiers(t *testing.T) {
	oversized := strings.Repeat("x", maxTimingClaudeMessageBytes+1)
	state := &SessionTimingState{ClaudeMessageID: oversized, ClaudeMessageBytes: len(oversized)}
	assert.False(t, state.Valid(), "oversized decoded cursor IDs must be rejected before resuming")
	source := `{"timestamp":"2026-04-26T10:00:00Z","type":"assistant","message":{"id":"` + oversized + `","content":[{"type":"tool_use","id":"call"}]}}`
	_, _, _, err := ReadSessionTimingChunk(t.Context(), strings.NewReader(source), "claude", "", nil, nil)
	assert.ErrorIs(t, err, ErrTimingSourceUnsupported, "a cold scan must enforce the same metadata bound")
}
