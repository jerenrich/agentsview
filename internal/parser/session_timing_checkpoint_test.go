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
			expected, err := ReadSessionTiming(t.Context(), strings.NewReader(tc.source), tc.agent)
			require.NoError(t, err)
			// Every byte split covers complete records, CRLF, partial JSON and valid
			// JSON without a final newline. Restart through the actual JSON codec.
			for split := 0; split <= len(tc.source); split++ {
				prefix := tc.source[:split]
				_, state, offset, err := ReadSessionTimingChunk(t.Context(), strings.NewReader(prefix), tc.agent, "", nil, nil)
				require.NoError(t, err)
				require.Equal(t, int64(strings.LastIndexByte(prefix, '\n')+1), offset)
				encoded, err := json.Marshal(state)
				require.NoError(t, err)
				assert.NotContains(t, string(encoded), "private-checkpoint-body-must-not-be-retained")
				var restart SessionTimingState
				require.NoError(t, json.Unmarshal(encoded, &restart))
				var observed bytes.Buffer
				actual, _, _, err := ReadSessionTimingChunk(t.Context(), strings.NewReader(tc.source[offset:]), tc.agent, "", &restart, func(b []byte, _ bool) error { _, err := observed.Write(b); return err })
				require.NoError(t, err)
				require.Equal(t, expected, actual, "split=%d", split)
				require.Equal(t, tc.source[offset:], observed.String())
			}
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
