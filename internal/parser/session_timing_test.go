package parser

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadSessionTimingCodex(t *testing.T) {
	source := strings.Join([]string{
		`{"timestamp":"2026-04-26T10:00:00Z","type":"session_meta","payload":{"id":"timing"}}`,
		`{"timestamp":"2026-04-26T10:00:00Z","type":"event_msg","payload":{"type":"task_started","turn_id":"turn"}}`,
		`{"timestamp":"2026-04-26T10:00:01Z","type":"response_item","payload":{"type":"custom_tool_call","call_id":"call"}}`,
		`{"timestamp":"2026-04-26T10:00:03Z","type":"response_item","payload":{"type":"custom_tool_call_output","call_id":"call","output":"private output"}}`,
		`{"timestamp":"2026-04-26T10:00:04Z","type":"event_msg","payload":{"type":"item_completed","item":{"type":"Reasoning"},"started_at_ms":1777197600000,"completed_at_ms":1777197604000}}`,
		`{"timestamp":"2026-04-26T10:00:05Z","type":"event_msg","payload":{"type":"task_complete","turn_id":"turn"}}`,
		`{"timestamp":"2026-04-26T11:00:00Z","type":"event_msg","payload":{"type":"task_started","turn_id":"interrupted"}}`,
		`{"timestamp":"2026-04-26T11:00:02Z","type":"event_msg","payload":{"type":"turn_aborted","turn_id":"interrupted"}}`,
	}, "\n")
	evidence, err := ReadSessionTiming(t.Context(), strings.NewReader(source), "codex")
	require.NoError(t, err)
	assert.Equal(t, "timing", evidence.SourceID)
	require.Len(t, evidence.Turns, 2)
	assert.Equal(t, int64(5000), evidence.Turns[0].End-evidence.Turns[0].Start)
	assert.Equal(t, int64(2000), evidence.Turns[1].End-evidence.Turns[1].Start)
	require.Len(t, evidence.Model, 1)
	assert.Equal(t, int64(4000), evidence.Model[0].End-evidence.Model[0].Start)
	require.Len(t, evidence.Calls, 1)
	assert.Equal(t, "call", evidence.Calls[0].ID)
	assert.Equal(t, int64(2000), evidence.Calls[0].End-evidence.Calls[0].Start)
}

func TestReadSessionTimingClaude(t *testing.T) {
	source := strings.Join([]string{
		`{"timestamp":"2026-04-26T10:00:00Z","type":"user","sessionId":"timing","message":{"content":"go"}}`,
		`{"timestamp":"2026-04-26T10:00:02Z","type":"assistant","sessionId":"timing","thinkingDurationMs":1000,"message":{"content":[{"type":"tool_use","id":"call","name":"Bash"}]}}`,
		`{"timestamp":"2026-04-26T10:00:03Z","type":"user","sessionId":"timing","message":{"content":[{"type":"tool_result","tool_use_id":"call","content":"private result"}]}}`,
		`{"timestamp":"2026-04-26T10:00:05Z","type":"assistant","sessionId":"timing","message":{"content":[{"type":"text","text":"done"}]}}`,
		`{"timestamp":"2026-04-26T11:00:00Z","type":"user","sessionId":"timing","message":{"content":"next"}}`,
		`{"timestamp":"2026-04-26T11:00:02Z","type":"assistant","sessionId":"timing","message":{"content":"answer"}}`,
	}, "\n")
	evidence, err := ReadSessionTiming(t.Context(), strings.NewReader(source), "claude")
	require.NoError(t, err)
	require.Len(t, evidence.Turns, 2)
	assert.Equal(t, int64(5000), evidence.Turns[0].End-evidence.Turns[0].Start)
	assert.Equal(t, int64(2000), evidence.Turns[1].End-evidence.Turns[1].Start)
	require.Len(t, evidence.Model, 1)
	assert.Equal(t, int64(1000), evidence.Model[0].End-evidence.Model[0].Start)
	require.Len(t, evidence.Calls, 1)
	assert.Equal(t, int64(1000), evidence.Calls[0].End-evidence.Calls[0].Start)
}

func TestReadSessionTimingRejectsReplayAndCancellation(t *testing.T) {
	for _, metadata := range []string{
		`{"forked_from_id":"parent"}`,
		`{"source":{"subagent":"worker"}}`,
	} {
		source := `{"timestamp":"2026-04-26T10:00:00Z","type":"session_meta","payload":` + metadata + `}`
		_, err := ReadSessionTiming(t.Context(), strings.NewReader(source), "codex")
		assert.ErrorIs(t, err, ErrTimingSourceUnsupported)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := ReadSessionTiming(ctx, strings.NewReader(""), "codex")
	assert.ErrorIs(t, err, context.Canceled)
}

func TestReadSessionTimingRejectsInvalidModelEndpoints(t *testing.T) {
	for _, endpoints := range []string{
		`"started_at_ms":1.5,"completed_at_ms":2`,
		`"started_at_ms":"1","completed_at_ms":2`,
		`"started_at_ms":2,"completed_at_ms":1`,
		`"started_at_ms":1,"completed_at_ms":9223372036854775807`,
	} {
		source := `{"timestamp":"2026-04-26T10:00:00Z","type":"event_msg","payload":{"type":"item_completed","item":{"type":"Reasoning"},` + endpoints + `}}`
		got, err := ReadSessionTiming(t.Context(), strings.NewReader(source), "codex")
		require.NoError(t, err)
		assert.Empty(t, got.Model)
	}
}
