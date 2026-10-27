package parser

import (
	"context"
	"runtime"
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

func TestReadSessionTimingDoesNotCarryOpenTurnAcrossIdleGap(t *testing.T) {
	source := strings.Join([]string{
		`{"timestamp":"2026-04-26T10:00:00Z","type":"event_msg","payload":{"type":"task_started","turn_id":"abandoned"}}`,
		`{"timestamp":"2026-04-26T10:00:03Z","type":"response_item","payload":{"type":"message","role":"assistant"}}`,
		`{"timestamp":"2026-04-26T11:00:00Z","type":"turn_context","payload":{}}`,
		`{"timestamp":"2026-04-26T11:00:00Z","type":"event_msg","payload":{"type":"user_message"}}`,
		`{"timestamp":"2026-04-26T11:00:00Z","type":"event_msg","payload":{"type":"task_started","turn_id":"next"}}`,
		`{"timestamp":"2026-04-26T11:00:02Z","type":"event_msg","payload":{"type":"task_complete","turn_id":"next"}}`,
	}, "\n")
	got, err := ReadSessionTiming(t.Context(), strings.NewReader(source), "codex")
	require.NoError(t, err)
	require.Len(t, got.Turns, 2)
	var duration int64
	for _, span := range got.Turns {
		duration += span.End - span.Start
	}
	assert.Equal(t, int64(5000), duration)
}

func TestReadSessionTimingClaudeHiddenMetaDoesNotSplitTurn(t *testing.T) {
	for _, channel := range []bool{false, true} {
		origin := ""
		if channel {
			origin = `,"origin":{"kind":"channel"}`
		}
		source := strings.Join([]string{
			`{"timestamp":"2026-04-26T10:00:00Z","type":"user","sessionId":"timing","message":{"content":"go"}}`,
			`{"timestamp":"2026-04-26T10:00:02Z","type":"assistant","sessionId":"timing","message":{"content":"working"}}`,
			`{"timestamp":"2026-04-26T10:00:03Z","type":"user","sessionId":"timing","isMeta":true` + origin + `,"message":{"content":"context"}}`,
			`{"timestamp":"2026-04-26T10:00:05Z","type":"assistant","sessionId":"timing","message":{"content":"done"}}`,
		}, "\n")
		got, err := ReadSessionTiming(t.Context(), strings.NewReader(source), "claude")
		require.NoError(t, err)
		if channel {
			require.Len(t, got.Turns, 2)
		} else {
			require.Len(t, got.Turns, 1)
			assert.Equal(t, int64(5000), got.Turns[0].End-got.Turns[0].Start)
		}
	}
}

func TestReadSessionTimingOverlappingReusedCallIDIsAmbiguous(t *testing.T) {
	source := strings.Join([]string{
		`{"timestamp":"2026-04-26T10:00:01Z","type":"response_item","payload":{"type":"function_call","call_id":"reused"}}`,
		`{"timestamp":"2026-04-26T10:00:02Z","type":"response_item","payload":{"type":"function_call","call_id":"reused"}}`,
		`{"timestamp":"2026-04-26T10:00:03Z","type":"response_item","payload":{"type":"function_call_output","call_id":"reused"}}`,
	}, "\n")
	got, err := ReadSessionTiming(t.Context(), strings.NewReader(source), "codex")
	require.NoError(t, err)
	assert.Empty(t, got.Calls, "a result cannot identify which overlapping request finished")
}

func TestReadSessionTimingAbortWithoutIDClosesOnlyActiveTurn(t *testing.T) {
	source := `{"timestamp":"2026-04-26T10:00:00Z","type":"event_msg","payload":{"type":"task_started","turn_id":"a"}}` + "\n" +
		`{"timestamp":"2026-04-26T10:00:02Z","type":"event_msg","payload":{"type":"turn_aborted"}}` + "\n" +
		`{"timestamp":"2026-04-26T11:00:00Z","type":"turn_context","payload":{}}`
	got, err := ReadSessionTiming(t.Context(), strings.NewReader(source), "codex")
	require.NoError(t, err)
	require.Len(t, got.Turns, 1)
	assert.Equal(t, int64(2000), got.Turns[0].End-got.Turns[0].Start)
}

func TestReadSessionTimingIdentityDoesNotRequireTimestamp(t *testing.T) {
	got, err := ReadSessionTiming(t.Context(), strings.NewReader(`{"type":"session_meta","payload":{"id":"one"}}`), "codex")
	require.NoError(t, err)
	assert.Equal(t, "one", got.SourceID)
	_, err = ReadSessionTiming(t.Context(), strings.NewReader(`{"type":"session_meta","payload":{"id":"one"}}`+"\n"+`{"type":"session_meta","payload":{"id":"two"}}`), "codex")
	assert.ErrorIs(t, err, ErrTimingSourceUnsupported)
}

func TestReadSessionTimingDoesNotRetainLargeToolBodies(t *testing.T) {
	// A tiny gjson substring can otherwise pin the entire output JSON. Allow
	// generous heap noise while checking that one 16 MiB body does not stay live.
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	evidence := func() *SessionTimingEvidence {
		source := `{"timestamp":"2026-04-26T10:00:00Z","type":"response_item","payload":{"type":"function_call","call_id":"small-id"}}` + "\n" +
			`{"timestamp":"2026-04-26T10:00:01Z","type":"response_item","payload":{"type":"function_call_output","call_id":"small-id","output":"` + strings.Repeat("x", 16<<20) + `"}}`
		got, err := ReadSessionTiming(t.Context(), strings.NewReader(source), "codex")
		require.NoError(t, err)
		require.Len(t, got.Calls, 1)
		assert.Equal(t, "small-id", got.Calls[0].ID)
		return got
	}()
	runtime.GC()
	runtime.ReadMemStats(&after)
	runtime.KeepAlive(evidence)
	assert.Less(t, int64(after.HeapAlloc)-int64(before.HeapAlloc), int64(8<<20), "timing identifiers must not retain transcript bodies")
}

func TestReadSessionTimingContinuationIdentity(t *testing.T) {
	page := revertThread + "_" + revertRollout
	path := "rollout-2026-09-01T10-00-00-" + page + ".jsonl"
	for _, tc := range []struct{ name, header, session, want string }{
		{"modern", revertThread, "", page},
		{"older", revertRollout, revertThread, page},
		{"wrong thread", revertOther, "", revertOther},
		{"wrong older thread", revertRollout, revertOther, revertRollout},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := revertPageMeta(tc.header, "", sessionIDField(tc.session)) + "\n"
			evidence, err := ReadSessionTiming(t.Context(), strings.NewReader(source), "codex", path)
			require.NoError(t, err)
			assert.Equal(t, tc.want, evidence.SourceID)
		})
	}
}
