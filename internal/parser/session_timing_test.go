package parser

import (
	"context"
	"encoding/json/v2"
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

func TestReadSessionTimingClaudeToolResultBoundsInterruptedTurn(t *testing.T) {
	prefix := []string{
		`{"timestamp":"2026-04-26T10:00:00Z","type":"user","sessionId":"timing","message":{"content":"go"}}`,
		`{"timestamp":"2026-04-26T10:00:02Z","type":"assistant","sessionId":"timing","message":{"content":[{"type":"tool_use","id":"call","name":"Bash"}]}}`,
	}
	result := `{"timestamp":"2026-04-26T10:00:30Z","type":"user","sessionId":"timing","message":{"content":[{"type":"tool_result","tool_use_id":"call","content":"done"}]}}`
	next := `{"timestamp":"2026-04-26T11:00:00Z","type":"user","sessionId":"timing","message":{"content":"next"}}`
	earlierAssistant := `{"timestamp":"2026-04-26T10:00:20Z","type":"assistant","sessionId":"timing","message":{"content":"working"}}`
	for _, tc := range []struct {
		name       string
		tail       []string
		duration   int64
		callCount  int
		nextPrompt bool
	}{
		{"completed at EOF", []string{result}, 30000, 1, false},
		{"completed before next prompt", []string{result, next}, 30000, 1, true},
		{"pending at EOF", nil, 2000, 0, false},
		{"pending before next prompt", []string{next}, 2000, 0, true},
		{"later assistant has earlier timestamp", []string{result, earlierAssistant}, 30000, 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			lines := append(append([]string(nil), prefix...), tc.tail...)
			evidence, err := ReadSessionTiming(t.Context(), strings.NewReader(strings.Join(lines, "\n")), "claude")
			require.NoError(t, err)
			assert.True(t, evidence.Open, "the final Claude prompt has no recorded completion")
			if tc.nextPrompt {
				// The unanswered prompt is an open turn that has not used time yet.
				require.Len(t, evidence.Turns, 2)
				assert.Equal(t, TimingInterval{1777201200000, 1777201200000}, evidence.Turns[1])
			} else {
				require.Len(t, evidence.Turns, 1)
			}
			assert.Equal(t, tc.duration, evidence.Turns[0].End-evidence.Turns[0].Start)
			require.Len(t, evidence.Calls, tc.callCount)
			if tc.callCount > 0 {
				assert.Equal(t, int64(28000), evidence.Calls[0].End-evidence.Calls[0].Start)
				assert.LessOrEqual(t, evidence.Calls[0].End, evidence.Turns[0].End, "observed waiting must fit inside the active turn")
			}
		})
	}
}

func TestReadSessionTimingClaudeSystemUserRecordsDoNotSplitTurn(t *testing.T) {
	for _, tc := range []struct {
		name     string
		text     string
		isPrompt bool
	}{
		{"interruption", "[Request interrupted by user]", false},
		{"IDE context", "<ide_opened_file>The user opened README.md.</ide_opened_file>", false},
		{"IDE selection", "<ide_selection>package main</ide_selection>", false},
		{"task notification", "<task-notification>done</task-notification>", false},
		{"continuation", "This session is being continued from a previous conversation.", false},
		{"resume", "<local-command-caveat>output</local-command-caveat>", false},
		{"IDE with real prompt", "<ide_opened_file>README.md</ide_opened_file>\nExplain this file", true},
		{"system reminder with real prompt", "<system-reminder>context</system-reminder>\nDo this next", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, array := range []bool{false, true} {
				var content any = tc.text
				if array {
					content = []map[string]string{{"type": "text", "text": tc.text}}
				}
				encoded, err := json.Marshal(content)
				require.NoError(t, err)
				source := strings.Join([]string{
					`{"timestamp":"2026-04-26T10:00:00Z","type":"user","sessionId":"timing","message":{"content":"go"}}`,
					`{"timestamp":"2026-04-26T10:00:02Z","type":"assistant","sessionId":"timing","message":{"content":"working"}}`,
					`{"timestamp":"2026-04-26T10:00:03Z","type":"user","sessionId":"timing","message":{"content":` + string(encoded) + `}}`,
					`{"timestamp":"2026-04-26T10:00:05Z","type":"assistant","sessionId":"timing","message":{"content":"done"}}`,
				}, "\n")
				evidence, err := ReadSessionTiming(t.Context(), strings.NewReader(source), "claude")
				require.NoError(t, err)
				if tc.isPrompt {
					require.Len(t, evidence.Turns, 2, "array=%t", array)
				} else {
					require.Len(t, evidence.Turns, 1, "array=%t", array)
					assert.Equal(t, int64(5000), evidence.Turns[0].End-evidence.Turns[0].Start)
				}
			}
		})
	}
}

func TestReadSessionTimingRejectsReplayAndCancellation(t *testing.T) {
	for _, metadata := range []string{
		`{"forked_from_id":"parent"}`,
		`{"source":{"subagent":"worker"}}`,
	} {
		source := `{"timestamp":"2026-04-26T10:00:00Z","type":"session_meta","payload":` + metadata + `}`
		_, err := ReadSessionTiming(t.Context(), strings.NewReader(source), "codex")
		require.ErrorIs(t, err, ErrTimingSourceUnsupported)
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

func TestReadSessionTimingSkipsOversizedRecordsLikeIngestion(t *testing.T) {
	lines := []string{
		`{"timestamp":"2026-04-26T10:00:00Z","type":"session_meta","payload":{"id":"oversized"}}`,
		`{"timestamp":"2026-04-26T10:00:00Z","type":"event_msg","payload":{"type":"task_started","turn_id":"turn"}}`,
		`{"timestamp":"2026-04-26T10:00:01Z","type":"response_item","payload":{"type":"function_call","call_id":"call"}}`,
		`{"timestamp":"2026-04-26T10:00:03Z","type":"response_item","payload":{"type":"function_call_output","call_id":"call"}}`,
		`{"timestamp":"2026-04-26T10:00:05Z","type":"event_msg","payload":{"type":"task_complete","turn_id":"turn"}}`,
	}
	without := strings.Join(lines, "\n") + "\n"
	want, err := ReadSessionTiming(t.Context(), strings.NewReader(without), "codex")
	require.NoError(t, err)
	huge := `{"timestamp":"2026-04-26T10:00:02Z","type":"response_item","payload":{"type":"message","role":"assistant","content":"` +
		strings.Repeat("x", maxLineSize) + `"}}`
	for _, tc := range []struct {
		name   string
		source string
	}{
		{"terminated", strings.Join(append(lines[:3:3], append([]string{huge}, lines[3:]...)...), "\n") + "\n"},
		{"unterminated at EOF", without + huge},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var observed int
			var pieces []bool
			got, state, complete, err := ReadSessionTimingChunk(t.Context(), strings.NewReader(tc.source), "codex", "", nil, func(p []byte, done bool) error {
				require.Equal(t, tc.source[observed:observed+len(p)], string(p), "bytes must be observed in source order")
				observed += len(p)
				pieces = append(pieces, done)
				return nil
			})
			require.NoError(t, err, "an oversized record must not discard the session's timing")
			assert.Equal(t, len(tc.source), observed)
			assert.Equal(t, want, got)
			require.NotNil(t, state)
			if tc.name == "terminated" {
				assert.Equal(t, int64(len(tc.source)), complete)
			} else {
				assert.Equal(t, int64(len(without)), complete, "the unterminated record is reread after resume")
				assert.False(t, pieces[len(pieces)-1])
			}
		})
	}
}

func TestReadSessionTimingClaudeQueuedCommands(t *testing.T) {
	for _, tc := range []struct {
		name, prompt string
		human        bool
	}{
		{"human prompt", "next request", true},
		{"command prompt", "<command-name>/review</command-name>", true},
		{"system reminder with prompt", "<system-reminder>context</system-reminder>\nnext request", true},
		{"peer message", "<cross-session-message sender=\"peer\">context</cross-session-message>", false},
		{"notification", "<task-notification>done</task-notification>", false},
		{"empty prompt", "  ", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			prompt, err := json.Marshal(tc.prompt)
			require.NoError(t, err)
			source := strings.Join([]string{
				`{"timestamp":"2026-04-26T10:00:00Z","type":"user","sessionId":"timing","message":{"content":"first request"}}`,
				`{"timestamp":"2026-04-26T10:00:02Z","type":"assistant","sessionId":"timing","message":{"content":"done"}}`,
				`{"timestamp":"2026-04-26T11:00:00Z","type":"attachment","sessionId":"timing","attachment":{"type":"queued_command","commandMode":"prompt","prompt":` + string(prompt) + `}}`,
				`{"timestamp":"2026-04-26T11:00:03Z","type":"assistant","sessionId":"timing","message":{"content":"done"}}`,
			}, "\n")
			sess, _ := runClaudeParserTest(t, "timing.jsonl", source)
			evidence, err := ReadSessionTiming(t.Context(), strings.NewReader(source), "claude")
			require.NoError(t, err)
			assert.False(t, sess.SourceTimingShared)
			if tc.human {
				assert.Equal(t, 2, sess.UserMessageCount)
				require.Len(t, evidence.Turns, 2)
				assert.Equal(t, int64(2000), evidence.Turns[0].End-evidence.Turns[0].Start)
				assert.Equal(t, int64(3000), evidence.Turns[1].End-evidence.Turns[1].Start)
			} else {
				assert.Equal(t, 1, sess.UserMessageCount)
				require.Len(t, evidence.Turns, 1)
			}
		})
	}

	t.Run("unanswered queued prompt remains open", func(t *testing.T) {
		source := `{"timestamp":"2026-04-26T10:00:00Z","type":"attachment","sessionId":"timing","attachment":{"type":"queued_command","prompt":"first request"}}`
		evidence, err := ReadSessionTiming(t.Context(), strings.NewReader(source), "claude")
		require.NoError(t, err)
		require.Len(t, evidence.Turns, 1)
		assert.Equal(t, evidence.Turns[0].Start, evidence.Turns[0].End)
		assert.True(t, evidence.Open)
	})
}

func TestReadSessionTimingClaudeCompactSummariesDoNotSplitTurn(t *testing.T) {
	for _, kind := range []string{"user", "assistant"} {
		t.Run(kind, func(t *testing.T) {
			source := strings.Join([]string{
				`{"timestamp":"2026-04-26T10:00:00Z","type":"user","sessionId":"timing","message":{"content":"first request"}}`,
				`{"timestamp":"2026-04-26T10:00:01Z","type":"assistant","sessionId":"timing","message":{"content":"working"}}`,
				`{"timestamp":"2026-04-26T10:00:02Z","type":"` + kind + `","isCompactSummary":true,"thinkingDurationMs":2000,"sessionId":"timing","message":{"content":"Summary of conversation so far..."}}`,
				`{"timestamp":"2026-04-26T10:00:03Z","type":"assistant","sessionId":"timing","message":{"content":"done"}}`,
			}, "\n")
			sess, messages := runClaudeParserTest(t, "timing.jsonl", source)
			assert.Equal(t, 1, sess.UserMessageCount)
			require.Len(t, messages, 4)
			assert.True(t, messages[2].IsSystem)
			assert.True(t, messages[2].IsCompactBoundary)
			evidence, err := ReadSessionTiming(t.Context(), strings.NewReader(source), "claude")
			require.NoError(t, err)
			require.Len(t, evidence.Turns, 1)
			assert.Equal(t, int64(3000), evidence.Turns[0].End-evidence.Turns[0].Start)
			assert.Empty(t, evidence.Model, "quoted compact summaries cannot supply model evidence")
		})
	}
}

func TestReadSessionTimingClaudeAssistantSnapshots(t *testing.T) {
	for _, tc := range []struct {
		name, firstID, nextID, between string
		wantCalls                      int
	}{
		{"same response snapshots", "response", "response", "", 1},
		{"snapshot across system record", "response", "response", `{"timestamp":"2026-04-26T10:00:01.5Z","type":"system","sessionId":"timing","content":"private-snapshot-text"}`, 1},
		{"snapshot across queued attachment", "response", "response", `{"timestamp":"2026-04-26T10:00:01.5Z","type":"attachment","sessionId":"timing","attachment":{"type":"queued_command","prompt":"private-snapshot-text"}}`, 1},
		{"different responses are ambiguous", "response-one", "response-two", "", 0},
		{"missing response IDs are ambiguous", "", "", "", 0},
		{"user record ends response run", "response", "response", `{"timestamp":"2026-04-26T10:00:01.5Z","type":"user","sessionId":"timing","message":{"content":"private-snapshot-text"}}`, 0},
		{"hidden user ends response run", "response", "response", `{"timestamp":"2026-04-26T10:00:01.5Z","type":"user","isMeta":true,"sessionId":"timing","message":{"content":"private-snapshot-text"}}`, 0},
		{"undated hidden user ends response run", "response", "response", `{"type":"user","isMeta":true,"sessionId":"timing","message":{"content":"private-snapshot-text"}}`, 0},
		{"ignored distinct assistant ends response run", "response", "response", `{"timestamp":"2026-04-26T10:00:01.5Z","type":"assistant","isMeta":true,"sessionId":"timing","message":{"id":"hidden-response","content":"private-snapshot-text"}}`, 0},
		{"compact boundary ends response run", "response", "response", `{"timestamp":"2026-04-26T10:00:01.5Z","type":"user","isCompactSummary":true,"sessionId":"timing","message":{"content":"private-snapshot-text"}}`, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := strings.Join([]string{
				`{"timestamp":"2026-04-26T10:00:00Z","type":"user","sessionId":"timing","message":{"content":"first request"}}`,
				`{"timestamp":"2026-04-26T10:00:01Z","type":"assistant","sessionId":"timing","message":{"id":"` + tc.firstID + `","content":[{"type":"text","text":"private-snapshot-text"},{"type":"tool_use","id":"call","name":"Read","input":{"file_path":"private-snapshot-path"}}]}}`,
				tc.between,
				`{"timestamp":"2026-04-26T10:00:02Z","type":"assistant","sessionId":"timing","message":{"id":"` + tc.nextID + `","content":[{"type":"text","text":"private-snapshot-text"},{"type":"tool_use","id":"call","name":"Read","input":{"file_path":"private-snapshot-path"}},{"type":"text","text":"More"}]}}`,
				`{"timestamp":"2026-04-26T10:00:04Z","type":"user","sessionId":"timing","message":{"content":[{"type":"tool_result","tool_use_id":"call","content":"private-snapshot-output"}]}}`,
			}, "\n")
			evidence, err := ReadSessionTiming(t.Context(), strings.NewReader(source), "claude")
			require.NoError(t, err)
			require.Len(t, evidence.Calls, tc.wantCalls)
			if tc.wantCalls > 0 {
				assert.Equal(t, int64(3000), evidence.Calls[0].End-evidence.Calls[0].Start, "keep the first observed request endpoint")
			}
			if strings.Contains(tc.between, `"type":"user","isMeta":true`) {
				_, messages := runClaudeParserTest(t, "timing.jsonl", source)
				calls := 0
				for _, message := range messages {
					calls += len(message.ToolCalls)
				}
				assert.Equal(t, 2, calls, "hidden users still separate normalized assistant calls")
			}
			if tc.between == "" && tc.firstID != "" && tc.firstID == tc.nextID {
				_, messages := runClaudeParserTest(t, "timing.jsonl", source)
				require.Len(t, messages, 3)
				require.Len(t, messages[1].ToolCalls, 1, "normalized ingestion also sees one call")
			}
		})
	}
}
