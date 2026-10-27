package parser

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCodexCompletedExecutionTiming(t *testing.T) {
	for _, kind := range []string{"CommandExecution", "McpToolCall"} {
		for _, status := range []string{"completed", "failed"} {
			t.Run(kind+"/"+status, func(t *testing.T) {
				call := `{"type":"response_item","timestamp":"2026-10-01T00:00:00Z","payload":{"type":"function_call","name":"exec_command","call_id":"call_tool","arguments":"{}"}}`
				output := `{"type":"response_item","timestamp":"2026-10-01T00:00:10Z","payload":{"type":"function_call_output","call_id":"call_tool","output":"original output"}}`
				// Delivery is much later than execution. The item arrives after
				// the output, when the builder has forgotten its pending call.
				completed := fmt.Sprintf(`{"type":"event_msg","timestamp":"2026-10-01T00:01:00Z","payload":{"type":"item_completed","started_at_ms":1790812801000,"completed_at_ms":1790812803500,"item":{"type":%q,"id":"call_tool","status":%q,"aggregated_output":"must not duplicate output"}}}`, kind, status)
				_, messages := runCodexParserTest(t, "", call+"\n"+output+"\n"+completed+"\n"+completed+"\n", false)
				require.Len(t, messages, 1)
				require.Len(t, messages[0].ToolCalls, 1)
				events := messages[0].ToolCalls[0].ResultEvents
				require.Len(t, events, 3, "repeated completion records must deduplicate")
				assert.Equal(t, "original output", events[0].Content)
				assert.Equal(t, "tool_execution", events[1].Source)
				assert.Equal(t, "started", events[1].Status)
				assert.Equal(t, time.Date(2026, 10, 1, 0, 0, 1, 0, time.UTC), events[1].Timestamp)
				assert.Equal(t, "tool_execution", events[2].Source)
				wantStatus := "completed"
				if status == "failed" {
					wantStatus = "errored"
				}
				assert.Equal(t, wantStatus, events[2].Status)
				assert.Equal(t, 2500*time.Millisecond, events[2].Timestamp.Sub(events[1].Timestamp))
				assert.Empty(t, events[1].Content)
				assert.Empty(t, events[2].Content)
			})
		}
	}
}

func TestCodexCompletedExecutionTimingRequiresEvidence(t *testing.T) {
	for _, tc := range []struct {
		name, item, endpoints string
	}{
		{"missing start", `"type":"CommandExecution","id":"call_tool","status":"completed"`, `"completed_at_ms":1790812803500`},
		{"missing end", `"type":"CommandExecution","id":"call_tool","status":"completed"`, `"started_at_ms":1790812801000`},
		{"null start", `"type":"CommandExecution","id":"call_tool","status":"completed"`, `"started_at_ms":null,"completed_at_ms":1790812803500`},
		{"string start", `"type":"CommandExecution","id":"call_tool","status":"completed"`, `"started_at_ms":"1790812801000","completed_at_ms":1790812803500`},
		{"fractional start", `"type":"CommandExecution","id":"call_tool","status":"completed"`, `"started_at_ms":1790812801000.5,"completed_at_ms":1790812803500`},
		{"overflow", `"type":"CommandExecution","id":"call_tool","status":"completed"`, `"started_at_ms":9223372036854775808,"completed_at_ms":1790812803500`},
		{"out of RFC3339 range", `"type":"CommandExecution","id":"call_tool","status":"completed"`, `"started_at_ms":253402300800000,"completed_at_ms":253402300800000`},
		{"reversed", `"type":"CommandExecution","id":"call_tool","status":"completed"`, `"started_at_ms":1790812804000,"completed_at_ms":1790812803500`},
		{"in progress", `"type":"CommandExecution","id":"call_tool","status":"in_progress"`, `"started_at_ms":1790812801000,"completed_at_ms":1790812803500`},
		{"declined", `"type":"CommandExecution","id":"call_tool","status":"declined"`, `"started_at_ms":1790812801000,"completed_at_ms":1790812803500`},
		{"missing id", `"type":"CommandExecution","status":"completed"`, `"started_at_ms":1790812801000,"completed_at_ms":1790812803500`},
		{"nested id", `"type":"CommandExecution","id":"exec_nested","status":"completed"`, `"started_at_ms":1790812801000,"completed_at_ms":1790812803500`},
		{"reasoning", `"type":"Reasoning","id":"call_tool","status":"completed"`, `"started_at_ms":1790812801000,"completed_at_ms":1790812803500`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			call := `{"type":"response_item","timestamp":"2026-10-01T00:00:00Z","payload":{"type":"function_call","name":"exec_command","call_id":"call_tool","arguments":"{}"}}`
			output := `{"type":"response_item","timestamp":"2026-10-01T00:00:10Z","payload":{"type":"function_call_output","call_id":"call_tool","output":"original output"}}`
			completed := fmt.Sprintf(`{"type":"event_msg","timestamp":"2026-10-01T00:01:00Z","payload":{"type":"item_completed",%s,"item":{%s}}}`, tc.endpoints, tc.item)
			_, messages := runCodexParserTest(t, "", call+"\n"+output+"\n"+completed+"\n", false)
			require.Len(t, messages, 1)
			require.Len(t, messages[0].ToolCalls, 1)
			require.Len(t, messages[0].ToolCalls[0].ResultEvents, 1)
			assert.Equal(t, "function_call_output", messages[0].ToolCalls[0].ResultEvents[0].Source)
		})
	}
}

func TestCodexCompletedExecutionTimingZeroDuration(t *testing.T) {
	_, messages := runCodexParserTest(t, "", `{"type":"response_item","payload":{"type":"function_call","name":"exec_command","call_id":"call_zero","arguments":"{}"}}
{"type":"event_msg","payload":{"type":"item_completed","started_at_ms":0,"completed_at_ms":0,"item":{"type":"CommandExecution","id":"call_zero","status":"completed"}}}
`, false)
	require.Len(t, messages, 1)
	events := messages[0].ToolCalls[0].ResultEvents
	require.Len(t, events, 2)
	assert.Equal(t, time.UnixMilli(0).UTC(), events[0].Timestamp)
	assert.Equal(t, events[0].Timestamp, events[1].Timestamp)
}
