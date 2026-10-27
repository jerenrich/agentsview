package parser

import (
	"context"
	"strconv"
	"time"

	"github.com/tidwall/gjson"
)

// Completed execution items carry their own endpoints. Envelope timestamps and
// response-item gaps describe delivery, not execution, and cannot replace them.
func (b *codexSessionBuilder) handleCompletedExecutionTiming(
	ctx context.Context, payload gjson.Result,
) {
	item := payload.Get("item")
	switch item.Get("type").Str {
	case "CommandExecution", "McpToolCall":
	default:
		return
	}
	callID := item.Get("id").Str
	if callID == "" {
		return
	}
	var status string
	switch item.Get("status").Str {
	case "completed":
		status = "completed"
	case "failed":
		status = "errored"
	default:
		return
	}
	start, startOK := codexExecutionTimestamp(payload.Get("started_at_ms"))
	end, endOK := codexExecutionTimestamp(payload.Get("completed_at_ms"))
	if !startOK || !endOK || end.Before(start) {
		return
	}
	// Item IDs must identify the model-visible call. Nested code-mode items
	// with different IDs must never be assigned to an enclosing exec call.
	// Existing sinks resolve exact IDs, including late incremental updates.
	b.appendToolResultEvent(ctx, callID, ParsedToolResultEvent{
		ToolUseID: callID,
		Source:    "tool_execution",
		Status:    "started",
		Timestamp: start,
	})
	b.appendToolResultEvent(ctx, callID, ParsedToolResultEvent{
		ToolUseID: callID,
		Source:    "tool_execution",
		Status:    status,
		Timestamp: end,
	})
}

func codexExecutionTimestamp(value gjson.Result) (time.Time, bool) {
	if value.Type != gjson.Number {
		return time.Time{}, false
	}
	ms, err := strconv.ParseInt(value.Raw, 10, 64)
	if err != nil {
		return time.Time{}, false
	}
	ts := time.UnixMilli(ms).UTC()
	// Stored execution endpoints must round-trip through RFC3339.
	if ts.Year() < 0 || ts.Year() > 9999 {
		return time.Time{}, false
	}
	return ts, true
}

// IsExecutionTiming identifies content-free execution updates whose exact call
// coordinates may be resolved from the archive after a checkpoint. Other late
// results still require the parser's occurrence-qualified target.
func (u ParsedToolCallUpdate) IsExecutionTiming() bool {
	if len(u.ResultEvents) == 0 {
		return false
	}
	for _, event := range u.ResultEvents {
		if event.Source != "tool_execution" || event.Content != "" ||
			event.AgentID != "" || event.SubagentSessionID != "" {
			return false
		}
	}
	return true
}
