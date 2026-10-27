package db

import (
	"slices"
	"sort"
	"time"

	"go.kenn.io/agentsview/internal/parser"
)

type ActivityTotals struct {
	ModelMs        int64 `json:"model_ms,omitzero"`
	ToolMs         int64 `json:"tool_ms"`
	UnattributedMs int64 `json:"unattributed_ms"`
}

type TurnActivity struct {
	ModelMs        int64  `json:"model_ms,omitzero"`
	MessageID      int64  `json:"message_id"`
	Ordinal        int    `json:"ordinal"`
	StartedAt      string `json:"started_at"`
	DurationMs     int64  `json:"duration_ms"`
	ToolMs         int64  `json:"tool_ms"`
	UnattributedMs int64  `json:"unattributed_ms"`
	Running        bool   `json:"running"`
}

type activityInterval struct {
	start int64
	end   int64
}

func timingTimestamp(value string) (int64, bool) {
	t, err := time.Parse(time.RFC3339Nano, value)
	return t.UnixMilli(), err == nil
}

func parseClosedInterval(startValue, endValue string) (activityInterval, bool) {
	start, startErr := time.Parse(time.RFC3339Nano, startValue)
	end, endErr := time.Parse(time.RFC3339Nano, endValue)
	if startErr != nil || endErr != nil || end.Before(start) {
		return activityInterval{}, false
	}
	return activityInterval{start.UnixMilli(), end.UnixMilli()}, true
}

func executionInterval(call CallRow) (activityInterval, bool) {
	return parseClosedInterval(call.ExecutionStart, call.ExecutionEnd)
}

func measuredCallEvidence(call CallRow) (activityInterval, string, bool) {
	if call.SubagentSessionID != nil {
		if interval, ok := parseClosedInterval(call.SubagentStart, call.SubagentEnd); ok {
			return interval, "subagent", true
		}
	}
	if interval, ok := executionInterval(call); ok {
		return interval, "execution", true
	}
	if call.HasWait && call.WaitEnd >= call.WaitStart {
		return activityInterval{call.WaitStart, call.WaitEnd}, "caller_wait", true
	}
	return activityInterval{}, "", false
}

func measuredCallInterval(call CallRow) (activityInterval, bool) {
	interval, _, ok := measuredCallEvidence(call)
	return interval, ok
}

// activityPrompts returns visible user prompts in timestamp order.
func activityPrompts(turns []TurnRow) ([]TurnActivity, []int64) {
	activity := []TurnActivity{}
	var starts []int64
	for _, row := range turns {
		if row.Role != "user" || row.IsSystem || row.IsSystemPrefixed ||
			row.SourceSubtype == "tool_result" || row.ContentLength <= 0 {
			continue
		}
		start, ok := timingTimestamp(row.Timestamp)
		if !ok || (len(starts) > 0 && start < starts[len(starts)-1]) {
			continue
		}
		starts = append(starts, start)
		activity = append(activity, TurnActivity{
			MessageID: row.MessageID, Ordinal: int(row.Ordinal),
			StartedAt: row.Timestamp,
		})
	}

	return activity, starts
}

// assembleTurnActivity clips aggregates to the session, retaining raw call evidence.
func assembleTurnActivity(
	out *SessionTiming, sess *Session, turns []TurnRow, calls []CallRow, now time.Time, source *parser.SessionTimingEvidence,
) []*activityInterval {
	var starts []int64
	out.Activity, starts = activityPrompts(turns)

	var lower, upper int64
	var hasLower, hasUpper bool
	if sess.StartedAt != nil {
		lower, hasLower = timingTimestamp(*sess.StartedAt)
	}
	if !hasLower && len(starts) > 0 {
		lower, hasLower = starts[0], true
	}
	if sess.EndedAt != nil {
		upper, hasUpper = timingTimestamp(*sess.EndedAt)
	}
	if out.Running {
		upper, hasUpper = now.UnixMilli(), true
	}
	if len(starts) > 0 && !hasUpper {
		upper, hasUpper = starts[len(starts)-1], true
	}

	var sourceTurns, model []activityInterval
	if source != nil {
		out.ModelTimingAvailable = len(source.Model) > 0
		for _, interval := range source.Turns {
			sourceTurns = append(sourceTurns, activityInterval{interval.Start, interval.End})
		}
		for _, interval := range source.Model {
			model = append(model, activityInterval{interval.Start, interval.End})
		}
		sort.Slice(sourceTurns, func(i, j int) bool { return sourceTurns[i].start < sourceTurns[j].start })
	}
	var windows []activityInterval
	sourceTurns = activityMergeIntervals(sourceTurns, false)
	if len(sourceTurns) > 0 {
		out.TimingSource = "source"
		windows = sourceTurns
		out.Activity = nil
		type anchor struct {
			stamp int64
			row   TurnRow
		}
		anchors := make([]anchor, 0, len(turns))
		for _, row := range turns {
			if row.IsSystem || row.IsSystemPrefixed || row.SourceSubtype == "tool_result" {
				continue
			}
			if stamp, valid := timingTimestamp(row.Timestamp); valid {
				anchors = append(anchors, anchor{stamp, row})
			}
		}
		sort.SliceStable(anchors, func(i, j int) bool { return anchors[i].stamp < anchors[j].stamp })
		for _, window := range windows {
			// Source lifecycle owns the bounds. A transcript message is only a
			// navigation anchor, including when a resumed turn has no prompt.
			var anchor TurnRow
			if len(anchors) > 0 {
				index := sort.Search(len(anchors), func(i int) bool { return anchors[i].stamp >= window.start })
				if index == len(anchors) || anchors[index].stamp > window.end {
					index = max(0, index-1)
				}
				anchor = anchors[index].row
			}
			out.Activity = append(out.Activity, TurnActivity{
				MessageID: anchor.MessageID, Ordinal: int(anchor.Ordinal),
				StartedAt:  time.UnixMilli(window.start).UTC().Format(time.RFC3339Nano),
				DurationMs: window.end - window.start,
			})
			out.ActiveDurationMs += window.end - window.start
		}
		// Session metadata can precede the first task, and lifecycle completion
		// can follow the final visible message. Preserve their combined extent.
		if hasLower {
			lower = min(lower, windows[0].start)
		} else {
			lower = windows[0].start
		}
		if hasUpper {
			upper = max(upper, windows[len(windows)-1].end)
		} else {
			upper = windows[len(windows)-1].end
		}
		hasLower, hasUpper = true, true
		out.TotalDurationMs = max(out.TotalDurationMs, upper-lower)
	} else {
		type assistantEnd struct {
			stamp      int64
			hasToolUse bool
		}
		var assistantEnds []assistantEnd
		for _, row := range turns {
			if row.Role != "assistant" || row.IsSystem {
				continue
			}
			if stamp, ok := timingTimestamp(row.Timestamp); ok {
				assistantEnds = append(assistantEnds, assistantEnd{stamp, row.HasToolUse})
			}
		}
		sort.SliceStable(assistantEnds, func(i, j int) bool { return assistantEnds[i].stamp < assistantEnds[j].stamp })
		for i := range out.Activity {
			start, end := starts[i], upper
			if hasLower {
				start = max(start, lower)
			}
			if i+1 < len(starts) {
				end = min(end, starts[i+1])
			}
			// A final assistant answer is the best archive-only endpoint. A tool
			// request alone does not prove that a turn has finished.
			closedByAnswer := false
			index := sort.Search(len(assistantEnds), func(j int) bool { return assistantEnds[j].stamp > end }) - 1
			if index >= 0 && assistantEnds[index].stamp >= start && !assistantEnds[index].hasToolUse {
				end = assistantEnds[index].stamp
				closedByAnswer = true
			}
			end = max(start, end)
			out.Activity[i].DurationMs = end - start
			out.Activity[i].Running = out.Running && i == len(out.Activity)-1 && !closedByAnswer
			windows = append(windows, activityInterval{start, end})
			out.ActiveDurationMs += end - start
		}
	}
	if hasLower && hasUpper {
		out.TotalDurationMs = max(out.TotalDurationMs, upper-lower)
	}
	out.TotalDurationMs = max(out.TotalDurationMs, out.ActiveDurationMs, 0)
	out.IdleDurationMs = out.TotalDurationMs - out.ActiveDurationMs
	var measured []activityInterval
	byCategory := map[string][]activityInterval{}
	counts := map[string]int{}
	intervals := make([]*activityInterval, len(calls))
	for i, call := range calls {
		counts[call.Category]++
		interval, ok := measuredCallInterval(call)
		if !ok {
			continue
		}
		intervals[i] = &interval
		clipped := interval
		if call.HasWait {
			clipped = activityInterval{call.WaitStart, call.WaitEnd}
		}
		if hasLower {
			clipped.start = max(clipped.start, lower)
		}
		if hasUpper {
			clipped.end = min(clipped.end, upper)
		}
		if clipped.end < clipped.start {
			continue
		}
		measured = append(measured, clipped)
		byCategory[call.Category] = append(byCategory[call.Category], clipped)
	}
	if len(windows) > 0 {
		measured = activityIntersections(measured, windows)
		for category, spans := range byCategory {
			byCategory[category] = activityIntersections(spans, windows)
		}
	}

	out.ToolDurationMs = activityUnionMs(measured)
	if len(windows) == 0 {
		out.ToolDurationMs = 0
		measured = nil
		byCategory = map[string][]activityInterval{}
	}
	for category, count := range counts {
		out.ByCategory = append(out.ByCategory, CategoryTotal{
			Category: category, CallCount: count,
			DurationMs: activityUnionMs(byCategory[category]),
		})
	}
	sort.Slice(out.ByCategory, func(i, j int) bool {
		if out.ByCategory[i].DurationMs == out.ByCategory[j].DurationMs {
			return out.ByCategory[i].Category < out.ByCategory[j].Category
		}
		return out.ByCategory[i].DurationMs > out.ByCategory[j].DurationMs
	})
	if len(windows) == 0 {
		return intervals
	}
	measured = activityMerge(measured)
	model = activityMerge(model)
	overlap := activityIntersections(model, measured)
	for i := range out.Activity {
		row := &out.Activity[i]
		row.ToolMs = activityWindowMs(measured, windows[i])
		row.ModelMs = activityWindowMs(model, windows[i]) - activityWindowMs(overlap, windows[i])
		row.UnattributedMs = max(0, row.DurationMs-row.ToolMs-row.ModelMs)
		out.ActivityTotals.ModelMs += row.ModelMs
		out.ActivityTotals.ToolMs += row.ToolMs
		out.ActivityTotals.UnattributedMs += row.UnattributedMs
	}
	return intervals
}

func activityMerge(intervals []activityInterval) []activityInterval {
	return activityMergeIntervals(intervals, true)
}

// Distinct lifecycle turns may touch at one endpoint; preserve those rows while
// union queries can coalesce touching intervals without changing the duration.
func activityMergeIntervals(intervals []activityInterval, mergeTouching bool) []activityInterval {
	intervals = slices.Clone(intervals)
	sort.Slice(intervals, func(i, j int) bool { return intervals[i].start < intervals[j].start })
	result := make([]activityInterval, 0, len(intervals))
	for _, interval := range intervals {
		if interval.end < interval.start {
			continue
		}
		if len(result) > 0 && (interval.start < result[len(result)-1].end || mergeTouching && interval.start == result[len(result)-1].end) {
			result[len(result)-1].end = max(result[len(result)-1].end, interval.end)
		} else {
			result = append(result, interval)
		}
	}
	return result
}

func activityUnionMs(intervals []activityInterval) int64 {
	var total int64
	for _, interval := range activityMerge(intervals) {
		total += interval.end - interval.start
	}
	return total
}

func activityIntersections(left, right []activityInterval) []activityInterval {
	left, right = activityMerge(left), activityMerge(right)
	var result []activityInterval
	for i, j := 0, 0; i < len(left) && j < len(right); {
		a, b := left[i], right[j]
		lo, hi := max(a.start, b.start), min(a.end, b.end)
		if hi > lo {
			result = append(result, activityInterval{lo, hi})
		}
		if a.end <= b.end {
			i++
		} else {
			j++
		}
	}
	return result
}

// activityWindowMs queries disjoint sorted intervals without scanning or
// sorting the whole session again for each visible prompt.
func activityWindowMs(intervals []activityInterval, window activityInterval) int64 {
	var total int64
	index := sort.Search(len(intervals), func(i int) bool { return intervals[i].end > window.start })
	for ; index < len(intervals) && intervals[index].start < window.end; index++ {
		total += max(0, min(intervals[index].end, window.end)-max(intervals[index].start, window.start))
	}
	return total
}
