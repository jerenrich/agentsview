package db

import (
	"sort"

	"go.kenn.io/agentsview/internal/parser"
)

// sourceTimedCalls attaches paired request/result waiting for aggregates and
// fills missing call durations. Explicit execution endpoints stay intact.
func sourceTimedCalls(turns []TurnRow, calls []CallRow, source *parser.SessionTimingEvidence) []CallRow {
	if source == nil {
		return calls
	}
	calls = append([]CallRow(nil), calls...)
	stamps := map[int64]int64{}
	for _, turn := range turns {
		if stamp, ok := timingTimestamp(turn.Timestamp); ok {
			stamps[turn.MessageID] = stamp
		}
	}
	byID := map[string][]parser.TimingCall{}
	occurrences := map[string]int{}
	for _, call := range calls {
		occurrences[call.ToolUseID]++
	}
	for _, call := range source.Calls {
		byID[call.ID] = append(byID[call.ID], call)
	}
	for _, candidates := range byID {
		sort.Slice(candidates, func(i, j int) bool { return candidates[i].Start < candidates[j].Start })
	}
	type choice struct {
		index int
		valid bool
	}
	choices := make([]choice, len(calls))
	claims := map[string]map[int]int{}
	for i, call := range calls {
		candidates := byID[call.ToolUseID]
		if len(candidates) == 0 || call.ToolUseID == "" {
			continue
		}
		stamp, known := stamps[call.MessageID]
		repeated := len(candidates) > 1 || occurrences[call.ToolUseID] > 1
		if repeated && !known {
			continue
		}
		index := sort.Search(len(candidates), func(j int) bool { return candidates[j].Start >= stamp })
		best := -1
		var distance int64
		tied := false
		// The two neighbours are sufficient unless equal starts are duplicated;
		// those are explicitly ambiguous below.
		for _, j := range []int{index - 1, index} {
			if j < 0 || j >= len(candidates) {
				continue
			}
			delta := candidates[j].Start - stamp
			if delta < 0 {
				delta = -delta
			}
			if best < 0 || delta < distance {
				best, distance, tied = j, delta, false
			} else if delta == distance {
				tied = true
			}
		}
		if best < 0 || tied || (repeated && distance > 2000) {
			continue
		}
		if best > 0 && candidates[best-1].Start == candidates[best].Start || best+1 < len(candidates) && candidates[best+1].Start == candidates[best].Start {
			continue
		}
		choices[i] = choice{best, true}
		if claims[call.ToolUseID] == nil {
			claims[call.ToolUseID] = map[int]int{}
		}
		claims[call.ToolUseID][best]++
	}
	for i := range calls {
		call := &calls[i]

		selected := choices[i]
		if !selected.valid || claims[call.ToolUseID][selected.index] != 1 {
			continue
		}
		paired := byID[call.ToolUseID][selected.index]
		call.WaitStart, call.WaitEnd = paired.Start, paired.End
		call.HasWait = true
	}
	return calls
}
