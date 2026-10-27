package sync

import (
	"context"

	"go.kenn.io/agentsview/internal/parser"
)

// Resolve only unique persisted IDs. Unknown nested execution IDs are discarded;
// reused IDs require an authoritative full parse to recover their occurrence.
func (e *Engine) resolveCodexTimingUpdates(
	ctx context.Context, sessionID string, updates []parser.ParsedToolCallUpdate,
) ([]parser.ParsedToolCallUpdate, bool, error) {
	resolved := updates[:0]
	for _, update := range updates {
		if !update.TargetKnown {
			if !update.IsExecutionTiming() {
				return nil, false, nil
			}
			positions, err := e.db.LookupToolCallPositions(ctx, sessionID, update.ToolUseID)
			if err != nil {
				return nil, false, err
			}
			switch len(positions) {
			case 0:
				continue
			case 1:
				update.MessageOrdinal = positions[0].MessageOrdinal
				update.CallIndex = positions[0].CallIndex
				update.TargetKnown = true
			default:
				return nil, false, nil
			}
		}
		resolved = append(resolved, update)
	}
	return resolved, true, nil
}
