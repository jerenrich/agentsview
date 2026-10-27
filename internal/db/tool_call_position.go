package db

import "context"

// LookupToolCallPositions returns at most two occurrences of an exact call ID.
// Incremental timing imports can distinguish an unknown ID, a unique target,
// and an ambiguous reused ID without loading the session transcript.
func (db *DB) LookupToolCallPositions(
	ctx context.Context, sessionID, toolUseID string,
) ([]ToolCallPosition, error) {
	rows, err := db.getReader().QueryContext(ctx, `
		SELECT m.ordinal, tc.call_index
		FROM tool_calls tc JOIN messages m ON m.id = tc.message_id
		WHERE tc.session_id = ? AND tc.tool_use_id = ? LIMIT 2
	`, sessionID, toolUseID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var positions []ToolCallPosition
	for rows.Next() {
		var position ToolCallPosition
		if err := rows.Scan(&position.MessageOrdinal, &position.CallIndex); err != nil {
			return nil, err
		}
		positions = append(positions, position)
	}
	return positions, rows.Err()
}
