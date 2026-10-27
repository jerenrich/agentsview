package db

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLookupToolCallPositions(t *testing.T) {
	d := testDB(t)
	for _, sessionID := range []string{"first", "other"} {
		timingInsertSession(t, d, sessionID, "2026-10-01T00:00:00Z", "2026-10-01T00:00:10Z")
		for ordinal := range 3 {
			timingInsertMessage(t, d, sessionID, ordinal, "assistant", "running", "2026-10-01T00:00:01Z", true)
			timingInsertToolCall(t, d, sessionID, timingMsgID(t, d, sessionID, ordinal), "reused", "exec_command", "Bash", "")
		}
		timingInsertMessage(t, d, sessionID, 3, "assistant", "running", "2026-10-01T00:00:02Z", true)
		timingInsertToolCall(t, d, sessionID, timingMsgID(t, d, sessionID, 3), "unique", "exec_command", "Bash", "")
	}
	for _, tc := range []struct {
		name, sessionID, callID string
		want                    int
	}{
		{"unique within session", "first", "unique", 1},
		{"bounded ambiguity", "first", "reused", 2},
		{"missing call", "first", "missing", 0},
		{"missing session", "missing", "unique", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			positions, err := d.LookupToolCallPositions(t.Context(), tc.sessionID, tc.callID)
			require.NoError(t, err)
			require.Len(t, positions, tc.want)
			if tc.want == 1 {
				assert.Equal(t, ToolCallPosition{MessageOrdinal: 3, CallIndex: 0}, positions[0])
			}
		})
	}
}
