package db

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/agentsview/internal/parser"
)

func TestGetSessionTimingSourceEvidenceExcludesIdle(t *testing.T) {
	d := testDB(t)
	ctx := t.Context()
	sid := "codex:timing"
	timingInsertSession(t, d, sid, "2026-04-26T10:00:00Z", "2026-04-26T11:00:04Z")
	for i, row := range []struct {
		role, stamp string
		tool        bool
	}{
		{"user", "2026-04-26T10:00:00Z", false},
		{"assistant", "2026-04-26T10:00:02Z", true},
		{"assistant", "2026-04-26T10:00:10Z", false},
		{"user", "2026-04-26T11:00:00Z", false},
		{"assistant", "2026-04-26T11:00:04Z", false},
	} {
		timingInsertMessage(t, d, sid, i, row.role, "message", row.stamp, row.tool)
	}
	timingInsertToolCall(t, d, sid, timingMsgID(t, d, sid, 1), "call", "exec", "Bash", "")
	source := strings.Join([]string{
		`{"timestamp":"2026-04-26T10:00:00Z","type":"session_meta","payload":{"id":"timing"}}`,
		`{"timestamp":"2026-04-26T10:00:00Z","type":"event_msg","payload":{"type":"task_started","turn_id":"first"}}`,
		`{"timestamp":"2026-04-26T10:00:02Z","type":"response_item","payload":{"type":"custom_tool_call","call_id":"call"}}`,
		`{"timestamp":"2026-04-26T10:00:06Z","type":"response_item","payload":{"type":"custom_tool_call_output","call_id":"call"}}`,
		`{"timestamp":"2026-04-26T10:00:05Z","type":"event_msg","payload":{"type":"item_completed","item":{"type":"Reasoning"},"started_at_ms":1777197600000,"completed_at_ms":1777197605000}}`,
		`{"timestamp":"2026-04-26T10:00:10Z","type":"event_msg","payload":{"type":"item_completed","item":{"type":"AgentMessage"},"started_at_ms":1777197608000,"completed_at_ms":1777197610000}}`,
		`{"timestamp":"2026-04-26T10:00:10Z","type":"event_msg","payload":{"type":"task_complete","turn_id":"first"}}`,
		`{"timestamp":"2026-04-26T11:00:00Z","type":"event_msg","payload":{"type":"task_started","turn_id":"second"}}`,
		`{"timestamp":"2026-04-26T11:00:02Z","type":"event_msg","payload":{"type":"item_completed","item":{"type":"Reasoning"},"started_at_ms":1777201200000,"completed_at_ms":1777201202000}}`,
		`{"timestamp":"2026-04-26T11:00:04Z","type":"event_msg","payload":{"type":"task_complete","turn_id":"second"}}`,
	}, "\n") + "\n"
	path := filepath.Join(t.TempDir(), "timing.jsonl")
	require.NoError(t, os.WriteFile(path, []byte(source), 0600))
	_, err := d.getWriter().Exec(ctx, "UPDATE sessions SET agent='codex', file_path=?, file_size=? WHERE id=?", path, len(source), sid)
	require.NoError(t, err)
	got, err := d.GetSessionTiming(ctx, sid)
	require.NoError(t, err)
	assert.Equal(t, "source", got.TimingSource)
	assert.Equal(t, int64(14000), got.ActiveDurationMs)
	assert.Equal(t, int64(3590000), got.IdleDurationMs)
	assert.Equal(t, ActivityTotals{ModelMs: 6000, ToolMs: 4000, UnattributedMs: 4000}, got.ActivityTotals)
	require.Len(t, got.Activity, 2)
	assert.Equal(t, int64(10000), got.Activity[0].DurationMs)
	assert.Equal(t, int64(4000), got.Activity[1].DurationMs)
	require.NotNil(t, got.SlowestCall)
	assert.Equal(t, new(int64(4000)), got.SlowestCall.DurationMs)
	// A cached source must still be present; an archive remains usable after
	// its source disappears and its final answers still exclude idle gaps.
	require.NoError(t, os.Remove(path))
	got, err = d.GetSessionTiming(ctx, sid)
	require.NoError(t, err)
	assert.Empty(t, got.TimingSource)
	assert.Zero(t, got.ActivityTotals.ModelMs)
	assert.Equal(t, int64(14000), got.ActiveDurationMs)
}

func TestTimingSourceCacheIdentityAndImportedPrefix(t *testing.T) {
	path := filepath.Join(t.TempDir(), "timing.jsonl")
	first := `{"timestamp":"2026-04-26T10:00:00Z","type":"session_meta","payload":{"id":"first"}}` + "\n"
	require.NoError(t, os.WriteFile(path, []byte(first), 0600))
	sess := &Session{ID: "codex:first", Agent: "codex", FilePath: &path, FileSize: new(int64(len(first)))}
	require.NotNil(t, readTimingSource(t.Context(), sess))
	wrong := *sess
	wrong.ID = "codex:other"
	assert.Nil(t, readTimingSource(t.Context(), &wrong))
	replacement := strings.ReplaceAll(first, "first", "other")
	temporary := path + ".replacement"
	require.NoError(t, os.WriteFile(temporary, []byte(replacement), 0600))
	require.NoError(t, os.Rename(temporary, path))
	assert.Nil(t, readTimingSource(t.Context(), sess))
	require.NotNil(t, readTimingSource(t.Context(), &wrong))
	// An append outside the archived byte count does not enter timing data.
	appended := replacement + `{"timestamp":"2026-04-26T10:00:01Z","type":"event_msg","payload":{"type":"item_completed","item":{"type":"Reasoning"},"started_at_ms":1777197600000,"completed_at_ms":1777197601000}}` + "\n"
	require.NoError(t, os.WriteFile(path, []byte(appended), 0600))
	evidence := readTimingSource(t.Context(), &wrong)
	require.NotNil(t, evidence)
	assert.Empty(t, evidence.Model)
}

func TestTimingActivityParallelModelAndToolsReconcile(t *testing.T) {
	start := time.Date(2026, 4, 26, 10, 0, 0, 0, time.UTC)
	stamp := func(ms int64) string { return start.Add(time.Duration(ms) * time.Millisecond).Format(time.RFC3339Nano) }
	epoch := start.UnixMilli()
	source := &parser.SessionTimingEvidence{
		Turns: []parser.TimingInterval{{Start: epoch, End: epoch + 10000}},
		Model: []parser.TimingInterval{{Start: epoch, End: epoch + 4000}, {Start: epoch + 2000, End: epoch + 6000}},
		Calls: []parser.TimingCall{
			{ID: "a", TimingInterval: parser.TimingInterval{Start: epoch + 3000, End: epoch + 7000}},
			{ID: "b", TimingInterval: parser.TimingInterval{Start: epoch + 5000, End: epoch + 8000}},
		},
	}
	got := AssembleTiming(&Session{ID: "parallel", StartedAt: new(stamp(0)), EndedAt: new(stamp(10000))},
		[]TurnRow{{MessageID: 1, Role: "user", Timestamp: stamp(0), ContentLength: 2}, {MessageID: 2, Role: "assistant", Timestamp: stamp(3000), HasToolUse: true}},
		[]CallRow{{MessageID: 2, ToolUseID: "a"}, {MessageID: 2, ToolUseID: "b"}}, start, source)
	assert.Equal(t, ActivityTotals{ModelMs: 3000, ToolMs: 5000, UnattributedMs: 2000}, got.ActivityTotals)
	assert.Equal(t, got.ActiveDurationMs, got.ActivityTotals.ModelMs+got.ActivityTotals.ToolMs+got.ActivityTotals.UnattributedMs)
}

func TestTimingActivityRapidTurnsKeepTheirOwnLifecycle(t *testing.T) {
	start := time.Date(2026, 4, 26, 10, 0, 0, 0, time.UTC)
	epoch := start.UnixMilli()
	stamp := func(ms int64) string { return time.UnixMilli(epoch + ms).UTC().Format(time.RFC3339Nano) }
	source := &parser.SessionTimingEvidence{Turns: []parser.TimingInterval{{Start: epoch, End: epoch + 1000}, {Start: epoch + 1200, End: epoch + 2000}}}
	got := AssembleTiming(&Session{ID: "rapid", StartedAt: new(stamp(0)), EndedAt: new(stamp(2000))},
		[]TurnRow{{MessageID: 1, Role: "user", Timestamp: stamp(0), ContentLength: 1}, {MessageID: 2, Ordinal: 1, Role: "user", Timestamp: stamp(1200), ContentLength: 1}}, nil, start, source)
	require.Len(t, got.Activity, 2)
	assert.Equal(t, int64(1000), got.Activity[0].DurationMs)
	assert.Equal(t, int64(800), got.Activity[1].DurationMs)
	assert.Equal(t, int64(200), got.IdleDurationMs)
}

func TestTimingActivityLifecycleWithoutVisiblePrompt(t *testing.T) {
	epoch := time.Date(2026, 4, 26, 10, 0, 0, 0, time.UTC).UnixMilli()
	stamp := func(ms int64) string { return time.UnixMilli(epoch + ms).UTC().Format(time.RFC3339Nano) }
	for _, promptAt := range []int64{-1, 4000, 10000} {
		t.Run(stamp(promptAt), func(t *testing.T) {
			turns := []TurnRow{{MessageID: 1, Timestamp: stamp(5000), Role: "assistant", HasToolUse: true}}
			if promptAt >= 0 {
				turns = append(turns, TurnRow{MessageID: 2, Ordinal: 1, Role: "user", Timestamp: stamp(promptAt), ContentLength: 1})
			}
			source := &parser.SessionTimingEvidence{
				Turns: []parser.TimingInterval{{Start: epoch, End: epoch + 6000}, {Start: epoch + 10000, End: epoch + 12000}},
				Model: []parser.TimingInterval{{Start: epoch, End: epoch + 2000}},
				Calls: []parser.TimingCall{{ID: "a", TimingInterval: parser.TimingInterval{Start: epoch + 3000, End: epoch + 6000}}},
			}
			got := AssembleTiming(&Session{StartedAt: new(stamp(5000)), EndedAt: new(stamp(12000))}, turns,
				[]CallRow{{MessageID: 1, ToolUseID: "a", ExecutionStart: stamp(4000), ExecutionEnd: stamp(5000)}}, time.Time{}, source)
			assert.Equal(t, int64(8000), got.ActiveDurationMs)
			assert.Equal(t, int64(4000), got.IdleDurationMs)
			assert.Equal(t, ActivityTotals{ModelMs: 2000, ToolMs: 3000, UnattributedMs: 3000}, got.ActivityTotals)
			require.Len(t, got.Activity, 2)
			assert.Equal(t, stamp(0), got.Activity[0].StartedAt)
			require.NotNil(t, got.SlowestCall)
			assert.Equal(t, new(int64(1000)), got.SlowestCall.DurationMs, "execution endpoints must survive paired wait enrichment")
		})
	}
}

func TestTimingSourceCompressedImportedPrefix(t *testing.T) {
	source := `{"timestamp":"2026-04-26T10:00:00Z","type":"session_meta","payload":{"id":"compressed"}}` + "\n" +
		`{"timestamp":"2026-04-26T10:00:00Z","type":"event_msg","payload":{"type":"task_started","turn_id":"a"}}` + "\n" +
		`{"timestamp":"2026-04-26T10:00:10Z","type":"event_msg","payload":{"type":"task_complete","turn_id":"a"}}` + "\n"
	encoder, err := zstd.NewWriter(nil)
	require.NoError(t, err)
	defer encoder.Close()
	compressed := encoder.EncodeAll([]byte(source), nil)
	path := filepath.Join(t.TempDir(), "timing.jsonl.zst")
	require.NoError(t, os.WriteFile(path, compressed, 0600))
	sess := &Session{ID: "codex:compressed", Agent: "codex", FilePath: &path, FileSize: new(int64(len(compressed)))}
	got := readTimingSource(t.Context(), sess)
	require.NotNil(t, got)
	require.Len(t, got.Turns, 1)
	assert.Equal(t, int64(10000), got.Turns[0].End-got.Turns[0].Start)
	// Later complete frames must not leak past the compressed imported byte size.
	appended := append(compressed, encoder.EncodeAll([]byte(source), nil)...)
	require.NoError(t, os.WriteFile(path, appended, 0600))
	got = readTimingSource(t.Context(), sess)
	require.NotNil(t, got)
	assert.Len(t, got.Turns, 1)
	// An imported prefix truncated inside a frame is rejected, not used partially.
	sess.FileSize = new(int64(len(compressed) - 3))
	assert.Nil(t, readTimingSource(t.Context(), sess))
}

func TestTimingLifecycleAnchorSkipsHiddenMessages(t *testing.T) {
	start := time.Date(2026, 4, 26, 10, 0, 0, 0, time.UTC)
	stamp := func(seconds int) string {
		return start.Add(time.Duration(seconds) * time.Second).Format(time.RFC3339Nano)
	}
	got := AssembleTiming(&Session{StartedAt: new(stamp(0)), EndedAt: new(stamp(10))}, []TurnRow{
		{MessageID: 1, Ordinal: 0, Timestamp: stamp(0), Role: "user", ContentLength: 1},
		{MessageID: 2, Ordinal: 1, Timestamp: stamp(1), Role: "user", IsSystemPrefixed: true},
	}, nil, start, &parser.SessionTimingEvidence{Turns: []parser.TimingInterval{{Start: start.Add(2 * time.Second).UnixMilli(), End: start.Add(10 * time.Second).UnixMilli()}}})
	require.Len(t, got.Activity, 1)
	assert.Equal(t, int64(1), got.Activity[0].MessageID)
	assert.Equal(t, 0, got.Activity[0].Ordinal)
	assert.Equal(t, stamp(2), got.Activity[0].StartedAt)
}
