package db

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"go.kenn.io/agentsview/internal/parser"
)

type timingSourceEntry struct {
	path     string
	agent    string
	limit    int64
	info     os.FileInfo
	evidence *parser.SessionTimingEvidence
}

var timingSources struct {
	sync.Mutex
	entries []timingSourceEntry
}

// AssembleTimingContext enriches every store's timing with the same optional
// local source evidence. Sources are never modified. Missing, remote, replaced,
// forked, oversized or malformed sources retain the archive-only result.
func AssembleTimingContext(ctx context.Context, sess *Session, turns []TurnRow, calls []CallRow, now time.Time) (*SessionTiming, error) {
	if sess.DeletedAt != nil {
		return nil, nil
	}
	evidence := readTimingSource(ctx, sess)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return AssembleTiming(sess, turns, calls, now, evidence), nil
}

func readTimingSource(ctx context.Context, sess *Session) *parser.SessionTimingEvidence {
	if sess.FilePath == nil || !filepath.IsAbs(*sess.FilePath) || !strings.HasSuffix(*sess.FilePath, ".jsonl") && !(sess.Agent == "codex" && strings.HasSuffix(*sess.FilePath, ".jsonl.zst")) || (sess.Agent != "codex" && sess.Agent != "claude") {
		return nil
	}
	path := *sess.FilePath
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() {
		return nil
	}
	limit := info.Size()
	if sess.FileSize != nil && *sess.FileSize > 0 {
		limit = *sess.FileSize
	}
	// Read only the imported prefix, not newly appended conversation content.
	if limit <= 0 || limit > info.Size() || limit > 256<<20 {
		return nil
	}
	timingSources.Lock()
	var evidence *parser.SessionTimingEvidence
	for _, cached := range timingSources.entries {
		if cached.path == path && cached.agent == sess.Agent && cached.limit == limit && cached.info.Size() == info.Size() && cached.info.ModTime() == info.ModTime() && os.SameFile(cached.info, info) {
			evidence = cached.evidence
			break
		}
	}
	timingSources.Unlock()
	if evidence == nil {
		file, err := os.Open(path)
		if err != nil {
			return nil
		}
		defer file.Close()
		before, err := file.Stat()
		if err != nil || !os.SameFile(before, info) || before.Size() != info.Size() || before.ModTime() != info.ModTime() {
			return nil
		}
		if strings.HasSuffix(path, ".jsonl.zst") {
			evidence, err = parser.ReadCompressedSessionTiming(ctx, io.LimitReader(file, limit), sess.Agent)
		} else {
			evidence, err = parser.ReadSessionTiming(ctx, io.LimitReader(file, limit), sess.Agent)
		}
		if err != nil {
			return nil
		}
		after, err := file.Stat()
		if err != nil || after.Size() != before.Size() || after.ModTime() != before.ModTime() {
			return nil
		}
		timingSources.Lock()
		// FIFO bounded by both source count and retained event count.
		events := len(evidence.Turns) + len(evidence.Model) + len(evidence.Calls)
		for _, cached := range timingSources.entries {
			events += len(cached.evidence.Turns) + len(cached.evidence.Model) + len(cached.evidence.Calls)
		}
		for len(timingSources.entries) > 0 && (len(timingSources.entries) >= 16 || events > 200000) {
			oldest := timingSources.entries[0].evidence
			events -= len(oldest.Turns) + len(oldest.Model) + len(oldest.Calls)
			timingSources.entries = timingSources.entries[1:]
		}
		timingSources.entries = append(timingSources.entries, timingSourceEntry{path, sess.Agent, limit, info, evidence})
		timingSources.Unlock()
	}
	expected := sess.SourceSessionID
	if expected == "" {
		expected = strings.TrimPrefix(sess.ID, "codex:")
	}
	if evidence.SourceID == "" || evidence.SourceID != expected {
		return nil
	}
	return evidence
}

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
	occurrences := map[string]map[int64]bool{}
	for _, call := range calls {
		if occurrences[call.ToolUseID] == nil {
			occurrences[call.ToolUseID] = map[int64]bool{}
		}
		occurrences[call.ToolUseID][call.MessageID] = true
	}
	for _, call := range source.Calls {
		byID[call.ID] = append(byID[call.ID], call)
	}
	for i := range calls {
		call := &calls[i]

		candidates := byID[call.ToolUseID]
		best := -1
		var distance int64
		for j, candidate := range candidates {
			delta := candidate.Start - stamps[call.MessageID]
			if delta < 0 {
				delta = -delta
			}
			if best < 0 || delta < distance {
				best, distance = j, delta
			}
		}
		if best < 0 || ((len(candidates) > 1 || len(occurrences[call.ToolUseID]) > 1) && distance > 2000) {
			continue
		}
		paired := candidates[best]
		// A paired caller wait is narrower than a background subagent lifetime.
		call.WaitStart, call.WaitEnd = paired.Start, paired.End
		call.HasWait = true
		if _, ok := measuredCallInterval(*call); ok {
			continue
		}
		call.SubagentStart, call.SubagentEnd = "", ""
		call.ExecutionStart = time.UnixMilli(paired.Start).UTC().Format(time.RFC3339Nano)
		call.ExecutionEnd = time.UnixMilli(paired.End).UTC().Format(time.RFC3339Nano)
	}
	return calls
}
