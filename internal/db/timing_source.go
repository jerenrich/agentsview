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

const maxTimingSourceBytes = 256 << 20

// Bound concurrent decompression and JSON scans, including requests for different
// sessions. A cancelled request waiting for capacity does no filesystem work.
var timingSourceReads = make(chan struct{}, 2)

func readTimingSource(ctx context.Context, sess *Session) *parser.SessionTimingEvidence {
	if ctx.Err() != nil || sess.FilePath == nil || !filepath.IsAbs(*sess.FilePath) || sess.FileSize == nil || *sess.FileSize <= 0 {
		return nil
	}
	path := *sess.FilePath
	supported := strings.HasSuffix(path, ".jsonl") || sess.Agent == "codex" && strings.HasSuffix(path, ".jsonl.zst")
	if !supported || sess.Agent != "codex" && sess.Agent != "claude" {
		return nil
	}
	info, err := os.Stat(path)
	limit := *sess.FileSize
	if err != nil || !info.Mode().IsRegular() || limit > info.Size() || limit > maxTimingSourceBytes {
		return nil
	}
	entry := timingSourceEntry{path: path, agent: sess.Agent, limit: limit, info: info}
	evidence, found := cachedTimingSource(entry)
	if !found {
		select {
		case timingSourceReads <- struct{}{}:
		case <-ctx.Done():
			return nil
		}
		defer func() { <-timingSourceReads }()
		// Another reader may have populated even a negative result while this
		// request waited for capacity.
		evidence, found = cachedTimingSource(entry)
		if !found {
			file, err := os.Open(path)
			if err != nil {
				return nil
			}
			defer file.Close()
			before, err := file.Stat()
			if err != nil || !sameTimingSourceInfo(before, info) {
				return nil
			}
			reader := io.LimitReader(file, limit)
			if strings.HasSuffix(path, ".jsonl.zst") {
				evidence, err = parser.ReadCompressedSessionTiming(ctx, reader, sess.Agent)
			} else {
				evidence, err = parser.ReadSessionTiming(ctx, reader, sess.Agent)
			}
			after, statErr := file.Stat()
			if ctx.Err() != nil || statErr != nil || !sameTimingSourceInfo(before, after) {
				return nil
			}
			if err != nil || evidence.SourceID == "" {
				evidence = nil
			}
			// Cache unsupported/malformed snapshots too. Their identity must
			// still match on reuse; cancellation and mutations never enter it.
			entry.evidence = evidence
			cacheTimingSource(entry)
		}
	}
	expected := sess.SourceSessionID
	if expected == "" {
		expected = strings.TrimPrefix(sess.ID, "codex:")
	}
	if evidence == nil || evidence.SourceID == "" || evidence.SourceID != expected {
		return nil
	}
	return evidence
}

func sameTimingSourceInfo(a, b os.FileInfo) bool {
	return a.Size() == b.Size() && a.ModTime().Equal(b.ModTime()) && os.SameFile(a, b)
}

func cachedTimingSource(entry timingSourceEntry) (*parser.SessionTimingEvidence, bool) {
	timingSources.Lock()
	defer timingSources.Unlock()
	for _, cached := range timingSources.entries {
		if cached.path == entry.path && cached.agent == entry.agent && cached.limit == entry.limit && sameTimingSourceInfo(cached.info, entry.info) {
			return cached.evidence, true
		}
	}
	return nil, false
}

func timingSourceEvents(source *parser.SessionTimingEvidence) int {
	if source == nil {
		return 0
	}
	return len(source.Turns) + len(source.Model) + len(source.Calls)
}

func cacheTimingSource(entry timingSourceEntry) {
	timingSources.Lock()
	defer timingSources.Unlock()
	// Replace older snapshots of this source, rather than retaining a copy on
	// every append or racing miss. FIFO remains bounded by entries and events.
	entries := make([]timingSourceEntry, 0, 16)
	events := timingSourceEvents(entry.evidence)
	for _, cached := range timingSources.entries {
		if cached.path == entry.path && cached.agent == entry.agent {
			continue
		}
		entries = append(entries, cached)
		events += timingSourceEvents(cached.evidence)
	}
	for len(entries) > 0 && (len(entries) >= 16 || events > 200000) {
		events -= timingSourceEvents(entries[0].evidence)
		entries[0] = timingSourceEntry{}
		entries = entries[1:]
	}
	timingSources.entries = append(entries, entry)
}
