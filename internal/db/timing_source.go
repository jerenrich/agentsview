package db

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
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
	hash     string
	limit    int64
	info     os.FileInfo
	evidence *parser.SessionTimingEvidence
}

var timingSources struct {
	sync.Mutex
	entries []timingSourceEntry
}

// ChildSessionsFunc lists a session's direct children with source paths,
// including trashed children whose records still occupy the source file.
type ChildSessionsFunc func(ctx context.Context, parentID string) ([]Session, error)

func (db *DB) timingSourceChildren(ctx context.Context, parentID string) ([]Session, error) {
	rows, err := db.getReader().QueryContext(ctx, `SELECT file_path, relationship_type
 FROM sessions WHERE parent_session_id=? AND relationship_type='fork'`, parentID)
	if err != nil {
		return nil, err
	}
	return ScanTimingSourceChildren(rows)
}

// ScanTimingSourceChildren reads the minimal ownership projection shared by
// archive and mirror timing reads. It owns and closes rows.
func ScanTimingSourceChildren(rows *sql.Rows) ([]Session, error) {
	defer rows.Close()
	var children []Session
	for rows.Next() {
		var child Session
		if err := rows.Scan(&child.FilePath, &child.RelationshipType); err != nil {
			return nil, err
		}
		children = append(children, child)
	}
	return children, rows.Err()
}

// AssembleTimingContext enriches every store's timing with the same optional
// local source evidence. Sources are never modified. Missing, remote, replaced,
// forked, shared, oversized or malformed sources retain the archive-only result.
// Children identify Claude files that also hold a fork session's records.
func AssembleTimingContext(ctx context.Context, sess *Session, turns []TurnRow, calls []CallRow, now time.Time, children ChildSessionsFunc) (*SessionTiming, error) {
	if sess.DeletedAt != nil {
		return nil, nil
	}
	evidence, err := readOwnedTimingSource(ctx, sess, children)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return AssembleTiming(sess, turns, calls, now, timingEvidenceForLiveSource(sess, evidence)), nil
}

// Archived evidence remains readable without a log, but cannot prove that its
// process is still running. A replaced or not-yet-imported source stays bounded
// until ingestion commits its new snapshot.
func timingEvidenceForLiveSource(sess *Session, evidence *parser.SessionTimingEvidence) *parser.SessionTimingEvidence {
	if evidence == nil || !evidence.Open {
		return evidence
	}
	available := false
	if sess.FilePath != nil && sess.FileSize != nil && sess.FileMtime != nil {
		info, err := os.Stat(*sess.FilePath)
		available = err == nil && info.Mode().IsRegular() && info.Size() >= *sess.FileSize && info.ModTime().UnixNano() == *sess.FileMtime
	}
	if available {
		return evidence
	}
	copy := *evidence
	copy.Open = false
	return &copy
}

// readOwnedTimingSource reads source evidence only when the whole file can be
// attributed to sess. Without a saved ingestion decision, Claude forks,
// continuations, subagent transcripts, and parents of in-file forks are
// treated as shared: their files can hold another session's records.
func readOwnedTimingSource(ctx context.Context, sess *Session, children ChildSessionsFunc) (*parser.SessionTimingEvidence, error) {
	if sess.FilePath == nil || !filepath.IsAbs(*sess.FilePath) || sess.FileSize == nil || *sess.FileSize <= 0 {
		return nil, nil
	}
	if sess.Agent == "claude" {
		if claudeSubagentTranscript(sess) || sess.RelationshipType == string(parser.RelFork) || sess.RelationshipType == string(parser.RelContinuation) {
			return nil, nil
		}
		if children == nil {
			return nil, nil
		}
		kids, err := children(ctx, sess.ID)
		if err != nil {
			return nil, err
		}
		for _, kid := range kids {
			if kid.RelationshipType == string(parser.RelFork) && kid.FilePath != nil && *kid.FilePath == *sess.FilePath {
				return nil, nil
			}
		}
	}
	return readTimingSource(ctx, sess), nil
}

const (
	maxTimingSourceBytes      = 256 << 20
	maxTimingSourceCacheBytes = 32 << 20
)

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
	if sess.FileHash != nil {
		entry.hash = *sess.FileHash
	}
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
			file, err := openTimingSourceFile(path)
			if err != nil {
				return nil
			}
			defer file.Close()
			before, err := file.Stat()
			if err != nil || !sameTimingSourceInfo(before, info) {
				return nil
			}
			bounded := &io.LimitedReader{R: file, N: limit}
			digest := sha256.New()
			reader := io.TeeReader(bounded, digest)
			if strings.HasSuffix(path, ".jsonl.zst") {
				evidence, err = parser.ReadCompressedSessionTiming(ctx, reader, sess.Agent, path)
			} else {
				evidence, err = parser.ReadSessionTiming(ctx, reader, sess.Agent, path)
			}
			if err == nil {
				_, err = io.Copy(io.Discard, reader)
			}
			if err == nil && (bounded.N != 0 || entry.hash != "" && hex.EncodeToString(digest.Sum(nil)) != entry.hash) {
				return nil
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
		if cached.path == entry.path && cached.agent == entry.agent && cached.hash == entry.hash && cached.limit == entry.limit && sameTimingSourceInfo(cached.info, entry.info) {
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

// Include retained string data and slice capacity, not just event cardinality:
// a single oversized source or call ID can otherwise exhaust the cache budget.
// The fixed allowance covers entry, evidence, file-info, and slice headers;
// 16/32 are the interval/call element sizes on the largest supported word size.
func timingSourceRetainedBytes(entry timingSourceEntry) int64 {
	bytes := int64(512 + len(entry.path) + len(entry.agent) + len(entry.hash))
	if source := entry.evidence; source != nil {
		bytes += int64(len(source.SourceID))
		bytes += int64(cap(source.Turns)+cap(source.Model)) * 16
		bytes += int64(cap(source.Calls)) * 32
		for _, call := range source.Calls {
			bytes += int64(len(call.ID))
		}
	}
	return bytes
}

func cacheTimingSource(entry timingSourceEntry) {
	timingSources.Lock()
	defer timingSources.Unlock()
	// Replace older snapshots of this source, rather than retaining a copy on
	// every append or racing miss. FIFO remains bounded by entries, events,
	// and estimated retained bytes, including potentially large identifiers.
	entries := make([]timingSourceEntry, 0, 16)
	events, bytes, incoming := 0, int64(0), 0
	entryBytes := timingSourceRetainedBytes(entry)
	cacheable := entryBytes <= maxTimingSourceCacheBytes
	if cacheable {
		events, bytes, incoming = timingSourceEvents(entry.evidence), entryBytes, 1
	}
	for _, cached := range timingSources.entries {
		if cached.path == entry.path && cached.agent == entry.agent {
			continue
		}
		entries = append(entries, cached)
		events += timingSourceEvents(cached.evidence)
		bytes += timingSourceRetainedBytes(cached)
	}
	for len(entries) > 0 && (len(entries)+incoming > 16 || events > 200000 || bytes > maxTimingSourceCacheBytes) {
		events -= timingSourceEvents(entries[0].evidence)
		bytes -= timingSourceRetainedBytes(entries[0])
		entries[0] = timingSourceEntry{}
		entries = entries[1:]
	}
	if cacheable {
		entries = append(entries, entry)
	}
	timingSources.entries = entries
}
