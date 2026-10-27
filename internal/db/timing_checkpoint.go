package db

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding"
	"encoding/hex"
	"encoding/json/v2"
	"errors"
	"hash"
	"io"
	"os"
	"path/filepath"
	"strings"

	"go.kenn.io/agentsview/internal/parser"
)

// Increment when the reducer semantics or encoded continuation state changes.
// Evidence versioning is independent: invalid cursors can always be rebuilt.
// Version 2 stops repeating closed evidence inside the cursor.
const timingCheckpointVersion = 2

// timingCursor is the stored reducer state without its closed evidence. The
// counts select the closed prefix of the evidence row committed with it: EOF
// previews and open turns only ever append after that prefix.
type timingCursor struct {
	State parser.SessionTimingState
	Turns int
	Model int
	Calls int
}

// Checkpoints are disposable, local optimization state. Their hash state is
// block-aligned: no partial SHA block containing transcript bytes is persisted.
type timingCaptureCheckpoint struct {
	Path      string
	Offset    int64
	HashState []byte
	State     *parser.SessionTimingState
}

// alignedTimingHash holds incomplete SHA blocks only in memory. Restoring a
// checkpoint rereads at most 63 bytes before its complete JSONL boundary.
type alignedTimingHash struct {
	h    hash.Hash
	tail [64]byte
	n    int
}

func newAlignedTimingHash(state []byte) (*alignedTimingHash, error) {
	h := sha256.New()
	if len(state) > 0 {
		if err := h.(encoding.BinaryUnmarshaler).UnmarshalBinary(state); err != nil {
			return nil, err
		}
	}
	return &alignedTimingHash{h: h}, nil
}

func (h *alignedTimingHash) Write(p []byte) {
	if h.n > 0 {
		n := copy(h.tail[h.n:], p)
		h.n += n
		p = p[n:]
		if h.n == len(h.tail) {
			_, _ = h.h.Write(h.tail[:])
			h.n = 0
		}
	}
	if n := len(p) / 64 * 64; n > 0 {
		_, _ = h.h.Write(p[:n])
		p = p[n:]
	}
	if len(p) > 0 {
		h.n = copy(h.tail[:], p)
	}
}

func (h *alignedTimingHash) state() []byte {
	state, _ := h.h.(encoding.BinaryMarshaler).MarshalBinary()
	return state
}

func (h *alignedTimingHash) sum() string {
	clone, _ := newAlignedTimingHash(h.state())
	_, _ = clone.h.Write(h.tail[:h.n])
	return hex.EncodeToString(clone.h.Sum(nil))
}

const loadTimingCheckpointSQL = `SELECT c.version,c.file_path,c.offset,c.hash_state,c.cursor,c.file_size,e.evidence
 FROM session_timing_checkpoints c JOIN session_timing_evidence e ON e.session_id=c.session_id
 WHERE c.session_id=? AND c.agent=? AND c.source_id=? AND e.version=?
 AND e.agent=c.agent AND e.source_id=c.source_id AND e.file_size=c.file_size AND e.file_hash=c.file_hash
 AND c.version=? AND c.file_path=? AND c.file_size<=?
 AND c.offset>0 AND c.offset<=c.file_size
 AND length(CAST(c.cursor AS BLOB))<=? AND length(CAST(c.hash_state AS BLOB)) BETWEEN 1 AND 4096
 AND length(CAST(e.evidence AS BLOB)) BETWEEN 1 AND ?`

func (db *DB) loadTimingCheckpoint(ctx context.Context, s *Session) *timingCaptureCheckpoint {
	if s.FilePath == nil || s.FileSize == nil {
		return nil
	}
	var version int
	var cp timingCaptureCheckpoint
	var cursor, payload []byte
	var importedSize int64
	// Filter incompatible and oversized rows in SQLite before the driver copies
	// their payloads into Go memory. Limits count bytes, not characters. A
	// rejected evidence row has an empty payload and never resumes.
	err := db.getReader().QueryRowContext(ctx, loadTimingCheckpointSQL, s.ID, s.Agent, timingSourceID(s), timingEvidenceVersion, timingCheckpointVersion, *s.FilePath, *s.FileSize, maxArchivedTimingBytes, maxArchivedTimingBytes).Scan(&version, &cp.Path, &cp.Offset, &cp.HashState, &cursor, &importedSize, &payload)
	if err != nil || version != timingCheckpointVersion || s.FilePath == nil || cp.Path != *s.FilePath || s.FileSize == nil || cp.Offset <= 0 || cp.Offset > *s.FileSize || importedSize > *s.FileSize || len(cursor) > maxArchivedTimingBytes || len(payload) > maxArchivedTimingBytes || len(cp.HashState) > 4096 {
		return nil
	}
	var stored timingCursor
	var evidence parser.SessionTimingEvidence
	if json.Unmarshal(cursor, &stored) != nil || json.Unmarshal(payload, &evidence) != nil {
		return nil
	}
	if stored.Turns < 0 || stored.Model < 0 || stored.Calls < 0 || stored.Turns > len(evidence.Turns) || stored.Model > len(evidence.Model) || stored.Calls > len(evidence.Calls) {
		return nil
	}
	state := stored.State
	state.Evidence = parser.SessionTimingEvidence{
		SourceID: evidence.SourceID,
		Turns:    evidence.Turns[:stored.Turns:stored.Turns],
		Model:    evidence.Model[:stored.Model:stored.Model],
		Calls:    evidence.Calls[:stored.Calls:stored.Calls],
	}
	if !state.Valid() || state.Evidence.SourceID != timingSourceID(s) {
		return nil
	}
	cp.State = &state
	return &cp
}

type timingCountingReader struct {
	r io.Reader
	n int64
}

func (r *timingCountingReader) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	r.n += int64(n)
	return n, err
}

// scanTimingCapture verifies the resumed digest against the canonical ingestion
// hash. A rewritten committed prefix therefore cannot reuse stale reducer state.
// A cold scan may return a Rejected record, but only for a failure that is
// deterministic for the imported bytes and survives later appends.
func scanTimingCapture(ctx context.Context, s *Session, prior *timingCaptureCheckpoint) (*timingEvidenceRecord, int64) {
	if ctx.Err() != nil || s.FilePath == nil || !filepath.IsAbs(*s.FilePath) || s.FileSize == nil || *s.FileSize <= 0 {
		return nil, 0
	}
	path := *s.FilePath
	if !strings.HasSuffix(path, ".jsonl") {
		return nil, 0
	}
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() || *s.FileSize > info.Size() {
		return nil, 0
	}
	file, err := openTimingSourceFile(path)
	if err != nil {
		return nil, 0
	}
	defer file.Close()
	before, err := file.Stat()
	if err != nil || !before.Mode().IsRegular() || !sameTimingSourceInfo(info, before) {
		return nil, 0
	}
	offset := int64(0)
	var state *parser.SessionTimingState
	var hashState []byte
	if prior != nil {
		// Without the new parser-verified full hash there is no proof that the
		// old prefix still belongs to this source. Cold capture remains available.
		if s.FileHash == nil || *s.FileHash == "" {
			return nil, 0
		}
		offset, state, hashState = prior.Offset, prior.State, prior.HashState
	}
	if *s.FileSize-offset > maxTimingSourceBytes {
		if prior == nil {
			// Sources only grow, so a cold source past the bound stays there.
			return rejectedTimingRecord(s), 0
		}
		return nil, 0
	}
	h, err := newAlignedTimingHash(hashState)
	if err != nil {
		return nil, 0
	}
	aligned := offset / 64 * 64
	if _, err = file.Seek(aligned, io.SeekStart); err != nil {
		return nil, 0
	}
	counter := &timingCountingReader{r: &io.LimitedReader{R: file, N: *s.FileSize - aligned}}
	if offset > aligned {
		var prefix [64]byte
		if _, err = io.ReadFull(counter, prefix[:offset-aligned]); err != nil {
			return nil, counter.n
		}
		h.Write(prefix[:offset-aligned])
	}
	// Save the hash at the last complete-line boundary: the start of the first
	// piece of a record whose terminator has not been observed yet.
	var savedHash []byte
	partial := false
	observe := func(p []byte, complete bool) error {
		if !complete && !partial {
			savedHash, partial = h.state(), true
		}
		h.Write(p)
		if complete {
			partial = false
		}
		return ctx.Err()
	}
	evidence, next, consumed, err := parser.ReadSessionTimingChunk(ctx, counter, s.Agent, path, state, observe)
	if errors.Is(err, parser.ErrTimingSourceUnsupported) && prior == nil && ctx.Err() == nil {
		// The reducer stops early. Prove the imported bytes caused it before
		// recording a rejection that later appends will carry forward.
		if timingPrefixMatches(file, before, *s.FileSize, s.FileHash) {
			return rejectedTimingRecord(s), counter.n + *s.FileSize
		}
		return nil, counter.n
	}
	if err != nil || ctx.Err() != nil {
		return nil, counter.n
	}
	after, err := file.Stat()
	if err != nil || !sameTimingSourceInfo(before, after) {
		return nil, counter.n
	}
	if s.FileHash != nil && *s.FileHash != "" && h.sum() != *s.FileHash {
		return nil, counter.n
	}
	if evidence.SourceID != timingSourceID(s) {
		// A conflicting identity is fixed by the source header or its first
		// session record. An empty identity may still appear later.
		if prior == nil && evidence.SourceID != "" && s.FileHash != nil && *s.FileHash != "" {
			return rejectedTimingRecord(s), counter.n
		}
		return nil, counter.n
	}
	record := bindTimingRecord(&timingEvidenceRecord{Evidence: evidence, SourceBytesRead: counter.n, Resumed: prior != nil}, s)
	if !partial {
		savedHash = h.state()
	}
	if safeOffset := offset + consumed; safeOffset > 0 && next.Evidence.SourceID == record.SourceID {
		record.Checkpoint = &timingCaptureCheckpoint{Path: path, Offset: safeOffset, HashState: savedHash, State: next}
	}
	return record, counter.n
}

// timingPrefixMatches hashes the imported prefix of an open source and checks
// that the descriptor did not change while it was read.
func timingPrefixMatches(file *os.File, before os.FileInfo, size int64, want *string) bool {
	if want == nil || *want == "" {
		return false
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return false
	}
	digest := sha256.New()
	if n, err := io.Copy(digest, io.LimitReader(file, size)); err != nil || n != size {
		return false
	}
	after, err := file.Stat()
	return err == nil && sameTimingSourceInfo(before, after) && hex.EncodeToString(digest.Sum(nil)) == *want
}

func (db *DB) captureTimingSource(ctx context.Context, s *Session) *timingEvidenceRecord {
	if s.FilePath == nil || s.FileSize == nil || *s.FileSize <= 0 {
		return nil
	}
	if strings.HasSuffix(*s.FilePath, ".jsonl.zst") {
		// Archived zstd streams are not byte-seekable. Representation changes and
		// compressed updates retain the bounded streaming full-scan path.
		evidence := readTimingSource(ctx, s)
		if evidence == nil {
			return nil
		}
		return bindTimingRecord(&timingEvidenceRecord{Evidence: evidence, SourceBytesRead: *s.FileSize}, s)
	}
	if !strings.HasSuffix(*s.FilePath, ".jsonl") {
		return nil
	}
	select {
	case timingSourceReads <- struct{}{}:
	case <-ctx.Done():
		return nil
	}
	defer func() { <-timingSourceReads }()
	var attempted int64
	if prior := db.loadTimingCheckpoint(ctx, s); prior != nil {
		record, n := scanTimingCapture(ctx, s, prior)
		if record != nil {
			return record
		}
		attempted = n
	}
	if ctx.Err() != nil {
		return nil
	}
	record, _ := scanTimingCapture(ctx, s, nil)
	if record != nil {
		record.SourceBytesRead += attempted
	}
	return record
}

func writeTimingCheckpoint(ctx context.Context, exec func(context.Context, string, ...any) (sql.Result, error), id string, r *timingEvidenceRecord) error {
	cp := r.Checkpoint
	if cp != nil && cp.State != nil {
		stored := timingCursor{State: *cp.State, Turns: len(cp.State.Evidence.Turns), Model: len(cp.State.Evidence.Model), Calls: len(cp.State.Evidence.Calls)}
		// Closed evidence is already in the committed evidence row.
		stored.State.Evidence = parser.SessionTimingEvidence{SourceID: cp.State.Evidence.SourceID}
		cursor, err := json.Marshal(stored)
		if err != nil {
			return err
		}
		if len(cursor) <= maxArchivedTimingBytes {
			_, err = exec(ctx, `INSERT INTO session_timing_checkpoints(session_id,version,agent,source_id,file_path,file_size,file_hash,offset,hash_state,cursor)
 VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(session_id) DO UPDATE SET version=excluded.version,agent=excluded.agent,source_id=excluded.source_id,
 file_path=excluded.file_path,file_size=excluded.file_size,file_hash=excluded.file_hash,offset=excluded.offset,hash_state=excluded.hash_state,cursor=excluded.cursor`, id, timingCheckpointVersion, r.Agent, r.SourceID, cp.Path, r.Size, r.Hash, cp.Offset, cp.HashState, string(cursor))
			return err
		}
	}
	_, err := exec(ctx, "DELETE FROM session_timing_checkpoints WHERE session_id=?", id)
	return err
}
