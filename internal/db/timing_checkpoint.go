package db

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding"
	"encoding/hex"
	"encoding/json/v2"
	"hash"
	"io"
	"os"
	"path/filepath"
	"strings"

	"go.kenn.io/agentsview/internal/parser"
)

// Increment when the reducer semantics or encoded continuation state changes.
// Evidence versioning is independent: invalid cursors can always be rebuilt.
const timingCheckpointVersion = 1

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
	copy, _ := newAlignedTimingHash(h.state())
	_, _ = copy.h.Write(h.tail[:h.n])
	return hex.EncodeToString(copy.h.Sum(nil))
}

const loadTimingCheckpointSQL = `SELECT c.version,c.file_path,c.offset,c.hash_state,c.cursor,c.file_size
 FROM session_timing_checkpoints c JOIN session_timing_evidence e ON e.session_id=c.session_id
 WHERE c.session_id=? AND c.agent=? AND c.source_id=? AND e.version=?
 AND e.agent=c.agent AND e.source_id=c.source_id AND e.file_size=c.file_size AND e.file_hash=c.file_hash
 AND c.version=? AND c.file_path=? AND c.file_size<=?
 AND c.offset>0 AND c.offset<=c.file_size
 AND length(CAST(c.cursor AS BLOB))<=? AND length(CAST(c.hash_state AS BLOB)) BETWEEN 1 AND 4096`

func (db *DB) loadTimingCheckpoint(ctx context.Context, s *Session) *timingCaptureCheckpoint {
	if s.FilePath == nil || s.FileSize == nil {
		return nil
	}
	var version int
	var cp timingCaptureCheckpoint
	var cursor []byte
	var importedSize int64
	// Filter incompatible and oversized rows in SQLite before the driver copies
	// their payloads into Go memory. Cursor limits count bytes, not characters.
	err := db.getReader().QueryRowContext(ctx, loadTimingCheckpointSQL, s.ID, s.Agent, timingSourceID(s), timingEvidenceVersion, timingCheckpointVersion, *s.FilePath, *s.FileSize, maxArchivedTimingBytes).Scan(&version, &cp.Path, &cp.Offset, &cp.HashState, &cursor, &importedSize)
	if err != nil || version != timingCheckpointVersion || s.FilePath == nil || cp.Path != *s.FilePath || s.FileSize == nil || cp.Offset <= 0 || cp.Offset > *s.FileSize || importedSize > *s.FileSize || len(cursor) > maxArchivedTimingBytes || len(cp.HashState) > 4096 {
		return nil
	}
	var state parser.SessionTimingState
	if json.Unmarshal(cursor, &state) != nil || !state.Valid() || state.Evidence.SourceID != timingSourceID(s) {
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
	var savedHash []byte
	observe := func(line []byte, complete bool) error {
		if !complete {
			savedHash = h.state()
		}
		h.Write(line)
		return ctx.Err()
	}
	evidence, next, consumed, err := parser.ReadSessionTimingChunk(ctx, counter, s.Agent, path, state, observe)
	if err != nil || ctx.Err() != nil {
		return nil, counter.n
	}
	after, err := file.Stat()
	if err != nil || !sameTimingSourceInfo(before, after) || evidence.SourceID != timingSourceID(s) {
		return nil, counter.n
	}
	if s.FileHash != nil && *s.FileHash != "" && h.sum() != *s.FileHash {
		return nil, counter.n
	}
	record := &timingEvidenceRecord{Evidence: evidence, Size: *s.FileSize, Agent: s.Agent, SourceID: timingSourceID(s), SourceBytesRead: counter.n, Resumed: prior != nil}
	if s.FileMtime != nil {
		record.Mtime = *s.FileMtime
	}
	if s.FileHash != nil {
		record.Hash = *s.FileHash
	}
	if savedHash == nil {
		savedHash = h.state()
	}
	if safeOffset := offset + consumed; safeOffset > 0 && next.Evidence.SourceID == record.SourceID {
		record.Checkpoint = &timingCaptureCheckpoint{Path: path, Offset: safeOffset, HashState: savedHash, State: next}
	}
	return record, counter.n
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
		record := &timingEvidenceRecord{Evidence: evidence, Size: *s.FileSize, Agent: s.Agent, SourceID: timingSourceID(s), SourceBytesRead: *s.FileSize}
		if s.FileMtime != nil {
			record.Mtime = *s.FileMtime
		}
		if s.FileHash != nil {
			record.Hash = *s.FileHash
		}
		return record
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
		if record, n := scanTimingCapture(ctx, s, prior); record != nil {
			return record
		} else {
			attempted = n
		}
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
	if cp != nil {
		cursor, err := json.Marshal(cp.State)
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
