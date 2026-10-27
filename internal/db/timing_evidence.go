package db

import (
	"context"
	"database/sql"
	"encoding/json/v2"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"go.kenn.io/agentsview/internal/parser"
)

// Timing evidence is local archive metadata, like parser checkpoints. It contains
// only source identifiers and timestamps, never transcript or tool bodies.
const (
	timingEvidenceVersion  = 1
	maxArchivedTimingBytes = 32 << 20
)

type timingEvidenceRecord struct {
	Checkpoint      *timingCaptureCheckpoint
	SourceBytesRead int64
	Resumed         bool
	Evidence        *parser.SessionTimingEvidence
	Size            int64
	Mtime           int64
	Hash            string
	Agent           string
	SourceID        string

	// Rejected records a deterministic decision that this source binding cannot
	// supply the session's timing. It is stored as an empty evidence payload and
	// stops later reads and appends from rescanning the same source.
	Rejected bool
}

func timingSourceID(s *Session) string {
	if s.SourceSessionID != "" {
		return s.SourceSessionID
	}
	return strings.TrimPrefix(s.ID, "codex:")
}

// bindTimingRecord ties a record to the session's imported source snapshot.
func bindTimingRecord(r *timingEvidenceRecord, s *Session) *timingEvidenceRecord {
	r.Agent, r.SourceID = s.Agent, timingSourceID(s)
	r.Size, r.Mtime, r.Hash = 0, 0, ""
	if s.FileSize != nil {
		r.Size = *s.FileSize
	}
	if s.FileMtime != nil {
		r.Mtime = *s.FileMtime
	}
	if s.FileHash != nil {
		r.Hash = *s.FileHash
	}
	return r
}

func rejectedTimingRecord(s *Session) *timingEvidenceRecord {
	return bindTimingRecord(&timingEvidenceRecord{Rejected: true}, s)
}

// ExcludeSessionTimingEvidence records that whole-source timing does not
// belong to this session, for example because its file also holds another
// session's records. The exclusion follows the session through every writer.
func ExcludeSessionTimingEvidence(s Session) Session {
	if s.Agent == "codex" || s.Agent == "claude" {
		s.TimingCapture = rejectedTimingRecord(&s)
	}
	return s
}

// claudeSubagentTranscript identifies subagents/agent-*.jsonl files. Every
// record is a sidechain, so they never carry main-session timing.
func claudeSubagentTranscript(s *Session) bool {
	return s.Agent == "claude" && s.FilePath != nil &&
		filepath.Base(filepath.Dir(*s.FilePath)) == "subagents"
}

// Capture before taking the writer lock. Verified plaintext checkpoints resume
// at the complete-line boundary; cold and compressed scans remain bounded.
// Rejections stay attached and are rebound to the current source snapshot.
func (db *DB) prepareTimingEvidence(ctx context.Context, s Session) Session {
	if s.TimingCapture != nil && s.TimingCapture.Rejected {
		s.TimingCapture = rejectedTimingRecord(&s)
		return s
	}
	if s.TimingCapture != nil && s.TimingCapture.Evidence != nil && timingEvidenceMatchesSession(s.TimingCapture, &s) {
		return s
	}
	s.TimingCapture = nil
	if s.Agent != "codex" && s.Agent != "claude" {
		return s
	}
	if claudeSubagentTranscript(&s) {
		s.TimingCapture = rejectedTimingRecord(&s)
		return s
	}
	record := db.captureTimingSource(ctx, &s)
	if record != nil {
		s.TimingCapture = record
	}

	return s
}

// PrepareSessionTimingEvidence captures source timing before a non-atomic full
// ingestion starts. The caller publishes this same capture only after its
// normalized content write succeeds; no second source read is needed.
func (db *DB) PrepareSessionTimingEvidence(ctx context.Context, s Session) Session {
	return db.prepareTimingEvidence(ctx, s)
}

// PublishSessionTimingEvidence completes a full ingestion's timing publication.
// Pending session upserts must not publish this evidence before their separately
// committed content, and a concurrent source replacement must not receive it.
func (db *DB) PublishSessionTimingEvidence(ctx context.Context, s Session) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.TimingCapture == nil {
		return nil
	}
	if err := db.requireWritable(); err != nil {
		return err
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	tx, err := db.getWriter().BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("beginning timing publication: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	current := Session{ID: s.ID}
	var deletedAt *string
	err = tx.QueryRowContext(ctx, `SELECT agent, source_session_id, file_size, file_mtime, file_hash, deleted_at
 FROM sessions WHERE id=?`, s.ID).Scan(&current.Agent, &current.SourceSessionID, &current.FileSize, &current.FileMtime, &current.FileHash, &deletedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrSessionChanged
	}
	if err != nil {
		return fmt.Errorf("checking timing publication source: %w", err)
	}
	if deletedAt != nil || !timingEvidenceMatchesSession(s.TimingCapture, &current) {
		return ErrSessionChanged
	}
	if err := writeTimingEvidence(ctx, tx.ExecContext, s.ID, s.TimingCapture); err != nil {
		return fmt.Errorf("publishing timing evidence: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing timing evidence: %w", err)
	}
	return nil
}

func timingEvidenceMatchesSession(r *timingEvidenceRecord, s *Session) bool {
	currentHash := ""
	if s.FileHash != nil {
		currentHash = *s.FileHash
	}
	currentSize := int64(0)
	if s.FileSize != nil {
		currentSize = *s.FileSize
	}
	currentMtime := int64(0)
	if s.FileMtime != nil {
		currentMtime = *s.FileMtime
	}
	return r.Agent == s.Agent && r.SourceID == timingSourceID(s) && r.Size == currentSize && r.Mtime == currentMtime && r.Hash == currentHash
}

func writeTimingEvidence(ctx context.Context, exec func(context.Context, string, ...any) (sql.Result, error), id string, r *timingEvidenceRecord) error {
	if r == nil {
		return nil
	}
	var payload []byte
	rejected := r.Rejected
	if !rejected {
		var err error
		payload, err = json.Marshal(r.Evidence)
		if err != nil {
			return err
		}
		// Optional timing metadata must not prevent archiving the transcript.
		// Evidence only grows with appends, so record the limit as a rejection.
		if len(payload) > maxArchivedTimingBytes {
			payload, rejected = nil, true
		}
	}
	_, err := exec(ctx, `INSERT INTO session_timing_evidence(session_id, version, agent, source_id, file_size, file_mtime, file_hash, evidence)
 VALUES(?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(session_id) DO UPDATE SET
 version=excluded.version, agent=excluded.agent, source_id=excluded.source_id,
 file_size=excluded.file_size, file_mtime=excluded.file_mtime, file_hash=excluded.file_hash, evidence=excluded.evidence`,
		id, timingEvidenceVersion, r.Agent, r.SourceID, r.Size, r.Mtime, r.Hash, string(payload))
	if err != nil {
		return err
	}
	if rejected {
		_, err = exec(ctx, "DELETE FROM session_timing_checkpoints WHERE session_id=?", id)
		return err
	}
	return writeTimingCheckpoint(ctx, exec, id, r)
}

func (db *DB) storedTimingEvidence(ctx context.Context, s *Session) (*parser.SessionTimingEvidence, bool, error) {
	return storedTimingEvidenceWithQuerier(ctx, db.getReader(), s)
}

// storedTimingEvidenceWithQuerier returns matching saved evidence. The bool
// reports a saved rejection for the current binding, which suppresses source
// fallbacks.
func storedTimingEvidenceWithQuerier(ctx context.Context, q recallQueryRower, s *Session) (*parser.SessionTimingEvidence, bool, error) {
	var version int
	var agent, sourceID, hash, payload string
	var size, mtime int64
	err := q.QueryRowContext(ctx, `SELECT version, agent, source_id, file_size, file_mtime, file_hash, evidence FROM session_timing_evidence WHERE session_id=?`, s.ID).Scan(&version, &agent, &sourceID, &size, &mtime, &hash, &payload)
	if err == sql.ErrNoRows {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	if version != timingEvidenceVersion || !timingEvidenceMatchesSession(&timingEvidenceRecord{Agent: agent, SourceID: sourceID, Size: size, Mtime: mtime, Hash: hash}, s) {
		return nil, false, nil
	}
	if payload == "" {
		return nil, true, nil
	}
	if len(payload) > maxArchivedTimingBytes {
		return nil, false, errors.New("timing evidence exceeds archive limit")
	}
	var evidence parser.SessionTimingEvidence
	if err := json.Unmarshal([]byte(payload), &evidence); err != nil {
		return nil, false, fmt.Errorf("decode archived timing evidence: %w", err)
	}
	if evidence.SourceID != sourceID || timingSourceEvents(&evidence) > 100000 {
		return nil, false, errors.New("invalid archived timing evidence")
	}
	return &evidence, false, nil
}

// Saved timing states for an incremental append, without loading payloads.
const (
	incrementalTimingNone = iota
	incrementalTimingEvidence
	incrementalTimingRejected
)

func (db *DB) prepareIncrementalTiming(ctx context.Context, id string, update IncrementalSessionUpdate) (IncrementalSessionUpdate, error) {
	update.TimingCapture = nil
	// One indexed metadata read, without transcript or curation hydration.
	s := Session{ID: id}
	var prior timingEvidenceRecord
	var saved int
	err := db.getReader().QueryRowContext(ctx, `SELECT s.agent, s.source_session_id, s.file_path, s.file_size, s.file_mtime, s.file_hash,
 COALESCE(e.agent,''), COALESCE(e.source_id,''), COALESCE(e.file_size,0), COALESCE(e.file_mtime,0), COALESCE(e.file_hash,''),
 CASE WHEN e.session_id IS NULL OR e.version<>? THEN ? WHEN e.evidence='' THEN ? ELSE ? END
 FROM sessions s LEFT JOIN session_timing_evidence e ON e.session_id=s.id
 WHERE s.id=? AND s.agent IN ('codex','claude')`,
		timingEvidenceVersion, incrementalTimingNone, incrementalTimingRejected, incrementalTimingEvidence, id,
	).Scan(&s.Agent, &s.SourceSessionID, &s.FilePath, &s.FileSize, &s.FileMtime, &s.FileHash,
		&prior.Agent, &prior.SourceID, &prior.Size, &prior.Mtime, &prior.Hash, &saved)
	if err == sql.ErrNoRows {
		return update, nil
	}
	if err != nil {
		return update, err
	}
	// A full replacement may advance the imported source while optional timing
	// capture is unavailable, leaving older evidence behind. Validate against
	// the pre-append binding before rebinding any saved decision to the tail.
	if !timingEvidenceMatchesSession(&prior, &s) {
		saved = incrementalTimingNone
	}
	s.FileSize, s.FileMtime = &update.FileSize, &update.FileMtime
	if update.FileHash != nil {
		s.FileHash = update.FileHash
	}
	switch {
	case saved == incrementalTimingRejected:
		// Every rejection reason survives an append, so carry it forward
		// without reading the source again. Rewrites take the full path.
		update.TimingCapture = rejectedTimingRecord(&s)
		return update, ctx.Err()
	case saved == incrementalTimingNone && s.Agent == "claude":
		// Only a full Claude parse knows whether the file holds records of
		// another session. Leave capture to it; reads stay conservative.
		return update, ctx.Err()
	}
	prepared := db.prepareTimingEvidence(ctx, s)
	update.TimingCapture = prepared.TimingCapture
	return update, ctx.Err()
}

// TimingCoverage reports source activity evidence separately from archived
// execution events. It is not a claim that every model or
// tool event was logged. Unattributed time stays explicit even with stored data.
type TimingCoverage struct {
	Evidence        string `json:"evidence"` // persisted, source, unavailable, unsupported
	Version         int    `json:"version"`
	TurnBoundaries  string `json:"turn_boundaries"` // recorded or estimated
	RecordedTurns   int    `json:"recorded_turns"`
	ModelIntervals  int    `json:"model_intervals"`
	ToolCalls       int    `json:"tool_calls"`
	TimedToolCalls  int    `json:"timed_tool_calls"`
	ExecutionCalls  int    `json:"execution_calls"`
	CallerWaitCalls int    `json:"caller_wait_calls"`
	SubagentCalls   int    `json:"subagent_calls"`
	UnattributedMs  int64  `json:"unattributed_ms"`
}

func timingCoverage(out *SessionTiming, s *Session, e *parser.SessionTimingEvidence, persisted bool) {
	c := TimingCoverage{Evidence: "unavailable", TurnBoundaries: "estimated", ToolCalls: out.ToolCallCount, UnattributedMs: out.ActivityTotals.UnattributedMs}
	if s.Agent != "codex" && s.Agent != "claude" {
		c.Evidence = "unsupported"
	}
	if e != nil {
		c.Evidence = "source"
		if persisted {
			c.Evidence = "persisted"
		}
		c.Version = timingEvidenceVersion
		c.RecordedTurns = len(e.Turns)
		c.ModelIntervals = len(e.Model)
	}
	if out.TimingSource == "source" {
		c.TurnBoundaries = "recorded"
	}
	for _, t := range out.Turns {
		for _, call := range t.Calls {
			if call.DurationMs == nil {
				continue
			}
			switch call.DurationSource {
			case "execution":
				c.ExecutionCalls++
				c.TimedToolCalls++
			case "caller_wait":
				c.CallerWaitCalls++
				c.TimedToolCalls++
			case "subagent":
				c.SubagentCalls++
				c.TimedToolCalls++
			}
		}
	}
	out.Coverage = c
}
