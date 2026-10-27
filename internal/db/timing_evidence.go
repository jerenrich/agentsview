package db

import (
	"context"
	"database/sql"
	"encoding/json/v2"
	"fmt"
	"strings"
	"time"

	"go.kenn.io/agentsview/internal/parser"
)

// Timing evidence is local archive metadata, like parser checkpoints. It contains
// only source identifiers and timestamps, never transcript or tool bodies.
const timingEvidenceVersion = 1
const maxArchivedTimingBytes = 32 << 20

type timingEvidenceRecord struct {
	Evidence *parser.SessionTimingEvidence
	Size     int64
	Mtime    int64
	Hash     string
	Agent    string
	SourceID string
}

func timingSourceID(s *Session) string {
	if s.SourceSessionID != "" {
		return s.SourceSessionID
	}
	return strings.TrimPrefix(s.ID, "codex:")
}

// Capture before taking the writer lock. Repeated scans are bounded by the
// existing source cache and scan semaphore, and only changed writes reach here.
func prepareTimingEvidence(ctx context.Context, s Session) Session {
	s.TimingCapture = nil
	if s.Agent != "codex" && s.Agent != "claude" {
		return s
	}
	evidence := readTimingSource(ctx, &s)
	if evidence == nil {
		// Never replace retained evidence on an unreadable source.
		return s
	}
	r := &timingEvidenceRecord{Evidence: evidence, Agent: s.Agent, SourceID: timingSourceID(&s)}
	if s.FileSize != nil {
		r.Size = *s.FileSize
	}
	if s.FileMtime != nil {
		r.Mtime = *s.FileMtime
	}
	if s.FileHash != nil {
		r.Hash = *s.FileHash
	}
	s.TimingCapture = r
	return s
}

func writeTimingEvidence(ctx context.Context, exec func(context.Context, string, ...any) (sql.Result, error), id string, r *timingEvidenceRecord) error {
	if r == nil {
		return nil
	}
	payload, err := json.Marshal(r.Evidence)
	if err != nil {
		return err
	}
	if len(payload) > maxArchivedTimingBytes {
		// Optional timing metadata must not prevent archiving the transcript.
		return nil
	}
	_, err = exec(ctx, `INSERT INTO session_timing_evidence(session_id, version, agent, source_id, file_size, file_mtime, file_hash, evidence)
 VALUES(?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(session_id) DO UPDATE SET
 version=excluded.version, agent=excluded.agent, source_id=excluded.source_id,
 file_size=excluded.file_size, file_mtime=excluded.file_mtime, file_hash=excluded.file_hash, evidence=excluded.evidence`,
		id, timingEvidenceVersion, r.Agent, r.SourceID, r.Size, r.Mtime, r.Hash, string(payload))
	return err
}

func (db *DB) storedTimingEvidence(ctx context.Context, s *Session) (*parser.SessionTimingEvidence, error) {
	var version int
	var agent, sourceID, hash, payload string
	var size, mtime int64
	err := db.getReader().QueryRowContext(ctx, `SELECT version, agent, source_id, file_size, file_mtime, file_hash, evidence FROM session_timing_evidence WHERE session_id=?`, s.ID).Scan(&version, &agent, &sourceID, &size, &mtime, &hash, &payload)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
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
	if version != timingEvidenceVersion || agent != s.Agent || sourceID != timingSourceID(s) || size != currentSize || mtime != currentMtime || hash != currentHash {
		return nil, nil
	}
	if len(payload) > maxArchivedTimingBytes {
		return nil, fmt.Errorf("timing evidence exceeds archive limit")
	}
	var evidence parser.SessionTimingEvidence
	if err := json.Unmarshal([]byte(payload), &evidence); err != nil {
		return nil, fmt.Errorf("decode archived timing evidence: %w", err)
	}
	if evidence.SourceID != sourceID || timingSourceEvents(&evidence) > 100000 {
		return nil, fmt.Errorf("invalid archived timing evidence")
	}
	return &evidence, nil
}

func (db *DB) prepareIncrementalTiming(ctx context.Context, id string, update IncrementalSessionUpdate) (IncrementalSessionUpdate, error) {
	update.TimingCapture = nil
	// One indexed metadata read, without transcript or curation hydration.
	s := Session{ID: id, FileSize: &update.FileSize, FileMtime: &update.FileMtime}
	err := db.getReader().QueryRowContext(ctx, `SELECT agent, source_session_id, file_path, file_hash
 FROM sessions WHERE id=? AND agent IN ('codex','claude')`, id).Scan(&s.Agent, &s.SourceSessionID, &s.FilePath, &s.FileHash)
	if err == sql.ErrNoRows {
		return update, nil
	}
	if err != nil {
		return update, err
	}
	if update.FileHash != nil {
		s.FileHash = update.FileHash
	}
	prepared := prepareTimingEvidence(ctx, s)
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

func (db *DB) assembleArchivedTiming(ctx context.Context, s *Session, turns []TurnRow, calls []CallRow) (*SessionTiming, error) {
	if s.DeletedAt != nil {
		return nil, nil
	}
	e, err := db.storedTimingEvidence(ctx, s)
	if err != nil {
		return nil, err
	}
	persisted := e != nil
	if e == nil {
		e = readTimingSource(ctx, s)
	} // Legacy archives can still be inspected before their next ingestion.
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	out := AssembleTiming(s, turns, calls, time.Now().UTC(), e)
	timingCoverage(out, s, e, persisted)
	return out, nil
}
