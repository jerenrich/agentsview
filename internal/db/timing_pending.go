package db

import (
	"context"
	"database/sql"
	"encoding/json/v2"
	"errors"
	"strings"
)

// A full import publishes source metadata before its content transaction. Keep
// only the previous timing binding and bounds, never transcript bodies. Its
// revision determines whether that content is still the committed transcript.
type timingReadSnapshot struct {
	Exists         bool
	Revision       string
	Agent          string
	SourceID       string
	Relationship   string
	Path           *string
	Size, Mtime    *int64
	Hash           *string
	Started, Ended *string
	Termination    *string
}

func previousTimingReadSnapshot(ctx context.Context, queryRow func(context.Context, string, ...any) rowScanner, id string) (*timingReadSnapshot, error) {
	s := &timingReadSnapshot{}
	err := queryRow(ctx, `SELECT transcript_revision, agent, source_session_id, relationship_type,
 file_path, file_size, file_mtime, file_hash, started_at, ended_at, termination_status
 FROM sessions WHERE id=?`, id).Scan(&s.Revision, &s.Agent, &s.SourceID, &s.Relationship,
		&s.Path, &s.Size, &s.Mtime, &s.Hash, &s.Started, &s.Ended, &s.Termination)
	if errors.Is(err, sql.ErrNoRows) {
		return s, nil
	}
	s.Exists = err == nil
	if s.Exists && s.SourceID == "" {
		s.SourceID = strings.TrimPrefix(id, "codex:")
	}
	return s, err
}

func savePendingTimingRead(ctx context.Context, exec func(context.Context, string, ...any) (sql.Result, error), id string, snapshot *timingReadSnapshot) error {
	encoded, err := json.Marshal(snapshot)
	if err != nil {
		return err
	}
	// A retry must retain the first committed snapshot, not the metadata from
	// an earlier failed attempt. Atomic writes and publication clear this row.
	_, err = exec(ctx, `INSERT INTO session_timing_pending(session_id, snapshot)
 VALUES(?, ?) ON CONFLICT(session_id) DO NOTHING`, id, string(encoded))
	return err
}

func clearPendingTimingRead(ctx context.Context, exec func(context.Context, string, ...any) (sql.Result, error), id string) error {
	_, err := exec(ctx, "DELETE FROM session_timing_pending WHERE session_id=?", id)
	return err
}

func restorePendingTimingRead(ctx context.Context, q recallQueryRower, s *Session) (pending, committed bool, err error) {
	// Optional for archives opened read-only before this optimization existed.
	var exists bool
	if err := q.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM sqlite_master
 WHERE type='table' AND name='session_timing_pending')`).Scan(&exists); err != nil || !exists {
		return false, true, err
	}
	var payload string
	err = q.QueryRowContext(ctx, "SELECT snapshot FROM session_timing_pending WHERE session_id=?", s.ID).Scan(&payload)
	if errors.Is(err, sql.ErrNoRows) {
		return false, true, nil
	}
	if err != nil {
		return false, false, err
	}
	var saved timingReadSnapshot
	if err := json.Unmarshal([]byte(payload), &saved); err != nil {
		return true, false, err
	}
	if saved.Exists && s.TranscriptRevision != nil && saved.Revision == *s.TranscriptRevision {
		s.Agent, s.SourceSessionID, s.RelationshipType = saved.Agent, saved.SourceID, saved.Relationship
		s.FilePath, s.FileSize, s.FileMtime, s.FileHash = saved.Path, saved.Size, saved.Mtime, saved.Hash
		s.StartedAt, s.EndedAt, s.TerminationStatus = saved.Started, saved.Ended, saved.Termination
		return true, true, nil
	}
	if !saved.Exists {
		s.StartedAt, s.EndedAt = nil, nil
	}
	return true, false, nil
}

// New committed content can outlive a failed timing publication. Only its
// normalized timestamps, not pending source bounds or wall time, own this read.
func boundPendingTimingRead(s *Session, turns []TurnRow) {
	s.StartedAt, s.EndedAt = nil, nil
	var first, last int64
	for _, row := range turns {
		stamp, ok := timingTimestamp(row.Timestamp)
		if !ok {
			continue
		}
		if s.StartedAt == nil || stamp < first {
			first, s.StartedAt = stamp, new(row.Timestamp)
		}
		if s.EndedAt == nil || stamp > last {
			last, s.EndedAt = stamp, new(row.Timestamp)
		}
	}
}
