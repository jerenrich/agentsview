package parser

import (
	"bufio"
	"context"
	"errors"
	"io"
	"maps"
	"path/filepath"
	"slices"
	"strings"

	"go.kenn.io/agentsview/internal/ctxio"

	"github.com/tidwall/gjson"
)

// Timing evidence contains identifiers and timestamps only. Message and tool
// bodies are never retained by the timing reader.
type TimingInterval struct {
	Start int64
	End   int64
}

type TimingCall struct {
	ID string
	TimingInterval
}

type SessionTimingEvidence struct {
	SourceID string
	Turns    []TimingInterval
	Model    []TimingInterval
	Calls    []TimingCall
	// Open marks the final Turns entry as an unfinished EOF turn. Its end is
	// the last observed activity, not a recorded completion.
	Open bool `json:",omitzero"`
}

var ErrTimingSourceUnsupported = errors.New("unsupported timing source")

// ReadCompressedSessionTiming decodes the same JSONL evidence using the shared
// bounded streaming decoder. The caller bounds the imported compressed prefix.
func ReadCompressedSessionTiming(ctx context.Context, source io.Reader, agent string, sourcePath ...string) (*SessionTimingEvidence, error) {
	decoder, err := newZstdStreamDecoder(ctxio.Reader{Context: ctx, Reader: source})
	if err != nil {
		return nil, err
	}
	defer decoder.Close()
	return ReadSessionTiming(ctx, decoder, agent, sourcePath...)
}

// ReadSessionTiming reads a bounded, already-imported source snapshot. Codex
// lifecycle events close turns; Claude's last assistant or tool-result timestamp
// provides a conservative endpoint. Forked/replayed sources are left to archive timing
// rather than mixing the parent and child timelines. An optional source path
// resolves continuation page identities using the normal Codex parser rules.
func ReadSessionTiming(ctx context.Context, source io.Reader, agent string, sourcePath ...string) (*SessionTimingEvidence, error) {
	path := ""
	if len(sourcePath) > 0 {
		path = sourcePath[0]
	}
	evidence, _, _, err := ReadSessionTimingChunk(ctx, source, agent, path, nil, nil)
	return evidence, err
}

// Assistant snapshot deduplication retains IDs only, with the same bounded
// metadata budget as a persisted timing cursor.
const maxTimingClaudeMessageBytes = 32 << 20

// SessionTimingState is a resumable, content-free timing reducer. Evidence holds
// only closed turns; Snapshot adds provisional EOF bounds without closing state.
// Persist only at a complete JSONL boundary, never with a partial source record.
type SessionTimingState struct {
	Evidence           SessionTimingEvidence
	Starts             map[string]int64
	Calls              map[string]int64
	AmbiguousCalls     map[string]bool
	LastActivity       int64
	Prompt             int64
	LastClaudeActivity int64
	ClaudeComplete     bool
	// Queued prompts share the outstanding requests of their original prompt.
	ClaudeWaitStart int64
	// Claude writes cumulative and additive snapshots under one message ID.
	// Keep the current assistant run's observed calls across checkpoint resumes.
	ClaudeMessageID    string
	ClaudeMessageCalls map[string]bool
	ClaudeMessageBytes int
}

func (s *SessionTimingState) Clone() *SessionTimingState {
	if s == nil {
		return &SessionTimingState{Starts: map[string]int64{}, Calls: map[string]int64{}, AmbiguousCalls: map[string]bool{}}
	}
	out := *s
	out.Evidence.Turns = slices.Clone(s.Evidence.Turns)
	out.Evidence.Model = slices.Clone(s.Evidence.Model)
	out.Evidence.Calls = slices.Clone(s.Evidence.Calls)
	// JSON checkpoints encode empty slices as arrays. Normalize them so a
	// restarted reducer has the same evidence shape as a fresh scan.
	if len(out.Evidence.Turns) == 0 {
		out.Evidence.Turns = nil
	}
	if len(out.Evidence.Model) == 0 {
		out.Evidence.Model = nil
	}
	if len(out.Evidence.Calls) == 0 {
		out.Evidence.Calls = nil
	}
	out.Starts = maps.Clone(s.Starts)
	out.Calls = maps.Clone(s.Calls)
	out.AmbiguousCalls = maps.Clone(s.AmbiguousCalls)
	out.ClaudeMessageCalls = maps.Clone(s.ClaudeMessageCalls)
	if out.Starts == nil {
		out.Starts = map[string]int64{}
	}
	if out.Calls == nil {
		out.Calls = map[string]int64{}
	}
	if out.AmbiguousCalls == nil {
		out.AmbiguousCalls = map[string]bool{}
	}
	return &out
}

func (s *SessionTimingState) closeOpenTurns() {
	for id, start := range s.Starts {
		s.Evidence.Turns = append(s.Evidence.Turns, TimingInterval{start, max(start, s.LastActivity)})
		delete(s.Starts, id)
	}
}

func (s *SessionTimingState) closeClaude() {
	if s.Prompt > 0 && s.LastClaudeActivity >= s.Prompt {
		s.Evidence.Turns = append(s.Evidence.Turns, TimingInterval{s.Prompt, s.LastClaudeActivity})
	}
}

// Snapshot returns evidence without mutating unfinished turns or tool requests.
// An unfinished turn is appended last and flagged Open, including a Claude
// prompt that has no recorded response yet.
func (s *SessionTimingState) Snapshot(agent string) *SessionTimingEvidence {
	out := s.Evidence
	out.Turns = slices.Clone(out.Turns)
	out.Open = false
	if agent == string(AgentClaude) {
		if s.Prompt > 0 {
			out.Turns = append(out.Turns, TimingInterval{s.Prompt, max(s.Prompt, s.LastClaudeActivity)})
			out.Open = !s.ClaudeComplete
		}
	} else {
		for _, start := range s.Starts {
			out.Turns = append(out.Turns, TimingInterval{start, max(start, s.LastActivity)})
			out.Open = true
		}
	}
	return &out
}

// Valid bounds decoded checkpoint metadata before it can allocate more state.
func (s *SessionTimingState) Valid() bool {
	if s == nil {
		return false
	}
	n := len(s.Evidence.Model) + len(s.Evidence.Calls) + len(s.Evidence.Turns) + len(s.Starts) + len(s.Calls) + len(s.AmbiguousCalls) + len(s.ClaudeMessageCalls)
	if n > 100000 || len(s.Starts) > 1 {
		return false
	}
	messageBytes := len(s.ClaudeMessageID)
	for id := range s.ClaudeMessageCalls {
		messageBytes += len(id)
		if messageBytes > maxTimingClaudeMessageBytes {
			return false
		}
	}
	if messageBytes > maxTimingClaudeMessageBytes || messageBytes != s.ClaudeMessageBytes ||
		(s.ClaudeMessageID == "" && len(s.ClaudeMessageCalls) > 0) {
		return false
	}
	for _, spans := range [][]TimingInterval{s.Evidence.Model, s.Evidence.Turns} {
		for _, v := range spans {
			if v.End < v.Start {
				return false
			}
		}
	}
	for _, v := range s.Evidence.Calls {
		if v.End < v.Start {
			return false
		}
	}
	return true
}

// ReadSessionTimingChunk resumes from a complete-line checkpoint. Observe sees
// every raw byte transiently and in order; complete is true only for bytes that
// end a newline-terminated record. Earlier pieces of an oversized record and a
// final unterminated record arrive with complete=false. Callers may hash them,
// but must not retain them in the checkpoint. Records longer than the provider
// line limit are skipped like normal ingestion skips them. The returned state
// excludes a partial final record while the evidence preserves legacy EOF
// parsing. CompleteBytes is relative to this chunk's beginning.
func ReadSessionTimingChunk(ctx context.Context, source io.Reader, agent, path string, previous *SessionTimingState, observe func([]byte, bool) error) (*SessionTimingEvidence, *SessionTimingState, int64, error) {
	if agent != string(AgentCodex) && agent != string(AgentClaude) {
		return nil, nil, 0, ErrTimingSourceUnsupported
	}
	if previous != nil && !previous.Valid() {
		return nil, nil, 0, ErrTimingSourceUnsupported
	}
	state := previous.Clone()
	bounded := &io.LimitedReader{R: ctxio.Reader{Context: ctx, Reader: source}, N: (256 << 20) + 1}
	reader := bufio.NewReaderSize(bounded, initialScanBufSize)
	emit := func(p []byte, complete bool) error {
		if observe == nil || len(p) == 0 {
			return nil
		}
		return observe(p, complete)
	}
	var completeBytes, pending int64
	var line []byte
	oversized := false
	for {
		if err := ctx.Err(); err != nil {
			return nil, nil, 0, err
		}
		chunk, readErr := reader.ReadSlice('\n')
		if len(chunk) > 0 {
			terminated := chunk[len(chunk)-1] == '\n'
			pending += int64(len(chunk))
			switch {
			case oversized:
				if err := emit(chunk, terminated); err != nil {
					return nil, nil, 0, err
				}
			case len(line) == 0 && terminated:
				// Common case: consume the reader's buffer without copying it.
				// One buffered slice is always shorter than the line limit.
				if err := emit(chunk, true); err != nil {
					return nil, nil, 0, err
				}
				if err := state.consume(chunk, agent, path); err != nil {
					return nil, nil, 0, err
				}
			default:
				line = append(line, chunk...)
				if timingLineContentLen(line) > maxLineSize {
					oversized = true
					if err := emit(line, terminated); err != nil {
						return nil, nil, 0, err
					}
					line = line[:0]
				} else if terminated {
					if err := emit(line, true); err != nil {
						return nil, nil, 0, err
					}
					if err := state.consume(line, agent, path); err != nil {
						return nil, nil, 0, err
					}
					line = line[:0]
				}
			}
			if terminated {
				completeBytes += pending
				pending, oversized = 0, false
			}
		}
		if readErr == nil || errors.Is(readErr, bufio.ErrBufferFull) {
			continue
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		return nil, nil, 0, readErr
	}
	if bounded.N == 0 {
		return nil, nil, 0, ErrTimingSourceUnsupported
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, 0, err
	}
	if len(line) > 0 {
		if err := emit(line, false); err != nil {
			return nil, nil, 0, err
		}
		preview := state.Clone()
		if err := preview.consume(line, agent, path); err != nil {
			return nil, nil, 0, err
		}
		return preview.Snapshot(agent), state, completeBytes, nil
	}
	return state.Snapshot(agent), state, completeBytes, nil
}

// timingLineContentLen measures a record the way lineReader does, excluding
// its line terminator.
func timingLineContentLen(line []byte) int {
	n := len(line)
	if n > 0 && line[n-1] == '\n' {
		n--
		if n > 0 && line[n-1] == '\r' {
			n--
		}
	}
	return n
}

func (s *SessionTimingState) consume(line []byte, agent, path string) error {
	out, starts, calls, ambiguousCalls := &s.Evidence, s.Starts, s.Calls, s.AmbiguousCalls
	request := func(id string, stamp int64) {
		id = strings.Clone(id)
		if id == "" {
			return
		}
		if previous, exists := calls[id]; exists && previous != stamp {
			ambiguousCalls[id] = true
		}
		calls[id] = stamp
	}
	result := func(id string, stamp int64) {
		if start, exists := calls[id]; id != "" && exists && !ambiguousCalls[id] && stamp >= start {
			out.Calls = append(out.Calls, TimingCall{strings.Clone(id), TimingInterval{start, stamp}})
		}
		delete(calls, id)
		delete(ambiguousCalls, id)
	}
	if !gjson.ValidBytes(line) {
		return nil
	}
	entry := gjson.ParseBytes(line)
	if agent == string(AgentCodex) && entry.Get("type").Str == "session_meta" {
		payload := entry.Get("payload")
		if payload.Get("forked_from_id").Str != "" || payload.Get("source.subagent").Exists() {
			return ErrTimingSourceUnsupported
		}
		id := payload.Get("id").Str
		// Use the same exact filename/header checks as normalized ingestion.
		// A continuation page has a distinct archive identity even when its
		// header identifies the thread (or an older header identifies the rollout).
		if path != "" {
			thread := codexRevertPageThreadID(CodexSessionUUIDFromFilename(filepath.Base(path)), id, payload.Get("session_id").Str)
			if page, _, ok := codexRevertPage(path, thread, "", "", false); ok {
				id = page
			}
		}
		if out.SourceID != "" && id != "" && out.SourceID != id {
			return ErrTimingSourceUnsupported
		}
		if id != "" {
			out.SourceID = strings.Clone(id)
		}
		return nil
	}
	if agent == string(AgentClaude) {
		// The provider merges assistant chunks before hiding metadata users.
		// Every user entry ends that run, including hidden or undated entries.
		// An ignored assistant with another ID also breaks the prior run.
		kind := entry.Get("type").Str
		messageID := entry.Get("message.id").Str
		if kind == "user" || kind == "assistant" && (messageID == "" || messageID != s.ClaudeMessageID) ||
			entry.Get("isCompactSummary").Bool() {
			s.resetClaudeMessage()
		}
	}
	ts := parseTimestamp(entry.Get("timestamp").Str)
	if ts.IsZero() {
		return nil
	}
	stamp := ts.UnixMilli()
	if agent == string(AgentCodex) {
		payload := entry.Get("payload")
		switch entry.Get("type").Str {
		case "event_msg":
			id := payload.Get("turn_id").Str
			switch payload.Get("type").Str {
			case "task_started":
				if _, exists := starts[id]; id != "" && !exists {
					// A resumed task cannot extend an abandoned predecessor
					// across the idle gap, even when its completion is absent.
					s.closeOpenTurns()
					starts[strings.Clone(id)], s.LastActivity = stamp, stamp
				}
			case "task_complete", "turn_aborted":
				// Older abort events can omit turn_id; a single active
				// lifecycle is the only unambiguous target.
				if id == "" && len(starts) == 1 {
					for active := range starts {
						id = active
					}
				}
				if start, ok := starts[id]; ok && stamp >= start {
					out.Turns = append(out.Turns, TimingInterval{start, stamp})
					delete(starts, id)
				}
			case "item_completed":
				kind := payload.Get("item.type").Str
				start, startOK := codexExecutionTimestamp(payload.Get("started_at_ms"))
				end, endOK := codexExecutionTimestamp(payload.Get("completed_at_ms"))
				if (kind == "Reasoning" || kind == "AgentMessage") && startOK && endOK && start.UnixMilli() > 0 && !end.Before(start) {
					out.Model = append(out.Model, TimingInterval{start.UnixMilli(), end.UnixMilli()})
				}
			}
		case "response_item":
			id := payload.Get("call_id").Str
			switch payload.Get("type").Str {
			case "function_call", "custom_tool_call":
				request(id, stamp)
			case "function_call_output", "custom_tool_call_output":
				result(id, stamp)
			}
		}
		if len(starts) > 0 {
			kind := payload.Get("type").Str
			responseActivity := entry.Get("type").Str == "response_item" && payload.Get("role").Str != "user" && payload.Get("role").Str != "developer" && payload.Get("role").Str != "system"
			eventActivity := entry.Get("type").Str == "event_msg" && kind != "task_started" && kind != "user_message" && kind != "thread_settings_applied"
			_, belongs := starts[payload.Get("turn_id").Str]
			if (payload.Get("turn_id").Str == "" || belongs) && (responseActivity || eventActivity) {
				s.LastActivity = max(s.LastActivity, stamp)
			}
		}
	} else {
		if entry.Get("isSidechain").Bool() || isHiddenClaudeMeta(entry.Raw) {
			return nil
		}
		if id := entry.Get("sessionId").Str; id != "" {
			if out.SourceID != "" && out.SourceID != id {
				return ErrTimingSourceUnsupported
			}
			out.SourceID = strings.Clone(id)
		}
		kind := entry.Get("type").Str
		if entry.Get("isCompactSummary").Bool() {
			// Both user and assistant compact summaries are system boundaries.
			// They end a streaming run but never start a human turn or supply
			// assistant/tool evidence from their quoted summary content.
			s.resetClaudeMessage()
			return nil
		}
		if kind == "attachment" {
			if queued, ok := extractQueuedCommand(entry.Raw); ok && !queuedCommandMessage(queued).IsSystem {
				// Queued prompts can arrive while the current tool is still
				// outstanding. That wait remains active up to this boundary.
				waiting := false
				for _, started := range calls {
					if s.ClaudeWaitStart > 0 && started >= s.ClaudeWaitStart && started <= stamp {
						s.LastClaudeActivity = max(s.LastClaudeActivity, stamp)
						waiting = true
						break
					}
				}
				s.closeClaude()
				if !waiting {
					s.ClaudeWaitStart = stamp
				}
				s.Prompt, s.LastClaudeActivity = stamp, 0
				s.ClaudeComplete = false
			}
			return nil
		}
		if kind != "user" && kind != "assistant" {
			return nil
		}
		if kind == "assistant" {
			messageID := entry.Get("message.id").Str
			if messageID == "" || messageID != s.ClaudeMessageID {
				s.resetClaudeMessage()
				if len(messageID) > maxTimingClaudeMessageBytes {
					return ErrTimingSourceUnsupported
				}
				s.ClaudeMessageID = strings.Clone(messageID)
				s.ClaudeMessageBytes = len(messageID)
			}
		}
		blocks := entry.Get("message.content")
		isResult := false
		blocks.ForEach(func(_, block gjson.Result) bool {
			switch block.Get("type").Str {
			case "tool_use":
				if kind == "assistant" {
					id := block.Get("id").Str
					if s.ClaudeMessageID != "" && id != "" {
						if s.ClaudeMessageCalls[id] {
							return true
						}
						if s.ClaudeMessageCalls == nil {
							s.ClaudeMessageCalls = map[string]bool{}
						}
						s.ClaudeMessageCalls[strings.Clone(id)] = true
						s.ClaudeMessageBytes += len(id)
					}
					request(id, stamp)
				}
			case "tool_result":
				isResult = true
				id := block.Get("tool_use_id").Str
				if kind == "user" {
					result(id, stamp)
				}
			}
			return true
		})
		if kind == "user" && !isResult {
			// Native transcripts also encode interruption markers, IDE
			// context, and task notifications as user entries. Match the
			// provider's classification so these do not split a real turn.
			text, skip := preprocessClaudeUserText(extractCompactSummary(entry.Raw))
			if !skip && classifyClaudeSystemMessage(text) == "interrupted" && s.Prompt > 0 && stamp >= s.Prompt && stamp >= s.LastClaudeActivity {
				// An interruption closes the current turn without creating a
				// human prompt. Cancelled requests cannot keep later queues busy.
				s.LastClaudeActivity, s.ClaudeComplete = stamp, true
				s.ClaudeWaitStart = 0
				clear(calls)
				clear(ambiguousCalls)
			}
			if skip || strings.TrimSpace(text) == "" || isClaudeSystemMessage(text) {
				return nil
			}
			s.closeClaude()
			s.Prompt, s.LastClaudeActivity = stamp, 0
			s.ClaudeWaitStart = stamp
			s.ClaudeComplete = false
		}
		// A tool result proves activity even if the process stops before
		// another assistant message is written. Keep its known waiting
		// interval inside this turn without extending it to wall time.
		// A cancellation acknowledgement after an interruption (or a late
		// background result after completion) cannot reopen the closed turn.
		if kind == "user" && isResult && !s.ClaudeComplete {
			s.LastClaudeActivity = max(s.LastClaudeActivity, stamp)
			s.ClaudeComplete = false
		}
		if kind == "assistant" {
			s.LastClaudeActivity = max(s.LastClaudeActivity, stamp)
			switch entry.Get("message.stop_reason").Str {
			case "end_turn", "max_tokens", "stop_sequence":
				s.ClaudeComplete = true
				s.ClaudeWaitStart = 0
			default:
				s.ClaudeComplete = false
			}
			if duration := entry.Get("thinkingDurationMs").Int(); duration > 0 && duration <= stamp {
				out.Model = append(out.Model, TimingInterval{stamp - duration, stamp})
			}
		}
	}
	if len(out.Model)+len(out.Calls)+len(out.Turns)+len(starts)+len(calls)+len(ambiguousCalls)+len(s.ClaudeMessageCalls) > 100000 ||
		s.ClaudeMessageBytes > maxTimingClaudeMessageBytes {
		return ErrTimingSourceUnsupported
	}
	return nil
}

// Only user/assistant entries participate in the provider's streaming runs;
// attachments and other system records are removed before its merge step.
func (s *SessionTimingState) resetClaudeMessage() {
	s.ClaudeMessageID = ""
	s.ClaudeMessageCalls = nil
	s.ClaudeMessageBytes = 0
}
