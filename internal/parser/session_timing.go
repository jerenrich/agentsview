package parser

import (
	"bufio"
	"context"
	"errors"
	"io"
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
}

var ErrTimingSourceUnsupported = errors.New("unsupported timing source")

// ReadCompressedSessionTiming decodes the same JSONL evidence using the shared
// bounded streaming decoder. The caller bounds the imported compressed prefix.
func ReadCompressedSessionTiming(ctx context.Context, source io.Reader, agent string) (*SessionTimingEvidence, error) {
	decoder, err := newZstdStreamDecoder(ctxio.Reader{Context: ctx, Reader: source})
	if err != nil {
		return nil, err
	}
	defer decoder.Close()
	return ReadSessionTiming(ctx, decoder, agent)
}

// ReadSessionTiming reads a bounded, already-imported source snapshot. Codex
// lifecycle events close turns; Claude's final assistant timestamp provides a
// conservative endpoint. Forked/replayed sources are left to archive timing
// rather than mixing the parent and child timelines.
func ReadSessionTiming(ctx context.Context, source io.Reader, agent string) (*SessionTimingEvidence, error) {
	if agent != string(AgentCodex) && agent != string(AgentClaude) {
		return nil, ErrTimingSourceUnsupported
	}
	out := &SessionTimingEvidence{}
	starts := map[string]int64{}
	calls := map[string]int64{}
	ambiguousCalls := map[string]bool{}
	var lastActivity int64
	closeOpenTurns := func() {
		for id, start := range starts {
			out.Turns = append(out.Turns, TimingInterval{start, max(start, lastActivity)})
			delete(starts, id)
		}
	}
	request := func(id string, stamp int64) {
		// gjson strings can reference a whole JSON record. Clone IDs that
		// outlive this iteration so a small key cannot pin a tool body.
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
	var prompt, lastAssistant int64
	closeClaude := func() {
		if prompt > 0 && lastAssistant >= prompt {
			out.Turns = append(out.Turns, TimingInterval{prompt, lastAssistant})
		}
	}
	bounded := &io.LimitedReader{R: source, N: (256 << 20) + 1}
	scanner := bufio.NewScanner(bounded)
	scanner.Buffer(make([]byte, initialScanBufSize), maxLineSize)
	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		line := scanner.Bytes()
		if !gjson.ValidBytes(line) {
			continue
		}
		entry := gjson.ParseBytes(line)
		if agent == string(AgentCodex) && entry.Get("type").Str == "session_meta" {
			payload := entry.Get("payload")
			if payload.Get("forked_from_id").Str != "" || payload.Get("source.subagent").Exists() {
				return nil, ErrTimingSourceUnsupported
			}
			id := payload.Get("id").Str
			if out.SourceID != "" && id != "" && out.SourceID != id {
				return nil, ErrTimingSourceUnsupported
			}
			if id != "" {
				out.SourceID = strings.Clone(id)
			}
			continue
		}
		ts := parseTimestamp(entry.Get("timestamp").Str)
		if ts.IsZero() {
			continue
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
						closeOpenTurns()
						starts[strings.Clone(id)], lastActivity = stamp, stamp
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
					lastActivity = max(lastActivity, stamp)
				}
			}
		} else {
			if entry.Get("isSidechain").Bool() || isHiddenClaudeMeta(entry.Raw) {
				continue
			}
			if id := entry.Get("sessionId").Str; id != "" {
				if out.SourceID != "" && out.SourceID != id {
					return nil, ErrTimingSourceUnsupported
				}
				out.SourceID = strings.Clone(id)
			}
			kind := entry.Get("type").Str
			if kind != "user" && kind != "assistant" {
				continue
			}
			blocks := entry.Get("message.content")
			isResult := false
			blocks.ForEach(func(_, block gjson.Result) bool {
				switch block.Get("type").Str {
				case "tool_use":
					if kind == "assistant" {
						request(block.Get("id").Str, stamp)
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
				closeClaude()
				prompt, lastAssistant = stamp, 0
			}
			if kind == "assistant" {
				lastAssistant = stamp
				if duration := entry.Get("thinkingDurationMs").Int(); duration > 0 && duration <= stamp {
					out.Model = append(out.Model, TimingInterval{stamp - duration, stamp})
				}
			}
		}
		// Bound retained metadata independently of potentially large payloads.
		if len(out.Model)+len(out.Calls)+len(out.Turns)+len(starts)+len(calls) > 100000 {
			return nil, ErrTimingSourceUnsupported
		}
	}
	if bounded.N == 0 {
		return nil, ErrTimingSourceUnsupported
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if agent == string(AgentClaude) {
		closeClaude()
	} else {
		// Stop at observed activity, never wall time or a later prompt.
		closeOpenTurns()
	}
	return out, nil
}
