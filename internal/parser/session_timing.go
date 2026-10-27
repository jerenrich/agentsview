package parser

import (
	"bufio"
	"context"
	"errors"
	"io"

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
	var prompt, lastAssistant, lastTimestamp int64
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
		ts := parseTimestamp(entry.Get("timestamp").Str)
		if ts.IsZero() {
			continue
		}
		stamp := ts.UnixMilli()
		lastTimestamp = max(lastTimestamp, stamp)
		if agent == string(AgentCodex) {
			payload := entry.Get("payload")
			switch entry.Get("type").Str {
			case "session_meta":
				if payload.Get("forked_from_id").Str != "" || payload.Get("source.subagent").Exists() {
					return nil, ErrTimingSourceUnsupported
				}
				out.SourceID = payload.Get("id").Str
			case "event_msg":
				id := payload.Get("turn_id").Str
				switch payload.Get("type").Str {
				case "task_started":
					if _, exists := starts[id]; id != "" && !exists {
						starts[id] = stamp
					}
				case "task_complete", "turn_aborted":
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
					if id != "" {
						calls[id] = stamp
					}
				case "function_call_output", "custom_tool_call_output":
					if start, ok := calls[id]; ok && stamp >= start {
						out.Calls = append(out.Calls, TimingCall{id, TimingInterval{start, stamp}})
						delete(calls, id)
					}
				}
			}
		} else {
			if entry.Get("isSidechain").Bool() {
				continue
			}
			if id := entry.Get("sessionId").Str; id != "" {
				if out.SourceID != "" && out.SourceID != id {
					return nil, ErrTimingSourceUnsupported
				}
				out.SourceID = id
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
						calls[block.Get("id").Str] = stamp
					}
				case "tool_result":
					isResult = true
					id := block.Get("tool_use_id").Str
					if start, ok := calls[id]; kind == "user" && ok && stamp >= start {
						out.Calls = append(out.Calls, TimingCall{id, TimingInterval{start, stamp}})
						delete(calls, id)
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
		// An unfinished turn stops at the observed snapshot, never at now:
		// old interrupted transcripts must not accumulate days of unknown time.
		for _, start := range starts {
			if lastTimestamp >= start {
				out.Turns = append(out.Turns, TimingInterval{start, lastTimestamp})
			}
		}
	}
	return out, nil
}
