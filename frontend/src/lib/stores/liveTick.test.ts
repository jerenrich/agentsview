import { afterEach, beforeEach, expect, it, vi } from "vite-plus/test";
import { tick } from "svelte";
import type { DbSessionTiming as SessionTiming } from "../api/generated/index.js";

const mocks = vi.hoisted(() => ({ fetch: vi.fn() }));
vi.mock("../api/generated/sessions/sessions.js", () => ({
  getApiV1SessionsByIdTiming: mocks.fetch,
}));

beforeEach(() => {
  mocks.fetch.mockReset();
});

afterEach(async () => {
  const { sessionTiming } = await import("./sessionTiming.svelte.js");
  sessionTiming.reset();
  await tick();
  vi.useRealTimers();
});

function liveSnapshot(): SessionTiming {
  return {
    session_id: "s",
    running: true,
    total_duration_ms: 5000,
    tool_duration_ms: 0,
    turn_count: 0,
    tool_call_count: 0,
    subagent_count: 0,
    slowest_call: null,
    by_category: [],
    turns: [],
    activity: [],
    activity_totals: { tool_ms: 0, unattributed_ms: 0 },
  };
}

it("rechecks quiet live sessions and stops ticking when the server marks them stale", async () => {
  vi.useFakeTimers();
  const start = Date.parse("2026-04-26T10:00:00Z");
  vi.setSystemTime(start);
  const snapshot = liveSnapshot();
  mocks.fetch
    .mockResolvedValueOnce(snapshot)
    .mockResolvedValueOnce({ ...snapshot, running: false });
  const { sessionTiming } = await import("./sessionTiming.svelte.js");
  const { liveTick } = await import("./liveTick.svelte.js");
  await sessionTiming.load("s");
  await tick();
  await vi.advanceTimersByTimeAsync(29000);
  expect(liveTick.now).toBe(start + 29000);
  expect(mocks.fetch).toHaveBeenCalledTimes(1);
  await vi.advanceTimersByTimeAsync(1000);
  await tick();
  expect(mocks.fetch).toHaveBeenCalledTimes(2);
  expect(sessionTiming.timing?.running).toBe(false);
  const stoppedAt = liveTick.now;
  await vi.advanceTimersByTimeAsync(60000);
  expect(mocks.fetch).toHaveBeenCalledTimes(2);
  expect(liveTick.now).toBe(stoppedAt);
  sessionTiming.reset();
  await tick();
});

it("pauses elapsed time but keeps bounded retries through transient refresh failures", async () => {
  vi.useFakeTimers();
  const start = Date.parse("2026-04-26T10:00:00Z");
  vi.setSystemTime(start);
  const snapshot = liveSnapshot();
  mocks.fetch
    .mockResolvedValueOnce(snapshot)
    .mockRejectedValueOnce(new Error("503 Service Unavailable"))
    .mockRejectedValueOnce(new Error("network unavailable"))
    .mockResolvedValueOnce({ ...snapshot, running: false });
  const { sessionTiming } = await import("./sessionTiming.svelte.js");
  const { liveTick } = await import("./liveTick.svelte.js");
  await sessionTiming.load("s");
  await tick();
  await vi.advanceTimersByTimeAsync(30000);
  await tick();
  expect(sessionTiming.timing).toEqual(snapshot);
  expect(sessionTiming.error).toBe("503 Service Unavailable");
  const pausedAt = liveTick.now;
  await vi.advanceTimersByTimeAsync(29000);
  expect(liveTick.now).toBe(pausedAt);
  expect(mocks.fetch).toHaveBeenCalledTimes(2);
  await vi.advanceTimersByTimeAsync(1000);
  expect(sessionTiming.error).toBe("network unavailable");
  expect(liveTick.now).toBe(pausedAt);
  expect(mocks.fetch).toHaveBeenCalledTimes(3);
  await vi.advanceTimersByTimeAsync(30000);
  await tick();
  expect(mocks.fetch).toHaveBeenCalledTimes(4);
  expect(sessionTiming.error).toBeNull();
  expect(sessionTiming.timing?.running).toBe(false);
  const stoppedAt = liveTick.now;
  await vi.advanceTimersByTimeAsync(60000);
  expect(mocks.fetch).toHaveBeenCalledTimes(4);
  expect(liveTick.now).toBe(stoppedAt);
});

it("releases a stalled refresh, pauses its clock, and accepts a later retry", async () => {
  vi.useFakeTimers();
  const start = Date.parse("2026-04-26T10:00:00Z");
  vi.setSystemTime(start);
  const snapshot = liveSnapshot();
  let resolveLate!: (value: SessionTiming) => void;
  let stalledSignal: AbortSignal | undefined;
  mocks.fetch
    .mockResolvedValueOnce(snapshot)
    .mockImplementationOnce((_params, { signal }) => {
      stalledSignal = signal;
      return new Promise<SessionTiming>((resolve) => {
        resolveLate = resolve;
      });
    })
    .mockResolvedValueOnce({ ...snapshot, running: false });
  const { sessionTiming } = await import("./sessionTiming.svelte.js");
  const { liveTick } = await import("./liveTick.svelte.js");
  await sessionTiming.load("s");
  await tick();
  await vi.advanceTimersByTimeAsync(30000);
  expect(mocks.fetch).toHaveBeenCalledTimes(2);
  expect(sessionTiming.loading).toBe(true);
  const pausedAt = liveTick.now;
  await vi.advanceTimersByTimeAsync(29000);
  expect(liveTick.now).toBe(pausedAt);
  expect(stalledSignal?.aborted).toBe(false);
  await vi.advanceTimersByTimeAsync(31000);
  await tick();
  expect(stalledSignal?.aborted).toBe(true);
  expect(mocks.fetch).toHaveBeenCalledTimes(3);
  expect(sessionTiming.timing?.running).toBe(false);
  expect(sessionTiming.error).toBeNull();
  resolveLate(snapshot);
  await tick();
  const stoppedAt = liveTick.now;
  await vi.advanceTimersByTimeAsync(60000);
  expect(sessionTiming.timing?.running).toBe(false);
  expect(mocks.fetch).toHaveBeenCalledTimes(3);
  expect(liveTick.now).toBe(stoppedAt);
});

it("suspends polling on route exit and revalidates before advancing on return", async () => {
  vi.useFakeTimers();
  const start = Date.parse("2026-04-26T10:00:00Z");
  vi.setSystemTime(start);
  const snapshot = liveSnapshot();
  let resolveReturn!: (value: SessionTiming) => void;
  mocks.fetch
    .mockResolvedValueOnce(snapshot)
    .mockImplementationOnce(
      () =>
        new Promise<SessionTiming>((resolve) => {
          resolveReturn = resolve;
        }),
    )
    .mockResolvedValueOnce({ ...snapshot, session_id: "different", running: false });
  const { sessionTiming } = await import("./sessionTiming.svelte.js");
  const { liveTick } = await import("./liveTick.svelte.js");
  await sessionTiming.load("s");
  await tick();
  await vi.advanceTimersByTimeAsync(10000);
  const atDeparture = liveTick.now;
  sessionTiming.pause();
  await tick();
  await vi.advanceTimersByTimeAsync(60000);
  expect(mocks.fetch).toHaveBeenCalledTimes(1);
  expect(liveTick.now).toBe(atDeparture);
  expect(sessionTiming.timing).toEqual(snapshot);
  const returning = sessionTiming.resume("s");
  await tick();
  expect(mocks.fetch).toHaveBeenCalledTimes(2);
  await vi.advanceTimersByTimeAsync(5000);
  expect(liveTick.now).toBe(atDeparture);
  resolveReturn(snapshot);
  await returning;
  await tick();
  await vi.advanceTimersByTimeAsync(1000);
  expect(liveTick.now).toBe(start + 76000);
  sessionTiming.pause();
  await tick();
  await sessionTiming.resume("different");
  await tick();
  expect(sessionTiming.timing?.session_id).toBe("different");
  sessionTiming.applyEvent(snapshot);
  await vi.advanceTimersByTimeAsync(60000);
  expect(sessionTiming.timing?.session_id).toBe("different");
  expect(mocks.fetch).toHaveBeenCalledTimes(3);
});

it("keeps polling an active session after SSE cancels a stalled HTTP read", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(Date.parse("2026-04-26T10:00:00Z"));
  const snapshot = liveSnapshot();
  let stalledSignal: AbortSignal | undefined;
  mocks.fetch
    .mockResolvedValueOnce(snapshot)
    .mockImplementationOnce((_params, { signal }) => {
      stalledSignal = signal;
      return new Promise(() => {});
    })
    .mockResolvedValueOnce({ ...snapshot, running: false });
  const { sessionTiming } = await import("./sessionTiming.svelte.js");
  await sessionTiming.load("s");
  await tick();
  await vi.advanceTimersByTimeAsync(30000);
  sessionTiming.applyEvent({ ...snapshot, total_duration_ms: 6000 });
  await tick();
  expect(stalledSignal?.aborted).toBe(true);
  expect(sessionTiming.pollingPaused).toBe(false);
  expect(sessionTiming.loading).toBe(false);
  await vi.advanceTimersByTimeAsync(30000);
  await tick();
  expect(mocks.fetch).toHaveBeenCalledTimes(3);
  expect(sessionTiming.timing?.running).toBe(false);
  expect(sessionTiming.error).toBeNull();
  expect(vi.getTimerCount()).toBe(0);
});
