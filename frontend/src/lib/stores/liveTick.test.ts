import { afterEach, expect, it, vi } from "vite-plus/test";
import { tick } from "svelte";
import type { DbSessionTiming as SessionTiming } from "../api/generated/index.js";

const mocks = vi.hoisted(() => ({ fetch: vi.fn() }));
vi.mock("../api/generated/sessions/sessions.js", () => ({
  getApiV1SessionsByIdTiming: mocks.fetch,
}));

afterEach(() => {
  vi.useRealTimers();
});

it("rechecks quiet live sessions and stops ticking when the server marks them stale", async () => {
  vi.useFakeTimers();
  const start = Date.parse("2026-04-26T10:00:00Z");
  vi.setSystemTime(start);
  const snapshot: SessionTiming = {
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
