import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";
import type { DbSessionTiming as SessionTiming } from "../api/generated/index.js";
import { sessionTiming } from "./sessionTiming.svelte.js";

const timingMocks = vi.hoisted(() => ({
  fetchSessionTiming: vi.fn(),
}));

vi.mock("../api/generated/sessions/sessions.js", () => ({
  getApiV1SessionsByIdTiming: timingMocks.fetchSessionTiming,
}));

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

function timing(sessionId: string, durationMs: number): SessionTiming {
  return {
    session_id: sessionId,
    total_duration_ms: durationMs,
    tool_duration_ms: 0,
    turn_count: 0,
    tool_call_count: 0,
    subagent_count: 0,
    slowest_call: null,
    by_category: [],
    turns: [],
    activity: [],
    activity_totals: { tool_ms: 0, unattributed_ms: 0 },
    running: false,
  };
}

beforeEach(() => {
  sessionTiming.reset();
  timingMocks.fetchSessionTiming.mockReset();
});

afterEach(() => {
  sessionTiming.reset();
  vi.useRealTimers();
});

describe("SessionTimingStore", () => {
  it.each(["resolve", "reject"] as const)(
    "releases a stalled refresh and ignores its late %s after recovery",
    async (outcome) => {
      vi.useFakeTimers();
      const initial = { ...timing("s1", 5000), running: true };
      const stalled = deferred<SessionTiming>();
      const signals: AbortSignal[] = [];
      timingMocks.fetchSessionTiming
        .mockImplementationOnce((_params, { signal }) => {
          signals.push(signal);
          return Promise.resolve(initial);
        })
        .mockImplementationOnce((_params, { signal }) => {
          signals.push(signal);
          return stalled.promise;
        });
      await sessionTiming.load("s1");
      const pending = sessionTiming.refresh();
      expect(sessionTiming.refresh()).toBe(pending);
      await vi.advanceTimersByTimeAsync(30000);
      await pending;
      expect(signals[1]?.aborted).toBe(true);
      expect(sessionTiming.loading).toBe(false);
      expect(sessionTiming.timing).toEqual(initial);
      expect(sessionTiming.error).toBe("Timing request timed out.");
      const completed = timing("s1", 6000);
      timingMocks.fetchSessionTiming.mockResolvedValueOnce(completed);
      await sessionTiming.refresh();
      if (outcome === "resolve") stalled.resolve(initial);
      else stalled.reject(new Error("obsolete response"));
      await Promise.resolve();
      await Promise.resolve();
      expect(sessionTiming.timing).toEqual(completed);
      expect(sessionTiming.error).toBeNull();
      expect(sessionTiming.loading).toBe(false);
    },
  );

  it("pauses cached reads, settles cancellation, and revalidates on route return", async () => {
    vi.useFakeTimers();
    const initial = { ...timing("s1", 5000), running: true };
    const stalled = deferred<SessionTiming>();
    let signal: AbortSignal | undefined;
    timingMocks.fetchSessionTiming
      .mockResolvedValueOnce(initial)
      .mockImplementationOnce((_params, options) => {
        signal = options.signal;
        return stalled.promise;
      });
    await sessionTiming.load("s1");
    const pending = sessionTiming.refresh();
    sessionTiming.pause();
    await pending;
    expect(signal?.aborted).toBe(true);
    expect(sessionTiming.pollingPaused).toBe(true);
    expect(sessionTiming.loading).toBe(false);
    expect(sessionTiming.timing).toEqual(initial);
    await sessionTiming.refresh();
    await sessionTiming.load("s2");
    sessionTiming.applyEvent(timing("s1", 9000));
    await vi.advanceTimersByTimeAsync(60000);
    expect(timingMocks.fetchSessionTiming).toHaveBeenCalledTimes(2);
    expect(sessionTiming.timing).toEqual(initial);
    expect(sessionTiming.error).toBeNull();
    const completed = timing("s1", 6000);
    timingMocks.fetchSessionTiming.mockResolvedValueOnce(completed);
    await sessionTiming.resume("s1");
    expect(sessionTiming.pollingPaused).toBe(false);
    expect(timingMocks.fetchSessionTiming).toHaveBeenCalledTimes(3);
    expect(sessionTiming.timing).toEqual(completed);
    stalled.resolve(initial);
    await Promise.resolve();
    await Promise.resolve();
    expect(sessionTiming.timing).toEqual(completed);
  });

  it("retains a loaded snapshot and failure state until a refresh recovers", async () => {
    const initial = { ...timing("s1", 5000), running: true };
    timingMocks.fetchSessionTiming
      .mockResolvedValueOnce(initial)
      .mockRejectedValueOnce(new Error("503 Service Unavailable"));
    await sessionTiming.load("s1");
    await sessionTiming.refresh();
    expect(sessionTiming.timing).toEqual(initial);
    expect(sessionTiming.error).toBe("503 Service Unavailable");
    const request = deferred<SessionTiming>();
    timingMocks.fetchSessionTiming.mockReturnValueOnce(request.promise);
    const retry = sessionTiming.refresh();
    expect(sessionTiming.error).toBe("503 Service Unavailable");
    expect(sessionTiming.timing).toEqual(initial);
    const completed = timing("s1", 6000);
    request.resolve(completed);
    await retry;
    expect(sessionTiming.timing).toEqual(completed);
    expect(sessionTiming.error).toBeNull();
  });

  it("refreshes a cached live snapshot and shares an in-flight liveness check", async () => {
    const initial = { ...timing("s1", 5000), running: true };
    timingMocks.fetchSessionTiming.mockResolvedValueOnce(initial);
    await sessionTiming.load("s1");
    const request = deferred<SessionTiming>();
    timingMocks.fetchSessionTiming.mockReturnValueOnce(request.promise);
    const refreshed = sessionTiming.refresh();
    expect(sessionTiming.refresh()).toBe(refreshed);
    expect(sessionTiming.timing).toEqual(initial);
    const completed = timing("s1", 6000);
    request.resolve(completed);
    await refreshed;
    expect(timingMocks.fetchSessionTiming).toHaveBeenCalledTimes(2);
    expect(sessionTiming.timing?.running).toBe(false);
  });

  it("shares one pending request and completion between App and panel callers", async () => {
    const request = deferred<SessionTiming>();
    timingMocks.fetchSessionTiming.mockReturnValue(request.promise);
    const appLoad = sessionTiming.load("s1");
    const panelLoad = sessionTiming.load("s1");
    let panelFinished = false;
    void panelLoad.then(() => {
      panelFinished = true;
    });
    await Promise.resolve();

    expect(timingMocks.fetchSessionTiming).toHaveBeenCalledTimes(1);
    expect(panelLoad).toBe(appLoad);
    expect(panelFinished).toBe(false);
    expect(sessionTiming.loading).toBe(true);
    const snapshot = timing("s1", 1000);
    request.resolve(snapshot);
    await panelLoad;
    await appLoad;

    expect(panelFinished).toBe(true);
    expect(sessionTiming.timing).toEqual(snapshot);
    expect(sessionTiming.loading).toBe(false);
  });

  it("allows retry after a synchronously failed transport", async () => {
    timingMocks.fetchSessionTiming.mockImplementationOnce(() => {
      throw new Error("transport unavailable");
    });
    await sessionTiming.load("s1");
    expect(sessionTiming.error).toBe("transport unavailable");
    const snapshot = timing("s1", 1000);
    timingMocks.fetchSessionTiming.mockResolvedValue(snapshot);
    await sessionTiming.load("s1");

    expect(timingMocks.fetchSessionTiming).toHaveBeenCalledTimes(2);
    expect(sessionTiming.timing).toEqual(snapshot);
    expect(sessionTiming.error).toBeNull();
  });

  it("starts a fresh request after canceling a shared pending read", async () => {
    const obsolete = deferred<SessionTiming>();
    const current = deferred<SessionTiming>();
    timingMocks.fetchSessionTiming
      .mockReturnValueOnce(obsolete.promise)
      .mockReturnValueOnce(current.promise);
    const first = sessionTiming.load("s1");
    expect(sessionTiming.load("s1")).toBe(first);
    sessionTiming.cancelInFlight();
    const replacement = sessionTiming.load("s1");
    const snapshot = timing("s1", 2000);
    current.resolve(snapshot);
    await replacement;
    obsolete.resolve(timing("s1", 1000));
    await first;

    expect(timingMocks.fetchSessionTiming).toHaveBeenCalledTimes(2);
    expect(replacement).not.toBe(first);
    expect(sessionTiming.timing).toEqual(snapshot);
    expect(sessionTiming.loading).toBe(false);
  });

  it("aborts timing when another session replaces it", async () => {
    const signals: AbortSignal[] = [];
    timingMocks.fetchSessionTiming.mockImplementation(({ id }, { signal }) => {
      signals.push(signal as AbortSignal);
      if (id === "s1") return new Promise(() => {});
      return Promise.resolve({ session_id: id });
    });

    void sessionTiming.load("s1");
    await Promise.resolve();
    await sessionTiming.load("s2");

    expect(signals[0]?.aborted).toBe(true);
  });

  it("aborts the current timing read on route exit", async () => {
    const signals: AbortSignal[] = [];
    timingMocks.fetchSessionTiming.mockImplementation((_params, { signal }) => {
      signals.push(signal as AbortSignal);
      return new Promise(() => {});
    });

    void sessionTiming.load("s1");
    await Promise.resolve();
    sessionTiming.pause();

    expect(signals[0]?.aborted).toBe(true);
    expect(sessionTiming.pollingPaused).toBe(true);
  });

  it.each(["resolve", "reject"] as const)(
    "keeps an SSE timing snapshot when an older HTTP read later %ss",
    async (outcome) => {
      const request = deferred<SessionTiming>();
      let signal: AbortSignal | undefined;
      timingMocks.fetchSessionTiming.mockImplementation((_params, options) => {
        signal = options.signal;
        return request.promise;
      });
      const loading = sessionTiming.load("s1");
      const newer = timing("s1", 2000);
      sessionTiming.applyEvent(newer);

      expect(signal?.aborted).toBe(true);
      expect(sessionTiming.loading).toBe(false);
      if (outcome === "resolve") request.resolve(timing("s1", 1000));
      else request.reject(new Error("obsolete HTTP failure"));
      await loading;

      expect(sessionTiming.timing).toEqual(newer);
      expect(sessionTiming.error).toBeNull();
      expect(sessionTiming.loading).toBe(false);
    },
  );

  it("keeps the pending read when an SSE snapshot belongs to another session", async () => {
    const request = deferred<SessionTiming>();
    let signal: AbortSignal | undefined;
    timingMocks.fetchSessionTiming.mockImplementation((_params, options) => {
      signal = options.signal;
      return request.promise;
    });
    const loading = sessionTiming.load("s1");
    sessionTiming.applyEvent(timing("s2", 2000));

    expect(signal?.aborted).toBe(false);
    expect(sessionTiming.loading).toBe(true);
    expect(sessionTiming.timing).toBeNull();
    const fetched = timing("s1", 1000);
    request.resolve(fetched);
    await loading;
    expect(sessionTiming.timing).toEqual(fetched);
    expect(sessionTiming.loading).toBe(false);
  });
});
