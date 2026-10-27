import { getApiV1SessionsByIdTiming as fetchSessionTiming } from "../api/generated/sessions/sessions.js";
import type { DbSessionTiming as SessionTiming } from "../api/generated/index.js";
import { LatestRead } from "../utils/latest-read.js";
import { m } from "../i18n/index.js";

const timingReadDeadlineMs = 30000;

/** Per-session timing snapshot fetched from
 *  GET /api/v1/sessions/{id}/timing and refreshed live by the
 *  session.timing SSE event on the watch stream. */
class SessionTimingStore {
  timing: SessionTiming | null = $state(null);
  loading: boolean = $state(false);
  error: string | null = $state(null);
  pollingPaused: boolean = $state(false);

  private currentSessionId: string | null = null;
  private loadVersion = 0;
  private timingRead = new LatestRead();
  private pendingLoad: Promise<void> | null = null;

  /** True when the current snapshot belongs to the given session. */
  isForSession(sessionId: string): boolean {
    return this.currentSessionId === sessionId;
  }

  /** Fetch the timing snapshot for sessionId. Cached per session;
   *  calling load() again for the same session is a no-op once a
   *  snapshot is in memory. Concurrent callers share the pending request.
   *  SSE events update the cached snapshot in place. */
  load(sessionId: string, refresh = false): Promise<void> {
    if (this.pollingPaused) return Promise.resolve();
    if (this.currentSessionId === sessionId) {
      if (this.pendingLoad) return this.pendingLoad;
      if (this.timing !== null && !refresh) return Promise.resolve();
    }
    if (this.currentSessionId !== sessionId) {
      this.timing = null;
      this.error = null;
    }
    this.currentSessionId = sessionId;
    const version = ++this.loadVersion;
    const signal = this.timingRead.begin();
    this.loading = true;
    // A background retry keeps its failure state until fresh evidence arrives.
    // LiveTick uses it to pause extrapolation while retaining the last snapshot.
    if (this.timing === null) this.error = null;
    let resolve!: () => void;
    let reject!: (reason: unknown) => void;
    const pending = new Promise<void>((res, rej) => {
      resolve = res;
      reject = rej;
    });
    // App and panel callers share completion as well as the network request.
    // Install before invoking a transport that can fail synchronously.
    this.pendingLoad = pending;
    void this.readSnapshot(sessionId, version, signal).then(resolve, reject);
    return pending;
  }

  /** Recheck liveness even when the source stops emitting stream updates. */
  refresh(): Promise<void> {
    return this.pollingPaused || this.currentSessionId === null
      ? Promise.resolve()
      : this.load(this.currentSessionId, true);
  }

  /** Leave the session route without discarding its cached timing. */
  pause(): void {
    this.pollingPaused = true;
    this.cancelInFlight();
  }

  /** Returning to a cached session must revalidate its runtime state. */
  resume(sessionId: string): Promise<void> {
    const refresh = this.pollingPaused;
    this.pollingPaused = false;
    return this.load(sessionId, refresh);
  }

  private async readSnapshot(
    sessionId: string,
    version: number,
    signal: AbortSignal,
  ): Promise<void> {
    const controller = new AbortController();
    let deadline: ReturnType<typeof setTimeout> | undefined;
    let abortRequest = () => {};
    try {
      // Settle our read on cancellation or timeout even if a transport ignores
      // AbortSignal. Its eventual result only settles the already-finished
      // promise and cannot overwrite a newer snapshot or hold pendingLoad.
      const t = await new Promise<SessionTiming>((resolve, reject) => {
        abortRequest = () => {
          controller.abort(signal.reason);
          reject(signal.reason);
        };
        signal.addEventListener("abort", abortRequest, { once: true });
        if (signal.aborted) {
          abortRequest();
          return;
        }
        deadline = setTimeout(() => {
          const error = new Error(m.session_timing_request_timeout());
          controller.abort(error);
          reject(error);
        }, timingReadDeadlineMs);
        void fetchSessionTiming({ id: sessionId }, { signal: controller.signal }).then(
          resolve,
          reject,
        );
      });
      if (version !== this.loadVersion || !this.timingRead.isCurrent(signal)) return;
      this.timing = t;
      this.error = null;
    } catch (e) {
      if (signal.aborted || version !== this.loadVersion || !this.timingRead.isCurrent(signal))
        return;
      this.error = e instanceof Error ? e.message : String(e);
      // Preserve a loaded snapshot so quiet-session polling can retry. Initial
      // load failures still have no snapshot and keep their existing error UI.
    } finally {
      clearTimeout(deadline);
      signal.removeEventListener("abort", abortRequest);
      if (this.timingRead.finish(signal)) {
        this.loading = false;
        this.pendingLoad = null;
      }
    }
  }

  /** Called by the SSE handler when a session.timing event
   *  arrives. Ignored if the payload is for a different session
   *  than the one currently loaded. */
  applyEvent(payload: SessionTiming): void {
    if (this.pollingPaused || payload.session_id !== this.currentSessionId) return;
    // The stream now owns this snapshot. An earlier HTTP response or failure
    // must not replace it, even when the transport settles after cancellation.
    this.cancelInFlight();
    this.timing = payload;
    this.error = null;
  }

  /** Drop cached state. Call when leaving session detail view. */
  reset(): void {
    this.pollingPaused = false;
    this.pendingLoad = null;
    this.timingRead.cancel();
    this.loadVersion++;
    this.currentSessionId = null;
    this.timing = null;
    this.loading = false;
    this.error = null;
  }

  cancelInFlight(): void {
    this.pendingLoad = null;
    this.loadVersion++;
    this.timingRead.cancel();
    this.loading = false;
  }
}

export const sessionTiming = new SessionTimingStore();
