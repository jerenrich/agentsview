import { getApiV1SessionsByIdTiming as fetchSessionTiming } from "../api/generated/sessions/sessions.js";
import type { DbSessionTiming as SessionTiming } from "../api/generated/index.js";
import { LatestRead } from "../utils/latest-read.js";

/** Per-session timing snapshot fetched from
 *  GET /api/v1/sessions/{id}/timing and refreshed live by the
 *  session.timing SSE event on the watch stream. */
class SessionTimingStore {
  timing: SessionTiming | null = $state(null);
  loading: boolean = $state(false);
  error: string | null = $state(null);

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
    this.error = null;
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
    return this.currentSessionId === null
      ? Promise.resolve()
      : this.load(this.currentSessionId, true);
  }

  private async readSnapshot(
    sessionId: string,
    version: number,
    signal: AbortSignal,
  ): Promise<void> {
    try {
      const t = await fetchSessionTiming({ id: sessionId }, { signal });
      if (version !== this.loadVersion || !this.timingRead.isCurrent(signal)) return;
      this.timing = t;
    } catch (e) {
      if (signal.aborted || version !== this.loadVersion || !this.timingRead.isCurrent(signal))
        return;
      this.error = e instanceof Error ? e.message : String(e);
      this.timing = null;
    } finally {
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
    if (payload.session_id !== this.currentSessionId) return;
    // The stream now owns this snapshot. An earlier HTTP response or failure
    // must not replace it, even when the transport settles after cancellation.
    this.cancelInFlight();
    this.timing = payload;
    this.error = null;
  }

  /** Drop cached state. Call when leaving session detail view. */
  reset(): void {
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
