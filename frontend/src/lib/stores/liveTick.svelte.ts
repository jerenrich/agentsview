import { sessionTiming } from "./sessionTiming.svelte.js";

/** Reactive clock for a running session in the active session view.
 *  Revalidation pauses elapsed time; inactive routes stop the timer.
 *  A $derived that reads
 *  `liveTick.now` re-runs on each tick, so running-duration
 *  labels can refresh at 1Hz without a per-component setInterval. */
class LiveTickStore {
  now: number = $state(Date.now());

  private timer: ReturnType<typeof setInterval> | null = null;
  private refreshedAt = 0;

  constructor() {
    $effect.root(() => {
      $effect(() => {
        const running = !sessionTiming.pollingPaused && (sessionTiming.timing?.running ?? false);
        if (running) {
          if (this.timer == null) {
            if (sessionTiming.error === null && !sessionTiming.loading) this.now = Date.now();
            this.refreshedAt = Date.now();
            this.timer = setInterval(() => {
              const now = Date.now();
              // Failed refreshes retain their snapshot for retry, but do not
              // provide evidence for more elapsed active time until recovery.
              if (sessionTiming.error === null && !sessionTiming.loading) this.now = now;
              // A quiet or stopped source emits no timing event. Recheck the
              // server's bounded liveness instead of extrapolating forever.
              if (now - this.refreshedAt >= 30000) {
                this.refreshedAt = now;
                void sessionTiming.refresh();
              }
            }, 1000);
          }
        } else if (this.timer != null) {
          clearInterval(this.timer);
          this.timer = null;
          if (!sessionTiming.pollingPaused) this.now = Date.now();
        }
      });
    });
  }
}

export const liveTick = new LiveTickStore();
