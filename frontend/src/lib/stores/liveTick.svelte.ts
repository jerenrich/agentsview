import { sessionTiming } from "./sessionTiming.svelte.js";

/** Reactive Date.now() that ticks every second while the loaded
 *  session has `running: true`. A $derived that reads
 *  `liveTick.now` re-runs on each tick, so running-duration
 *  labels can refresh at 1Hz without a per-component setInterval. */
class LiveTickStore {
  now: number = $state(Date.now());

  private timer: ReturnType<typeof setInterval> | null = null;
  private refreshedAt = 0;

  constructor() {
    $effect.root(() => {
      $effect(() => {
        const running = sessionTiming.timing?.running ?? false;
        if (running) {
          if (this.timer == null) {
            this.now = Date.now();
            this.refreshedAt = this.now;
            this.timer = setInterval(() => {
              this.now = Date.now();
              // A quiet or stopped source emits no timing event. Recheck the
              // server's bounded liveness instead of extrapolating forever.
              if (this.now - this.refreshedAt >= 30000) {
                this.refreshedAt = this.now;
                void sessionTiming.refresh();
              }
            }, 1000);
          }
        } else if (this.timer != null) {
          clearInterval(this.timer);
          this.timer = null;
          this.now = Date.now();
        }
      });
    });
  }
}

export const liveTick = new LiveTickStore();
