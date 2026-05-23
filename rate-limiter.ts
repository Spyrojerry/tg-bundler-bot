// ─────────────────────────────────────────────────────────────────────────────
//  rate-limiter.ts  —  Global GMGN request queue with adaptive throttling
//
//  Design goals:
//    • All GMGN HTTP requests pass through ONE Bottleneck limiter.
//    • On 429, exponential backoff grows the minTime automatically.
//    • On recovery (several consecutive 2xx), minTime shrinks back.
//    • No request is ever fired twice for the same token in the same window.
// ─────────────────────────────────────────────────────────────────────────────

import Bottleneck from 'bottleneck';
import { createLogger } from './logger';

const log = createLogger('RATE');

// ── Constants ─────────────────────────────────────────────────────────────────

const BACKOFF_MIN_TIME_CAP = 10_000;   // never exceed 10 s between requests
const BACKOFF_MULTIPLIER   = 2;
const RECOVERY_THRESHOLD   = 5;        // consecutive successes before stepping down
const RECOVERY_STEP_DOWN   = 0.75;     // multiply minTime by this on recovery

// ── RateLimiter class ─────────────────────────────────────────────────────────

export class RateLimiter {
  private limiter: Bottleneck;
  private currentMinTime: number;
  private consecutiveSuccesses = 0;
  private consecutiveFailures  = 0;
  private isThrottled          = false;

  constructor(minTime: number, maxConcurrent: number) {
    this.currentMinTime = minTime;

    this.limiter = new Bottleneck({
      maxConcurrent,
      minTime,
      // Reservoir prevents huge bursts after idle periods
      reservoir: 10,
      reservoirRefreshInterval: 5_000,
      reservoirRefreshAmount: 10,
    });

    // Bubble Bottleneck errors so callers can handle them
    this.limiter.on('error', (err) => {
      log.warn('Bottleneck internal error', err);
    });

    log.info(`Rate limiter initialised`, { minTime, maxConcurrent });
  }

  /**
   * Schedule a function through the global queue.
   * Returns the function's result or re-throws on error.
   */
  async schedule<T>(fn: () => Promise<T>): Promise<T> {
    return this.limiter.schedule(fn);
  }

  /**
   * Call after each successful GMGN response (HTTP 2xx with valid data).
   * Gradually reduces minTime back toward the configured baseline.
   */
  onSuccess(baselineMinTime: number): void {
    this.consecutiveFailures = 0;
    this.consecutiveSuccesses++;

    if (
      this.isThrottled &&
      this.consecutiveSuccesses >= RECOVERY_THRESHOLD
    ) {
      const next = Math.max(
        baselineMinTime,
        Math.round(this.currentMinTime * RECOVERY_STEP_DOWN)
      );
      if (next !== this.currentMinTime) {
        this.currentMinTime = next;
        this.limiter.updateSettings({ minTime: next });
        log.info(`Rate limit relaxed → minTime=${next}ms`);
      }
      if (next <= baselineMinTime) {
        this.isThrottled = false;
        log.info('Rate limit fully recovered to baseline');
      }
    }
  }

  /**
   * Call when a 429 (or network error that looks rate-limit related) is received.
   * Doubles minTime up to the cap.
   */
  onRateLimited(retryAfterMs?: number): void {
    this.consecutiveSuccesses = 0;
    this.consecutiveFailures++;
    this.isThrottled = true;

    const next = retryAfterMs
      ? Math.min(retryAfterMs, BACKOFF_MIN_TIME_CAP)
      : Math.min(
          this.currentMinTime * BACKOFF_MULTIPLIER,
          BACKOFF_MIN_TIME_CAP
        );

    this.currentMinTime = next;
    this.limiter.updateSettings({ minTime: next });
    log.warn(`Rate limited — backoff applied → minTime=${next}ms (failure #${this.consecutiveFailures})`);
  }

  /** Returns how many ms we're currently waiting between requests */
  get currentDelay(): number {
    return this.currentMinTime;
  }

  get throttled(): boolean {
    return this.isThrottled;
  }

  /** Gracefully drain pending jobs then stop */
  async drain(): Promise<void> {
    await this.limiter.stop({ dropWaitingJobs: false });
    log.info('Rate limiter drained and stopped');
  }
}
