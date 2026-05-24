// ─────────────────────────────────────────────────────────────────────────────
//  scheduler.ts  —  Single-loop scheduler for GMGN metric fetches
//
//  Changes from v1:
//    • Each token is monitored for exactly MONITOR_WINDOW_MS (default 60 s).
//    • When the window expires the scheduler:
//        1. Waits for any in-flight request to land (pendingRequest guard).
//        2. Reads all samples for the token from the DB.
//        3. Computes and logs a full summary (first/last/min/max/avg).
//        4. Removes the token from the active set.
//    • MONITOR_INTERVAL default changed to 2 s.
//    • All other rate-limit / adaptive / priority / dedup logic unchanged.
// ─────────────────────────────────────────────────────────────────────────────

import { createLogger } from './logger';
import { EventEmitter } from 'events';
import { GmgnClient } from './gmgn-client';
import { MonitorDatabase } from './database';
import { RateLimiter } from './rate-limiter';
import {
  BundlerMetrics,
  FilterFailEvent,
  FilterPassEvent,
  MonitorSampleEvent,
  NewTokenEvent,
  SchedulerEntry,
  ServiceConfig,
  TokenSummary,
  WalletFilterProfileSettings,
  WalletFilterSettings,
} from './types';

const log = createLogger('SCHED');

const SCHEDULER_TICK_MS = 500; // internal resolution — half the min interval

// ── Scheduler ─────────────────────────────────────────────────────────────────

export class Scheduler extends EventEmitter {
  private readonly entries = new Map<string, SchedulerEntry>();
  private readonly config:  ServiceConfig;
  private readonly client:  GmgnClient;
  private readonly db:      MonitorDatabase;
  private readonly limiter: RateLimiter;

  private tickTimer:   NodeJS.Timeout | null = null;
  private running      = false;
  private totalFetches = 0;

  constructor(
    config:  ServiceConfig,
    client:  GmgnClient,
    db:      MonitorDatabase,
    limiter: RateLimiter
  ) {
    super();
    this.config  = config;
    this.client  = client;
    this.db      = db;
    this.limiter = limiter;
  }

  // ── Lifecycle ──────────────────────────────────────────────────────────────

  start(): void {
    if (this.running) return;
    this.running = true;
    this.scheduleTick();
    log.info(
      `Scheduler started — interval ${this.config.monitorInterval}ms, ` +
      `window ${this.config.monitoringWindowMs}ms per token`
    );
  }

  stop(): void {
    this.running = false;
    if (this.tickTimer) {
      clearTimeout(this.tickTimer);
      this.tickTimer = null;
    }
    log.info(`Scheduler stopped — total fetches: ${this.totalFetches}`);
  }

  // ── Token management ───────────────────────────────────────────────────────

  addToken(event: NewTokenEvent): void {
    const key = this.entryKey(event.walletAddress, event.mint);
    if (this.entries.has(key)) return;

    const now = Date.now();
    const entry: SchedulerEntry = {
      walletAddress:        event.walletAddress,
      mint:                event.mint,
      lastFetchedAt:       0,       // force immediate first fetch
      pendingRequest:      false,
      priority:            event.detectedAt,
      monitoringStartedAt: now,
      filterAlerted:       false,
      filterPassed:        false,
      buySol:              event.buySol,
      matchingWallets:     event.matchingWallets ?? [],
    };

    this.entries.set(key, entry);

    const windowSec = Math.round(this.config.monitoringWindowMs / 1_000);
    log.info(
      `Scheduler: tracking ${event.mint} for ${windowSec}s ` +
      `(${this.entries.size} active)`
    );
    this.logCapacityWarning();
  }

  removeToken(mint: string): void {
    const key = [...this.entries.entries()].find(([, entry]) => entry.mint === mint)?.[0];
    if (key) this.entries.delete(key);
    log.info(`Scheduler: removed ${mint} (${this.entries.size} active)`);
  }

  private removeEntry(entry: SchedulerEntry): void {
    this.entries.delete(this.entryKey(entry.walletAddress, entry.mint));
    log.info(`Scheduler: removed ${entry.mint} (${this.entries.size} active)`);
  }

  private entryKey(walletAddress: string, mint: string): string {
    return `${walletAddress}:${mint}`;
  }

  get activeCount(): number {
    return this.entries.size;
  }

  // ── Capacity advisory ─────────────────────────────────────────────────────

  get safeTokenCapacity(): number {
    return Math.floor(this.config.monitorInterval / this.limiter.currentDelay);
  }

  private logCapacityWarning(): void {
    const cap = this.safeTokenCapacity;
    if (this.entries.size > cap) {
      const effectiveInterval = this.entries.size * this.limiter.currentDelay;
      log.warn(
        `Token count (${this.entries.size}) exceeds safe capacity (${cap}). ` +
        `Effective poll interval stretched to ~${effectiveInterval}ms per token.`
      );
    }
  }

  // ── Scheduler tick ────────────────────────────────────────────────────────

  private scheduleTick(): void {
    if (!this.running) return;
    this.tickTimer = setTimeout(() => {
      this.tick().catch((err) => log.error('Tick error', err));
    }, SCHEDULER_TICK_MS);
  }

  private async tick(): Promise<void> {
    try {
      if (this.entries.size === 0) return;

      const now = Date.now();

      // ── 1. Expire tokens whose window has elapsed ─────────────────────────
      for (const entry of this.entries.values()) {
        const elapsed = now - entry.monitoringStartedAt;
        if (elapsed >= this.config.monitoringWindowMs && !entry.pendingRequest) {
          // Fire-and-forget the expiry so it doesn't block the tick loop
          this.expireToken(entry).catch((err) =>
            log.error(`Expiry error for ${entry.mint}`, err)
          );
        }
      }

      // ── 2. Adaptive effective interval ────────────────────────────────────
      const activeEntries = [...this.entries.values()];
      const effectiveInterval = Math.max(
        this.config.monitorInterval,
        activeEntries.length * this.limiter.currentDelay
      );

      // ── 3. Collect tokens due for a fetch ─────────────────────────────────
      const due: SchedulerEntry[] = [];
      for (const entry of activeEntries) {
        const elapsed = now - entry.monitoringStartedAt;
        if (
          !entry.pendingRequest &&
          elapsed < this.config.monitoringWindowMs &&          // still in window
          now - entry.lastFetchedAt >= effectiveInterval
        ) {
          due.push(entry);
        }
      }

      if (due.length === 0) return;

      // Sort: newest tokens (largest detectedAt) get highest priority
      due.sort((a, b) => b.priority - a.priority);

      for (const entry of due) {
        entry.pendingRequest = true;
        this.doFetch(entry).catch((err) =>
          log.error(`Unhandled fetch error for ${entry.mint}`, err)
        );
      }
    } finally {
      this.scheduleTick();
    }
  }

  // ── Window expiry + summary ───────────────────────────────────────────────

  private async expireToken(entry: SchedulerEntry): Promise<void> {
    const { mint } = entry;

    // Remove first so no more fetches are queued
    this.removeEntry(entry);

    // Update DB status
    this.db.updateTokenStatus(entry.walletAddress, mint, 'stopped');

    // Fetch ALL samples recorded during the window
    const samples = this.db.getLatestMetricsForWallet(
      entry.walletAddress,
      mint,
      10_000
    ); // effectively all
    // getLatestMetrics returns newest-first; reverse for chronological order
    samples.reverse();

    const summary = this.buildSummary(entry.walletAddress, mint, samples);
    this.logSummary(summary);
    this.emit('summary', summary);
  }

  // ── Summary computation ───────────────────────────────────────────────────

  private buildSummary(walletAddress: string, mint: string, samples: BundlerMetrics[]): TokenSummary {
    const percents = samples
      .map((s) => s.bundlersPercent)
      .filter((v): v is number => v !== null);

    const counts = samples
      .map((s) => s.bundlersCount)
      .filter((v): v is number => v !== null);

    return {
      walletAddress,
      mint,
      windowMs:     this.config.monitoringWindowMs,
      totalSamples: samples.length,
      firstSeen:    samples.at(0)?.timestamp  ?? new Date().toISOString(),
      lastSeen:     samples.at(-1)?.timestamp ?? new Date().toISOString(),
      bundlersPercent: {
        first: percents.at(0)  ?? null,
        last:  percents.at(-1) ?? null,
        min:   percents.length ? Math.min(...percents) : null,
        max:   percents.length ? Math.max(...percents) : null,
      },
      bundlersCount: {
        first: counts.at(0)  ?? null,
        last:  counts.at(-1) ?? null,
        min:   counts.length ? Math.min(...counts) : null,
        max:   counts.length ? Math.max(...counts) : null,
      },
    };
  }

  // ── Summary logging ───────────────────────────────────────────────────────

  private logSummary(s: TokenSummary): void {
    const windowSec = Math.round(s.windowMs / 1_000);
    const fmt = (v: number | null, suffix = ''): string =>
      v === null ? 'N/A' : `${v}${suffix}`;

    const lines = [
      ``,
      `╔══════════════════════════════════════════════════════════╗`,
      `║              [SUMMARY]  Monitoring Window Closed          ║`,
      `╠══════════════════════════════════════════════════════════╣`,
      `║  Mint    : ${s.mint}`,
      `║  Wallet  : ${s.walletAddress}`,
      `║  Window  : ${windowSec}s   Samples: ${s.totalSamples}`,
      `║  Period  : ${s.firstSeen}  →  ${s.lastSeen}`,
      `╠══════════════════════════════════════════════════════════╣`,
      `║  Bundlers %`,
      `║    First : ${fmt(s.bundlersPercent.first, '%')}   Last : ${fmt(s.bundlersPercent.last, '%')}`,
      `║    Min   : ${fmt(s.bundlersPercent.min,   '%')}   Max  : ${fmt(s.bundlersPercent.max,  '%')}`,
      `║  Bundlers Count`,
      `║    First : ${fmt(s.bundlersCount.first)}   Last : ${fmt(s.bundlersCount.last)}`,
      `║    Min   : ${fmt(s.bundlersCount.min)}   Max  : ${fmt(s.bundlersCount.max)}`,
      `╚══════════════════════════════════════════════════════════╝`,
      ``,
    ];

    for (const line of lines) {
      log.info(line);
    }

    // Machine-readable JSON line for piping / log aggregation
    process.stdout.write(JSON.stringify({ event: 'summary', ...s }) + '\n');
  }

  // ── Fetch + persist ────────────────────────────────────────────────────────

  private async doFetch(entry: SchedulerEntry): Promise<void> {
    const { mint } = entry;

    try {
      const result = await this.client.fetchBundlerMetrics(mint);
      entry.lastFetchedAt = Date.now();
      this.totalFetches++;

      const elapsed = Math.round((Date.now() - entry.monitoringStartedAt) / 1_000);

      if (result.success) {
        const { metrics } = result;
        metrics.walletAddress = entry.walletAddress;
        this.db.insertMetrics(metrics);
        const sampleNumber = this.db.metricsCountForWallet(entry.walletAddress, mint);

        log.info(
          `[MONITOR +${elapsed}s]  Mint: ${mint.slice(0, 8)}…  ` +
          `Bundlers%: ${metrics.bundlersPercent ?? 'N/A'}  ` +
          `Count: ${metrics.bundlersCount ?? 'N/A'}`,
          {
            mint,
            time:            metrics.timestamp,
            bundlersPercent: metrics.bundlersPercent,
            bundlersCount:   metrics.bundlersCount,
          }
        );

        process.stdout.write(
          JSON.stringify({
            mint,
            time:            metrics.timestamp,
            bundlersPercent: metrics.bundlersPercent,
            bundlersCount:   metrics.bundlersCount,
          }) + '\n'
        );

        const sampleEvent: MonitorSampleEvent = {
          walletAddress: entry.walletAddress,
          mint,
          elapsedSec: elapsed,
          metrics,
          sampleNumber,
        };
        this.emit('sample', sampleEvent);

        this.evaluateFilters(entry, elapsed, metrics, sampleNumber);
      } else {
        log.warn(`Fetch failed for ${mint}: ${result.error}`);

        if (result.retryAfterMs !== undefined) {
          entry.lastFetchedAt = Date.now() + result.retryAfterMs;
        }
      }
    } finally {
      entry.pendingRequest = false;
    }
  }

  private evaluateFilters(
    entry: SchedulerEntry,
    elapsedSec: number,
    metrics: BundlerMetrics,
    sampleNumber: number
  ): void {
    if (entry.filterAlerted || entry.filterPassed) return;

    const activeProfiles: Array<{
      name: 'Massive' | 'Minimal';
      sourceWallet: string;
      settings: WalletFilterSettings;
      profile: WalletFilterProfileSettings;
      threshold: number;
    }> = [];
    for (const sourceWallet of entry.matchingWallets) {
      const settings = this.db.getWalletSettings(sourceWallet);
      if (settings.maxBundlersCountChange !== null) {
        activeProfiles.push({
          name: 'Massive',
          sourceWallet,
          settings,
          profile: settings.massive,
          threshold: settings.maxBundlersCountChange,
        });
      }
      if (settings.minBundlersCountChange !== null) {
        activeProfiles.push({
          name: 'Minimal',
          sourceWallet,
          settings,
          profile: settings.minimal,
          threshold: settings.minBundlersCountChange,
        });
      }
    }
    if (activeProfiles.length === 0) return;
    if (sampleNumber < Math.max(...activeProfiles.map((p) => p.profile.applyAtSample))) return;

    const samples = this.db
      .getLatestMetricsForWallet(entry.walletAddress, entry.mint, 10_000)
      .reverse();
    const validPercentSamples = samples.filter(
      (sample) => sample.bundlersPercent !== null && sample.bundlersPercent >= 1
    );
    const validPercents = validPercentSamples.map((sample) => sample.bundlersPercent as number);
    const counts = samples
      .map((sample) => sample.bundlersCount)
      .filter((value): value is number => value !== null);

    const reasons: string[] = [];
    const latestValidPercent = [...validPercentSamples].at(-1)?.bundlersPercent ?? null;
    const latestCount = counts.at(-1) ?? null;
    const countChange = counts.length >= 2
      ? Math.max(...counts) - Math.min(...counts)
      : null;
    if (countChange === null) return;

    for (const active of activeProfiles) {
      const prefix = `[${active.name} ${active.sourceWallet.slice(0, 4)}...${active.sourceWallet.slice(-4)}]`;
      if (active.name === 'Massive' && countChange >= active.threshold) {
        reasons.push(
          `${prefix} bundler wallet count changed by ${countChange}, threshold ${active.threshold}`
        );
      }
      if (active.name === 'Minimal' && countChange < active.threshold) {
        reasons.push(
          `${prefix} bundler wallet count changed by ${countChange}, below required ${active.threshold}`
        );
      }
      reasons.push(...this.evaluateProfileFilters(
        prefix,
        active.profile,
        latestValidPercent,
        latestCount,
        validPercents,
        samples
      ));
    }

    if (reasons.length === 0) {
      entry.filterPassed = true;
      const event: FilterPassEvent = {
        walletAddress: entry.walletAddress,
        mint: entry.mint,
        sampleNumber,
        elapsedSec,
        settings: activeProfiles[0].settings,
        metrics,
        buySol: entry.buySol,
        matchingWallets: entry.matchingWallets,
      };
      this.emit('filterPass', event);
      return;
    }

    entry.filterAlerted = true;
    const event: FilterFailEvent = {
      walletAddress: entry.walletAddress,
      mint: entry.mint,
      sampleNumber,
      elapsedSec,
      reasons,
      settings: activeProfiles[0].settings,
      metrics,
      buySol: entry.buySol,
      matchingWallets: entry.matchingWallets,
    };
    this.emit('filterFail', event);
  }

  private evaluateProfileFilters(
    prefix: string,
    settings: WalletFilterProfileSettings,
    latestValidPercent: number | null,
    latestCount: number | null,
    validPercents: number[],
    samples: BundlerMetrics[]
  ): string[] {
    const reasons: string[] = [];
    if (
      latestValidPercent !== null &&
      settings.minBundlersPercent !== null &&
      latestValidPercent < settings.minBundlersPercent
    ) {
      reasons.push(`${prefix} bundlers % ${latestValidPercent} below min ${settings.minBundlersPercent}`);
    }
    if (
      latestValidPercent !== null &&
      settings.maxBundlersPercent !== null &&
      latestValidPercent > settings.maxBundlersPercent
    ) {
      reasons.push(`${prefix} bundlers % ${latestValidPercent} above max ${settings.maxBundlersPercent}`);
    }
    if (
      latestCount !== null &&
      settings.minBundlersCount !== null &&
      latestCount < settings.minBundlersCount
    ) {
      reasons.push(`${prefix} bundlers count ${latestCount} below min ${settings.minBundlersCount}`);
    }
    if (
      latestCount !== null &&
      settings.maxBundlersCount !== null &&
      latestCount > settings.maxBundlersCount
    ) {
      reasons.push(`${prefix} bundlers count ${latestCount} above max ${settings.maxBundlersCount}`);
    }
    if (settings.maxPctAboveValue !== null && settings.maxPctAboveOccurrences !== null) {
      const occurrences = validPercents.filter((value) => value > settings.maxPctAboveValue!).length;
      if (occurrences > settings.maxPctAboveOccurrences) {
        reasons.push(
          `${prefix} ${occurrences} valid samples above ${settings.maxPctAboveValue}%, max allowed ${settings.maxPctAboveOccurrences}`
        );
      }
    }
    if (settings.maxPctBelowValue !== null && settings.maxPctBelowOccurrences !== null) {
      const occurrences = validPercents.filter((value) => value < settings.maxPctBelowValue!).length;
      if (occurrences > settings.maxPctBelowOccurrences) {
        reasons.push(
          `${prefix} ${occurrences} valid samples below ${settings.maxPctBelowValue}%, max allowed ${settings.maxPctBelowOccurrences}`
        );
      }
    }
    if (settings.sellIfFirstThreePctZero) {
      const firstThree = samples.slice(0, 3).map((sample) => sample.bundlersPercent);
      if (firstThree.length === 3 && firstThree.every((value) => value === 0)) {
        reasons.push(`${prefix} first three bundlers % samples are 0%`);
      }
    }
    if (settings.sellIfNoTeenOrTwentyPct) {
      const hasTeenOrTwenty = validPercents.some((value) => value >= 10 && value < 30);
      if (!hasTeenOrTwenty) {
        reasons.push(`${prefix} no valid bundlers % sample in the 10%-29.99% range`);
      }
    }
    return reasons;
  }
}
