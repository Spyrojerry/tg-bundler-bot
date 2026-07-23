// ─────────────────────────────────────────────────────────────────────────────
//  scheduler.ts  —  Single-loop scheduler for GMGN metric fetches
//
//  Linked trading-wallet tokens are monitored through sample #20 and summarized.
// ─────────────────────────────────────────────────────────────────────────────

import { createLogger } from './logger';
import { EventEmitter } from 'events';
import { GmgnClient } from './gmgn-client';
import { MonitorDatabase } from './database';
import { RateLimiter } from './rate-limiter';
import {
  BundlerMetrics,
  FilterFailEvent,
  MonitorSampleEvent,
  NewTokenEvent,
  SchedulerEntry,
  ServiceConfig,
  TokenSummary,
} from './types';

const log = createLogger('SCHED');

const SCHEDULER_TICK_MS = 500; // internal resolution — half the min interval
const SCHEDULER_POLL_INTERVAL_MS = 2_000;
const DEFAULT_APPLY_SAMPLE = 20;

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
      `Scheduler started — interval ${SCHEDULER_POLL_INTERVAL_MS}ms, ` +
      `sample-count driven summaries`
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

    log.info(
      `Scheduler: tracking ${event.mint} until apply-sample decision ` +
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
    return Math.floor(SCHEDULER_POLL_INTERVAL_MS / this.limiter.currentDelay);
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

      // ── 2. Adaptive effective interval ────────────────────────────────────
      const activeEntries = [...this.entries.values()];
      const effectiveInterval = Math.max(
        SCHEDULER_POLL_INTERVAL_MS,
        activeEntries.length * this.limiter.currentDelay
      );

      // ── 3. Collect tokens due for a fetch ─────────────────────────────────
      const due: SchedulerEntry[] = [];
      for (const entry of activeEntries) {
        const elapsed = now - entry.monitoringStartedAt;
        if (
          !entry.pendingRequest &&
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

  // ── Decision summary ──────────────────────────────────────────────────────

  private async expireToken(entry: SchedulerEntry): Promise<void> {
    const { mint } = entry;

    // Remove first so no more fetches are queued
    this.removeEntry(entry);

    // Update DB status
    this.db.updateTokenStatus(entry.walletAddress, mint, 'stopped');

    // Fetch ALL samples recorded before the apply-sample decision.
    const samples = this.db.getLatestMetricsForWallet(
      entry.walletAddress,
      mint,
      10_000
    ); // effectively all
    // getLatestMetrics returns newest-first; reverse for chronological order
    samples.reverse();

    const summary = this.buildSummary(
      entry.walletAddress,
      mint,
      samples,
      Math.max(0, Date.now() - entry.monitoringStartedAt)
    );
    this.logSummary(summary);
    this.emit('summary', summary);
  }

  // ── Summary computation ───────────────────────────────────────────────────

  private buildSummary(
    walletAddress: string,
    mint: string,
    samples: BundlerMetrics[],
    windowMs: number
  ): TokenSummary {
    const percents = samples
      .map((s) => s.bundlersPercent)
      .filter((v): v is number => v !== null);
    const validPercents = percents.filter((v) => v >= 1);

    const counts = samples
      .map((s) => s.bundlersCount)
      .filter((v): v is number => v !== null);
    const topWallets = samples
      .map((s) => s.topWallets)
      .filter((v): v is number => v !== null);
    const top10HolderRates = samples
      .map((s) => s.top10HolderRate)
      .filter((v): v is number => v !== null);

    return {
      walletAddress,
      mint,
      windowMs,
      totalSamples: samples.length,
      firstSeen:    samples.at(0)?.timestamp  ?? new Date().toISOString(),
      lastSeen:     samples.at(-1)?.timestamp ?? new Date().toISOString(),
      bundlersPercent: {
        first: validPercents.at(0)  ?? null,
        last:  percents.at(-1) ?? null,
        min:   validPercents.length ? Math.min(...validPercents) : null,
        max:   validPercents.length ? Math.max(...validPercents) : null,
      },
      bundlersCount: {
        first: counts.at(0)  ?? null,
        last:  counts.at(-1) ?? null,
        min:   counts.length ? Math.min(...counts) : null,
        max:   counts.length ? Math.max(...counts) : null,
      },
      initialBaseReserve: samples.find((sample) => sample.initialBaseReserve !== null)?.initialBaseReserve ?? null,
      topWallets: {
        first: topWallets.at(0) ?? null,
        last: topWallets.at(-1) ?? null,
        min: topWallets.length ? Math.min(...topWallets) : null,
        max: topWallets.length ? Math.max(...topWallets) : null,
      },
      top10HolderRate: {
        first: top10HolderRates.at(0) ?? null,
        last: top10HolderRates.at(-1) ?? null,
        min: top10HolderRates.length ? Math.min(...top10HolderRates) : null,
        max: top10HolderRates.length ? Math.max(...top10HolderRates) : null,
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
      `║  Top Wallets`,
      `║    First : ${fmt(s.topWallets.first)}   Last : ${fmt(s.topWallets.last)}`,
      `║    Min   : ${fmt(s.topWallets.min)}   Max  : ${fmt(s.topWallets.max)}`,
      `║  Top 10 Holder %`,
      `║    First : ${fmt(s.top10HolderRate.first, '%')}   Last : ${fmt(s.top10HolderRate.last, '%')}`,
      `║    Min   : ${fmt(s.top10HolderRate.min, '%')}   Max  : ${fmt(s.top10HolderRate.max, '%')}`,
      `╚══════════════════════════════════════════════════════════╝`,
      ``,
    ];

    for (const line of lines) {
      log.info(line);
    }

    process.stdout.write(JSON.stringify({
      event: 'summary',
      walletAddress: s.walletAddress,
      mint: s.mint,
      windowMs: s.windowMs,
      totalSamples: s.totalSamples,
      firstSeen: s.firstSeen,
      lastSeen: s.lastSeen,
      bundlersPercent: s.bundlersPercent,
      bundlersCount: s.bundlersCount,
      topWallets: s.topWallets,
      top10HolderRate: s.top10HolderRate,
    }) + '\n');
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
          `Count: ${metrics.bundlersCount ?? 'N/A'}  ` +
          `TopWallets: ${metrics.topWallets ?? 'N/A'}  ` +
          `Top10Holder%: ${metrics.top10HolderRate ?? 'N/A'}`,
          {
            mint,
            time:            metrics.timestamp,
            bundlersPercent: metrics.bundlersPercent,
            bundlersCount:   metrics.bundlersCount,
            topWallets: metrics.topWallets,
            top10HolderRate: metrics.top10HolderRate,
          }
        );

        process.stdout.write(
          JSON.stringify({
            mint,
            time:            metrics.timestamp,
            bundlersPercent: metrics.bundlersPercent,
            bundlersCount:   metrics.bundlersCount,
            topWallets: metrics.topWallets,
            top10HolderRate: metrics.top10HolderRate,
          }) + '\n'
        );

        const sampleEvent: MonitorSampleEvent = {
          walletAddress: entry.walletAddress,
          mint,
          elapsedSec: elapsed,
          metrics,
          sampleNumber,
          matchingWallets: entry.matchingWallets,
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
    if (!entry.filterAlerted && entry.matchingWallets.length > 0 && (sampleNumber === 2 || sampleNumber === 3)) {
      const samples = this.db
        .getLatestMetricsForWallet(entry.walletAddress, entry.mint, 3)
        .reverse();
      const topWallets = samples.map((sample) => sample.topWallets);
      const first = topWallets[0] ?? null;
      const second = topWallets[1] ?? null;
      const validFirst = first === 0 || first === 1;
      const validSecond = second === 1 || second === 3;
        const shouldSell = sampleNumber === 2
          ? !validFirst || !validSecond
          : !validFirst || !validSecond || topWallets[2] !== second;

        if (shouldSell) {
          entry.filterAlerted = true;
          const settings = this.db.getWalletSettings(entry.matchingWallets[0]);
          const expected = sampleNumber === 2
            ? '#1 must be 0 or 1, and #2 must be 1 or 3'
            : '#1 must be 0 or 1, and #2-#3 must be 1-1 or 3-3';
          log.warn(`[FILTER FAIL] ${entry.mint} sample #${sampleNumber}; top wallets ${topWallets.map((value) => value ?? 'N/A').join(' -> ')}`, {
            walletAddress: entry.walletAddress,
            mint: entry.mint,
            matchingWallets: entry.matchingWallets,
            sampleNumber,
            topWallets,
            expected,
          });
          const event: FilterFailEvent = {
            walletAddress: entry.walletAddress,
            mint: entry.mint,
            sampleNumber,
            elapsedSec,
          reasons: [
            `Top wallets pattern failed by sample #${sampleNumber}: observed ${topWallets.map((value) => value ?? 'N/A').join(' -> ')}; expected ${expected}.`,
          ],
          settings,
          metrics,
          buySol: entry.buySol,
          matchingWallets: entry.matchingWallets,
        };
        this.emit('filterFail', event);
      } else {
        log.info(`[FILTER HOLD] ${entry.mint} sample #${sampleNumber}; top wallets ${topWallets.map((value) => value ?? 'N/A').join(' -> ')}`, {
          walletAddress: entry.walletAddress,
          mint: entry.mint,
          matchingWallets: entry.matchingWallets,
          sampleNumber,
          topWallets,
        });
      }
    }

    if (sampleNumber >= DEFAULT_APPLY_SAMPLE) {
      this.expireToken(entry).catch((err) =>
        log.error(`Sample #${DEFAULT_APPLY_SAMPLE} summary error for ${entry.mint}`, err)
      );
    }
  }
}
