// ─────────────────────────────────────────────────────────────────────────────
//  follow-token-migration-orchestrator.ts — Listen for Pump.fun migrate events
//  via PumpPortal WebSocket, apply filters, validate first-four bundler logic,
//  then hand off to InsiderBot (follow-token flow — no follow wallet required).
// ─────────────────────────────────────────────────────────────────────────────

import { EventEmitter } from 'events';
import { createLogger } from './logger';
import { HeliusClient } from './helius-client';
import { InsiderBot } from './insider-bot';
import type { ServiceConfig } from './types';
import { TelegramBot } from './telegram-bot';
import { isExchangeFundedBy } from './pump-migrate-detector';
import { extractFirstUniqueEarlyBundlerBuys } from './wallet-swap-detector';
import { PumpPortalWsClient } from './pump-portal-ws';
import { BitqueryClient } from './bitquery-client';

const log = createLogger('FOLLOW-TOKEN');

const PUMP_MINT_SUFFIX = 'pump';
const DEFAULT_MAX_MIGRATION_AGE_SEC = 60;
const REQUIRED_BUNDLER_COUNT = 4;
const BUNDLER_INDEXING_RETRY_DELAYS_MS = [4_000, 8_000];

interface CoreMigrationFilterContext {
  devWallet: string;
  migrationAgeSec: number;
  funding: Awaited<ReturnType<HeliusClient['getWalletFundedBy']>>;
}

export class FollowTokenMigrationOrchestrator extends EventEmitter {
  private readonly heliusClient: HeliusClient;
  private readonly bitqueryClient: BitqueryClient | null;
  private readonly insiderBots: InsiderBot[];
  private readonly maxMigrationAgeSec: number;
  private pumpPortalWs: PumpPortalWsClient | null = null;
  private isEnabled = false;
  private isShuttingDown = false;
  private readonly seenMigrationMints = new Set<string>();
  private readonly seenMigrationSignatures = new Set<string>();
  private readonly inFlightMints = new Set<string>();

  constructor(
    private readonly config: ServiceConfig,
    insiderBots: InsiderBot[],
    private readonly telegramBot: TelegramBot | null = null,
  ) {
    super();
    this.insiderBots = insiderBots;
    const heliusKey = config.insiderHeliusApiKey || config.heliusApiKey;
    this.heliusClient = new HeliusClient(heliusKey, {
      label: 'Follow-Token Helius',
      projectId: config.insiderHeliusProjectId || undefined,
    });
    this.bitqueryClient = config.bitqueryAccessToken
      ? new BitqueryClient(config.bitqueryAccessToken)
      : null;
    this.maxMigrationAgeSec =
      config.insiderFollowTokenMaxMigrationAgeSec > 0
        ? config.insiderFollowTokenMaxMigrationAgeSec
        : DEFAULT_MAX_MIGRATION_AGE_SEC;
  }

  isRunning(): boolean {
    return this.isEnabled;
  }

  async start(): Promise<void> {
    if (this.isEnabled || this.isShuttingDown) return;
    if (!this.config.pumpPortalApiKey) {
      throw new Error('Follow-token requires PUMPPORTAL_API_KEY in .env');
    }
    if (!this.bitqueryClient) {
      throw new Error('Follow-token requires BITQUERY_ACCESS_TOKEN in .env');
    }
    this.pumpPortalWs?.close();
    this.pumpPortalWs = new PumpPortalWsClient(
      this.config.pumpPortalApiKey,
      'Follow-Token PumpPortal',
    );
    this.isEnabled = true;
    this.pumpPortalWs.onMigration((event) => {
      void this.processMigrationCandidate(
        event.mint,
        event.signature,
        event.timestamp,
      );
    });
    this.pumpPortalWs.connect();
    log.info('Follow-token migration listener started', {
      source: 'PumpPortal subscribeMigration',
      maxMigrationAgeSec: this.maxMigrationAgeSec,
    });
    void this.sendTelegram([
      '<b>▶️ Follow-Token: Pump Migration Listener Started</b>',
      'Source: <b>PumpPortal subscribeMigration</b>',
      `Filters: mint ends <b>${PUMP_MINT_SUFFIX}</b>, dev created exactly 1 Pump.fun token (Bitquery), migrate ≤ <b>${this.maxMigrationAgeSec}s</b> after create, dev funded by <b>Centralized Exchange</b>, first-four bundler logic.`,
    ]);
  }

  stop(reason = 'Stopped'): void {
    if (!this.isEnabled) return;
    this.isEnabled = false;
    this.pumpPortalWs?.close();
    log.info('Follow-token migration listener stopped', { reason });
    void this.sendTelegram([
      `<b>⏹️ Follow-Token: Migration Listener Stopped</b>`,
      reason,
    ]);
  }

  async shutdown(): Promise<void> {
    this.isShuttingDown = true;
    this.stop('Process shutdown');
  }

  private async processMigrationCandidate(
    mint: string,
    signature: string,
    migrationTimestamp: number,
  ): Promise<void> {
    if (!this.isEnabled) return;
    if (this.seenMigrationSignatures.has(signature)) return;
    this.seenMigrationSignatures.add(signature);
    if (this.seenMigrationMints.has(mint) || this.inFlightMints.has(mint)) return;
    this.inFlightMints.add(mint);

    try {
      const coreResult = await this.evaluateCoreMigrationFilters(
        mint,
        migrationTimestamp,
      );
      if (typeof coreResult === 'string') {
        this.seenMigrationMints.add(mint);
        log.debug('Follow-token migration skipped before core filters', {
          skipReason: coreResult,
        });
        return;
      }

      const { devWallet, migrationAgeSec, funding } = coreResult;
      log.info('Follow-token migration passed core filters', {
        mint,
        signature,
        devWallet,
        migrationAgeSec,
        devFunder: funding?.funder ?? null,
        devFunderName: funding?.funderName ?? null,
        devFunderType: funding?.funderType ?? null,
      });

      const bundlerOk = await this.validateFirstFourBundlers(mint);
      if (!bundlerOk) {
        this.seenMigrationMints.add(mint);
        log.info('Follow-token migration skipped — first-four bundlers not ready', {
          mint,
          signature,
        });
        return;
      }

      if (this.config.insiderFollowTokenEnabled) {
        void this.sendMigrationTelegram([
          '<b>✅ Follow-Token: Migration Filters Passed</b>',
          `Token: <code>${this.html(mint)}</code>`,
          `Migrate tx: <code>${this.html(signature)}</code>`,
          `Dev: <code>${this.html(devWallet)}</code>`,
          `Create → migrate: <b>${migrationAgeSec.toFixed(0)}s</b>`,
          `Dev funder: <code>${this.html(funding?.funder ?? 'unknown')}</code>${funding?.funderName ? ` (${this.html(funding.funderName)})` : ''}`,
          'First-four unique SWAP buys confirmed — starting bundler-funder monitoring…',
        ]);
      }

      const started = await this.tryStartFollowTokenFlow(mint, signature);
      this.seenMigrationMints.add(mint);
      if (started) {
        log.info('Follow-token migration passed all filters and started insider flow', {
          mint,
          signature,
        });
      }
    } catch (err) {
      log.warn('Follow-token migration processing failed', {
        mint,
        signature,
        error: err instanceof Error ? err.message : String(err),
      });
    } finally {
      this.inFlightMints.delete(mint);
    }
  }

  /** Core filters only — mint suffix, single dev create, migrate age, exchange funder. */
  private async evaluateCoreMigrationFilters(
    mint: string,
    migrationTimestamp: number,
  ): Promise<string | CoreMigrationFilterContext> {
    if (!mint.endsWith(PUMP_MINT_SUFFIX)) {
      return 'mint does not end in pump';
    }

    const createTx = await this.heliusClient.getMintCreateTransaction(mint);
    if (!createTx?.timestamp) {
      return 'mint create transaction not found';
    }
    const devWallet = createTx.feePayer ?? null;
    if (!devWallet) {
      return 'dev wallet unknown on create tx';
    }

    const migrationAgeSec = migrationTimestamp - createTx.timestamp;
    if (!Number.isFinite(migrationAgeSec) || migrationAgeSec < 0) {
      return 'invalid migration/create timestamps';
    }
    if (migrationAgeSec > this.maxMigrationAgeSec) {
      return `migration ${migrationAgeSec.toFixed(0)}s after create (max ${this.maxMigrationAgeSec}s)`;
    }

    const devCreateCount =
      await this.bitqueryClient!.countPumpFunTokensCreatedByWallet(devWallet);
    if (devCreateCount !== 1) {
      return `dev created ${devCreateCount} Pump.fun tokens (expected exactly 1)`;
    }

    const funding = await this.heliusClient.getWalletFundedBy(devWallet);
    if (!isExchangeFundedBy(funding)) {
      return `dev funder is not a Centralized Exchange (${funding?.funderType ?? funding?.funder ?? 'unknown'})`;
    }

    return { devWallet, migrationAgeSec, funding };
  }

  private async validateFirstFourBundlers(mint: string): Promise<boolean> {
    for (let attempt = 0; attempt <= BUNDLER_INDEXING_RETRY_DELAYS_MS.length; attempt += 1) {
      try {
        const swaps = await this.heliusClient.getEarlyInsiderSwaps(mint, REQUIRED_BUNDLER_COUNT);
        const earlyBuys = extractFirstUniqueEarlyBundlerBuys(swaps, mint, REQUIRED_BUNDLER_COUNT);
        if (earlyBuys.length >= REQUIRED_BUNDLER_COUNT) {
          return true;
        }
      } catch (err) {
        log.debug('First-four bundler fetch not ready yet', {
          mint,
          attempt,
          error: err instanceof Error ? err.message : String(err),
        });
      }
      const delayMs = BUNDLER_INDEXING_RETRY_DELAYS_MS[attempt];
      if (delayMs === undefined) break;
      await new Promise((resolve) => setTimeout(resolve, delayMs));
    }
    return false;
  }

  private pickIdleInsiderBot(): InsiderBot | null {
    for (const bot of this.insiderBots) {
      if (bot.isStoppedForHeliusCredits()) continue;
      if (bot.isIdleForFunderFirst()) return bot;
    }
    return null;
  }

  private async tryStartFollowTokenFlow(
    mint: string,
    migrationSignature: string,
  ): Promise<boolean> {
    const targetBot = this.pickIdleInsiderBot();
    if (!targetBot) {
      log.warn('Follow-token handoff delayed — all insider bots busy', { mint });
      void this.sendMigrationTelegram([
        '<b>⏳ Follow-Token: Handoff Delayed</b>',
        `Token: <code>${this.html(mint)}</code>`,
        'All insider bots are busy — migration skipped for this session.',
      ]);
      return false;
    }

    const started = await targetBot.startFromFollowTokenMigration(
      mint,
      migrationSignature,
    );
    if (!started) {
      void this.sendMigrationTelegram([
        '<b>⏭️ Follow-Token: Insider Flow Not Started</b>',
        `Token: <code>${this.html(mint)}</code>`,
        'Idle bot rejected handoff (mint claimed or filter failed).',
      ]);
    }
    return started;
  }

  private html(value: string): string {
    return value
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;');
  }

  private async sendTelegram(lines: string[]): Promise<void> {
    if (!this.telegramBot) return;
    try {
      await this.telegramBot.sendDefault(lines.join('\n'));
    } catch (err) {
      log.warn('Follow-token Telegram notification failed', {
        error: err instanceof Error ? err.message : String(err),
      });
    }
  }

  /** Token-level TG alerts — only when INSIDER_FOLLOW_TOKEN_ENABLED=true. */
  private async sendMigrationTelegram(lines: string[]): Promise<void> {
    if (!this.config.insiderFollowTokenEnabled) return;
    await this.sendTelegram(lines);
  }
}
