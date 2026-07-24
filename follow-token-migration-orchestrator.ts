// ─────────────────────────────────────────────────────────────────────────────
//  follow-token-migration-orchestrator.ts — Listen for Pump.fun migrate events
//  via PumpPortal WebSocket, apply filters, validate first-four bundler logic,
//  then hand off to InsiderBot (follow-token flow — no follow wallet required).
// ─────────────────────────────────────────────────────────────────────────────

import { EventEmitter } from 'events';
import { createLogger } from './logger';
import { HeliusClient } from './helius-client';
import {
  InsiderBot,
  type InsiderTokenFlowEndedEvent,
} from './insider-bot';
import type { ServiceConfig } from './types';
import { TelegramBot } from './telegram-bot';
import { isExchangeFundedBy } from './pump-migrate-detector';
import { extractFirstUniqueEarlyBundlerBuys } from './wallet-swap-detector';
import { PumpPortalWsClient } from './pump-portal-ws';
import {
  hasRequiredIpfsIoBafUri,
  REQUIRED_IPFS_IO_BAF_URI_PREFIX,
  TokenMetaplexMetadataClient,
} from './token-metaplex-metadata';

const log = createLogger('FOLLOW-TOKEN');

const PUMP_MINT_SUFFIX = 'pump';
const DEFAULT_MAX_MIGRATION_AGE_SEC = 5;
const REQUIRED_BUNDLER_COUNT = 4;
/** Accepted dev CREATE history counts (Helius fee-payer CREATE txs). */
const FOLLOW_TOKEN_DEV_CREATE_COUNT_MIN = 1;
const FOLLOW_TOKEN_DEV_CREATE_COUNT_MAX = 3;
/** Delays before retrying Helius when mint CREATE / early SWAP data is not indexed yet. */
const HELIUS_INDEXING_RETRY_DELAYS_MS = [4_000, 8_000];

interface EarlyMigrationFilterContext {
  uri: string;
  metadataUrl: string;
  metadataPda: string;
}

interface CoreMigrationFilterContext {
  devWallet: string;
  migrationAgeSec: number;
  funding: Awaited<ReturnType<HeliusClient['getWalletFundedBy']>>;
  metadata: EarlyMigrationFilterContext;
  devCreateCount: number;
}

export class FollowTokenMigrationOrchestrator extends EventEmitter {
  private readonly heliusClient: HeliusClient;
  private readonly metadataClient: TokenMetaplexMetadataClient;
  private readonly insiderBots: InsiderBot[];
  private readonly maxMigrationAgeSec: number;
  private pumpPortalWs: PumpPortalWsClient | null = null;
  private isEnabled = false;
  private isShuttingDown = false;
  private activeFollowTokenMint: string | null = null;
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
    this.metadataClient = new TokenMetaplexMetadataClient(heliusKey);
    this.maxMigrationAgeSec =
      config.insiderFollowTokenMaxMigrationAgeSec > 0
        ? config.insiderFollowTokenMaxMigrationAgeSec
        : DEFAULT_MAX_MIGRATION_AGE_SEC;

    insiderBots.forEach((bot) => {
      bot.on('tokenFlowEnded', (event: InsiderTokenFlowEndedEvent) => {
        this.handleInsiderTokenFlowEnded(event);
      });
    });
  }

  isRunning(): boolean {
    return this.isEnabled;
  }

  async start(): Promise<void> {
    if (this.isEnabled || this.isShuttingDown) return;
    if (!this.config.pumpPortalApiKey) {
      throw new Error('Follow-token requires PUMPPORTAL_API_KEY in .env');
    }
    this.pumpPortalWs?.close();
    this.pumpPortalWs = new PumpPortalWsClient(
      this.config.pumpPortalApiKey,
      'Follow-Token PumpPortal',
    );
    this.isEnabled = true;
    this.activeFollowTokenMint = null;
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
      `Filters: mint ends <b>${PUMP_MINT_SUFFIX}</b>, metadata URI via <b>${REQUIRED_IPFS_IO_BAF_URI_PREFIX}…</b>, dev created <b>${FOLLOW_TOKEN_DEV_CREATE_COUNT_MIN}–${FOLLOW_TOKEN_DEV_CREATE_COUNT_MAX}</b> tokens (Helius CREATE history), migrate ≤ <b>${this.maxMigrationAgeSec}s</b> after create, dev funded by <b>Centralized Exchange</b>, first-four bundler logic.`,
      'PumpPortal migration feed unsubscribes while a follow-token bundler-funder flow is active; resubscribes when the token is skipped or reset (not after dev rug alone).',
    ]);
  }

  stop(reason = 'Stopped'): void {
    if (!this.isEnabled) return;
    this.isEnabled = false;
    this.activeFollowTokenMint = null;
    this.pumpPortalWs?.close();
    this.pumpPortalWs = null;
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

  private handleInsiderTokenFlowEnded(event: InsiderTokenFlowEndedEvent): void {
    if (event.source !== 'follow-token') return;
    if (!this.isEnabled || !this.pumpPortalWs) return;
    if (
      this.activeFollowTokenMint &&
      event.mint &&
      event.mint !== this.activeFollowTokenMint
    ) {
      return;
    }

    const endedMint = event.mint ?? this.activeFollowTokenMint;
    this.activeFollowTokenMint = null;

    if (!this.pumpPortalWs.isMigrationFeedSuspended()) {
      log.debug('Follow-token flow ended but PumpPortal migration feed was not suspended', {
        mint: endedMint,
        reason: event.reason,
        hadPosition: event.hadPosition,
        feePayer: event.feePayer,
      });
      return;
    }

    this.pumpPortalWs.resumeMigrationFeed(
      `follow-token flow ended (${event.reason}${event.hadPosition ? ', had position' : ''})`,
    );
    log.info(
      'Follow-token flow ended — token watches torn down; PumpPortal migration feed resubscribed',
      {
        mint: endedMint,
        reason: event.reason,
        hadPosition: event.hadPosition,
        feePayer: event.feePayer,
      },
    );
  }

  suspendMigrationFeedForActiveFlow(mint: string): void {
    this.unsubscribeMigrationFeedForActiveFlow(mint);
  }

  private unsubscribeMigrationFeedForActiveFlow(mint: string): void {
    this.activeFollowTokenMint = mint;
    this.pumpPortalWs?.suspendMigrationFeed(
      'follow-token bundler-funder flow started',
    );
    log.info('Follow-token bundler-funder flow started — PumpPortal migration feed unsubscribed', {
      mint,
    });
  }

  private async processMigrationCandidate(
    mint: string,
    signature: string,
    migrationTimestamp: number,
  ): Promise<void> {
    if (!this.isEnabled) return;
    if (this.activeFollowTokenMint || this.pumpPortalWs?.isMigrationFeedSuspended()) {
      log.debug('Follow-token migration ignored — PumpPortal migration feed unsubscribed (active follow-token flow)', {
        mint,
        signature,
        activeMint: this.activeFollowTokenMint,
      });
      return;
    }
    if (this.seenMigrationSignatures.has(signature)) {
      log.debug('Follow-token migration skipped — duplicate signature', {
        mint,
        signature,
      });
      return;
    }
    this.seenMigrationSignatures.add(signature);
    if (this.seenMigrationMints.has(mint)) {
      log.debug('Follow-token migration skipped — mint already processed', {
        mint,
        signature,
      });
      return;
    }
    if (this.inFlightMints.has(mint)) {
      log.debug('Follow-token migration skipped — mint already in flight', {
        mint,
        signature,
      });
      return;
    }
    this.inFlightMints.add(mint);

    try {
      const earlyResult = await this.evaluateEarlyMigrationFilters(mint);
      if (!earlyResult.ok) {
        this.seenMigrationMints.add(mint);
        log.debug('Follow-token migration skipped — early filter', {
          mint,
          signature,
          skipReason: earlyResult.reason,
        });
        return;
      }

      log.info('Follow-token migration passed early filters (pump + metadata URI)', {
        mint,
        signature,
        metadataUrl: earlyResult.metadata.metadataUrl,
        uri: earlyResult.metadata.uri,
        metadataPda: earlyResult.metadata.metadataPda,
      });

      const coreResult = await this.evaluateCoreMigrationFilters(
        mint,
        migrationTimestamp,
        earlyResult.metadata,
      );
      if (typeof coreResult === 'string') {
        this.seenMigrationMints.add(mint);
        log.info('Follow-token migration skipped — core filter', {
          mint,
          signature,
          skipReason: coreResult,
        });
        return;
      }

      const { devWallet, migrationAgeSec, funding, devCreateCount } = coreResult;
      log.info('Follow-token migration passed core filters', {
        mint,
        signature,
        devWallet,
        migrationAgeSec,
        devCreateCount,
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
          'First-four unique SWAP buys confirmed — starting GMGN bundler watch…',
        ]);
      }

      const started = await this.tryStartFollowTokenFlow(mint, signature);
      this.seenMigrationMints.add(mint);
      if (started) {
        this.unsubscribeMigrationFeedForActiveFlow(mint);
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

  /** Gate 1: mint suffix + Helius DAS metadata URI (info logs start after both pass). */
  private async evaluateEarlyMigrationFilters(
    mint: string,
  ): Promise<
    | { ok: true; metadata: EarlyMigrationFilterContext }
    | { ok: false; reason: string }
  > {
    if (!mint.endsWith(PUMP_MINT_SUFFIX)) {
      return { ok: false, reason: 'mint does not end in pump' };
    }

    const metadataResult = await this.fetchMetadataUriWithRetry(mint);
    if (!metadataResult.ok) {
      return { ok: false, reason: metadataResult.reason };
    }

    return {
      ok: true,
      metadata: {
        uri: metadataResult.uri,
        metadataUrl: metadataResult.metadataUrl,
        metadataPda: metadataResult.metadataPda,
      },
    };
  }

  /** Core filters: CREATE tx, migrate age, dev create count, exchange funder. */
  private async evaluateCoreMigrationFilters(
    mint: string,
    migrationTimestamp: number,
    metadata: EarlyMigrationFilterContext,
  ): Promise<string | CoreMigrationFilterContext> {
    const createTx = await this.fetchMintCreateTransactionWithRetry(mint);
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
      await this.heliusClient.countDevCreatedTokenMints(devWallet);
    if (
      devCreateCount < FOLLOW_TOKEN_DEV_CREATE_COUNT_MIN ||
      devCreateCount > FOLLOW_TOKEN_DEV_CREATE_COUNT_MAX
    ) {
      return `dev created ${devCreateCount} tokens in Helius CREATE history (expected ${FOLLOW_TOKEN_DEV_CREATE_COUNT_MIN}–${FOLLOW_TOKEN_DEV_CREATE_COUNT_MAX})`;
    }

    const funding = await this.heliusClient.getWalletFundedBy(devWallet);
    if (!isExchangeFundedBy(funding)) {
      return `dev funder is not a Centralized Exchange (${funding?.funderType ?? funding?.funder ?? 'unknown'})`;
    }

    return { devWallet, migrationAgeSec, funding, metadata, devCreateCount };
  }

  private async fetchMetadataUriWithRetry(
    mint: string,
  ): Promise<
    | { ok: true; uri: string; metadataUrl: string; metadataPda: string }
    | { ok: false; reason: string; uri?: string; metadataUrl?: string }
  > {
    for (
      let attempt = 0;
      attempt <= HELIUS_INDEXING_RETRY_DELAYS_MS.length;
      attempt += 1
    ) {
      const metadata = await this.metadataClient.fetchTokenMetadataUri(mint);
      if (metadata) {
        if (hasRequiredIpfsIoBafUri(metadata.metadataUrl)) {
          return {
            ok: true,
            uri: metadata.uri,
            metadataUrl: metadata.metadataUrl,
            metadataPda: metadata.metadataPda,
          };
        }

        return {
          ok: false,
          reason: `metadata uri does not start with ${REQUIRED_IPFS_IO_BAF_URI_PREFIX} (${metadata.metadataUrl})`,
          uri: metadata.uri,
          metadataUrl: metadata.metadataUrl,
        };
      }

      const delayMs = HELIUS_INDEXING_RETRY_DELAYS_MS[attempt];
      if (delayMs === undefined) break;

      log.debug('Helius DAS getAsset metadata not indexed yet — retrying', {
        mint,
        attempt: attempt + 1,
        retryInMs: delayMs,
      });
      await new Promise((resolve) => setTimeout(resolve, delayMs));
    }

    return { ok: false, reason: 'Helius DAS getAsset metadata uri not found' };
  }

  private async fetchMintCreateTransactionWithRetry(
    mint: string,
  ): Promise<Awaited<ReturnType<HeliusClient['getMintCreateTransaction']>>> {
    for (
      let attempt = 0;
      attempt <= HELIUS_INDEXING_RETRY_DELAYS_MS.length;
      attempt += 1
    ) {
      const createTx = await this.heliusClient.getMintCreateTransaction(mint);
      if (createTx?.timestamp) {
        if (attempt > 0) {
          log.info('Mint CREATE transaction found after Helius indexing retry', {
            mint,
            attempt,
            createSignature: createTx.signature,
          });
        }
        return createTx;
      }

      const delayMs = HELIUS_INDEXING_RETRY_DELAYS_MS[attempt];
      if (delayMs === undefined) break;

      log.info('Mint CREATE transaction not indexed yet — retrying', {
        mint,
        attempt: attempt + 1,
        retryInMs: delayMs,
      });
      await new Promise((resolve) => setTimeout(resolve, delayMs));
    }

    return null;
  }

  private async validateFirstFourBundlers(mint: string): Promise<boolean> {
    for (let attempt = 0; attempt <= HELIUS_INDEXING_RETRY_DELAYS_MS.length; attempt += 1) {
      try {
        const swaps = await this.heliusClient.getEarlyInsiderSwaps(mint, REQUIRED_BUNDLER_COUNT);
        const earlyBuys = extractFirstUniqueEarlyBundlerBuys(swaps, mint, REQUIRED_BUNDLER_COUNT);
        if (earlyBuys.length >= REQUIRED_BUNDLER_COUNT) {
          return true;
        }
      } catch (err) {
        log.info('First-four bundler fetch not ready yet', {
          mint,
          attempt,
          error: err instanceof Error ? err.message : String(err),
        });
      }
      const delayMs = HELIUS_INDEXING_RETRY_DELAYS_MS[attempt];
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
