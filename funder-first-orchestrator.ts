// ─────────────────────────────────────────────────────────────────────────────
//  funder-first-orchestrator.ts — Parallel "feePayer funder first" discovery
//
//  Runs alongside the existing follow-wallet → backtrack feePayer flow.
//  The user supplies a top-level feePayer *funder* wallet; this orchestrator:
//    1. Always watches that funder for SOL transfer-outs (Enhanced WSS).
//    2. For each new recipient, opens a potential-feePayer watch (also WSS).
//    3. Tracks bundler-funding patterns on each potential feePayer:
//         • 4 recipients within 10s whose *post-balance* is ≥20 SOL.
//    4. Subscribes to those recipient wallets and waits for a SWAP buy.
//    5. Confirms at least one recipient is among the token's first-four bundlers.
//    6. Normal → hands off to InsiderBot.startFromFunderFirst for buy/sell.
//    7. After a normal trade completes, the feePayer stays in cooldown until the
//       token's dev wallet CLOSE_ACCOUNTs or native SOL reaches zero (rug), then
//       resumes watching. On cooldown entry, recent dev txs are REST-synced so a
//       rug is not missed if it landed before the dev Enhanced WSS subscription
//       was armed; dev SOL balance is also subscribed immediately.
//
//  Group / recipient rules:
//    • Multiple 10s bundler groups (≥20 SOL only) can be monitored concurrently.
//    • A valid group is exactly 4 unique recipients in 10s whose post-balances
//      are all within 0.5 SOL of each other (all ≥20 SOL). If 5+ recipients in
//      the window meet the tolerance, the window is skipped entirely.
//    • Sub-20 SOL bundler sends are ignored (no Telegram, no backend info logs).
//    • Keep watching the feePayer for new groups until a recipient buy overlaps
//      the token's first-four bundlers.
//    • Per recipient in the active group: stop watching when native SOL → zero
//      (unless a token buy was already seen — then keep monitoring for buy logic).
//    • Follow wallet merged with a bundler recipient uses one Enhanced WSS watch.
//
//  Stop-watching rules for a potential feePayer wallet itself:
//    • On any outgoing SOL transfer ≤50% of the initial funder-receive balance,
//      resolve the recipient via Helius wallet identity; if type is "exchange",
//      stop and unsubscribe immediately.
//    • Keep monitoring until native SOL balance hits zero.
//    • On zero: stop and unsubscribe (no handoff to drain recipient).
//    • Telegram /start menu can fast-track a potential feePayer manually
//      (same pipeline as a funder SOL transfer-out, using current balance baseline).
// ─────────────────────────────────────────────────────────────────────────────

import { Connection, LAMPORTS_PER_SOL, PublicKey } from '@solana/web3.js';
import { EventEmitter } from 'events';
import { createLogger } from './logger';
import { HeliusClient, HeliusTransaction, HeliusWalletIdentity } from './helius-client';
import { HeliusEnhancedWsClient } from './helius-enhanced-ws';
import {
  FunderFirstEarlyBuy,
  InsiderBot,
  InsiderTokenFlowEndedEvent,
} from './insider-bot';
import type { ServiceConfig } from './types';
import { TelegramBot } from './telegram-bot';
import { isDevRugCloseAccountTx, UNKNOWN_COUNTERPARTY } from './tx-normalizer';
import { findWalletSwapBuyMint } from './wallet-swap-detector';

const log = createLogger('FUNDER-FIRST');

const SOL_MINT = 'So11111111111111111111111111111111111111112';
const GROUP_WINDOW_SEC = 10;
const BUNDLER_GROUP_SIZE = 4;
const NORMAL_MIN_POST_SOL = 20;
const ZERO_BALANCE_EPSILON_SOL = 1e-6;
/** Recipients in a 4-bundler group must have post-balances within this spread (SOL). */
const POST_BALANCE_TOLERANCE_SOL = 0.5;
const POTENTIAL_FEEPAYER_SYNC_LIMIT = 20;
const POTENTIAL_FEEPAYER_SYNC_MIN_INTERVAL_MS = 1_000;
const COOLDOWN_DEV_SYNC_LIMIT = 30;
const COOLDOWN_DEV_SYNC_MIN_INTERVAL_MS = 1_000;
/** Outgoing transfers up to this fraction of initial funder-receive balance trigger exchange-identity checks. */
const EXCHANGE_DRAIN_CHECK_MAX_FRACTION = 0.5;

type PotentialFeePayerStatus =
  | 'watching'
  | 'normal_candidate'
  | 'active'
  | 'cooldown'
  | 'stopped';

interface BundlerFundingEvent {
  recipient: string;
  amountSol: number;
  recipientPostBalanceSol: number;
  signature: string;
  timestamp: number;
}

interface ActiveBundlerGroup {
  anchorKey: string;
  recipients: Set<string>;
  stoppedRecipients: Set<string>;
}

interface PotentialFeePayerWatch {
  address: string;
  status: PotentialFeePayerStatus;
  enhancedWatchId: number | null;
  solBalanceSubId: number | null;
  /** Recipient wallets currently subscribed for SWAP detection on this feePayer. */
  subscribedRecipients: Set<string>;
  processedSignatures: Set<string>;
  recipientProcessedSignatures: Map<string, Set<string>>;
  balanceAtFunderReceiveSol: number | null;
  balanceAtFunderReceiveSignature: string | null;
  /** Unix seconds when this feePayer was armed (funder receive). */
  balanceAtFunderReceiveTimestamp: number | null;
  /** REST sync cursor — txs after funder-receive signature. */
  cursorSignature: string | null;
  isSyncing: boolean;
  syncPending: boolean;
  syncPendingForce: boolean;
  lastSyncAt: number;
  bundlerFundingEvents: BundlerFundingEvent[];
  mode: 'normal' | null;
  detectedMint: string | null;
  detectedDevWallet: string | null;
  cooldownDevWatchId: number | null;
  /** Native SOL balance subscription on dev during cooldown — zero = rug resume. */
  cooldownDevSolBalanceSubId: number | null;
  /** Mint tied to the active cooldown — used for rug sync/resume. */
  cooldownMint: string | null;
  /** Dev create tx timestamp — ignore CLOSE_ACCOUNT txs at or before this. */
  cooldownDevCreateTimestamp: number | null;
  cooldownDevProcessedSignatures: Set<string>;
  cooldownDevSyncing: boolean;
  cooldownDevSyncPending: boolean;
  cooldownDevLastSyncAt: number;
  /** Concurrent active 4-bundler groups keyed by anchor. */
  activeGroups: Map<string, ActiveBundlerGroup>;
  exhaustedGroupAnchors: Set<string>;
  notifiedGroupAnchors: Set<string>;
  /** Post-balance when the feePayer sent SOL to each recipient. */
  recipientBalanceAtReceive: Map<string, number>;
  recipientZeroBalanceSubIds: Map<string, number>;
  /** Recipients that emitted a token buy — skip drain-based unsubscribe. */
  recipientsWithBuySeen: Set<string>;
}

export class FunderFirstOrchestrator extends EventEmitter {
  private readonly heliusClient: HeliusClient;
  private readonly connection: Connection;
  private readonly enhancedWs: HeliusEnhancedWsClient | null;
  private readonly insiderBots: InsiderBot[];
  private readonly primaryInsiderBotIndex: number;

  private funderAddress: string | null = null;
  private isEnabled = false;
  private isShuttingDown = false;
  private funderWatchId: number | null = null;
  /** One batched `transactionSubscribe` for all active recipient wallets. */
  private recipientBatchWatchId: number | null = null;
  /** recipient wallet → potential feePayer that subscribed it. */
  private readonly recipientToFeePayer = new Map<string, string>();
  private readonly potentialFeePayers = new Map<string, PotentialFeePayerWatch>();
  private readonly cooldownsByDev = new Map<string, string>();
  /** Cached Helius wallet identity: address → true when type is public exchange. */
  private readonly walletExchangeIdentityCache = new Map<string, boolean>();

  constructor(
    private readonly config: ServiceConfig,
    insiderBots: InsiderBot[],
    primaryInsiderBotIndex = 0,
    private readonly telegramBot: TelegramBot | null = null,
    enhancedWs: HeliusEnhancedWsClient | null = null,
  ) {
    super();
    this.insiderBots = insiderBots;
    this.primaryInsiderBotIndex = primaryInsiderBotIndex;
    const heliusKey = config.insiderHeliusApiKey || config.heliusApiKey;
    this.heliusClient = new HeliusClient(heliusKey, {
      projectId: config.insiderHeliusProjectId,
      label: 'Funder-First Helius',
    });
    this.connection = new Connection(config.insiderSolanaRpcUrl, {
      commitment: 'processed',
      wsEndpoint: config.insiderSolanaWsUrl,
    });
    this.enhancedWs = enhancedWs;
    if (config.insiderFeePayerFunderAddress) {
      try {
        this.funderAddress = new PublicKey(
          config.insiderFeePayerFunderAddress,
        ).toBase58();
      } catch {
        log.warn('Invalid INSIDER_FEEPAYER_FUNDER_ADDRESS in config; ignoring');
      }
    }
    insiderBots.forEach((bot) => {
      bot.on('tokenFlowEnded', (event: InsiderTokenFlowEndedEvent) => {
        void this.handleInsiderTokenFlowEnded(event);
      });
    });
  }

  private getPrimaryInsiderBot(): InsiderBot {
    return this.insiderBots[this.primaryInsiderBotIndex] ?? this.insiderBots[0];
  }

  private pickIdleInsiderBot(): InsiderBot | null {
    for (const bot of this.insiderBots) {
      if (bot.isStoppedForHeliusCredits?.()) continue;
      if (bot.isIdleForFunderFirst()) return bot;
    }
    return null;
  }

  setFunderAddress(address: string): string {
    const normalized = new PublicKey(address).toBase58();
    const changed = normalized !== this.funderAddress;
    this.funderAddress = normalized;
    log.info('FeePayer funder address set', { funderAddress: normalized });
    if (this.isEnabled && changed) {
      void this.resubscribeFunder();
    }
    return normalized;
  }

  getFunderAddress(): string | null {
    return this.funderAddress;
  }

  getWatchedPotentialFeePayers(): Array<{
    address: string;
    status: PotentialFeePayerStatus;
    mode: 'normal' | null;
    mint: string | null;
  }> {
    return [...this.potentialFeePayers.values()].map((watch) => ({
      address: watch.address,
      status: watch.status,
      mode: watch.mode,
      mint: watch.detectedMint,
    }));
  }

  isRunning(): boolean {
    return this.isEnabled;
  }

  /**
   * Manually arm a potential feePayer watch — same pipeline as a funder SOL
   * transfer-out, using the wallet's current native SOL balance as baseline.
   */
  async fastTrackPotentialFeePayer(
    rawAddress: string,
  ): Promise<
    | { ok: true; address: string; postBalanceSol: number; alreadyWatching?: boolean }
    | { ok: false; error: string }
  > {
    if (!this.isEnabled) {
      return {
        ok: false,
        error: 'Start funder-first mode before fast-tracking a potential feePayer.',
      };
    }
    if (!this.enhancedWs) {
      return {
        ok: false,
        error: 'Funder-first requires Enhanced WSS (Developer-plan Helius key).',
      };
    }

    let address: string;
    try {
      address = new PublicKey(rawAddress.trim()).toBase58();
    } catch {
      return { ok: false, error: 'Invalid Solana wallet address.' };
    }

    const existing = this.potentialFeePayers.get(address);
    if (existing?.status === 'active') {
      return {
        ok: false,
        error: 'That wallet is already active (handed to Insider bot).',
      };
    }
    if (existing?.status === 'cooldown') {
      return {
        ok: false,
        error:
          'That wallet is in cooldown after a trade — wait for dev rug or remove it from the menu first.',
      };
    }
    if (
      existing &&
      (existing.status === 'watching' || existing.status === 'normal_candidate')
    ) {
      const postBalanceSol =
        existing.balanceAtFunderReceiveSol ??
        (await this.connection.getBalance(new PublicKey(address))) /
          LAMPORTS_PER_SOL;
      log.info('Potential feePayer already on watch list — fast-track skipped reset', {
        potentialFeePayer: address,
        status: existing.status,
      });
      return { ok: true, address, postBalanceSol, alreadyWatching: true };
    }
    if (existing?.status === 'stopped') {
      this.potentialFeePayers.delete(address);
    }

    let postBalanceSol: number;
    try {
      const lamports = await this.connection.getBalance(new PublicKey(address));
      postBalanceSol = lamports / LAMPORTS_PER_SOL;
    } catch (err) {
      return {
        ok: false,
        error: `Failed to fetch wallet balance: ${err instanceof Error ? err.message : String(err)}`,
      };
    }

    if (postBalanceSol <= ZERO_BALANCE_EPSILON_SOL) {
      return {
        ok: false,
        error: 'Wallet native SOL balance is zero — nothing to watch.',
      };
    }

    const watch = this.ensurePotentialFeePayerWatch(address);
    if (!watch) {
      return { ok: false, error: 'Could not open potential feePayer watch.' };
    }

    this.unsubscribeRecipientsForWatch(watch);

    const baselineTimestamp = Math.floor(Date.now() / 1000);
    watch.status = 'watching';
    watch.mode = null;
    watch.detectedMint = null;
    watch.detectedDevWallet = null;
    watch.balanceAtFunderReceiveSol = postBalanceSol;
    watch.balanceAtFunderReceiveSignature = null;
    watch.balanceAtFunderReceiveTimestamp = baselineTimestamp;
    watch.cursorSignature = null;
    watch.processedSignatures.clear();
    watch.bundlerFundingEvents = [];
    watch.activeGroups.clear();
    watch.exhaustedGroupAnchors.clear();
    watch.notifiedGroupAnchors.clear();
    watch.recipientBalanceAtReceive.clear();
    watch.recipientsWithBuySeen.clear();

    log.info('Potential feePayer manually fast-tracked', {
      potentialFeePayer: address,
      postBalanceSol,
      baselineTimestamp,
    });
    log.info(
      'Potential feePayer pipeline armed — watching for 4 bundler sends in 10s (≥20 SOL post-balance, ≤0.5 SOL spread)',
      { potentialFeePayer: address, postBalanceSol },
    );

    void this.sendTelegram([
      '<b>⚡ Funder-First: Potential FeePayer Fast-Tracked</b>',
      `Wallet: <code>${this.html(address)}</code>`,
      `Current balance baseline: <b>${postBalanceSol.toFixed(4)} SOL</b>`,
      '',
      'Watching for 4 bundler funding txs in 10s…',
    ]);

    await this.syncPotentialFeePayerTransactions(address, true);
    return { ok: true, address, postBalanceSol };
  }

  /** Stop watching and unsubscribe a potential feePayer (manual remove from menu). */
  removePotentialFeePayer(
    rawAddress: string,
  ): { ok: true; address: string } | { ok: false; error: string } {
    if (!this.isEnabled) {
      return {
        ok: false,
        error: 'Start funder-first mode before removing a potential feePayer.',
      };
    }

    let address: string;
    try {
      address = new PublicKey(rawAddress.trim()).toBase58();
    } catch {
      return { ok: false, error: 'Invalid Solana wallet address.' };
    }

    const watch = this.potentialFeePayers.get(address);
    if (!watch) {
      return { ok: false, error: 'That wallet is not on the potential feePayer watch list.' };
    }
    if (watch.status === 'active') {
      return {
        ok: false,
        error:
          'That wallet is active on Insider bot — wait for the token flow to finish before removing.',
      };
    }

    this.stopPotentialFeePayerWatch(address, 'Removed manually from Telegram menu');
    void this.sendTelegram([
      '<b>🗑 Funder-First: Potential FeePayer Removed</b>',
      `Wallet: <code>${this.html(address)}</code>`,
      '',
      'Unsubscribed from Enhanced WSS and recipient watches.',
    ]);
    return { ok: true, address };
  }

  /**
   * Follow-wallet Enhanced WSS txs forwarded here when the follow wallet is an
   * active bundler recipient — avoids a duplicate recipient-batch subscription.
   */
  handleMergedFollowWalletTx(tx: HeliusTransaction): void {
    const followWallet = this.getPrimaryInsiderBot().getFollowedWallet();
    if (!followWallet || !this.isEnabled) return;

    for (const [feePayer, watch] of this.potentialFeePayers) {
      if (watch.status === 'stopped' || watch.status === 'active') continue;
      const inActiveGroup = [...watch.activeGroups.values()].some(
        (group) =>
          group.recipients.has(followWallet) &&
          !group.stoppedRecipients.has(followWallet),
      );
      if (!inActiveGroup) continue;
      void this.handleRecipientTx(feePayer, followWallet, tx);
    }
  }

  private isMergedFollowWalletRecipient(recipient: string): boolean {
    const followWallet = this.getPrimaryInsiderBot().getFollowedWallet();
    return !!followWallet && recipient === followWallet;
  }

  async start(): Promise<void> {
    if (!this.funderAddress) {
      throw new Error(
        'Set a feePayer funder address before starting funder-first mode.',
      );
    }
    if (!this.enhancedWs) {
      throw new Error(
        'Funder-first mode requires INSIDER_HELIUS_API_KEY (Developer plan) for Enhanced WSS.',
      );
    }
    if (this.isEnabled) return;
    this.isEnabled = true;
    this.subscribeFunder();
    log.info('Funder-first mode started', { funderAddress: this.funderAddress });
  }

  stop(reason = 'Stopped manually'): void {
    this.isEnabled = false;
    void this.unsubscribeFunder();
    for (const address of [...this.potentialFeePayers.keys()]) {
      this.stopPotentialFeePayerWatch(address, reason);
    }
    log.info('Funder-first mode stopped', { reason });
  }

  async shutdown(): Promise<void> {
    this.isShuttingDown = true;
    this.stop('Service shutting down');
  }

  private subscribeFunder(): void {
    if (!this.enhancedWs || !this.funderAddress || this.funderWatchId !== null) {
      return;
    }
    const funder = this.funderAddress;
    this.funderWatchId = this.enhancedWs.watch(funder, (tx) => {
      void this.handleFunderTx(tx, funder);
    });
    log.info('Subscribed to feePayer funder via Enhanced WSS', { funder });
  }

  private async unsubscribeFunder(): Promise<void> {
    if (this.funderWatchId === null || !this.enhancedWs) return;
    const id = this.funderWatchId;
    this.funderWatchId = null;
    await this.enhancedWs.unwatch(id).catch(() => undefined);
  }

  private async resubscribeFunder(): Promise<void> {
    await this.unsubscribeFunder();
    this.subscribeFunder();
  }

  private async handleFunderTx(
    tx: HeliusTransaction,
    funder: string,
  ): Promise<void> {
    if (!this.isEnabled || this.funderAddress !== funder) return;
    if (!this.hasOutgoingSolFrom(tx, funder)) return;

    for (const { to: recipient, amountSol } of this.extractOutgoingSolTransfers(
      tx,
      funder,
    )) {
      let existing = this.potentialFeePayers.get(recipient);
      if (
        existing &&
        (existing.status === 'active' || existing.status === 'cooldown')
      ) {
        log.debug('Funder sent SOL to confirmed feePayer — skipping funder tx', {
          recipient,
          status: existing.status,
          signature: tx.signature,
        });
        continue;
      }

      if (existing?.status === 'stopped') {
        this.potentialFeePayers.delete(recipient);
        existing = undefined;
      }

      const postBalanceSol = this.getAccountPostBalanceSol(tx, recipient);
      if (postBalanceSol === null) {
        log.warn('Funder sent SOL but recipient post-balance unknown — skipping', {
          recipient,
          signature: tx.signature,
        });
        continue;
      }

      const watch = this.ensurePotentialFeePayerWatch(recipient);
      if (!watch || watch.status === 'stopped') continue;

      const isTopUp = watch.balanceAtFunderReceiveSol !== null;
      watch.balanceAtFunderReceiveSol = postBalanceSol;
      watch.balanceAtFunderReceiveSignature = tx.signature;
      watch.balanceAtFunderReceiveTimestamp = tx.timestamp;
      watch.cursorSignature = tx.signature;
      watch.processedSignatures.add(tx.signature);

      if (!isTopUp) {
        log.info('Potential feePayer received SOL from funder', {
          potentialFeePayer: recipient,
          amountSol,
          postBalanceSol,
          signature: tx.signature,
          enhancedWatchId: watch.enhancedWatchId,
        });
        log.info(
          'Potential feePayer pipeline armed — watching for 4 bundler sends in 10s (≥20 SOL post-balance, ≤0.5 SOL spread)',
          {
            potentialFeePayer: recipient,
            postBalanceSol,
          },
        );
        void this.sendTelegram([
          '<b>👀 Funder-First: New Potential FeePayer</b>',
          `Funder: <code>${this.html(funder)}</code>`,
          `Recipient: <code>${this.html(recipient)}</code>`,
          `Received: <b>${amountSol.toFixed(4)} SOL</b>`,
          `Post-balance: <b>${postBalanceSol.toFixed(4)} SOL</b>`,
          '',
          'Watching for 4 bundler funding txs in 10s…',
        ]);
        await this.syncPotentialFeePayerTransactions(recipient, true);
      } else {
        log.info('Potential feePayer topped up from funder — updated balance baseline', {
          potentialFeePayer: recipient,
          amountSol,
          postBalanceSol,
          status: watch.status,
          signature: tx.signature,
        });
        await this.syncPotentialFeePayerTransactions(recipient, true);
      }
    }
  }

  private ensurePotentialFeePayerWatch(address: string): PotentialFeePayerWatch | null {
    if (!this.enhancedWs) return null;

    let watch = this.potentialFeePayers.get(address);
    if (watch?.status === 'stopped') {
      this.potentialFeePayers.delete(address);
      watch = undefined;
    }

    if (!watch) {
      watch = {
        address,
        status: 'watching',
        enhancedWatchId: null,
        solBalanceSubId: null,
        subscribedRecipients: new Set(),
        processedSignatures: new Set(),
        recipientProcessedSignatures: new Map(),
        balanceAtFunderReceiveSol: null,
        balanceAtFunderReceiveSignature: null,
        balanceAtFunderReceiveTimestamp: null,
        cursorSignature: null,
        isSyncing: false,
        syncPending: false,
        syncPendingForce: false,
        lastSyncAt: 0,
        bundlerFundingEvents: [],
        mode: null,
        detectedMint: null,
        detectedDevWallet: null,
        cooldownDevWatchId: null,
        cooldownDevSolBalanceSubId: null,
        cooldownMint: null,
        cooldownDevCreateTimestamp: null,
        cooldownDevProcessedSignatures: new Set(),
        cooldownDevSyncing: false,
        cooldownDevSyncPending: false,
        cooldownDevLastSyncAt: 0,
        activeGroups: new Map(),
        exhaustedGroupAnchors: new Set(),
        notifiedGroupAnchors: new Set(),
        recipientBalanceAtReceive: new Map(),
        recipientZeroBalanceSubIds: new Map(),
        recipientsWithBuySeen: new Set(),
      };
      this.potentialFeePayers.set(address, watch);
    }

    if (watch.enhancedWatchId === null) {
      watch.enhancedWatchId = this.enhancedWs.watch(address, (tx) => {
        void this.handlePotentialFeePayerTx(address, tx);
      });
      log.info('Subscribed to potential feePayer via Enhanced WSS', {
        address,
        enhancedWatchId: watch.enhancedWatchId,
      });
    }

    if (watch.solBalanceSubId === null) {
      watch.solBalanceSubId = this.connection.onAccountChange(
        new PublicKey(address),
        (info) => {
          if (info.lamports === 0) {
            void this.handleFeePayerZeroBalanceDetected(address);
          }
        },
        'processed',
      );
    }

    if (watch.status === 'stopped') {
      watch.status = 'watching';
    }

    return watch;
  }

  private isInvalidHeliusAfterSignatureError(error: unknown): boolean {
    const message = error instanceof Error ? error.message : String(error);
    return /too old or does not exist/i.test(message);
  }

  /** Fallback when Helius rejects after-signature on REST sync cursor. */
  private async fetchPotentialFeePayerTxsAfterTimestamp(
    address: string,
    afterTimestamp: number,
  ): Promise<HeliusTransaction[]> {
    const recent = await this.heliusClient.getWalletTransactionsDesc(
      address,
      POTENTIAL_FEEPAYER_SYNC_LIMIT * 2,
    );
    return recent
      .filter((tx) => tx.timestamp >= afterTimestamp)
      .sort((a, b) => a.timestamp - b.timestamp || a.slot - b.slot)
      .slice(0, POTENTIAL_FEEPAYER_SYNC_LIMIT);
  }

  private async syncPotentialFeePayerTransactions(
    address: string,
    force = false,
  ): Promise<void> {
    const watch = this.potentialFeePayers.get(address);
    if (!watch || !this.isEnabled) return;
    if (
      watch.status === 'stopped' ||
      watch.status === 'active' ||
      watch.status === 'cooldown'
    ) {
      return;
    }

    if (watch.isSyncing) {
      watch.syncPending = true;
      if (force) watch.syncPendingForce = true;
      return;
    }
    if (
      !force &&
      Date.now() - watch.lastSyncAt < POTENTIAL_FEEPAYER_SYNC_MIN_INTERVAL_MS
    ) {
      return;
    }

    watch.isSyncing = true;
    watch.lastSyncAt = Date.now();
    const syncingAddress = address;
    const afterSignature =
      watch.balanceAtFunderReceiveSignature ?? watch.cursorSignature ?? undefined;
    let fetchedCount = 0;
    let processedCount = 0;

    log.info('Potential feePayer REST sync started', {
      address: syncingAddress,
      afterSignature: afterSignature ?? null,
      force,
    });

    try {
      let txs: HeliusTransaction[];
      if (!afterSignature && watch.balanceAtFunderReceiveTimestamp !== null) {
        txs = await this.fetchPotentialFeePayerTxsAfterTimestamp(
          syncingAddress,
          watch.balanceAtFunderReceiveTimestamp,
        );
      } else {
        try {
          txs = await this.heliusClient.getAddressTransactionsAsc(
            syncingAddress,
            afterSignature,
            POTENTIAL_FEEPAYER_SYNC_LIMIT,
          );
        } catch (err) {
          if (
            this.isInvalidHeliusAfterSignatureError(err) &&
            watch.balanceAtFunderReceiveTimestamp !== null
          ) {
            log.info(
              'Potential feePayer REST sync after-signature rejected — falling back to recent desc + timestamp filter',
              {
                address: syncingAddress,
                afterSignature: afterSignature ?? null,
                afterTimestamp: watch.balanceAtFunderReceiveTimestamp,
              },
            );
            txs = await this.fetchPotentialFeePayerTxsAfterTimestamp(
              syncingAddress,
              watch.balanceAtFunderReceiveTimestamp!,
            );
          } else {
            throw err;
          }
        }
      }
      fetchedCount = txs.length;
      for (const tx of txs) {
        if (!this.potentialFeePayers.has(syncingAddress)) break;
        const current = this.potentialFeePayers.get(syncingAddress);
        if (!current) break;
        if (current.processedSignatures.has(tx.signature)) {
          current.cursorSignature = tx.signature;
          continue;
        }
        processedCount += 1;
        const handoff = await this.processPotentialFeePayerTx(
          syncingAddress,
          tx,
          'rest',
        );
        const after = this.potentialFeePayers.get(syncingAddress);
        if (!after) break;
        after.cursorSignature = tx.signature;
        if (handoff) break;
      }
    } catch (err) {
      void this.heliusClient.handlePossibleRateLimitError(err);
      log.warn('Potential feePayer REST sync failed', {
        address: syncingAddress,
        afterSignature: afterSignature ?? null,
        error: err instanceof Error ? err.message : String(err),
      });
    } finally {
      const current = this.potentialFeePayers.get(syncingAddress);
      if (current) {
        current.isSyncing = false;
        log.info('Potential feePayer REST sync completed', {
          address: syncingAddress,
          fetched: fetchedCount,
          processed: processedCount,
          fundingEvents: current.bundlerFundingEvents.length,
          activeGroups: current.activeGroups.size,
          status: current.status,
        });
        if (current.syncPending) {
          current.syncPending = false;
          const pendingForce = current.syncPendingForce;
          current.syncPendingForce = false;
          void this.syncPotentialFeePayerTransactions(syncingAddress, pendingForce);
        }
      }
    }
  }

  private async handlePotentialFeePayerTx(
    address: string,
    tx: HeliusTransaction,
  ): Promise<void> {
    await this.processPotentialFeePayerTx(address, tx, 'wss');
  }

  /**
   * Processes one potential feePayer tx from WSS or REST sync.
   * Returns true when the watch was stopped or handed off (caller should stop iterating).
   */
  private async processPotentialFeePayerTx(
    address: string,
    tx: HeliusTransaction,
    source: 'wss' | 'rest',
  ): Promise<boolean> {
    const watch = this.potentialFeePayers.get(address);
    if (!watch || !this.isEnabled) return false;
    if (watch.status === 'stopped' || watch.status === 'active') return true;
    if (watch.processedSignatures.has(tx.signature)) return false;
    watch.processedSignatures.add(tx.signature);

    if (watch.status === 'cooldown') return false;

    for (const transfer of this.extractOutgoingSolTransfers(tx, address)) {
      if (
        await this.maybeStopPotentialFeePayerForExchangeRecipient(
          watch,
          transfer,
          tx,
          source,
        )
      ) {
        return true;
      }

      const recipientPostBalanceSol = this.getAccountPostBalanceSol(
        tx,
        transfer.to,
      );
      if (recipientPostBalanceSol === null) continue;
      if (recipientPostBalanceSol < NORMAL_MIN_POST_SOL) continue;

      watch.bundlerFundingEvents.push({
        recipient: transfer.to,
        amountSol: transfer.amountSol,
        recipientPostBalanceSol,
        signature: tx.signature,
        timestamp: tx.timestamp,
      });
      watch.recipientBalanceAtReceive.set(transfer.to, recipientPostBalanceSol);
      log.info('Potential feePayer bundler funding event recorded', {
        potentialFeePayer: address,
        bundlerRecipient: transfer.to,
        amountSol: transfer.amountSol,
        recipientPostBalanceSol,
        signature: tx.signature,
        source,
        totalFundingEvents: watch.bundlerFundingEvents.length,
      });
    }

    const groupsBefore = watch.activeGroups.size;
    await this.evaluateBundlerGroups(watch);
    const newGroups = watch.activeGroups.size - groupsBefore;
    if (newGroups > 0) {
      log.info('Potential feePayer group evaluation', {
        potentialFeePayer: address,
        status: watch.status,
        fundingEvents: watch.bundlerFundingEvents.length,
        activeGroups: watch.activeGroups.size,
        newGroups,
        source,
      });
    }

    const selfPostBalanceSol = this.getAccountPostBalanceSol(tx, address);
    if (
      selfPostBalanceSol !== null &&
      selfPostBalanceSol <= ZERO_BALANCE_EPSILON_SOL
    ) {
      return this.finalizeFeePayerZeroBalance(watch, tx, source);
    }
    return false;
  }

  private isFeePayerBalanceZero(balanceSol: number | null): boolean {
    return balanceSol !== null && balanceSol <= ZERO_BALANCE_EPSILON_SOL;
  }

  private isExchangeDrainCheckTransfer(
    watch: PotentialFeePayerWatch,
    amountSol: number,
  ): boolean {
    const baselineSol = watch.balanceAtFunderReceiveSol;
    if (baselineSol === null || baselineSol <= 0) return false;
    return amountSol <= baselineSol * EXCHANGE_DRAIN_CHECK_MAX_FRACTION;
  }

  private isPublicExchangeIdentity(identity: HeliusWalletIdentity): boolean {
    return identity.type?.trim().toLowerCase() === 'exchange';
  }

  private async resolveIsPublicExchangeWallet(wallet: string): Promise<boolean> {
    const cached = this.walletExchangeIdentityCache.get(wallet);
    if (cached !== undefined) return cached;

    try {
      const identity = await this.heliusClient.getWalletIdentity(wallet);
      const isExchange =
        identity !== null && this.isPublicExchangeIdentity(identity);
      this.walletExchangeIdentityCache.set(wallet, isExchange);
      if (isExchange) {
        log.info('Helius wallet identity resolved as public exchange', {
          wallet,
          name: identity?.name ?? null,
          category: identity?.category ?? null,
        });
      }
      return isExchange;
    } catch (err) {
      log.warn('Helius wallet identity lookup failed — continuing watch', {
        wallet,
        error: err instanceof Error ? err.message : String(err),
      });
      return false;
    }
  }

  private async maybeStopPotentialFeePayerForExchangeRecipient(
    watch: PotentialFeePayerWatch,
    transfer: { to: string; amountSol: number },
    tx: HeliusTransaction,
    source: 'wss' | 'rest',
  ): Promise<boolean> {
    if (!this.isExchangeDrainCheckTransfer(watch, transfer.amountSol)) {
      return false;
    }

    const isExchange = await this.resolveIsPublicExchangeWallet(transfer.to);
    if (!isExchange) return false;

    log.warn('Potential feePayer sent SOL to public exchange — stopping watch', {
      potentialFeePayer: watch.address,
      exchangeRecipient: transfer.to,
      amountSol: transfer.amountSol,
      initialReceiveSol: watch.balanceAtFunderReceiveSol,
      signature: tx.signature,
      source,
    });

    void this.sendTelegram([
      '<b>⏹️ Funder-First: Potential FeePayer Stopped — Exchange Send</b>',
      `FeePayer: <code>${this.html(watch.address)}</code>`,
      `Exchange recipient: <code>${this.html(transfer.to)}</code>`,
      `Sent: <b>${transfer.amountSol.toFixed(4)} SOL</b>`,
      `Initial receive baseline: <b>${(watch.balanceAtFunderReceiveSol ?? 0).toFixed(4)} SOL</b>`,
      '',
      'Unsubscribed — not a bundler-funding feePayer pattern.',
    ]);

    this.stopPotentialFeePayerWatch(
      watch.address,
      'outgoing SOL transfer to public exchange wallet',
    );
    return true;
  }

  /** Fee payer hit zero — stop monitoring (no handoff). */
  private finalizeFeePayerZeroBalance(
    watch: PotentialFeePayerWatch,
    tx: HeliusTransaction,
    source: 'wss' | 'rest',
  ): boolean {
    log.info('Potential feePayer reached zero — stopping watch', {
      potentialFeePayer: watch.address,
      signature: tx.signature,
      source,
    });
    this.stopPotentialFeePayerWatch(
      watch.address,
      'native SOL balance reached zero',
    );
    return true;
  }

  private async handleFeePayerZeroBalanceDetected(address: string): Promise<void> {
    const watch = this.potentialFeePayers.get(address);
    if (!watch || !this.isEnabled) return;
    if (
      watch.status === 'stopped' ||
      watch.status === 'active' ||
      watch.status === 'cooldown'
    ) {
      return;
    }

    this.stopPotentialFeePayerWatch(address, 'native SOL balance reached zero');
  }

  private stopPotentialFeePayerWatch(address: string, reason: string): void {
    const watch = this.potentialFeePayers.get(address);
    if (!watch) return;
    watch.status = 'stopped';
    if (watch.enhancedWatchId !== null) {
      void this.enhancedWs?.unwatch(watch.enhancedWatchId).catch(() => undefined);
      watch.enhancedWatchId = null;
    }
    if (watch.solBalanceSubId !== null) {
      void this.connection
        .removeAccountChangeListener(watch.solBalanceSubId)
        .catch(() => undefined);
      watch.solBalanceSubId = null;
    }
    this.unsubscribeRecipientsForWatch(watch);
    for (const [, subId] of watch.recipientZeroBalanceSubIds) {
      void this.connection.removeAccountChangeListener(subId).catch(() => undefined);
    }
    watch.recipientZeroBalanceSubIds.clear();
    if (watch.cooldownDevWatchId !== null) {
      void this.enhancedWs?.unwatch(watch.cooldownDevWatchId).catch(() => undefined);
      watch.cooldownDevWatchId = null;
    }
    this.stopCooldownDevSolBalanceWatch(watch);
    if (watch.detectedDevWallet) {
      this.cooldownsByDev.delete(watch.detectedDevWallet);
    }
    this.potentialFeePayers.delete(address);
    log.info('Stopped watching potential feePayer', { address, reason });
  }

  private getGroupAnchorKey(anchor: BundlerFundingEvent): string {
    return `${anchor.timestamp}:${anchor.signature}`;
  }

  private async evaluateBundlerGroups(watch: PotentialFeePayerWatch): Promise<void> {
    if (watch.status !== 'watching' && watch.status !== 'normal_candidate') {
      return;
    }

    for (const [anchorKey, group] of [...watch.activeGroups]) {
      if (group.stoppedRecipients.size >= group.recipients.size) {
        this.abandonActiveGroup(
          watch,
          anchorKey,
          'all recipients in group drained or zero',
          false,
        );
      }
    }

    for (const group of this.findAllBundlerGroups(watch)) {
      this.activateGroup(watch, group);
    }

    this.updateWatchCandidateStatus(watch);
  }

  private updateWatchCandidateStatus(watch: PotentialFeePayerWatch): void {
    if (watch.activeGroups.size > 0) {
      watch.status = 'normal_candidate';
      watch.mode = 'normal';
    } else if (watch.subscribedRecipients.size === 0) {
      watch.status = 'watching';
      watch.mode = null;
    }
  }

  private recipientStillNeeded(
    watch: PotentialFeePayerWatch,
    recipient: string,
  ): boolean {
    for (const group of watch.activeGroups.values()) {
      if (group.recipients.has(recipient) && !group.stoppedRecipients.has(recipient)) {
        return true;
      }
    }
    return false;
  }

  private postBalancesClustered(
    events: BundlerFundingEvent[],
    toleranceSol = POST_BALANCE_TOLERANCE_SOL,
  ): boolean {
    if (events.length === 0) return false;
    const balances = events.map((e) => e.recipientPostBalanceSol);
    return Math.max(...balances) - Math.min(...balances) <= toleranceSol;
  }

  private findClusteredGroupInWindow(
    window: BundlerFundingEvent[],
    excludeRecipients: ReadonlySet<string> = new Set(),
  ): BundlerFundingEvent[] | null {
    const unique = new Map<string, BundlerFundingEvent>();
    for (const e of window) {
      if (excludeRecipients.has(e.recipient)) continue;
      if (!unique.has(e.recipient)) unique.set(e.recipient, e);
    }
    if (unique.size < BUNDLER_GROUP_SIZE) return null;

    const byBalance = [...unique.values()].sort(
      (a, b) => a.recipientPostBalanceSol - b.recipientPostBalanceSol,
    );

    let best: BundlerFundingEvent[] | null = null;
    let maxClusterSize = 0;

    for (let left = 0; left < byBalance.length; left += 1) {
      for (let right = left; right < byBalance.length; right += 1) {
        const spread =
          byBalance[right]!.recipientPostBalanceSol -
          byBalance[left]!.recipientPostBalanceSol;
        if (spread > POST_BALANCE_TOLERANCE_SOL) break;

        const count = right - left + 1;
        maxClusterSize = Math.max(maxClusterSize, count);
        if (count !== BUNDLER_GROUP_SIZE) continue;

        const selected = byBalance.slice(left, right + 1);
        if (!this.postBalancesClustered(selected)) continue;

        const ordered = [...selected].sort((a, b) => a.timestamp - b.timestamp);
        const isBetter =
          !best || ordered[0]!.timestamp < best[0]!.timestamp;
        if (isBetter) best = ordered;
      }
    }

    if (maxClusterSize > BUNDLER_GROUP_SIZE) return null;
    return best;
  }

  private findAllBundlerGroups(
    watch: PotentialFeePayerWatch,
  ): BundlerFundingEvent[][] {
    const events = watch.bundlerFundingEvents
      .filter((e) => e.recipientPostBalanceSol >= NORMAL_MIN_POST_SOL)
      .sort((a, b) => a.timestamp - b.timestamp);
    const found: BundlerFundingEvent[][] = [];
    const claimedAnchorKeys = new Set<string>();
    const claimedRecipients = new Set<string>();
    for (const group of watch.activeGroups.values()) {
      for (const recipient of group.recipients) {
        if (!group.stoppedRecipients.has(recipient)) {
          claimedRecipients.add(recipient);
        }
      }
    }

    for (let i = 0; i < events.length; i += 1) {
      const anchor = events[i]!;
      const anchorKey = this.getGroupAnchorKey(anchor);
      if (watch.exhaustedGroupAnchors.has(anchorKey)) continue;
      if (watch.activeGroups.has(anchorKey)) continue;
      if (claimedAnchorKeys.has(anchorKey)) continue;

      const window = events.filter(
        (e) =>
          e.timestamp >= anchor.timestamp &&
          e.timestamp <= anchor.timestamp + GROUP_WINDOW_SEC,
      );
      const group = this.findClusteredGroupInWindow(window, claimedRecipients);
      if (!group) continue;

      const groupAnchor = group[0]!;
      const groupAnchorKey = this.getGroupAnchorKey(groupAnchor);
      if (
        watch.exhaustedGroupAnchors.has(groupAnchorKey) ||
        watch.activeGroups.has(groupAnchorKey) ||
        claimedAnchorKeys.has(groupAnchorKey)
      ) {
        continue;
      }

      claimedAnchorKeys.add(groupAnchorKey);
      for (const event of group) claimedRecipients.add(event.recipient);
      found.push(group);
    }
    return found;
  }

  private activateGroup(
    watch: PotentialFeePayerWatch,
    group: BundlerFundingEvent[],
  ): void {
    const anchor = group[0]!;
    const anchorKey = this.getGroupAnchorKey(anchor);
    if (watch.exhaustedGroupAnchors.has(anchorKey)) return;

    if (watch.activeGroups.has(anchorKey)) {
      this.syncAllActiveGroupSubscriptions(watch);
      return;
    }

    watch.activeGroups.set(anchorKey, {
      anchorKey,
      recipients: new Set(group.map((e) => e.recipient)),
      stoppedRecipients: new Set(),
    });

    for (const event of group) {
      watch.recipientBalanceAtReceive.set(
        event.recipient,
        event.recipientPostBalanceSol,
      );
    }

    if (!watch.notifiedGroupAnchors.has(anchorKey)) {
      watch.notifiedGroupAnchors.add(anchorKey);
      const spread =
        Math.max(...group.map((e) => e.recipientPostBalanceSol)) -
        Math.min(...group.map((e) => e.recipientPostBalanceSol));
      void this.sendTelegram([
        '<b>✅ Funder-First: Normal Mode Candidate (≥20 SOL post-balance)</b>',
        `Potential FeePayer: <code>${this.html(watch.address)}</code>`,
        `Bundlers (${group.length} in 10s, post-balance spread ≤0.5 SOL):`,
        ...group.map(
          (e) =>
            `• <code>${this.html(e.recipient)}</code> — post <b>${e.recipientPostBalanceSol.toFixed(2)} SOL</b>`,
        ),
        `Spread: <b>${spread.toFixed(2)} SOL</b>`,
        '',
        'Waiting for a SWAP buy from one of these wallets…',
      ]);
    }

    this.syncAllActiveGroupSubscriptions(watch);
    this.updateWatchCandidateStatus(watch);
  }

  private abandonActiveGroup(
    watch: PotentialFeePayerWatch,
    anchorKey: string,
    reason: string,
    reevaluate = true,
  ): void {
    const group = watch.activeGroups.get(anchorKey);
    if (!group) return;
    watch.exhaustedGroupAnchors.add(anchorKey);
    watch.activeGroups.delete(anchorKey);
    log.info('Abandoning bundler group; other active groups continue', {
      feePayer: watch.address,
      anchorKey,
      reason,
      recipients: [...group.recipients],
      remainingGroups: watch.activeGroups.size,
    });
    for (const recipient of group.recipients) {
      if (!this.recipientStillNeeded(watch, recipient)) {
        this.unsubscribeSingleRecipient(watch, recipient);
      }
    }
    this.updateWatchCandidateStatus(watch);
    if (reevaluate) void this.evaluateBundlerGroups(watch);
  }

  private syncAllActiveGroupSubscriptions(watch: PotentialFeePayerWatch): void {
    if (!this.enhancedWs) return;
    const toSubscribe = new Set<string>();
    for (const group of watch.activeGroups.values()) {
      for (const recipient of group.recipients) {
        if (!group.stoppedRecipients.has(recipient)) toSubscribe.add(recipient);
      }
    }
    const merged = [...toSubscribe].filter((r) =>
      this.isMergedFollowWalletRecipient(r),
    );
    for (const recipient of merged) {
      toSubscribe.delete(recipient);
      log.info(
        'Bundler recipient merged with follow wallet — using follow-wallet Enhanced WSS only',
        {
          feePayer: watch.address,
          recipient,
        },
      );
    }
    this.subscribeRecipients(watch, [...toSubscribe]);
    for (const recipient of toSubscribe) {
      this.ensureRecipientZeroBalanceSub(watch, recipient);
    }
  }

  private ensureRecipientZeroBalanceSub(
    watch: PotentialFeePayerWatch,
    recipient: string,
  ): void {
    if (this.isMergedFollowWalletRecipient(recipient)) return;
    if (watch.recipientZeroBalanceSubIds.has(recipient)) return;
    const subId = this.connection.onAccountChange(
      new PublicKey(recipient),
      (info) => {
        if (info.lamports === 0) {
          this.markRecipientStoppedInGroup(
            watch.address,
            recipient,
            'native SOL balance reached zero',
          );
        }
      },
      'processed',
    );
    watch.recipientZeroBalanceSubIds.set(recipient, subId);
  }

  private unsubscribeSingleRecipient(
    watch: PotentialFeePayerWatch,
    recipient: string,
  ): void {
    if (watch.subscribedRecipients.has(recipient)) {
      watch.subscribedRecipients.delete(recipient);
      const owner = this.recipientToFeePayer.get(recipient);
      if (owner === watch.address) {
        this.recipientToFeePayer.delete(recipient);
      }
      void this.syncRecipientBatch();
    }
    const subId = watch.recipientZeroBalanceSubIds.get(recipient);
    if (subId !== undefined) {
      void this.connection.removeAccountChangeListener(subId).catch(() => undefined);
      watch.recipientZeroBalanceSubIds.delete(recipient);
    }
  }

  private markRecipientStoppedInGroup(
    feePayerAddress: string,
    recipient: string,
    reason: string,
  ): void {
    const watch = this.potentialFeePayers.get(feePayerAddress);
    if (!watch || watch.activeGroups.size === 0) return;

    let touched = false;
    for (const [anchorKey, group] of watch.activeGroups) {
      if (!group.recipients.has(recipient)) continue;
      if (group.stoppedRecipients.has(recipient)) continue;
      touched = true;
      group.stoppedRecipients.add(recipient);
      log.info('Stopped watching bundler recipient in active group', {
        feePayer: feePayerAddress,
        anchorKey,
        recipient,
        reason,
        stopped: group.stoppedRecipients.size,
        total: group.recipients.size,
      });
      if (group.stoppedRecipients.size >= group.recipients.size) {
        this.abandonActiveGroup(
          watch,
          anchorKey,
          'all recipients in group drained or zero',
          false,
        );
      }
    }
    if (!touched) return;

    if (!this.recipientStillNeeded(watch, recipient)) {
      this.unsubscribeSingleRecipient(watch, recipient);
    }
    this.updateWatchCandidateStatus(watch);
    void this.evaluateBundlerGroups(watch);
  }

  private checkRecipientDrain(
    watch: PotentialFeePayerWatch,
    recipient: string,
    tx: HeliusTransaction,
  ): boolean {
    if (watch.recipientsWithBuySeen.has(recipient)) return false;

    const inAnyGroup = [...watch.activeGroups.values()].some((g) =>
      g.recipients.has(recipient),
    );
    if (!inAnyGroup) return false;
    if (
      [...watch.activeGroups.values()].every(
        (g) => !g.recipients.has(recipient) || g.stoppedRecipients.has(recipient),
      )
    ) {
      return false;
    }

    const postBalanceSol = this.getAccountPostBalanceSol(tx, recipient);
    if (postBalanceSol === null) return false;

    if (
      this.hasOutgoingSolFrom(tx, recipient) &&
      postBalanceSol <= ZERO_BALANCE_EPSILON_SOL
    ) {
      this.markRecipientStoppedInGroup(
        watch.address,
        recipient,
        'native SOL balance reached zero after feePayer receive',
      );
      return true;
    }
    return false;
  }

  private subscribeRecipients(watch: PotentialFeePayerWatch, recipients: string[]): void {
    if (!this.enhancedWs) return;
    let changed = false;
    for (const recipient of recipients) {
      if (watch.subscribedRecipients.has(recipient)) continue;
      watch.subscribedRecipients.add(recipient);
      this.recipientToFeePayer.set(recipient, watch.address);
      changed = true;
    }
    if (changed) void this.syncRecipientBatch();
  }

  private unsubscribeRecipientsForWatch(watch: PotentialFeePayerWatch): void {
    if (watch.subscribedRecipients.size === 0) return;
    for (const recipient of watch.subscribedRecipients) {
      const owner = this.recipientToFeePayer.get(recipient);
      if (owner === watch.address) {
        this.recipientToFeePayer.delete(recipient);
      }
      const subId = watch.recipientZeroBalanceSubIds.get(recipient);
      if (subId !== undefined) {
        void this.connection.removeAccountChangeListener(subId).catch(() => undefined);
        watch.recipientZeroBalanceSubIds.delete(recipient);
      }
    }
    watch.subscribedRecipients.clear();
    void this.syncRecipientBatch();
  }

  private async syncRecipientBatch(): Promise<void> {
    if (!this.enhancedWs) return;
    const addresses = [...this.recipientToFeePayer.keys()];
    if (addresses.length === 0) {
      if (this.recipientBatchWatchId !== null) {
        const id = this.recipientBatchWatchId;
        this.recipientBatchWatchId = null;
        await this.enhancedWs.unwatch(id).catch(() => undefined);
      }
      return;
    }
    if (this.recipientBatchWatchId === null) {
      this.recipientBatchWatchId = this.enhancedWs.watchMulti(
        addresses,
        (recipient, tx) => {
          const feePayer = this.recipientToFeePayer.get(recipient);
          if (feePayer) void this.handleRecipientTx(feePayer, recipient, tx);
        },
      );
      return;
    }
    await this.enhancedWs
      .updateWatchAddresses(this.recipientBatchWatchId, addresses)
      .catch(() => undefined);
  }

  private async handleRecipientTx(
    feePayerAddress: string,
    recipient: string,
    tx: HeliusTransaction,
  ): Promise<void> {
    const watch = this.potentialFeePayers.get(feePayerAddress);
    if (!watch || watch.status === 'stopped' || watch.status === 'active') return;

    let seen = watch.recipientProcessedSignatures.get(recipient);
    if (!seen) {
      seen = new Set();
      watch.recipientProcessedSignatures.set(recipient, seen);
    }
    if (seen.has(tx.signature)) return;
    seen.add(tx.signature);

    const mint = findWalletSwapBuyMint(tx, recipient);
    if (mint && mint !== SOL_MINT) {
      watch.recipientsWithBuySeen.add(recipient);
      await this.tryConfirmToken(watch, mint, recipient);
      return;
    }

    if (this.checkRecipientDrain(watch, recipient, tx)) return;
  }

  private async tryConfirmToken(
    watch: PotentialFeePayerWatch,
    mint: string,
    buyWallet: string,
  ): Promise<void> {
    if (watch.detectedMint === mint) return;

    try {
      const swaps = await this.heliusClient.getEarlyInsiderSwaps(mint, 4);
      const earlyBuys: FunderFirstEarlyBuy[] = [];
      const seenWallets = new Set<string>();
      for (const tx of swaps) {
        if (tx.type !== 'SWAP') continue;
        for (const transfer of tx.tokenTransfers ?? []) {
          if (transfer.mint !== mint) continue;
          const wallet = transfer.toUserAccount;
          if (!wallet || seenWallets.has(wallet)) continue;
          seenWallets.add(wallet);
          earlyBuys.push({
            wallet,
            tokenAmount: transfer.tokenAmount ?? 0,
            signature: tx.signature,
            buySol: null,
            feePayer: tx.feePayer ?? null,
            timestamp: tx.timestamp,
          });
          if (earlyBuys.length >= 4) break;
        }
        if (earlyBuys.length >= 4) break;
      }
      if (earlyBuys.length < 4) return;

      const candidateRecipients = new Set(
        watch.bundlerFundingEvents.map((e) => e.recipient),
      );
      const firstFourWallets = earlyBuys.map((b) => b.wallet);
      const overlap = firstFourWallets.filter((w) => candidateRecipients.has(w));
      if (overlap.length === 0) {
        log.info('Recipient buy seen but no overlap with first-four bundlers', {
          mint,
          feePayer: watch.address,
          buyWallet,
          firstFourWallets,
          candidateRecipients: [...candidateRecipients],
        });
        return;
      }

      const createTx = await this.heliusClient.getMintCreateTransaction(mint);
      const devWallet = createTx?.feePayer ?? null;
      watch.detectedMint = mint;
      watch.detectedDevWallet = devWallet;
      watch.cooldownDevCreateTimestamp = createTx?.timestamp ?? null;

      const targetBot = this.pickIdleInsiderBot();
      if (!targetBot) {
        log.warn('Normal-mode funder-first handoff delayed — all Insider bots busy', {
          mint,
          feePayer: watch.address,
        });
        return;
      }

      watch.status = 'active';
      watch.activeGroups.clear();
      this.unsubscribePotentialFeePayerOnly(watch);

      const started = await targetBot.startFromFunderFirst(
        mint,
        watch.address,
        earlyBuys,
      );
      if (!started) {
        watch.status = 'normal_candidate';
        this.resubscribePotentialFeePayer(watch);
        void this.evaluateBundlerGroups(watch);
        return;
      }

      void this.sendTelegram([
        '<b>🚀 Funder-First: Normal Mode — Handed to Insider Bot</b>',
        `Token: <code>${this.html(mint)}</code>`,
        `FeePayer: <code>${this.html(watch.address)}</code>`,
        `Matched bundlers: <b>${overlap.length}</b>`,
        this.getPrimaryInsiderBot().getFollowedWallet()
          ? `Follow wallet <code>${this.html(this.getPrimaryInsiderBot().getFollowedWallet()!)}</code> is among bundlers — normal mode applies.`
          : '',
      ].filter(Boolean));
    } catch (err) {
      log.warn('Failed to confirm funder-first token', {
        mint,
        feePayer: watch.address,
        error: err instanceof Error ? err.message : String(err),
      });
    }
  }

  private unsubscribePotentialFeePayerOnly(watch: PotentialFeePayerWatch): void {
    if (watch.enhancedWatchId !== null) {
      void this.enhancedWs?.unwatch(watch.enhancedWatchId).catch(() => undefined);
      watch.enhancedWatchId = null;
    }
    if (watch.solBalanceSubId !== null) {
      void this.connection
        .removeAccountChangeListener(watch.solBalanceSubId)
        .catch(() => undefined);
      watch.solBalanceSubId = null;
    }
    this.unsubscribeRecipientsForWatch(watch);
  }

  private resubscribePotentialFeePayer(watch: PotentialFeePayerWatch): void {
    const refreshed = this.ensurePotentialFeePayerWatch(watch.address);
    if (!refreshed) return;
    log.info('Resumed potential feePayer watch after cooldown', {
      address: watch.address,
      enhancedWatchId: refreshed.enhancedWatchId,
    });
    if (refreshed.balanceAtFunderReceiveSignature) {
      void this.syncPotentialFeePayerTransactions(refreshed.address, true);
    }
  }

  private enterCooldown(
    watch: PotentialFeePayerWatch,
    mint: string,
    devWallet: string | null,
  ): void {
    watch.status = 'cooldown';
    watch.activeGroups.clear();
    this.unsubscribePotentialFeePayerOnly(watch);
    watch.cooldownMint = mint;
    watch.cooldownDevProcessedSignatures.clear();
    if (!devWallet || !this.enhancedWs) {
      void this.syncCooldownDevTransactions(watch.address, true);
      return;
    }
    this.cooldownsByDev.set(devWallet, watch.address);
    watch.cooldownDevWatchId = this.enhancedWs.watch(devWallet, (tx) => {
      void this.processCooldownDevTx(watch.address, mint, devWallet, tx, 'wss');
    });
    this.subscribeCooldownDevSolBalance(watch, mint, devWallet);
    log.info('FeePayer entered cooldown; watching dev for rug', {
      feePayer: watch.address,
      mint,
      devWallet,
    });
    void this.syncCooldownDevTransactions(watch.address, true);
    void this.checkCooldownDevZeroBalanceImmediate(watch, mint, devWallet);
  }

  private subscribeCooldownDevSolBalance(
    watch: PotentialFeePayerWatch,
    mint: string,
    devWallet: string,
  ): void {
    if (watch.cooldownDevSolBalanceSubId !== null) return;
    watch.cooldownDevSolBalanceSubId = this.connection.onAccountChange(
      new PublicKey(devWallet),
      (accountInfo) => {
        if (accountInfo.lamports <= 0) {
          void this.handleDevZeroBalanceDuringCooldown(watch.address, mint, devWallet);
        }
      },
      'processed',
    );
    log.info('Subscribed to dev wallet SOL balance during cooldown', {
      feePayer: watch.address,
      mint,
      devWallet,
    });
  }

  private stopCooldownDevSolBalanceWatch(watch: PotentialFeePayerWatch): void {
    if (watch.cooldownDevSolBalanceSubId === null) return;
    const subId = watch.cooldownDevSolBalanceSubId;
    watch.cooldownDevSolBalanceSubId = null;
    void this.connection.removeAccountChangeListener(subId).catch(() => undefined);
  }

  private async checkCooldownDevZeroBalanceImmediate(
    watch: PotentialFeePayerWatch,
    mint: string,
    devWallet: string,
  ): Promise<void> {
    if (watch.status !== 'cooldown') return;
    try {
      const lamports = await this.connection.getBalance(new PublicKey(devWallet));
      if (lamports === 0) {
        await this.handleDevZeroBalanceDuringCooldown(watch.address, mint, devWallet);
      }
    } catch (err) {
      log.warn('Failed immediate dev zero-balance check during cooldown', {
        feePayer: watch.address,
        mint,
        devWallet,
        error: err instanceof Error ? err.message : String(err),
      });
    }
  }

  private async ensureCooldownDevCreateTimestamp(
    watch: PotentialFeePayerWatch,
    mint: string,
  ): Promise<void> {
    if (watch.cooldownDevCreateTimestamp !== null) return;
    try {
      const createTx = await this.heliusClient.getMintCreateTransaction(mint);
      watch.cooldownDevCreateTimestamp = createTx?.timestamp ?? null;
    } catch (err) {
      log.warn('Failed to fetch mint create tx for cooldown dev sync baseline', {
        mint,
        feePayer: watch.address,
        error: err instanceof Error ? err.message : String(err),
      });
    }
  }

  /**
   * REST backfill for dev CLOSE_ACCOUNT while in cooldown — catches rugs that
   * landed before the Enhanced WSS subscription was armed.
   */
  private async syncCooldownDevTransactions(
    feePayerAddress: string,
    force = false,
  ): Promise<void> {
    const watch = this.potentialFeePayers.get(feePayerAddress);
    if (!watch || !this.isEnabled || watch.status !== 'cooldown') return;

    const devWallet = watch.detectedDevWallet;
    const mint = watch.cooldownMint ?? watch.detectedMint;
    if (!devWallet || !mint) return;

    if (watch.cooldownDevSyncing) {
      watch.cooldownDevSyncPending = true;
      return;
    }
    if (
      !force &&
      Date.now() - watch.cooldownDevLastSyncAt < COOLDOWN_DEV_SYNC_MIN_INTERVAL_MS
    ) {
      return;
    }

    watch.cooldownDevSyncing = true;
    watch.cooldownDevLastSyncAt = Date.now();

    log.info('Cooldown dev REST sync started', {
      feePayer: feePayerAddress,
      mint,
      devWallet,
      force,
    });

    try {
      await this.ensureCooldownDevCreateTimestamp(watch, mint);
      const txs = await this.heliusClient.getWalletTransactionsDesc(
        devWallet,
        COOLDOWN_DEV_SYNC_LIMIT,
      );
      const sorted = [...txs].sort(
        (a, b) => a.timestamp - b.timestamp || a.slot - b.slot,
      );
      for (const tx of sorted) {
        const stopped = await this.processCooldownDevTx(
          feePayerAddress,
          mint,
          devWallet,
          tx,
          'rest',
        );
        if (stopped) return;
      }
      log.info('Cooldown dev REST sync completed — no rug found', {
        feePayer: feePayerAddress,
        mint,
        devWallet,
        scanned: sorted.length,
      });
    } catch (err) {
      log.warn('Cooldown dev REST sync failed', {
        feePayer: feePayerAddress,
        mint,
        devWallet,
        error: err instanceof Error ? err.message : String(err),
      });
    } finally {
      watch.cooldownDevSyncing = false;
      if (watch.cooldownDevSyncPending) {
        watch.cooldownDevSyncPending = false;
        void this.syncCooldownDevTransactions(feePayerAddress, true);
      }
    }
  }

  /**
   * Processes one dev tx from WSS or REST sync during cooldown.
   * Returns true when cooldown ended (rug detected and feePayer resumed).
   */
  private async processCooldownDevTx(
    feePayerAddress: string,
    mint: string,
    devWallet: string,
    tx: HeliusTransaction,
    source: 'wss' | 'rest',
  ): Promise<boolean> {
    const watch = this.potentialFeePayers.get(feePayerAddress);
    if (!watch || watch.status !== 'cooldown') return false;
    if (watch.cooldownDevProcessedSignatures.has(tx.signature)) return false;
    watch.cooldownDevProcessedSignatures.add(tx.signature);

    if (!isDevRugCloseAccountTx(tx, devWallet)) return false;
    if (
      watch.cooldownDevCreateTimestamp !== null &&
      tx.timestamp <= watch.cooldownDevCreateTimestamp
    ) {
      log.debug('Ignoring dev CLOSE_ACCOUNT at or before mint create during cooldown sync', {
        feePayer: feePayerAddress,
        mint,
        devWallet,
        signature: tx.signature,
        txTimestamp: tx.timestamp,
        devCreateTimestamp: watch.cooldownDevCreateTimestamp,
        source,
      });
      return false;
    }

    log.info('Dev rug CLOSE_ACCOUNT detected during cooldown', {
      feePayer: feePayerAddress,
      mint,
      devWallet,
      signature: tx.signature,
      source,
    });
    await this.resumePotentialFeePayerAfterDevRug(
      feePayerAddress,
      mint,
      devWallet,
      'close_account',
      tx.signature,
    );
    return watch.status !== 'cooldown';
  }

  private async handleDevZeroBalanceDuringCooldown(
    feePayerAddress: string,
    mint: string,
    devWallet: string,
  ): Promise<void> {
    const watch = this.potentialFeePayers.get(feePayerAddress);
    if (!watch || watch.status !== 'cooldown') return;

    log.info('Dev native SOL reached zero during cooldown — treating as rug', {
      feePayer: feePayerAddress,
      mint,
      devWallet,
    });
    await this.resumePotentialFeePayerAfterDevRug(
      feePayerAddress,
      mint,
      devWallet,
      'zero_balance',
    );
  }

  private async resumePotentialFeePayerAfterDevRug(
    feePayerAddress: string,
    mint: string,
    devWallet: string,
    signal: 'close_account' | 'zero_balance',
    txSignature?: string,
  ): Promise<void> {
    const watch = this.potentialFeePayers.get(feePayerAddress);
    if (!watch || watch.status !== 'cooldown') return;

    const telegramTitle =
      signal === 'close_account'
        ? '<b>🧹 Funder-First: Dev Rug Detected — Resuming FeePayer Watch</b>'
        : '<b>🧹 Funder-First: Dev Zero Balance — Resuming FeePayer Watch</b>';
    void this.sendTelegram([
      telegramTitle,
      `Token: <code>${this.html(mint)}</code>`,
      `Dev: <code>${this.html(devWallet)}</code>`,
      `FeePayer: <code>${this.html(feePayerAddress)}</code>`,
      ...(txSignature ? [`Tx: <code>${this.html(txSignature)}</code>`] : []),
      ...(signal === 'zero_balance'
        ? ['', 'Dev native SOL reached zero — treated as rug.']
        : []),
    ]);

    if (watch.cooldownDevWatchId !== null) {
      void this.enhancedWs?.unwatch(watch.cooldownDevWatchId).catch(() => undefined);
      watch.cooldownDevWatchId = null;
    }
    this.stopCooldownDevSolBalanceWatch(watch);
    this.cooldownsByDev.delete(devWallet);
    watch.status = 'watching';
    watch.detectedMint = null;
    watch.detectedDevWallet = null;
    watch.cooldownMint = null;
    watch.cooldownDevCreateTimestamp = null;
    watch.cooldownDevProcessedSignatures.clear();
    watch.cooldownDevSyncing = false;
    watch.cooldownDevSyncPending = false;
    watch.mode = null;
    watch.activeGroups.clear();
    watch.exhaustedGroupAnchors.clear();
    watch.notifiedGroupAnchors.clear();
    watch.recipientBalanceAtReceive.clear();
    watch.bundlerFundingEvents = [];
    this.resubscribePotentialFeePayer(watch);
  }

  private async handleDevTx(
    feePayerAddress: string,
    mint: string,
    devWallet: string,
    tx: HeliusTransaction,
  ): Promise<void> {
    await this.resumePotentialFeePayerAfterDevRug(
      feePayerAddress,
      mint,
      devWallet,
      'close_account',
      tx.signature,
    );
  }

  private async handleInsiderTokenFlowEnded(
    event: InsiderTokenFlowEndedEvent,
  ): Promise<void> {
    if (event.source !== 'funder-first' || !event.feePayer) return;
    const watch = this.potentialFeePayers.get(event.feePayer);
    if (!watch) return;

    if (!event.hadPosition) {
      await this.resumePotentialFeePayerAfterPreBuySkip(
        watch,
        event.mint ?? watch.detectedMint,
        event.reason,
      );
      return;
    }

    const devWallet = watch.detectedDevWallet;
    const mint = event.mint ?? watch.detectedMint;
    if (!devWallet || !mint) {
      watch.status = 'watching';
      this.resubscribePotentialFeePayer(watch);
      return;
    }

    void this.sendTelegram([
      '<b>⏸️ Funder-First: Trade Complete — FeePayer Paused Until Rug</b>',
      `Token: <code>${this.html(mint)}</code>`,
      `FeePayer: <code>${this.html(event.feePayer)}</code>`,
      `Dev: <code>${this.html(devWallet)}</code>`,
      '',
      'Watching dev wallet for CLOSE_ACCOUNT or zero native SOL before resuming this feePayer.',
    ]);
    this.enterCooldown(watch, mint, devWallet);
  }

  /** Pre-buy skip/reset — resume watching the feePayer for the next token (no dev cooldown). */
  private async resumePotentialFeePayerAfterPreBuySkip(
    watch: PotentialFeePayerWatch,
    mint: string | null,
    reason: InsiderTokenFlowEndedEvent['reason'],
  ): Promise<void> {
    if (watch.cooldownDevWatchId !== null) {
      void this.enhancedWs?.unwatch(watch.cooldownDevWatchId).catch(() => undefined);
      watch.cooldownDevWatchId = null;
    }
    this.stopCooldownDevSolBalanceWatch(watch);
    if (watch.detectedDevWallet) {
      this.cooldownsByDev.delete(watch.detectedDevWallet);
    }

    watch.status = 'watching';
    watch.detectedMint = null;
    watch.detectedDevWallet = null;
    watch.cooldownMint = null;
    watch.cooldownDevCreateTimestamp = null;
    watch.cooldownDevProcessedSignatures.clear();
    watch.cooldownDevSyncing = false;
    watch.cooldownDevSyncPending = false;
    watch.mode = null;

    void this.sendTelegram([
      '<b>↩️ Funder-First: Token Skipped — FeePayer Watch Resumed</b>',
      mint ? `Token: <code>${this.html(mint)}</code>` : '',
      `FeePayer: <code>${this.html(watch.address)}</code>`,
      '',
      reason === 'reset'
        ? 'Pre-buy skip (dust group, rug guard, etc.) — continuing to watch this feePayer for the next opportunity.'
        : 'Token flow ended without a held position — feePayer watch resumed.',
    ].filter(Boolean));

    log.info('Resumed potential feePayer watch after pre-buy token skip', {
      feePayer: watch.address,
      mint,
      reason,
    });

    this.resubscribePotentialFeePayer(watch);
    void this.evaluateBundlerGroups(watch);
  }

  private getAccountPostBalanceSol(
    tx: HeliusTransaction,
    account: string,
  ): number | null {
    const entry = (tx.accountData ?? []).find((a) => a.account === account);
    if (entry?.nativePostBalance === undefined) return null;
    return entry.nativePostBalance / LAMPORTS_PER_SOL;
  }

  private hasOutgoingSolFrom(tx: HeliusTransaction, from: string): boolean {
    return (tx.nativeTransfers ?? []).some(
      (t) => t.fromUserAccount === from && t.amount > 0,
    );
  }

  private transfersSolTo(
    from: string,
    to: string,
    tx: HeliusTransaction,
  ): boolean {
    return this.extractOutgoingSolTransfers(tx, from).some(
      (transfer) => transfer.to === to,
    );
  }

  private extractOutgoingSolTransfers(
    tx: HeliusTransaction,
    from: string,
  ): Array<{ to: string; amountSol: number }> {
    const out: Array<{ to: string; amountSol: number }> = [];
    for (const transfer of tx.nativeTransfers ?? []) {
      if (transfer.fromUserAccount !== from) continue;
      const to = transfer.toUserAccount;
      if (!to || to === UNKNOWN_COUNTERPARTY) continue;
      const amountSol = transfer.amount / LAMPORTS_PER_SOL;
      if (amountSol <= 0) continue;
      out.push({ to, amountSol });
    }
    if (out.length > 0) return out;

    // Enhanced WSS delta reconstruction emits outgoing legs as
    // from=<wallet>, to=__pool__ and recipients as from=__pool__, to=<wallet>.
    if (!this.hasOutgoingSolFrom(tx, from)) return [];

    for (const transfer of tx.nativeTransfers ?? []) {
      if (transfer.fromUserAccount !== UNKNOWN_COUNTERPARTY) continue;
      const to = transfer.toUserAccount;
      if (!to || to === from || to === UNKNOWN_COUNTERPARTY) continue;
      const amountSol = transfer.amount / LAMPORTS_PER_SOL;
      if (amountSol <= 0) continue;
      out.push({ to, amountSol });
    }
    return out;
  }

  private async sendTelegram(lines: string[]): Promise<void> {
    if (!this.telegramBot) return;
    await this.telegramBot
      .sendDefault(lines.join('\n'))
      .catch((err) =>
        log.warn('Telegram notification failed', {
          error: err instanceof Error ? err.message : String(err),
        }),
      );
  }

  private html(value: string): string {
    return value
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;');
  }
}
