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
//       token's dev wallet CLOSE_ACCOUNTs (rug), then resumes watching.
//
//  Group / recipient rules:
//    • Multiple 10s bundler groups (≥20 SOL only) can be monitored concurrently.
//    • A valid group is exactly 4 unique recipients in 10s whose post-balances
//      are all within 0.5 SOL of each other (all ≥20 SOL). If 5+ recipients in
//      the window meet the tolerance, the window is skipped entirely.
//    • Sub-20 SOL bundler sends are ignored (no Telegram, no backend info logs).
//    • Keep watching the feePayer for new groups until a recipient buy overlaps
//      the token's first-four bundlers.
//    • Per recipient in the active group: stop watching if post-balance after the
//      feePayer send drops to ≤50% of that receive baseline, or native SOL → zero.
//
//  Stop-watching rules for a potential feePayer wallet itself:
//    • Keep monitoring until native SOL balance hits zero.
//    • On zero: follow the wallet that received the final drain; if that wallet
//      is the top-level funder, stop and unsubscribe; otherwise restart fresh
//      on the recipient.
// ─────────────────────────────────────────────────────────────────────────────

import { Connection, LAMPORTS_PER_SOL, PublicKey } from '@solana/web3.js';
import { EventEmitter } from 'events';
import { createLogger } from './logger';
import { HeliusClient, HeliusTransaction } from './helius-client';
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
const HALF_DRAIN_RATIO = 0.5;
const ZERO_BALANCE_EPSILON_SOL = 1e-6;
/** Recipients in a 4-bundler group must have post-balances within this spread (SOL). */
const POST_BALANCE_TOLERANCE_SOL = 0.5;
const POTENTIAL_FEEPAYER_SYNC_LIMIT = 20;
const POTENTIAL_FEEPAYER_SYNC_MIN_INTERVAL_MS = 1_000;

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
  /** Unix seconds when this feePayer was armed (funder receive or handoff). */
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
  /** Concurrent active 4-bundler groups keyed by anchor. */
  activeGroups: Map<string, ActiveBundlerGroup>;
  exhaustedGroupAnchors: Set<string>;
  notifiedGroupAnchors: Set<string>;
  /** Post-balance when the feePayer sent SOL to each recipient. */
  recipientBalanceAtReceive: Map<string, number>;
  recipientZeroBalanceSubIds: Map<string, number>;
}

export class FunderFirstOrchestrator extends EventEmitter {
  private readonly heliusClient: HeliusClient;
  private readonly connection: Connection;
  private readonly enhancedWs: HeliusEnhancedWsClient | null;
  private readonly insiderBot: InsiderBot;

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

  constructor(
    private readonly config: ServiceConfig,
    insiderBot: InsiderBot,
    private readonly telegramBot: TelegramBot | null = null,
    enhancedWs: HeliusEnhancedWsClient | null = null,
  ) {
    super();
    this.insiderBot = insiderBot;
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
    insiderBot.on('tokenFlowEnded', (event: InsiderTokenFlowEndedEvent) => {
      void this.handleInsiderTokenFlowEnded(event);
    });
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
        activeGroups: new Map(),
        exhaustedGroupAnchors: new Set(),
        notifiedGroupAnchors: new Set(),
        recipientBalanceAtReceive: new Map(),
        recipientZeroBalanceSubIds: new Map(),
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

  /** Fallback when Helius rejects after-signature (common after zero-balance handoff). */
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

  /**
   * Fee payer hit zero — follow the primary drain recipient, unless it is the
   * top-level funder (then stop and unsubscribe).
   */
  private finalizeFeePayerZeroBalance(
    watch: PotentialFeePayerWatch,
    tx: HeliusTransaction,
    source: 'wss' | 'rest',
  ): boolean {
    const funder = this.funderAddress;
    const outgoing = this.extractOutgoingSolTransfers(tx, watch.address).filter(
      (transfer) => transfer.to !== funder,
    );
    const drain =
      [...outgoing].sort((a, b) => b.amountSol - a.amountSol)[0] ?? null;

    if (!drain || (funder && drain.to === funder)) {
      log.info('Potential feePayer reached zero — drain returned to funder; stopping watch', {
        potentialFeePayer: watch.address,
        funder,
        signature: tx.signature,
        source,
      });
      this.stopPotentialFeePayerWatch(
        watch.address,
        'native SOL balance reached zero (returned to funder)',
      );
      return true;
    }

    const nextPostBalanceSol = this.getAccountPostBalanceSol(tx, drain.to);
    if (nextPostBalanceSol === null) {
      log.warn('Potential feePayer zero-balance follow skipped — recipient post-balance unknown', {
        potentialFeePayer: watch.address,
        nextPotentialFeePayer: drain.to,
        signature: tx.signature,
        source,
      });
      this.stopPotentialFeePayerWatch(
        watch.address,
        'native SOL balance reached zero (follow recipient post-balance unknown)',
      );
      return true;
    }

    log.info('Potential feePayer reached zero — following drain recipient', {
      stoppedFeePayer: watch.address,
      nextPotentialFeePayer: drain.to,
      drainAmountSol: drain.amountSol,
      nextPostBalanceSol,
      signature: tx.signature,
      source,
    });

    const oldAddress = watch.address;
    this.stopPotentialFeePayerWatch(
      oldAddress,
      `zero-balance follow to ${drain.to}`,
    );
    this.startFreshPotentialFeePayerWatch(
      drain.to,
      tx.signature,
      tx.timestamp,
      nextPostBalanceSol,
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

    try {
      const recent = await this.heliusClient.getWalletTransactionsDesc(address, 5);
      for (const tx of recent) {
        if (watch.processedSignatures.has(tx.signature)) continue;
        if (!this.hasOutgoingSolFrom(tx, address)) continue;
        const selfPost = this.getAccountPostBalanceSol(tx, address);
        if (!this.isFeePayerBalanceZero(selfPost)) continue;
        watch.processedSignatures.add(tx.signature);
        if (this.finalizeFeePayerZeroBalance(watch, tx, 'rest')) return;
      }
    } catch (err) {
      log.warn('Failed to resolve zero-balance feePayer drain tx', {
        address,
        error: err instanceof Error ? err.message : String(err),
      });
    }

    this.stopPotentialFeePayerWatch(address, 'native SOL balance reached zero');
  }

  private startFreshPotentialFeePayerWatch(
    address: string,
    receiveSignature: string,
    receiveTimestamp: number,
    postBalanceSol: number,
  ): void {
    if (this.potentialFeePayers.has(address)) {
      this.stopPotentialFeePayerWatch(address, 'reset for fresh handoff');
    }
    const watch = this.ensurePotentialFeePayerWatch(address);
    if (!watch) return;
    watch.balanceAtFunderReceiveSol = postBalanceSol;
    watch.balanceAtFunderReceiveSignature = receiveSignature;
    watch.balanceAtFunderReceiveTimestamp = receiveTimestamp;
    watch.cursorSignature = receiveSignature;
    watch.processedSignatures.add(receiveSignature);
    log.info('Started fresh potential feePayer watch after handoff', {
      address,
      receiveSignature,
      postBalanceSol,
      enhancedWatchId: watch.enhancedWatchId,
    });
    log.info(
      'Potential feePayer pipeline armed — watching for 4 bundler sends in 10s (≥20 SOL post-balance, ≤0.5 SOL spread)',
      { potentialFeePayer: address, postBalanceSol },
    );
    void this.syncPotentialFeePayerTransactions(address, true);
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
    this.subscribeRecipients(watch, [...toSubscribe]);
    for (const recipient of toSubscribe) {
      this.ensureRecipientZeroBalanceSub(watch, recipient);
    }
  }

  private ensureRecipientZeroBalanceSub(
    watch: PotentialFeePayerWatch,
    recipient: string,
  ): void {
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

    const baseline = watch.recipientBalanceAtReceive.get(recipient);
    if (baseline === undefined) return false;

    const postBalanceSol = this.getAccountPostBalanceSol(tx, recipient);
    if (postBalanceSol === null) return false;

    if (
      this.hasOutgoingSolFrom(tx, recipient) &&
      postBalanceSol <= baseline * HALF_DRAIN_RATIO
    ) {
      this.markRecipientStoppedInGroup(
        watch.address,
        recipient,
        'balance drained to ≤50% after feePayer receive',
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

    if (this.checkRecipientDrain(watch, recipient, tx)) return;

    const mint = findWalletSwapBuyMint(tx, recipient);
    if (!mint || mint === SOL_MINT) return;

    await this.tryConfirmToken(watch, mint, recipient);
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

      if (!this.insiderBot.isIdleForFunderFirst()) {
        log.warn('Normal-mode funder-first handoff delayed — Insider bot busy', {
          mint,
          feePayer: watch.address,
        });
        return;
      }

      watch.status = 'active';
      watch.activeGroups.clear();
      this.unsubscribePotentialFeePayerOnly(watch);

      const started = await this.insiderBot.startFromFunderFirst(
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
        this.insiderBot.getFollowedWallet()
          ? `Follow wallet <code>${this.html(this.insiderBot.getFollowedWallet()!)}</code> is among bundlers — normal mode applies.`
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
    if (!devWallet || !this.enhancedWs) return;
    this.cooldownsByDev.set(devWallet, watch.address);
    watch.cooldownDevWatchId = this.enhancedWs.watch(devWallet, (tx) => {
      void this.handleDevTx(watch.address, mint, devWallet, tx);
    });
    log.info('FeePayer entered cooldown; watching dev for rug', {
      feePayer: watch.address,
      mint,
      devWallet,
    });
  }

  private async handleDevTx(
    feePayerAddress: string,
    mint: string,
    devWallet: string,
    tx: HeliusTransaction,
  ): Promise<void> {
    if (!isDevRugCloseAccountTx(tx, devWallet)) return;
    const watch = this.potentialFeePayers.get(feePayerAddress);
    if (!watch || watch.status !== 'cooldown') return;

    void this.sendTelegram([
      '<b>🧹 Funder-First: Dev Rug Detected — Resuming FeePayer Watch</b>',
      `Token: <code>${this.html(mint)}</code>`,
      `Dev: <code>${this.html(devWallet)}</code>`,
      `FeePayer: <code>${this.html(feePayerAddress)}</code>`,
      `Tx: <code>${this.html(tx.signature)}</code>`,
    ]);

    if (watch.cooldownDevWatchId !== null) {
      void this.enhancedWs?.unwatch(watch.cooldownDevWatchId).catch(() => undefined);
      watch.cooldownDevWatchId = null;
    }
    this.cooldownsByDev.delete(devWallet);
    watch.status = 'watching';
    watch.detectedMint = null;
    watch.detectedDevWallet = null;
    watch.mode = null;
    watch.activeGroups.clear();
    watch.exhaustedGroupAnchors.clear();
    watch.notifiedGroupAnchors.clear();
    watch.recipientBalanceAtReceive.clear();
    watch.bundlerFundingEvents = [];
    this.resubscribePotentialFeePayer(watch);
  }

  private async handleInsiderTokenFlowEnded(
    event: InsiderTokenFlowEndedEvent,
  ): Promise<void> {
    if (event.source !== 'funder-first' || !event.feePayer) return;
    const watch = this.potentialFeePayers.get(event.feePayer);
    if (!watch) return;

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
      'Watching dev wallet for CLOSE_ACCOUNT before resuming this feePayer.',
    ]);
    this.enterCooldown(watch, mint, devWallet);
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
