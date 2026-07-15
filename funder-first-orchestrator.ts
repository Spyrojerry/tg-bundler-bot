// ─────────────────────────────────────────────────────────────────────────────
//  funder-first-orchestrator.ts — Parallel "feePayer funder first" discovery
//
//  Runs alongside the existing follow-wallet → backtrack feePayer flow.
//  The user supplies a top-level feePayer *funder* wallet; this orchestrator:
//    1. Always watches that funder for SOL transfer-outs (Enhanced WSS).
//    2. For each new recipient, opens a potential-feePayer watch (also WSS).
//    3. Tracks bundler-funding patterns on each potential feePayer:
//         • Normal: 3–4 recipients within 10s whose *post-balance* is ≥20 SOL.
//         • Low:    3–4 recipients within 10s whose post-balance is 5–19.99 SOL.
//    4. Subscribes to those recipient wallets and waits for a SWAP buy.
//    5. Confirms at least one recipient is among the token's first-four bundlers.
//    6. Normal → hands off to InsiderBot.startFromFunderFirst for buy/sell.
//       Low    → Telegram "skipped", cooldown feePayer, watch dev for rug.
//    7. After a normal trade completes or a low-funding note, the feePayer stays
//       in cooldown until the token's dev wallet CLOSE_ACCOUNTs (rug), then
//       resumes watching that feePayer for the next opportunity.
//
//  Group / recipient rules:
//    • Multiple 10s bundler groups (normal or low) can be monitored concurrently.
//    • A valid group is exactly 3–4 unique recipients in 10s whose post-balances
//      are all within 0.5 SOL of each other (and in the normal or low band). If 5+
//      recipients in the window meet the tolerance, the window is skipped entirely.
//    • Keep watching the feePayer for new groups until a recipient buy overlaps
//      the token's first-four bundlers.
//    • Per recipient in the active group: stop watching if post-balance after the
//      feePayer send drops to ≤50% of that receive baseline, or native SOL → zero.
//    • If every recipient in the active 3–4 group drains/stops, abandon that group
//      and look for the next 10s window.
//
//  Stop-watching rules for a potential feePayer wallet itself:
//    • Before any bundler group is found: half-drain (≤50% of post-funder-receive
//      balance) triggers REST sync from the funder-receive tx; if the drain tx
//      returns SOL to the top-level funder, keep watching; otherwise hand off to
//      the SOL recipient and restart fresh for that wallet.
//    • After at least one bundler group is active: only native SOL → zero drops
//      the feePayer watch (no more half-drain on the feePayer itself).
//    • Native SOL balance hits zero (account subscription) always stops the watch.
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
import { UNKNOWN_COUNTERPARTY } from './tx-normalizer';

const log = createLogger('FUNDER-FIRST');

const SOL_MINT = 'So11111111111111111111111111111111111111112';
const GROUP_WINDOW_SEC = 10;
const MIN_BUNDLER_GROUP = 3;
const MAX_BUNDLER_GROUP = 4;
const NORMAL_MIN_POST_SOL = 20;
const LOW_MIN_POST_SOL = 5;
const LOW_MAX_POST_SOL = 19.99;
const HALF_DRAIN_RATIO = 0.5;
/** Recipients in a 3–4 bundler group must have post-balances within this spread (SOL). */
const POST_BALANCE_TOLERANCE_SOL = 0.5;
const POTENTIAL_FEEPAYER_SYNC_LIMIT = 20;
const POTENTIAL_FEEPAYER_SYNC_MIN_INTERVAL_MS = 1_000;

type PotentialFeePayerStatus =
  | 'watching'
  | 'normal_candidate'
  | 'low_candidate'
  | 'active'
  | 'cooldown'
  | 'stopped';

interface BundlerFundingEvent {
  recipient: string;
  amountSol: number;
  recipientPostBalanceSol: number;
  signature: string;
  timestamp: number;
  band: 'normal' | 'low';
}

interface ActiveBundlerGroup {
  anchorKey: string;
  band: 'normal' | 'low';
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
  /** REST sync cursor — txs after funder-receive signature. */
  cursorSignature: string | null;
  isSyncing: boolean;
  syncPending: boolean;
  syncPendingForce: boolean;
  lastSyncAt: number;
  bundlerFundingEvents: BundlerFundingEvent[];
  mode: 'normal' | 'low' | null;
  detectedMint: string | null;
  detectedDevWallet: string | null;
  cooldownDevWatchId: number | null;
  /** Concurrent active 3–4 bundler groups keyed by anchor. */
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
    mode: 'normal' | 'low' | null;
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
          'Potential feePayer pipeline armed — watching for 3–4 bundler sends in 10s (normal ≥20 SOL or low 5–19.99 post-balance, ≤0.5 SOL spread)',
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
          'Watching for 3–4 bundler funding txs in 10s…',
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
            this.stopPotentialFeePayerWatch(address, 'native SOL balance reached zero');
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
      const txs = await this.heliusClient.getAddressTransactionsAsc(
        syncingAddress,
        afterSignature,
        POTENTIAL_FEEPAYER_SYNC_LIMIT,
      );
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

    if (watch.activeGroups.size === 0) {
      const handedOff = await this.handlePreGroupHalfDrain(watch, tx, source);
      if (handedOff) return true;
    }

    for (const transfer of this.extractOutgoingSolTransfers(tx, address)) {
      const recipientPostBalanceSol = this.getAccountPostBalanceSol(
        tx,
        transfer.to,
      );
      if (recipientPostBalanceSol === null) continue;

      let band: 'normal' | 'low' | null = null;
      if (recipientPostBalanceSol >= NORMAL_MIN_POST_SOL) {
        band = 'normal';
      } else if (
        recipientPostBalanceSol >= LOW_MIN_POST_SOL &&
        recipientPostBalanceSol <= LOW_MAX_POST_SOL
      ) {
        band = 'low';
      }
      if (!band) {
        log.debug(
          'Outgoing SOL ignored — recipient post-balance outside bundler bands (need ≥20 normal or 5–19.99 low)',
          {
            potentialFeePayer: address,
            bundlerRecipient: transfer.to,
            recipientPostBalanceSol,
            signature: tx.signature,
            source,
          },
        );
        continue;
      }

      watch.bundlerFundingEvents.push({
        recipient: transfer.to,
        amountSol: transfer.amountSol,
        recipientPostBalanceSol,
        signature: tx.signature,
        timestamp: tx.timestamp,
        band,
      });
      watch.recipientBalanceAtReceive.set(transfer.to, recipientPostBalanceSol);
      log.info('Potential feePayer bundler funding event recorded', {
        potentialFeePayer: address,
        bundlerRecipient: transfer.to,
        band,
        amountSol: transfer.amountSol,
        recipientPostBalanceSol,
        signature: tx.signature,
        source,
        totalFundingEvents: watch.bundlerFundingEvents.length,
      });
    }

    const groupsBefore = watch.activeGroups.size;
    await this.evaluateBundlerGroups(watch);
    if (watch.bundlerFundingEvents.length > 0) {
      log.info('Potential feePayer group evaluation', {
        potentialFeePayer: address,
        status: watch.status,
        fundingEvents: watch.bundlerFundingEvents.length,
        activeGroups: watch.activeGroups.size,
        newGroups: watch.activeGroups.size - groupsBefore,
        source,
      });
    }
    return false;
  }

  /**
   * Before any bundler group exists: if an outgoing tx drops balance to ≤50% of the
   * post-funder-receive baseline, check whether SOL went back to the top funder.
   * If not, hand off to the primary SOL recipient and restart fresh there.
   */
  private async handlePreGroupHalfDrain(
    watch: PotentialFeePayerWatch,
    tx: HeliusTransaction,
    source: 'wss' | 'rest',
  ): Promise<boolean> {
    if (watch.balanceAtFunderReceiveSol === null) return false;

    const selfPostBalanceSol = this.getAccountPostBalanceSol(tx, watch.address);
    if (selfPostBalanceSol === null) return false;
    if (!this.hasOutgoingSolFrom(tx, watch.address)) return false;
    if (selfPostBalanceSol > watch.balanceAtFunderReceiveSol * HALF_DRAIN_RATIO) {
      return false;
    }

    const funder = this.funderAddress;
    if (funder && this.transfersSolTo(watch.address, funder, tx)) {
      log.info('Potential feePayer half-drain returned SOL to funder — keeping watch', {
        potentialFeePayer: watch.address,
        funder,
        signature: tx.signature,
        source,
        balanceAtReceive: watch.balanceAtFunderReceiveSol,
        balanceNow: selfPostBalanceSol,
      });
      return false;
    }

    const outgoing = this.extractOutgoingSolTransfers(tx, watch.address).filter(
      (transfer) => transfer.to !== funder,
    );
    const handoff = [...outgoing].sort((a, b) => b.amountSol - a.amountSol)[0];
    if (!handoff) {
      void this.sendTelegram([
        '<b>⏹️ Funder-First: FeePayer Watch Stopped</b>',
        `FeePayer: <code>${this.html(watch.address)}</code>`,
        `Reason: <b>half-drain with no handoff recipient</b>`,
        `Tx: <code>${this.html(tx.signature)}</code>`,
      ]);
      this.stopPotentialFeePayerWatch(
        watch.address,
        'half drain with no identifiable handoff recipient',
      );
      return true;
    }

    const handoffPostBalanceSol = this.getAccountPostBalanceSol(tx, handoff.to);
    if (handoffPostBalanceSol === null) {
      log.warn('Half-drain handoff skipped — recipient post-balance unknown', {
        potentialFeePayer: watch.address,
        handoffRecipient: handoff.to,
        signature: tx.signature,
      });
      return false;
    }

    void this.sendTelegram([
      '<b>🔀 Funder-First: FeePayer Handoff</b>',
      `Stopped: <code>${this.html(watch.address)}</code>`,
      `New potential feePayer: <code>${this.html(handoff.to)}</code>`,
      `Reason: <b>half-drain did not return SOL to funder</b>`,
      `Balance at receive: <b>${watch.balanceAtFunderReceiveSol.toFixed(4)} SOL</b>`,
      `Balance after drain: <b>${selfPostBalanceSol.toFixed(4)} SOL</b>`,
      `Handoff amount: <b>${handoff.amountSol.toFixed(4)} SOL</b>`,
      `Handoff post-balance: <b>${handoffPostBalanceSol.toFixed(4)} SOL</b>`,
      `Tx: <code>${this.html(tx.signature)}</code>`,
    ]);

    const oldAddress = watch.address;
    this.stopPotentialFeePayerWatch(
      oldAddress,
      `half-drain handoff to ${handoff.to}`,
    );
    this.startFreshPotentialFeePayerWatch(
      handoff.to,
      tx.signature,
      handoffPostBalanceSol,
    );
    return true;
  }

  private startFreshPotentialFeePayerWatch(
    address: string,
    receiveSignature: string,
    postBalanceSol: number,
  ): void {
    if (this.potentialFeePayers.has(address)) {
      this.stopPotentialFeePayerWatch(address, 'reset for fresh handoff');
    }
    const watch = this.ensurePotentialFeePayerWatch(address);
    if (!watch) return;
    watch.balanceAtFunderReceiveSol = postBalanceSol;
    watch.balanceAtFunderReceiveSignature = receiveSignature;
    watch.cursorSignature = receiveSignature;
    watch.processedSignatures.add(receiveSignature);
    log.info('Started fresh potential feePayer watch after handoff', {
      address,
      receiveSignature,
      postBalanceSol,
      enhancedWatchId: watch.enhancedWatchId,
    });
    log.info(
      'Potential feePayer pipeline armed — watching for 3–4 bundler sends in 10s (normal ≥20 SOL or low 5–19.99 post-balance, ≤0.5 SOL spread)',
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

  private getGroupAnchorKey(
    anchor: BundlerFundingEvent,
    band: 'normal' | 'low',
  ): string {
    return `${band}:${anchor.timestamp}:${anchor.signature}`;
  }

  private async evaluateBundlerGroups(watch: PotentialFeePayerWatch): Promise<void> {
    if (
      watch.status !== 'watching' &&
      watch.status !== 'normal_candidate' &&
      watch.status !== 'low_candidate'
    ) {
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

    for (const group of this.findAllBundlerGroups(watch, 'normal')) {
      this.activateGroup(watch, group, 'normal');
    }
    for (const group of this.findAllBundlerGroups(watch, 'low')) {
      this.activateGroup(watch, group, 'low');
    }

    this.updateWatchCandidateStatus(watch);
  }

  private updateWatchCandidateStatus(watch: PotentialFeePayerWatch): void {
    const groups = [...watch.activeGroups.values()];
    if (groups.some((g) => g.band === 'normal')) {
      watch.status = 'normal_candidate';
      watch.mode = 'normal';
    } else if (groups.length > 0) {
      watch.status = 'low_candidate';
      watch.mode = 'low';
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
    if (unique.size < MIN_BUNDLER_GROUP) return null;

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
        if (count < MIN_BUNDLER_GROUP || count > MAX_BUNDLER_GROUP) continue;

        const selected = byBalance.slice(left, right + 1);
        if (!this.postBalancesClustered(selected)) continue;

        const ordered = [...selected].sort((a, b) => a.timestamp - b.timestamp);
        const isBetter =
          !best ||
          ordered.length > best.length ||
          (ordered.length === best.length &&
            ordered[0]!.timestamp < best[0]!.timestamp);
        if (isBetter) best = ordered;
      }
    }

    if (maxClusterSize > MAX_BUNDLER_GROUP) return null;
    return best;
  }

  private findAllBundlerGroups(
    watch: PotentialFeePayerWatch,
    band: 'normal' | 'low',
  ): BundlerFundingEvent[][] {
    const events = watch.bundlerFundingEvents
      .filter((e) => e.band === band)
      .sort((a, b) => a.timestamp - b.timestamp);
    const found: BundlerFundingEvent[][] = [];
    const claimedAnchorKeys = new Set<string>();
    const claimedRecipients = new Set<string>();
    for (const group of watch.activeGroups.values()) {
      if (group.band !== band) continue;
      for (const recipient of group.recipients) {
        if (!group.stoppedRecipients.has(recipient)) {
          claimedRecipients.add(recipient);
        }
      }
    }

    for (let i = 0; i < events.length; i += 1) {
      const anchor = events[i]!;
      const anchorKey = this.getGroupAnchorKey(anchor, band);
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
      const groupAnchorKey = this.getGroupAnchorKey(groupAnchor, band);
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
    band: 'normal' | 'low',
  ): void {
    const anchor = group[0]!;
    const anchorKey = this.getGroupAnchorKey(anchor, band);
    if (watch.exhaustedGroupAnchors.has(anchorKey)) return;

    if (watch.activeGroups.has(anchorKey)) {
      this.syncAllActiveGroupSubscriptions(watch);
      return;
    }

    watch.activeGroups.set(anchorKey, {
      anchorKey,
      band,
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
      if (band === 'normal') {
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
      } else {
        void this.sendTelegram([
          '<b>⏭️ Funder-First: Low-Funding Pattern (5–19.99 SOL) — <u>Skipped</u></b>',
          `Potential FeePayer: <code>${this.html(watch.address)}</code>`,
          `Bundlers (${group.length} in 10s, post-balance spread ≤0.5 SOL):`,
          ...group.map(
            (e) =>
              `• <code>${this.html(e.recipient)}</code> — post <b>${e.recipientPostBalanceSol.toFixed(2)} SOL</b>`,
          ),
          `Spread: <b>${spread.toFixed(2)} SOL</b>`,
          '',
          'No buy — watching for a token buy to note the dev wallet, then waiting for rug before resuming this feePayer.',
        ]);
      }
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

  private findBundlerGroup(
    watch: PotentialFeePayerWatch,
    band: 'normal' | 'low',
  ): BundlerFundingEvent[] | null {
    return this.findAllBundlerGroups(watch, band)[0] ?? null;
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

    const mint = this.findSwapBuyMint(tx, recipient);
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

      const buyWalletGroup = [...watch.activeGroups.values()].find((g) =>
        g.recipients.has(buyWallet),
      );
      const confirmBand = buyWalletGroup?.band ?? watch.mode;

      if (confirmBand === 'low') {
        void this.sendTelegram([
          '<b>📋 Funder-First: Low-Funding Token Noted (no buy)</b>',
          `Token: <code>${this.html(mint)}</code>`,
          `FeePayer: <code>${this.html(watch.address)}</code>`,
          `Dev: <code>${this.html(devWallet ?? 'unknown')}</code>`,
          `Matched bundler: <code>${this.html(buyWallet)}</code>`,
          '',
          'FeePayer watch paused until dev rugs (CLOSE_ACCOUNT).',
        ]);
        this.enterCooldown(watch, mint, devWallet);
        return;
      }

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
    if (!this.isDevFullExitCloseAccountTx(tx, devWallet)) return;
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

  private findSwapBuyMint(tx: HeliusTransaction, wallet: string): string | null {
    if (tx.type !== 'SWAP') return null;
    for (const transfer of tx.tokenTransfers ?? []) {
      if (transfer.mint === SOL_MINT) continue;
      if (transfer.toUserAccount === wallet) return transfer.mint;
    }
    return null;
  }

  private isDevFullExitCloseAccountTx(
    tx: HeliusTransaction,
    devWallet: string,
  ): boolean {
    return (
      tx.type === 'CLOSE_ACCOUNT' &&
      tx.source === 'SOLANA_PROGRAM_LIBRARY' &&
      tx.feePayer === devWallet
    );
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
