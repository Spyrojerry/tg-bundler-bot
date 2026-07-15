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
//  Stop-watching rules for a potential feePayer:
//    • After receiving SOL from the funder, any outgoing transfer that leaves
//      the wallet at ≤50% of the balance right after the funder receive.
//    • Native SOL balance hits zero (account subscription).
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
  bundlerFundingEvents: BundlerFundingEvent[];
  mode: 'normal' | 'low' | null;
  detectedMint: string | null;
  detectedDevWallet: string | null;
  cooldownDevWatchId: number | null;
  lowNoteSent: boolean;
  normalNoteSent: boolean;
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
    for (const transfer of tx.nativeTransfers ?? []) {
      if (transfer.fromUserAccount !== funder) continue;
      const recipient = transfer.toUserAccount;
      if (!recipient || recipient === UNKNOWN_COUNTERPARTY) continue;
      const amountSol = transfer.amount / LAMPORTS_PER_SOL;
      if (amountSol <= 0) continue;

      const existing = this.potentialFeePayers.get(recipient);
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

      if (!existing) {
        this.ensurePotentialFeePayerWatch(recipient);
      }

      const watch = this.potentialFeePayers.get(recipient);
      if (!watch || watch.status === 'stopped') continue;

      const postBalanceSol = this.getAccountPostBalanceSol(tx, recipient);
      if (postBalanceSol === null) continue;

      const isTopUp = watch.balanceAtFunderReceiveSol !== null;
      watch.balanceAtFunderReceiveSol = postBalanceSol;
      watch.balanceAtFunderReceiveSignature = tx.signature;

      if (!isTopUp) {
        log.info('Potential feePayer received SOL from funder', {
          potentialFeePayer: recipient,
          amountSol,
          postBalanceSol,
          signature: tx.signature,
        });
        void this.sendTelegram([
          '<b>👀 Funder-First: New Potential FeePayer</b>',
          `Funder: <code>${this.html(funder)}</code>`,
          `Recipient: <code>${this.html(recipient)}</code>`,
          `Received: <b>${amountSol.toFixed(4)} SOL</b>`,
          `Post-balance: <b>${postBalanceSol.toFixed(4)} SOL</b>`,
        ]);
      } else {
        log.info('Potential feePayer topped up from funder — updated balance baseline', {
          potentialFeePayer: recipient,
          amountSol,
          postBalanceSol,
          status: watch.status,
          signature: tx.signature,
        });
      }
    }
  }

  private ensurePotentialFeePayerWatch(address: string): void {
    if (this.potentialFeePayers.has(address)) return;
    if (!this.enhancedWs) return;
    const watch: PotentialFeePayerWatch = {
      address,
      status: 'watching',
      enhancedWatchId: null,
      solBalanceSubId: null,
      subscribedRecipients: new Set(),
      processedSignatures: new Set(),
      recipientProcessedSignatures: new Map(),
      balanceAtFunderReceiveSol: null,
      balanceAtFunderReceiveSignature: null,
      bundlerFundingEvents: [],
      mode: null,
      detectedMint: null,
      detectedDevWallet: null,
      cooldownDevWatchId: null,
      lowNoteSent: false,
      normalNoteSent: false,
    };
    watch.enhancedWatchId = this.enhancedWs.watch(address, (tx) => {
      void this.handlePotentialFeePayerTx(address, tx);
    });
    watch.solBalanceSubId = this.connection.onAccountChange(
      new PublicKey(address),
      (info) => {
        if (info.lamports === 0) {
          this.stopPotentialFeePayerWatch(address, 'native SOL balance reached zero');
        }
      },
      'processed',
    );
    this.potentialFeePayers.set(address, watch);
    log.info('Started watching potential feePayer', { address });
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
    if (watch.cooldownDevWatchId !== null) {
      void this.enhancedWs?.unwatch(watch.cooldownDevWatchId).catch(() => undefined);
      watch.cooldownDevWatchId = null;
    }
    this.potentialFeePayers.delete(address);
    log.info('Stopped watching potential feePayer', { address, reason });
  }

  private async handlePotentialFeePayerTx(
    address: string,
    tx: HeliusTransaction,
  ): Promise<void> {
    const watch = this.potentialFeePayers.get(address);
    if (!watch || !this.isEnabled) return;
    if (watch.status === 'stopped' || watch.status === 'active') return;
    if (watch.processedSignatures.has(tx.signature)) return;
    watch.processedSignatures.add(tx.signature);

    if (watch.status === 'cooldown') return;

    const selfPostBalanceSol = this.getAccountPostBalanceSol(tx, address);
    if (
      watch.balanceAtFunderReceiveSol !== null &&
      selfPostBalanceSol !== null &&
      this.hasOutgoingSolFrom(tx, address)
    ) {
      if (selfPostBalanceSol <= watch.balanceAtFunderReceiveSol * HALF_DRAIN_RATIO) {
        void this.sendTelegram([
          '<b>⏹️ Funder-First: FeePayer Watch Stopped</b>',
          `FeePayer: <code>${this.html(address)}</code>`,
          `Reason: <b>balance drained to ≤50% after funder receive</b>`,
          `Balance at receive: <b>${watch.balanceAtFunderReceiveSol.toFixed(4)} SOL</b>`,
          `Balance now: <b>${selfPostBalanceSol.toFixed(4)} SOL</b>`,
        ]);
        this.stopPotentialFeePayerWatch(
          address,
          'balance drained to half or less after funder receive',
        );
        return;
      }
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
      if (!band) continue;

      watch.bundlerFundingEvents.push({
        recipient: transfer.to,
        amountSol: transfer.amountSol,
        recipientPostBalanceSol,
        signature: tx.signature,
        timestamp: tx.timestamp,
        band,
      });
    }

    await this.evaluateBundlerGroups(watch);
  }

  private async evaluateBundlerGroups(watch: PotentialFeePayerWatch): Promise<void> {
    if (watch.status !== 'watching' && watch.status !== 'normal_candidate' && watch.status !== 'low_candidate') {
      return;
    }

    const normalGroup = this.findBundlerGroup(watch, 'normal');
    if (normalGroup) {
      watch.status = 'normal_candidate';
      watch.mode = 'normal';
      if (!watch.normalNoteSent) {
        watch.normalNoteSent = true;
        void this.sendTelegram([
          '<b>✅ Funder-First: Normal Mode Candidate (≥20 SOL post-balance)</b>',
          `Potential FeePayer: <code>${this.html(watch.address)}</code>`,
          `Bundlers (${normalGroup.length} in 10s):`,
          ...normalGroup.map(
            (e) =>
              `• <code>${this.html(e.recipient)}</code> — post <b>${e.recipientPostBalanceSol.toFixed(2)} SOL</b>`,
          ),
          '',
          'Waiting for a SWAP buy from one of these wallets…',
        ]);
      }
      this.subscribeRecipients(watch, normalGroup.map((e) => e.recipient));
      return;
    }

    const lowGroup = this.findBundlerGroup(watch, 'low');
    if (lowGroup) {
      watch.status = 'low_candidate';
      watch.mode = 'low';
      if (!watch.lowNoteSent) {
        watch.lowNoteSent = true;
        void this.sendTelegram([
          '<b>⏭️ Funder-First: Low-Funding Pattern (5–19.99 SOL) — <u>Skipped</u></b>',
          `Potential FeePayer: <code>${this.html(watch.address)}</code>`,
          `Bundlers (${lowGroup.length} in 10s):`,
          ...lowGroup.map(
            (e) =>
              `• <code>${this.html(e.recipient)}</code> — post <b>${e.recipientPostBalanceSol.toFixed(2)} SOL</b>`,
          ),
          '',
          'No buy — watching for a token buy to note the dev wallet, then waiting for rug before resuming this feePayer.',
        ]);
      }
      this.subscribeRecipients(watch, lowGroup.map((e) => e.recipient));
    }
  }

  private findBundlerGroup(
    watch: PotentialFeePayerWatch,
    band: 'normal' | 'low',
  ): BundlerFundingEvent[] | null {
    const events = watch.bundlerFundingEvents
      .filter((e) => e.band === band)
      .sort((a, b) => a.timestamp - b.timestamp);
    for (let i = 0; i < events.length; i += 1) {
      const anchor = events[i];
      const window = events.filter(
        (e) =>
          e.timestamp >= anchor.timestamp &&
          e.timestamp <= anchor.timestamp + GROUP_WINDOW_SEC,
      );
      const uniqueRecipients = new Map<string, BundlerFundingEvent>();
      for (const e of window) {
        if (!uniqueRecipients.has(e.recipient)) uniqueRecipients.set(e.recipient, e);
      }
      const group = [...uniqueRecipients.values()];
      if (group.length >= MIN_BUNDLER_GROUP && group.length <= MAX_BUNDLER_GROUP) {
        return group;
      }
    }
    return null;
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

      if (watch.mode === 'low') {
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
      this.unsubscribePotentialFeePayerOnly(watch);

      const started = await this.insiderBot.startFromFunderFirst(
        mint,
        watch.address,
        earlyBuys,
      );
      if (!started) {
        watch.status = 'normal_candidate';
        this.resubscribePotentialFeePayer(watch);
        const group = this.findBundlerGroup(watch, 'normal');
        if (group) this.subscribeRecipients(watch, group.map((e) => e.recipient));
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
    if (!this.enhancedWs || watch.enhancedWatchId !== null) return;
    watch.enhancedWatchId = this.enhancedWs.watch(watch.address, (tx) => {
      void this.handlePotentialFeePayerTx(watch.address, tx);
    });
    if (watch.solBalanceSubId === null) {
      watch.solBalanceSubId = this.connection.onAccountChange(
        new PublicKey(watch.address),
        (info) => {
          if (info.lamports === 0) {
            this.stopPotentialFeePayerWatch(
              watch.address,
              'native SOL balance reached zero',
            );
          }
        },
        'processed',
      );
    }
  }

  private enterCooldown(
    watch: PotentialFeePayerWatch,
    mint: string,
    devWallet: string | null,
  ): void {
    watch.status = 'cooldown';
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
    watch.normalNoteSent = false;
    watch.lowNoteSent = false;
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
