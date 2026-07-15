// ─────────────────────────────────────────────────────────────────────────────
//  wallet-monitor.ts  —  Watches a Solana wallet for new token buys
//
//  On start:
//    1. Fetch all current token accounts → existingTokens set (NOT monitored)
//    2. Push-driven buy detection (Enhanced WSS when available, else onLogs + RPC)
//    3. Any mint not in existingTokens and not yet tracked → emit 'newToken'
// ─────────────────────────────────────────────────────────────────────────────

import {
  Connection,
  LAMPORTS_PER_SOL,
  PublicKey,
  ParsedAccountData,
  TokenAccountBalancePair,
} from '@solana/web3.js';
import { EventEmitter } from 'events';
import { createLogger, Logger } from './logger';
import { HeliusTransaction } from './helius-client';
import { HeliusEnhancedWsClient } from './helius-enhanced-ws';
import { NewTokenEvent, ServiceConfig, TokenHolding } from './types';

const WALLET_COMMITMENT = 'processed';
const SOL_MINT = 'So11111111111111111111111111111111111111112';

const TOKEN_PROGRAM_IDS = [
  new PublicKey('TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'),
  new PublicKey('TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb'),
];

// ── WalletMonitor ─────────────────────────────────────────────────────────────

export class WalletMonitor extends EventEmitter {
  private readonly log: Logger;
  private readonly connection: Connection;
  private readonly walletPubkey: PublicKey;
  private readonly minBuySol: number;
  private readonly wsEndpoint: string;

  /** Mints present at startup — we ignore these */
  private existingTokens: Set<string> = new Set();

  /** All mints we've emitted a newToken event for (to avoid duplicates) */
  private knownMints: Set<string> = new Set();
  /** Mints currently held with positive balance. */
  private heldMints: Set<string> = new Set();

  /** Websocket logs subscription id returned by Connection.onLogs (fallback path). */
  private logsSubscriptionId: number | null = null;
  /** Enhanced WSS watch handle when `options.enhancedWs` is provided. */
  private enhancedWatchId: number | null = null;
  private readonly enhancedWs: HeliusEnhancedWsClient | null;
  private readonly enhancedSeenSignatures = new Set<string>();

  /** Signatures currently being parsed from websocket notifications. */
  private pendingSignatures = new Set<string>();
  private minBuyUnknownLogged = new Set<string>();

  private running = false;
  private activeProcessingCount = 0;
  private readonly MAX_CONCURRENT_PROCESSING = 3;

  constructor(
    config: ServiceConfig,
    walletAddress?: string,
    options: {
      enforceMinBuySol?: boolean;
      minBuySol?: number;
      rpcUrl?: string;
      wsUrl?: string;
      logLabel?: string;
      /** When set, uses Helius `transactionSubscribe` instead of `onLogs` + RPC fetch. */
      enhancedWs?: HeliusEnhancedWsClient | null;
    } = {}
  ) {
    super();
    this.log = createLogger(options.logLabel ?? 'WALLET GENERAL 1');

    if (!walletAddress) {
      throw new Error('WalletMonitor requires a wallet address');
    }

    const rpcUrl = options.rpcUrl ?? config.solanaRpcUrl;
    const wsUrl = options.wsUrl ?? config.solanaWsUrl;

    this.connection = new Connection(rpcUrl, {
      commitment: WALLET_COMMITMENT,
      wsEndpoint: wsUrl,
    });

    try {
      this.walletPubkey = new PublicKey(walletAddress);
    } catch {
      throw new Error(`Invalid wallet address: ${walletAddress}`);
    }

    if (options.minBuySol !== undefined) {
      this.minBuySol = options.minBuySol;
    } else {
      this.minBuySol = options.enforceMinBuySol === false ? 0 : config.minBuySol;
    }
    this.wsEndpoint = wsUrl;
    this.enhancedWs = options.enhancedWs ?? null;
  }

  // ── Lifecycle ──────────────────────────────────────────────────────────────

  async start(): Promise<void> {
    if (this.running) return;
    this.running = true;

    this.log.info(`Starting wallet monitor for ${this.walletPubkey.toBase58()}`);
    if (this.enhancedWs) {
      this.log.info('Push path: Helius Enhanced WSS transactionSubscribe');
    } else {
      this.log.info(`Websocket endpoint: ${this.wsEndpoint}`);
    }

    // One-time snapshot so pre-existing holdings are not treated as new buys.
    const initial = await this.fetchHoldings();
    for (const h of initial) {
      if (!this.hasPositiveBalance(h)) continue;
      this.existingTokens.add(h.mint);
      this.knownMints.add(h.mint);
      this.heldMints.add(h.mint);
    }

    this.log.info(
      `Snapshot taken: ${this.existingTokens.size} existing token(s) — these will NOT be monitored`,
      { mints: [...this.existingTokens] },
    );

    if (this.enhancedWs) {
      this.startEnhancedWsSubscription();
    } else {
      this.startLogsSubscription();
    }
  }

  stop(): void {
    this.running = false;
    if (this.enhancedWatchId !== null && this.enhancedWs) {
      const id = this.enhancedWatchId;
      this.enhancedWatchId = null;
      void this.enhancedWs.unwatch(id).catch((err) =>
        this.log.warn('Failed to remove Enhanced WSS watch', err),
      );
    }
    if (this.logsSubscriptionId !== null) {
      const subscriptionId = this.logsSubscriptionId;
      this.logsSubscriptionId = null;
      this.connection
        .removeOnLogsListener(subscriptionId)
        .catch((err) => this.log.warn('Failed to remove logs subscription', err));
    }
    this.log.info('Wallet monitor stopped');
  }

  get existingMints(): ReadonlySet<string> {
    return this.existingTokens;
  }

  private startEnhancedWsSubscription(): void {
    if (!this.enhancedWs) return;
    const wallet = this.walletPubkey.toBase58();
    this.log.info(`Enhanced WSS subscribing to wallet ${wallet}`);
    this.enhancedWatchId = this.enhancedWs.watch(wallet, (tx) => {
      this.handleEnhancedWsTx(tx).catch((err) =>
        this.log.error(`Failed to process Enhanced WSS tx for ${wallet}`, err),
      );
    });
  }

  private async handleEnhancedWsTx(tx: HeliusTransaction): Promise<void> {
    if (!this.running || this.enhancedSeenSignatures.has(tx.signature)) return;
    this.enhancedSeenSignatures.add(tx.signature);

    const wallet = this.walletPubkey.toBase58();
    const boughtMints = this.detectBoughtMintsFromHeliusTx(tx, wallet);
    if (boughtMints.length === 0) return;

    for (const buy of boughtMints) {
      if (this.knownMints.has(buy.mint)) {
        this.heldMints.add(buy.mint);
        continue;
      }
      this.log.info(`[ENHANCED WS BUY] ${tx.signature} -> ${buy.mint}`, {
        buySol: buy.buySol,
      });
      this.emit('buyDetected', {
        walletAddress: wallet,
        mint: buy.mint,
        detectedAt: Date.now(),
        buySol: buy.buySol,
        signature: tx.signature,
        timestamp: tx.timestamp,
      });
      this.emitNewToken(
        buy.mint,
        Date.now(),
        'tx-detected',
        'enhancedWs',
        buy.buySol,
        tx.signature,
        tx.timestamp,
      );
    }
  }

  private detectBoughtMintsFromHeliusTx(
    tx: HeliusTransaction,
    wallet: string,
  ): Array<{ mint: string; buySol: number | null }> {
    const buys: Array<{ mint: string; buySol: number | null }> = [];
    const seenMints = new Set<string>();
    for (const transfer of tx.tokenTransfers ?? []) {
      if (transfer.mint === SOL_MINT) continue;
      if (transfer.toUserAccount !== wallet) continue;
      if (seenMints.has(transfer.mint)) continue;
      seenMints.add(transfer.mint);
      buys.push({
        mint: transfer.mint,
        buySol: this.estimateSolSpentFromHeliusTx(tx, wallet),
      });
    }
    return buys;
  }

  private estimateSolSpentFromHeliusTx(
    tx: HeliusTransaction,
    wallet: string,
  ): number | null {
    const entry = tx.accountData?.find((a) => a.account === wallet);
    if (!entry?.nativeBalanceChange) return null;
    const spentLamports = -entry.nativeBalanceChange;
    return spentLamports > 0
      ? parseFloat((spentLamports / LAMPORTS_PER_SOL).toFixed(6))
      : 0;
  }

  private startLogsSubscription(): void {
    this.log.info(`WS subscribing to wallet logs for ${this.walletPubkey.toBase58()}`);

    this.logsSubscriptionId = this.connection.onLogs(
      this.walletPubkey,
      (logInfo) => {
        if (logInfo.err) {
          this.log.debug(`Wallet logs notification had tx error: ${logInfo.signature}`);
          return;
        }

        const logs = logInfo.logs || [];
        const isLikelyTokenTx = logs.some((l) =>
          l.includes('Transfer') ||
          l.includes('Swap') ||
          l.includes('Buy') ||
          l.includes('Sell') ||
          l.includes('Route') ||
          l.includes('Token'),
        );

        if (!isLikelyTokenTx) {
          this.log.debug(`[WS SKIP] ${logInfo.signature} (non-token tx)`);
          return;
        }

        this.log.info(`[WS TX] ${logInfo.signature}`);
        this.processSignature(logInfo.signature, 'logsSubscribe').catch((err) =>
          this.log.error(`Failed to process logs signature ${logInfo.signature}`, err),
        );
      },
      WALLET_COMMITMENT,
    );

    this.log.info(`WS logsSubscribe active (id=${this.logsSubscriptionId})`);
  }

  private async fetchHoldings(): Promise<TokenHolding[]> {
    const holdings: TokenHolding[] = [];

    for (const programId of TOKEN_PROGRAM_IDS) {
      const resp = await this.connection.getParsedTokenAccountsByOwner(
        this.walletPubkey,
        { programId },
        WALLET_COMMITMENT,
      );

      for (const { account } of resp.value) {
        const parsedData = account.data as ParsedAccountData;
        const info = parsedData?.parsed?.info as {
          mint: string;
          tokenAmount: TokenAccountBalancePair;
        } | undefined;

        if (!info?.mint) continue;

        holdings.push({
          mint: info.mint,
          amount: info.tokenAmount.amount,
          decimals: info.tokenAmount.decimals,
          uiAmount: info.tokenAmount.uiAmount,
        });
      }
    }

    return holdings;
  }

  private async processSignature(signature: string, source: string): Promise<void> {
    if (!this.running || this.pendingSignatures.has(signature)) return;

    while (this.activeProcessingCount >= this.MAX_CONCURRENT_PROCESSING) {
      await new Promise((resolve) => setTimeout(resolve, 100));
      if (!this.running) return;
    }

    this.pendingSignatures.add(signature);
    this.activeProcessingCount++;

    try {
      const boughtMints = await this.fetchBoughtMintsFromSignature(signature);
      if (!boughtMints) return;

      for (const buy of boughtMints) {
        if (this.knownMints.has(buy.mint)) {
          this.heldMints.add(buy.mint);
          continue;
        }

        this.log.info(`[WS BUY] ${signature} -> ${buy.mint}`, { buySol: buy.buySol });

        this.emit('buyDetected', {
          walletAddress: this.walletPubkey.toBase58(),
          mint: buy.mint,
          detectedAt: Date.now(),
          buySol: buy.buySol,
          signature,
          timestamp: buy.timestamp,
        });

        this.emitNewToken(
          buy.mint,
          Date.now(),
          'tx-detected',
          source,
          buy.buySol,
          signature,
          buy.timestamp,
        );
      }
    } finally {
      this.pendingSignatures.delete(signature);
      this.activeProcessingCount--;
    }
  }

  private async fetchBoughtMintsFromSignature(
    signature: string,
  ): Promise<Array<{ mint: string; buySol: number | null; timestamp?: number }> | null> {
    let tx = null;
    for (let attempt = 1; attempt <= 5; attempt++) {
      tx = await this.connection.getParsedTransaction(signature, {
        commitment: 'confirmed',
        maxSupportedTransactionVersion: 0,
      });
      if (tx) break;
      await new Promise((resolve) => setTimeout(resolve, attempt * 250));
    }

    if (!tx) {
      this.log.debug(`Transaction ${signature} was not parsed after retries`);
      return null;
    }

    const boughtMints = new Set<string>();
    const preBalances = tx.meta?.preTokenBalances ?? [];
    const postBalances = tx.meta?.postTokenBalances ?? [];
    const accountKeys = tx.transaction.message.accountKeys;
    const walletIndex = accountKeys.findIndex((key) =>
      key.pubkey.equals(this.walletPubkey),
    );
    const buySol = this.estimateSolSpent(
      tx.meta?.preBalances,
      tx.meta?.postBalances,
      walletIndex,
    );

    for (const post of postBalances) {
      if (post.owner !== this.walletPubkey.toBase58()) continue;

      const before = preBalances.find(
        (pre) => pre.accountIndex === post.accountIndex && pre.mint === post.mint,
      );
      const preAmount = BigInt(before?.uiTokenAmount.amount ?? '0');
      const postAmount = BigInt(post.uiTokenAmount.amount);

      if (postAmount > preAmount) {
        boughtMints.add(post.mint);
      }
    }

    return [...boughtMints].map((mint) => ({
      mint,
      buySol,
      timestamp: tx.blockTime ?? undefined,
    }));
  }

  private estimateSolSpent(
    preBalances: number[] | undefined,
    postBalances: number[] | undefined,
    walletIndex: number,
  ): number | null {
    if (!preBalances || !postBalances || walletIndex < 0) return null;
    const pre = preBalances[walletIndex];
    const post = postBalances[walletIndex];
    if (!Number.isFinite(pre) || !Number.isFinite(post)) return null;
    const spentLamports = pre - post;
    return spentLamports > 0
      ? parseFloat((spentLamports / LAMPORTS_PER_SOL).toFixed(6))
      : 0;
  }

  private emitNewToken(
    mint: string,
    detectedAt: number,
    amount: string | number,
    source: string,
    buySol: number | null,
    signature?: string,
    timestamp?: number,
  ): void {
    if (this.knownMints.has(mint)) return;

    if (this.minBuySol > 0) {
      if (buySol === null) {
        if (!this.minBuyUnknownLogged.has(mint)) {
          this.log.info(
            `[WAIT TOKEN] Mint: ${mint}  Source: ${source}  ` +
              `Reason: buy SOL unknown, waiting for tx parse to check min ${this.minBuySol} SOL`,
          );
          this.minBuyUnknownLogged.add(mint);
        }
        return;
      }
      if (buySol < this.minBuySol) {
        this.log.info(
          `[SKIP TOKEN] Mint: ${mint}  Buy: ${buySol} SOL  ` +
            `Min: ${this.minBuySol} SOL  Source: ${source}`,
        );
        this.knownMints.add(mint);
        return;
      }
    }

    this.knownMints.add(mint);
    this.log.info(
      `[NEW TOKEN] Mint: ${mint}  Amount: ${amount}  Source: ${source}  BuySOL: ${buySol ?? 'unknown'}`,
    );

    const event: NewTokenEvent = {
      walletAddress: this.walletPubkey.toBase58(),
      mint,
      detectedAt,
      buySol,
      signature,
      timestamp,
    };
    this.emit('newToken', event);
  }

  private hasPositiveBalance(holding: TokenHolding): boolean {
    return BigInt(holding.amount) > 0n;
  }
}
