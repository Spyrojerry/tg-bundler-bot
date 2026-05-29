// ─────────────────────────────────────────────────────────────────────────────
//  wallet-monitor.ts  —  Watches a Solana wallet for new token buys
//
//  On start:
//    1. Fetch all current token accounts → existingTokens set (NOT monitored)
//    2. Subscribe to wallet logs over websocket for immediate buy detection
//    3. Poll token-account state as a fallback/backfill
//    4. Any mint not in existingTokens and not yet tracked → emit 'newToken'
// ─────────────────────────────────────────────────────────────────────────────

import {
  Connection,
  LAMPORTS_PER_SOL,
  PublicKey,
  ParsedAccountData,
  TokenAccountBalancePair,
} from '@solana/web3.js';
import { EventEmitter } from 'events';
import { createLogger } from './logger';
import { NewTokenEvent, ServiceConfig, TokenExitEvent, TokenHolding } from './types';

const log = createLogger('WALLET');
const WALLET_COMMITMENT = 'processed';

const TOKEN_PROGRAM_IDS = [
  new PublicKey('TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'),
  new PublicKey('TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb'),
];

// ── WalletMonitor ─────────────────────────────────────────────────────────────

export class WalletMonitor extends EventEmitter {
  private readonly connection: Connection;
  private readonly walletPubkey: PublicKey;
  private readonly pollInterval: number;
  private readonly minBuySol: number;
  private readonly wsEndpoint: string;

  /** Mints present at startup — we ignore these */
  private existingTokens: Set<string> = new Set();

  /** All mints we've emitted a newToken event for (to avoid duplicates) */
  private knownMints: Set<string> = new Set();
  /** Mints currently held with positive balance. */
  private heldMints: Set<string> = new Set();

  /** Websocket logs subscription id returned by Connection.onLogs. */
  private logsSubscriptionId: number | null = null;

  /** Signatures currently being parsed from websocket notifications. */
  private pendingSignatures: Set<string> = new Set();
  private minBuyUnknownLogged: Set<string> = new Set();

  private pollTimer: NodeJS.Timeout | null = null;
  private running = false;

  constructor(
    config: ServiceConfig,
    walletAddress?: string,
    options: { enforceMinBuySol?: boolean; minBuySol?: number } = {}
  ) {
    super();

    if (!walletAddress) {
      throw new Error('WalletMonitor requires a wallet address');
    }

    this.connection = new Connection(config.solanaRpcUrl, {
      commitment: WALLET_COMMITMENT,
      wsEndpoint: config.solanaWsUrl,
    });

    try {
      this.walletPubkey = new PublicKey(walletAddress);
    } catch {
      throw new Error(`Invalid wallet address: ${walletAddress}`);
    }

    this.pollInterval = config.walletPollInterval;
    if (options.minBuySol !== undefined) {
      this.minBuySol = options.minBuySol;
    } else {
      this.minBuySol = options.enforceMinBuySol === false ? 0 : config.minBuySol;
    }
    this.wsEndpoint = config.solanaWsUrl;
  }

  // ── Lifecycle ──────────────────────────────────────────────────────────────

  async start(): Promise<void> {
    if (this.running) return;
    this.running = true;

    log.info(`Starting wallet monitor for ${this.walletPubkey.toBase58()}`);
    log.info(`Poll interval: ${this.pollInterval}ms`);
    log.info(`Websocket endpoint: ${this.wsEndpoint}`);

    // Snapshot existing holdings — these are NOT monitored
    const initial = await this.fetchHoldings();
    for (const h of initial) {
      if (!this.hasPositiveBalance(h)) continue;
      this.existingTokens.add(h.mint);
      this.knownMints.add(h.mint);
      this.heldMints.add(h.mint);
    }

    log.info(
      `Snapshot taken: ${this.existingTokens.size} existing token(s) — these will NOT be monitored`,
      { mints: [...this.existingTokens] }
    );

    this.startLogsSubscription();
    this.schedulePoll();
  }

  stop(): void {
    this.running = false;
    if (this.pollTimer) {
      clearTimeout(this.pollTimer);
      this.pollTimer = null;
    }
    if (this.logsSubscriptionId !== null) {
      const subscriptionId = this.logsSubscriptionId;
      this.logsSubscriptionId = null;
      this.connection
        .removeOnLogsListener(subscriptionId)
        .catch((err) => log.warn('Failed to remove logs subscription', err));
    }
    log.info('Wallet monitor stopped');
  }

  // ── Snapshot accessor (for DB pre-population) ─────────────────────────────

  get existingMints(): ReadonlySet<string> {
    return this.existingTokens;
  }

  // ── Polling ───────────────────────────────────────────────────────────────

  private schedulePoll(): void {
    if (!this.running) return;
    this.pollTimer = setTimeout(() => {
      this.poll().catch((err) => log.error('Poll error', err));
    }, this.pollInterval);
  }

  private startLogsSubscription(): void {
    log.info(`WS subscribing to wallet logs for ${this.walletPubkey.toBase58()}`);

    this.logsSubscriptionId = this.connection.onLogs(
      this.walletPubkey,
      (logInfo) => {
        if (logInfo.err) {
          log.debug(`Wallet logs notification had tx error: ${logInfo.signature}`);
          return;
        }

        log.info(`[WS TX] ${logInfo.signature}`);
        this.processSignature(logInfo.signature, 'logsSubscribe').catch((err) =>
          log.error(`Failed to process logs signature ${logInfo.signature}`, err)
        );
      },
      WALLET_COMMITMENT
    );

    log.info(`WS logsSubscribe active (id=${this.logsSubscriptionId})`);
  }

  private async poll(): Promise<void> {
    try {
      const holdings = await this.fetchHoldings();
      const now = Date.now();
      const currentlyHeld = new Set<string>();

      log.debug(`Poll complete: ${holdings.length} holding(s)`);

      for (const holding of holdings) {
        if (!this.hasPositiveBalance(holding)) {
          log.debug(`Skipping zero-balance mint ${holding.mint}; will keep watching`);
          continue;
        }
        currentlyHeld.add(holding.mint);

        this.emitNewToken(holding.mint, now, holding.uiAmount ?? holding.amount, 'account-poll', null);
      }
      this.detectExitedTokens(currentlyHeld, now, 'account-poll');
    } catch (err) {
      log.error('Failed to poll wallet holdings', err);
    } finally {
      this.schedulePoll();
    }
  }

  // ── RPC: fetch SPL token accounts ─────────────────────────────────────────

  private async fetchHoldings(): Promise<TokenHolding[]> {
    const holdings: TokenHolding[] = [];

    for (const programId of TOKEN_PROGRAM_IDS) {
      const resp = await this.connection.getParsedTokenAccountsByOwner(
        this.walletPubkey,
        { programId },
        WALLET_COMMITMENT
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

    this.pendingSignatures.add(signature);
    try {
      const boughtMints = await this.fetchBoughtMintsFromSignature(signature);
      if (!boughtMints) return;

      if (boughtMints.length === 0) {
        log.debug(`[WS TX] ${signature} parsed: no wallet token balance increase`);
      }

      for (const buy of boughtMints) {
        log.info(`[WS BUY] ${signature} -> ${buy.mint}`, { buySol: buy.buySol });
        this.emitNewToken(buy.mint, Date.now(), 'tx-detected', source, buy.buySol);
      }
    } finally {
      this.pendingSignatures.delete(signature);
    }
  }

  private async fetchBoughtMintsFromSignature(
    signature: string
  ): Promise<Array<{ mint: string; buySol: number | null }> | null> {
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
      log.debug(`Transaction ${signature} was not parsed after retries`);
      return null;
    }

    const boughtMints = new Set<string>();
    const preBalances = tx.meta?.preTokenBalances ?? [];
    const postBalances = tx.meta?.postTokenBalances ?? [];
    const accountKeys = tx.transaction.message.accountKeys;
    const walletIndex = accountKeys.findIndex((key) =>
      key.pubkey.equals(this.walletPubkey)
    );
    const buySol = this.estimateSolSpent(tx.meta?.preBalances, tx.meta?.postBalances, walletIndex);

    for (const post of postBalances) {
      if (post.owner !== this.walletPubkey.toBase58()) continue;

      const before = preBalances.find(
        (pre) => pre.accountIndex === post.accountIndex && pre.mint === post.mint
      );
      const preAmount = BigInt(before?.uiTokenAmount.amount ?? '0');
      const postAmount = BigInt(post.uiTokenAmount.amount);

      if (postAmount > preAmount) {
        boughtMints.add(post.mint);
      }
    }

    return [...boughtMints].map((mint) => ({ mint, buySol }));
  }

  private estimateSolSpent(
    preBalances: number[] | undefined,
    postBalances: number[] | undefined,
    walletIndex: number
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

  private detectExitedTokens(currentlyHeld: Set<string>, detectedAt: number, source: string): void {
    for (const mint of this.heldMints) {
      if (currentlyHeld.has(mint)) continue;
      this.knownMints.delete(mint);
      this.existingTokens.delete(mint);
      this.minBuyUnknownLogged.delete(mint);
      log.info(`[TOKEN EXITED] Mint: ${mint}  Source: ${source}`);
      const event: TokenExitEvent = {
        walletAddress: this.walletPubkey.toBase58(),
        mint,
        detectedAt,
        source,
      };
      this.emit('tokenExited', event);
    }
    this.heldMints = currentlyHeld;
  }

  private emitNewToken(
    mint: string,
    detectedAt: number,
    amount: string | number,
    source: string,
    buySol: number | null
  ): void {
    if (this.knownMints.has(mint)) return;

    if (this.minBuySol > 0) {
      if (buySol === null) {
        if (!this.minBuyUnknownLogged.has(mint)) {
          log.info(
            `[WAIT TOKEN] Mint: ${mint}  Source: ${source}  ` +
            `Reason: buy SOL unknown, waiting for tx parse to check min ${this.minBuySol} SOL`
          );
          this.minBuyUnknownLogged.add(mint);
        }
        return;
      }
      if (buySol < this.minBuySol) {
        log.info(
          `[SKIP TOKEN] Mint: ${mint}  Buy: ${buySol} SOL  ` +
          `Min: ${this.minBuySol} SOL  Source: ${source}`
        );
        this.knownMints.add(mint);
        return;
      }
    }

    this.knownMints.add(mint);
    log.info(`[NEW TOKEN] Mint: ${mint}  Amount: ${amount}  Source: ${source}  BuySOL: ${buySol ?? 'unknown'}`);

    const event: NewTokenEvent = {
      walletAddress: this.walletPubkey.toBase58(),
      mint,
      detectedAt,
      buySol,
    };
    this.emit('newToken', event);
  }

  private hasPositiveBalance(holding: TokenHolding): boolean {
    return BigInt(holding.amount) > 0n;
  }
}
