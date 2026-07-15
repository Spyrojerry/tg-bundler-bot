// ─────────────────────────────────────────────────────────────────────────────
//  token-transfer-orchestrator.ts  —  "Token Transfer" mode
//
//  Manually started/stopped mode: watches a user-supplied dev wallet's
//  transactions in two steps:
//    1. A SWAP tx where the dev wallet receives a token it isn't already
//       being watched for (i.e. a fresh buy) marks that mint as a
//       "candidate" — something the dev has newly acquired.
//    2. A plain SPL token TRANSFER (not a swap/sell) where the dev sends a
//       *candidate* mint out to another wallet triggers the buy.
//  This two-step gate means only tokens the dev wallet actually just bought
//  are eligible to trigger a buy on transfer-out — an unrelated transfer of
//  some old/unrelated token the dev happens to hold is ignored.
//
//  Once a transfer-out fires, the buy is triggered immediately — but unlike
//  before, the dev-wallet watch does NOT stop. It keeps polling the same
//  wallet, now looking for a plain transfer-IN of that *same* mint back to
//  the dev wallet. The moment an incoming transfer's USD worth is the same
//  as or greater than the original outgoing transfer's USD worth, that's
//  treated as a sell signal and the position is closed automatically. There
//  is otherwise no automatic sell — the position can also be closed any time
//  via the Telegram "Sell Position" button, or automatically cleared (and
//  the mode left stopped) if the trading wallet's balance for that token is
//  independently observed to reach zero. Whichever way the position closes,
//  the dev-wallet watch stops at that point and stays stopped until the user
//  presses Start again.
//
//  Dedicated to Helius API key 4 (config.insiderHeliusApiKey4) for both the
//  dev-wallet transaction polling and the post-buy market-cap monitoring, so
//  this mode never competes with the Insider bots' shared Helius key pool.
// ─────────────────────────────────────────────────────────────────────────────

import { PublicKey } from '@solana/web3.js';
import { EventEmitter } from 'events';
import { createLogger } from './logger';
import { HeliusClient, HeliusTransaction } from './helius-client';
import { HeliusEnhancedWsClient } from './helius-enhanced-ws';
import { PumpReserveMarketCapClient } from './pump-reserve-market-cap';
import type { ServiceConfig } from './types';
import { TelegramBot } from './telegram-bot';

const log = createLogger('TOKEN TRANSFER');

const SOL_MINT = 'So11111111111111111111111111111111111111112';
/** Cadence used only while the Enhanced WSS push connection is down — same value as the original always-on poll, so a WS outage degrades to exactly the old behavior. */
const DEV_WALLET_POLL_INTERVAL_MS = 4_000;
/** Cadence used while the Enhanced WSS push connection is healthy — this is now purely a rare safety net for a missed/silently-dropped notification, not the primary detection path. */
const DEV_WALLET_BACKSTOP_POLL_INTERVAL_MS = 45_000;
const MC_MONITOR_INTERVAL_MS = 5_000;
const DEV_WALLET_TX_FETCH_LIMIT = 25;

export interface TokenTransferPosition {
  mint: string;
  devAddress: string;
  recipient: string;
  transferSignature: string;
  buySol: number;
  entryMc: number | null;
  /** Raw token amount the dev wallet originally sent out (the transfer-out that triggered the buy). */
  transferOutTokenAmount: number;
  /** USD worth of that outgoing transfer, captured lazily once a price is available (via the MC monitor). Null until then. */
  transferOutUsdValue: number | null;
}

export interface TokenTransferBuyTrigger {
  mint: string;
  devAddress: string;
  recipient: string;
  signature: string;
}

export interface TokenTransferSellSignal {
  mint: string;
  signature: string;
  /** Wallet that sent the token back to the dev wallet. */
  from: string;
  tokenAmount: number;
  /** USD worth of the incoming transfer, if a price was available at detection time. */
  incomingUsdValue: number | null;
  transferOutTokenAmount: number;
  transferOutUsdValue: number | null;
}

export interface TokenTransferOrchestrator {
  on(event: 'buyTrigger', listener: (trigger: TokenTransferBuyTrigger) => void): this;
  on(event: 'sellSignal', listener: (signal: TokenTransferSellSignal) => void): this;
  on(event: 'error', listener: (error: Error) => void): this;
  emit(event: 'buyTrigger', trigger: TokenTransferBuyTrigger): boolean;
  emit(event: 'sellSignal', signal: TokenTransferSellSignal): boolean;
  emit(event: 'error', error: Error): boolean;
}

export class TokenTransferOrchestrator extends EventEmitter {
  private readonly heliusClient: HeliusClient;
  private readonly marketCapClient: PumpReserveMarketCapClient;
  /** Enhanced WSS (transactionSubscribe) client — always keyed to the Developer-plan key (config.insiderHeliusApiKey), NOT key 4, since transactionSubscribe requires a Developer+ plan and key 4 isn't guaranteed to be on one. Null (and this mode falls back to pure REST polling) if that key isn't configured. */
  private readonly enhancedWs: HeliusEnhancedWsClient | null;
  private enhancedWatchId: number | null = null;

  private devAddress: string | null = null;
  private buySol: number;
  private isEnabled = false;
  private isShuttingDown = false;
  private isPolling = false;
  private pollTimer: NodeJS.Timeout | null = null;
  private mcMonitorTimer: NodeJS.Timeout | null = null;
  private cursorSignature: string | null = null;
  private readonly processedSignatures = new Set<string>();
  /** Mints the dev wallet has been seen buying (via SWAP) that we're now watching for a transfer-out. */
  private readonly candidateMints = new Set<string>();
  private activePosition: TokenTransferPosition | null = null;
  private latestMarketCapUsd: number | null = null;
  private latestMarketCapSource: string | null = null;

  constructor(
    private readonly config: ServiceConfig,
    private readonly telegramBot: TelegramBot | null = null,
    enhancedWs: HeliusEnhancedWsClient | null = null,
  ) {
    super();
    this.buySol = config.insiderBuySol;

    const heliusKey = config.insiderHeliusApiKey4 || config.heliusApiKey;
    if (!config.insiderHeliusApiKey4) {
      log.warn(
        'INSIDER_HELIUS_API_KEY_4 is not configured; Token Transfer mode is falling back to HELIUS_API_KEY.',
      );
    }
    this.heliusClient = new HeliusClient(heliusKey, {
      projectId: config.insiderHeliusProjectId4,
      label: 'Token Transfer Helius 4',
    });
    this.marketCapClient = new PumpReserveMarketCapClient(
      config.insiderSolanaRpcUrl4 || config.solanaRpcUrl,
      config.insiderSolanaWsUrl4 || config.solanaWsUrl,
      heliusKey,
    );

    const enhancedWsApiKey = config.insiderHeliusApiKey || config.heliusApiKey;
    this.enhancedWs =
      enhancedWs ??
      (enhancedWsApiKey
        ? new HeliusEnhancedWsClient(enhancedWsApiKey, 'Token Transfer Enhanced WS')
        : null);
    if (!this.enhancedWs) {
      log.warn(
        'No Developer-plan Helius key configured (INSIDER_HELIUS_API_KEY); Token Transfer mode is falling back to REST polling of the dev wallet every ' +
          `${DEV_WALLET_POLL_INTERVAL_MS}ms.`,
      );
    }
  }

  setDevAddress(address: string): string {
    const normalized = new PublicKey(address).toBase58();
    this.devAddress = normalized;
    this.cursorSignature = null;
    this.processedSignatures.clear();
    this.candidateMints.clear();
    log.info('Token Transfer dev address set', { devAddress: normalized });
    return normalized;
  }

  getDevAddress(): string | null {
    return this.devAddress;
  }

  /** Mints currently identified as "newly bought by the dev wallet" and being watched for a transfer-out. */
  getWatchedCandidateMints(): string[] {
    return [...this.candidateMints];
  }

  setBuySol(value: number): void {
    if (!Number.isFinite(value) || value <= 0) {
      throw new Error('Token Transfer buy SOL must be greater than 0');
    }
    this.buySol = value;
    log.info('Token Transfer buy SOL updated', { buySol: value });
  }

  getBuySol(): number {
    return this.buySol;
  }

  isRunning(): boolean {
    return this.isEnabled;
  }

  getActivePosition(): TokenTransferPosition | null {
    return this.activePosition;
  }

  getLatestMarketCap(): { marketCap: number; source: string } | null {
    if (this.latestMarketCapUsd === null || this.latestMarketCapSource === null) {
      return null;
    }
    return { marketCap: this.latestMarketCapUsd, source: this.latestMarketCapSource };
  }

  /** On-demand MC fetch via the dedicated Helius-4-backed market cap client (used by the Refresh button). */
  async fetchMarketCapUsd(mint: string): Promise<number | null> {
    try {
      const result = await this.marketCapClient.fetchMarketCapUsd(mint);
      if (result.ok) {
        this.latestMarketCapUsd = result.marketCap;
        this.latestMarketCapSource = result.source;
        return result.marketCap;
      }
      return this.activePosition?.mint === mint ? this.latestMarketCapUsd : null;
    } catch (err) {
      log.warn('On-demand market cap fetch failed', {
        mint,
        error: err instanceof Error ? err.message : String(err),
      });
      return this.activePosition?.mint === mint ? this.latestMarketCapUsd : null;
    }
  }

  async start(): Promise<void> {
    if (!this.devAddress) {
      throw new Error('Set a dev address before starting Token Transfer mode.');
    }
    if (this.activePosition) {
      throw new Error(
        'Token Transfer mode is already holding a position; sell it before watching a new dev wallet.',
      );
    }
    if (this.isEnabled) return;

    this.isEnabled = true;
    if (!this.cursorSignature) {
      try {
        const recent = await this.heliusClient.getWalletTransactionsDesc(this.devAddress, 1);
        this.cursorSignature = recent[0]?.signature ?? null;
      } catch (err) {
        log.warn(
          'Failed to establish a baseline signature for the dev wallet; will process from the beginning of its history',
          {
            devAddress: this.devAddress,
            error: err instanceof Error ? err.message : String(err),
          },
        );
      }
    }

    log.info('Token Transfer mode started; watching dev wallet for new-token swap buys and transfer-outs', {
      devAddress: this.devAddress,
      buySol: this.buySol,
      baselineSignature: this.cursorSignature,
      alreadyWatchedCandidates: [...this.candidateMints],
      pushDriven: Boolean(this.enhancedWs),
    });
    if (this.enhancedWs) {
      this.subscribeEnhancedWs(this.devAddress);
      // Rare safety net only — the Enhanced WSS push above is the primary
      // detection path now. If the push connection drops, isConnected will
      // report false and the tick below falls back to the tight interval.
      this.schedulePoll(DEV_WALLET_BACKSTOP_POLL_INTERVAL_MS);
    } else {
      this.schedulePoll(0);
    }
  }

  stop(reason = 'Stopped from Telegram'): void {
    const wasRunning = this.isEnabled;
    this.isEnabled = false;
    if (this.pollTimer) {
      clearTimeout(this.pollTimer);
      this.pollTimer = null;
    }
    void this.unsubscribeEnhancedWs();
    if (wasRunning) {
      log.info('Token Transfer dev-wallet watch stopped', {
        reason,
        devAddress: this.devAddress,
      });
    }
  }

  /** Subscribes to the dev wallet via Helius Enhanced WSS `transactionSubscribe` — the primary, near-free detection path. Each notification already carries the fully parsed transaction, so no follow-up REST fetch is needed. */
  private subscribeEnhancedWs(devAddress: string): void {
    if (!this.enhancedWs || this.enhancedWatchId !== null) return;
    this.enhancedWatchId = this.enhancedWs.watch(devAddress, (tx) => {
      void this.processDevWalletTx(tx, devAddress).catch((err) => {
        log.warn('Failed to process Enhanced WSS dev-wallet notification', {
          devAddress,
          signature: tx.signature,
          error: err instanceof Error ? err.message : String(err),
        });
      });
    });
    log.info('Subscribed to dev wallet via Helius Enhanced WSS (transactionSubscribe)', {
      devAddress,
    });
  }

  private async unsubscribeEnhancedWs(): Promise<void> {
    if (this.enhancedWatchId === null || !this.enhancedWs) return;
    const id = this.enhancedWatchId;
    this.enhancedWatchId = null;
    await this.enhancedWs.unwatch(id).catch(() => undefined);
  }

  /**
   * Clears the held position (after a manual or auto-detected sell) and
   * makes sure the dev-wallet watch stays stopped — the mode is fully idle
   * until the user presses Start again.
   */
  clearActivePosition(reason = 'Position closed'): void {
    const mint = this.activePosition?.mint ?? null;
    this.activePosition = null;
    this.stopMarketCapMonitoring();
    this.stop(reason);
    log.info('Token Transfer active position cleared; mode is idle until Start is pressed again', {
      mint,
      reason,
    });
  }

  async shutdown(): Promise<void> {
    this.isShuttingDown = true;
    this.stop('Service shutting down');
    this.stopMarketCapMonitoring();
  }

  markPositionBought(entryMc: number | null): void {
    if (!this.activePosition) return;
    this.activePosition = { ...this.activePosition, entryMc };
    this.latestMarketCapUsd = entryMc;
    this.startMarketCapMonitoring(this.activePosition.mint);
  }

  private schedulePoll(delayMs: number): void {
    if (!this.isEnabled || this.isShuttingDown) return;
    if (this.pollTimer) clearTimeout(this.pollTimer);
    this.pollTimer = setTimeout(() => {
      void this.pollDevWallet();
    }, delayMs);
  }

  /**
   * REST-based backstop poll. While the Enhanced WSS push connection is
   * healthy this only runs every DEV_WALLET_BACKSTOP_POLL_INTERVAL_MS (a
   * rare safety net for a missed notification); if the push connection is
   * down (or was never configured), it runs at the original tight interval
   * and becomes the primary detection path again, exactly like before this
   * migration.
   */
  private async pollDevWallet(): Promise<void> {
    if (!this.isEnabled || this.isShuttingDown || !this.devAddress) return;
    if (this.isPolling) {
      this.schedulePoll(DEV_WALLET_POLL_INTERVAL_MS);
      return;
    }
    this.isPolling = true;
    const watchedAddress = this.devAddress;
    try {
      const txs = await this.heliusClient.getAddressTransactionsAsc(
        watchedAddress,
        this.cursorSignature ?? undefined,
        DEV_WALLET_TX_FETCH_LIMIT,
      );
      for (const tx of txs) {
        if (!this.isEnabled || this.devAddress !== watchedAddress) break;
        await this.processDevWalletTx(tx, watchedAddress);
      }
    } catch (err) {
      void this.heliusClient.handlePossibleRateLimitError(err);
      log.warn('Failed to poll dev wallet transactions', {
        devAddress: watchedAddress,
        error: err instanceof Error ? err.message : String(err),
      });
    } finally {
      this.isPolling = false;
    }
    const wsHealthy = this.enhancedWs?.isConnected ?? false;
    this.schedulePoll(wsHealthy ? DEV_WALLET_BACKSTOP_POLL_INTERVAL_MS : DEV_WALLET_POLL_INTERVAL_MS);
  }

  /**
   * Shared per-transaction processing, fed either by a fresh Enhanced WSS
   * `transactionSubscribe` notification (already fully parsed, no REST call)
   * or by the REST backstop poll above. Identical logic either way — only
   * the data source differs.
   */
  private async processDevWalletTx(tx: HeliusTransaction, watchedAddress: string): Promise<void> {
    if (!this.isEnabled || this.devAddress !== watchedAddress) return;
    if (this.processedSignatures.has(tx.signature)) return;
    this.processedSignatures.add(tx.signature);
    this.cursorSignature = tx.signature;

    // While holding a position, the only thing we watch for is a
    // transfer-in of that same mint back to the dev wallet — the
    // new-token-candidate / transfer-out detection below is irrelevant
    // until the current position closes.
    if (this.activePosition) {
      const incoming = this.findMatchingTransferIn(tx, watchedAddress, this.activePosition.mint);
      if (incoming) {
        await this.evaluateSellSignal(tx, incoming);
      }
      return;
    }

    const newCandidateMint = this.findNewTokenSwapBuy(tx, watchedAddress);
    if (newCandidateMint && !this.candidateMints.has(newCandidateMint)) {
      this.candidateMints.add(newCandidateMint);
      log.info('Dev wallet swap-bought a new token; now watching it for a transfer-out', {
        devAddress: watchedAddress,
        mint: newCandidateMint,
        signature: tx.signature,
        totalWatchedCandidates: this.candidateMints.size,
      });
    }

    const found = this.findTokenTransferOut(tx, watchedAddress);
    if (found) {
      await this.handleTransferOutDetected(tx, found);
    }
  }

  /**
   * Matches a SWAP tx where the dev wallet is the *recipient* of a
   * non-SOL token — i.e. the dev buying into a token — for a mint that
   * isn't already an active candidate. Returns that mint so it can be
   * added to the watch set.
   */
  private findNewTokenSwapBuy(tx: HeliusTransaction, devAddress: string): string | null {
    if (tx.type !== 'SWAP') return null;
    const bought = tx.tokenTransfers?.find(
      (t) =>
        t.toUserAccount === devAddress &&
        t.fromUserAccount !== devAddress &&
        t.mint !== SOL_MINT &&
        !this.candidateMints.has(t.mint),
    );
    return bought?.mint ?? null;
  }

  /**
   * Matches a plain wallet-to-wallet SPL token transfer (not a SWAP) where
   * the dev wallet is the sender, some other wallet is the recipient, and
   * the mint is one we've already flagged as a recent dev swap-buy.
   */
  private findTokenTransferOut(
    tx: HeliusTransaction,
    devAddress: string,
  ): { mint: string; recipient: string; tokenAmount: number } | null {
    if (tx.type && tx.type !== 'TRANSFER') return null;
    const transfer = tx.tokenTransfers?.find(
      (t) =>
        t.fromUserAccount === devAddress &&
        !!t.toUserAccount &&
        t.toUserAccount !== devAddress &&
        this.candidateMints.has(t.mint),
    );
    if (!transfer) return null;
    return { mint: transfer.mint, recipient: transfer.toUserAccount, tokenAmount: transfer.tokenAmount };
  }

  /**
   * Matches a plain wallet-to-wallet SPL token transfer (not a SWAP) where
   * the dev wallet is the *recipient* of the held mint — i.e. someone
   * sending that same token back to the dev wallet while we're holding a
   * position bought off the back of its transfer-out.
   */
  private findMatchingTransferIn(
    tx: HeliusTransaction,
    devAddress: string,
    mint: string,
  ): { from: string; tokenAmount: number } | null {
    if (tx.type && tx.type !== 'TRANSFER') return null;
    const transfer = tx.tokenTransfers?.find(
      (t) =>
        t.mint === mint &&
        t.toUserAccount === devAddress &&
        !!t.fromUserAccount &&
        t.fromUserAccount !== devAddress,
    );
    if (!transfer) return null;
    return { from: transfer.fromUserAccount, tokenAmount: transfer.tokenAmount };
  }

  private async handleTransferOutDetected(
    tx: HeliusTransaction,
    found: { mint: string; recipient: string; tokenAmount: number },
  ): Promise<void> {
    const devAddress = this.devAddress!;
    this.candidateMints.clear();
    // Deliberately NOT calling this.stop() here — the dev-wallet watch keeps
    // running so it can catch a same-or-greater-value transfer-in of this
    // mint back to the dev wallet, which is treated as a sell signal (see
    // findMatchingTransferIn / evaluateSellSignal below).

    this.activePosition = {
      mint: found.mint,
      devAddress,
      recipient: found.recipient,
      transferSignature: tx.signature,
      buySol: this.buySol,
      entryMc: null,
      transferOutTokenAmount: found.tokenAmount,
      transferOutUsdValue: null,
    };

    log.warn('Dev wallet token transfer-out detected; triggering buy', {
      devAddress,
      mint: found.mint,
      recipient: found.recipient,
      tokenAmount: found.tokenAmount,
      signature: tx.signature,
    });

    await this.sendTransferDetectedNotification(found.mint, devAddress, found.recipient, tx.signature);

    this.emit('buyTrigger', {
      mint: found.mint,
      devAddress,
      recipient: found.recipient,
      signature: tx.signature,
    });
  }

  /**
   * Evaluated for every transfer-in of the held mint back to the dev wallet
   * while a position is open. Prices both the original transfer-out and this
   * incoming transfer; if the incoming USD value is the same as or greater
   * than the outgoing one, emits a sell signal. Falls back to a raw
   * token-amount comparison if a price isn't available yet (e.g. right after
   * a very fresh buy, before the MC monitor's first tick has priced it).
   */
  private async evaluateSellSignal(
    tx: HeliusTransaction,
    incoming: { from: string; tokenAmount: number },
  ): Promise<void> {
    const position = this.activePosition;
    if (!position) return;

    let incomingUsdValue: number | null = null;
    try {
      const priceResult = await this.marketCapClient.fetchMarketCapUsd(position.mint);
      if (priceResult.ok) {
        incomingUsdValue = incoming.tokenAmount * priceResult.priceUsd;
        if (position.transferOutUsdValue === null) {
          position.transferOutUsdValue = position.transferOutTokenAmount * priceResult.priceUsd;
        }
      }
    } catch (err) {
      log.warn('Failed to price a dev-wallet transfer-in while evaluating a sell signal', {
        mint: position.mint,
        signature: tx.signature,
        error: err instanceof Error ? err.message : String(err),
      });
    }

    const meetsThreshold =
      position.transferOutUsdValue !== null && incomingUsdValue !== null
        ? incomingUsdValue >= position.transferOutUsdValue
        : incoming.tokenAmount >= position.transferOutTokenAmount;

    if (!meetsThreshold) {
      log.info('Dev wallet received a transfer-in of the held token, but it did not match/exceed the original transfer-out', {
        mint: position.mint,
        signature: tx.signature,
        from: incoming.from,
        tokenAmount: incoming.tokenAmount,
        incomingUsdValue,
        transferOutTokenAmount: position.transferOutTokenAmount,
        transferOutUsdValue: position.transferOutUsdValue,
      });
      return;
    }

    log.warn('Dev wallet received a transfer-in worth the same or more than the original transfer-out; sell signal', {
      mint: position.mint,
      signature: tx.signature,
      from: incoming.from,
      tokenAmount: incoming.tokenAmount,
      incomingUsdValue,
      transferOutTokenAmount: position.transferOutTokenAmount,
      transferOutUsdValue: position.transferOutUsdValue,
    });

    this.emit('sellSignal', {
      mint: position.mint,
      signature: tx.signature,
      from: incoming.from,
      tokenAmount: incoming.tokenAmount,
      incomingUsdValue,
      transferOutTokenAmount: position.transferOutTokenAmount,
      transferOutUsdValue: position.transferOutUsdValue,
    });
  }

  private startMarketCapMonitoring(mint: string): void {
    this.stopMarketCapMonitoring();
    const tick = async (): Promise<void> => {
      if (!this.activePosition || this.activePosition.mint !== mint) return;
      try {
        const result = await this.marketCapClient.fetchMarketCapUsd(mint);
        if (result.ok) {
          this.latestMarketCapUsd = result.marketCap;
          this.latestMarketCapSource = result.source;
          log.info('[TOKEN TRANSFER MC CHECK]', {
            mint,
            marketCap: result.marketCap,
            source: result.source,
          });
          if (this.activePosition.transferOutUsdValue === null) {
            this.activePosition.transferOutUsdValue =
              this.activePosition.transferOutTokenAmount * result.priceUsd;
            log.info('Captured USD value of the original dev-wallet transfer-out; watching for a same-or-greater transfer-in as a sell signal', {
              mint,
              transferOutTokenAmount: this.activePosition.transferOutTokenAmount,
              priceUsd: result.priceUsd,
              transferOutUsdValue: this.activePosition.transferOutUsdValue,
            });
          }
        }
      } catch (err) {
        log.warn('Token Transfer market cap monitoring tick failed', {
          mint,
          error: err instanceof Error ? err.message : String(err),
        });
      }
      if (this.activePosition?.mint === mint) {
        this.mcMonitorTimer = setTimeout(() => void tick(), MC_MONITOR_INTERVAL_MS);
      }
    };
    void tick();
  }

  private stopMarketCapMonitoring(): void {
    if (this.mcMonitorTimer) {
      clearTimeout(this.mcMonitorTimer);
      this.mcMonitorTimer = null;
    }
    this.latestMarketCapUsd = null;
    this.latestMarketCapSource = null;
  }

  private async sendTransferDetectedNotification(
    mint: string,
    devAddress: string,
    recipient: string,
    signature: string,
  ): Promise<void> {
    if (!this.telegramBot) return;
    await this.telegramBot
      .sendDefault(
        [
          '<b>Token Transfer Detected</b>',
          `Dev wallet: <code>${this.html(devAddress)}</code>`,
          `Sent to: <code>${this.html(recipient)}</code>`,
          `Token: <code>${this.html(mint)}</code>`,
          `Tx: https://solscan.io/tx/${this.html(signature)}`,
          '',
          `Buying with <b>${this.buySol} SOL</b>...`,
        ].join('\n'),
      )
      .catch((err) => log.warn('Failed to send Token Transfer detected notification', err));
  }

  private html(value: string): string {
    return value.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  }
}
