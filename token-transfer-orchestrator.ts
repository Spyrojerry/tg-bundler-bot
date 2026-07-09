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
//  Once a transfer-out fires, the bot stops watching the dev wallet and
//  switches to market-cap monitoring only. There is no automatic sell — the
//  position is closed manually via the Telegram "Sell Position" button, or
//  automatically cleared (and the mode left stopped) if the trading wallet's
//  balance for that token is independently observed to reach zero. Either
//  way, the dev-wallet watch stays stopped until the user presses Start
//  again.
//
//  Dedicated to Helius API key 4 (config.insiderHeliusApiKey4) for both the
//  dev-wallet transaction polling and the post-buy market-cap monitoring, so
//  this mode never competes with the Insider bots' shared Helius key pool.
// ─────────────────────────────────────────────────────────────────────────────

import { PublicKey } from '@solana/web3.js';
import { EventEmitter } from 'events';
import { createLogger } from './logger';
import { HeliusClient, HeliusTransaction } from './helius-client';
import { PumpReserveMarketCapClient } from './pump-reserve-market-cap';
import type { ServiceConfig } from './types';
import { TelegramBot } from './telegram-bot';

const log = createLogger('TOKEN TRANSFER');

const SOL_MINT = 'So11111111111111111111111111111111111111112';
const DEV_WALLET_POLL_INTERVAL_MS = 4_000;
const MC_MONITOR_INTERVAL_MS = 5_000;
const DEV_WALLET_TX_FETCH_LIMIT = 25;

export interface TokenTransferPosition {
  mint: string;
  devAddress: string;
  recipient: string;
  transferSignature: string;
  buySol: number;
  entryMc: number | null;
}

export interface TokenTransferBuyTrigger {
  mint: string;
  devAddress: string;
  recipient: string;
  signature: string;
}

export interface TokenTransferOrchestrator {
  on(event: 'buyTrigger', listener: (trigger: TokenTransferBuyTrigger) => void): this;
  on(event: 'error', listener: (error: Error) => void): this;
  emit(event: 'buyTrigger', trigger: TokenTransferBuyTrigger): boolean;
  emit(event: 'error', error: Error): boolean;
}

export class TokenTransferOrchestrator extends EventEmitter {
  private readonly heliusClient: HeliusClient;
  private readonly marketCapClient: PumpReserveMarketCapClient;

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
    });
    this.schedulePoll(0);
  }

  stop(reason = 'Stopped from Telegram'): void {
    const wasRunning = this.isEnabled;
    this.isEnabled = false;
    if (this.pollTimer) {
      clearTimeout(this.pollTimer);
      this.pollTimer = null;
    }
    if (wasRunning) {
      log.info('Token Transfer dev-wallet watch stopped', {
        reason,
        devAddress: this.devAddress,
      });
    }
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
        if (this.processedSignatures.has(tx.signature)) continue;
        this.processedSignatures.add(tx.signature);
        this.cursorSignature = tx.signature;

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
          return;
        }
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
    this.schedulePoll(DEV_WALLET_POLL_INTERVAL_MS);
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
  ): { mint: string; recipient: string } | null {
    if (tx.type && tx.type !== 'TRANSFER') return null;
    const transfer = tx.tokenTransfers?.find(
      (t) =>
        t.fromUserAccount === devAddress &&
        !!t.toUserAccount &&
        t.toUserAccount !== devAddress &&
        this.candidateMints.has(t.mint),
    );
    if (!transfer) return null;
    return { mint: transfer.mint, recipient: transfer.toUserAccount };
  }

  private async handleTransferOutDetected(
    tx: HeliusTransaction,
    found: { mint: string; recipient: string },
  ): Promise<void> {
    const devAddress = this.devAddress!;
    this.stop('Token transfer-out detected; switching to market-cap monitoring');
    this.candidateMints.clear();

    this.activePosition = {
      mint: found.mint,
      devAddress,
      recipient: found.recipient,
      transferSignature: tx.signature,
      buySol: this.buySol,
      entryMc: null,
    };

    log.warn('Dev wallet token transfer-out detected; triggering buy', {
      devAddress,
      mint: found.mint,
      recipient: found.recipient,
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
