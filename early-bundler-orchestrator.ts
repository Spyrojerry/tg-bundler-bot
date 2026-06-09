import { PublicKey } from '@solana/web3.js';
import { EventEmitter } from 'events';
import { createLogger } from './logger';
import { HeliusClient, HeliusTransaction } from './helius-client';
import { MonitorDatabase } from './database';
import type { ServiceConfig, NewTokenEvent } from './types';
import { TelegramBot } from './telegram-bot';
import { GmgnClient } from './gmgn-client';
import { WalletMonitor } from './wallet-monitor';

const log = createLogger('BUNDLER');
const sleep = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));
const MAX_FOLLOW_BUY_AGE_MS = 10 * 60 * 1000;
const MAX_PATTERN_MONITOR_MS = 5 * 60 * 1000;

type TransferSide = 'buy' | 'sell' | 'unknown';

export interface EarlyBundlerPosition {
  positionId: number;
  tradingWallet: string;
  followedWallet: string;
  mint: string;
  buySol: number | null;
  entryMc?: number | null;
  matchedWallet?: string;
}

export interface BundlerBuyTrigger {
  position: EarlyBundlerPosition;
  signature: string;
  matchedWallet: string;
}

export interface BundlerSellReason {
  type: 'mcap_exit' | 'rug' | 'manual';
  reason?: string;
}

export interface EarlyBundlerOrchestrator {
  on(event: 'buyTrigger', listener: (trigger: BundlerBuyTrigger) => void): this;
  on(event: 'sellTrigger', listener: (trigger: BundlerSellReason & { position: EarlyBundlerPosition }) => void): this;
  on(event: 'error', listener: (error: Error) => void): this;
  emit(event: 'buyTrigger', trigger: BundlerBuyTrigger): boolean;
  emit(event: 'sellTrigger', trigger: BundlerSellReason & { position: EarlyBundlerPosition }): boolean;
  emit(event: 'error', error: Error): boolean;
}

export class EarlyBundlerOrchestrator extends EventEmitter {
  private readonly heliusClient: HeliusClient;
  private followMonitor: WalletMonitor | null = null;
  private followedWallet: string | null = null;
  private activePosition: EarlyBundlerPosition | null = null;
  private watchingMint: string | null = null;
  private boughtMints = new Set<string>();
  private isEnabled = false;
  private isShuttingDown = false;
  private buySol: number;
  private exitPercent: number;
  private nextPositionId = 1;

  constructor(
    private readonly config: ServiceConfig,
    private readonly db: MonitorDatabase,
    private readonly telegramBot: TelegramBot | null = null,
    private readonly gmgnClient: GmgnClient | null = null
  ) {
    super();
    this.heliusClient = new HeliusClient(config.heliusApiKey || config.insiderHeliusApiKey);
    this.buySol = config.insiderBuySol;
    this.exitPercent = config.insiderExitPercent;
    void this.db;

    if (!config.heliusApiKey && !config.insiderHeliusApiKey) {
      log.warn('No HELIUS_API_KEY configured; bundler transfer-pattern detection will not work.');
    }
  }

  async followWallet(address: string): Promise<void> {
    const normalized = new PublicKey(address).toBase58();
    if (this.followedWallet !== normalized) {
      this.boughtMints.clear();
    }

    await this.stopFollowMonitor();
    this.followedWallet = normalized;
    this.isEnabled = true;

    if (this.activePosition || this.watchingMint) {
      log.info('Bundler follow wallet set, but an existing token is still active', {
        followedWallet: normalized,
        activeMint: this.activePosition?.mint,
        watchingMint: this.watchingMint,
      });
      return;
    }

    await this.startFollowMonitor();
  }

  setEnabled(enabled: boolean): void {
    this.isEnabled = enabled;
    if (!enabled) {
      void this.stopActiveMonitoring('Disabled');
    } else if (this.followedWallet && !this.followMonitor && !this.activePosition && !this.watchingMint) {
      void this.startFollowMonitor();
    }
  }

  setBuySol(value: number): void {
    if (!Number.isFinite(value) || value <= 0) {
      throw new Error('Bundler buy SOL must be greater than 0');
    }
    this.buySol = value;
  }

  getBuySol(): number {
    return this.buySol;
  }

  setExitPercent(value: number): void {
    if (!Number.isFinite(value) || value < 0) {
      throw new Error('Bundler exit percent must be a non-negative number');
    }
    this.exitPercent = value;
  }

  getExitPercent(): number {
    return this.exitPercent;
  }

  getFollowedWallet(): string | null {
    return this.followedWallet;
  }

  getWatchingMint(): string | null {
    return this.watchingMint;
  }

  getActivePosition(): EarlyBundlerPosition | null {
    return this.activePosition;
  }

  isRunning(): boolean {
    return this.followMonitor !== null;
  }

  markPositionBought(position: EarlyBundlerPosition, entryMc: number | null): void {
    this.activePosition = { ...position, entryMc };
    this.watchingMint = null;
    this.boughtMints.add(position.mint);
  }

  async shutdown(): Promise<void> {
    this.isShuttingDown = true;
    this.isEnabled = false;
    await this.stopFollowMonitor();
    this.activePosition = null;
    this.watchingMint = null;
  }

  async stopActiveMonitoring(reason = 'Bundler mode stopped'): Promise<void> {
    this.isEnabled = false;
    await this.stopFollowMonitor();
    this.activePosition = null;
    this.watchingMint = null;
    log.info('Bundler monitoring stopped', { reason });
  }

  clearActivePosition(): void {
    this.activePosition = null;
    this.watchingMint = null;
    log.info('Bundler active position cleared; resuming follow wallet if configured');
    if (this.isEnabled && this.followedWallet && !this.followMonitor) {
      void this.startFollowMonitor();
    }
  }

  private async startFollowMonitor(): Promise<void> {
    if (!this.isEnabled || this.isShuttingDown || !this.followedWallet || this.followMonitor) return;

    this.followMonitor = new WalletMonitor(this.config, this.followedWallet, { enforceMinBuySol: false });
    this.followMonitor.on('newToken', (event) => {
      this.handleFollowWalletBuy(event).catch((err) => {
        log.error('Failed to handle bundler follow-wallet buy', err);
        this.emit('error', err instanceof Error ? err : new Error(String(err)));
      });
    });

    await this.followMonitor.start();
    log.info('Bundler follow-wallet monitoring started', {
      followedWallet: this.followedWallet,
      buySol: this.buySol,
      exitPercent: this.exitPercent,
    });
  }

  private async stopFollowMonitor(): Promise<void> {
    if (!this.followMonitor) return;
    this.followMonitor.stop();
    this.followMonitor = null;
  }

  private async handleFollowWalletBuy(event: NewTokenEvent): Promise<void> {
    if (!this.isEnabled || this.isShuttingDown) return;
    if (this.activePosition || this.watchingMint) return;
    if (this.boughtMints.has(event.mint)) return;

    this.watchingMint = event.mint;
    this.boughtMints.add(event.mint);
    await this.stopFollowMonitor();

    const ageOk = await this.isFollowBuyWithinCreationWindow(event);
    if (!ageOk) {
      this.watchingMint = null;
      if (this.isEnabled && !this.activePosition) {
        await this.startFollowMonitor();
      }
      return;
    }

    log.info('Bundler follow wallet bought new token; checking transfer pattern', {
      followedWallet: event.walletAddress,
      mint: event.mint,
    });

    await this.sendTokenDetectedNotification(event.mint, event.walletAddress);
    await this.monitorTokenTransfers(event.mint, event.walletAddress);
  }

  private async monitorTokenTransfers(mint: string, followedWallet: string): Promise<void> {
    const startedAt = Date.now();

    while (this.isEnabled && !this.isShuttingDown && this.watchingMint === mint) {
      const remainingMs = Math.max(1_000, MAX_PATTERN_MONITOR_MS - (Date.now() - startedAt));
      const transactions = await this.heliusClient.getTokenSystemTransfers(mint, 10, remainingMs);
      log.info('Bundler Helius transfer check complete', {
        mint,
        records: transactions.length,
        elapsedSec: Math.round((Date.now() - startedAt) / 1000),
        action: 'evaluating immediately',
      });

      const match = this.findRepeatedBuyer(transactions, mint);
      if (match) {
        const position: EarlyBundlerPosition = {
          positionId: this.nextPositionId++,
          tradingWallet: this.config.tradingWalletAddress ?? this.config.walletAddress ?? followedWallet,
          followedWallet,
          mint,
          buySol: this.buySol,
          matchedWallet: match.wallet,
        };

        this.activePosition = position;
        this.watchingMint = null;

        await this.sendPatternMatchedNotification(position, match);
        this.emit('buyTrigger', {
          position,
          signature: match.signature,
          matchedWallet: match.wallet,
        });
        return;
      }

      if (transactions.length >= 10) {
        log.info('Bundler pattern rejected after checking up to 10 transfer records', { mint });
        await this.sendPatternRejectedNotification(mint);
        this.watchingMint = null;
        if (this.isEnabled && !this.activePosition) {
          await this.startFollowMonitor();
        }
        return;
      }

      if (Date.now() - startedAt >= MAX_PATTERN_MONITOR_MS) {
        log.info('Bundler pattern rejected after 5 minute Helius monitoring timeout', {
          mint,
          records: transactions.length,
        });
        await this.sendPatternRejectedNotification(mint, 'No matching pattern found within 5 minutes.');
        this.watchingMint = null;
        if (this.isEnabled && !this.activePosition) {
          await this.startFollowMonitor();
        }
        return;
      }

      await sleep(2_000);
    }
  }

  private async isFollowBuyWithinCreationWindow(event: NewTokenEvent): Promise<boolean> {
    try {
      const creationTimestamp = await this.heliusClient.getTokenCreationTimestamp(event.mint);
      const buyTimestamp = event.timestamp ?? Math.floor(event.detectedAt / 1000);

      if (creationTimestamp === null) {
        log.warn('Bundler token rejected because creation timestamp could not be found', {
          mint: event.mint,
          followedWallet: event.walletAddress,
          signature: event.signature,
        });
        await this.sendPatternRejectedNotification(event.mint, 'Token creation timestamp could not be verified.');
        return false;
      }

      const ageMs = (buyTimestamp - creationTimestamp) * 1000;
      if (ageMs <= MAX_FOLLOW_BUY_AGE_MS) {
        log.info('Bundler follow buy passed token age check', {
          mint: event.mint,
          followedWallet: event.walletAddress,
          ageSec: Math.max(0, Math.round(ageMs / 1000)),
        });
        return true;
      }

      log.info('Bundler token rejected because follow buy was older than 10 minutes from creation', {
        mint: event.mint,
        followedWallet: event.walletAddress,
        ageSec: Math.round(ageMs / 1000),
      });
      await this.sendPatternRejectedNotification(event.mint, 'Follow-wallet first buy was more than 10 minutes after token creation.');
      return false;
    } catch (err) {
      log.warn('Bundler token rejected because token age check failed', {
        mint: event.mint,
        followedWallet: event.walletAddress,
        error: err instanceof Error ? err.message : String(err),
      });
      await this.sendPatternRejectedNotification(event.mint, 'Token age check failed.');
      return false;
    }
  }

  private findRepeatedBuyer(
    transactions: HeliusTransaction[],
    mint: string
  ): { wallet: string; signature: string; firstTwo: TransferSide[] } | null {
    const byFeePayer = new Map<string, Array<{ tx: HeliusTransaction; side: TransferSide }>>();

    for (const tx of transactions) {
      if (!tx.feePayer) continue;
      const side = this.classifyTransferSide(tx, mint);
      const entries = byFeePayer.get(tx.feePayer) ?? [];
      entries.push({ tx, side });
      byFeePayer.set(tx.feePayer, entries);
    }

    for (const [wallet, entries] of byFeePayer.entries()) {
      if (entries.length < 2) continue;
      const firstTwo = entries.slice(0, 2).map((entry) => entry.side);
      if (firstTwo[0] === 'buy' && firstTwo[1] === 'buy') {
        return {
          wallet,
          signature: entries[1].tx.signature,
          firstTwo,
        };
      }

      log.info('Repeated feePayer found, but first two actions were not both buys', {
        mint,
        wallet,
        firstTwo,
      });
    }

    return null;
  }

  private classifyTransferSide(tx: HeliusTransaction, mint: string): TransferSide {
    const transfer = tx.tokenTransfers?.find((item) => item.mint === mint);
    if (!transfer || !tx.feePayer) return 'unknown';
    if (transfer.toUserAccount === tx.feePayer) return 'buy';
    if (transfer.fromUserAccount === tx.feePayer) return 'sell';
    return 'unknown';
  }

  private async sendTokenDetectedNotification(mint: string, followedWallet: string): Promise<void> {
    if (!this.telegramBot) return;
    await this.telegramBot.sendDefault([
      '<b>Bundler Follow Buy Detected</b>',
      `Follow wallet: <code>${this.html(followedWallet)}</code>`,
      `Token: <code>${this.html(mint)}</code>`,
      '',
      'Checking the first 10 Helius system transfers for repeated buyer pattern.',
    ].join('\n')).catch((err) => log.warn('Failed to send bundler detected notification', err));
  }

  private async sendPatternMatchedNotification(
    position: EarlyBundlerPosition,
    match: { wallet: string; firstTwo: TransferSide[] }
  ): Promise<void> {
    if (!this.telegramBot) return;
    const marketCapUsd = this.gmgnClient ? await this.gmgnClient.fetchTokenMarketCapUsd(position.mint).catch(() => null) : null;
    await this.telegramBot.sendDefault([
      '<b>Bundler Buy Pattern Matched</b>',
      `Token: <code>${this.html(position.mint)}</code>`,
      `Repeated feePayer: <code>${this.html(match.wallet)}</code>`,
      `First two actions: <b>${match.firstTwo.join(' + ')}</b>`,
      `Market Cap: <b>$${marketCapUsd?.toLocaleString() ?? 'Unknown'}</b>`,
      `Buying: <b>${position.buySol} SOL</b>`,
      '',
      'Submitting swap...',
    ].join('\n')).catch((err) => log.warn('Failed to send bundler pattern notification', err));
  }

  private async sendPatternRejectedNotification(mint: string, reason?: string): Promise<void> {
    if (!this.telegramBot) return;
    await this.telegramBot.sendDefault([
      '<b>Bundler Pattern Rejected</b>',
      `Token: <code>${this.html(mint)}</code>`,
      reason ?? 'No repeated feePayer with two buys found in the first 10 Helius system transfers.',
    ].join('\n')).catch((err) => log.warn('Failed to send bundler reject notification', err));
  }

  private html(value: string): string {
    return value.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  }
}
