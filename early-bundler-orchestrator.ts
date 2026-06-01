// ─────────────────────────────────────────────────────────────────────────────
//  early-bundler-orchestrator.ts  —  Orchestrates the early bundler bot flow
//
//  Flow:
//    1. Trading wallet buys token → detect via WalletMonitor
//    2. Fetch first 5 transactions from Helius (mint + 4 bundlers)
//    3. Store position and 4 bundler wallets in database
//    4. Start BundlerMonitor for each wallet
//    5. On bundler BUY → sell 100% immediately
//    6. On bundler SELL → track cumulative, sell 100% at 40% threshold
//    7. Send Telegram notifications for all events
//    8. When trading wallet exits → cleanup and stop monitoring
// ─────────────────────────────────────────────────────────────────────────────

import { Connection } from '@solana/web3.js';
import { EventEmitter } from 'events';
import { createLogger } from './logger';
import { HeliusClient, EarlyBundlerInfo } from './helius-client';
import { BundlerMonitor, BundlerWallet, BundlerTransaction } from './bundler-monitor';
import { MonitorDatabase } from './database';
import type { ServiceConfig, NewTokenEvent, TokenExitEvent } from './types';
import { TelegramBot } from './telegram-bot';

const log = createLogger('EARLY-BUNDLER');

export interface EarlyBundlerPosition {
  positionId: number;
  tradingWallet: string;
  mint: string;
  tokenAmount: number;
  buySol: number | null;
  creatorVaultAddress?: string;
  bundlerWallets: BundlerWallet[];
}

export interface BundlerSellReason {
  type: 'bundler_buy' | 'bundler_sell_40pct' | 'creator_vault_f1';
  walletAddress?: string;
  soldPercentage?: number;
  reason?: string;
}

export declare interface EarlyBundlerOrchestrator {
  on(event: 'sellTrigger', listener: (trigger: BundlerSellReason & { position: EarlyBundlerPosition }) => void): this;
  on(event: 'bundlerDetected', listener: (data: { position: EarlyBundlerPosition; bundlers: EarlyBundlerInfo[] }) => void): this;
  on(event: 'error', listener: (error: Error) => void): this;
  emit(event: 'sellTrigger', trigger: BundlerSellReason & { position: EarlyBundlerPosition }): boolean;
  emit(event: 'bundlerDetected', data: { position: EarlyBundlerPosition; bundlers: EarlyBundlerInfo[] }): boolean;
  emit(event: 'error', error: Error): boolean;
}

export class EarlyBundlerOrchestrator extends EventEmitter {
  private readonly config: ServiceConfig;
  private readonly db: MonitorDatabase;
  private readonly telegramBot: TelegramBot | null;
  private readonly heliusClient: HeliusClient;
  private readonly connection: Connection;
  private bundlerMonitor: BundlerMonitor | null = null;
  private activePosition: EarlyBundlerPosition | null = null;
  private isEnabled = true;
  private isShuttingDown = false;

  constructor(config: ServiceConfig, db: MonitorDatabase, telegramBot: TelegramBot | null = null) {
    super();
    this.config = config;
    this.db = db;
    this.telegramBot = telegramBot;
    
    // Initialize Helius client with API key from config
    if (!config.heliusApiKey) {
      log.warn('HELIUS_API_KEY not configured - early bundler bot will not function');
    }
    this.heliusClient = new HeliusClient(config.heliusApiKey);
    
    this.connection = new Connection(config.solanaRpcUrl, {
      commitment: 'confirmed',
    });
  }

  /**
   * Handle a new token buy from the trading wallet
   * This triggers the early bundler detection flow
   */
  async handleTradingWalletBuy(event: NewTokenEvent): Promise<void> {
    if (this.isShuttingDown || !this.isEnabled) {
      log.warn('Early bundler orchestrator is inactive, ignoring trading wallet buy');
      return;
    }

    // Check if we already have an active position
    if (this.activePosition) {
      log.warn('Already have an active position, skipping new position', {
        existingMint: this.activePosition.mint,
        newMint: event.mint,
      });
      return;
    }

    log.info(`[EARLY BUNDLER] Trading wallet bought token - fetching early bundlers`, {
      tradingWallet: event.walletAddress,
      mint: event.mint,
      buySol: event.buySol,
    });

    try {
      // Step 1: Fetch early bundlers from Helius
      const earlyBundlers = await this.heliusClient.getEarlyBundlers(event.mint);

      if (this.isShuttingDown || !this.isEnabled) {
        log.info('Early bundler mode became inactive during fetch; skipping setup', {
          mint: event.mint,
        });
        return;
      }
      
      if (earlyBundlers.length === 0) {
        log.warn('No early bundlers found for token', { mint: event.mint });
        return;
      }

      log.info(`Found ${earlyBundlers.length} early transactions (including mint)`, {
        bundlers: earlyBundlers.map(b => ({
          wallet: b.walletAddress.slice(0, 8) + '...',
          isMint: b.isMint,
          tokenAmount: b.tokenAmount,
        })),
      });

      // Step 2: Create database position (skip the first one as it's the mint)
      const creatorVaultAddress = earlyBundlers.find(b => b.isMint)?.creatorVaultAddress;
      const bundlerWallets = earlyBundlers.filter(b => !b.isMint);
      
      if (bundlerWallets.length === 0) {
        log.warn('No bundler wallets found (only mint transaction)', { mint: event.mint });
        return;
      }

      const positionId = this.db.insertEarlyBundlerPosition(
        event.walletAddress,
        event.mint,
        0, // Token amount will be updated later
        event.buySol
      );

      // Step 3: Store bundler wallets in database
      const storedBundlerWallets: BundlerWallet[] = [];
      
      for (const bundler of bundlerWallets) {
        const bundlerWalletId = this.db.insertEarlyBundlerWallet(
          positionId,
          bundler.walletAddress,
          bundler.tokenAmount,
          bundler.signature,
          bundler.slot,
          bundler.timestamp
        );

        storedBundlerWallets.push({
          id: bundlerWalletId,
          walletAddress: bundler.walletAddress,
          initialTokenAmount: bundler.tokenAmount,
          totalSoldAmount: 0,
        });
      }

      // Step 4: Set up active position
      this.activePosition = {
        positionId,
        tradingWallet: event.walletAddress,
        mint: event.mint,
        tokenAmount: 0,
        buySol: event.buySol,
        creatorVaultAddress,
        bundlerWallets: storedBundlerWallets,
      };

      // Step 5: Start bundler monitor
      this.bundlerMonitor = new BundlerMonitor(this.config, this.heliusClient);
      this.setupBundlerMonitorListeners();
      
      await this.bundlerMonitor.startMonitoring(
        positionId,
        event.walletAddress,
        event.mint,
        storedBundlerWallets,
        creatorVaultAddress
      );

      // Step 6: Emit event
      this.emit('bundlerDetected', {
        position: this.activePosition,
        bundlers: earlyBundlers,
      });

      // Step 7: Send Telegram notification
      await this.sendBundlerDetectedNotification(this.activePosition, earlyBundlers);

      log.info(`[EARLY BUNDLER] Setup complete - monitoring ${storedBundlerWallets.length} bundler wallets`, {
        positionId,
        mint: event.mint,
        creatorVaultAddress,
      });

    } catch (err) {
      log.error('Failed to handle trading wallet buy for early bundler detection', err);
      this.emit('error', err instanceof Error ? err : new Error(String(err)));
    }
  }

  /**
   * Handle trading wallet exit - cleanup and stop monitoring
   */
  async handleTradingWalletExit(event: TokenExitEvent): Promise<void> {
    if (!this.activePosition || this.activePosition.mint !== event.mint) {
      log.warn('Trading wallet exit for unknown position', {
        mint: event.mint,
        activeMint: this.activePosition?.mint,
      });
      return;
    }

    log.info(`[EARLY BUNDLER] Trading wallet exited position - cleaning up`, {
      tradingWallet: event.walletAddress,
      mint: event.mint,
      positionId: this.activePosition.positionId,
    });

    // Stop bundler monitor
    if (this.bundlerMonitor) {
      await this.bundlerMonitor.stopMonitoring();
      this.bundlerMonitor = null;
    }

    // Close position in database
    this.db.closeEarlyBundlerPosition(
      this.activePosition.positionId,
      'Trading wallet exited position'
    );

    // Clear active position
    const closedPosition = this.activePosition;
    this.activePosition = null;

    // Send Telegram notification
    await this.sendPositionClosedNotification(closedPosition, 'Trading wallet exited position');

    log.info(`[EARLY BUNDLER] Cleanup complete`, {
      positionId: closedPosition.positionId,
      mint: closedPosition.mint,
    });
  }

  /**
   * Set up listeners for bundler monitor events
   */
  private setupBundlerMonitorListeners(): void {
    if (!this.bundlerMonitor) return;

    // Handle bundler buy → immediate sell
    this.bundlerMonitor.on('bundlerBuy', async (event) => {
      const label = event.source === 'receiver' ? 'Receiver wallet' : 'Bundler';
      log.info(`[EARLY BUNDLER] Bundler BUY detected - triggering immediate sell`, {
        walletAddress: event.walletAddress,
        source: event.source,
        parentWalletAddress: event.parentWalletAddress,
        mint: event.mint,
        tokenAmount: event.tokenAmount,
        signature: event.signature,
      });

      await this.triggerSell({
        type: 'bundler_buy',
        walletAddress: event.walletAddress,
      }, `${label} ${event.walletAddress.slice(0, 8)}... bought the token - selling immediately`);
    });

    // Handle bundler sell → check 40% threshold
    this.bundlerMonitor.on('bundlerSell', async (event) => {
      log.info(`[EARLY BUNDLER] Bundler SELL detected`, {
        walletAddress: event.walletAddress,
        source: event.source,
        parentWalletAddress: event.parentWalletAddress,
        mint: event.mint,
        tokenAmount: event.tokenAmount,
        cumulativeSoldPercentage: event.cumulativeSoldPercentage,
        signature: event.signature,
      });

      // Update database with sell
      if (this.activePosition) {
        const bundlerWallet = this.activePosition.bundlerWallets.find(
          b => b.walletAddress === event.walletAddress
        );
        
        if (bundlerWallet) {
          this.db.recordBundlerWalletSell(
            bundlerWallet.id,
            event.signature,
            event.tokenAmount,
            event.slot,
            event.timestamp
          );
        }
      }

      // Send notification about bundler sell
      await this.sendBundlerSellNotification(event);
    });

    // Handle 40% threshold reached
    this.bundlerMonitor.on('thresholdReached', async (event) => {
      const label = event.source === 'receiver' ? 'Receiver wallet' : 'Bundler';
      log.info(`[EARLY BUNDLER] 40% sell threshold reached - triggering sell`, {
        walletAddress: event.walletAddress,
        source: event.source,
        parentWalletAddress: event.parentWalletAddress,
        mint: event.mint,
        soldPercentage: event.soldPercentage,
      });

      await this.triggerSell({
        type: 'bundler_sell_40pct',
        walletAddress: event.walletAddress,
        soldPercentage: event.soldPercentage,
      }, `${label} ${event.walletAddress.slice(0, 8)}... sold ${event.soldPercentage.toFixed(1)}% of holdings - selling immediately`);
    });

    this.bundlerMonitor.on('creatorVaultF1', async (event) => {
      log.info('[EARLY BUNDLER] Creator vault F1 program detected - triggering sell', {
        creatorVaultAddress: event.creatorVaultAddress,
        mint: event.mint,
        signature: event.signature,
        programId: event.programId,
      });

      await this.triggerSell({
        type: 'creator_vault_f1',
        walletAddress: event.creatorVaultAddress,
        reason: `Creator vault ${event.creatorVaultAddress.slice(0, 8)}... used F1 program ${event.programId}`,
      }, `Creator vault ${event.creatorVaultAddress.slice(0, 8)}... used F1 program - selling immediately`);
    });
  }

  /**
   * Trigger a sell for the active position
   */
  private async triggerSell(
    reason: BundlerSellReason,
    message: string
  ): Promise<void> {
    if (!this.isEnabled) {
      log.info('Ignoring early bundler sell trigger because orchestrator is inactive', { reason });
      return;
    }

    if (!this.activePosition) {
      log.warn('Cannot trigger sell - no active position');
      return;
    }

    // Stop bundler monitor
    if (this.bundlerMonitor) {
      await this.bundlerMonitor.stopMonitoring();
      this.bundlerMonitor = null;
    }

    // Close position in database
    this.db.closeEarlyBundlerPosition(
      this.activePosition.positionId,
      message
    );

    // Get position and clear active
    const position = this.activePosition;
    this.activePosition = null;

    // Emit sell trigger event
    this.emit('sellTrigger', {
      ...reason,
      position,
    });

    // Send Telegram notification
    await this.sendSellTriggerNotification(position, reason, message);

    log.info(`[EARLY BUNDLER] Sell triggered and position closed`, {
      positionId: position.positionId,
      mint: position.mint,
      reason,
    });
  }

  /**
   * Shutdown the orchestrator and stop any active monitoring
   */
  async shutdown(): Promise<void> {
    this.isShuttingDown = true;
    this.isEnabled = false;
    
    if (this.bundlerMonitor) {
      await this.bundlerMonitor.stopMonitoring();
      this.bundlerMonitor = null;
    }
    
    this.activePosition = null;
    log.info('Early Bundler Orchestrator shut down');
  }

  async stopActiveMonitoring(reason = 'Bundler mode stopped'): Promise<void> {
    this.isEnabled = false;

    if (this.bundlerMonitor) {
      await this.bundlerMonitor.stopMonitoring();
      this.bundlerMonitor = null;
    }

    if (this.activePosition) {
      this.db.closeEarlyBundlerPosition(this.activePosition.positionId, reason);
      log.info('[EARLY BUNDLER] Active position closed because bundler mode stopped', {
        positionId: this.activePosition.positionId,
        mint: this.activePosition.mint,
        reason,
      });
    }

    this.activePosition = null;
  }

  setEnabled(enabled: boolean): void {
    this.isEnabled = enabled;
  }

  // ── Telegram Notifications ────────────────────────────────────────────────

  private async sendBundlerDetectedNotification(
    position: EarlyBundlerPosition,
    bundlers: EarlyBundlerInfo[]
  ): Promise<void> {
    if (!this.telegramBot) return;

    const html = (value: string): string =>
      value.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');

    const bundlerLines = bundlers
      .filter(b => !b.isMint)
      .map((b, i) => `${i + 1}. <code>${html(b.walletAddress)}</code> (${b.tokenAmount.toLocaleString()} tokens)`);

    await this.telegramBot.sendDefault([
      '<b>🚨 Early Bundler Detected</b>',
      `Token: <code>${html(position.mint)}</code>`,
      `Trading Wallet: <code>${html(position.tradingWallet)}</code>`,
      position.creatorVaultAddress ? `Creator Vault: <code>${html(position.creatorVaultAddress)}</code>` : '',
      '',
      '<b>Detected Bundler Wallets:</b>',
      ...bundlerLines,
      '',
      'I am now monitoring these wallets. If any of them buy more or sell 40% of their holdings, I will trigger an immediate sell.',
    ].filter(Boolean).join('\n')).catch(err => log.warn('Failed to send bundler detected notification', err));
  }

  private async sendBundlerSellNotification(
    event: BundlerTransaction & { cumulativeSoldPercentage: number }
  ): Promise<void> {
    if (!this.telegramBot) return;

    const html = (value: string): string =>
      value.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');

    await this.telegramBot.sendDefault([
      '<b>⚠️ Bundler Sell Activity</b>',
      `Token: <code>${html(event.mint)}</code>`,
      `Bundler: <code>${html(event.walletAddress)}</code>`,
      `Sold: <b>${event.tokenAmount.toLocaleString()}</b> tokens`,
      `Cumulative Sold: <b>${event.cumulativeSoldPercentage.toFixed(2)}%</b>`,
      '',
      event.cumulativeSoldPercentage >= 40 
        ? '<b>Threshold (40%) reached! Triggering sell...</b>'
        : 'Monitoring continues until 40% threshold is reached.',
    ].join('\n')).catch(err => log.warn('Failed to send bundler sell notification', err));
  }

  private async sendSellTriggerNotification(
    position: EarlyBundlerPosition,
    reason: BundlerSellReason,
    message: string
  ): Promise<void> {
    if (!this.telegramBot) return;

    const html = (value: string): string =>
      value.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');

    await this.telegramBot.sendDefault([
      '<b>📉 Early Bundler Sell Triggered</b>',
      `Token: <code>${html(position.mint)}</code>`,
      `Reason: <b>${html(reason.type)}</b>`,
      `Detail: ${html(message)}`,
      '',
      'Executing sell on GMGN...',
    ].join('\n')).catch(err => log.warn('Failed to send sell trigger notification', err));
  }

  private async sendPositionClosedNotification(
    position: EarlyBundlerPosition,
    reason: string
  ): Promise<void> {
    if (!this.telegramBot) return;

    const html = (value: string): string =>
      value.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');

    await this.telegramBot.sendDefault([
      '<b>ℹ️ Early Bundler Monitoring Stopped</b>',
      `Token: <code>${html(position.mint)}</code>`,
      `Reason: ${html(reason)}`,
    ].join('\n')).catch(err => log.warn('Failed to send position closed notification', err));
  }

  // ── Getters ──────────────────────────────────────────────────────────────

  getActivePosition(): EarlyBundlerPosition | null {
    return this.activePosition;
  }

  isMonitoring(): boolean {
    return !this.isShuttingDown && this.bundlerMonitor !== null;
  }
}
