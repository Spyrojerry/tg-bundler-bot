import { EventEmitter } from 'events';
import { createLogger } from './logger';
import { WalletMonitor } from './wallet-monitor';
import { MonitorDatabase } from './database';
import type { ServiceConfig, NewTokenEvent, TokenExitEvent } from './types';
import { TelegramBot } from './telegram-bot';

const log = createLogger('REVERSE-COPYSELL');

export interface ReverseCopySellPosition {
  tradingWallet: string;
  mint: string;
  buySol: number | null;
  detectedAt: number;
}

export interface ReverseCopySellOrchestrator {
  on(event: 'sellTrigger', listener: (data: { position: ReverseCopySellPosition; targetWallet: string }) => void): this;
  emit(event: 'sellTrigger', data: { position: ReverseCopySellPosition; targetWallet: string }): boolean;
}

export class ReverseCopySellOrchestrator extends EventEmitter {
  private activePosition: ReverseCopySellPosition | null = null;
  private targetWalletMonitor: WalletMonitor | null = null;
  private isEnabled = false;
  private isShuttingDown = false;

  constructor(
    private readonly config: ServiceConfig,
    private readonly db: MonitorDatabase,
    private readonly telegramBot: TelegramBot | null = null
  ) {
    super();
  }

  setEnabled(enabled: boolean): void {
    this.isEnabled = enabled;
    if (!enabled) {
      this.stopActiveMonitoring('Disabled');
    }
  }

  getActivePosition(): ReverseCopySellPosition | null {
    return this.activePosition;
  }

  /**
   * Handle a new token buy from the trading wallet
   */
  async handleTradingWalletBuy(event: NewTokenEvent): Promise<void> {
    if (this.isShuttingDown || !this.isEnabled) return;

    if (this.activePosition) {
      log.warn('Already have an active reverse-copysell position, skipping new position', {
        existingMint: this.activePosition.mint,
        newMint: event.mint,
      });
      return;
    }

    if (!this.config.reverseCopySellTargetWallet) {
      log.warn('No REVERSE_COPYSELL_TARGET_WALLET configured; cannot monitor target.');
      return;
    }

    log.info(`[REVERSE-COPYSELL] Trading wallet bought token - starting target watch`, {
      tradingWallet: event.walletAddress,
      mint: event.mint,
      targetWallet: this.config.reverseCopySellTargetWallet,
    });

    this.activePosition = {
      tradingWallet: event.walletAddress,
      mint: event.mint,
      buySol: event.buySol,
      detectedAt: Date.now(),
    };

    // Start monitoring the target wallet
    try {
      this.targetWalletMonitor = new WalletMonitor(this.config, this.config.reverseCopySellTargetWallet, { enforceMinBuySol: false });
      
      this.targetWalletMonitor.on('buyDetected', async (targetEvent: NewTokenEvent) => {
        if (!this.activePosition || targetEvent.mint !== this.activePosition.mint) return;

        log.info(`[REVERSE-COPYSELL] Target wallet bought the same token - triggering sell`, {
          targetWallet: targetEvent.walletAddress,
          mint: targetEvent.mint,
        });

        await this.triggerSell(`Target wallet ${targetEvent.walletAddress.slice(0, 8)}... bought the token`);
      });

      await this.targetWalletMonitor.start();
      
      await this.sendTargetWatchStartedNotification();
    } catch (err) {
      log.error('Failed to start target wallet monitor', err);
      this.activePosition = null;
    }
  }

  /**
   * Handle trading wallet exit - cleanup and stop monitoring
   */
  async handleTradingWalletExit(event: TokenExitEvent): Promise<void> {
    if (!this.activePosition || this.activePosition.mint !== event.mint) return;

    log.info(`[REVERSE-COPYSELL] Trading wallet exited position - cleaning up`, {
      mint: event.mint,
    });

    await this.stopActiveMonitoring('Trading wallet exited position');
  }

  private async triggerSell(reason: string): Promise<void> {
    if (!this.activePosition || !this.isEnabled) return;

    const position = this.activePosition;
    const targetWallet = this.config.reverseCopySellTargetWallet!;

    await this.stopActiveMonitoring(reason);

    this.emit('sellTrigger', { position, targetWallet });
    await this.sendSellTriggerNotification(position, targetWallet, reason);
  }

  async stopActiveMonitoring(reason: string): Promise<void> {
    if (this.targetWalletMonitor) {
      this.targetWalletMonitor.stop();
      this.targetWalletMonitor = null;
    }
    this.activePosition = null;
    log.info(`[REVERSE-COPYSELL] Monitoring stopped: ${reason}`);
  }

  async shutdown(): Promise<void> {
    this.isShuttingDown = true;
    await this.stopActiveMonitoring('Shutdown');
  }

  private async sendTargetWatchStartedNotification(): Promise<void> {
    if (!this.telegramBot || !this.activePosition) return;
    const html = (value: string): string => value.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
    
    await this.telegramBot.sendDefault([
      '<b>🔄 Reverse CopySell Active</b>',
      `Token: <code>${html(this.activePosition.mint)}</code>`,
      `Target Wallet: <code>${html(this.config.reverseCopySellTargetWallet!)}</code>`,
      '',
      'Watching target wallet. If it buys this token, I will sell your position immediately.',
    ].join('\n'), {
      replyMarkup: {
        inline_keyboard: [[{ text: '🔄 Refresh P/L & MC', callback_data: `r:m:${this.activePosition.mint}:r` }]],
      },
    }).catch(err => log.warn('Failed to send reverse-copysell notification', err));
  }

  private async sendSellTriggerNotification(position: ReverseCopySellPosition, targetWallet: string, reason: string): Promise<void> {
    if (!this.telegramBot) return;
    const html = (value: string): string => value.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');

    await this.telegramBot.sendDefault([
      '<b>🚨 Reverse CopySell Triggered</b>',
      `Token: <code>${html(position.mint)}</code>`,
      `Reason: <b>${reason}</b>`,
      '',
      'Selling 100% of your position now.',
    ].join('\n')).catch(err => log.warn('Failed to send reverse-copysell sell notification', err));
  }
}
