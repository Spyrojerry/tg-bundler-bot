import { Connection, PublicKey } from '@solana/web3.js';
import { EventEmitter } from 'events';
import { createLogger } from './logger';
import type { ServiceConfig } from './types';
import { TelegramBot } from './telegram-bot';
import { WalletMonitor } from './wallet-monitor';

const log = createLogger('INSIDER');
const SOL_MINT = 'So11111111111111111111111111111111111111112';

interface HeliusEnhancedTransaction {
  signature?: string;
  timestamp?: number;
  type?: string;
  description?: string;
  feePayer?: string;
  tokenTransfers?: Array<{
    fromUserAccount?: string;
    toUserAccount?: string;
    tokenAmount?: number;
    mint?: string;
  }>;
  nativeTransfers?: Array<{
    fromUserAccount?: string;
    toUserAccount?: string;
    amount?: number;
  }>;
  events?: {
    swap?: {
      nativeInput?: { account: string; amount: string };
      nativeOutput?: { account: string; amount: string };
      tokenInputs?: Array<{ userCashFlowAccount: string; amount: string; mint: string }>;
      tokenOutputs?: Array<{ userCashFlowAccount: string; amount: string; mint: string }>;
    };
  };
}

type HeliusPoolSwapDirection = 'buy' | 'sell';

interface HeliusPoolSwap {
  wallet: string;
  direction: HeliusPoolSwapDirection;
  mint: string;
  amount: number;
}

export interface InsiderBuyTrigger {
  followedWallet: string;
  mint: string;
  signature: string;
  buySol: number;
  entryMc?: number;
  tradersListStr?: string;
}

export interface InsiderSellTrigger {
  followedWallet: string;
  positionMint: string;
  signature: string;
  reason: string;
}

export interface InsiderBot {
  on(event: 'buyTrigger', listener: (trigger: InsiderBuyTrigger) => void): this;
  on(event: 'sellTrigger', listener: (trigger: InsiderSellTrigger) => void): this;
  on(event: 'error', listener: (error: Error) => void): this;
  emit(event: 'buyTrigger', trigger: InsiderBuyTrigger): boolean;
  emit(event: 'sellTrigger', trigger: InsiderSellTrigger): boolean;
  emit(event: 'error', error: Error): boolean;
  getActivePosition(): { followedWallet: string; mint: string } | null;
  getPreBuyMint(): string | null;
  markPositionBought(trigger: InsiderBuyTrigger): void;
  clearActivePosition(): void;
  clearPreBuyMint(): void;
  getEntryMc(): number;
  getExitMc(): number;
  setExitMc(value: number): void;
  getExitPercent(): number;
  setExitPercent(value: number): void;
  getFollowedWallet(): string | null;
  getBuySol(): number;
  isBuyDisabled(): boolean;
  setBuyDisabled(value: boolean): void;
  getMinTransferProfit(): number;
  setMinTransferProfit(value: number): void;
  getProfitType(): 'realized' | 'total' | 'both';
  setProfitType(value: 'realized' | 'total' | 'both'): void;
}

export class InsiderBot extends EventEmitter {
  private readonly config: ServiceConfig;
  private readonly connection: Connection;
  private readonly telegramBot: TelegramBot | null;
  private followedWallet: string | null = null;
  private buySol: number;
  private entryMc: number;
  private exitMc: number;
  private exitPercent: number = 50;
  private buyDisabled: boolean = false;
  private minTransferProfit: number = 70;
  private profitType: 'realized' | 'total' | 'both' = 'both';
  private followMonitor: WalletMonitor | null = null;
  private watchingMint: string | null = null;
  private activePosition: {
    followedWallet: string;
    mint: string;
  } | null = null;
  private boughtMints = new Set<string>();
  private isBuyExecuting: boolean = false;

  constructor(
    config: ServiceConfig,
    rpcUrl: string,
    wsUrl: string,
    telegramBot: TelegramBot | null = null
  ) {
    super();
    this.config = config;
    this.telegramBot = telegramBot;
    this.buySol = config.insiderBuySol;
    this.entryMc = config.insiderEntryMc;
    this.exitMc = config.insiderExitMc;
    this.connection = new Connection(rpcUrl, {
      commitment: 'processed',
      wsEndpoint: wsUrl,
    });
  }

  getActivePosition(): { followedWallet: string; mint: string } | null {
    return this.activePosition;
  }

  getPreBuyMint(): string | null {
    return this.watchingMint;
  }

  clearActivePosition(): void {
    void this.resetForNewToken();
  }

  clearPreBuyMint(): void {
    void this.resetForNewToken();
  }

  setBuySol(value: number): void {
    if (!Number.isFinite(value) || value <= 0) {
      throw new Error('Insider buy SOL must be greater than 0');
    }
    this.buySol = value;
  }

  getBuySol(): number {
    return this.buySol;
  }

  setEntryMc(value: number): void {
    if (!Number.isFinite(value) || value < 0) {
      throw new Error('Entry MC must be a non-negative number');
    }
    this.entryMc = value;
  }

  getEntryMc(): number {
    return this.entryMc;
  }

  setExitMc(value: number): void {
    if (!Number.isFinite(value) || value < 0) {
      throw new Error('Exit MC must be a non-negative number');
    }
    this.exitMc = value;
  }

  getExitMc(): number {
    return this.exitMc;
  }

  setExitPercent(value: number): void {
    if (!Number.isFinite(value) || value < 0) {
      throw new Error('Exit percent must be a non-negative number');
    }
    this.exitPercent = value;
  }

  getExitPercent(): number {
    return this.exitPercent;
  }

  isBuyDisabled(): boolean {
    return this.buyDisabled;
  }

  setBuyDisabled(value: boolean): void {
    this.buyDisabled = value;
  }

  getMinTransferProfit(): number {
    return this.minTransferProfit;
  }

  setMinTransferProfit(value: number): void {
    if (!Number.isFinite(value) || value < 0) {
      throw new Error('Min transfer profit must be a non-negative number');
    }
    this.minTransferProfit = value;
  }

  getProfitType(): 'realized' | 'total' | 'both' {
    return this.profitType;
  }

  setProfitType(value: 'realized' | 'total' | 'both'): void {
    this.profitType = value;
  }

  getFollowedWallet(): string | null {
    return this.followedWallet;
  }

  setBuyExecuting(executing: boolean): void {
    this.isBuyExecuting = executing;
  }

  isBuyInProgress(): boolean {
    return this.isBuyExecuting;
  }

  isRunning(): boolean {
    return this.followMonitor !== null;
  }

  async followWallet(address: string): Promise<void> {
    const normalized = new PublicKey(address).toBase58();
    if (this.followedWallet !== normalized) {
      this.boughtMints.clear();
    }
    await this.stop();
    this.followedWallet = normalized;

    this.followMonitor = new WalletMonitor(this.config, normalized, { enforceMinBuySol: false });
    this.followMonitor.on('newToken', (event) => {
      if (this.boughtMints.has(event.mint)) return;
      if (this.activePosition || this.watchingMint) {
        log.info('Already watching or holding a token; ignoring new buy from followed wallet', {
          newMint: event.mint,
        });
        return;
      }
      log.info('Followed wallet buy detected - pausing monitor and starting MC monitoring for entry', {
        followedWallet: this.followedWallet,
        mint: event.mint,
      });

      // Pause the monitor to focus entirely on the current token and avoid RPC overhead
      if (this.followMonitor) {
        this.followMonitor.stop();
        this.followMonitor = null;
      }

      this.boughtMints.add(event.mint);
      this.watchingMint = event.mint;
    });

    await this.followMonitor.start();

    log.info('Insider follow wallet monitoring started', {
      followedWallet: normalized,
      buySol: this.buySol,
      entryMc: this.entryMc,
      exitMc: this.exitMc,
    });
  }

  async stop(): Promise<void> {
    if (this.followMonitor) {
      this.followMonitor.stop();
      this.followMonitor = null;
    }
    this.activePosition = null;
    this.watchingMint = null;
  }

  markPositionBought(trigger: InsiderBuyTrigger): void {
    this.activePosition = {
      followedWallet: trigger.followedWallet,
      mint: trigger.mint,
    };
    this.watchingMint = null;
    this.boughtMints.add(trigger.mint);
  }

  private async resetForNewToken(): Promise<void> {
    this.activePosition = null;
    this.watchingMint = null;
    log.info('InsiderBot reset; resuming followed wallet monitoring');
    
    // Resume monitoring the followed wallet if one was set
    if (this.followedWallet && !this.followMonitor) {
      await this.followWallet(this.followedWallet);
    }
  }
}

const KNOWN_POOL_AUTHORITIES = new Set([
  'FhVo3mqL8PW5pH5U2CN4XE33DokiyZnUwuGpH2hmHLuM',
  '5Q544fKrZM6W6y77W4A2B4L2S97E6q5nN5T6v8D5H5',
  '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8',
  '9W959DqmcGTu2YHcR6Yn3S3XN6XN6S6S6S6S6S6S',
  'Eo7WjKq67rjJQSvbdBk6RToZp4EAtX4F2Xv7V95v2',
  '6EF8rrecthR5DkjtvAXth2Jy1Gq3BvF4YQDQe4N362K',
  'TSLpA7P3qPqbeXh3WfJLue2osyZH1G1A2A2A2A2A2A2',
]);
