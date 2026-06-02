import { Connection, PublicKey } from '@solana/web3.js';
import { EventEmitter } from 'events';
import { createLogger } from './logger';
import type { ServiceConfig } from './types';
import { TelegramBot } from './telegram-bot';

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
  insiderWallet: string;
  mint: string;
  signature: string;
  buySol: number;
  entryWindowBuySignatures?: string[];
}

export interface InsiderSellTrigger {
  followedWallet: string;
  insiderWallet: string;
  positionMint: string;
  insiderBuyMint: string;
  signature: string;
}

export interface InsiderBot {
  on(event: 'buyTrigger', listener: (trigger: InsiderBuyTrigger) => void): this;
  on(event: 'sellTrigger', listener: (trigger: InsiderSellTrigger) => void): this;
  on(event: 'error', listener: (error: Error) => void): this;
  emit(event: 'buyTrigger', trigger: InsiderBuyTrigger): boolean;
  emit(event: 'sellTrigger', trigger: InsiderSellTrigger): boolean;
  emit(event: 'error', error: Error): boolean;
  getActivePosition(): { followedWallet: string; insiderWallet: string; mint: string } | null;
  getPreBuyMint(): string | null;
  markPositionBought(trigger: InsiderBuyTrigger): void;
  clearActivePosition(): void;
}

export class InsiderBot extends EventEmitter {
  private readonly config: ServiceConfig;
  private readonly connection: Connection;
  private readonly telegramBot: TelegramBot | null;
  private followedWallet: string | null = null;
  private buySol: number;
  private followSubId: number | null = null;
  private activePosition: {
    followedWallet: string;
    insiderWallet: string;
    mint: string;
  } | null = null;
  private processedSignatures = new Set<string>();

  constructor(config: ServiceConfig, telegramBot: TelegramBot | null = null) {
    super();
    this.config = config;
    this.telegramBot = telegramBot;
    this.buySol = config.insiderBuySol;
    this.connection = new Connection(config.insiderSolanaRpcUrl, {
      commitment: 'processed',
      wsEndpoint: config.insiderSolanaWsUrl,
    });
  }

  getActivePosition(): { followedWallet: string; insiderWallet: string; mint: string } | null {
    return this.activePosition;
  }

  getPreBuyMint(): string | null {
    return null; // Simplified mode has no pre-buy sequence
  }

  clearActivePosition(): void {
    this.activePosition = null;
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

  getFollowedWallet(): string | null {
    return this.followedWallet;
  }

  isRunning(): boolean {
    return this.followSubId !== null;
  }

  async followWallet(address: string): Promise<void> {
    const normalized = new PublicKey(address).toBase58();
    await this.stop();
    this.followedWallet = normalized;

    const subId = this.connection.onLogs(
      new PublicKey(normalized),
      (logInfo) => {
        if (logInfo.err) return;
        this.processFollowWalletSignature(logInfo.signature).catch((err) => {
          log.error(`Failed to process followed wallet signature ${logInfo.signature}`, err);
          this.emit('error', err instanceof Error ? err : new Error(String(err)));
        });
      },
      'processed'
    );

    this.followSubId = subId;
    log.info('Insider follow wallet monitoring started (Simplified Mode)', {
      followedWallet: normalized,
      buySol: this.buySol,
    });
  }

  async stop(): Promise<void> {
    if (this.followSubId !== null) {
      await this.connection.removeOnLogsListener(this.followSubId).catch(() => undefined);
      this.followSubId = null;
    }
    this.activePosition = null;
    this.processedSignatures.clear();
  }

  markPositionBought(trigger: InsiderBuyTrigger): void {
    this.activePosition = {
      followedWallet: trigger.followedWallet,
      insiderWallet: trigger.insiderWallet,
      mint: trigger.mint,
    };
    log.info('Insider position marked as bought', { mint: trigger.mint });
  }

  private async processFollowWalletSignature(signature: string): Promise<void> {
    if (!this.followedWallet || this.processedSignatures.has(signature)) return;
    this.processedSignatures.add(signature);

    const tx = await this.fetchHeliusTransaction(signature);
    if (!tx) return;

    const swaps = this.getHeliusPoolSwaps(tx);
    const walletSwaps = swaps.filter(s => s.wallet === this.followedWallet);

    for (const swap of walletSwaps) {
      if (!this.activePosition) {
        // Trigger BUY on Insider's first SELL
        if (swap.direction === 'sell') {
          log.warn('Insider SELL detected - triggering bot BUY', {
            followedWallet: this.followedWallet,
            mint: swap.mint,
            signature,
          });
          this.emit('buyTrigger', {
            followedWallet: this.followedWallet,
            insiderWallet: this.followedWallet,
            mint: swap.mint,
            signature,
            buySol: this.buySol,
          });
          break; // Only trigger one buy per tx
        }
      } else {
        // Trigger SELL on Insider's next BUY
        if (swap.direction === 'buy') {
          log.warn('Insider BUY detected while in position - triggering bot SELL', {
            followedWallet: this.followedWallet,
            positionMint: this.activePosition.mint,
            insiderBuyMint: swap.mint,
            signature,
          });
          this.emit('sellTrigger', {
            followedWallet: this.followedWallet,
            insiderWallet: this.followedWallet,
            positionMint: this.activePosition.mint,
            insiderBuyMint: swap.mint,
            signature,
          });
          break; // Only trigger one sell per tx
        }
      }
    }
  }

  private async fetchHeliusTransaction(signature: string): Promise<HeliusEnhancedTransaction | null> {
    if (!this.config.insiderHeliusApiKey) {
      log.error('HELIUS_API_KEY is required for InsiderBot simplified mode');
      return null;
    }
    const url = `https://api-mainnet.helius-rpc.com/v0/transactions/?api-key=${this.config.insiderHeliusApiKey}`;
    
    for (let attempt = 1; attempt <= 3; attempt++) {
      try {
        const response = await fetch(url, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ transactions: [signature] }),
        });
        if (response.ok) {
          const data = await response.json() as HeliusEnhancedTransaction[];
          return data[0] || null;
        }
      } catch (err) {
        log.error(`Helius fetch error (attempt ${attempt})`, err);
      }
      await new Promise(r => setTimeout(r, 500 * attempt));
    }
    return null;
  }

  private getHeliusPoolSwaps(tx: HeliusEnhancedTransaction): HeliusPoolSwap[] {
    const swaps: HeliusPoolSwap[] = [];

    if (tx.events?.swap) {
      const e = tx.events.swap;
      for (const out of e.tokenOutputs ?? []) {
        swaps.push({
          wallet: out.userCashFlowAccount,
          direction: 'buy',
          mint: out.mint,
          amount: parseFloat(out.amount),
        });
      }
      for (const input of e.tokenInputs ?? []) {
        swaps.push({
          wallet: input.userCashFlowAccount,
          direction: 'sell',
          mint: input.mint,
          amount: parseFloat(input.amount),
        });
      }
      if (swaps.length > 0) return swaps;
    }

    const tokenTransfers = tx.tokenTransfers ?? [];
    const nativeTransfers = tx.nativeTransfers ?? [];

    const userActivity = new Map<string, {
      tokenIn: Array<{ mint: string, amount: number }>,
      tokenOut: Array<{ mint: string, amount: number }>,
      solIn: number,
      solOut: number
    }>();

    const getOrCreate = (wallet: string) => {
      if (!userActivity.has(wallet)) {
        userActivity.set(wallet, { tokenIn: [], tokenOut: [], solIn: 0, solOut: 0 });
      }
      return userActivity.get(wallet)!;
    };

    for (const t of tokenTransfers) {
      if (!t.mint) continue;
      if (t.mint === SOL_MINT) {
        if (t.toUserAccount) getOrCreate(t.toUserAccount).solIn += t.tokenAmount ?? 0;
        if (t.fromUserAccount) getOrCreate(t.fromUserAccount).solOut += t.tokenAmount ?? 0;
      } else {
        if (t.toUserAccount) getOrCreate(t.toUserAccount).tokenIn.push({ mint: t.mint, amount: t.tokenAmount ?? 0 });
        if (t.fromUserAccount) getOrCreate(t.fromUserAccount).tokenOut.push({ mint: t.mint, amount: t.tokenAmount ?? 0 });
      }
    }

    for (const n of nativeTransfers) {
      const solAmount = (n.amount ?? 0) / 1e9;
      if (n.toUserAccount) getOrCreate(n.toUserAccount).solIn += solAmount;
      if (n.fromUserAccount) getOrCreate(n.fromUserAccount).solOut += solAmount;
    }

    for (const [wallet, activity] of userActivity.entries()) {
      // Buy: SOL Out, Token In
      if ((activity.solOut > 0 || activity.solIn < 0) && activity.tokenIn.length > 0) {
        for (const tin of activity.tokenIn) {
          swaps.push({ wallet, direction: 'buy', mint: tin.mint, amount: tin.amount });
        }
      }
      // Sell: Token Out, SOL In
      if (activity.tokenOut.length > 0 && (activity.solIn > 0 || activity.solOut < 0)) {
        for (const tout of activity.tokenOut) {
          swaps.push({ wallet, direction: 'sell', mint: tout.mint, amount: tout.amount });
        }
      }
    }

    return swaps;
  }
}
