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
  private insiderSubId: number | null = null;
  private mintScanActive = false;
  private activePosition: {
    followedWallet: string;
    insiderWallet: string;
    mint: string;
  } | null = null;
  private processedSignatures = new Set<string>();
  private boughtMints = new Set<string>();

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
    return null;
  }

  clearActivePosition(): void {
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

  getFollowedWallet(): string | null {
    return this.followedWallet;
  }

  isRunning(): boolean {
    return this.followSubId !== null || this.insiderSubId !== null || this.mintScanActive;
  }

  async followWallet(address: string): Promise<void> {
    const normalized = new PublicKey(address).toBase58();
    if (this.followedWallet !== normalized) {
      this.boughtMints.clear();
    }
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
    log.info('Insider follow wallet monitoring started', {
      followedWallet: normalized,
      buySol: this.buySol,
    });
  }

  async stop(): Promise<void> {
    const removals: Array<Promise<void>> = [];
    if (this.followSubId !== null) {
      removals.push(this.connection.removeOnLogsListener(this.followSubId));
      this.followSubId = null;
    }
    if (this.insiderSubId !== null) {
      removals.push(this.connection.removeOnLogsListener(this.insiderSubId));
      this.insiderSubId = null;
    }
    await Promise.allSettled(removals);
    this.mintScanActive = false;
    this.activePosition = null;
    this.processedSignatures.clear();
  }

  markPositionBought(trigger: InsiderBuyTrigger): void {
    this.activePosition = {
      followedWallet: trigger.followedWallet,
      insiderWallet: trigger.insiderWallet,
      mint: trigger.mint,
    };
    this.boughtMints.add(trigger.mint);
    this.startInsiderWalletWatch(trigger.insiderWallet, trigger.mint).catch((err) => {
      log.error('Failed to start insider watch after buy', err);
    });
  }

  private async processFollowWalletSignature(signature: string): Promise<void> {
    if (!this.followedWallet || this.processedSignatures.has(signature)) return;
    this.processedSignatures.add(signature);

    const tx = await this.fetchHeliusTransaction(signature);
    if (!tx) return;

    const swaps = this.getHeliusPoolSwaps(tx);
    const followedBuy = swaps.find(s => s.wallet === this.followedWallet && s.direction === 'buy');

    if (followedBuy) {
      if (this.boughtMints.has(followedBuy.mint)) return;
      log.info('Followed wallet buy detected - starting mint scan to find insider', {
        followedWallet: this.followedWallet,
        mint: followedBuy.mint,
        signature,
      });
      void this.scanEarliestMintTransactions(this.followedWallet, followedBuy.mint);
    }
  }

  private async scanEarliestMintTransactions(followedWallet: string, mint: string): Promise<void> {
    this.mintScanActive = true;
    let attempt = 0;

    while (this.mintScanActive) {
      attempt += 1;
      const { swapTxs } = await this.fetchEarliestMintSwapTransactions(mint);

      for (let i = 0; i < swapTxs.length; i++) {
        const tx = swapTxs[i];
        const insiderWallet = this.findEarlyHeliusInsider(tx, swapTxs.slice(0, i), followedWallet, mint);
        
        if (insiderWallet) {
          log.warn('Insider wallet detected for mint - starting reverse watch', {
            followedWallet,
            insiderWallet,
            mint,
          });
          this.mintScanActive = false;
          await this.startInsiderWalletWatch(insiderWallet, mint);
          return;
        }
      }

      if (swapTxs.length >= 20 || attempt >= 5) {
        log.info('Mint scan finished without finding insider', { mint, txsScanned: swapTxs.length });
        this.mintScanActive = false;
        return;
      }

      await new Promise(r => setTimeout(r, 2000));
    }
  }

  private async fetchEarliestMintSwapTransactions(mint: string): Promise<{ swapTxs: HeliusEnhancedTransaction[] }> {
    const url = `https://api-mainnet.helius-rpc.com/v0/addresses/${mint}/transactions?token-accounts=none&sort-order=asc&api-key=${this.config.insiderHeliusApiKey}&limit=50`;
    const swapTxs: HeliusEnhancedTransaction[] = [];

    try {
      const response = await fetch(url);
      if (response.ok) {
        const data = await response.json() as HeliusEnhancedTransaction[];
        for (const tx of data) {
          if (this.getHeliusPoolSwaps(tx, mint).length > 0) {
            swapTxs.push(tx);
            if (swapTxs.length >= 20) break;
          }
        }
      }
    } catch (err) {
      log.error('Failed to fetch earliest mint transactions', err);
    }

    return { swapTxs };
  }

  private findEarlyHeliusInsider(
    tx: HeliusEnhancedTransaction,
    priorTxs: HeliusEnhancedTransaction[],
    followedWallet: string,
    mint: string
  ): string | null {
    const sellers = new Set(this.getHeliusPoolSwaps(tx, mint).filter(s => s.direction === 'sell').map(s => s.wallet));
    
    for (const seller of sellers) {
      if (seller === followedWallet) continue;
      if (this.isKnownPoolAuthority(seller)) continue;

      // Insider is someone who sells early without a prior buy in the first 20 txs
      const hasPriorBuy = priorTxs.some(ptx => 
        this.getHeliusPoolSwaps(ptx, mint).some(s => s.wallet === seller && s.direction === 'buy')
      );

      if (!hasPriorBuy) return seller;
    }

    return null;
  }

  private async startInsiderWalletWatch(insiderWallet: string, positionMint: string): Promise<void> {
    if (this.insiderSubId !== null) {
      await this.connection.removeOnLogsListener(this.insiderSubId).catch(() => undefined);
    }

    log.info('Starting watch on insider wallet', { insiderWallet, positionMint });

    this.insiderSubId = this.connection.onLogs(
      new PublicKey(insiderWallet),
      (logInfo) => {
        if (logInfo.err) return;
        this.processInsiderWalletSignature(insiderWallet, positionMint, logInfo.signature).catch((err) => {
          log.error('Failed to process insider signature', err);
        });
      },
      'processed'
    );
  }

  private async processInsiderWalletSignature(insiderWallet: string, positionMint: string, signature: string): Promise<void> {
    if (this.processedSignatures.has(signature)) return;
    this.processedSignatures.add(signature);

    const tx = await this.fetchHeliusTransaction(signature);
    if (!tx) return;

    const swaps = this.getHeliusPoolSwaps(tx);
    const insiderSwaps = swaps.filter(s => s.wallet === insiderWallet);

    for (const swap of insiderSwaps) {
      if (!this.activePosition) {
        // Bot buys on insider's first SELL of the token
        if (swap.direction === 'sell' && swap.mint === positionMint) {
          log.warn('Insider SELL detected - triggering bot BUY', { insiderWallet, mint: positionMint });
          this.emit('buyTrigger', {
            followedWallet: this.followedWallet!,
            insiderWallet,
            mint: positionMint,
            signature,
            buySol: this.buySol,
          });
          break;
        }
      } else {
        // Bot sells on insider's next BUY (any token)
        if (swap.direction === 'buy') {
          log.warn('Insider BUY detected - triggering bot SELL', { insiderWallet, positionMint });
          this.emit('sellTrigger', {
            followedWallet: this.followedWallet!,
            insiderWallet,
            positionMint,
            insiderBuyMint: swap.mint,
            signature,
          });
          break;
        }
      }
    }
  }

  private async resetForNewToken(): Promise<void> {
    if (this.insiderSubId !== null) {
      await this.connection.removeOnLogsListener(this.insiderSubId).catch(() => undefined);
      this.insiderSubId = null;
    }
    this.activePosition = null;
    this.mintScanActive = false;
    log.info('InsiderBot reset; waiting for next followed wallet buy');
  }

  private isKnownPoolAuthority(addr: string): boolean {
    return KNOWN_POOL_AUTHORITIES.has(addr);
  }

  private async fetchHeliusTransaction(signature: string): Promise<HeliusEnhancedTransaction | null> {
    const url = `https://api-mainnet.helius-rpc.com/v0/transactions/?api-key=${this.config.insiderHeliusApiKey}`;
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
    } catch {}
    return null;
  }

  private getHeliusPoolSwaps(tx: HeliusEnhancedTransaction, onlyMint?: string): HeliusPoolSwap[] {
    const swaps: HeliusPoolSwap[] = [];
    if (tx.events?.swap) {
      const e = tx.events.swap;
      for (const out of e.tokenOutputs ?? []) {
        if (onlyMint && out.mint !== onlyMint) continue;
        swaps.push({ wallet: out.userCashFlowAccount, direction: 'buy', mint: out.mint, amount: parseFloat(out.amount) });
      }
      for (const input of e.tokenInputs ?? []) {
        if (onlyMint && input.mint !== onlyMint) continue;
        swaps.push({ wallet: input.userCashFlowAccount, direction: 'sell', mint: input.mint, amount: parseFloat(input.amount) });
      }
    }
    // Simple transfer fallback
    if (swaps.length === 0) {
      for (const t of tx.tokenTransfers ?? []) {
        if (!t.mint || t.mint === SOL_MINT) continue;
        if (onlyMint && t.mint !== onlyMint) continue;
        const fromPool = this.isKnownPoolAuthority(t.fromUserAccount ?? '');
        const toPool = this.isKnownPoolAuthority(t.toUserAccount ?? '');
        if (fromPool && !toPool) swaps.push({ wallet: t.toUserAccount!, direction: 'buy', mint: t.mint, amount: t.tokenAmount ?? 0 });
        if (!fromPool && toPool) swaps.push({ wallet: t.fromUserAccount!, direction: 'sell', mint: t.mint, amount: t.tokenAmount ?? 0 });
      }
    }
    return swaps;
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
