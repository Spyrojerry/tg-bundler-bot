import { Connection, PublicKey } from '@solana/web3.js';
import { EventEmitter } from 'events';
import { createLogger } from './logger';
import type { ServiceConfig } from './types';
import { TelegramBot } from './telegram-bot';

const log = createLogger('INSIDER');
const SOL_MINT = 'So11111111111111111111111111111111111111112';
const KNOWN_POOL_AUTHORITIES = new Set([
  'FhVo3mqL8PW5pH5U2CN4XE33DokiyZnUwuGpH2hmHLuM',
  '5Q544fKrZM6W6y77W4A2B4L2S97E6q5nN5T6v8D5H5', // Raydium Authority
  '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8', // Raydium V4
  '9W959DqmcGTu2YHcR6Yn3S3XN6XN6S6S6S6S6S6S6S', // Orca
  'Eo7WjKq67rjJQSvbdBk6RToZp4EAtX4F2Xv7V95v2', // Meteora
  '6EF8rrecthR5DkjtvAXth2Jy1Gq3BvF4YQDQe4N362K', // Pump.fun
  'TSLpA7P3qPqbeXh3WfJLue2osyZH1G1A2A2A2A2A2A2', // Meteora Vault
]);

type ParsedTokenBalance = {
  accountIndex: number;
  mint: string;
  owner?: string;
  uiTokenAmount: {
    amount: string;
    decimals: number;
  };
};

interface TokenDelta {
  owner: string;
  mint: string;
  rawDiff: bigint;
  amount: number;
}

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

interface InsiderAnalysis {
  mint: string;
  isTransferIn: boolean; // Received tokens from non-pool
  isBuy: boolean;        // Swap SOL/WSOL -> Token
  isSell: boolean;       // Swap Token -> SOL/WSOL
  isActivity: boolean;   // Any transaction involving the target mint
}

export class InsiderBot extends EventEmitter {
  private readonly config: ServiceConfig;
  private readonly connection: Connection;
  private readonly telegramBot: TelegramBot | null;
  private followedWallet: string | null = null;
  private buySol: number;
  private followSubId: number | null = null;
  private mintSubId: number | null = null;
  private insiderSubId: number | null = null;
  private firstBuyHandled = false;
  private mintScanCount = 0;
  private mintScanActive = false;
  private mintBuyers = new Set<string>();
  private processedSignatures = new Set<string>();
  private activePosition: {
    followedWallet: string;
    insiderWallet: string;
    mint: string;
  } | null = null;
  private preBuySequence: {
    followedWallet: string;
    insiderWallet: string;
    mint: string;
    state: 'WAITING_FOR_TX_1' | 'WAITING_FOR_TX_2';
    entrySignature: string;
  } | null = null;

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
    return this.preBuySequence?.mint ?? null;
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
    return this.followSubId !== null || this.mintSubId !== null || this.insiderSubId !== null;
  }

  async followWallet(address: string): Promise<void> {
    const normalized = new PublicKey(address).toBase58();
    await this.stop();
    this.followedWallet = normalized;
    this.firstBuyHandled = false;

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
      rpc: this.config.insiderSolanaRpcUrl,
    });
  }

  async stop(): Promise<void> {
    const removals: Array<Promise<void>> = [];
    if (this.followSubId !== null) {
      removals.push(this.connection.removeOnLogsListener(this.followSubId));
      this.followSubId = null;
    }
    if (this.mintSubId !== null) {
      removals.push(this.connection.removeOnLogsListener(this.mintSubId));
      this.mintSubId = null;
    }
    if (this.insiderSubId !== null) {
      removals.push(this.connection.removeOnLogsListener(this.insiderSubId));
      this.insiderSubId = null;
    }
    await Promise.allSettled(removals);
    this.firstBuyHandled = false;
    this.mintScanCount = 0;
    this.mintScanActive = false;
    this.mintBuyers.clear();
    this.processedSignatures.clear();
    this.activePosition = null;
    this.preBuySequence = null;
  }

  markPositionBought(trigger: InsiderBuyTrigger): void {
    this.activePosition = {
      followedWallet: trigger.followedWallet,
      insiderWallet: trigger.insiderWallet,
      mint: trigger.mint,
    };
    this.preBuySequence = null;
    this.startInsiderWalletWatch(trigger.insiderWallet, trigger.mint, trigger.signature).catch((err) => {
      log.error('Failed to restart insider watch after buy', err);
    });
  }

  private async processFollowWalletSignature(signature: string): Promise<void> {
    if (!this.followedWallet || this.firstBuyHandled || this.processedSignatures.has(signature)) return;
    this.processedSignatures.add(signature);
    const tx = await this.fetchParsedTransaction(signature);
    if (!tx) return;

    const buy = this.findWalletBuy(tx, this.followedWallet);
    if (!buy) return;

    this.firstBuyHandled = true;
    log.info('Followed wallet first buy detected', {
      followedWallet: this.followedWallet,
      mint: buy.mint,
      signature,
    });
    await this.startMintScan(this.followedWallet, buy.mint);
  }

  private async startMintScan(followedWallet: string, mint: string): Promise<void> {
    if (this.mintSubId !== null) {
      await this.connection.removeOnLogsListener(this.mintSubId).catch(() => undefined);
      this.mintSubId = null;
    }

    this.mintScanCount = 0;
    this.mintScanActive = true;
    this.mintBuyers.clear();

    if (this.config.insiderHeliusApiKey) {
      void this.scanEarliestMintTransactions(followedWallet, mint).catch((err) => {
        log.error(`Failed to scan earliest mint transactions for ${mint}`, err);
        this.emit('error', err instanceof Error ? err : new Error(String(err)));
      });
      log.info('Insider Helius earliest mint scan started', {
        followedWallet,
        mint,
        maxTxs: 20,
      });
      return;
    }

    this.mintSubId = this.connection.onLogs(
      new PublicKey(mint),
      (logInfo) => {
        if (logInfo.err) return;
        this.processMintSignature(followedWallet, mint, logInfo.signature).catch((err) => {
          log.error(`Failed to process mint signature ${logInfo.signature}`, err);
          this.emit('error', err instanceof Error ? err : new Error(String(err)));
        });
      },
      'processed'
    );

    log.info('Insider mint scan started', {
      followedWallet,
      mint,
      maxTxs: 20,
    });
  }

  private async scanEarliestMintTransactions(followedWallet: string, mint: string): Promise<void> {
    let attempt = 0;

    while (this.mintScanActive) {
      attempt += 1;
      const { fetchedTxs, swapTxs } = await this.fetchEarliestMintSwapTransactions(mint);
      this.mintScanCount = swapTxs.length;
      this.mintBuyers.clear();

      if (swapTxs.length > 0 && attempt === 1) {
        log.info('Analyzing earliest mint transactions', {
          mint,
          firstTxTimestamp: swapTxs[0].timestamp,
          firstTxSignature: swapTxs[0].signature,
        });
      }

      for (let i = 0; i < swapTxs.length; i++) {
        const tx = swapTxs[i];
        const insiderWallet = this.findEarlyHeliusInsider(
          tx,
          swapTxs.slice(0, i),
          followedWallet,
          mint
        );
        if (!insiderWallet) continue;

        const signature = tx.signature ?? '';
        log.warn('Insider wallet detected; starting entry sequence watch (Waiting for Tx #1)', {
          followedWallet,
          insiderWallet,
          mint,
          signature,
          scannedSwapTxs: this.mintScanCount,
          fetchedTxs,
        });
        await this.stopMintScanOnly();

        this.preBuySequence = {
          followedWallet,
          insiderWallet,
          mint,
          state: 'WAITING_FOR_TX_1',
          entrySignature: signature,
        };

        await this.startInsiderWalletWatch(insiderWallet, mint, signature);
        return;
      }

      if (swapTxs.length >= 20) {
        log.info('Insider mint scan completed without insider signal', {
          followedWallet,
          mint,
          scannedSwapTxs: this.mintScanCount,
          fetchedTxs,
        });
        await this.stopMintScanOnly();
        return;
      }

      log.info('Insider mint scan waiting for earliest 20 transactions', {
        followedWallet,
        mint,
        receivedSwapTxs: swapTxs.length,
        fetchedTxs,
        attempt,
      });
      await new Promise((resolve) => setTimeout(resolve, Math.min(1_000 + attempt * 500, 5_000)));
    }
  }

  private async fetchEarliestMintSwapTransactions(
    mint: string
  ): Promise<{ fetchedTxs: number; swapTxs: HeliusEnhancedTransaction[] }> {
    const swapTxs: HeliusEnhancedTransaction[] = [];
    let fetchedTxs = 0;
    let afterSignature: string | null = null;

    while (swapTxs.length < 20 && this.mintScanActive) {
      const batch = await this.fetchEarliestMintTransactionPage(mint, afterSignature);
      fetchedTxs += batch.length;
      if (batch.length === 0) break;

      for (const tx of batch) {
        if (this.getHeliusPoolSwaps(tx, mint).length === 0) continue;
        swapTxs.push(tx);
        if (swapTxs.length >= 20) break;
      }

      afterSignature = batch[batch.length - 1]?.signature ?? null;
      if (batch.length < 50 || !afterSignature) break;
    }

    return {
      fetchedTxs,
      swapTxs: swapTxs.slice(0, 20),
    };
  }

  private async fetchEarliestMintTransactionPage(
    mint: string,
    afterSignature: string | null
  ): Promise<HeliusEnhancedTransaction[]> {
    const afterParam = afterSignature ? `&after-signature=${afterSignature}` : '';
    const url = `https://api-mainnet.helius-rpc.com/v0/addresses/${mint}/transactions?token-accounts=none&sort-order=asc&api-key=${this.config.insiderHeliusApiKey}${afterParam}&limit=50`;

    for (let attempt = 1; attempt <= 5; attempt++) {
      const response = await fetch(url);
      if (response.ok) {
        return await response.json() as HeliusEnhancedTransaction[];
      }

      const text = await response.text();
      log.warn(`Helius earliest mint transaction fetch attempt ${attempt}/5 failed`, {
        mint,
        status: response.status,
        body: text,
      });
      await new Promise((resolve) => setTimeout(resolve, attempt * 750));
    }

    return [];
  }

  private findEarlyHeliusInsider(
    tx: HeliusEnhancedTransaction,
    priorTxs: HeliusEnhancedTransaction[],
    followedWallet: string,
    mint: string
  ): string | null {
    for (const seller of this.getHeliusMintSellers(tx, mint)) {
      if (
        seller !== followedWallet
        && !this.isKnownPoolAuthority(seller)
        && !this.hasPriorHeliusMintBuy(priorTxs, seller, mint)
      ) {
        return seller;
      }
    }

    return null;
  }

  private hasPriorHeliusMintBuy(
    priorTxs: HeliusEnhancedTransaction[],
    wallet: string,
    mint: string
  ): boolean {
    return priorTxs.some((tx) => this.getHeliusMintBuyers(tx, mint).has(wallet));
  }

  private getHeliusMintBuyers(tx: HeliusEnhancedTransaction, mint: string): Set<string> {
    return new Set(
      this.getHeliusPoolSwaps(tx, mint)
        .filter((swap) => swap.direction === 'buy')
        .map((swap) => swap.wallet)
    );
  }

  private getHeliusMintSellers(tx: HeliusEnhancedTransaction, mint: string): Set<string> {
    return new Set(
      this.getHeliusPoolSwaps(tx, mint)
        .filter((swap) => swap.direction === 'sell')
        .map((swap) => swap.wallet)
    );
  }

  private getHeliusPoolSwaps(tx: HeliusEnhancedTransaction, onlyMint?: string): HeliusPoolSwap[] {
    const swaps: HeliusPoolSwap[] = [];

    // 1. Try to use Helius native swap events first (most accurate)
    if (tx.events?.swap) {
      const e = tx.events.swap;
      // Handle token outputs (Buy)
      for (const out of e.tokenOutputs ?? []) {
        if (onlyMint && out.mint !== onlyMint) continue;
        swaps.push({
          wallet: out.userCashFlowAccount,
          direction: 'buy',
          mint: out.mint,
          amount: parseFloat(out.amount),
        });
      }
      // Handle token inputs (Sell)
      for (const input of e.tokenInputs ?? []) {
        if (onlyMint && input.mint !== onlyMint) continue;
        swaps.push({
          wallet: input.userCashFlowAccount,
          direction: 'sell',
          mint: input.mint,
          amount: parseFloat(input.amount),
        });
      }
      if (swaps.length > 0) return swaps;
    }

    // 2. Value Exchange Detection (Implicit Swaps)
    // If Helius missed the swap event, look for tokens moving one way and SOL/WSOL the other way
    // involving the same user account.
    const tokenTransfers = tx.tokenTransfers ?? [];
    const nativeTransfers = tx.nativeTransfers ?? [];

    // Group transfers by user account
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
      if (this.isKnownPoolAuthority(wallet)) continue;

      // Buy: SOL Out, Token In
      if ((activity.solOut > 0 || activity.solIn < 0) && activity.tokenIn.length > 0) {
        for (const tin of activity.tokenIn) {
          if (onlyMint && tin.mint !== onlyMint) continue;
          swaps.push({ wallet, direction: 'buy', mint: tin.mint, amount: tin.amount });
        }
      }
      // Sell: Token Out, SOL In
      if (activity.tokenOut.length > 0 && (activity.solIn > 0 || activity.solOut < 0)) {
        for (const tout of activity.tokenOut) {
          if (onlyMint && tout.mint !== onlyMint) continue;
          swaps.push({ wallet, direction: 'sell', mint: tout.mint, amount: tout.amount });
        }
      }
    }

    if (swaps.length > 0) return swaps;

    // 3. Fallback to transaction type and description + Known Pool Authorities
    const isHeliusSwap = tx.type === 'SWAP' || (tx.description && tx.description.toLowerCase().includes('swapped'));

    for (const transfer of tokenTransfers) {
      if (!transfer.mint || transfer.mint === SOL_MINT) continue;
      if (onlyMint && transfer.mint !== onlyMint) continue;

      const fromIsPool = this.isKnownPoolAuthority(transfer.fromUserAccount ?? '');
      const toIsPool = this.isKnownPoolAuthority(transfer.toUserAccount ?? '');
      
      let wallet: string | undefined;
      let direction: HeliusPoolSwapDirection | undefined;

      if (fromIsPool !== toIsPool) {
        wallet = fromIsPool ? transfer.toUserAccount : transfer.fromUserAccount;
        direction = fromIsPool ? 'buy' : 'sell';
      } else if (isHeliusSwap) {
        // If it's a swap but we don't know the pool, the fee payer or signer is usually the user
        // and the other side is the pool.
        if (transfer.fromUserAccount === tx.feePayer) {
          wallet = transfer.fromUserAccount;
          direction = 'sell';
        } else if (transfer.toUserAccount === tx.feePayer) {
          wallet = transfer.toUserAccount;
          direction = 'buy';
        }
      }

      if (wallet && direction && !this.isKnownPoolAuthority(wallet)) {
        swaps.push({
          wallet,
          direction,
          mint: transfer.mint,
          amount: transfer.tokenAmount ?? 0,
        });
      }
    }

    return swaps;
  }

  private getHeliusSolDirection(
    tx: HeliusEnhancedTransaction,
    wallet: string
  ): HeliusPoolSwapDirection | null {
    const walletSentSolToPool = (tx.tokenTransfers ?? []).some((transfer) =>
      transfer.mint === SOL_MINT
      && transfer.fromUserAccount === wallet
      && this.isKnownPoolAuthority(transfer.toUserAccount ?? '')
    ) || (tx.nativeTransfers ?? []).some((transfer) =>
      transfer.fromUserAccount === wallet
      && this.isKnownPoolAuthority(transfer.toUserAccount ?? '')
      && (transfer.amount ?? 0) > 0
    );
    if (walletSentSolToPool) return 'buy';

    const poolSentSolToWallet = (tx.tokenTransfers ?? []).some((transfer) =>
      transfer.mint === SOL_MINT
      && this.isKnownPoolAuthority(transfer.fromUserAccount ?? '')
      && transfer.toUserAccount === wallet
    ) || (tx.nativeTransfers ?? []).some((transfer) =>
      this.isKnownPoolAuthority(transfer.fromUserAccount ?? '')
      && transfer.toUserAccount === wallet
      && (transfer.amount ?? 0) > 0
    );
    if (poolSentSolToWallet) return 'sell';

    return null;
  }

  private async processMintSignature(
    followedWallet: string,
    mint: string,
    signature: string
  ): Promise<void> {
    const key = `mint:${signature}`;
    if (this.processedSignatures.has(key) || this.mintScanCount >= 20) return;
    this.processedSignatures.add(key);
    this.mintScanCount += 1;

    const tx = await this.fetchParsedTransaction(signature);
    if (!tx) return;

    const deltas = this.getTokenDeltas(tx.meta?.preTokenBalances ?? [], tx.meta?.postTokenBalances ?? [])
      .filter((delta) => delta.mint === mint);
    const signerAddresses = this.getSignerAddresses(tx);

    for (const delta of deltas) {
      if (delta.rawDiff > 0n && !this.isKnownPoolAuthority(delta.owner)) {
        this.mintBuyers.add(delta.owner);
      }
    }

    const insiderSell = deltas.find((delta) =>
      delta.rawDiff < 0n
      && delta.owner !== followedWallet
      && !this.isKnownPoolAuthority(delta.owner)
      && signerAddresses.has(delta.owner)
      && !this.mintBuyers.has(delta.owner)
    );

    if (insiderSell) {
      log.warn('Insider wallet detected; starting entry sequence watch (Waiting for Tx #1)', {
        followedWallet,
        insiderWallet: insiderSell.owner,
        mint,
        signature,
        scannedTxs: this.mintScanCount,
      });
      await this.stopMintScanOnly();
      
      this.preBuySequence = {
        followedWallet,
        insiderWallet: insiderSell.owner,
        mint,
        state: 'WAITING_FOR_TX_1',
        entrySignature: signature,
      };

      await this.startInsiderWalletWatch(insiderSell.owner, mint, signature);
      return;
    }

    if (this.mintScanCount >= 20) {
      log.info('Insider mint scan completed without insider signal', {
        followedWallet,
        mint,
        scannedTxs: this.mintScanCount,
      });
      await this.stopMintScanOnly();
    }
  }

  private async stopMintScanOnly(): Promise<void> {
    this.mintScanActive = false;
    if (this.mintSubId !== null) {
      const subId = this.mintSubId;
      this.mintSubId = null;
      await this.connection.removeOnLogsListener(subId).catch(() => undefined);
    }
  }

  private async resetForNewToken(): Promise<void> {
    log.info('Resetting InsiderBot for new token detection');
    
    // 1. Stop any active mint scanning
    await this.stopMintScanOnly();

    // 2. Stop any active insider wallet monitoring
    if (this.insiderSubId !== null) {
      await this.connection.removeOnLogsListener(this.insiderSubId).catch(() => undefined);
      this.insiderSubId = null;
    }

    // 3. Clear all transient state except the followed wallet
    this.firstBuyHandled = false;
    this.mintScanCount = 0;
    this.mintBuyers.clear();
    this.activePosition = null;
    this.preBuySequence = null;
    this.processedSignatures.clear();

    log.info('InsiderBot reset complete; waiting for next buy from followed wallet', {
      followedWallet: this.followedWallet
    });
  }

  private async startInsiderWalletWatch(
    insiderWallet: string,
    positionMint: string,
    startSignature?: string
  ): Promise<void> {
    if (this.insiderSubId !== null) {
      await this.connection.removeOnLogsListener(this.insiderSubId).catch(() => undefined);
      this.insiderSubId = null;
    }

    log.info('Starting insider wallet watch', { insiderWallet, positionMint, startSignature });

    // 1. If we have a start signature, catch up on all transactions since then
    if (startSignature && this.config.insiderHeliusApiKey) {
      try {
        log.info('Performing insider catch-up...', { insiderWallet, startSignature });
        // Process the start signature itself first to ensure we don't miss anything in it
        await this.processInsiderWalletSignature(insiderWallet, positionMint, startSignature);
        // Then catch up on everything after it
        await this.catchupInsiderWallet(insiderWallet, positionMint, startSignature);
        log.info('Insider catch-up complete');
      } catch (err) {
        log.error('Insider catch-up failed; proceeding with real-time only', err);
      }
    }

    // 2. Start real-time monitoring
    this.insiderSubId = this.connection.onLogs(
      new PublicKey(insiderWallet),
      (logInfo) => {
        if (logInfo.err) return;
        
        // Use batch fetch for real-time updates as well to ensure we don't miss txs in the same block
        void this.catchupInsiderWallet(insiderWallet, positionMint, logInfo.signature, true);
      },
      'processed'
    );

    log.info('Insider wallet real-time watch active', {
      insiderWallet,
      positionMint,
    });
  }

  private async catchupInsiderWallet(
    insiderWallet: string,
    positionMint: string,
    afterSignature: string,
    isRealtimeUpdate = false
  ): Promise<void> {
    // 1. Fetch transactions
    let txs: HeliusEnhancedTransaction[] = [];
    
    if (isRealtimeUpdate) {
      log.info('Processing real-time transaction via Enhanced API', { signature: afterSignature });
      const heliusTx = await this.fetchHeliusTransaction(afterSignature);
      if (heliusTx) txs = [heliusTx];
      else {
        await this.pollAddressHistoryForMissing(insiderWallet, positionMint, afterSignature);
        return;
      }
    } else {
      let currentAfter = afterSignature;
      let iterations = 0;
      let hasMore = true;

      while (hasMore && iterations < 5) {
        iterations++;
        const batch = await this.fetchHeliusTransactionsAfter(insiderWallet, currentAfter);
        if (batch === null || batch.length === 0) {
          if (batch === null && iterations === 1) {
            log.warn('Helius after-signature failed; falling back to recent transactions', { insiderWallet, afterSignature });
            const recent = await this.fetchHeliusTransactionsRecent(insiderWallet);
            const startIndex = recent.findIndex(tx => tx.signature === currentAfter);
            if (startIndex !== -1) {
              txs = recent.slice(0, startIndex).reverse();
            }
          }
          hasMore = false;
        } else {
          txs.push(...batch);
          currentAfter = batch[batch.length - 1].signature ?? currentAfter;
          if (batch.length < 100) hasMore = false;
        }
      }
    }

    // 2. Process transactions in order
    if (txs.length > 0) {
      log.info(`Processing ${txs.length} transactions for insider ${insiderWallet}`, { isRealtimeUpdate });
      for (const tx of txs) {
        if (!tx.signature) continue;
        await this.processInsiderWalletSignature(insiderWallet, positionMint, tx.signature, tx);
      }
    }
  }

  private async pollAddressHistoryForMissing(
    address: string,
    positionMint: string,
    missingSignature: string
  ): Promise<void> {
    const recent = await this.fetchHeliusTransactionsRecent(address);
    const tx = recent.find(t => t.signature === missingSignature);
    if (tx) {
      await this.processInsiderWalletSignature(address, positionMint, missingSignature, tx);
    }
  }

  private async fetchHeliusTransactionsAfter(
    address: string,
    afterSignature: string
  ): Promise<HeliusEnhancedTransaction[] | null> {
    const url = `https://api-mainnet.helius-rpc.com/v0/addresses/${address}/transactions?token-accounts=none&sort-order=asc&api-key=${this.config.insiderHeliusApiKey}&after-signature=${afterSignature}`;

    for (let attempt = 1; attempt <= 3; attempt++) {
      try {
        const response = await fetch(url);
        if (response.ok) {
          return await response.json() as HeliusEnhancedTransaction[];
        }
        const text = await response.text();
        
        // Return null for 400 errors to trigger fallback
        if (response.status === 400) {
          return null;
        }

        log.warn(`Helius catch-up fetch attempt ${attempt}/3 failed`, { status: response.status, body: text });
      } catch (err) {
        log.warn(`Helius catch-up fetch attempt ${attempt}/3 error`, err);
      }
      await new Promise(r => setTimeout(r, attempt * 1000));
    }

    return [];
  }

  private async fetchHeliusTransactionsRecent(
    address: string
  ): Promise<HeliusEnhancedTransaction[]> {
    const url = `https://api-mainnet.helius-rpc.com/v0/addresses/${address}/transactions?token-accounts=none&api-key=${this.config.insiderHeliusApiKey}`;
    
    try {
      const response = await fetch(url);
      if (response.ok) {
        return await response.json() as HeliusEnhancedTransaction[];
      }
    } catch (err) {
      log.error('Failed to fetch recent Helius transactions', err);
    }
    return [];
  }

  private async processInsiderWalletSignature(
    insiderWallet: string,
    positionMint: string,
    signature: string,
    historicalTx?: HeliusEnhancedTransaction
  ): Promise<void> {
    const key = `insider:${signature}`;
    if (this.processedSignatures.has(key)) return;
    this.processedSignatures.add(key);

    // Case 1: Active Position (Exit Monitoring)
    if (this.activePosition) {
      const buy = await this.findInsiderWalletBuy(signature, insiderWallet, historicalTx);
      if (!buy) return;

      log.warn('🚨🚨 [INSIDER EXIT SIGNAL] 🚨🚨', {
        insiderWallet,
        positionMint,
        insiderBuyMint: buy.mint,
        amount: buy.amount,
        signature,
      });

      const html = (value: string): string => value.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
      this.telegramBot?.sendDefault([
        '<b>🚨 Insider Exit Signal Detected</b>',
        `Insider: <code>${html(insiderWallet)}</code>`,
        `Current Position: <code>${html(positionMint)}</code>`,
        `Insider Buy: <code>${html(buy.mint)}</code>`,
        `Buy Amount: <b>${buy.amount.toLocaleString()} tokens</b>`,
        '',
        '<b>Triggering immediate sell...</b>',
      ].join('\n')).catch(() => undefined);

      this.emit('sellTrigger', {
        followedWallet: this.activePosition?.followedWallet ?? this.followedWallet ?? '',
        insiderWallet,
        positionMint,
        insiderBuyMint: buy.mint,
        signature,
      });

      if (this.insiderSubId !== null) {
        const subId = this.insiderSubId;
        this.insiderSubId = null;
        await this.connection.removeOnLogsListener(subId).catch(() => undefined);
      }
      return;
    }

    // Case 2: Pre-Buy Sequence (Entry Monitoring)
    if (this.preBuySequence && this.preBuySequence.insiderWallet === insiderWallet) {
      const analysis = await this.analyzeInsiderTransaction(signature, insiderWallet, positionMint, historicalTx);
      if (!analysis) return;

      const { isActivity, mint } = analysis;
      const html = (value: string): string => value.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');

      if (this.preBuySequence.state === 'WAITING_FOR_TX_1') {
        if (isActivity && mint === positionMint) {
          log.warn('Insider Entry Stage 1: Tx #1 detected', { insiderWallet, positionMint, signature });
          this.preBuySequence.state = 'WAITING_FOR_TX_2';

          this.telegramBot?.sendDefault([
            '<b>🔄 Insider Sequence: [1/2] Tx #1 Detected</b>',
            `Insider: <code>${html(insiderWallet)}</code>`,
            `Token: <code>${html(positionMint)}</code>`,
            '',
            'Waiting for 1 more transaction before buying.',
          ].join('\n')).catch(() => undefined);
        }
      } else if (this.preBuySequence.state === 'WAITING_FOR_TX_2') {
        if (isActivity && mint === positionMint) {
          log.warn('Insider Entry Stage 2: Tx #2 detected - TRIGGERING BUY', { insiderWallet, positionMint, signature });
          
          const seq = this.preBuySequence;
          this.preBuySequence = null;

          this.telegramBot?.sendDefault([
            '<b>🚀 Insider Sequence: [2/2] Tx #2 Detected - BUYING</b>',
            `Insider: <code>${html(insiderWallet)}</code>`,
            `Token: <code>${html(positionMint)}</code>`,
            `Signature: <code>${html(signature)}</code>`,
          ].join('\n')).catch(() => undefined);

          this.emit('buyTrigger', {
            followedWallet: seq.followedWallet,
            insiderWallet: seq.insiderWallet,
            mint: seq.mint,
            signature: signature,
            buySol: this.buySol,
          });
        }
      }
    }
  }

  private async analyzeInsiderTransaction(
    signature: string,
    insiderWallet: string,
    targetMint: string,
    historicalTx?: HeliusEnhancedTransaction
  ): Promise<InsiderAnalysis | null> {
    if (this.config.insiderHeliusApiKey) {
      const heliusTx = historicalTx || await this.fetchHeliusTransaction(signature);
      if (heliusTx) {
        const swaps = this.getHeliusPoolSwaps(heliusTx);
        const result: InsiderAnalysis = {
          mint: targetMint,
          isTransferIn: false,
          isBuy: false,
          isSell: false,
          isActivity: false,
        };

        log.debug('Analyzing transaction', {
          signature,
          type: heliusTx.type,
          swapsFound: swaps.length,
          insiderWallet,
          targetMint
        });

        // 1. Check Swaps
        for (const swap of swaps) {
          if (swap.wallet === insiderWallet && swap.mint === targetMint) {
            result.isActivity = true;
            if (swap.direction === 'buy') result.isBuy = true;
            if (swap.direction === 'sell') result.isSell = true;
          }
        }

        // 2. Check Transfers
        for (const transfer of heliusTx.tokenTransfers ?? []) {
          if (transfer.mint === targetMint) {
            const isToInsider = transfer.toUserAccount === insiderWallet;
            const isFromInsider = transfer.fromUserAccount === insiderWallet;

            if (isToInsider || isFromInsider) {
              result.isActivity = true;
            }

            if (isToInsider) {
              const fromIsPool = this.isKnownPoolAuthority(transfer.fromUserAccount ?? '');
              if (!fromIsPool) {
                result.isTransferIn = true;
              }
            }
          }
        }

        // 3. Fallback for activity
        if (!result.isActivity) {
          const types = ['SWAP', 'CLOSE_ACCOUNT', 'TRANSFER'];
          if (heliusTx.type && types.includes(heliusTx.type)) {
            const involvesTarget = (heliusTx.tokenTransfers ?? []).some(t => 
              t.mint === targetMint && (t.fromUserAccount === insiderWallet || t.toUserAccount === insiderWallet)
            );
            if (involvesTarget) {
              result.isActivity = true;
            }
          }
        }

        if (result.isActivity || result.isTransferIn) {
          log.info('Insider transaction analyzed', { 
            signature, 
            isTransferIn: result.isTransferIn, 
            isBuy: result.isBuy, 
            isSell: result.isSell, 
            isActivity: result.isActivity 
          });
          return result;
        }
      }
    }

    // Standard RPC fallback
    if (historicalTx) return null; 

    const tx = await this.fetchParsedTransaction(signature);
    if (!tx) return null;

    const deltas = this.getTokenDeltas(tx.meta?.preTokenBalances ?? [], tx.meta?.postTokenBalances ?? []);
    const result: InsiderAnalysis = {
      mint: targetMint,
      isTransferIn: false,
      isBuy: false,
      isSell: false,
      isActivity: false,
    };

    const targetDelta = deltas.find(d => d.owner === insiderWallet && d.mint === targetMint);
    if (targetDelta) {
      result.isActivity = true;
      if (targetDelta.rawDiff > 0n) result.isTransferIn = true; // RPC fallback treats all incoming as transfer
    }

    const otherBuy = deltas.find(d => d.owner === insiderWallet && d.rawDiff > 0n && d.mint !== SOL_MINT);
    if (otherBuy) {
      result.isBuy = true;
      result.mint = otherBuy.mint;
    }

    return (result.isActivity || result.isTransferIn) ? result : null;
  }

  private async findInsiderWalletBuy(
    signature: string,
    insiderWallet: string,
    historicalTx?: HeliusEnhancedTransaction
  ): Promise<{ mint: string; amount: number } | null> {
    if (this.config.insiderHeliusApiKey) {
      const heliusTx = historicalTx || await this.fetchHeliusTransaction(signature);
      const heliusBuy = heliusTx ? this.findHeliusWalletBuy(heliusTx, insiderWallet) : null;
      if (heliusBuy) return heliusBuy;
    }

    if (historicalTx) return null;

    const tx = await this.fetchParsedTransaction(signature);
    return tx ? this.findWalletBuy(tx, insiderWallet) : null;
  }

  private findHeliusWalletBuy(tx: HeliusEnhancedTransaction, wallet: string): { mint: string; amount: number } | null {
    const buy = this.getHeliusPoolSwaps(tx).find(s => s.direction === 'buy' && s.wallet === wallet);
    return buy ? { mint: buy.mint, amount: buy.amount } : null;
  }

  private async fetchParsedTransaction(signature: string): Promise<NonNullable<Awaited<ReturnType<Connection['getParsedTransaction']>>> | null> {
    for (let attempt = 1; attempt <= 5; attempt++) {
      const tx = await this.connection.getParsedTransaction(signature, {
        commitment: 'confirmed',
        maxSupportedTransactionVersion: 0,
      });
      if (tx) return tx;
      await new Promise((resolve) => setTimeout(resolve, attempt * 250));
    }
    return null;
  }

  private async fetchHeliusTransaction(signature: string): Promise<HeliusEnhancedTransaction | null> {
    const url = `https://api-mainnet.helius-rpc.com/v0/transactions?api-key=${this.config.insiderHeliusApiKey}`;

    for (let attempt = 1; attempt <= 4; attempt++) {
      const response = await fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ transactions: [signature] }),
      });

      if (response.ok) {
        const data = await response.json() as HeliusEnhancedTransaction[];
        return data[0] ?? null;
      }

      const text = await response.text();
      log.warn(`Helius insider transaction fetch attempt ${attempt}/4 failed`, {
        signature,
        status: response.status,
        body: text,
      });
      await new Promise((resolve) => setTimeout(resolve, attempt * 500));
    }

    return null;
  }

  private findWalletBuy(
    tx: NonNullable<Awaited<ReturnType<Connection['getParsedTransaction']>>>,
    wallet: string
  ): { mint: string; amount: number } | null {
    const deltas = this.getTokenDeltas(tx.meta?.preTokenBalances ?? [], tx.meta?.postTokenBalances ?? []);
    const buy = deltas.find((delta) =>
      delta.owner === wallet
      && !this.isKnownPoolAuthority(delta.owner)
      && delta.rawDiff > 0n
      && delta.mint !== SOL_MINT
    );
    return buy ? { mint: buy.mint, amount: buy.amount } : null;
  }

  private isKnownPoolAuthority(address: string): boolean {
    return KNOWN_POOL_AUTHORITIES.has(address);
  }

  private getSignerAddresses(
    tx: NonNullable<Awaited<ReturnType<Connection['getParsedTransaction']>>>
  ): Set<string> {
    const signers = new Set<string>();
    for (const key of tx.transaction.message.accountKeys) {
      if (!key.signer) continue;
      signers.add(key.pubkey.toBase58());
    }
    return signers;
  }

  private getTokenDeltas(
    preBalances: readonly ParsedTokenBalance[],
    postBalances: readonly ParsedTokenBalance[]
  ): TokenDelta[] {
    const byAccount = new Map<number, {
      owner?: string;
      mint: string;
      preAmount: bigint;
      postAmount: bigint;
      decimals: number;
    }>();

    for (const pre of preBalances ?? []) {
      byAccount.set(pre.accountIndex, {
        owner: pre.owner,
        mint: pre.mint,
        preAmount: BigInt(pre.uiTokenAmount.amount),
        postAmount: 0n,
        decimals: pre.uiTokenAmount.decimals,
      });
    }

    for (const post of postBalances ?? []) {
      const existing = byAccount.get(post.accountIndex);
      if (existing) {
        existing.owner = post.owner ?? existing.owner;
        existing.postAmount = BigInt(post.uiTokenAmount.amount);
        existing.decimals = post.uiTokenAmount.decimals;
        existing.mint = post.mint;
      } else {
        byAccount.set(post.accountIndex, {
          owner: post.owner,
          mint: post.mint,
          preAmount: 0n,
          postAmount: BigInt(post.uiTokenAmount.amount),
          decimals: post.uiTokenAmount.decimals,
        });
      }
    }

    return [...byAccount.values()]
      .filter((entry) => entry.owner)
      .map((entry) => {
        const rawDiff = entry.postAmount - entry.preAmount;
        return {
          owner: entry.owner!,
          mint: entry.mint,
          rawDiff,
          amount: Math.abs(Number(rawDiff) / Math.pow(10, entry.decimals)),
        };
      })
      .filter((delta) => delta.rawDiff !== 0n);
  }
}
