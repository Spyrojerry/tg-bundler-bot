import { Connection, PublicKey } from '@solana/web3.js';
import { EventEmitter } from 'events';
import { createLogger } from './logger';
import type { ServiceConfig } from './types';
import { TelegramBot } from './telegram-bot';

const log = createLogger('INSIDER');
const SOL_MINT = 'So11111111111111111111111111111111111111112';
const KNOWN_POOL_AUTHORITIES = new Set([
  'FhVo3mqL8PW5pH5U2CN4XE33DokiyZnUwuGpH2hmHLuM',
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
    state: 'WAITING_FOR_TRANSFER' | 'WAITING_FOR_TX_1' | 'WAITING_FOR_TX_2';
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

  clearActivePosition(): void {
    this.activePosition = null;
    this.preBuySequence = null;
    if (this.insiderSubId !== null) {
      const subId = this.insiderSubId;
      this.insiderSubId = null;
      this.connection.removeOnLogsListener(subId).catch(() => undefined);
    }
    log.info('Insider active position cleared');
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
        log.warn('Insider wallet detected; starting entry sequence watch (Waiting for Transfer)', {
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
          state: 'WAITING_FOR_TRANSFER',
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

    for (const transfer of tx.tokenTransfers ?? []) {
      if (!transfer.mint || transfer.mint === SOL_MINT) continue;
      if (onlyMint && transfer.mint !== onlyMint) continue;

      const fromIsPool = this.isKnownPoolAuthority(transfer.fromUserAccount ?? '');
      const toIsPool = this.isKnownPoolAuthority(transfer.toUserAccount ?? '');
      if (fromIsPool === toIsPool) continue;

      const wallet = fromIsPool ? transfer.toUserAccount : transfer.fromUserAccount;
      if (!wallet || this.isKnownPoolAuthority(wallet)) continue;

      const tokenDirection: HeliusPoolSwapDirection = fromIsPool ? 'buy' : 'sell';
      const solDirection = this.getHeliusSolDirection(tx, wallet);

      if (solDirection && solDirection !== tokenDirection) {
        continue;
      }

      swaps.push({
        wallet,
        direction: tokenDirection,
        mint: transfer.mint,
        amount: transfer.tokenAmount ?? 0,
      });
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
      log.warn('Insider wallet detected; starting entry sequence watch (Waiting for Transfer)', {
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
        state: 'WAITING_FOR_TRANSFER',
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
        this.processInsiderWalletSignature(insiderWallet, positionMint, logInfo.signature).catch((err) => {
          log.error(`Failed to process insider wallet signature ${logInfo.signature}`, err);
          this.emit('error', err instanceof Error ? err : new Error(String(err)));
        });
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
    afterSignature: string
  ): Promise<void> {
    let currentAfter = afterSignature;
    let hasMore = true;

    while (hasMore) {
      const txs = await this.fetchHeliusTransactionsAfter(insiderWallet, currentAfter);
      if (txs.length === 0) {
        hasMore = false;
        break;
      }

      log.info(`Processing ${txs.length} historical transactions for catch-up`, { insiderWallet });
      
      // Helius returns txs in chronological order when using after-signature? 
      // Actually, standard Helius /transactions endpoint returns newest first.
      // But user mentioned: sort-order=asc
      
      for (const tx of txs) {
        if (!tx.signature) continue;
        await this.processInsiderWalletSignature(insiderWallet, positionMint, tx.signature, tx);
        currentAfter = tx.signature;
      }

      // If we got less than 100, we're likely caught up
      if (txs.length < 100) {
        hasMore = false;
      }
    }
  }

  private async fetchHeliusTransactionsAfter(
    address: string,
    afterSignature: string
  ): Promise<HeliusEnhancedTransaction[]> {
    const url = `https://api-mainnet.helius-rpc.com/v0/addresses/${address}/transactions?token-accounts=none&sort-order=asc&api-key=${this.config.insiderHeliusApiKey}&after-signature=${afterSignature}`;

    for (let attempt = 1; attempt <= 3; attempt++) {
      try {
        const response = await fetch(url);
        if (response.ok) {
          return await response.json() as HeliusEnhancedTransaction[];
        }
        const text = await response.text();
        log.warn(`Helius catch-up fetch attempt ${attempt}/3 failed`, { status: response.status, body: text });
      } catch (err) {
        log.warn(`Helius catch-up fetch attempt ${attempt}/3 error`, err);
      }
      await new Promise(r => setTimeout(r, attempt * 1000));
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

      const { type, mint } = analysis;
      const html = (value: string): string => value.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');

      if (this.preBuySequence.state === 'WAITING_FOR_TRANSFER') {
        if (type === 'transfer' && mint === positionMint) {
          log.info('Insider entry sequence: Transfer detected. Moving to Tx #1 watch.', { signature, mint });
          this.preBuySequence.state = 'WAITING_FOR_TX_1';
          
          this.telegramBot?.sendDefault([
            '<b>🔄 Insider Sequence: Transfer Detected</b>',
            `Token: <code>${html(positionMint)}</code>`,
            `Insider: <code>${html(insiderWallet)}</code>`,
            'Waiting for 2 subsequent transactions before buying.',
          ].join('\n')).catch(() => undefined);
        }
      } else if (this.preBuySequence.state === 'WAITING_FOR_TX_1') {
        if ((type === 'buy' || type === 'sell') && mint === positionMint) {
          log.info('Insider entry sequence: Tx #1 detected. Moving to Tx #2 watch.', { signature, mint, type });
          this.preBuySequence.state = 'WAITING_FOR_TX_2';

          this.telegramBot?.sendDefault([
            '<b>🔄 Insider Sequence: Tx #1 Detected</b>',
            `Token: <code>${html(positionMint)}</code>`,
            `Action: <b>${type.toUpperCase()}</b>`,
            'Waiting for 1 more transaction before buying.',
          ].join('\n')).catch(() => undefined);
        }
      } else if (this.preBuySequence.state === 'WAITING_FOR_TX_2') {
        if ((type === 'buy' || type === 'sell') && mint === positionMint) {
          log.warn('Insider entry sequence: Tx #2 detected. TRIGGERING BUY.', { signature, mint, type });
          
          const seq = this.preBuySequence;
          this.preBuySequence = null;

          this.telegramBot?.sendDefault([
            '<b>🚀 Insider Sequence: Tx #2 Detected - BUYING</b>',
            `Token: <code>${html(positionMint)}</code>`,
            `Action: <b>${type.toUpperCase()}</b>`,
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
  ): Promise<{ type: 'buy' | 'sell' | 'transfer' | 'other'; mint?: string } | null> {
    if (this.config.insiderHeliusApiKey) {
      const heliusTx = historicalTx || await this.fetchHeliusTransaction(signature);
      if (heliusTx) {
        const swaps = this.getHeliusPoolSwaps(heliusTx);
        const buy = swaps.find(s => s.direction === 'buy' && s.wallet === insiderWallet);
        if (buy) return { type: 'buy', mint: buy.mint };
        
        const sell = swaps.find(s => s.direction === 'sell' && s.wallet === insiderWallet && s.mint === targetMint);
        if (sell) return { type: 'sell', mint: sell.mint };

        for (const transfer of heliusTx.tokenTransfers ?? []) {
          if (transfer.fromUserAccount !== insiderWallet && transfer.toUserAccount === insiderWallet && transfer.mint === targetMint) {
            return { type: 'transfer', mint: transfer.mint };
          }
        }
      }
    }

    // Standard RPC fallback only if not catch-up (historicalTx)
    if (historicalTx) return null; 

    const tx = await this.fetchParsedTransaction(signature);
    if (!tx) return null;

    const deltas = this.getTokenDeltas(tx.meta?.preTokenBalances ?? [], tx.meta?.postTokenBalances ?? []);
    
    // Check for buy
    const isBuy = deltas.some(d => d.owner === insiderWallet && d.rawDiff > 0n && d.mint !== SOL_MINT);
    if (isBuy) {
      const b = deltas.find(d => d.owner === insiderWallet && d.rawDiff > 0n && d.mint !== SOL_MINT);
      return { type: 'buy', mint: b?.mint };
    }

    // Check for sell vs transfer
    const targetDelta = deltas.find(d => d.owner === insiderWallet && d.mint === targetMint);
    if (targetDelta) {
      if (targetDelta.rawDiff < 0n) {
        const poolReceived = deltas.some(d => this.isKnownPoolAuthority(d.owner) && d.rawDiff > 0n && d.mint === targetMint);
        return { type: poolReceived ? 'sell' : 'other', mint: targetMint };
      }
      if (targetDelta.rawDiff > 0n) {
        // Transfer in
        return { type: 'transfer', mint: targetMint };
      }
    }

    return { type: 'other' };
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
