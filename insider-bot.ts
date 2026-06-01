import { Connection, PublicKey } from '@solana/web3.js';
import { EventEmitter } from 'events';
import { createLogger } from './logger';
import type { ServiceConfig } from './types';

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

export declare interface InsiderBot {
  on(event: 'buyTrigger', listener: (trigger: InsiderBuyTrigger) => void): this;
  on(event: 'sellTrigger', listener: (trigger: InsiderSellTrigger) => void): this;
  on(event: 'error', listener: (error: Error) => void): this;
  emit(event: 'buyTrigger', trigger: InsiderBuyTrigger): boolean;
  emit(event: 'sellTrigger', trigger: InsiderSellTrigger): boolean;
  emit(event: 'error', error: Error): boolean;
}

export class InsiderBot extends EventEmitter {
  private readonly config: ServiceConfig;
  private readonly connection: Connection;
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

  constructor(config: ServiceConfig) {
    super();
    this.config = config;
    this.buySol = config.insiderBuySol;
    this.connection = new Connection(config.insiderSolanaRpcUrl, {
      commitment: 'processed',
      wsEndpoint: config.insiderSolanaWsUrl,
    });
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
  }

  markPositionBought(trigger: InsiderBuyTrigger): void {
    this.activePosition = {
      followedWallet: trigger.followedWallet,
      insiderWallet: trigger.insiderWallet,
      mint: trigger.mint,
    };
    void this.startInsiderWalletWatch(trigger.insiderWallet, trigger.mint);
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
      const txs = await this.fetchEarliestMintTransactions(mint);
      this.mintScanCount = Math.min(txs.length, 20);
      this.mintBuyers.clear();

      const earliestTxs = txs.slice(0, 20);
      for (let i = 0; i < earliestTxs.length; i++) {
        const tx = earliestTxs[i];
        const insiderWallet = this.findEarlyHeliusInsider(
          tx,
          earliestTxs.slice(0, i),
          followedWallet,
          mint
        );
        if (!insiderWallet) continue;

        const signature = tx.signature ?? '';
        log.warn('Insider wallet detected from earliest mint sell before visible buy', {
          followedWallet,
          insiderWallet,
          mint,
          signature,
          scannedTxs: this.mintScanCount,
        });
        await this.stopMintScanOnly();
        this.emit('buyTrigger', {
          followedWallet,
          insiderWallet,
          mint,
          signature,
          buySol: this.buySol,
        });
        return;
      }

      if (txs.length >= 20) {
        log.info('Insider mint scan completed without insider signal', {
          followedWallet,
          mint,
          scannedTxs: this.mintScanCount,
        });
        await this.stopMintScanOnly();
        return;
      }

      log.info('Insider mint scan waiting for earliest 20 transactions', {
        followedWallet,
        mint,
        receivedTxs: txs.length,
        attempt,
      });
      await new Promise((resolve) => setTimeout(resolve, Math.min(1_000 + attempt * 500, 5_000)));
    }
  }

  private async fetchEarliestMintTransactions(mint: string): Promise<HeliusEnhancedTransaction[]> {
    const url = `https://api-mainnet.helius-rpc.com/v0/addresses/${mint}/transactions?token-accounts=none&sort-order=asc&api-key=${this.config.insiderHeliusApiKey}&limit=20`;

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
      log.warn('Insider wallet detected from early mint sell before visible buy', {
        followedWallet,
        insiderWallet: insiderSell.owner,
        mint,
        signature,
        scannedTxs: this.mintScanCount,
      });
      await this.stopMintScanOnly();
      this.emit('buyTrigger', {
        followedWallet,
        insiderWallet: insiderSell.owner,
        mint,
        signature,
        buySol: this.buySol,
      });
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

  private async startInsiderWalletWatch(insiderWallet: string, positionMint: string): Promise<void> {
    if (this.insiderSubId !== null) {
      await this.connection.removeOnLogsListener(this.insiderSubId).catch(() => undefined);
      this.insiderSubId = null;
    }

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

    log.info('Insider wallet exit watch started', {
      insiderWallet,
      positionMint,
    });
  }

  private async processInsiderWalletSignature(
    insiderWallet: string,
    positionMint: string,
    signature: string
  ): Promise<void> {
    const key = `insider:${signature}`;
    if (this.processedSignatures.has(key)) return;
    this.processedSignatures.add(key);
    const buy = await this.findInsiderWalletBuy(signature, insiderWallet);
    if (!buy) return;

    log.warn('Insider wallet made a later buy; sell trigger fired', {
      insiderWallet,
      positionMint,
      insiderBuyMint: buy.mint,
      signature,
    });

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

  private async findInsiderWalletBuy(
    signature: string,
    insiderWallet: string
  ): Promise<{ mint: string; amount: number } | null> {
    if (this.config.insiderHeliusApiKey) {
      const heliusTx = await this.fetchHeliusTransaction(signature);
      const heliusBuy = heliusTx ? this.findHeliusWalletBuy(heliusTx, insiderWallet) : null;
      if (heliusBuy) return heliusBuy;
    }

    const tx = await this.fetchParsedTransaction(signature);
    return tx ? this.findWalletBuy(tx, insiderWallet) : null;
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

  private findHeliusWalletBuy(
    tx: HeliusEnhancedTransaction,
    wallet: string
  ): { mint: string; amount: number } | null {
    const buy = this.getHeliusPoolSwaps(tx)
      .find((swap) => swap.direction === 'buy' && swap.wallet === wallet);
    if (buy) {
      return {
        mint: buy.mint,
        amount: buy.amount,
      };
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
