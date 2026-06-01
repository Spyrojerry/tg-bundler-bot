import { Connection, PublicKey } from '@solana/web3.js';
import { EventEmitter } from 'events';
import { createLogger } from './logger';
import type { ServiceConfig } from './types';

const log = createLogger('INSIDER');
const SOL_MINT = 'So11111111111111111111111111111111111111112';

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
    this.mintBuyers.clear();

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

    for (const delta of deltas) {
      if (delta.rawDiff > 0n) {
        this.mintBuyers.add(delta.owner);
      }
    }

    const insiderSell = deltas.find((delta) =>
      delta.rawDiff < 0n
      && delta.owner !== followedWallet
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
    const tx = await this.fetchParsedTransaction(signature);
    if (!tx) return;

    const buy = this.findWalletBuy(tx, insiderWallet);
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

  private findWalletBuy(
    tx: NonNullable<Awaited<ReturnType<Connection['getParsedTransaction']>>>,
    wallet: string
  ): { mint: string; amount: number } | null {
    const deltas = this.getTokenDeltas(tx.meta?.preTokenBalances ?? [], tx.meta?.postTokenBalances ?? []);
    const buy = deltas.find((delta) =>
      delta.owner === wallet
      && delta.rawDiff > 0n
      && delta.mint !== SOL_MINT
    );
    return buy ? { mint: buy.mint, amount: buy.amount } : null;
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
