// ─────────────────────────────────────────────────────────────────────────────
//  bundler-monitor.ts  —  Monitors early bundler wallets for buy/sell activity
//
//  Flow:
//    1. Subscribe to 4 early bundler wallets via Helius webhook or polling
//    2. Detect buys → trigger immediate sell of entire position
//    3. Detect sells → accumulate until 40% of holdings sold → trigger sell
//    4. Send Telegram notifications for all events
// ─────────────────────────────────────────────────────────────────────────────

import { Connection, PublicKey } from '@solana/web3.js';
import { EventEmitter } from 'events';
import { createLogger } from './logger';
import { HeliusClient, HeliusTransaction } from './helius-client';
import type { ServiceConfig } from './types';

const log = createLogger('BUNDLER');

export interface BundlerWallet {
  id: number;
  walletAddress: string;
  initialTokenAmount: number;
  totalSoldAmount: number;
}

export interface BundlerTransaction {
  signature: string;
  walletAddress: string;
  mint: string;
  tokenAmount: number;
  slot: number;
  timestamp: number;
  type: 'buy' | 'sell';
}

export interface BundlerMonitorEvents {
  bundlerBuy: (event: BundlerTransaction & { tradingWallet: string; positionId: number }) => void;
  bundlerSell: (event: BundlerTransaction & { 
    tradingWallet: string; 
    positionId: number;
    cumulativeSoldPercentage: number;
  }) => void;
  thresholdReached: (event: {
    walletAddress: string;
    mint: string;
    tradingWallet: string;
    positionId: number;
    soldPercentage: number;
  }) => void;
  error: (error: Error) => void;
}

export declare interface BundlerMonitor {
  on<E extends keyof BundlerMonitorEvents>(
    event: E,
    listener: BundlerMonitorEvents[E]
  ): this;
  emit<E extends keyof BundlerMonitorEvents>(
    event: E,
    ...args: Parameters<BundlerMonitorEvents[E]>
  ): boolean;
}

export class BundlerMonitor extends EventEmitter {
  private readonly connection: Connection;
  private readonly config: ServiceConfig;
  private readonly heliusClient: HeliusClient;
  private positionId: number | null = null;
  private tradingWallet: string | null = null;
  private mint: string | null = null;
  private bundlerWallets: Map<string, BundlerWallet> = new Map();
  private isRunning = false;
  private processedSignatures: Set<string> = new Set();
  private subscriptionIds: Map<string, number> = new Map();

  constructor(config: ServiceConfig, heliusClient: HeliusClient) {
    super();
    this.config = config;
    this.heliusClient = heliusClient;
    this.connection = new Connection(config.solanaRpcUrl, {
      commitment: 'processed',
      wsEndpoint: config.solanaWsUrl,
    });
  }

  /**
   * Start monitoring a set of early bundler wallets for a specific position
   */
  async startMonitoring(
    positionId: number,
    tradingWallet: string,
    mint: string,
    bundlerWallets: BundlerWallet[]
  ): Promise<void> {
    if (this.isRunning) {
      log.warn('BundlerMonitor already running, stopping previous monitoring');
      await this.stopMonitoring();
    }

    this.positionId = positionId;
    this.tradingWallet = tradingWallet;
    this.mint = mint;
    this.bundlerWallets.clear();

    for (const wallet of bundlerWallets) {
      this.bundlerWallets.set(wallet.walletAddress, wallet);
    }

    this.isRunning = true;
    
    log.info(`Started monitoring ${bundlerWallets.length} early bundler wallets for position ${positionId}`, {
      tradingWallet,
      mint,
      walletAddresses: bundlerWallets.map(w => w.walletAddress),
    });

    // Start WebSocket monitoring for each bundler wallet
    this.startWsMonitoring();
  }

  /**
   * Stop monitoring all bundler wallets
   */
  async stopMonitoring(): Promise<void> {
    if (!this.isRunning) {
      return;
    }

    this.isRunning = false;

    // Unsubscribe from all WebSocket listeners
    for (const [walletAddress, subId] of this.subscriptionIds.entries()) {
      try {
        await this.connection.removeOnLogsListener(subId);
        log.debug(`Unsubscribed from logs for ${walletAddress}`);
      } catch (err) {
        log.warn(`Failed to unsubscribe from logs for ${walletAddress}`, err);
      }
    }
    this.subscriptionIds.clear();

    log.info(`Stopped monitoring early bundler wallets for position ${this.positionId}`);

    this.positionId = null;
    this.tradingWallet = null;
    this.mint = null;
    this.bundlerWallets.clear();
    this.processedSignatures.clear();
  }

  /**
   * Update the sold amount for a bundler wallet (called when sell is detected)
   */
  updateSoldAmount(walletAddress: string, soldAmount: number): void {
    const wallet = this.bundlerWallets.get(walletAddress);
    if (!wallet) {
      log.warn(`Cannot update sold amount for unknown bundler wallet: ${walletAddress}`);
      return;
    }

    wallet.totalSoldAmount += soldAmount;
    
    const soldPercentage = (wallet.totalSoldAmount / wallet.initialTokenAmount) * 100;
    
    log.info(`Updated sold amount for bundler ${walletAddress}: ${soldPercentage.toFixed(2)}% sold`, {
      totalSold: wallet.totalSoldAmount,
      initialAmount: wallet.initialTokenAmount,
    });

    // Check if 40% threshold is reached
    if (soldPercentage >= 40) {
      log.info(`40% sell threshold reached for bundler ${walletAddress}: ${soldPercentage.toFixed(2)}%`);
      
      if (this.positionId !== null && this.tradingWallet && this.mint) {
        this.emit('thresholdReached', {
          walletAddress,
          mint: this.mint,
          tradingWallet: this.tradingWallet,
          positionId: this.positionId,
          soldPercentage,
        });
      }
    }
  }

  private startWsMonitoring(): void {
    if (!this.isRunning || !this.mint) return;

    for (const walletAddress of this.bundlerWallets.keys()) {
      const pubkey = new PublicKey(walletAddress);
      
      const subId = this.connection.onLogs(
        pubkey,
        (logInfo) => {
          if (logInfo.err) return;
          
          log.info(`[BUNDLER WS TX] ${walletAddress.slice(0, 8)}...: ${logInfo.signature}`);
          this.processSignature(logInfo.signature, walletAddress).catch((err) => {
            log.error(`Failed to process bundler logs signature ${logInfo.signature}`, err);
          });
        },
        'processed'
      );
      
      this.subscriptionIds.set(walletAddress, subId);
      log.debug(`Subscribed to logs for bundler ${walletAddress} (id=${subId})`);
    }
  }

  private async processSignature(signature: string, walletAddress: string): Promise<void> {
    if (!this.isRunning || this.processedSignatures.has(signature)) return;

    this.processedSignatures.add(signature);
    
    try {
      // Retry a few times to get the transaction if it's not immediately available
      let tx = null;
      for (let attempt = 1; attempt <= 5; attempt++) {
        tx = await this.connection.getParsedTransaction(signature, {
          commitment: 'confirmed',
          maxSupportedTransactionVersion: 0,
        });
        if (tx) break;
        await new Promise((resolve) => setTimeout(resolve, attempt * 250));
      }

      if (!tx || !this.mint) return;

      // ── Detect Market Swap vs. Simple Transfer ──────────────────────────────
      // To be a buy or sell (market swap), native SOL balance must also change
      // in the opposite direction (e.g., token UP, SOL DOWN).
      const accountKeys = tx.transaction.message.accountKeys.map(k => k.pubkey.toBase58());
      const walletIndex = accountKeys.indexOf(walletAddress);
      
      if (walletIndex === -1) {
        log.warn(`Wallet ${walletAddress} not found in transaction accounts for ${signature}`);
        return;
      }

      const preSol = BigInt(tx.meta?.preBalances[walletIndex] ?? '0');
      const postSol = BigInt(tx.meta?.postBalances[walletIndex] ?? '0');
      const solDelta = postSol - preSol; // Positive = received SOL, Negative = spent SOL
      const fee = BigInt(tx.meta?.fee ?? 0);

      // A transfer typically only spends the fee. A swap spends/receives much more than the fee.
      // We use a small buffer (e.g., 0.001 SOL) to distinguish swaps from transfers.
      const SWAP_THRESHOLD_LAMPORTS = 1_000_000n; // 0.001 SOL
      const isSpendSol = solDelta < -(fee + SWAP_THRESHOLD_LAMPORTS);
      const isReceiveSol = solDelta > SWAP_THRESHOLD_LAMPORTS;

      const preBalances = tx.meta?.preTokenBalances ?? [];
      const postBalances = tx.meta?.postTokenBalances ?? [];
      
      // Find relevant token transfers for our mint and the bundler wallet
      for (const post of postBalances) {
        if (post.owner !== walletAddress || post.mint !== this.mint) continue;

        const before = preBalances.find(
          (pre) => pre.accountIndex === post.accountIndex && pre.mint === post.mint
        );
        
        const preAmount = BigInt(before?.uiTokenAmount.amount ?? '0');
        const postAmount = BigInt(post.uiTokenAmount.amount);
        const diff = postAmount - preAmount;
        const diffUi = Math.abs(Number(diff) / Math.pow(10, post.uiTokenAmount.decimals));

        if (diff > 0n) {
          // Token balance increased
          if (isSpendSol) {
            // Market BUY (Token UP, SOL DOWN)
            this.processBundlerBuy(walletAddress, {
              signature,
              mint: this.mint,
              tokenAmount: diffUi,
              slot: tx.slot,
              timestamp: tx.blockTime || Math.floor(Date.now() / 1000),
              walletAddress,
            });
          } else {
            log.info(`[BUNDLER IGNORE] Token increase detected for ${walletAddress} but no SOL spend (Transfer In).`);
          }
        } else if (diff < 0n) {
          // Token balance decreased
          if (isReceiveSol) {
            // Market SELL (Token DOWN, SOL UP)
            this.processBundlerSell(walletAddress, {
              signature,
              mint: this.mint,
              tokenAmount: post.uiTokenAmount.uiAmount || 0, // This will be overwritten by actualSoldAmount in processBundlerSell
              slot: tx.slot,
              timestamp: tx.blockTime || Math.floor(Date.now() / 1000),
              walletAddress,
            }, diffUi);
          } else {
            log.info(`[BUNDLER IGNORE] Token decrease detected for ${walletAddress} but no SOL receive (Transfer Out).`);
          }
        }
      }
    } catch (err) {
       log.error(`Error processing bundler signature ${signature}`, err);
     }
   }
 
   /**
    * Process a detected bundler buy transaction
   */
  processBundlerBuy(walletAddress: string, transaction: Omit<BundlerTransaction, 'type'>): void {
    if (this.positionId === null || !this.tradingWallet || !this.mint) {
      log.warn('Cannot process bundler buy - monitor not initialized');
      return;
    }

    const wallet = this.bundlerWallets.get(walletAddress);
    if (!wallet) {
      log.warn(`Cannot process buy for unknown bundler wallet: ${walletAddress}`);
      return;
    }

    log.info(`Processing bundler BUY from ${walletAddress}`, {
      mint: this.mint,
      tokenAmount: transaction.tokenAmount,
      signature: transaction.signature,
    });

    this.emit('bundlerBuy', {
      ...transaction,
      type: 'buy',
      walletAddress,
      tradingWallet: this.tradingWallet,
      positionId: this.positionId,
    });
  }

  /**
   * Process a detected bundler sell transaction
   */
  processBundlerSell(
    walletAddress: string,
    transaction: Omit<BundlerTransaction, 'type'>,
    actualSoldAmount: number
  ): void {
    if (this.positionId === null || !this.tradingWallet || !this.mint) {
      log.warn('Cannot process bundler sell - monitor not initialized');
      return;
    }

    const wallet = this.bundlerWallets.get(walletAddress);
    if (!wallet) {
      log.warn(`Cannot process sell for unknown bundler wallet: ${walletAddress}`);
      return;
    }

    // Update sold amount
    this.updateSoldAmount(walletAddress, actualSoldAmount);

    const walletData = this.bundlerWallets.get(walletAddress)!;
    const cumulativeSoldPercentage = (walletData.totalSoldAmount / walletData.initialTokenAmount) * 100;

    log.info(`Processing bundler SELL from ${walletAddress}`, {
      mint: this.mint,
      tokenAmount: actualSoldAmount,
      cumulativeSoldPercentage,
      signature: transaction.signature,
    });

    this.emit('bundlerSell', {
      ...transaction,
      type: 'sell',
      tokenAmount: actualSoldAmount,
      walletAddress,
      tradingWallet: this.tradingWallet,
      positionId: this.positionId,
      cumulativeSoldPercentage,
    });
  }
}
