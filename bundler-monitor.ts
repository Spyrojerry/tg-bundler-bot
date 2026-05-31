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
const F1_PROGRAM_ID = 'FJX4qJbmhQ7ou8a99LNJMB1QYaKq5bvbqwxbawiUwkD2';
const F1_SINGLE_TX_DELAY_MS = 150;

type ParsedTokenBalance = {
  accountIndex: number;
  mint: string;
  owner?: string;
  uiTokenAmount: {
    amount: string;
    decimals: number;
  };
};

type ParsedInstructionLike = {
  programId?: unknown;
  innerInstructions?: ParsedInstructionLike[];
};

export interface BundlerWallet {
  id: number;
  walletAddress: string;
  initialTokenAmount: number;
  totalSoldAmount: number;
  source?: 'early_bundler' | 'receiver';
  parentWalletAddress?: string;
}

export interface BundlerTransaction {
  signature: string;
  walletAddress: string;
  mint: string;
  tokenAmount: number;
  slot: number;
  timestamp: number;
  type: 'buy' | 'sell';
  source: 'early_bundler' | 'receiver';
  parentWalletAddress?: string;
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
    source: 'early_bundler' | 'receiver';
    parentWalletAddress?: string;
  }) => void;
  creatorVaultF1: (event: {
    creatorVaultAddress: string;
    mint: string;
    tradingWallet: string;
    positionId: number;
    signature: string;
    programId: string;
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
  private readonly receiverConnection: Connection;
  private readonly f1Connection: Connection;
  private readonly config: ServiceConfig;
  private readonly heliusClient: HeliusClient;
  private readonly receiverHeliusClient: HeliusClient;
  private positionId: number | null = null;
  private tradingWallet: string | null = null;
  private mint: string | null = null;
  private creatorVaultAddress: string | null = null;
  private bundlerWallets: Map<string, BundlerWallet> = new Map();
  private walletSources: Map<string, 'early_bundler' | 'receiver'> = new Map();
  private isRunning = false;
  private processedSignatures: Set<string> = new Set();
  private subscriptionIds: Map<string, { id: number; connection: Connection }> = new Map();
  private f1SignatureQueue: string[] = [];
  private f1QueueProcessing = false;

  constructor(config: ServiceConfig, heliusClient: HeliusClient) {
    super();
    this.config = config;
    this.heliusClient = heliusClient;
    this.receiverHeliusClient = new HeliusClient(config.receiverHeliusApiKey || config.heliusApiKey);
    this.connection = new Connection(config.solanaRpcUrl, {
      commitment: 'processed',
      wsEndpoint: config.solanaWsUrl,
    });
    this.receiverConnection = new Connection(config.receiverSolanaRpcUrl, {
      commitment: 'processed',
      wsEndpoint: config.receiverSolanaWsUrl,
    });
    this.f1Connection = new Connection(config.f1SolanaRpcUrl, {
      commitment: 'processed',
      wsEndpoint: config.f1SolanaWsUrl,
    });
  }

  /**
   * Start monitoring a set of early bundler wallets for a specific position
   */
  async startMonitoring(
    positionId: number,
    tradingWallet: string,
    mint: string,
    bundlerWallets: BundlerWallet[],
    creatorVaultAddress?: string
  ): Promise<void> {
    if (this.isRunning) {
      log.warn('BundlerMonitor already running, stopping previous monitoring');
      await this.stopMonitoring();
    }

    this.positionId = positionId;
    this.tradingWallet = tradingWallet;
    this.mint = mint;
    this.creatorVaultAddress = creatorVaultAddress ?? null;
    this.bundlerWallets.clear();
    this.walletSources.clear();

    for (const wallet of bundlerWallets) {
      const source = wallet.source ?? 'early_bundler';
      this.bundlerWallets.set(wallet.walletAddress, { ...wallet, source });
      this.walletSources.set(wallet.walletAddress, source);
    }

    this.isRunning = true;
    
    log.info(`Started monitoring ${bundlerWallets.length} early bundler wallets for position ${positionId}`, {
      tradingWallet,
      mint,
      walletAddresses: bundlerWallets.map(w => w.walletAddress),
    });

    // Start WebSocket monitoring for each bundler wallet
    this.startWsMonitoring();
    this.startCreatorVaultMonitoring();
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
    for (const [walletAddress, subscription] of this.subscriptionIds.entries()) {
      try {
        await subscription.connection.removeOnLogsListener(subscription.id);
        log.debug(`Unsubscribed from logs for ${walletAddress}`);
      } catch (err) {
        log.warn(`Failed to unsubscribe from logs for ${walletAddress}`, err);
      }
    }
    this.subscriptionIds.clear();
    this.f1SignatureQueue = [];
    this.f1QueueProcessing = false;

    log.info(`Stopped monitoring early bundler wallets for position ${this.positionId}`);

    this.positionId = null;
    this.tradingWallet = null;
    this.mint = null;
    this.creatorVaultAddress = null;
    this.bundlerWallets.clear();
    this.walletSources.clear();
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
          source: wallet.source ?? 'early_bundler',
          parentWalletAddress: wallet.parentWalletAddress,
        });
      }
    }
  }

  private startWsMonitoring(): void {
    if (!this.isRunning || !this.mint) return;

    for (const walletAddress of this.bundlerWallets.keys()) {
      this.subscribeWallet(walletAddress, 'early_bundler');
    }
  }

  private subscribeWallet(walletAddress: string, source: 'early_bundler' | 'receiver'): void {
    if (!this.isRunning || this.subscriptionIds.has(walletAddress)) return;

    try {
      const pubkey = new PublicKey(walletAddress);
      const connection = source === 'receiver' ? this.receiverConnection : this.connection;
      
      const subId = connection.onLogs(
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
      
      this.subscriptionIds.set(walletAddress, { id: subId, connection });
      log.debug(`Subscribed to logs for ${source} wallet ${walletAddress} (id=${subId})`);
    } catch (err) {
      log.warn(`Failed to subscribe to ${source} wallet ${walletAddress}`, err);
    }
  }

  private startCreatorVaultMonitoring(): void {
    if (!this.isRunning || !this.creatorVaultAddress) {
      log.warn('Creator vault F1 monitoring skipped: no creator vault address found for mint', {
        mint: this.mint,
      });
      return;
    }

    const subscriptionKey = `creator-vault:${this.creatorVaultAddress}`;
    if (this.subscriptionIds.has(subscriptionKey)) return;

    try {
      const pubkey = new PublicKey(this.creatorVaultAddress);
      const subId = this.f1Connection.onLogs(
        pubkey,
        (logInfo) => {
          if (logInfo.err) return;

          log.info(`[F1 VAULT WS TX] ${this.creatorVaultAddress?.slice(0, 8)}...: ${logInfo.signature}`);
          this.enqueueCreatorVaultSignature(logInfo.signature);
        },
        'processed'
      );

      this.subscriptionIds.set(subscriptionKey, { id: subId, connection: this.f1Connection });
      log.info('Creator vault F1 monitoring started', {
        creatorVaultAddress: this.creatorVaultAddress,
        mint: this.mint,
        f1RpcUrl: this.config.f1SolanaRpcUrl,
      });
    } catch (err) {
      log.warn(`Failed to subscribe to creator vault ${this.creatorVaultAddress}`, err);
    }
  }

  private enqueueCreatorVaultSignature(signature: string): void {
    const processedKey = `creator-vault:${signature}`;
    if (this.processedSignatures.has(processedKey) || this.f1SignatureQueue.includes(signature)) return;

    this.f1SignatureQueue.push(signature);
    if (!this.f1QueueProcessing) void this.drainCreatorVaultSignatureQueue();
  }

  private async drainCreatorVaultSignatureQueue(): Promise<void> {
    if (this.f1QueueProcessing) return;
    this.f1QueueProcessing = true;

    try {
      while (this.f1SignatureQueue.length > 0) {
        if (!this.isRunning || !this.creatorVaultAddress || !this.mint || !this.tradingWallet || this.positionId === null) {
          this.f1SignatureQueue = [];
          return;
        }

        const signature = this.f1SignatureQueue.shift();
        if (!signature) continue;

        await this.processCreatorVaultSignature(signature);
        if (this.f1SignatureQueue.length > 0) {
          await new Promise((resolve) => setTimeout(resolve, F1_SINGLE_TX_DELAY_MS));
        }
      }
    } finally {
      this.f1QueueProcessing = false;
      if (this.f1SignatureQueue.length > 0) void this.drainCreatorVaultSignatureQueue();
    }
  }

  private async processCreatorVaultSignature(signature: string): Promise<void> {
    if (!this.isRunning || !this.creatorVaultAddress || !this.mint || !this.tradingWallet || this.positionId === null) {
      return;
    }

    const processedKey = `creator-vault:${signature}`;
    if (this.processedSignatures.has(processedKey)) return;
    this.processedSignatures.add(processedKey);

    try {
      let tx = null;
      for (let attempt = 1; attempt <= 5; attempt++) {
        tx = await this.f1Connection.getParsedTransaction(signature, {
          commitment: 'confirmed',
          maxSupportedTransactionVersion: 0,
        });
        if (tx) break;
        await new Promise((resolve) => setTimeout(resolve, attempt * 250));
      }

      if (tx) this.processCreatorVaultTransaction(signature, tx);
    } catch (err) {
      log.error(`Error processing creator vault signature ${signature}`, err);
    }
  }

  private processCreatorVaultTransaction(
    signature: string,
    tx: NonNullable<Awaited<ReturnType<Connection['getParsedTransaction']>>>
  ): void {
    if (!this.creatorVaultAddress || !this.mint || !this.tradingWallet || this.positionId === null) return;

    if (!this.hasProgramId(tx.transaction.message.instructions, F1_PROGRAM_ID)) {
      const innerInstructions = tx.meta?.innerInstructions?.flatMap((inner) => inner.instructions) ?? [];
      if (!this.hasProgramId(innerInstructions, F1_PROGRAM_ID)) return;
    }

    log.info('[F1 PROGRAM DETECTED] Creator vault transaction matched F1 program', {
      creatorVaultAddress: this.creatorVaultAddress,
      mint: this.mint,
      signature,
      programId: F1_PROGRAM_ID,
    });

    this.emit('creatorVaultF1', {
      creatorVaultAddress: this.creatorVaultAddress,
      mint: this.mint,
      tradingWallet: this.tradingWallet,
      positionId: this.positionId,
      signature,
      programId: F1_PROGRAM_ID,
    });
  }

  private hasProgramId(instructions: readonly ParsedInstructionLike[], programId: string): boolean {
    for (const instruction of instructions) {
      const instructionProgramId = this.programIdToString(instruction.programId);
      if (instructionProgramId === programId) return true;
      if (instruction.innerInstructions && this.hasProgramId(instruction.innerInstructions, programId)) {
        return true;
      }
    }
    return false;
  }

  private programIdToString(programId: unknown): string | null {
    if (!programId) return null;
    if (typeof programId === 'string') return programId;
    if (typeof programId === 'object' && 'toBase58' in programId) {
      const value = (programId as { toBase58: () => string }).toBase58();
      return value;
    }
    return String(programId);
  }

  private async processSignature(signature: string, walletAddress: string): Promise<void> {
    if (!this.isRunning || this.processedSignatures.has(signature)) return;

    this.processedSignatures.add(signature);
    
    try {
      if (await this.processEnhancedSignature(signature, walletAddress)) return;

      // Retry a few times to get the transaction if it's not immediately available
      const source = this.walletSources.get(walletAddress) ?? 'early_bundler';
      const transactionConnection = source === 'receiver' ? this.receiverConnection : this.connection;
      let tx = null;
      for (let attempt = 1; attempt <= 5; attempt++) {
        tx = await transactionConnection.getParsedTransaction(signature, {
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

      const tokenDeltas = this.getTokenDeltas(tx.meta?.preTokenBalances ?? [], tx.meta?.postTokenBalances ?? []);
      const walletTokenDelta = tokenDeltas
        .filter((delta) => delta.owner === walletAddress)
        .reduce((sum, delta) => sum + delta.diff, 0n);
      const decimals = tokenDeltas.find((delta) => delta.owner === walletAddress)?.decimals
        ?? tokenDeltas[0]?.decimals
        ?? 0;
      const diffUi = Math.abs(Number(walletTokenDelta) / Math.pow(10, decimals));

      if (walletTokenDelta > 0n) {
        if (isSpendSol) {
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
      } else if (walletTokenDelta < 0n) {
        if (isReceiveSol) {
          this.processBundlerSell(walletAddress, {
            signature,
            mint: this.mint,
            tokenAmount: diffUi,
            slot: tx.slot,
            timestamp: tx.blockTime || Math.floor(Date.now() / 1000),
            walletAddress,
          }, diffUi);
        } else {
          log.info(`[BUNDLER TRANSFER OUT] ${source} ${walletAddress} sent ${diffUi} token(s)`, {
            signature,
            mint: this.mint,
          });
          if (source === 'early_bundler') {
            this.processTransferOut(walletAddress, tokenDeltas, signature, tx.slot, tx.blockTime || Math.floor(Date.now() / 1000));
          }
        }
      }
    } catch (err) {
       log.error(`Error processing bundler signature ${signature}`, err);
     }
   }

  private async processEnhancedSignature(signature: string, walletAddress: string): Promise<boolean> {
    if (!this.mint) return false;

    const source = this.walletSources.get(walletAddress) ?? 'early_bundler';
    const client = source === 'receiver' ? this.receiverHeliusClient : this.heliusClient;
    let enhancedTx: HeliusTransaction | undefined;

    try {
      const transactions = await client.getTransactionsBySignatures([signature]);
      enhancedTx = transactions[0];
    } catch (err) {
      log.debug(`Enhanced Helius fetch failed for ${signature}; falling back to parsed RPC`, err);
      return false;
    }

    if (!enhancedTx || enhancedTx.signature !== signature) return false;
    if (enhancedTx.type !== 'SWAP' && enhancedTx.type !== 'TRANSFER') return false;

    const transfers = (enhancedTx.tokenTransfers ?? []).filter((transfer) =>
      transfer.mint === this.mint
      && transfer.tokenAmount > 0
      && (transfer.fromUserAccount === walletAddress || transfer.toUserAccount === walletAddress)
    );

    if (transfers.length === 0) return true;

    const slot = enhancedTx.slot;
    const timestamp = enhancedTx.timestamp || Math.floor(Date.now() / 1000);

    if (enhancedTx.type === 'SWAP') {
      const boughtAmount = transfers
        .filter((transfer) => transfer.toUserAccount === walletAddress)
        .reduce((sum, transfer) => sum + transfer.tokenAmount, 0);
      const soldAmount = transfers
        .filter((transfer) => transfer.fromUserAccount === walletAddress)
        .reduce((sum, transfer) => sum + transfer.tokenAmount, 0);

      if (boughtAmount > 0) {
        this.processBundlerBuy(walletAddress, {
          signature,
          mint: this.mint,
          tokenAmount: boughtAmount,
          slot,
          timestamp,
          walletAddress,
        });
      }

      if (soldAmount > 0) {
        this.processBundlerSell(walletAddress, {
          signature,
          mint: this.mint,
          tokenAmount: soldAmount,
          slot,
          timestamp,
          walletAddress,
        }, soldAmount);
      }

      return true;
    }

    if (enhancedTx.type === 'TRANSFER' && source === 'early_bundler') {
      const receiverTransfers = transfers.filter((transfer) =>
        transfer.fromUserAccount === walletAddress
        && transfer.toUserAccount
        && transfer.toUserAccount !== walletAddress
        && transfer.toUserAccount !== this.tradingWallet
      );

      for (const transfer of receiverTransfers) {
        this.addReceiverWallet(
          walletAddress,
          transfer.toUserAccount,
          transfer.tokenAmount,
          signature,
          slot,
          timestamp
        );
      }

      return true;
    }

    log.info(`[BUNDLER IGNORE] ${source} ${walletAddress} ${enhancedTx.type} did not match buy/sell/receiver rules`, {
      signature,
      mint: this.mint,
    });
    return true;
  }

  private getTokenDeltas(
    preBalances: readonly ParsedTokenBalance[],
    postBalances: readonly ParsedTokenBalance[]
  ): Array<{ accountIndex: number; owner: string | undefined; diff: bigint; decimals: number }> {
    const byAccount = new Map<number, {
      accountIndex: number;
      owner: string | undefined;
      preAmount: bigint;
      postAmount: bigint;
      decimals: number;
    }>();

    for (const pre of preBalances ?? []) {
      if (pre.mint !== this.mint) continue;
      byAccount.set(pre.accountIndex, {
        accountIndex: pre.accountIndex,
        owner: pre.owner,
        preAmount: BigInt(pre.uiTokenAmount.amount),
        postAmount: 0n,
        decimals: pre.uiTokenAmount.decimals,
      });
    }

    for (const post of postBalances ?? []) {
      if (post.mint !== this.mint) continue;
      const existing = byAccount.get(post.accountIndex);
      if (existing) {
        existing.owner = post.owner ?? existing.owner;
        existing.postAmount = BigInt(post.uiTokenAmount.amount);
        existing.decimals = post.uiTokenAmount.decimals;
      } else {
        byAccount.set(post.accountIndex, {
          accountIndex: post.accountIndex,
          owner: post.owner,
          preAmount: 0n,
          postAmount: BigInt(post.uiTokenAmount.amount),
          decimals: post.uiTokenAmount.decimals,
        });
      }
    }

    return [...byAccount.values()]
      .map((entry) => ({
        accountIndex: entry.accountIndex,
        owner: entry.owner,
        diff: entry.postAmount - entry.preAmount,
        decimals: entry.decimals,
      }))
      .filter((entry) => entry.diff !== 0n);
  }

  private processTransferOut(
    senderWalletAddress: string,
    tokenDeltas: Array<{ owner: string | undefined; diff: bigint; decimals: number }>,
    signature: string,
    slot: number,
    timestamp: number
  ): void {
    if (!this.mint || !this.tradingWallet || this.positionId === null) return;

    const receivers = tokenDeltas.filter((delta) =>
      delta.diff > 0n
      && delta.owner
      && delta.owner !== senderWalletAddress
      && delta.owner !== this.tradingWallet
    );

    for (const receiver of receivers) {
      const receiverAddress = receiver.owner!;
      const receivedAmount = Number(receiver.diff) / Math.pow(10, receiver.decimals);
      this.addReceiverWallet(senderWalletAddress, receiverAddress, receivedAmount, signature, slot, timestamp);
    }
  }

  private addReceiverWallet(
    senderWalletAddress: string,
    receiverAddress: string,
    receivedAmount: number,
    signature: string,
    slot: number,
    timestamp: number
  ): void {
    if (!this.mint || receivedAmount <= 0) return;

    const existing = this.bundlerWallets.get(receiverAddress);
    if (existing) {
      existing.initialTokenAmount += receivedAmount;
      log.info(`[RECEIVER WALLET UPDATED] ${receiverAddress} received more ${this.mint}`, {
        senderWalletAddress,
        receivedAmount,
        initialTokenAmount: existing.initialTokenAmount,
        signature,
      });
      return;
    }

    const wallet: BundlerWallet = {
      id: -1,
      walletAddress: receiverAddress,
      initialTokenAmount: receivedAmount,
      totalSoldAmount: 0,
      source: 'receiver',
      parentWalletAddress: senderWalletAddress,
    };

    this.bundlerWallets.set(receiverAddress, wallet);
    this.walletSources.set(receiverAddress, 'receiver');
    this.subscribeWallet(receiverAddress, 'receiver');

    log.info(`[RECEIVER WALLET MONITORING STARTED] ${receiverAddress}`, {
      senderWalletAddress,
      mint: this.mint,
      receivedAmount,
      signature,
      slot,
      timestamp,
      receiverRpcUrl: this.config.receiverSolanaRpcUrl,
    });
  }
 
   /**
    * Process a detected bundler buy transaction
   */
  processBundlerBuy(walletAddress: string, transaction: Omit<BundlerTransaction, 'type' | 'source' | 'parentWalletAddress'>): void {
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
      source: wallet.source ?? 'early_bundler',
      parentWalletAddress: wallet.parentWalletAddress,
    });
  }

  /**
   * Process a detected bundler sell transaction
   */
  processBundlerSell(
    walletAddress: string,
    transaction: Omit<BundlerTransaction, 'type' | 'source' | 'parentWalletAddress'>,
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
      source: wallet.source ?? 'early_bundler',
      parentWalletAddress: wallet.parentWalletAddress,
    });
  }
}
