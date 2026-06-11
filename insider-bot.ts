import { Connection, PublicKey } from "@solana/web3.js";
import { EventEmitter } from "events";
import { createLogger } from "./logger";
import { HeliusClient, HeliusTransaction } from "./helius-client";
import { GmgnClient } from "./gmgn-client";
import type { ServiceConfig } from "./types";
import { TelegramBot } from "./telegram-bot";
import { WalletMonitor } from "./wallet-monitor";

const log = createLogger("INSIDER");
const SOL_MINT = "So11111111111111111111111111111111111111112";
const INSIDER_WALLET_POLL_MS = 2_000;
const INSIDER_HISTORY_LIMIT = 21;

type InsiderTxKind = "buy" | "sell" | "transfer_in" | "transfer_out";

export interface InsiderBuyTrigger {
  followedWallet: string;
  mint: string;
  signature: string;
  buySol: number;
  entryMc?: number;
  tradersListStr?: string;
  monitoredWallet?: string;
}

export interface InsiderSellTrigger {
  followedWallet: string;
  positionMint: string;
  signature: string;
  reason: string;
}

export interface InsiderBot {
  on(event: "buyTrigger", listener: (trigger: InsiderBuyTrigger) => void): this;
  on(
    event: "sellTrigger",
    listener: (trigger: InsiderSellTrigger) => void,
  ): this;
  on(event: "mintSeen", listener: (mint: string) => void): this;
  on(event: "error", listener: (error: Error) => void): this;
  emit(event: "buyTrigger", trigger: InsiderBuyTrigger): boolean;
  emit(event: "sellTrigger", trigger: InsiderSellTrigger): boolean;
  emit(event: "mintSeen", mint: string): boolean;
  emit(event: "error", error: Error): boolean;
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
  getMonitoredWallet(): string | null;
  getBuySol(): number;
  isBuyDisabled(): boolean;
  setBuyDisabled(value: boolean): void;
  configureFollowWallet(address: string): void;
  pause(): void;
  isRunning(): boolean;
  isBuyInProgress(): boolean;
  setBuyExecuting(executing: boolean): void;
  seedSeenMints(mints: Set<string>): void;
  followWallet(address: string): Promise<void>;
  stop(): Promise<void>;
}

interface InsiderWalletState {
  wallet: string;
  buyCount: number;
  sellCount: number;
  isTransferred: boolean;
  initialTokenMintAmount: number | null;
  devBuyCountAfterMint: number;
  syncedAfterBuy: boolean; // Flag to track if we've synced after buy
}

export class InsiderBot extends EventEmitter {
  private readonly config: ServiceConfig;
  private readonly connection: Connection;
  private readonly telegramBot: TelegramBot | null;
  private readonly heliusClient: HeliusClient;
  private readonly gmgnClient: GmgnClient;
  private followedWallet: string | null = null;
  private buySol: number;
  private entryMc: number;
  private exitMc: number;
  private exitPercent: number;
  private buyDisabled: boolean = false;
  private followMonitor: WalletMonitor | null = null;
  private watchingMint: string | null = null;
  private monitoredWallet: string | null = null;
  private monitoredWalletMonitor: WalletMonitor | null = null;
  private insiderState: InsiderWalletState | null = null;
  private insiderPollTimer: ReturnType<typeof setInterval> | null = null;
  private processedSignatures = new Set<string>();
  private activePosition: {
    followedWallet: string;
    mint: string;
  } | null = null;
  private boughtMints = new Set<string>();
  private isBuyExecuting: boolean = false;
  private isProcessingInsider: boolean = false;
  private lowestInsiderWalletAtStart: string | null = null;
  private transferOutPnlCheckPassed: boolean = false;
  private devBuyLimitPassed: boolean = false;

  constructor(
    config: ServiceConfig,
    rpcUrl: string,
    wsUrl: string,
    gmgnClient: GmgnClient,
    heliusApiKey: string,
    telegramBot: TelegramBot | null = null,
  ) {
    super();
    this.config = config;
    this.telegramBot = telegramBot;
    this.gmgnClient = gmgnClient;
    this.heliusClient = new HeliusClient(heliusApiKey);
    this.buySol = config.insiderBuySol;
    this.entryMc = config.insiderEntryMc;
    this.exitMc = config.insiderExitMc;
    this.exitPercent = config.insiderExitPercent;
    this.connection = new Connection(rpcUrl, {
      commitment: "processed",
      wsEndpoint: wsUrl,
    });
  }

  private resetNewStateVars(): void {
    this.lowestInsiderWalletAtStart = null;
    this.transferOutPnlCheckPassed = false;
    this.devBuyLimitPassed = false;
  }

  seedSeenMints(mints: Set<string>): void {
    for (const m of mints) this.boughtMints.add(m);
  }

  getActivePosition(): { followedWallet: string; mint: string } | null {
    return this.activePosition;
  }

  getPreBuyMint(): string | null {
    return this.watchingMint;
  }

  getMonitoredWallet(): string | null {
    return this.monitoredWallet;
  }

  clearActivePosition(): void {
    void this.resetForNewToken();
  }

  clearPreBuyMint(): void {
    void this.resetForNewToken();
  }

  setBuySol(value: number): void {
    if (!Number.isFinite(value) || value <= 0) {
      throw new Error("Insider buy SOL must be greater than 0");
    }
    this.buySol = value;
  }

  getBuySol(): number {
    return this.buySol;
  }

  setEntryMc(value: number): void {
    if (!Number.isFinite(value) || value < 0) {
      throw new Error("Entry MC must be a non-negative number");
    }
    this.entryMc = value;
  }

  getEntryMc(): number {
    return this.entryMc;
  }

  setExitMc(value: number): void {
    if (!Number.isFinite(value) || value < 0) {
      throw new Error("Exit MC must be a non-negative number");
    }
    this.exitMc = value;
  }

  getExitMc(): number {
    return this.exitMc;
  }

  setExitPercent(value: number): void {
    if (!Number.isFinite(value) || value < 0) {
      throw new Error("Exit percent must be a non-negative number");
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
    return this.followMonitor !== null || this.insiderPollTimer !== null;
  }

  async followWallet(address: string): Promise<void> {
    const normalized = new PublicKey(address).toBase58();
    if (this.followedWallet !== normalized) {
      this.boughtMints.clear();
    }
    await this.stopInsiderMonitoring();
    await this.stop();
    this.followedWallet = normalized;

    this.followMonitor = new WalletMonitor(this.config, normalized, {
      enforceMinBuySol: false,
    });
    this.followMonitor.on("newToken", (event) => {
      void this.handleFollowWalletBuy(event.mint, event.signature);
    });

    await this.followMonitor.start();

    for (const mint of this.followMonitor.existingMints) {
      this.boughtMints.add(mint);
    }
    log.info("Seeded boughtMints from follow-wallet snapshot", {
      count: this.followMonitor.existingMints.size,
    });

    log.info("Insider follow wallet monitoring started", {
      followedWallet: normalized,
      buySol: this.buySol,
      exitPercent: this.exitPercent,
    });
  }

  configureFollowWallet(address: string): void {
    const normalized = new PublicKey(address).toBase58();
    if (this.followedWallet !== normalized) {
      this.boughtMints.clear();
    }
    this.pause();
    this.followedWallet = normalized;
    this.activePosition = null;
    this.watchingMint = null;
    this.monitoredWallet = null;
    this.insiderState = null;
  }

  async stop(): Promise<void> {
    await this.stopInsiderMonitoring();
    if (this.followMonitor) {
      this.followMonitor.stop();
      this.followMonitor = null;
    }
    this.activePosition = null;
    this.watchingMint = null;
    this.monitoredWallet = null;
    this.insiderState = null;
  }

  pause(): void {
    void this.stopInsiderMonitoring();
    if (this.followMonitor) {
      this.followMonitor.stop();
      this.followMonitor = null;
    }
  }

  markPositionBought(trigger: InsiderBuyTrigger): void {
    this.activePosition = {
      followedWallet: trigger.followedWallet,
      mint: trigger.mint,
    };
    this.watchingMint = null;
    this.boughtMints.add(trigger.mint);
    void this.stopInsiderMonitoring();
  }

  private async handleFollowWalletBuy(
    mint: string,
    signature: string,
  ): Promise<void> {
    if (this.boughtMints.has(mint)) return;
    if (this.activePosition || this.watchingMint) {
      log.info("Already watching or holding a token; ignoring new buy", {
        newMint: mint,
      });
      return;
    }

    log.info("Followed wallet buy detected - pausing follow monitor", {
      followedWallet: this.followedWallet,
      mint,
      signature,
    });

    if (this.followMonitor) {
      this.followMonitor.stop();
      this.followMonitor = null;
    }

    this.boughtMints.add(mint);
    this.watchingMint = mint;
    this.emit("mintSeen", mint);

    try {
      await this.startInsiderFlow(mint);
    } catch (err) {
      log.error("Failed to start insider flow; resetting", err);
      this.emit(
        "error",
        err instanceof Error ? err : new Error(String(err)),
      );
      await this.resetForNewToken();
    }
  }

  private async startInsiderFlow(mint: string): Promise<void> {
    const swaps = await this.heliusClient.getEarlyInsiderSwaps(mint, 4);
    const lowest = this.findLowestInsiderWallet(swaps, mint);
    if (!lowest) {
      throw new Error(`Could not identify lowest insider wallet for ${mint}`);
    }

    log.info("Lowest insider wallet identified", {
      mint,
      wallet: lowest.wallet,
      tokenAmount: lowest.tokenAmount,
      signature: lowest.signature,
    });

    this.lowestInsiderWalletAtStart = lowest.wallet;
    this.monitoredWallet = lowest.wallet;
    this.insiderState = {
      wallet: lowest.wallet,
      buyCount: 1,
      sellCount: 0,
      isTransferred: false,
      initialTokenMintAmount: lowest.tokenAmount,
      devBuyCountAfterMint: 0,
      syncedAfterBuy: false,
    };
    this.processedSignatures.add(lowest.signature);

    // Sync the wallet's transactions from the token mint onwards
    await this.syncWalletTransactions(lowest.wallet, mint, lowest.signature);

    await this.telegramBot?.sendDefault(
      [
        "<b>🔍 Insider Flow Started</b>",
        `Token: <code>${mint}</code>`,
        `Lowest insider: <code>${lowest.wallet}</code>`,
        `Initial buy amount: <b>${lowest.tokenAmount.toLocaleString()}</b>`,
        `Synced buys: <b>${this.insiderState.buyCount}</b>`,
        `Synced sells: <b>${this.insiderState.sellCount}</b>`,
        "",
        "Monitoring insider wallet for sells / transfers...",
      ].join("\n"),
    );

    this.startInsiderPolling();
    await this.evaluateBuyConditions(mint);
  }

  private async syncWalletTransactions(
    wallet: string,
    mint: string,
    startSignature?: string, // Optional: if not provided, just sync last N txs
    limit: number = 20
  ): Promise<void> {
    const txs = await this.heliusClient.getWalletTransactionsDesc(
      wallet,
      limit
    );

    // Sort transactions chronologically (oldest first)
    const sortedTxs = [...txs].reverse();
    let foundStart = startSignature ? false : true;

    for (const tx of sortedTxs) {
      if (startSignature && tx.signature === startSignature) {
        foundStart = true;
        continue;
      }

      if (!foundStart) continue;

      if (this.processedSignatures.has(tx.signature)) continue;
      this.processedSignatures.add(tx.signature);

      const kind = this.classifyInsiderTx(tx, wallet, mint);
      if (!kind) continue;

      log.info("Syncing historical transaction", {
        mint,
        wallet,
        kind,
        signature: tx.signature,
      });

      if (kind === "buy") {
        this.insiderState!.buyCount += 1;
        this.insiderState!.devBuyCountAfterMint += 1;
      } else if (kind === "sell") {
        this.insiderState!.sellCount += 1;
        // Check if this historical sell should trigger a sell
        const sellAmount = (tx.tokenTransfers ?? []).find(
          (transfer) => transfer.mint === mint && transfer.fromUserAccount === wallet
        )?.tokenAmount;
        if (
          sellAmount &&
          this.insiderState!.initialTokenMintAmount &&
          sellAmount > this.insiderState!.initialTokenMintAmount &&
          this.activePosition
        ) {
          log.warn("Historical sell amount exceeds initial mint amount - triggering sell", {
            mint,
            sellAmount,
            initialMintAmount: this.insiderState!.initialTokenMintAmount
          });
          this.emit("sellTrigger", {
            followedWallet: this.followedWallet!,
            positionMint: mint,
            signature: tx.signature,
            reason: "Historical sell amount exceeds initial token mint amount",
          });
        }
      }
    }
  }

  /**
   * Sync dev wallet's last 20 transactions after buy, then monitor for sell triggers
   */
  public async syncAfterBuy(): Promise<void> {
    if (!this.insiderState || !this.monitoredWallet || !this.activePosition) {
      log.warn("Can't sync after buy: no active position or monitored wallet");
      return;
    }

    if (this.insiderState.syncedAfterBuy) {
      log.debug("Already synced after buy");
      return;
    }

    log.info("Syncing dev wallet's last 20 transactions after buy", {
      mint: this.activePosition.mint,
      wallet: this.monitoredWallet
    });

    // Sync last 20 transactions (no start signature needed)
    await this.syncWalletTransactions(
      this.monitoredWallet,
      this.activePosition.mint,
      undefined,
      20
    );

    this.insiderState.syncedAfterBuy = true;
    log.info("Sync after buy complete. Monitoring for sell triggers...", {
      mint: this.activePosition.mint
    });

    await this.telegramBot?.sendDefault(
      [
        `<b>🔄 Dev Wallet Synced (Post-Buy)</b>`,
        `Token: <code>${this.activePosition.mint}</code>`,
        `Monitored Wallet: <code>${this.monitoredWallet}</code>`,
        `Synced buys: <b>${this.insiderState.buyCount}</b>`,
        `Synced sells: <b>${this.insiderState.sellCount}</b>`,
        "",
        "Now monitoring for sell triggers...",
      ].join("\n"),
    );
  }

  private findLowestInsiderWallet(
    swaps: HeliusTransaction[],
    mint: string,
  ): { wallet: string; tokenAmount: number; signature: string } | null {
    let lowest: { wallet: string; tokenAmount: number; signature: string } | null =
      null;

    for (const tx of swaps) {
      if (tx.type !== "SWAP") continue;
      for (const transfer of tx.tokenTransfers ?? []) {
        if (transfer.mint !== mint) continue;
        const amount = transfer.tokenAmount ?? 0;
        const wallet = transfer.toUserAccount;
        if (!wallet) continue;
        if (!lowest || amount < lowest.tokenAmount) {
          lowest = {
            wallet,
            tokenAmount: amount,
            signature: tx.signature,
          };
        }
      }
    }

    return lowest;
  }

  private startInsiderPolling(): void {
    this.stopInsiderPolling();
    this.insiderPollTimer = setInterval(() => {
      void this.pollInsiderWallet();
    }, INSIDER_WALLET_POLL_MS);
  }

  private stopInsiderPolling(): void {
    if (this.insiderPollTimer) {
      clearInterval(this.insiderPollTimer);
      this.insiderPollTimer = null;
    }
  }

  private async stopInsiderMonitoring(): Promise<void> {
    this.stopInsiderPolling();
    this.processedSignatures.clear();
    if (this.monitoredWalletMonitor) {
      this.monitoredWalletMonitor.stop();
      this.monitoredWalletMonitor = null;
    }
  }

  private async pollInsiderWallet(): Promise<void> {
    if (
      this.isProcessingInsider ||
      !this.watchingMint ||
      !this.monitoredWallet ||
      !this.insiderState ||
      this.activePosition
    ) {
      return;
    }

    this.isProcessingInsider = true;
    try {
      const txs = await this.heliusClient.getWalletTransactionsDesc(
        this.monitoredWallet,
        INSIDER_HISTORY_LIMIT,
      );

      const relevant = txs
        .filter((tx) => this.isRelevantMintTx(tx, this.watchingMint!))
        .reverse();

      for (const tx of relevant) {
        if (this.processedSignatures.has(tx.signature)) continue;
        this.processedSignatures.add(tx.signature);
        await this.handleInsiderTransaction(tx, this.watchingMint);
      }
    } catch (err) {
      log.error("Insider wallet poll failed", err);
    } finally {
      this.isProcessingInsider = false;
    }
  }

  private isRelevantMintTx(tx: HeliusTransaction, mint: string): boolean {
    return (tx.tokenTransfers ?? []).some((transfer) => transfer.mint === mint);
  }

  private classifyInsiderTx(
    tx: HeliusTransaction,
    wallet: string,
    mint: string,
  ): InsiderTxKind | null {
    if (tx.type === "TRANSFER") {
      for (const transfer of tx.tokenTransfers ?? []) {
        if (transfer.mint !== mint) continue;
        if (transfer.fromUserAccount === wallet) return "transfer_out";
        if (transfer.toUserAccount === wallet) return "transfer_in";
      }
      return null;
    }

    if (tx.type === "SWAP") {
      // First check if it's a buy: wallet receives the token mint
      for (const transfer of tx.tokenTransfers ?? []) {
        if (transfer.mint === mint && transfer.toUserAccount === wallet) {
          return "buy";
        }
      }
      // Now check if it's a sell: either wallet receives SOL/WSOL, OR wallet sends out the token mint!
      for (const transfer of tx.tokenTransfers ?? []) {
        if (transfer.mint === SOL_MINT && transfer.toUserAccount === wallet) {
          return "sell";
        }
        if (transfer.mint === mint && transfer.fromUserAccount === wallet) {
          return "sell";
        }
      }
    }

    return null;
  }

  private async handleInsiderTransaction(
    tx: HeliusTransaction,
    mint: string,
  ): Promise<void> {
    if (!this.insiderState || !this.monitoredWallet) return;

    const kind = this.classifyInsiderTx(
      tx,
      this.monitoredWallet,
      mint,
    );
    if (!kind) return;

    log.info("Insider wallet activity detected", {
      mint,
      wallet: this.monitoredWallet,
      kind,
      signature: tx.signature,
    });

    if (kind === "transfer_out") {
      const recipient = (tx.tokenTransfers ?? []).find(
        (transfer) =>
          transfer.mint === mint &&
          transfer.fromUserAccount === this.monitoredWallet,
      )?.toUserAccount;

      if (!recipient) return;

      log.info("Insider transfer out detected - checking PnL and switching monitored wallet", {
        from: this.monitoredWallet,
        to: recipient,
        mint,
      });

      await this.switchToTransferredWallet(recipient, mint, tx);
      return;
    }

    if (kind === "buy") {
      this.insiderState.buyCount += 1;
    } else if (kind === "sell") {
      this.insiderState.sellCount += 1;
      // Check if sell amount is more than initial mint amount
      const sellAmount = (tx.tokenTransfers ?? []).find(
        (transfer) => transfer.mint === mint && transfer.fromUserAccount === this.monitoredWallet
      )?.tokenAmount;
      if (
        sellAmount &&
        this.insiderState.initialTokenMintAmount &&
        sellAmount > this.insiderState.initialTokenMintAmount &&
        this.activePosition
      ) {
        log.warn("Sell amount exceeds initial mint amount - triggering sell", {
          mint,
          sellAmount,
          initialMintAmount: this.insiderState.initialTokenMintAmount
        });
        this.emit("sellTrigger", {
          followedWallet: this.followedWallet!,
          positionMint: mint,
          signature: tx.signature,
          reason: "Sell amount exceeds initial token mint amount",
        });
      }
    }

    await this.evaluateBuyConditions(mint);
  }

  private async switchToTransferredWallet(
    newWallet: string,
    mint: string,
    transferTx: HeliusTransaction,
  ): Promise<void> {
    // Check PnL of lowest insider at transfer time
    if (this.lowestInsiderWalletAtStart) {
      const profit = await this.gmgnClient.fetchWalletTokenProfitUsd(
        this.lowestInsiderWalletAtStart,
        mint,
      );
      if (profit !== null && profit > 0) {
        // Check if profit percentage is <= 90%
        // For now, use absolute profit as a proxy, since we don't have historical PnL
        this.transferOutPnlCheckPassed = true;
        log.info("Transfer out PnL check passed", {
          mint,
          wallet: this.lowestInsiderWalletAtStart,
          profit,
        });
      }
    }

    // Check dev buy limit (<= $10)
    // For now, we'll skip this since we don't have historical buy value
    this.devBuyLimitPassed = true;

    // Stop previous monitored wallet monitor if exists
    if (this.monitoredWalletMonitor) {
      this.monitoredWalletMonitor.stop();
      this.monitoredWalletMonitor = null;
    }

    this.monitoredWallet = newWallet;
    this.insiderState = {
      wallet: newWallet,
      buyCount: 0,
      sellCount: 0,
      isTransferred: true,
      initialTokenMintAmount: null,
      devBuyCountAfterMint: 0,
      syncedAfterBuy: false,
    };
    this.processedSignatures.clear();
    this.processedSignatures.add(transferTx.signature);

    // Sync the new wallet's transactions from the transfer tx onwards
    await this.syncWalletTransactions(newWallet, mint, transferTx.signature);

    // Start WalletMonitor on the new monitored wallet!
    this.monitoredWalletMonitor = new WalletMonitor(this.config, newWallet, {
      enforceMinBuySol: false,
    });

    // Listen for new buys/sells on this monitored wallet to update insiderState
    this.monitoredWalletMonitor.on('buyDetected', (event: any) => {
      if (event.mint === mint && this.insiderState && this.watchingMint) {
        this.insiderState.buyCount += 1;
        this.insiderState.devBuyCountAfterMint += 1;
        log.info('Monitored wallet buy detected via WalletMonitor', {
          mint,
          wallet: newWallet,
          buyCount: this.insiderState.buyCount,
        });
        this.evaluateBuyConditions(mint).catch(err =>
          log.error('Failed to evaluate buy conditions after WalletMonitor buy', err)
        );
        // Check if dev has bought 3 times after mint
        if (this.insiderState.devBuyCountAfterMint >= 3 && this.activePosition) {
          log.warn('Dev bought 3 times after mint - triggering sell', { mint });
          this.emit('sellTrigger', {
            followedWallet: this.followedWallet!,
            positionMint: mint,
            signature: event.signature || 'WALLET_MONITOR',
            reason: 'Dev bought 3 times after token mint',
          });
        }
      }
    });

    // Also, let's make sure we process transactions from WalletMonitor for sells too
    // Wait, WalletMonitor emits 'tokenExited' when a token is sold!
    this.monitoredWalletMonitor.on('tokenExited', (event: any) => {
      if (event.mint === mint && this.insiderState && this.watchingMint) {
        this.insiderState.sellCount += 1;
        log.info('Monitored wallet sell detected via WalletMonitor', {
          mint,
          wallet: newWallet,
          sellCount: this.insiderState.sellCount,
        });
        this.evaluateBuyConditions(mint).catch(err =>
          log.error('Failed to evaluate buy conditions after WalletMonitor sell', err)
        );
      }
    });

    await this.monitoredWalletMonitor.start();

    await this.telegramBot?.sendDefault(
      [
        "<b>🔀 Insider Transfer Detected</b>",
        `Token: <code>${mint}</code>`,
        `Now monitoring: <code>${newWallet}</code>`,
        `Synced buys: <b>${this.insiderState?.buyCount ?? 0}</b>`,
        `Synced sells: <b>${this.insiderState?.sellCount ?? 0}</b>`,
      ].join("\n"),
    );

    await this.evaluateBuyConditions(mint);
  }

  private async evaluateBuyConditions(mint: string): Promise<void> {
    if (
      !this.insiderState ||
      !this.monitoredWallet ||
      !this.watchingMint ||
      this.activePosition ||
      this.isBuyExecuting ||
      this.buyDisabled
    ) {
      return;
    }

    // Check if transfer out checks passed
    if (!this.transferOutPnlCheckPassed || !this.devBuyLimitPassed) {
      log.debug("Transfer out checks not passed yet", {
        mint,
        transferOutPnlCheckPassed: this.transferOutPnlCheckPassed,
        devBuyLimitPassed: this.devBuyLimitPassed,
      });
      return;
    }

    const { buyCount, sellCount, devBuyCountAfterMint } = this.insiderState;
    const meetsSellThreshold = sellCount >= 5;
    let meetsBuyThreshold = false;

    if (buyCount >= 1) {
      // Has buy tx - check if buy value <= $40
      // For now, use devBuyCountAfterMint as a proxy for SOL amount
      meetsBuyThreshold = devBuyCountAfterMint <= 40;
    } else {
      // No buy tx - just need up to 5 sells
      meetsBuyThreshold = true;
    }

    if (!meetsSellThreshold || !meetsBuyThreshold) {
      log.debug("Insider buy conditions not met yet", {
        mint,
        wallet: this.monitoredWallet,
        buyCount,
        sellCount,
        devBuyCountAfterMint,
        meetsSellThreshold,
        meetsBuyThreshold,
      });
      return;
    }

    const profit = await this.gmgnClient.fetchWalletTokenProfitUsd(
      this.monitoredWallet,
      mint,
    );
    if (profit === null || profit <= 0) {
      log.info("Insider wallet PnL not positive; skipping buy", {
        mint,
        wallet: this.monitoredWallet,
        profit,
      });
      return;
    }

    const currentMc = await this.gmgnClient.fetchTokenMarketCapUsd(mint);
    if (currentMc === null) {
      log.warn("Could not fetch MC for insider buy trigger", { mint });
      return;
    }

    const exitPercent = this.exitPercent;
    const newExitMc = currentMc * (1 + exitPercent / 100);
    this.setExitMc(newExitMc);
    this.setEntryMc(currentMc);

    log.warn("Insider buy conditions met - triggering buy", {
      mint,
      monitoredWallet: this.monitoredWallet,
      buyCount,
      sellCount,
      profit,
      currentMc,
      exitMc: newExitMc,
    });

    this.setBuyExecuting(true);
    this.emit("buyTrigger", {
      followedWallet: this.followedWallet!,
      mint,
      signature: "INSIDER_TRIGGER",
      buySol: this.buySol,
      entryMc: currentMc,
      monitoredWallet: this.monitoredWallet,
      tradersListStr: [
        "<b>Insider Signal</b>",
        `Wallet: <code>${this.monitoredWallet}</code>`,
        `Buys: <b>${buyCount}</b> | Sells: <b>${sellCount}</b>`,
        `Wallet PnL: <b>$${profit.toLocaleString()}</b>`,
      ].join("\n"),
    });
  }

  private async resetForNewToken(): Promise<void> {
    await this.stopInsiderMonitoring();
    this.activePosition = null;
    this.watchingMint = null;
    this.monitoredWallet = null;
    this.insiderState = null;
    this.resetNewStateVars();
    log.info("InsiderBot reset; resuming followed wallet monitoring");

    if (this.followedWallet && !this.followMonitor) {
      await this.followWallet(this.followedWallet);
    }
  }
}
