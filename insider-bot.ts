import { Connection, PublicKey } from "@solana/web3.js";
import { EventEmitter } from "events";
import { getAssociatedTokenAddressSync } from "@solana/spl-token";
import { createLogger } from "./logger";
import { HeliusClient, HeliusTransaction } from "./helius-client";
import { GmgnClient } from "./gmgn-client";
import type { ServiceConfig } from "./types";
import { TelegramBot } from "./telegram-bot";
import { WalletMonitor } from "./wallet-monitor";

const log = createLogger("INSIDER");
const SOL_MINT = "So11111111111111111111111111111111111111112";
const POLL_MS = 2_000;
const INSIDER_HISTORY_LIMIT = 21;
const REQUIRED_BUNDLER_MATCHES = 2;
const AXIOM_TRADER_SCAN_LIMIT = 50;
const AXIOM_EXIT_VALID_WALLET_THRESHOLD = 10;
const AXIOM_EXIT_SOLD_WALLET_THRESHOLD = 5;

type InsiderTxKind = "buy" | "sell" | "transfer_in" | "transfer_out";
type FlowPhase = "pre_buy" | "holding";

class InsiderMinBuySolFilterError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "InsiderMinBuySolFilterError";
  }
}

export type InsiderMintClaimFn = (mint: string) => boolean;
export type InsiderMintReleaseFn = (mint: string) => void;

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
  getBundlerBuyMinUsd(): number;
  setBundlerBuyMinUsd(value: number): void;
  getBundlerBuyMaxUsd(): number;
  setBundlerBuyMaxUsd(value: number): void;
  getRequiredInsiderSells(): number;
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
  resetBuyAttempt(): void;
  seedSeenMints(mints: Set<string>): void;
  followWallet(address: string): Promise<void>;
  stop(): Promise<void>;
}

interface InsiderWalletState {
  wallet: string;
  sellCount: number;
  isTransferred: boolean;
}

interface BundlerMatch {
  address: string;
  buyUsd: number;
  buyTxCount: number;
}

interface BundlerWatchState {
  wallets: string[];
  sellCounts: Map<string, number>;
}

type BundlerMatchType = "single_buy" | "multi_buy";

interface EarlyInsiderBuy {
  wallet: string;
  tokenAmount: number;
  signature: string;
  buySol: number | null;
}

interface AxiomWatchedWallet {
  address: string;
  buyUsd: number;
  tags: string[];
  ata: PublicKey;
}

export class InsiderBot extends EventEmitter {
   private readonly config: ServiceConfig;
  private readonly connection: Connection;
  private readonly telegramBot: TelegramBot | null;
  private readonly heliusClient: HeliusClient;
  private readonly gmgnClient: GmgnClient;
  private readonly bundlerGmgnClient: GmgnClient;
  /** GMGN_API_KEY_3 — pre-buy axiom/empty single-buy scan only. */
  private readonly preBuyAxiomGmgnClient: GmgnClient;
  private readonly claimMint: InsiderMintClaimFn | null;
  private readonly releaseMint: InsiderMintReleaseFn | null;
  private readonly rpcUrl: string;
  private readonly wsUrl: string;
  private readonly label: string;
  

  private followedWallet: string | null = null;
  private buySol: number;
  private entryMc: number;
  private exitMc: number;
  private exitPercent: number;
  private bundlerBuyMinUsd: number;
  private bundlerBuyMaxUsd: number;
  private requiredInsiderSells: number;
  private buyDisabled = false;

  private followMonitor: WalletMonitor | null = null;
  private watchingMint: string | null = null;
  private phase: FlowPhase | null = null;

  private monitoredWallet: string | null = null;
  private insiderState: InsiderWalletState | null = null;
  private bundlerWatch: BundlerWatchState | null = null;
  private matchedBundlers: BundlerMatch[] = [];
  /** First-seen single-buy bundlers locked at discovery (snapshot frozen). */
  private accumulatedSingleBuyBundlers: BundlerMatch[] = [];
  /** First-seen multi-buy bundlers locked at discovery (snapshot frozen). */
  private accumulatedMultiBuyBundlers: BundlerMatch[] = [];
  private bundlerMatchType: BundlerMatchType | null = null;
  /** Wallets from the first 4 early SWAP buys — fixed at flow start for trader-scan exclusions. */
  private initialInsiderWallets = new Set<string>();
  /** Token dev wallet (CREATE tx fee payer) — fixed at flow start for trader-scan exclusions. */
  private devWallet: string | null = null;
  private axiomTraderWatchActive = false;
  private axiomWatchedWallets = new Map<string, AxiomWatchedWallet>();
  private preBuyStopped = false;
  private positionSellTriggered = false;
  private insiderSellsReady = false;
  private bundlerMatchesReady = false;

  private tokenBuyCount = 0;
  private tokenSellCount = 0;

  private insiderLogsSubId: number | null = null;
  private bundlerLogsSubIds = new Map<string, number>();

  private processedSignatures = new Set<string>();
  private pendingSignaturesBatch: string[] = [];
  private batchTimer: ReturnType<typeof setTimeout> | null = null;
  private pollTimer: ReturnType<typeof setInterval> | null = null;

  private readonly BATCH_WINDOW_MS = 1000;
  private readonly MAX_BATCH_SIZE = 100;

  private activePosition: { followedWallet: string; mint: string } | null = null;
  private boughtMints = new Set<string>();
  private claimedMint: string | null = null;
  private buySubmitted = false;
  private isBuyExecuting = false;
  private isProcessing = false;

  constructor(
    config: ServiceConfig,
    rpcUrl: string,
    wsUrl: string,
    gmgnClient: GmgnClient,
    bundlerGmgnClient: GmgnClient,
    preBuyAxiomGmgnClient: GmgnClient,
    heliusApiKey: string,
    telegramBot: TelegramBot | null = null,
    claimMint: InsiderMintClaimFn | null = null,
    releaseMint: InsiderMintReleaseFn | null = null,
    label: string = "Insider",
  ) {
    super();
    this.config = config;
    this.rpcUrl = rpcUrl;
    this.wsUrl = wsUrl;
    this.telegramBot = telegramBot;
    this.gmgnClient = gmgnClient;
    this.bundlerGmgnClient = bundlerGmgnClient;
    this.preBuyAxiomGmgnClient = preBuyAxiomGmgnClient;
    this.heliusClient = new HeliusClient(heliusApiKey);
    this.claimMint = claimMint;
    this.releaseMint = releaseMint;
    this.label = label;
    this.buySol = config.insiderBuySol;
    this.entryMc = config.insiderEntryMc;
    this.exitMc = config.insiderExitMc;
    this.exitPercent = config.insiderExitPercent;
    this.bundlerBuyMinUsd = config.insiderBundlerBuyMinUsd;
    this.bundlerBuyMaxUsd = config.insiderBundlerBuyMaxUsd;
    this.requiredInsiderSells = config.insiderRequiredSells;
    this.connection = new Connection(rpcUrl, {
      commitment: "processed",
      wsEndpoint: wsUrl,
    });
  }

  seedSeenMints(mints: Set<string>): void {
  for (const m of mints) this.boughtMints.add(m);
}

  getActivePosition() {
    return this.activePosition;
  }

  getPreBuyMint() {
    return this.watchingMint;
  }

  getMonitoredWallet() {
    return this.monitoredWallet;
  }

  clearActivePosition(): void {
    void this.resetForNewToken(true);
  }

  clearPreBuyMint(): void {
    void this.resetForNewToken(true);
  }

  setBuySol(value: number): void {
    if (!Number.isFinite(value) || value <= 0) {
      throw new Error("Insider buy SOL must be greater than 0");
    }
    this.buySol = value;
  }

  getBuySol() {
    return this.buySol;
  }

  setEntryMc(value: number): void {
    if (!Number.isFinite(value) || value < 0) {
      throw new Error("Entry MC must be a non-negative number");
    }
    this.entryMc = value;
  }

  getEntryMc() {
    return this.entryMc;
  }

  setExitMc(value: number): void {
    if (!Number.isFinite(value) || value < 0) {
      throw new Error("Exit MC must be a non-negative number");
    }
    this.exitMc = value;
  }

  getExitMc() {
    return this.exitMc;
  }

  setExitPercent(value: number): void {
    if (!Number.isFinite(value) || value < 0) {
      throw new Error("Exit percent must be a non-negative number");
    }
    this.exitPercent = value;
  }

  getExitPercent() {
    return this.exitPercent;
  }

  setBundlerBuyMinUsd(value: number): void {
    if (!Number.isFinite(value) || value < 0) {
      throw new Error("Bundler min USD must be non-negative");
    }
    this.bundlerBuyMinUsd = value;
  }

  getBundlerBuyMinUsd() {
    return this.bundlerBuyMinUsd;
  }

  setBundlerBuyMaxUsd(value: number): void {
    if (!Number.isFinite(value) || value < 0) {
      throw new Error("Bundler max USD must be non-negative");
    }
    this.bundlerBuyMaxUsd = value;
  }

  getBundlerBuyMaxUsd() {
    return this.bundlerBuyMaxUsd;
  }

  getRequiredInsiderSells() {
    return this.requiredInsiderSells;
  }

  isBuyDisabled() {
    return this.buyDisabled;
  }

  setBuyDisabled(value: boolean): void {
    this.buyDisabled = value;
  }

  getFollowedWallet() {
    return this.followedWallet;
  }

  setBuyExecuting(executing: boolean): void {
    this.isBuyExecuting = executing;
  }

  resetBuyAttempt(): void {
    this.isBuyExecuting = false;
    this.buySubmitted = false;
    if (!this.activePosition && this.watchingMint) {
      this.phase = "pre_buy";
      if (this.monitoredWallet && !this.insiderSellsReady) {
        this.startInsiderMonitoring();
      }
      void this.evaluateBuyGate(this.watchingMint);
    }
  }

  isBuyInProgress() {
    return this.isBuyExecuting;
  }

  isRunning() {
    return (
      this.followMonitor !== null ||
      this.insiderLogsSubId !== null ||
      this.bundlerLogsSubIds.size > 0 ||
      this.pollTimer !== null
    );
  }

  async followWallet(address: string): Promise<void> {
    const normalized = new PublicKey(address).toBase58();
    if (this.followedWallet !== normalized) {
      this.boughtMints.clear();
    }
    await this.stopFlowMonitoring();
    if (this.followMonitor) {
      this.followMonitor.stop();
      this.followMonitor = null;
    }
    this.followedWallet = normalized;

   this.followMonitor = new WalletMonitor(this.config, normalized, {
      enforceMinBuySol: false,
      rpcUrl: this.rpcUrl,
      wsUrl: this.wsUrl,
    });
    this.followMonitor.on("newToken", (event) => {
      void this.handleFollowWalletBuy(event.mint, event.signature);
    });

    await this.followMonitor.start();
    for (const mint of this.followMonitor.existingMints) {
      this.boughtMints.add(mint);
    }

    log.info("Insider follow wallet monitoring started", {
      followedWallet: normalized,
      buySol: this.buySol,
      bundlerUsdRange: `${this.bundlerBuyMinUsd}-${this.bundlerBuyMaxUsd}`,
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
    await this.stopFlowMonitoring();
    if (this.followMonitor) {
      this.followMonitor.stop();
      this.followMonitor = null;
    }
    if (this.claimedMint) {
      this.releaseMint?.(this.claimedMint);
      this.claimedMint = null;
    }
    this.activePosition = null;
    this.watchingMint = null;
    this.phase = null;
    this.monitoredWallet = null;
    this.insiderState = null;
    this.bundlerWatch = null;
    this.clearBundlerAccumulation();
    this.clearAxiomWatchedWallets();
  }

  pause(): void {
    void this.stopFlowMonitoring();
    if (this.followMonitor) {
      this.followMonitor.stop();
      this.followMonitor = null;
    }
  }

  markPositionBought(trigger: InsiderBuyTrigger): void {
    void this.stopPreBuyMonitoring();
    this.activePosition = {
      followedWallet: trigger.followedWallet,
      mint: trigger.mint,
    };
    this.watchingMint = null;
    this.monitoredWallet = null;
    this.insiderState = null;
    this.boughtMints.add(trigger.mint);
    this.phase = "holding";
    this.axiomTraderWatchActive = true;

    const wallets = this.matchedBundlers.map((b) => b.address);
    if (wallets.length >= REQUIRED_BUNDLER_MATCHES) {
      void this.startBundlerMonitoring(wallets, trigger.mint);
    }
    void this.scanAxiomSingleBuyTradersPostBuy(trigger.mint);
  }

  private resetTokenTxCounts(): void {
    this.tokenBuyCount = 0;
    this.tokenSellCount = 0;
  }

  private logTokenTx(
    mint: string,
    kind: "buy" | "sell",
    context: string,
    signature: string,
    wallet: string,
  ): void {
    if (kind === "buy") {
      this.tokenBuyCount += 1;
    } else {
      this.tokenSellCount += 1;
    }
    log.info(`Token ${kind} tx processed`, {
      mint,
      context,
      wallet,
      signature,
      totalBuyTxs: this.tokenBuyCount,
      totalSellTxs: this.tokenSellCount,
    });
  }

  private async handleFollowWalletBuy(mint: string, signature: string): Promise<void> {
    if (this.boughtMints.has(mint)) return;
    if (this.activePosition || this.watchingMint) return;
    if (this.claimMint && !this.claimMint(mint)) {
      log.info("Mint active on other insider bot; ignoring follow-wallet buy", {
        mint,
        signature,
      });
      return;
    }

    if (this.followMonitor) {
      this.followMonitor.stop();
      this.followMonitor = null;
    }

    this.boughtMints.add(mint);
    this.watchingMint = mint;
    this.claimedMint = mint;
    this.emit("mintSeen", mint);

    try {
      await this.startInsiderFlow(mint);
    } catch (err) {
      this.releaseMint?.(mint);
      if (err instanceof InsiderMinBuySolFilterError) {
        log.info("Insider flow skipped by min-buy SOL filter; resetting", {
          mint,
          reason: err.message,
        });
      } else {
        log.error("Failed to start insider flow; resetting", err);
        this.emit("error", err instanceof Error ? err : new Error(String(err)));
      }
      await this.resetForNewToken(true);
    }
  }

  private async startInsiderFlow(mint: string): Promise<void> {
    this.resetTokenTxCounts();
    this.insiderSellsReady = false;
    this.bundlerMatchesReady = false;
    this.clearBundlerAccumulation();
    this.clearAxiomWatchedWallets();

    const swaps = await this.heliusClient.getEarlyInsiderSwaps(mint, 4);
    const earlyInsiderBuys = this.extractEarlyInsiderBuys(swaps, mint);
    this.assertEarlyInsidersMeetMinBuySol(mint, earlyInsiderBuys);

    const lowest = this.findLowestInsiderWallet(earlyInsiderBuys);
    if (!lowest) {
      throw new Error(`Could not identify lowest insider wallet for ${mint}`);
    }

    this.monitoredWallet = lowest.wallet;
    this.initialInsiderWallets.clear();
    for (const wallet of this.extractEarlyInsiderWallets(earlyInsiderBuys)) {
      this.initialInsiderWallets.add(wallet);
    }
    const createTx = await this.heliusClient.getMintCreateTransaction(mint);
    this.devWallet = createTx?.feePayer ?? null;
    if (this.devWallet) {
      log.info("Dev wallet identified for trader-scan exclusions", {
        mint,
        devWallet: this.devWallet,
      });
    }
    this.preBuyStopped = false;
    this.positionSellTriggered = false;
    this.axiomTraderWatchActive = false;
    this.insiderState = {
      wallet: lowest.wallet,
      sellCount: 0,
      isTransferred: false,
    };
    this.phase = "pre_buy";
    this.processedSignatures.add(lowest.signature);

    await this.syncWalletHistory(
      lowest.wallet,
      mint,
      lowest.signature,
      INSIDER_HISTORY_LIMIT,
      "insider",
    );

    await this.telegramBot?.sendDefault(
      [
        `<b>🔍 ${this.label} Flow Started</b>`,
        `Token: <code>${mint}</code>`,
        `Lowest insider: <code>${lowest.wallet}</code>`,
        `Insider sells: <b>${this.insiderState.sellCount}</b> / ${this.requiredInsiderSells}`,
        `Bundler matches: <b>${this.matchedBundlers.length}</b> / ${REQUIRED_BUNDLER_MATCHES}`,
        "",
        "Monitoring insider + scanning GMGN bundlers in parallel (each stops when its target is met)...",
      ].join("\n"),
    );

    this.startInsiderMonitoring();
    this.startPollLoop();
    await this.scanBundlerTraders(mint);
    await this.scanAxiomSingleBuyTradersPreBuy(mint);
    await this.evaluateBuyGate(mint);
  }

  private extractEarlyInsiderBuys(swaps: HeliusTransaction[], mint: string): EarlyInsiderBuy[] {
    const buys: EarlyInsiderBuy[] = [];
    for (const tx of swaps) {
      if (tx.type !== "SWAP") continue;
      for (const transfer of tx.tokenTransfers ?? []) {
        if (transfer.mint !== mint) continue;
        const wallet = transfer.toUserAccount;
        if (!wallet) continue;
        buys.push({
          wallet,
          tokenAmount: transfer.tokenAmount ?? 0,
          signature: tx.signature,
          buySol: this.estimateWalletSolSpent(tx, wallet),
        });
      }
    }
    return buys;
  }

  private extractEarlyInsiderWallets(buys: EarlyInsiderBuy[]): string[] {
    return [...new Set(buys.map((buy) => buy.wallet))];
  }

  private assertEarlyInsidersMeetMinBuySol(mint: string, buys: EarlyInsiderBuy[]): void {
    const minBuySol = this.config.minBuySol;
    if (minBuySol <= 0) return;

    const failing = buys.filter((buy) => buy.buySol === null || buy.buySol < minBuySol);
    if (!failing.length) {
      log.info("Early insider min-buy SOL check passed", {
        mint,
        minBuySol,
        insiderBuys: buys.map((buy) => ({
          wallet: buy.wallet,
          buySol: buy.buySol,
          tokenAmount: buy.tokenAmount,
          signature: buy.signature,
        })),
      });
      return;
    }

    log.warn("Early insider min-buy SOL check failed; resetting token flow", {
      mint,
      minBuySol,
      failingInsiders: failing.map((buy) => ({
        wallet: buy.wallet,
        buySol: buy.buySol,
        tokenAmount: buy.tokenAmount,
        signature: buy.signature,
      })),
      insiderBuys: buys.map((buy) => ({
        wallet: buy.wallet,
        buySol: buy.buySol,
        tokenAmount: buy.tokenAmount,
        signature: buy.signature,
      })),
    });

    throw new InsiderMinBuySolFilterError(
      `Early insider buy SOL below MIN_BUY_SOL ${minBuySol} for ${mint}`,
    );
  }

  private estimateWalletSolSpent(tx: HeliusTransaction, wallet: string): number | null {
    let spentLamports = 0;
    for (const transfer of tx.nativeTransfers ?? []) {
      if (transfer.fromUserAccount === wallet) spentLamports += transfer.amount ?? 0;
      if (transfer.toUserAccount === wallet) spentLamports -= transfer.amount ?? 0;
    }

    if (spentLamports <= 0) return null;
    return parseFloat((spentLamports / 1_000_000_000).toFixed(6));
  }

  private findLowestInsiderWallet(buys: EarlyInsiderBuy[]) {
    let lowest: { wallet: string; tokenAmount: number; signature: string } | null = null;
    for (const buy of buys) {
      if (!lowest || buy.tokenAmount < lowest.tokenAmount) {
        lowest = {
          wallet: buy.wallet,
          tokenAmount: buy.tokenAmount,
          signature: buy.signature,
        };
      }
    }
    return lowest;
  }

  private startPollLoop(): void {
    this.stopPollLoop();
    this.pollTimer = setInterval(() => {
      void this.runPollTick();
    }, POLL_MS);
  }

  private stopPollLoop(): void {
    if (this.pollTimer) {
      clearInterval(this.pollTimer);
      this.pollTimer = null;
    }
  }

  private async runPollTick(): Promise<void> {
    if (this.isProcessing) return;
    this.isProcessing = true;
    try {
      const mint = this.watchingMint ?? this.activePosition?.mint;
      if (!mint) return;

      if (this.phase === "pre_buy" && !this.preBuyStopped) {
        if (this.monitoredWallet && !this.insiderSellsReady) {
          await this.pollWallet(this.monitoredWallet, mint, "insider");
        }
        if (!this.bundlerMatchesReady && !this.buySubmitted) {
          await this.scanBundlerTraders(mint);
        }
        await this.scanAxiomSingleBuyTradersPreBuy(mint);
      }

      if (this.phase === "holding") {
        if (this.bundlerWatch) {
          for (const wallet of this.bundlerWatch.wallets) {
            await this.pollWallet(wallet, mint, "bundler");
          }
        }
        if (this.axiomTraderWatchActive) {
          await this.scanAxiomSingleBuyTradersPostBuy(mint);
        }
      }
    } finally {
      this.isProcessing = false;
    }
  }

  private async pollWallet(
    wallet: string,
    mint: string,
    context: "insider" | "bundler",
  ): Promise<void> {
    const txs = await this.heliusClient.getWalletTransactionsDesc(wallet, INSIDER_HISTORY_LIMIT);
    const relevant = txs
      .filter((tx) => this.isRelevantMintTx(tx, mint))
      .reverse();

    for (const tx of relevant) {
      if (this.processedSignatures.has(tx.signature)) continue;
      this.processedSignatures.add(tx.signature);
      if (context === "insider") {
        await this.handleInsiderTransaction(tx, mint);
      } else {
        await this.handleBundlerTransaction(tx, mint, wallet);
      }
    }
  }

  private startInsiderMonitoring(): void {
    if (!this.monitoredWallet) return;
    this.stopInsiderMonitoring();
    const pubkey = new PublicKey(this.monitoredWallet);
    this.insiderLogsSubId = this.connection.onLogs(
      pubkey,
      (logInfo) => {
        if (!logInfo.err) this.queueSignature(logInfo.signature, "insider");
      },
      "processed",
    );
  }

  private async stopInsiderMonitoring(): Promise<void> {
    if (this.insiderLogsSubId !== null) {
      const id = this.insiderLogsSubId;
      this.insiderLogsSubId = null;
      await this.connection.removeOnLogsListener(id).catch(() => undefined);
    }
  }

  private async startBundlerMonitoring(wallets: string[], mint: string): Promise<void> {
    await this.stopBundlerMonitoring();
    this.bundlerWatch = {
      wallets,
      sellCounts: new Map(wallets.map((w) => [w, 0])),
    };

    for (const wallet of wallets) {
      const pubkey = new PublicKey(wallet);
      const subId = this.connection.onLogs(
        pubkey,
        (logInfo) => {
          if (!logInfo.err) this.queueSignature(logInfo.signature, "bundler", wallet);
        },
        "processed",
      );
      this.bundlerLogsSubIds.set(wallet, subId);
    }

    for (const wallet of wallets) {
      await this.syncWalletHistory(wallet, mint, undefined, INSIDER_HISTORY_LIMIT, "bundler");
    }

    log.info("Started post-buy bundler monitoring", { mint, wallets });
  }

  private async stopBundlerMonitoring(): Promise<void> {
    for (const [wallet, subId] of this.bundlerLogsSubIds) {
      await this.connection.removeOnLogsListener(subId).catch(() => undefined);
      this.bundlerLogsSubIds.delete(wallet);
    }
    this.bundlerWatch = null;
  }

  private queueSignature(
    signature: string,
    context: "insider" | "bundler",
    bundlerWallet?: string,
  ): void {
    if (this.processedSignatures.has(signature)) return;
    this.pendingSignaturesBatch.push(signature);

    const process = () => {
      void this.processSignatureBatch(context, bundlerWallet);
    };

    if (this.pendingSignaturesBatch.length >= this.MAX_BATCH_SIZE) {
      process();
    } else if (!this.batchTimer) {
      this.batchTimer = setTimeout(process, this.BATCH_WINDOW_MS);
    }
  }

  private async processSignatureBatch(
    context: "insider" | "bundler",
    bundlerWallet?: string,
  ): Promise<void> {
    if (this.batchTimer) {
      clearTimeout(this.batchTimer);
      this.batchTimer = null;
    }
    const signatures = [...this.pendingSignaturesBatch];
    this.pendingSignaturesBatch = [];
    const fresh = signatures.filter((s) => !this.processedSignatures.has(s));
    if (fresh.length === 0) return;

    try {
      const txs = await this.heliusClient.getTransactionsBySignatures(fresh);
      for (const tx of txs) {
        if (this.processedSignatures.has(tx.signature)) continue;
        const mint = this.watchingMint ?? this.activePosition?.mint;
        if (!mint || !this.isRelevantMintTx(tx, mint)) continue;
        this.processedSignatures.add(tx.signature);

        if (context === "insider") {
          await this.handleInsiderTransaction(tx, mint);
        } else if (context === "bundler" && bundlerWallet) {
          await this.handleBundlerTransaction(tx, mint, bundlerWallet);
        }
      }
    } catch (err) {
      log.error("Failed to process signature batch", err);
    }
  }

  private async stopPreBuyMonitoring(): Promise<void> {
    if (this.preBuyStopped) return;
    this.preBuyStopped = true;
    await this.stopInsiderMonitoring();
    log.info("Pre-buy monitoring stopped (insider + GMGN bundler scan)", {
      mint: this.watchingMint ?? this.activePosition?.mint,
      initialInsiderWallets: [...this.initialInsiderWallets],
      devWallet: this.devWallet,
    });
  }

  private async stopFlowMonitoring(): Promise<void> {
    this.stopPollLoop();
    await this.stopPreBuyMonitoring();
    await this.stopInsiderMonitoring();
    await this.stopBundlerMonitoring();
    this.axiomTraderWatchActive = false;
    this.clearAxiomWatchedWallets();
    if (this.batchTimer) {
      clearTimeout(this.batchTimer);
      this.batchTimer = null;
    }
    this.processedSignatures.clear();
    this.pendingSignaturesBatch = [];
  }

  private isRelevantMintTx(tx: HeliusTransaction, mint: string): boolean {
    return (tx.tokenTransfers ?? []).some((t) => t.mint === mint);
  }

  private classifyTx(
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
      for (const transfer of tx.tokenTransfers ?? []) {
        if (transfer.mint === mint && transfer.toUserAccount === wallet) return "buy";
      }
      for (const transfer of tx.tokenTransfers ?? []) {
        if (transfer.mint === SOL_MINT && transfer.toUserAccount === wallet) return "sell";
        if (transfer.mint === mint && transfer.fromUserAccount === wallet) return "sell";
      }
    }
    return null;
  }

  private async syncWalletHistory(
    wallet: string,
    mint: string,
    startSignature: string | undefined,
    limit: number,
    context: "insider" | "bundler",
  ): Promise<void> {
    const txs = await this.heliusClient.getWalletTransactionsDesc(wallet, limit);
    const sorted = [...txs].reverse();
    let foundStart = !startSignature;

    for (const tx of sorted) {
      if (startSignature && tx.signature === startSignature) {
        foundStart = true;
        continue;
      }
      if (!foundStart) continue;
      if (this.processedSignatures.has(tx.signature)) continue;
      if (!this.isRelevantMintTx(tx, mint)) continue;
      this.processedSignatures.add(tx.signature);

      if (context === "insider") {
        await this.applyInsiderTx(tx, mint, wallet);
      } else {
        await this.applyBundlerTx(tx, mint, wallet);
      }
    }
  }

  private async handleInsiderTransaction(tx: HeliusTransaction, mint: string): Promise<void> {
    if (!this.insiderState || !this.monitoredWallet) return;
    await this.applyInsiderTx(tx, mint, this.monitoredWallet);
  }

  private async applyInsiderTx(
    tx: HeliusTransaction,
    mint: string,
    wallet: string,
  ): Promise<void> {
    if (!this.insiderState) return;
    const kind = this.classifyTx(tx, wallet, mint);
    if (!kind) return;

    if (kind === "buy" || kind === "sell") {
      this.logTokenTx(mint, kind, "insider", tx.signature, wallet);
    }

    if (kind === "transfer_out") {
      const recipient = (tx.tokenTransfers ?? []).find(
        (t) => t.mint === mint && t.fromUserAccount === wallet,
      )?.toUserAccount;
      if (!recipient) return;
      await this.switchToTransferredWallet(recipient, mint, tx.signature);
      return;
    }

    if (kind === "sell") {
      if (this.insiderSellsReady) return;
      this.insiderState.sellCount += 1;
      if (this.insiderState.sellCount >= this.requiredInsiderSells) {
        await this.markInsiderSellsReady(mint);
      }
    }
  }

  private async markInsiderSellsReady(mint: string): Promise<void> {
    if (this.insiderSellsReady) return;
    this.insiderSellsReady = true;
    await this.stopInsiderMonitoring();
    log.info(
      "Insider sell threshold reached — stopped insider monitoring, waiting for bundler matches",
      {
        mint,
        sellCount: this.insiderState?.sellCount,
        required: this.requiredInsiderSells,
        bundlerMatchesReady: this.bundlerMatchesReady,
      },
    );
    await this.evaluateBuyGate(mint);
  }

  private async switchToTransferredWallet(
    newWallet: string,
    mint: string,
    transferSignature: string,
  ): Promise<void> {
    await this.stopInsiderMonitoring();
    this.monitoredWallet = newWallet;
    this.insiderState = {
      wallet: newWallet,
      sellCount: 0,
      isTransferred: true,
    };
    this.insiderSellsReady = false;
    this.processedSignatures.clear();
    this.processedSignatures.add(transferSignature);

    await this.syncWalletHistory(newWallet, mint, transferSignature, INSIDER_HISTORY_LIMIT, "insider");
    this.startInsiderMonitoring();

    await this.telegramBot?.sendDefault(
      [
        `<b>🔀 ${this.label} Transfer Detected</b>`,
        `Token: <code>${mint}</code>`,
        `Now monitoring: <code>${newWallet}</code>`,
        `Insider sells: <b>${this.insiderState.sellCount}</b> / ${this.requiredInsiderSells}`,
        `Bundler matches: <b>${this.matchedBundlers.length}</b> / ${REQUIRED_BUNDLER_MATCHES}`,
        this.bundlerMatchesReady
          ? "Bundler scan already met — waiting for insider sells on new wallet."
          : "",
      ]
        .filter(Boolean)
        .join("\n"),
    );

    await this.evaluateBuyGate(mint);
  }

  private clearBundlerAccumulation(): void {
    this.accumulatedSingleBuyBundlers = [];
    this.accumulatedMultiBuyBundlers = [];
    this.matchedBundlers = [];
    this.bundlerMatchType = null;
  }

  private clearAxiomWatchedWallets(): void {
    this.axiomWatchedWallets.clear();
  }

  private bundlerMatchTypeLabel(type: BundlerMatchType): string {
    return type === "single_buy" ? "Single-buy pair" : "Multi-buy pair";
  }

  private async tryCompleteBundlerGate(mint: string): Promise<boolean> {
    if (this.accumulatedSingleBuyBundlers.length >= REQUIRED_BUNDLER_MATCHES) {
      return this.completeBundlerGate(
        mint,
        "single_buy",
        this.accumulatedSingleBuyBundlers,
      );
    }
    if (this.accumulatedMultiBuyBundlers.length >= REQUIRED_BUNDLER_MATCHES) {
      return this.completeBundlerGate(
        mint,
        "multi_buy",
        this.accumulatedMultiBuyBundlers,
      );
    }
    return false;
  }

  private async completeBundlerGate(
    mint: string,
    matchType: BundlerMatchType,
    source: BundlerMatch[],
  ): Promise<boolean> {
    this.matchedBundlers = source.slice(0, REQUIRED_BUNDLER_MATCHES);
    this.bundlerMatchType = matchType;
    this.bundlerMatchesReady = true;
    log.info(
      `Bundler match threshold reached (${this.bundlerMatchTypeLabel(matchType)}) — stopped GMGN bundler scan`,
      {
        mint,
        matchType,
        wallets: this.matchedBundlers.map((m) => m.address),
        insiderSellsReady: this.insiderSellsReady,
        insiderSellCount: this.insiderState?.sellCount ?? 0,
        requiredInsiderSells: this.requiredInsiderSells,
      },
    );
    await this.evaluateBuyGate(mint);
    return true;
  }

  private knownBundlerAddresses(): Set<string> {
    return new Set([
      ...this.accumulatedSingleBuyBundlers.map((b) => b.address),
      ...this.accumulatedMultiBuyBundlers.map((b) => b.address),
    ]);
  }

  private parseBundlerCandidate(entry: Record<string, unknown>): BundlerMatch | null {
    const buyUsd = this.parseBuyVolumeUsd(entry);
    const buyTxCount = this.parseBuyTxCount(entry);
    const address = entry.address as string | undefined;
    if (!address || buyUsd === null || buyTxCount === null) return null;
    if (buyUsd < this.bundlerBuyMinUsd || buyUsd > this.bundlerBuyMaxUsd) return null;
    return { address, buyUsd, buyTxCount };
  }

  private traderScanSkipAddresses(): Set<string> {
    const skip = new Set(this.initialInsiderWallets);
    if (this.devWallet) skip.add(this.devWallet);
    return skip;
  }

  private isExcludedTraderScan(entry: Record<string, unknown>): boolean {
    const skip = this.traderScanSkipAddresses();
    const address = entry.address as string | undefined;
    if (!address) return true;
    if (skip.has(address)) return true;

    for (const field of ["token_transfer_in", "token_transfer"] as const) {
      const transfer = entry[field] as { address?: string } | undefined;
      const source = transfer?.address?.trim();
      if (source && skip.has(source)) return true;
    }
    return false;
  }

  private hasAxiomOrEmptyTag(entry: Record<string, unknown>): boolean {
    const tags = entry.tags;
    if (!Array.isArray(tags)) return false;
    if (tags.length === 0) return true;
    return tags.length === 1 && tags[0] === "axiom";
  }

  private isAxiomSingleBuyCandidate(entry: Record<string, unknown>): boolean {
    if (!this.hasAxiomOrEmptyTag(entry)) return false;

    const buyTxCount = this.parseBuyTxCount(entry);
    if (buyTxCount !== 1) return false;

    const buyUsd = this.parseBuyVolumeUsd(entry);
    if (buyUsd === null) return false;
    return buyUsd >= this.bundlerBuyMinUsd && buyUsd <= this.bundlerBuyMaxUsd;
  }

  private hasSoldAllPosition(entry: Record<string, unknown>): boolean {
    const balance = Number(entry.balance ?? entry.amount_cur ?? NaN);
    const amountPct = Number(entry.amount_percentage ?? NaN);
    const sellPct = Number(entry.sell_amount_percentage ?? 0);

    if (Number.isFinite(balance) && balance <= 0) {
      if (!Number.isFinite(amountPct) || amountPct <= 0) return true;
    }
    if (sellPct >= 1 && Number.isFinite(balance) && balance <= 0) return true;
    return false;
  }

  private collectAxiomSingleBuyMatches(
    list: Array<Record<string, unknown>>,
  ): {
    matchingWallets: Array<{
      address: string;
      buyUsd: number;
      sold: boolean;
      tags: string[];
    }>;
    apiTotal: number;
    excludedCount: number;
    skippedMultiBuy: number;
    validCount: number;
    soldAmongValid: number;
    soldPositionRatio: string;
  } {
    let excludedCount = 0;
    let skippedMultiBuy = 0;
    let validCount = 0;
    let soldAmongValid = 0;
    const matchingWallets: Array<{
      address: string;
      buyUsd: number;
      sold: boolean;
      tags: string[];
    }> = [];

    for (const entry of list) {
      if (this.isExcludedTraderScan(entry)) {
        excludedCount += 1;
        continue;
      }

      const buyTxCount = this.parseBuyTxCount(entry);
      if (buyTxCount !== 1) {
        if (buyTxCount !== null && buyTxCount > 1) skippedMultiBuy += 1;
        continue;
      }

      if (!this.isAxiomSingleBuyCandidate(entry)) continue;

      validCount += 1;
      const address = entry.address as string;
      const buyUsd = this.parseBuyVolumeUsd(entry)!;
      const sold = this.hasSoldAllPosition(entry);
      const tags = Array.isArray(entry.tags)
        ? (entry.tags as string[])
        : [];

      if (sold) soldAmongValid += 1;
      matchingWallets.push({ address, buyUsd, sold, tags });
    }

    const soldPositionRatio =
      validCount > 0 ? `${soldAmongValid}/${validCount}` : "0/0";

    return {
      matchingWallets,
      apiTotal: list.length,
      excludedCount,
      skippedMultiBuy,
      validCount,
      soldAmongValid,
      soldPositionRatio,
    };
  }

  private logAxiomSingleBuyTraderScan(
    mint: string,
    phase: "pre_buy" | "post_buy",
    list: Array<Record<string, unknown>>,
  ): ReturnType<InsiderBot["collectAxiomSingleBuyMatches"]> | null {
    if (!list.length) return null;

    const stats = this.collectAxiomSingleBuyMatches(list);
    const soldWallets = stats.matchingWallets
      .filter((w) => w.sold)
      .map((w) => w.address);
    const holdingWallets = stats.matchingWallets
      .filter((w) => !w.sold)
      .map((w) => w.address);

    log.info(
      `Axiom/empty single-buy GMGN scan [${phase}] — ${stats.soldPositionRatio} sold all position (skip-list excl., buy $${this.bundlerBuyMinUsd}-$${this.bundlerBuyMaxUsd}, tag axiom or []; order buy_volume_cur; limit ${AXIOM_TRADER_SCAN_LIMIT})`,
      {
        mint,
        phase,
        orderBy: "buy_volume_cur",
        tag: null,
        buyUsdRange: `${this.bundlerBuyMinUsd}-${this.bundlerBuyMaxUsd}`,
        apiTotal: stats.apiTotal,
        excludedCount: stats.excludedCount,
        skippedMultiBuy: stats.skippedMultiBuy,
        validCount: stats.validCount,
        soldAmongValid: stats.soldAmongValid,
        soldPositionRatio: stats.soldPositionRatio,
        matchingWallets: stats.matchingWallets,
        soldWallets,
        holdingWallets,
        initialInsiderWallets: [...this.initialInsiderWallets],
        devWallet: this.devWallet,
      },
    );

    return stats;
  }

  private rememberAxiomWatchedWallets(
    mint: string,
    wallets: Array<{ address: string; buyUsd: number; tags: string[] }>,
  ): void {
    let added = 0;
    for (const wallet of wallets) {
      if (this.axiomWatchedWallets.has(wallet.address)) continue;
      this.axiomWatchedWallets.set(wallet.address, {
        ...wallet,
        ata: getAssociatedTokenAddressSync(
          new PublicKey(mint),
          new PublicKey(wallet.address),
          true,
        ),
      });
      added += 1;
    }
    log.info("Axiom cumulative watched wallets updated", {
      mint,
      addedFromScan: added,
      cumulativeValidWallets: this.axiomWatchedWallets.size,
    });
  }

  private async checkAxiomWatchedWalletAtaExits(mint: string): Promise<boolean> {
    if (this.axiomWatchedWallets.size === 0) return false;

    const watched = [...this.axiomWatchedWallets.values()];
    const soldWallets: AxiomWatchedWallet[] = [];
    const holdingWallets: AxiomWatchedWallet[] = [];
    const chunkSize = 100;

    for (let start = 0; start < watched.length; start += chunkSize) {
      const chunk = watched.slice(start, start + chunkSize);
      const infos = await this.connection.getMultipleAccountsInfo(
        chunk.map((wallet) => wallet.ata),
        "processed",
      );

      for (let i = 0; i < chunk.length; i++) {
        const wallet = chunk[i];
        const info = infos[i];
        if (!info) {
          soldWallets.push(wallet);
          continue;
        }

        const amount =
          info.data.length >= 72 ? info.data.readBigUInt64LE(64) : 1n;
        if (amount === 0n) {
          soldWallets.push(wallet);
        } else {
          holdingWallets.push(wallet);
        }
      }
    }

    log.info("Axiom watched-wallet ATA poll", {
      mint,
      watchedCount: watched.length,
      soldAllCount: soldWallets.length,
      holdingCount: holdingWallets.length,
      rule: `watched >= ${AXIOM_EXIT_VALID_WALLET_THRESHOLD}, sold >= ${AXIOM_EXIT_SOLD_WALLET_THRESHOLD}`,
      soldWallets: soldWallets.map((wallet) => wallet.address),
      holdingWallets: holdingWallets.map((wallet) => wallet.address),
    });

    if (
      watched.length < AXIOM_EXIT_VALID_WALLET_THRESHOLD ||
      soldWallets.length < AXIOM_EXIT_SOLD_WALLET_THRESHOLD
    ) {
      return false;
    }

    const walletLines = soldWallets
      .slice(0, AXIOM_EXIT_SOLD_WALLET_THRESHOLD)
      .map(
        (wallet, i) =>
          `${i + 1}. <code>${wallet.address}</code> buy: <b>$${wallet.buyUsd.toFixed(2)}</b> tags: <b>${wallet.tags.length ? wallet.tags.join(", ") : "[]"}</b>`,
      )
      .join("\n");

    log.warn(
      "Axiom watched-wallet ATA sold threshold reached — triggering position sell",
      {
        mint,
        watchedCount: watched.length,
        soldAllCount: soldWallets.length,
        soldWallets: soldWallets.map((wallet) => wallet.address),
      },
    );

    await this.triggerPositionSell(
      mint,
      `${soldWallets.length}/${watched.length} cumulative axiom/empty single-buy wallets have zero ATA balance`,
      [
        "<b>🚨 Axiom ATA Exit Threshold</b>",
        `Token: <code>${mint}</code>`,
        `Sold all / no ATA: <b>${soldWallets.length}</b> / <b>${watched.length}</b>`,
        `Rule: watched wallets >= <b>${AXIOM_EXIT_VALID_WALLET_THRESHOLD}</b> and sold/no-ATA wallets >= <b>${AXIOM_EXIT_SOLD_WALLET_THRESHOLD}</b>.`,
        "",
        "<b>First sold wallets:</b>",
        walletLines,
      ],
      "AXIOM_SINGLE_BUY_ATA_EXIT_TRIGGER",
    );
    return true;
  }

  private async scanAxiomSingleBuyTradersPreBuy(mint: string): Promise<void> {
    if (
      this.phase !== "pre_buy" ||
      this.preBuyStopped ||
      this.buySubmitted ||
      !this.watchingMint
    ) {
      return;
    }

    const traders = await this.preBuyAxiomGmgnClient.fetchBuyVolumeTraders(
      mint,
      AXIOM_TRADER_SCAN_LIMIT,
    );
    const list = this.extractTraderList(traders);
    if (!list.length) {
      log.info("Axiom/empty single-buy GMGN scan [pre_buy] — no traders returned", {
        mint,
        phase: "pre_buy",
        orderBy: "buy_volume_cur",
        tag: null,
        buyUsdRange: `${this.bundlerBuyMinUsd}-${this.bundlerBuyMaxUsd}`,
        limit: AXIOM_TRADER_SCAN_LIMIT,
        responseShape: this.describeTraderResponseShape(traders),
      });
      return;
    }

    const stats = this.logAxiomSingleBuyTraderScan(mint, "pre_buy", list);
    if (stats && stats.validCount > 0) {
      this.rememberAxiomWatchedWallets(mint, stats.matchingWallets);
    }
  }

  private async scanAxiomSingleBuyTradersPostBuy(mint: string): Promise<void> {
    if (
      !this.axiomTraderWatchActive ||
      this.phase !== "holding" ||
      !this.activePosition ||
      this.positionSellTriggered
    ) {
      return;
    }

    const traders = await this.bundlerGmgnClient.fetchBuyVolumeTraders(
      mint,
      AXIOM_TRADER_SCAN_LIMIT,
    );
    const list = this.extractTraderList(traders);
    if (!list.length) {
      log.info("Axiom/empty single-buy GMGN scan [post_buy] — no traders returned", {
        mint,
        phase: "post_buy",
        orderBy: "buy_volume_cur",
        tag: null,
        buyUsdRange: `${this.bundlerBuyMinUsd}-${this.bundlerBuyMaxUsd}`,
        limit: AXIOM_TRADER_SCAN_LIMIT,
        responseShape: this.describeTraderResponseShape(traders),
      });
      await this.checkAxiomWatchedWalletAtaExits(mint);
      return;
    }

    const stats = this.logAxiomSingleBuyTraderScan(mint, "post_buy", list);
    if (!stats || stats.validCount === 0) {
      await this.checkAxiomWatchedWalletAtaExits(mint);
      return;
    }

    this.rememberAxiomWatchedWallets(mint, stats.matchingWallets);
    await this.checkAxiomWatchedWalletAtaExits(mint);
  }

  private async scanBundlerTraders(mint: string): Promise<void> {
    if (
      this.phase !== "pre_buy" ||
      this.preBuyStopped ||
      this.bundlerMatchesReady ||
      this.buySubmitted ||
      this.buyDisabled
    ) {
      return;
    }

    const traders = await this.bundlerGmgnClient.fetchBundlerTraders(mint, 20);
    const list = this.extractTraderList(traders);
    if (!list.length) {
      log.debug("No bundler traders returned from GMGN", { mint });
      return;
    }

    const known = this.knownBundlerAddresses();

    log.info("Bundler GMGN scan", {
      mint,
      totalTraders: list.length,
      lockedSingleBuy: this.accumulatedSingleBuyBundlers.length,
      lockedMultiBuy: this.accumulatedMultiBuyBundlers.length,
      range: `${this.bundlerBuyMinUsd}-${this.bundlerBuyMaxUsd}`,
      insiderSellsReady: this.insiderSellsReady,
      insiderSellCount: this.insiderState?.sellCount ?? 0,
    });

    for (const entry of list) {
      const candidate = this.parseBundlerCandidate(entry);
      if (!candidate || known.has(candidate.address)) continue;

      if (candidate.buyTxCount <= 1) {
        if (this.accumulatedSingleBuyBundlers.length < REQUIRED_BUNDLER_MATCHES) {
          this.accumulatedSingleBuyBundlers.push(candidate);
          known.add(candidate.address);
          if (
            this.accumulatedSingleBuyBundlers.length >=
            this.accumulatedMultiBuyBundlers.length
          ) {
            this.matchedBundlers = [...this.accumulatedSingleBuyBundlers];
          }
          log.info("Locked single-buy bundler (first-seen snapshot)", {
            mint,
            wallet: candidate.address,
            buyUsd: candidate.buyUsd,
            buyTxCount: candidate.buyTxCount,
            locked: this.accumulatedSingleBuyBundlers.length,
            required: REQUIRED_BUNDLER_MATCHES,
          });
        }
      } else if (this.accumulatedMultiBuyBundlers.length < REQUIRED_BUNDLER_MATCHES) {
        this.accumulatedMultiBuyBundlers.push(candidate);
        known.add(candidate.address);
        if (
          this.accumulatedMultiBuyBundlers.length >
          this.accumulatedSingleBuyBundlers.length
        ) {
          this.matchedBundlers = [...this.accumulatedMultiBuyBundlers];
        }
        log.info("Locked multi-buy bundler in range (first-seen snapshot)", {
          mint,
          wallet: candidate.address,
          buyUsd: candidate.buyUsd,
          buyTxCount: candidate.buyTxCount,
          locked: this.accumulatedMultiBuyBundlers.length,
        });
      }

      if (await this.tryCompleteBundlerGate(mint)) return;
    }
  }

  private async evaluateBuyGate(mint: string): Promise<void> {
    if (
      this.phase !== "pre_buy" ||
      !this.insiderSellsReady ||
      !this.bundlerMatchesReady ||
      this.buySubmitted ||
      this.isBuyExecuting ||
      this.buyDisabled
    ) {
      return;
    }

    if (this.matchedBundlers.length < REQUIRED_BUNDLER_MATCHES) return;
    if (!this.bundlerMatchType) return;

    await this.stopPreBuyMonitoring();

    const currentMc = await this.gmgnClient.fetchTokenMarketCapUsd(mint);
    if (currentMc === null) {
      log.warn("Could not fetch MC for buy gate", { mint });
      this.preBuyStopped = false;
      if (this.monitoredWallet) this.startInsiderMonitoring();
      return;
    }

    const newExitMc = currentMc * (1 + this.exitPercent / 100);
    this.setExitMc(newExitMc);
    this.setEntryMc(currentMc);

    const tradersListStr = this.matchedBundlers
      .map(
        (b, i) =>
          `${i + 1}. <code>${b.address}</code> buy: <b>$${b.buyUsd.toFixed(2)}</b> (${b.buyTxCount} buy tx)`,
      )
      .join("\n");

    log.warn("Buy gate passed — both insider sells and bundler matches ready", {
      mint,
      bundlerMatchType: this.bundlerMatchType,
      insiderSells: this.insiderState?.sellCount,
      bundlers: this.matchedBundlers.map((b) => b.address),
      totalBuyTxs: this.tokenBuyCount,
      totalSellTxs: this.tokenSellCount,
      currentMc,
      exitMc: newExitMc,
    });

    this.setBuyExecuting(true);
    this.buySubmitted = true;

    log.info("Buy gate complete — both pre-buy monitors stopped, triggering buy", {
      mint,
      insiderSells: this.insiderState?.sellCount,
      bundlers: this.matchedBundlers.map((b) => b.address),
    });

    this.emit("buyTrigger", {
      followedWallet: this.followedWallet!,
      mint,
      signature: "BUY_GATE_TRIGGER",
      buySol: this.buySol,
      entryMc: currentMc,
      monitoredWallet: this.monitoredWallet ?? undefined,
      tradersListStr: [
        "<b>Buy Gate Passed</b>",
        `Bundler trigger: <b>${this.bundlerMatchTypeLabel(this.bundlerMatchType)}</b>`,
        `Insider sells: <b>${this.insiderState?.sellCount ?? 0}</b>`,
        tradersListStr,
      ].join("\n"),
    });
  }

  private extractTraderList(traders: Record<string, unknown> | null): Array<Record<string, unknown>> {
    if (!traders) return [];
    if (Array.isArray(traders)) return traders as Array<Record<string, unknown>>;
    let list = traders.list;
    if (!Array.isArray(list)) {
      list =
        traders.traders ||
        (traders.data as { list?: unknown })?.list ||
        (traders.data as { traders?: unknown })?.traders ||
        (traders.data as { items?: unknown })?.items ||
        traders.items;
    }
    if (!Array.isArray(list) && Array.isArray(traders.data)) {
      list = traders.data;
    }
    return Array.isArray(list) ? list : [];
  }

  private describeTraderResponseShape(traders: Record<string, unknown> | null): Record<string, unknown> {
    if (!traders) return { type: "null" };
    if (Array.isArray(traders)) return { type: "array", length: traders.length };

    const data = traders.data;
    const dataRecord =
      data && typeof data === "object" && !Array.isArray(data)
        ? (data as Record<string, unknown>)
        : {};

    return {
      type: "object",
      keys: Object.keys(traders).slice(0, 12),
      source: traders.source ?? null,
      listLength: Array.isArray(traders.list) ? traders.list.length : null,
      tradersLength: Array.isArray(traders.traders) ? traders.traders.length : null,
      itemsLength: Array.isArray(traders.items) ? traders.items.length : null,
      dataIsArray: Array.isArray(data),
      dataLength: Array.isArray(data) ? data.length : null,
      dataKeys: Object.keys(dataRecord).slice(0, 12),
    };
  }

  private parseBuyVolumeUsd(entry: Record<string, unknown>): number | null {
    const raw = entry.buy_volume_cur ?? entry.history_bought_cost;
    const n = typeof raw === "number" ? raw : Number(raw);
    return Number.isFinite(n) ? n : null;
  }

  private parseBuyTxCount(entry: Record<string, unknown>): number | null {
    const raw = entry.buy_tx_count_cur;
    const n = typeof raw === "number" ? raw : Number(raw);
    return Number.isFinite(n) ? n : null;
  }

  private async handleBundlerTransaction(
    tx: HeliusTransaction,
    mint: string,
    wallet: string,
  ): Promise<void> {
    await this.applyBundlerTx(tx, mint, wallet);
  }

  private async applyBundlerTx(
    tx: HeliusTransaction,
    mint: string,
    wallet: string,
  ): Promise<void> {
    if (!this.bundlerWatch || this.phase !== "holding") return;
    const kind = this.classifyTx(tx, wallet, mint);
    if (!kind) return;

    if (kind === "buy" || kind === "sell") {
      this.logTokenTx(mint, kind, "bundler", tx.signature, wallet);
    }

    if (kind === "transfer_out") {
      const recipient = (tx.tokenTransfers ?? []).find(
        (t) => t.mint === mint && t.fromUserAccount === wallet,
      )?.toUserAccount;
      log.warn("Bundler wallet transfer-out detected (post-buy) — selling ASAP", {
        mint,
        wallet,
        recipient,
        signature: tx.signature,
        totalBuyTxs: this.tokenBuyCount,
        totalSellTxs: this.tokenSellCount,
      });
    await this.triggerPositionSell(
      mint,
      recipient
          ? `Bundler wallet ${wallet} transferred token out to ${recipient}`
          : `Bundler wallet ${wallet} transferred token out`,
      [
        "<b>🚨 Bundler Transfer-Out — Selling ASAP</b>",
        `Token: <code>${mint}</code>`,
        `Bundler: <code>${wallet}</code>`,
        recipient ? `Recipient: <code>${recipient}</code>` : "",
        "Tracked bundler moved tokens out — immediate sell triggered.",
      ].filter(Boolean),
      "BUNDLER_TRANSFER_OUT_TRIGGER",
    );
      return;
    }

    if (kind !== "sell") return;

    const current = this.bundlerWatch.sellCounts.get(wallet) ?? 0;
    this.bundlerWatch.sellCounts.set(wallet, current + 1);

    log.info("Bundler wallet sell detected (post-buy)", {
      mint,
      wallet,
      walletSellCount: current + 1,
      totalBuyTxs: this.tokenBuyCount,
      totalSellTxs: this.tokenSellCount,
      signature: tx.signature,
    });

    await this.checkBundlerSellTrigger(mint);
  }

  private async checkBundlerSellTrigger(mint: string): Promise<void> {
    if (!this.bundlerWatch || !this.activePosition) return;

    const allSold = this.bundlerWatch.wallets.every(
      (w) => (this.bundlerWatch!.sellCounts.get(w) ?? 0) >= 1,
    );
    if (!allSold) return;

    log.warn("Both bundler wallets sold — triggering position sell", {
      mint,
      wallets: this.bundlerWatch.wallets,
      sellCounts: Object.fromEntries(this.bundlerWatch.sellCounts),
      totalBuyTxs: this.tokenBuyCount,
      totalSellTxs: this.tokenSellCount,
    });

    await this.triggerPositionSell(
      mint,
      "Both tracked bundler wallets have sold at least once",
      [
        "<b>🚨 Bundler Sell Signal</b>",
        `Token: <code>${mint}</code>`,
        "Both tracked bundler wallets have sold at least once.",
        `Total sell txs seen: <b>${this.tokenSellCount}</b>`,
      ],
      "BUNDLER_SELL_TRIGGER",
    );
  }

  private async triggerPositionSell(
    mint: string,
    reason: string,
    telegramLines: string[],
    signature: string,
  ): Promise<void> {
    if (!this.activePosition || this.positionSellTriggered) return;
    this.positionSellTriggered = true;
    this.axiomTraderWatchActive = false;

    await this.telegramBot?.sendDefault(telegramLines.join("\n"));

    this.emit("sellTrigger", {
      followedWallet: this.followedWallet!,
      positionMint: mint,
      signature,
      reason,
    });

    await this.completeFlowCycle();
  }

  private async completeFlowCycle(): Promise<void> {
    if (this.claimedMint) {
      this.releaseMint?.(this.claimedMint);
      this.claimedMint = null;
    }

    await this.stopBundlerMonitoring();
    this.watchingMint = null;
    this.monitoredWallet = null;
    this.insiderState = null;
    this.clearBundlerAccumulation();
    this.initialInsiderWallets.clear();
    this.devWallet = null;
    this.axiomTraderWatchActive = false;
    this.clearAxiomWatchedWallets();
    this.preBuyStopped = false;
    this.insiderSellsReady = false;
    this.bundlerMatchesReady = false;
    this.phase = this.activePosition ? "holding" : null;

    if (!this.activePosition) {
      this.stopPollLoop();
      this.resetTokenTxCounts();
    }

    if (this.followedWallet && !this.followMonitor) {
      await this.followWallet(this.followedWallet);
    }
  }

  private async resetForNewToken(clearPosition: boolean): Promise<void> {
    if (this.claimedMint) {
      this.releaseMint?.(this.claimedMint);
      this.claimedMint = null;
    }

    await this.stopFlowMonitoring();
    if (clearPosition) {
    this.activePosition = null;
    }
    this.watchingMint = null;
    this.phase = null;
    this.monitoredWallet = null;
    this.insiderState = null;
    this.bundlerWatch = null;
    this.clearBundlerAccumulation();
    this.initialInsiderWallets.clear();
    this.devWallet = null;
    this.axiomTraderWatchActive = false;
    this.clearAxiomWatchedWallets();
    this.preBuyStopped = false;
    this.positionSellTriggered = false;
    this.insiderSellsReady = false;
    this.bundlerMatchesReady = false;
    this.buySubmitted = false;
    this.resetTokenTxCounts();

    log.info("InsiderBot reset; resuming followed wallet monitoring");
    if (this.followedWallet && !this.followMonitor) {
      await this.followWallet(this.followedWallet);
    }
  }
}
