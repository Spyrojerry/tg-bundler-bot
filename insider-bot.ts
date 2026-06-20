import {
  AccountInfo,
  Connection,
  LAMPORTS_PER_SOL,
  PublicKey,
} from "@solana/web3.js";
import { EventEmitter } from "events";
import {
  getAssociatedTokenAddressSync,
  TOKEN_2022_PROGRAM_ID,
  TOKEN_PROGRAM_ID,
} from "@solana/spl-token";
import { createLogger, Logger } from "./logger";
import { HeliusClient, HeliusTransaction } from "./helius-client";
import { GmgnClient } from "./gmgn-client";
import type { ServiceConfig } from "./types";
import { TelegramBot } from "./telegram-bot";
import { WalletMonitor } from "./wallet-monitor";

const SOL_MINT = "So11111111111111111111111111111111111111112";
const INSIDER_HISTORY_LIMIT = 21;
const REQUIRED_BUNDLER_MATCHES = 2;
const AXIOM_TRADER_SCAN_LIMIT = 50;
const AXIOM_BUY_MIN_EXISTING_ATA_WALLETS = 5;
const AXIOM_BUY_MAX_SOLD_ANY_WALLETS = 3;
const AXIOM_EXIT_SOLD_ANY_WALLET_THRESHOLD = 5;
const AXIOM_EXIT_MIN_SOLD_ANY_RATIO = 0.2;
const AXIOM_EXIT_COLLAPSED_EXISTING_ATA_WALLETS = 2;
const AXIOM_EXIT_NEAR_ZERO_SINGLE_SELL_THRESHOLD = 2;
const MAX_FOLLOW_WALLET_START_MARKET_CAP_USD = 50_000;

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
  rearmPositionMonitoringAfterSellFailure(mint: string): void;
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
  atas: PublicKey[];
}

interface ExistingAtaWalletSolBalance {
  address: string;
  tokenStatus: "holding" | "sold_all";
  tokenBalanceRaw: string;
  soldAny: boolean;
  sellType: "single_sell" | "multi_sell" | null;
  firstSellBalanceBefore: string | null;
  firstSellBalanceAfter: string | null;
  solBalance: number;
  solBalanceUsd: number;
  similarBalanceGroup: number | null;
}

interface SimilarSolBalanceGroup {
  group: number;
  walletCount: number;
  minUsd: number;
  maxUsd: number;
  spreadUsd: number;
  wallets: ExistingAtaWalletSolBalance[];
}

export class InsiderBot extends EventEmitter {
  private readonly log: Logger;
   private readonly config: ServiceConfig;
  private readonly connection: Connection;
  private readonly telegramBot: TelegramBot | null;
  private readonly heliusClient: HeliusClient;
  private readonly gmgnClient: GmgnClient;
  private readonly bundlerGmgnClient: GmgnClient;
  /** This bot's GMGN client for pre-buy axiom/empty single-buy discovery. */
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
  private axiomSkippedMultiBuyWallets = new Set<string>();
  private axiomPreviousTokenBalances = new Map<string, bigint>();
  private axiomSoldAnyWallets = new Map<
    string,
    {
      sellType: "single_sell" | "multi_sell";
      balanceBefore: string;
      balanceAfter: string;
    }
  >();
  private maxObservedLargestSimilarBalanceGroupCount = 0;
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
  private axiomAtaPollTimer: ReturnType<typeof setInterval> | null = null;

  private readonly BATCH_WINDOW_MS = 1000;
  private readonly MAX_BATCH_SIZE = 100;

  private activePosition: { followedWallet: string; mint: string } | null = null;
  private boughtMints = new Set<string>();
  private claimedMint: string | null = null;
  private buySubmitted = false;
  private isBuyExecuting = false;
  private isBuyGateEvaluating = false;
  private isProcessing = false;
  private isAxiomAtaPolling = false;
  private cachedSolPriceUsd: number | null = null;
  private cachedSolPriceAt = 0;

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
    this.log = createLogger(label.toUpperCase());
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

  rearmPositionMonitoringAfterSellFailure(mint: string): void {
    if (!this.activePosition || this.activePosition.mint !== mint) return;
    this.positionSellTriggered = false;
    this.axiomTraderWatchActive = true;
    this.phase = "holding";
    this.startPollLoop();
    this.startAxiomAtaPollLoop();
    this.log.warn("Sell failed; active position and monitoring retained", {
      mint,
      cumulativeAxiomWallets: this.axiomWatchedWallets.size,
    });
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
    this.isBuyGateEvaluating = false;
    this.buySubmitted = false;
    if (!this.activePosition && this.watchingMint) {
      this.phase = "pre_buy";
      this.preBuyStopped = false;
      if (this.monitoredWallet && !this.insiderSellsReady) {
        this.startInsiderMonitoring();
      }
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
      this.pollTimer !== null ||
      this.axiomAtaPollTimer !== null
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
      logLabel: `WALLET ${this.label.toUpperCase()}`,
    });
    this.followMonitor.on("newToken", (event) => {
      void this.handleFollowWalletBuy(event.mint, event.signature);
    });

    await this.followMonitor.start();
    for (const mint of this.followMonitor.existingMints) {
      this.boughtMints.add(mint);
    }

    this.log.info("Insider follow wallet monitoring started", {
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
    this.log.info(`Token ${kind} tx processed`, {
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
      this.log.info("Mint active on other insider bot; ignoring follow-wallet buy", {
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
      const followWalletBuyMc =
        await this.gmgnClient.fetchTokenMarketCapUsd(mint);
      if (
        followWalletBuyMc !== null &&
        followWalletBuyMc > MAX_FOLLOW_WALLET_START_MARKET_CAP_USD
      ) {
        this.log.warn(
          "Follow-wallet buy market cap above monitoring ceiling; skipping token",
          {
            mint,
            signature,
            followWalletBuyMc,
            maxFollowWalletStartMarketCapUsd:
              MAX_FOLLOW_WALLET_START_MARKET_CAP_USD,
            action: "reset token flow",
          },
        );
        void this.sendTelegramSafe(
          [
            `<b>⏭️ ${this.label} Token Skipped</b>`,
            `Token: <code>${mint}</code>`,
            `Follow-wallet buy MC: <b>$${followWalletBuyMc.toLocaleString()}</b>`,
            `Monitoring ceiling: <b>$${MAX_FOLLOW_WALLET_START_MARKET_CAP_USD.toLocaleString()}</b>`,
            "Flow reset — waiting for the next token.",
          ].join("\n"),
          "high-MC skip notification",
        );
        await this.resetForNewToken(true);
        return;
      }
      if (followWalletBuyMc === null) {
        this.log.warn(
          "Could not fetch follow-wallet buy MC; continuing token monitoring",
          { mint, signature },
        );
      } else {
        this.log.info("Follow-wallet buy MC accepted; starting token monitoring", {
          mint,
          signature,
          followWalletBuyMc,
          maxFollowWalletStartMarketCapUsd:
            MAX_FOLLOW_WALLET_START_MARKET_CAP_USD,
        });
      }

      await this.startInsiderFlow(mint);
    } catch (err) {
      this.releaseMint?.(mint);
      if (err instanceof InsiderMinBuySolFilterError) {
        this.log.info("Insider flow skipped by min-buy SOL filter; resetting", {
          mint,
          reason: err.message,
        });
      } else {
        this.log.error("Failed to start insider flow; resetting", err);
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
      this.log.info("Dev wallet identified for trader-scan exclusions", {
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

    void this.sendTelegramSafe(
      [
        `<b>🔍 ${this.label} Flow Started</b>`,
        `Token: <code>${mint}</code>`,
        `Lowest insider: <code>${lowest.wallet}</code>`,
        `Insider sells: <b>${this.insiderState.sellCount}</b> / ${this.requiredInsiderSells}`,
        "",
        "Monitoring insider while discovering cumulative Axiom/empty single-buy wallets...",
      ].join("\n"),
      "flow-start notification",
    );

    this.startInsiderMonitoring();
    this.startPollLoop();
    this.startAxiomAtaPollLoop();
    await this.scanAxiomSingleBuyTradersPreBuy(mint);
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
      this.log.info("Early insider min-buy SOL check passed", {
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

    this.log.warn("Early insider min-buy SOL check failed; resetting token flow", {
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
    }, this.config.monitorInterval);
  }

  private stopPollLoop(): void {
    if (this.pollTimer) {
      clearInterval(this.pollTimer);
      this.pollTimer = null;
    }
  }

  private startAxiomAtaPollLoop(): void {
    this.stopAxiomAtaPollLoop();
    this.axiomAtaPollTimer = setInterval(() => {
      void this.runAxiomAtaPollTick();
    }, this.config.monitorInterval);
  }

  private stopAxiomAtaPollLoop(): void {
    if (this.axiomAtaPollTimer) {
      clearInterval(this.axiomAtaPollTimer);
      this.axiomAtaPollTimer = null;
    }
    this.isAxiomAtaPolling = false;
  }

  private async runAxiomAtaPollTick(): Promise<void> {
    if (this.isAxiomAtaPolling || this.axiomWatchedWallets.size === 0) return;

    const mint = this.watchingMint ?? this.activePosition?.mint;
    if (!mint) return;

    const phase =
      this.phase === "pre_buy"
        ? "pre_buy"
        : this.phase === "holding"
          ? "post_buy"
          : null;
    if (!phase) return;
    if (phase === "pre_buy" && (this.preBuyStopped || this.buySubmitted)) return;
    if (phase === "post_buy" && this.positionSellTriggered) return;

    this.isAxiomAtaPolling = true;
    try {
      await this.checkAxiomWatchedWalletAtaExits(mint, {
        phase,
        triggerSell: phase === "post_buy",
      });
    } catch (err) {
      this.log.warn(`Independent Axiom ATA poll [${phase}] failed`, {
        mint,
        error: err instanceof Error ? err.message : String(err),
      });
    } finally {
      this.isAxiomAtaPolling = false;
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
        await this.scanAxiomSingleBuyTradersPreBuy(mint);
      }

      if (this.phase === "holding") {
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

    this.log.info("Started post-buy bundler monitoring", { mint, wallets });
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
      this.log.error("Failed to process signature batch", err);
    }
  }

  private async stopPreBuyMonitoring(): Promise<void> {
    if (this.preBuyStopped) return;
    this.preBuyStopped = true;
    await this.stopInsiderMonitoring();
    this.log.info("Pre-buy monitoring stopped", {
      mint: this.watchingMint ?? this.activePosition?.mint,
      initialInsiderWallets: [...this.initialInsiderWallets],
      devWallet: this.devWallet,
    });
  }

  private async stopFlowMonitoring(): Promise<void> {
    this.stopPollLoop();
    this.stopAxiomAtaPollLoop();
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
    this.log.info(
      "Insider sell threshold reached — insider wallet tracking complete; Axiom monitoring continues",
      {
        mint,
        sellCount: this.insiderState?.sellCount,
        required: this.requiredInsiderSells,
        bundlerMatchesReady: this.bundlerMatchesReady,
        phase: this.phase,
        cumulativeAxiomWallets: this.axiomWatchedWallets.size,
      },
    );

    // The legacy insider sell counter is not a buy gate. Keep the token's
    // independent GMGN discovery and ATA polling alive after it completes.
    this.startPollLoop();
    this.startAxiomAtaPollLoop();
    if (this.phase === "pre_buy" && !this.preBuyStopped && this.watchingMint) {
      void this.scanAxiomSingleBuyTradersPreBuy(mint).catch((err) => {
        this.log.warn(
          "Immediate Axiom pre-buy scan after insider threshold failed; periodic scan remains active",
          {
            mint,
            error: err instanceof Error ? err.message : String(err),
          },
        );
      });
    }
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

    void this.sendTelegramSafe(
      [
        `<b>🔀 ${this.label} Transfer Detected</b>`,
        `Token: <code>${mint}</code>`,
        `Now monitoring: <code>${newWallet}</code>`,
        `Insider sells: <b>${this.insiderState.sellCount}</b> / ${this.requiredInsiderSells}`,
      ]
        .filter(Boolean)
        .join("\n"),
      "transfer notification",
    );
  }

  private clearBundlerAccumulation(): void {
    this.accumulatedSingleBuyBundlers = [];
    this.accumulatedMultiBuyBundlers = [];
    this.matchedBundlers = [];
    this.bundlerMatchType = null;
  }

  private clearAxiomWatchedWallets(): void {
    this.axiomWatchedWallets.clear();
    this.axiomSkippedMultiBuyWallets.clear();
    this.axiomPreviousTokenBalances.clear();
    this.axiomSoldAnyWallets.clear();
    this.maxObservedLargestSimilarBalanceGroupCount = 0;
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
    this.log.info(
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
    skippedMultiBuyWallets: string[];
    validCount: number;
    soldAmongValid: number;
    soldPositionRatio: string;
  } {
    let excludedCount = 0;
    let skippedMultiBuy = 0;
    let validCount = 0;
    let soldAmongValid = 0;
    const skippedMultiBuyWallets: string[] = [];
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
        if (buyTxCount !== null && buyTxCount > 1) {
          skippedMultiBuy += 1;
          skippedMultiBuyWallets.push(entry.address as string);
        }
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
      skippedMultiBuyWallets,
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
    let addedMultiBuyWallets = 0;
    for (const address of stats.skippedMultiBuyWallets) {
      if (this.axiomSkippedMultiBuyWallets.has(address)) continue;
      this.axiomSkippedMultiBuyWallets.add(address);
      addedMultiBuyWallets += 1;
    }
    this.log.info(
      `Axiom/empty single-buy GMGN scan [${phase}] — ${stats.soldPositionRatio} sold all position (skip-list excl., buy $${this.bundlerBuyMinUsd}-$${this.bundlerBuyMaxUsd}, tag axiom or []; order buy_volume_cur; limit ${AXIOM_TRADER_SCAN_LIMIT})`,
      {
        mint,
        phase,
        buyUsdRange: `${this.bundlerBuyMinUsd}-${this.bundlerBuyMaxUsd}`,
        apiTotal: stats.apiTotal,
        excludedCount: stats.excludedCount,
        skippedMultiBuy: stats.skippedMultiBuy,
        addedCumulativeMultiBuyWallets: addedMultiBuyWallets,
        cumulativeSkippedMultiBuy:
          this.axiomSkippedMultiBuyWallets.size,
        validCount: stats.validCount,
        soldAmongValid: stats.soldAmongValid,
        soldPositionRatio: stats.soldPositionRatio,
        matchingWallets: stats.matchingWallets,
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
        atas: [TOKEN_PROGRAM_ID, TOKEN_2022_PROGRAM_ID].map((tokenProgram) =>
          getAssociatedTokenAddressSync(
            new PublicKey(mint),
            new PublicKey(wallet.address),
            true,
            tokenProgram,
          ),
        ),
      });
      added += 1;
    }
    this.log.info("Axiom cumulative watched wallets updated", {
      mint,
      addedFromScan: added,
      cumulativeValidWallets: this.axiomWatchedWallets.size,
    });
  }

  private async checkAxiomWatchedWalletAtaExits(
    mint: string,
    options: { phase: "pre_buy" | "post_buy"; triggerSell: boolean },
  ): Promise<boolean> {
    if (this.axiomWatchedWallets.size === 0) return false;

    const watched = [...this.axiomWatchedWallets.values()];
    const ataLookups = watched.flatMap((wallet) =>
      wallet.atas.map((ata) => ({ wallet, ata })),
    );
    const hasAnyAta = new Set<string>();
    const hasPositiveBalance = new Set<string>();
    const currentTokenBalances = new Map<string, bigint>();
    let existingAtaCount = 0;
    let positiveAtaCount = 0;
    const chunkSize = 100;

    for (let start = 0; start < ataLookups.length; start += chunkSize) {
      const chunk = ataLookups.slice(start, start + chunkSize);
      const infos = await this.connection.getMultipleAccountsInfo(
        chunk.map((lookup) => lookup.ata),
        "processed",
      );

      for (let i = 0; i < chunk.length; i++) {
        const lookup = chunk[i];
        const info = infos[i];
        if (!info) continue;

        existingAtaCount += 1;
        hasAnyAta.add(lookup.wallet.address);

        const amount =
          info.data.length >= 72 ? info.data.readBigUInt64LE(64) : 0n;
        currentTokenBalances.set(
          lookup.wallet.address,
          (currentTokenBalances.get(lookup.wallet.address) ?? 0n) + amount,
        );
        if (amount > 0n) {
          positiveAtaCount += 1;
          hasPositiveBalance.add(lookup.wallet.address);
        }
      }
    }

    for (const wallet of watched) {
      if (!hasAnyAta.has(wallet.address)) continue;
      const currentBalance = currentTokenBalances.get(wallet.address) ?? 0n;
      const previousBalance = this.axiomPreviousTokenBalances.get(
        wallet.address,
      );
      if (
        previousBalance !== undefined &&
        currentBalance < previousBalance &&
        !this.axiomSoldAnyWallets.has(wallet.address)
      ) {
        this.axiomSoldAnyWallets.set(wallet.address, {
          sellType: currentBalance === 0n ? "single_sell" : "multi_sell",
          balanceBefore: previousBalance.toString(),
          balanceAfter: currentBalance.toString(),
        });
      }
      this.axiomPreviousTokenBalances.set(wallet.address, currentBalance);
    }

    const soldWallets: AxiomWatchedWallet[] = [];
    const holdingWallets: AxiomWatchedWallet[] = [];
    const missingAtaWallets: AxiomWatchedWallet[] = [];

    for (const wallet of watched) {
      if (hasPositiveBalance.has(wallet.address)) {
        holdingWallets.push(wallet);
      } else if (hasAnyAta.has(wallet.address)) {
        soldWallets.push(wallet);
      } else {
        missingAtaWallets.push(wallet);
      }
    }

    const existingAtaWalletCount = soldWallets.length + holdingWallets.length;
    const {
      solPriceUsd,
      walletBalances: existingAtaWalletSolBalances,
      groups: similarSolBalanceGroups,
    } = await this.fetchAndGroupExistingAtaWalletSolBalances(
      holdingWallets,
      soldWallets,
    );
    const largestSimilarBalanceGroup = similarSolBalanceGroups[0] ?? null;
    const largestGroupExistingAtaWalletCount =
      largestSimilarBalanceGroup?.walletCount ?? 0;
    const largestGroupSoldAnyWalletBalances =
      largestSimilarBalanceGroup?.wallets.filter(
        (wallet) => wallet.soldAny,
      ) ?? [];
    const largestGroupSoldAnyWalletAddresses = new Set(
      largestGroupSoldAnyWalletBalances.map((wallet) => wallet.address),
    );
    const largestGroupSoldAnyWallets = watched.filter((wallet) =>
      largestGroupSoldAnyWalletAddresses.has(wallet.address),
    );
    const largestGroupHoldingCount =
      largestGroupExistingAtaWalletCount -
      largestGroupSoldAnyWallets.length;
    const largestGroupSoldAnyRatio =
      largestGroupExistingAtaWalletCount > 0
        ? largestGroupSoldAnyWallets.length /
          largestGroupExistingAtaWalletCount
        : 0;
    const previousMaxLargestSimilarBalanceGroupCount =
      this.maxObservedLargestSimilarBalanceGroupCount;
    this.maxObservedLargestSimilarBalanceGroupCount = Math.max(
      this.maxObservedLargestSimilarBalanceGroupCount,
      largestGroupExistingAtaWalletCount,
    );
    const cumulativeSkippedMultiBuy =
      this.axiomSkippedMultiBuyWallets.size;
    const ataConversionRatio =
      watched.length > 0 ? existingAtaWalletCount / watched.length : 0;
    const multiBuyToAtaRatio =
      existingAtaWalletCount > 0
        ? cumulativeSkippedMultiBuy / existingAtaWalletCount
        : 0;
    const soldRatio =
      existingAtaWalletCount > 0
        ? soldWallets.length / existingAtaWalletCount
        : 0;
    const largestSimilarSolGroup = largestSimilarBalanceGroup
      ? {
          walletCount: largestGroupExistingAtaWalletCount,
          holdingCount: largestGroupHoldingCount,
          soldAnyCount: largestGroupSoldAnyWallets.length,
          soldAnyRatio: Number(largestGroupSoldAnyRatio.toFixed(4)),
          balanceUsdRange: `${largestSimilarBalanceGroup.minUsd.toFixed(2)}-${largestSimilarBalanceGroup.maxUsd.toFixed(2)}`,
          spreadUsd: largestSimilarBalanceGroup.spreadUsd,
          wallets: largestSimilarBalanceGroup.wallets.map((wallet) => ({
            address: wallet.address,
            status: wallet.tokenStatus,
            sellType: wallet.sellType,
            solBalanceUsd: wallet.solBalanceUsd,
          })),
        }
      : null;
    const nearZeroWalletBalances =
      solPriceUsd === null
        ? []
        : existingAtaWalletSolBalances
            .filter(
              (wallet) =>
                wallet.solBalanceUsd >= 0 && wallet.solBalanceUsd <= 1,
            )
            .sort((a, b) => a.solBalanceUsd - b.solBalanceUsd);
    const nearZeroSoldAnyCount = nearZeroWalletBalances.filter(
      (wallet) => wallet.soldAny,
    ).length;
    const nearZeroSingleSellWallets = nearZeroWalletBalances.filter(
      (wallet) =>
        wallet.tokenStatus === "sold_all" &&
        wallet.sellType === "single_sell",
    );
    const nearZeroMultiSellWallets = nearZeroWalletBalances.filter(
      (wallet) => wallet.sellType === "multi_sell",
    );
    const nearZeroSingleSellTriggerPassed =
      nearZeroMultiSellWallets.length === 0 &&
      nearZeroSingleSellWallets.length >=
        AXIOM_EXIT_NEAR_ZERO_SINGLE_SELL_THRESHOLD;
    const largestNearZeroSolGroup =
      nearZeroWalletBalances.length > 0
        ? {
            walletCount: nearZeroWalletBalances.length,
            holdingCount:
              nearZeroWalletBalances.length - nearZeroSoldAnyCount,
            soldAnyCount: nearZeroSoldAnyCount,
            soldAnyRatio: Number(
              (nearZeroSoldAnyCount / nearZeroWalletBalances.length).toFixed(4),
            ),
            balanceUsdRange: `${nearZeroWalletBalances[0].solBalanceUsd.toFixed(2)}-${nearZeroWalletBalances[nearZeroWalletBalances.length - 1].solBalanceUsd.toFixed(2)}`,
            maximumBalanceUsd: 1,
            spreadUsd: Number(
              (
                nearZeroWalletBalances[nearZeroWalletBalances.length - 1]
                  .solBalanceUsd - nearZeroWalletBalances[0].solBalanceUsd
              ).toFixed(2),
            ),
            wallets: nearZeroWalletBalances.map((wallet) => ({
              address: wallet.address,
              status: wallet.tokenStatus,
              sellType: wallet.sellType,
              solBalanceUsd: wallet.solBalanceUsd,
            })),
          }
        : null;
    const collapsedToTwo =
      previousMaxLargestSimilarBalanceGroupCount >
        AXIOM_EXIT_COLLAPSED_EXISTING_ATA_WALLETS &&
      largestGroupExistingAtaWalletCount ===
        AXIOM_EXIT_COLLAPSED_EXISTING_ATA_WALLETS;
    const buyGateFailedConditions: string[] = [];
    if (
      largestGroupExistingAtaWalletCount <
      AXIOM_BUY_MIN_EXISTING_ATA_WALLETS
    ) {
      buyGateFailedConditions.push(
        `group_size: ${largestGroupExistingAtaWalletCount} < ${AXIOM_BUY_MIN_EXISTING_ATA_WALLETS}`,
      );
    }
    if (largestGroupSoldAnyWallets.length > AXIOM_BUY_MAX_SOLD_ANY_WALLETS) {
      buyGateFailedConditions.push(
        `sold_any: ${largestGroupSoldAnyWallets.length} > ${AXIOM_BUY_MAX_SOLD_ANY_WALLETS}`,
      );
    }
    const sellGateFailedConditions: string[] = [];
    if (
      largestGroupSoldAnyWallets.length <
      AXIOM_EXIT_SOLD_ANY_WALLET_THRESHOLD
    ) {
      sellGateFailedConditions.push(
        `sold_any: ${largestGroupSoldAnyWallets.length} < ${AXIOM_EXIT_SOLD_ANY_WALLET_THRESHOLD}`,
      );
    }
    if (largestGroupSoldAnyRatio < AXIOM_EXIT_MIN_SOLD_ANY_RATIO) {
      sellGateFailedConditions.push(
        `sold_any_ratio: ${largestGroupSoldAnyRatio.toFixed(4)} < ${AXIOM_EXIT_MIN_SOLD_ANY_RATIO}`,
      );
    }

    if (existingAtaWalletCount === 0) {
      this.log.warn(
        `Axiom watched-wallet ATA poll [${options.phase}] found no SPL or Token-2022 ATAs for any watched wallet; skipping sell trigger`,
        {
          mint,
          phase: options.phase,
          watchedCount: watched.length,
          existingAtaWalletCount,
          existingAtaCount,
          positiveAtaCount,
          cumulativeSkippedMultiBuy,
          note: "Skipping to avoid a false positive from token-program mismatch or stale GMGN discovery.",
        },
      );
      return false;
    }

    this.log.info(`Axiom watched-wallet ATA poll [${options.phase}] — ${largestGroupSoldAnyWallets.length}/${largestGroupExistingAtaWalletCount} sold any in largest similar-SOL group`, {
      mint,
      phase: options.phase,
      watchedCount: watched.length,
      existingAtaWalletCount,
      cumulativeSkippedMultiBuy,
      solPriceUsd:
        solPriceUsd === null ? null : Number(solPriceUsd.toFixed(2)),
      largestSimilarSolGroup,
      largestNearZeroSolGroup,
      ...(options.phase === "pre_buy"
        ? {
            buyGate: {
              requiredGroupSize: AXIOM_BUY_MIN_EXISTING_ATA_WALLETS,
              maximumSoldAny: AXIOM_BUY_MAX_SOLD_ANY_WALLETS,
              passed: buyGateFailedConditions.length === 0,
              failedConditions: buyGateFailedConditions,
            },
          }
        : {
            sellGate: {
              requiredSoldAny: AXIOM_EXIT_SOLD_ANY_WALLET_THRESHOLD,
              requiredSoldAnyRatio: AXIOM_EXIT_MIN_SOLD_ANY_RATIO,
              collapseGroupSize: AXIOM_EXIT_COLLAPSED_EXISTING_ATA_WALLETS,
              nearZeroSingleSellThreshold:
                AXIOM_EXIT_NEAR_ZERO_SINGLE_SELL_THRESHOLD,
              nearZeroSingleSellCount: nearZeroSingleSellWallets.length,
              nearZeroMultiSellCount: nearZeroMultiSellWallets.length,
              nearZeroSingleSellFilterSkipped:
                nearZeroMultiSellWallets.length > 0,
              collapsedToTwo,
              passed:
                nearZeroSingleSellTriggerPassed ||
                collapsedToTwo ||
                sellGateFailedConditions.length === 0,
              failedConditions:
                nearZeroSingleSellTriggerPassed || collapsedToTwo
                  ? []
                  : sellGateFailedConditions,
            },
          }),
    });

    if (options.phase === "pre_buy") {
      await this.evaluateAxiomAtaBuyGate(
        mint,
        largestGroupExistingAtaWalletCount,
        largestGroupSoldAnyWallets.length,
        existingAtaWalletCount,
        watched.length,
        missingAtaWallets.length,
        cumulativeSkippedMultiBuy,
      );
      return false;
    }

    if (!options.triggerSell) {
      return false;
    }

    if (nearZeroSingleSellTriggerPassed) {
      const walletLines = nearZeroSingleSellWallets
        .slice(0, AXIOM_EXIT_NEAR_ZERO_SINGLE_SELL_THRESHOLD)
        .map(
          (wallet, i) =>
            `${i + 1}. <code>${wallet.address}</code> status: <b>${wallet.tokenStatus}</b> type: <b>${wallet.sellType}</b> SOL: <b>$${wallet.solBalanceUsd.toFixed(2)}</b>`,
        )
        .join("\n");

      this.log.warn(
        "Axiom near-zero SOL group single-sell threshold reached — triggering position sell",
        {
          mint,
          threshold: AXIOM_EXIT_NEAR_ZERO_SINGLE_SELL_THRESHOLD,
          nearZeroWalletCount: nearZeroWalletBalances.length,
          qualifyingSingleSellCount: nearZeroSingleSellWallets.length,
          multiSellCount: nearZeroMultiSellWallets.length,
          qualifyingWallets: nearZeroSingleSellWallets,
        },
      );

      await this.triggerPositionSell(
        mint,
        `${nearZeroSingleSellWallets.length} wallets in the near-zero SOL group sold all in one transaction`,
        [
          "<b>🚨 Axiom Near-Zero SOL Single-Sell Exit</b>",
          `Token: <code>${mint}</code>`,
          `Qualifying wallets: <b>${nearZeroSingleSellWallets.length}</b>`,
          `Rule: sell when at least <b>${AXIOM_EXIT_NEAR_ZERO_SINGLE_SELL_THRESHOLD}</b> near-zero SOL wallets have status <b>sold_all</b> and type <b>single_sell</b>.`,
          "",
          walletLines,
        ],
        "AXIOM_NEAR_ZERO_SINGLE_SELL_EXIT_TRIGGER",
      );
      return true;
    }

    if (collapsedToTwo) {
      this.log.warn(
        "Axiom largest similar-SOL group count collapsed to 2 — triggering position sell",
        {
          mint,
          previousMaxLargestSimilarBalanceGroupCount,
          largestGroupExistingAtaWalletCount,
          soldAnyCount: largestGroupSoldAnyWallets.length,
          soldAnyPositionRatio: `${largestGroupSoldAnyWallets.length}/${largestGroupExistingAtaWalletCount}`,
        },
      );

      await this.triggerPositionSell(
        mint,
        `Axiom largest similar-SOL group collapsed from ${previousMaxLargestSimilarBalanceGroupCount} to ${largestGroupExistingAtaWalletCount} (${largestGroupSoldAnyWallets.length}/${largestGroupExistingAtaWalletCount} sold any)`,
        [
          "<b>🚨 Axiom Similar-SOL Group Collapse</b>",
          `Token: <code>${mint}</code>`,
          `Largest group wallets: <b>${previousMaxLargestSimilarBalanceGroupCount}</b> → <b>${largestGroupExistingAtaWalletCount}</b>`,
          `Sold any / largest group: <b>${largestGroupSoldAnyWallets.length}</b> / <b>${largestGroupExistingAtaWalletCount}</b>`,
          `Rule: sell when the post-buy largest similar-SOL group reaches <b>${AXIOM_EXIT_COLLAPSED_EXISTING_ATA_WALLETS}</b>.`,
        ],
        "AXIOM_EXISTING_ATA_COUNT_COLLAPSE_TRIGGER",
      );
      return true;
    }

    if (
      largestGroupSoldAnyWallets.length <
        AXIOM_EXIT_SOLD_ANY_WALLET_THRESHOLD ||
      largestGroupSoldAnyRatio < AXIOM_EXIT_MIN_SOLD_ANY_RATIO
    ) {
      return false;
    }

    const walletLines = largestGroupSoldAnyWalletBalances
      .slice(0, AXIOM_EXIT_SOLD_ANY_WALLET_THRESHOLD)
      .map(
        (wallet, i) =>
          `${i + 1}. <code>${wallet.address}</code> type: <b>${wallet.sellType}</b> token status: <b>${wallet.tokenStatus}</b>`,
      )
      .join("\n");

    this.log.warn(
      "Axiom largest similar-SOL group sold-any threshold reached — triggering position sell",
      {
        mint,
        watchedCount: watched.length,
        largestGroupExistingAtaWalletCount,
        soldAnyCount: largestGroupSoldAnyWallets.length,
        soldAnyRatio: largestGroupSoldAnyRatio,
        soldAnyWallets: largestGroupSoldAnyWalletBalances,
      },
    );

    await this.triggerPositionSell(
      mint,
      `${largestGroupSoldAnyWallets.length}/${largestGroupExistingAtaWalletCount} wallets in the largest similar-SOL group reduced their token balance`,
      [
        "<b>🚨 Axiom Similar-SOL Group Exit Threshold</b>",
        `Token: <code>${mint}</code>`,
        `Sold any / largest group: <b>${largestGroupSoldAnyWallets.length}</b> / <b>${largestGroupExistingAtaWalletCount}</b>`,
        `Group sold-any ratio: <b>${(largestGroupSoldAnyRatio * 100).toFixed(1)}%</b>`,
        `Cumulative valid wallets watched: <b>${watched.length}</b>`,
        `Missing ATA wallets ignored: <b>${missingAtaWallets.length}</b>`,
        `Rule: largest-group sold-any wallets >= <b>${AXIOM_EXIT_SOLD_ANY_WALLET_THRESHOLD}</b> and group sold-any ratio >= <b>${(AXIOM_EXIT_MIN_SOLD_ANY_RATIO * 100).toFixed(0)}%</b>.`,
        "",
        "<b>First sold-any wallets:</b>",
        walletLines,
      ],
      "AXIOM_SINGLE_BUY_ATA_EXIT_TRIGGER",
    );
    return true;
  }

  private async fetchAndGroupExistingAtaWalletSolBalances(
    holdingWallets: AxiomWatchedWallet[],
    soldWallets: AxiomWatchedWallet[],
  ): Promise<{
    solPriceUsd: number | null;
    walletBalances: ExistingAtaWalletSolBalance[];
    groups: SimilarSolBalanceGroup[];
  }> {
    const existingWallets = [
      ...holdingWallets.map((wallet) => ({
        wallet,
        tokenStatus: "holding" as const,
      })),
      ...soldWallets.map((wallet) => ({
        wallet,
        tokenStatus: "sold_all" as const,
      })),
    ];
    if (existingWallets.length === 0) {
      return { solPriceUsd: null, walletBalances: [], groups: [] };
    }

    const accountInfos: Array<AccountInfo<Buffer> | null> = [];
    for (let start = 0; start < existingWallets.length; start += 100) {
      const chunk = existingWallets.slice(start, start + 100);
      accountInfos.push(
        ...(await this.connection.getMultipleAccountsInfo(
          chunk.map(({ wallet }) => new PublicKey(wallet.address)),
          "processed",
        )),
      );
    }
    const solPriceUsd = await this.getCachedSolPriceUsd();
    const walletBalances: ExistingAtaWalletSolBalance[] = existingWallets.map(
      ({ wallet, tokenStatus }, index) => {
        const solBalance =
          (accountInfos[index]?.lamports ?? 0) / LAMPORTS_PER_SOL;
        const sellState = this.axiomSoldAnyWallets.get(wallet.address);
        return {
          address: wallet.address,
          tokenStatus,
          tokenBalanceRaw:
            this.axiomPreviousTokenBalances.get(wallet.address)?.toString() ??
            "0",
          soldAny: this.axiomSoldAnyWallets.has(wallet.address),
          sellType: sellState?.sellType ?? null,
          firstSellBalanceBefore: sellState?.balanceBefore ?? null,
          firstSellBalanceAfter: sellState?.balanceAfter ?? null,
          solBalance: Number(solBalance.toFixed(9)),
          solBalanceUsd:
            solPriceUsd === null
              ? 0
              : Number((solBalance * solPriceUsd).toFixed(2)),
          similarBalanceGroup: null,
        };
      },
    );

    if (solPriceUsd === null) {
      return { solPriceUsd, walletBalances, groups: [] };
    }

    const sorted = [...walletBalances].sort(
      (a, b) => a.solBalanceUsd - b.solBalanceUsd,
    );
    let left = 0;
    let largestGroupWallets: ExistingAtaWalletSolBalance[] = [];
    for (let right = 0; right < sorted.length; right += 1) {
      while (
        sorted[right].solBalanceUsd - sorted[left].solBalanceUsd > 1
      ) {
        left += 1;
      }
      const candidate = sorted.slice(left, right + 1);
      if (candidate.length > largestGroupWallets.length) {
        largestGroupWallets = candidate;
      }
    }

    const groups: SimilarSolBalanceGroup[] = [];
    if (largestGroupWallets.length >= 2) {
      for (const wallet of largestGroupWallets) {
        wallet.similarBalanceGroup = 1;
      }
      const minUsd = largestGroupWallets[0].solBalanceUsd;
      const maxUsd =
        largestGroupWallets[largestGroupWallets.length - 1].solBalanceUsd;
      groups.push({
        group: 1,
        walletCount: largestGroupWallets.length,
        minUsd: Number(minUsd.toFixed(2)),
        maxUsd: Number(maxUsd.toFixed(2)),
        spreadUsd: Number((maxUsd - minUsd).toFixed(2)),
        wallets: largestGroupWallets,
      });
    }

    return { solPriceUsd, walletBalances, groups };
  }

  private async getCachedSolPriceUsd(): Promise<number | null> {
    const now = Date.now();
    if (
      this.cachedSolPriceUsd !== null &&
      now - this.cachedSolPriceAt < 30_000
    ) {
      return this.cachedSolPriceUsd;
    }

    const solPriceUsd = await this.gmgnClient.fetchSolPriceUsd();
    if (solPriceUsd !== null) {
      this.cachedSolPriceUsd = solPriceUsd;
      this.cachedSolPriceAt = now;
      return solPriceUsd;
    }
    return this.cachedSolPriceUsd;
  }

  private async evaluateAxiomAtaBuyGate(
    mint: string,
    largestGroupExistingAtaWalletCount: number,
    largestGroupSoldAnyCount: number,
    overallExistingAtaWalletCount: number,
    watchedCount: number,
    missingAtaWalletCount: number,
    cumulativeSkippedMultiBuy: number,
  ): Promise<void> {
    const ataConversionRatio =
      watchedCount > 0 ? overallExistingAtaWalletCount / watchedCount : 0;
    const multiBuyToAtaRatio =
      overallExistingAtaWalletCount > 0
        ? cumulativeSkippedMultiBuy / overallExistingAtaWalletCount
        : 0;
    const largestGroupSoldAnyRatio =
      largestGroupExistingAtaWalletCount > 0
        ? largestGroupSoldAnyCount / largestGroupExistingAtaWalletCount
        : 0;

    if (
      this.phase !== "pre_buy" ||
      this.preBuyStopped ||
      this.buySubmitted ||
      this.isBuyExecuting ||
      this.isBuyGateEvaluating ||
      this.buyDisabled ||
      largestGroupExistingAtaWalletCount <
        AXIOM_BUY_MIN_EXISTING_ATA_WALLETS ||
      largestGroupSoldAnyCount > AXIOM_BUY_MAX_SOLD_ANY_WALLETS
    ) {
      return;
    }

    this.isBuyGateEvaluating = true;
    try {
      await this.stopPreBuyMonitoring();

      const currentMc = await this.gmgnClient.fetchTokenMarketCapUsd(mint);
      if (currentMc === null) {
        this.log.warn("Could not fetch MC for Axiom ATA buy gate", { mint });
        this.preBuyStopped = false;
        if (this.monitoredWallet && !this.insiderSellsReady) {
          this.startInsiderMonitoring();
        }
        return;
      }

      const newExitMc = currentMc * (1 + this.exitPercent / 100);
      this.setExitMc(newExitMc);
      this.setEntryMc(currentMc);
      this.setBuyExecuting(true);
      this.buySubmitted = true;

      this.log.warn("Axiom ATA buy gate passed — triggering buy", {
        mint,
        largestGroupExistingAtaWalletCount,
        largestGroupSoldAnyCount,
        overallExistingAtaWalletCount,
        watchedCount,
        missingAtaWalletCount,
        cumulativeSkippedMultiBuy,
        ataConversionRatio,
        multiBuyToAtaRatio,
        largestGroupSoldAnyRatio,
        currentMc,
        exitMc: newExitMc,
      });

      this.emit("buyTrigger", {
        followedWallet: this.followedWallet!,
        mint,
        signature: "AXIOM_ATA_BUY_TRIGGER",
        buySol: this.buySol,
        entryMc: currentMc,
        monitoredWallet: this.monitoredWallet ?? undefined,
        tradersListStr: [
          "<b>Axiom ATA Buy Gate Passed</b>",
          `Largest similar-SOL group: <b>${largestGroupExistingAtaWalletCount}</b> wallets`,
          `Sold any in largest group: <b>${largestGroupSoldAnyCount}</b>`,
          `Overall existing ATA wallets: <b>${overallExistingAtaWalletCount}</b>`,
          `Cumulative valid wallets: <b>${watchedCount}</b>`,
          `Missing ATA wallets ignored: <b>${missingAtaWalletCount}</b>`,
          `Cumulative skipped multi-buy wallets: <b>${cumulativeSkippedMultiBuy}</b>`,
          `ATA conversion: <b>${(ataConversionRatio * 100).toFixed(1)}%</b>`,
          `Largest-group sold-any ratio: <b>${(largestGroupSoldAnyRatio * 100).toFixed(1)}%</b>`,
          `Rule: largest similar-SOL group &gt;= <b>${AXIOM_BUY_MIN_EXISTING_ATA_WALLETS}</b>.`,
          `Sold any in largest group: &lt;= <b>${AXIOM_BUY_MAX_SOLD_ANY_WALLETS}</b> wallets.`,
        ].join("\n"),
      });
    } finally {
      this.isBuyGateEvaluating = false;
    }
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
      this.log.info("Axiom/empty single-buy GMGN scan [pre_buy] — no traders returned", {
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
      this.log.info("Axiom/empty single-buy GMGN scan [post_buy] — no traders returned", {
        mint,
        phase: "post_buy",
        orderBy: "buy_volume_cur",
        tag: null,
        buyUsdRange: `${this.bundlerBuyMinUsd}-${this.bundlerBuyMaxUsd}`,
        limit: AXIOM_TRADER_SCAN_LIMIT,
        responseShape: this.describeTraderResponseShape(traders),
      });
      return;
    }

    const stats = this.logAxiomSingleBuyTraderScan(mint, "post_buy", list);
    if (!stats || stats.validCount === 0) {
      return;
    }

    this.rememberAxiomWatchedWallets(mint, stats.matchingWallets);
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
      this.log.warn("Bundler wallet transfer-out detected (post-buy) — selling ASAP", {
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

    this.log.info("Bundler wallet sell detected (post-buy)", {
      mint,
      wallet,
      walletSellCount: current + 1,
      totalBuyTxs: this.tokenBuyCount,
      totalSellTxs: this.tokenSellCount,
      signature: tx.signature,
    });
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

    void this.sendTelegramSafe(
      telegramLines.join("\n"),
      "sell-trigger notification",
    );

    this.emit("sellTrigger", {
      followedWallet: this.followedWallet!,
      positionMint: mint,
      signature,
      reason,
    });
  }

  private async sendTelegramSafe(
    text: string,
    context: string,
  ): Promise<void> {
    if (!this.telegramBot) return;
    try {
      await this.telegramBot.sendDefault(text);
    } catch (err) {
      this.log.warn(`Telegram ${context} failed; continuing bot flow`, {
        error: err instanceof Error ? err.message : String(err),
      });
    }
  }

  private async completeFlowCycle(): Promise<void> {
    if (this.claimedMint) {
      this.releaseMint?.(this.claimedMint);
      this.claimedMint = null;
    }

    await this.stopBundlerMonitoring();
    this.stopAxiomAtaPollLoop();
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
    this.isBuyGateEvaluating = false;
    this.resetTokenTxCounts();

    this.log.info("InsiderBot reset; resuming followed wallet monitoring");
    if (this.followedWallet && !this.followMonitor) {
      await this.followWallet(this.followedWallet);
    }
  }
}
