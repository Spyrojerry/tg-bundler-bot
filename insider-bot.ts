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
import {
  HeliusClient,
  HeliusCreditExhaustionInfo,
  HeliusTransaction,
} from "./helius-client";
import { GmgnClient } from "./gmgn-client";
import type { ServiceConfig } from "./types";
import { TelegramBot } from "./telegram-bot";
import { WalletMonitor } from "./wallet-monitor";

const SOL_MINT = "So11111111111111111111111111111111111111112";
const NATIVE_SOL_BALANCE_MINT =
  "So11111111111111111111111111111111111111111";
const INSIDER_HISTORY_LIMIT = 21;
const REQUIRED_BUNDLER_MATCHES = 2;
const AXIOM_TRADER_SCAN_LIMIT = 50;
const AXIOM_AUTHORITY_CANDIDATE_COUNT = 5;
const AXIOM_AUTHORITY_INITIAL_TX_COUNT = 15;
const AXIOM_AUTHORITY_INITIAL_AFTER_LIMIT = 14;
const AXIOM_AUTHORITY_EARLY_PROBE_TX_COUNT = 3;
const AXIOM_AUTHORITY_BATCH_LIMIT = 50;
const AXIOM_AUTHORITY_MIN_SYNC_INTERVAL_MS = 2_000;
const AXIOM_AUTHORITY_EXTRACTION_MAX_ATTEMPTS = 3;
const AXIOM_AUTHORITY_SIMILAR_BUY_SPREAD_USD = 1;
const AXIOM_AUTHORITY_LARGE_BUY_MIN_USD = 200;
const INSIDER_RUG_MARKET_CAP_USD = 5_000;
const ADDRESS_LOOKUP_TABLE_PROGRAM =
  "AddressLookupTab1e1111111111111111111111111";
const AXIOM_AUTHORITY_EARLY_PROBE_GROUP_SIZE = 2;
const AXIOM_AUTHORITY_FALLBACK_GROUP_SIZE = 5;
const AXIOM_AUTHORITY_NORMAL_BUY_GROUP_MIN_COUNT = 10;
const AXIOM_BUY_MAX_SOLD_ANY_WALLETS = 3;
const AXIOM_EXIT_SOLD_ANY_WALLET_THRESHOLD = 5;
const AXIOM_EXIT_MIN_SOLD_ANY_RATIO = 0.2;
const AXIOM_EXIT_COLLAPSED_EXISTING_ATA_WALLETS = 2;
const AXIOM_EXIT_NEAR_ZERO_SINGLE_SELL_THRESHOLD = 2;
const MAX_FOLLOW_WALLET_START_MARKET_CAP_USD = 60_000;
const BUNDLER_FUNDER_TRANSFER_LIMIT = 5;
const BUNDLER_FUNDER_REQUIRED_COUNT = 4;
const BUNDLER_FUNDER_EXTRA_SOL = 2;
const BUNDLER_FUNDER_SYNC_LIMIT = 50;
const BUNDLER_FUNDER_SYNC_MIN_INTERVAL_MS = 1_000;
const BUNDLER_FUNDER_WS_SYNC_DELAY_MS = 50;
const BUNDLER_FUNDER_MAX_RECIPIENT_WATCHES = 2;
const BUNDLER_FUNDER_RECIPIENT_SYNC_INTERVAL_MS = 1_500;
const BUNDLER_FUNDER_RECIPIENT_BATCH_SIZE = 2;
const HELIUS_POOL_MAX_CONCURRENT = 2;
const HELIUS_POOL_MIN_TIME_MS = 150;
const HELIUS_POOL_REQUEST_TIMEOUT_MS = 10_000;
const HELIUS_POOL_BASE_BACKOFF_MS = 2_000;
const HELIUS_POOL_MAX_BACKOFF_MS = 60_000;
const HELIUS_POOL_METRICS_INTERVAL_MS = 30_000;

type InsiderTxKind = "buy" | "sell" | "transfer_in" | "transfer_out";
type FlowPhase = "pre_buy" | "holding";

class InsiderMinBuySolFilterError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "InsiderMinBuySolFilterError";
  }
}

class HeliusTransientError extends Error {
  constructor(
    message: string,
    readonly cause: unknown,
  ) {
    super(message);
    this.name = "HeliusTransientError";
  }
}

class AsyncRequestQueue {
  private active = 0;
  private lastStartedAt = 0;
  private readonly pending: Array<() => void> = [];

  constructor(
    private readonly maxConcurrent: number,
    private readonly minTimeMs: number,
  ) {}

  async run<T>(task: () => Promise<T>): Promise<T> {
    await this.acquire();
    try {
      return await task();
    } finally {
      this.active -= 1;
      this.pump();
    }
  }

  private acquire(): Promise<void> {
    return new Promise((resolve) => {
      this.pending.push(resolve);
      this.pump();
    });
  }

  private pump(): void {
    if (this.active >= this.maxConcurrent) return;
    const next = this.pending.shift();
    if (!next) return;

    const waitMs = Math.max(
      0,
      this.minTimeMs - (Date.now() - this.lastStartedAt),
    );
    this.active += 1;
    setTimeout(() => {
      this.lastStartedAt = Date.now();
      next();
      this.pump();
    }, waitMs);
  }
}

interface HeliusPoolStats {
  requests: number;
  successes: number;
  fallbacks: number;
  rateLimits: number;
  transientFailures: number;
  permanentFailures: number;
}

interface HeliusPoolEntry {
  client: HeliusClient;
  index: number;
  label: string;
  unavailableUntil: number;
  backoffMs: number;
  stats: HeliusPoolStats;
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
  on(
    event: "heliusCreditsExhausted",
    listener: (info: HeliusCreditExhaustionInfo) => void,
  ): this;
  on(event: "error", listener: (error: Error) => void): this;
  getActivePosition(): { followedWallet: string; mint: string } | null;
  getPreBuyMint(): string | null;
  markPositionBought(trigger: InsiderBuyTrigger): void;
  clearActivePosition(): void;
  rearmPositionMonitoringAfterSellFailure(mint: string): void;
  clearPreBuyMint(): void;
  getEntryMc(): number;
  getExitMc(): number;
  isProfitExitDisabled(): boolean;
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
  stopForHeliusCredits(): Promise<void>;
  isStoppedForHeliusCredits(): boolean;
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
  feePayer: string | null;
  timestamp: number;
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

interface AuthorityCandidateWallet {
  address: string;
  buyUsd: number;
  tags: string[];
}

interface AuthorityPatternWalletState {
  atas: PublicKey[];
  ataBalances: Map<string, bigint>;
  baselineBalance: bigint;
  currentBalance: bigint;
  seenPositiveBalance: boolean;
  completed: boolean;
  completedBalance: bigint | null;
}

interface AuthorityMonitorState {
  mint: string;
  candidates: AuthorityCandidateWallet[];
  authority: string;
  firstBuySignature: string;
  firstBuyTransaction: HeliusTransaction;
  initialTransactions: HeliusTransaction[];
  initialCursorSignature: string | null;
  cursorSignature: string | null;
  initialReady: boolean;
  earlyProbeCompleted: boolean;
  decisionMode: "pending" | "direct_200_buy" | "normal_non_similar";
  processedSignatures: Set<string>;
  nonSimilarWallets: Set<string>;
  patternStates: Map<string, AuthorityPatternWalletState>;
}

interface LargeBuyerWatchState {
  mint: string;
  wallet: string;
  qualifyingSignature: string;
  buyUsd: number;
  boughtAmount: number;
  atas: PublicKey[];
  ataBalances: Map<string, bigint>;
  currentBalance: bigint;
  seenPositiveBalance: boolean;
}

interface BundlerFundingRecord {
  bundlerWallet: string;
  bundlerBuySignature: string;
  fundingSignature: string;
  fundingFeePayer: string;
  senderWallet: string;
  amountSol: number;
  timestamp: number;
}

interface FunderRecipientWatch {
  wallet: string;
  fundingSignature: string;
  outAmountSol: number;
  heliusPreferredIndex: number;
  tokenActions: Array<{
    kind: "buy" | "sell";
    signature: string;
    amount: number;
  }>;
  boughtAmount: number;
  soldAmount: number;
  followSellExit: boolean;
  firstBuySignature: string | null;
}

interface BundlerFunderWatchState {
  mint: string;
  funderWallet: string;
  earliestFundingTimestamp: number;
  earliestFundingSignature: string;
  largestFundingSol: number;
  minTransferOutSol: number;
  cursorSignature: string | null;
  processedSignatures: Set<string>;
  validOutSignatures: Set<string>;
  invalidOutSignatures: Set<string>;
  recipientWatches: Map<string, FunderRecipientWatch>;
  pendingTransferOut: {
    signature: string;
    recipient: string;
    amountSol: number;
    timestamp: number;
  } | null;
}

export class InsiderBot extends EventEmitter {
  private readonly log: Logger;
  private readonly config: ServiceConfig;
  private readonly connection: Connection;
  private readonly telegramBot: TelegramBot | null;
  private readonly heliusClient: HeliusClient;
  private readonly heliusClients: HeliusClient[] = [];
  private readonly heliusPool: HeliusPoolEntry[] = [];
  private readonly heliusRequestQueue = new AsyncRequestQueue(
    HELIUS_POOL_MAX_CONCURRENT,
    HELIUS_POOL_MIN_TIME_MS,
  );
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
  private profitExitDisabled = false;
  private insiderSellsReady = false;
  private bundlerMatchesReady = false;

  private tokenBuyCount = 0;
  private tokenSellCount = 0;

  private insiderLogsSubId: number | null = null;
  private bundlerLogsSubIds = new Map<string, number>();

  private processedSignatures = new Set<string>();
  private queuedSignatures = new Set<string>();
  private pendingSignaturesBatch: string[] = [];
  private batchTimer: ReturnType<typeof setTimeout> | null = null;
  private pollTimer: ReturnType<typeof setInterval> | null = null;
  private axiomAtaPollTimer: ReturnType<typeof setInterval> | null = null;
  private authorityLogsSubId: number | null = null;
  private authorityPatternAtaSubIds = new Map<string, number>();
  private largeBuyerAtaSubIds = new Map<string, number>();
  private authorityMonitor: AuthorityMonitorState | null = null;
  private largeBuyerWatch: LargeBuyerWatchState | null = null;
  private bundlerFunderWatch: BundlerFunderWatchState | null = null;
  private bundlerFunderLogsSubId: number | null = null;
  private recipientLogsSubIds = new Map<string, number>();
  private isBundlerFunderSyncing = false;
  private bundlerFunderSyncPending = false;
  private bundlerFunderSyncPendingForce = false;
  private bundlerFunderWsSyncTimer: ReturnType<typeof setTimeout> | null = null;
  private lastBundlerFunderSyncAt = 0;
  private dirtyFunderRecipients = new Set<string>();
  private dirtyFunderRecipientSignatures = new Map<string, Set<string>>();
  private isFunderRecipientBatchSyncing = false;
  private funderRecipientBatchSyncPending = false;
  private lastFunderRecipientBatchSyncAt = 0;
  private lastHeliusPoolMetricsAt = 0;
  private isAuthoritySyncing = false;
  private authoritySyncPending = false;
  private isAuthorityAtaChecking = false;
  private authorityAtaCheckPending = false;
  private isLargeBuyerSyncing = false;
  private largeBuyerSyncPending = false;
  private lastAuthoritySyncAt = 0;
  private lastLargeBuyerSyncAt = 0;
  private stoppedForHeliusCredits = false;
  private authorityProbeFailedAtTwo = false;
  private isSwitchingInsiderWallet = false;
  private insiderWalletChain = new Set<string>();

  private readonly BATCH_WINDOW_MS = 1000;
  private readonly MAX_BATCH_SIZE = 100;

  private activePosition: { followedWallet: string; mint: string } | null =
    null;
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
    heliusProjectId: string,
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
    this.heliusClient = new HeliusClient(heliusApiKey, {
      projectId: heliusProjectId,
      label,
      onCreditsExhausted: (info) => {
        this.emit("heliusCreditsExhausted", info);
      },
    });
    const apiKeys = [
      heliusApiKey,
      config.insiderHeliusApiKey || config.heliusApiKey,
      config.insiderHeliusApiKey2,
      config.insiderHeliusApiKey3,
      config.insiderHeliusApiKey4,
    ]
      .map((key) => key?.trim())
      .filter((key): key is string => Boolean(key));
    const projectIds = [
      heliusProjectId,
      config.insiderHeliusProjectId,
      config.insiderHeliusProjectId2,
      config.insiderHeliusProjectId3,
      config.insiderHeliusProjectId4,
    ];
    const seenHeliusKeys = new Set<string>();
    for (let index = 0; index < apiKeys.length; index += 1) {
      const key = apiKeys[index];
      if (seenHeliusKeys.has(key)) continue;
      seenHeliusKeys.add(key);
      const client =
        index === 0
          ? this.heliusClient
          : new HeliusClient(key, {
              projectId: projectIds[index] ?? "",
              label: `${label} fallback Helius ${index + 1}`,
              onCreditsExhausted: (info) => {
                this.emit("heliusCreditsExhausted", info);
              },
            });
      this.heliusClients.push(client);
      this.heliusPool.push({
        client,
        index,
        label: index === 0 ? `${label} primary Helius` : `${label} Helius ${index + 1}`,
        unavailableUntil: 0,
        backoffMs: HELIUS_POOL_BASE_BACKOFF_MS,
        stats: {
          requests: 0,
          successes: 0,
          fallbacks: 0,
          rateLimits: 0,
          transientFailures: 0,
          permanentFailures: 0,
        },
      });
    }
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
    this.axiomTraderWatchActive = false;
    this.phase = "holding";
    this.stopAxiomAtaPollLoop();
    this.startPollLoop();
    void this.syncAuthorityTransactions();
    void this.syncLargeBuyerAtaBalances();
    this.log.warn(
      "Sell failed; active position retained and authority-based monitoring rearmed",
      {
        mint,
        authority: this.authorityMonitor?.authority ?? null,
        largeBuyerWallet: this.largeBuyerWatch?.wallet ?? null,
        legacyAxiomPostBuyExitsEnabled: false,
      },
    );
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

  isProfitExitDisabled() {
    return this.profitExitDisabled;
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
      this.bundlerFunderLogsSubId !== null ||
      this.recipientLogsSubIds.size > 0 ||
      this.pollTimer !== null ||
      this.axiomAtaPollTimer !== null
    );
  }

  async followWallet(address: string): Promise<void> {
    if (this.stoppedForHeliusCredits) {
      this.log.warn(
        "Follow-wallet start blocked because Helius credits are exhausted",
        {
          followedWallet: address,
        },
      );
      return;
    }
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
    this.bundlerFunderWatch = null;
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

  async stopForHeliusCredits(): Promise<void> {
    if (this.stoppedForHeliusCredits) return;
    this.stoppedForHeliusCredits = true;
    await this.stopFlowMonitoring();
    if (this.followMonitor) {
      this.followMonitor.stop();
      this.followMonitor = null;
    }
    if (this.claimedMint) {
      this.releaseMint?.(this.claimedMint);
      this.claimedMint = null;
    }
    this.watchingMint = null;
    this.monitoredWallet = null;
    this.insiderState = null;
    this.phase = this.activePosition ? "holding" : null;
    this.log.error(
      "Insider bot stopped because its Helius project has no credits",
      {
        activePositionMint: this.activePosition?.mint ?? null,
      },
    );
  }

  isStoppedForHeliusCredits(): boolean {
    return this.stoppedForHeliusCredits;
  }

  markPositionBought(trigger: InsiderBuyTrigger): void {
    void this.stopPreBuyMonitoring();
    void this.stopAuthorityPatternAtaMonitoring();
    this.activePosition = {
      followedWallet: trigger.followedWallet,
      mint: trigger.mint,
    };
    this.watchingMint = null;
    this.monitoredWallet = null;
    this.insiderState = null;
    this.boughtMints.add(trigger.mint);
    this.phase = "holding";
    this.axiomTraderWatchActive = false;
    this.profitExitDisabled = false;
    if (this.authorityMonitor?.initialCursorSignature) {
      this.authorityMonitor.cursorSignature =
        this.authorityMonitor.initialCursorSignature;
    }

    void this.syncAuthorityTransactions();
    void this.syncLargeBuyerAtaBalances();
    void this.syncBundlerFunderTransactions();
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

  private async handleFollowWalletBuy(
    mint: string,
    signature: string,
  ): Promise<void> {
    if (this.boughtMints.has(mint)) return;
    if (this.activePosition || this.watchingMint) return;
    if (this.claimMint && !this.claimMint(mint)) {
      this.log.info(
        "Mint active on other insider bot; ignoring follow-wallet buy",
        {
          mint,
          signature,
        },
      );
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
        this.log.info(
          "Follow-wallet buy MC accepted; starting token monitoring",
          {
            mint,
            signature,
            followWalletBuyMc,
            maxFollowWalletStartMarketCapUsd:
              MAX_FOLLOW_WALLET_START_MARKET_CAP_USD,
          },
        );
      }

      await this.startInsiderFlow(mint);
    } catch (err) {
      void this.heliusClient.handlePossibleRateLimitError(err);
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
    this.authorityProbeFailedAtTwo = false;
    this.clearBundlerAccumulation();
    this.clearAxiomWatchedWallets();

    const swaps = await this.withHeliusFallback((client) =>
      client.getEarlyInsiderSwaps(mint, 4),
    );
    const earlyInsiderBuys = this.extractFirstUniqueEarlyBundlerBuys(
      swaps,
      mint,
    );
    const earlyBundlerWallets = this.extractEarlyInsiderWallets(earlyInsiderBuys);
    this.initialInsiderWallets.clear();
    for (const wallet of earlyBundlerWallets) this.initialInsiderWallets.add(wallet);

    if (!this.followedWallet || !earlyBundlerWallets.includes(this.followedWallet)) {
      this.log.warn(
        "Follow wallet is not one of the first four unique bundler first-buy wallets; resetting token flow",
        {
          mint,
          followedWallet: this.followedWallet,
          earlyBundlers: earlyBundlerWallets,
        },
      );
      await this.resetForNewToken(true);
      return;
    }

    const createTx = await this.withHeliusFallback((client) =>
      client.getMintCreateTransaction(mint),
    );
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
    this.monitoredWallet = null;
    this.insiderState = null;
    this.phase = "pre_buy";

    void this.sendTelegramSafe(
      [
        `<b>🔍 ${this.label} Bundler-Funder Flow Started</b>`,
        `Token: <code>${mint}</code>`,
        `Follow wallet: <code>${this.followedWallet}</code>`,
        `First unique bundler wallets: <b>${earlyBundlerWallets.length}</b>`,
        "",
        "Finding each bundler's first valid funding transfer, requiring those funding txs to share one feePayer, then watching that feePayer's transfer-outs with an immediate next-tx transfer-in invalidation check.",
      ].join("\n"),
      "flow-start notification",
    );

    this.startPollLoop();
    await this.startBundlerFunderFlow(mint, earlyInsiderBuys);
  }

  private extractEarlyInsiderBuys(
    swaps: HeliusTransaction[],
    mint: string,
  ): EarlyInsiderBuy[] {
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
          buySol: this.estimateEarlyBuySol(tx, wallet),
          feePayer: tx.feePayer ?? null,
          timestamp: tx.timestamp,
        });
      }
    }
    return buys;
  }

  private extractFirstUniqueEarlyBundlerBuys(
    swaps: HeliusTransaction[],
    mint: string,
  ): EarlyInsiderBuy[] {
    const firstBuys: EarlyInsiderBuy[] = [];
    const seenWallets = new Set<string>();
    const sortedSwaps = [...swaps].sort(
      (a, b) => a.slot - b.slot || a.timestamp - b.timestamp,
    );

    for (const tx of sortedSwaps) {
      if (tx.type !== "SWAP") continue;
      for (const transfer of tx.tokenTransfers ?? []) {
        if (transfer.mint !== mint) continue;
        const wallet = transfer.toUserAccount;
        if (!wallet) continue;
        if (seenWallets.has(wallet)) {
          this.log.info(
            "Ignoring follow-up bundler buy; first wallet tx already locked",
            {
              mint,
              wallet,
              ignoredSignature: tx.signature,
            },
          );
          continue;
        }
        seenWallets.add(wallet);
        firstBuys.push({
          wallet,
          tokenAmount: transfer.tokenAmount ?? 0,
          signature: tx.signature,
          buySol: this.estimateEarlyBuySol(tx, wallet),
          feePayer: tx.feePayer ?? null,
          timestamp: tx.timestamp,
        });
        if (firstBuys.length >= BUNDLER_FUNDER_REQUIRED_COUNT) {
          return firstBuys;
        }
      }
    }

    return firstBuys;
  }

  private extractEarlyInsiderWallets(buys: EarlyInsiderBuy[]): string[] {
    return [...new Set(buys.map((buy) => buy.wallet))];
  }

  private assertEarlyInsidersMeetMinBuySol(
    mint: string,
    buys: EarlyInsiderBuy[],
  ): void {
    const minBuySol = this.config.minBuySol;
    if (minBuySol <= 0) return;

    const failing = buys.filter(
      (buy) => buy.buySol === null || buy.buySol < minBuySol,
    );
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

    this.log.warn(
      "Early insider min-buy SOL check failed; resetting token flow",
      {
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
      },
    );

    throw new InsiderMinBuySolFilterError(
      `Early insider buy SOL below MIN_BUY_SOL ${minBuySol} for ${mint}`,
    );
  }

  private estimateWalletSolSpent(
    tx: HeliusTransaction,
    wallet: string,
  ): number | null {
    let spentLamports = 0;
    for (const transfer of tx.nativeTransfers ?? []) {
      if (transfer.fromUserAccount === wallet)
        spentLamports += transfer.amount ?? 0;
      if (transfer.toUserAccount === wallet)
        spentLamports -= transfer.amount ?? 0;
    }

    if (spentLamports <= 0) return null;
    return parseFloat((spentLamports / 1_000_000_000).toFixed(6));
  }

  private estimateEarlyBuySol(
    tx: HeliusTransaction,
    buyWallet: string,
  ): number | null {
    return (
      this.estimateWalletSolSpent(tx, buyWallet) ??
      (tx.feePayer ? this.estimateWalletSolSpent(tx, tx.feePayer) : null)
    );
  }

  private findLowestInsiderWallet(buys: EarlyInsiderBuy[]) {
    let lowest: {
      wallet: string;
      tokenAmount: number;
      signature: string;
    } | null = null;
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

  private async withHeliusFallback<T>(
    fn: (client: HeliusClient, index: number) => Promise<T>,
    preferredIndex = 0,
  ): Promise<T> {
    const pool = this.heliusPool.length
      ? this.heliusPool
      : [
          {
            client: this.heliusClient,
            index: 0,
            label: `${this.label} primary Helius`,
            unavailableUntil: 0,
            backoffMs: HELIUS_POOL_BASE_BACKOFF_MS,
            stats: {
              requests: 0,
              successes: 0,
              fallbacks: 0,
              rateLimits: 0,
              transientFailures: 0,
              permanentFailures: 0,
            },
          },
        ];
    let lastError: unknown = null;
    for (let offset = 0; offset < pool.length; offset += 1) {
      const entry = this.pickHeliusPoolEntry(pool, preferredIndex, offset);
      if (!entry) break;
      const index = entry.index;
      try {
        const result = await this.runHeliusPoolRequest(entry, () =>
          fn(entry.client, index),
        );
        if (offset > 0) entry.stats.fallbacks += 1;
        this.logHeliusPoolMetricsIfDue();
        return result;
      } catch (err) {
        lastError = err;
        await entry.client.handlePossibleRateLimitError(err);
        const transient = this.isTransientHeliusError(err);
        if (!transient) {
          entry.stats.permanentFailures += 1;
          this.log.warn("Helius request failed with non-retryable error", {
            preferredIndex,
            attemptedIndex: index,
            error: err instanceof Error ? err.message : String(err),
          });
          this.logHeliusPoolMetricsIfDue(true);
          throw err instanceof Error ? err : new Error(String(err));
        }

        entry.stats.transientFailures += 1;
        if (this.isRateLimitError(err)) entry.stats.rateLimits += 1;
        this.applyHeliusPoolBackoff(entry);
        this.log.warn("Helius transient request failed; trying fallback key if available", {
          preferredIndex,
          attemptedIndex: index,
          hasFallback: offset + 1 < pool.length,
          backoffMs: entry.backoffMs,
          unavailableUntil: new Date(entry.unavailableUntil).toISOString(),
          error: err instanceof Error ? err.message : String(err),
        });
      }
    }
    this.logHeliusPoolMetricsIfDue(true);
    throw lastError instanceof Error ? lastError : new Error(String(lastError));
  }

  private pickHeliusPoolEntry(
    pool: HeliusPoolEntry[],
    preferredIndex: number,
    offset: number,
  ): HeliusPoolEntry | null {
    const now = Date.now();
    const ordered = pool.map((_, i) => pool[(preferredIndex + i) % pool.length]);
    const available = ordered.filter((entry) => entry.unavailableUntil <= now);
    const coolingDown = ordered.filter((entry) => entry.unavailableUntil > now);
    const candidates = [...available, ...coolingDown];
    return candidates[offset] ?? null;
  }

  private async runHeliusPoolRequest<T>(
    entry: HeliusPoolEntry,
    fn: () => Promise<T>,
  ): Promise<T> {
    entry.stats.requests += 1;
    return await this.heliusRequestQueue.run(async () => {
      const result = await this.withRequestTimeout(fn(), HELIUS_POOL_REQUEST_TIMEOUT_MS);
      entry.stats.successes += 1;
      entry.backoffMs = HELIUS_POOL_BASE_BACKOFF_MS;
      entry.unavailableUntil = 0;
      return result;
    });
  }

  private async withRequestTimeout<T>(
    promise: Promise<T>,
    timeoutMs: number,
  ): Promise<T> {
    let timeout: ReturnType<typeof setTimeout> | null = null;
    try {
      return await Promise.race([
        promise,
        new Promise<T>((_, reject) => {
          timeout = setTimeout(
            () =>
              reject(
                new HeliusTransientError(
                  `Helius request timed out after ${timeoutMs}ms`,
                  null,
                ),
              ),
            timeoutMs,
          );
        }),
      ]);
    } finally {
      if (timeout) clearTimeout(timeout);
    }
  }

  private isTransientHeliusError(error: unknown): boolean {
    if (error instanceof HeliusTransientError) return true;
    const message = error instanceof Error ? error.message : String(error);
    if (/\b429\b|too many requests/i.test(message)) return true;
    if (/\b5\d\d\b/.test(message)) return true;
    if (/timeout|timed out|network|fetch failed|econnreset|etimedout|enotfound|socket|tls/i.test(message)) {
      return true;
    }
    return false;
  }

  private isRateLimitError(error: unknown): boolean {
    const message = error instanceof Error ? error.message : String(error);
    return /\b429\b|too many requests/i.test(message);
  }

  private applyHeliusPoolBackoff(entry: HeliusPoolEntry): void {
    entry.unavailableUntil = Date.now() + entry.backoffMs;
    entry.backoffMs = Math.min(
      entry.backoffMs * 2,
      HELIUS_POOL_MAX_BACKOFF_MS,
    );
  }

  private logHeliusPoolMetricsIfDue(force = false): void {
    const now = Date.now();
    if (!force && now - this.lastHeliusPoolMetricsAt < HELIUS_POOL_METRICS_INTERVAL_MS) {
      return;
    }
    this.lastHeliusPoolMetricsAt = now;
    this.log.info("Helius pool metrics", {
      queue: {
        maxConcurrent: HELIUS_POOL_MAX_CONCURRENT,
        minTimeMs: HELIUS_POOL_MIN_TIME_MS,
        requestTimeoutMs: HELIUS_POOL_REQUEST_TIMEOUT_MS,
      },
      keys: this.heliusPool.map((entry) => ({
        index: entry.index,
        label: entry.label,
        unavailableMs: Math.max(0, entry.unavailableUntil - now),
        nextBackoffMs: entry.backoffMs,
        ...entry.stats,
      })),
    });
  }

  private async startBundlerFunderFlow(
    mint: string,
    earlyBuys: EarlyInsiderBuy[],
  ): Promise<void> {
    const firstFour = earlyBuys.slice(0, BUNDLER_FUNDER_REQUIRED_COUNT);
    if (firstFour.length < BUNDLER_FUNDER_REQUIRED_COUNT) {
      this.log.warn("Token has fewer than four early bundler buys; resetting", {
        mint,
        earlyBuyCount: firstFour.length,
      });
      await this.resetForNewToken(true);
      return;
    }

    const fundingRecords = await Promise.all(
      firstFour.map((buy, index) =>
        this.findValidBundlerFundingRecord(mint, buy, index),
      ),
    );
    if (fundingRecords.some((record) => !record)) {
      this.log.warn("Could not validate all four bundler funding records; resetting", {
        mint,
        fundingRecords,
      });
      await this.resetForNewToken(true);
      return;
    }

    const records = fundingRecords as BundlerFundingRecord[];
    const feePayers = new Set(records.map((record) => record.fundingFeePayer));
    if (feePayers.size !== 1) {
      this.log.warn("First four bundler funding tx feePayers did not match; resetting", {
        mint,
        fundingRecords: records,
      });
      await this.resetForNewToken(true);
      return;
    }

    const earliest = records.reduce((best, record) =>
      record.timestamp < best.timestamp ? record : best,
    );
    const largestFundingSol = Math.max(...records.map((record) => record.amountSol));
    const funderWallet = records[0].fundingFeePayer;
    this.bundlerFunderWatch = {
      mint,
      funderWallet,
      earliestFundingTimestamp: earliest.timestamp,
      earliestFundingSignature: earliest.fundingSignature,
      largestFundingSol,
      minTransferOutSol: largestFundingSol + BUNDLER_FUNDER_EXTRA_SOL,
      cursorSignature: earliest.fundingSignature,
      processedSignatures: new Set(records.map((record) => record.fundingSignature)),
      validOutSignatures: new Set<string>(),
      invalidOutSignatures: new Set<string>(),
      recipientWatches: new Map<string, FunderRecipientWatch>(),
      pendingTransferOut: null,
    };

    this.subscribeBundlerFunder(funderWallet);
    await this.syncBundlerFunderTransactions(true);

    this.log.warn("First-four bundler funding feePayer gate passed; shared feePayer watch started", {
      mint,
      sharedFeePayer: funderWallet,
      earliestFundingTimestamp: earliest.timestamp,
      earliestFundingSignature: earliest.fundingSignature,
      largestFundingSol,
      minTransferOutSol: this.bundlerFunderWatch.minTransferOutSol,
      fundingRecords: records,
    });
    void this.sendTelegramSafe(
      [
        `<b>✅ ${this.label} Shared FeePayer Locked</b>`,
        `Token: <code>${mint}</code>`,
        `FeePayer: <code>${funderWallet}</code>`,
        `Largest bundler funding: <b>${largestFundingSol.toFixed(4)} SOL</b>`,
        `Watching feePayer transfer-outs: <b>${this.bundlerFunderWatch.minTransferOutSol.toFixed(4)} SOL+</b>`,
        "",
        "A transfer-out only confirms after the next feePayer tx is not a SOL transfer-in.",
      ].join("\n"),
      "shared feePayer notification",
    );
  }

  private async findValidBundlerFundingRecord(
    mint: string,
    buy: EarlyInsiderBuy,
    preferredClientIndex: number,
  ): Promise<BundlerFundingRecord | null> {
    const txs = await this.withHeliusFallback(
      (client) =>
        client.getAddressTransferTransactionsDescBefore(
          buy.wallet,
          buy.signature,
          BUNDLER_FUNDER_TRANSFER_LIMIT,
        ),
      preferredClientIndex,
    );

    for (let index = 0; index < txs.length; index += 1) {
      const tx = txs[index];
      if (tx.type && tx.type !== "TRANSFER") {
        this.log.info("Bundler funding candidate rejected: tx is not TRANSFER", {
          mint,
          bundlerWallet: buy.wallet,
          candidateSignature: tx.signature,
          type: tx.type,
          index,
        });
        continue;
      }
      if (!tx.feePayer) {
        this.log.info("Bundler funding candidate rejected: missing feePayer", {
          mint,
          bundlerWallet: buy.wallet,
          candidateSignature: tx.signature,
          index,
        });
        continue;
      }
      const incoming = this.extractSolIncomingToWallet(tx, buy.wallet);
      if (!incoming) {
        this.log.info("Bundler funding candidate rejected: no incoming SOL to bundler", {
          mint,
          bundlerWallet: buy.wallet,
          candidateSignature: tx.signature,
          index,
          timestamp: tx.timestamp,
          nativeTransferCount: tx.nativeTransfers?.length ?? 0,
          tokenTransferCount: tx.tokenTransfers?.length ?? 0,
          nativeBalanceChange: (tx.accountData ?? []).find(
            (account) => account.account === buy.wallet,
          )?.nativeBalanceChange ?? null,
        });
        continue;
      }

      const currentBalance = await this.fetchSolBalanceAt(
        buy.wallet,
        tx.timestamp,
        preferredClientIndex,
      );
      if (currentBalance <= 0) {
        this.log.info("Bundler funding candidate rejected: balance-at candidate timestamp is not positive", {
          mint,
          bundlerWallet: buy.wallet,
          candidateSignature: tx.signature,
          fundingFeePayer: tx.feePayer,
          senderWallet: incoming.from,
          amountSol: incoming.amountSol,
          timestamp: tx.timestamp,
          currentBalance,
        });
        continue;
      }

      const olderTx = txs[index + 1];
      if (!olderTx) {
        this.log.info("Bundler funding candidate rejected: no older transfer in current page", {
          mint,
          bundlerWallet: buy.wallet,
          fundingSignature: tx.signature,
          fundingFeePayer: tx.feePayer,
          senderWallet: incoming.from,
          amountSol: incoming.amountSol,
          currentBalance,
          transferLimit: BUNDLER_FUNDER_TRANSFER_LIMIT,
        });
        continue;
      }
      const olderBalance = await this.fetchSolBalanceAt(
        buy.wallet,
        olderTx.timestamp,
        preferredClientIndex,
      );
      if (olderBalance !== 0) {
        this.log.info("Bundler funding candidate rejected: previous transfer balance was not zero", {
          mint,
          bundlerWallet: buy.wallet,
          candidateSignature: tx.signature,
          previousSignature: olderTx.signature,
          fundingFeePayer: tx.feePayer,
          senderWallet: incoming.from,
          amountSol: incoming.amountSol,
          candidateTimestamp: tx.timestamp,
          previousTimestamp: olderTx.timestamp,
          currentBalance,
          olderBalance,
        });
        continue;
      }

      this.log.warn("Bundler funding transfer validated", {
        mint,
        bundlerWallet: buy.wallet,
        bundlerBuySignature: buy.signature,
        fundingSignature: tx.signature,
        fundingFeePayer: tx.feePayer,
        senderWallet: incoming.from,
        amountSol: incoming.amountSol,
        timestamp: tx.timestamp,
        previousSignature: olderTx.signature,
        currentBalance,
        olderBalance,
      });
      return {
        bundlerWallet: buy.wallet,
        bundlerBuySignature: buy.signature,
        fundingSignature: tx.signature,
        fundingFeePayer: tx.feePayer,
        senderWallet: incoming.from,
        amountSol: incoming.amountSol,
        timestamp: tx.timestamp,
      };
    }

    this.log.warn("No valid first funding transfer found for bundler", {
      mint,
      bundlerWallet: buy.wallet,
      bundlerBuySignature: buy.signature,
      transferCount: txs.length,
    });
    return null;
  }

  private async fetchSolBalanceAt(
    wallet: string,
    timestamp: number,
    preferredClientIndex: number,
  ): Promise<number> {
    const balance = await this.withHeliusFallback(
      (client) =>
        client.getWalletBalanceAt(wallet, NATIVE_SOL_BALANCE_MINT, timestamp),
      preferredClientIndex,
    );
    const parsed = Number(balance.balance);
    return Number.isFinite(parsed) ? parsed : 0;
  }

  private extractSolIncomingToWallet(
    tx: HeliusTransaction,
    wallet: string,
  ): { from: string; amountSol: number } | null {
    const described = this.parseSolTransferDescription(tx.description);
    if (
      described &&
      described.to === wallet &&
      described.from !== wallet &&
      described.amountSol > 0
    ) {
      return {
        from: described.from,
        amountSol: described.amountSol,
      };
    }

    const nativeIncoming = (tx.nativeTransfers ?? [])
      .filter(
        (transfer) =>
          transfer.toUserAccount === wallet &&
          transfer.fromUserAccount !== wallet &&
          (transfer.amount ?? 0) > 0,
      )
      .sort((a, b) => (b.amount ?? 0) - (a.amount ?? 0))[0];
    if (!nativeIncoming) return null;

    const tokenTransfer = (tx.tokenTransfers ?? []).find(
      (transfer) =>
        transfer.mint === SOL_MINT &&
        transfer.toUserAccount === wallet &&
        transfer.fromUserAccount !== wallet,
    );
    if (tokenTransfer?.fromUserAccount) {
      return {
        from: tokenTransfer.fromUserAccount,
        amountSol: tokenTransfer.tokenAmount ?? 0,
      };
    }

    const accountChange = (tx.accountData ?? []).find(
      (account) => account.account === wallet,
    )?.nativeBalanceChange;
    if (accountChange !== undefined && accountChange <= 0) return null;
    return {
      from: nativeIncoming.fromUserAccount,
      amountSol: (nativeIncoming.amount ?? 0) / LAMPORTS_PER_SOL,
    };
  }

  private startPollLoop(): void {
    this.stopPollLoop();
    this.pollTimer = setInterval(() => {
      void this.runPollTick().catch((err) => {
        void this.heliusClient.handlePossibleRateLimitError(err);
        this.log.warn("Insider poll tick failed", {
          error: err instanceof Error ? err.message : String(err),
        });
      });
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
    if (phase === "pre_buy" && (this.preBuyStopped || this.buySubmitted))
      return;
    if (phase === "post_buy" && this.positionSellTriggered) return;

    this.isAxiomAtaPolling = true;
    try {
      await this.checkAxiomWatchedWalletAtaExits(mint, {
        phase,
        triggerSell: phase === "post_buy",
      });
    } catch (err) {
      void this.heliusClient.handlePossibleRateLimitError(err);
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
        if (this.bundlerFunderWatch) {
          await this.syncBundlerFunderTransactions();
          await this.syncFunderRecipientBatch();
        } else if (this.monitoredWallet && !this.insiderSellsReady) {
          await this.pollWallet(this.monitoredWallet, mint, "insider");
        } else if (this.authorityMonitor) {
          await this.syncAuthorityTransactions();
        } else if (this.monitoredWallet) {
          await this.scanAxiomSingleBuyTradersPreBuy(mint);
        }
      }

      if (this.phase === "holding") {
        await this.syncBundlerFunderTransactions();
        await this.syncFunderRecipientBatch();
        await this.syncAuthorityTransactions();
        await this.syncLargeBuyerAtaBalances();
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
    const txs = await this.withHeliusFallback((client) =>
      client.getWalletTransactionsDesc(wallet, INSIDER_HISTORY_LIMIT),
    );
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

  private async startBundlerMonitoring(
    wallets: string[],
    mint: string,
  ): Promise<void> {
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
          if (!logInfo.err)
            this.queueSignature(logInfo.signature, "bundler", wallet);
        },
        "processed",
      );
      this.bundlerLogsSubIds.set(wallet, subId);
    }

    for (const wallet of wallets) {
      await this.syncWalletHistory(
        wallet,
        mint,
        undefined,
        INSIDER_HISTORY_LIMIT,
        "bundler",
      );
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

  private async stopAuthorityMonitoring(): Promise<void> {
    if (this.authorityLogsSubId !== null) {
      const subId = this.authorityLogsSubId;
      this.authorityLogsSubId = null;
      await this.connection.removeOnLogsListener(subId).catch(() => undefined);
    }
    this.isAuthoritySyncing = false;
    this.authoritySyncPending = false;
    await this.stopAuthorityPatternAtaMonitoring();
    this.isAuthorityAtaChecking = false;
    this.authorityAtaCheckPending = false;
  }

  private async stopAuthorityPatternAtaMonitoring(): Promise<void> {
    for (const [ata, subId] of this.authorityPatternAtaSubIds) {
      await this.connection
        .removeAccountChangeListener(subId)
        .catch(() => undefined);
      this.authorityPatternAtaSubIds.delete(ata);
    }
  }

  private async retireLegacyInsiderWalletMonitoring(
    mint: string,
    authority: string,
  ): Promise<void> {
    const retiredWallet = this.monitoredWallet;
    await this.stopInsiderMonitoring();
    this.monitoredWallet = null;
    this.insiderState = null;
    this.insiderSellsReady = true;
    this.log.info(
      "Stopped legacy insider-wallet monitoring after authority flow started",
      {
        mint,
        authority,
        retiredWallet,
        action:
          "stop wallet history polling and WebSocket logs; keep authority and ATA monitoring active",
      },
    );
  }

  private async stopLargeBuyerMonitoring(): Promise<void> {
    for (const [ata, subId] of this.largeBuyerAtaSubIds) {
      await this.connection
        .removeAccountChangeListener(subId)
        .catch(() => undefined);
      this.largeBuyerAtaSubIds.delete(ata);
    }
    this.largeBuyerWatch = null;
    this.isLargeBuyerSyncing = false;
    this.largeBuyerSyncPending = false;
  }

  private subscribeAuthority(address: string): void {
    if (this.authorityLogsSubId !== null) return;
    this.authorityLogsSubId = this.connection.onLogs(
      new PublicKey(address),
      (logInfo) => {
        if (!logInfo.err) void this.syncAuthorityTransactions();
      },
      "processed",
    );
    this.log.info("Subscribed to lookup-table authority transactions", {
      address,
      batchLimit: AXIOM_AUTHORITY_BATCH_LIMIT,
    });
  }

  private subscribeAuthorityPatternAtas(state: AuthorityMonitorState): void {
    for (const [wallet, walletState] of state.patternStates) {
      for (const ata of walletState.atas) {
        const ataAddress = ata.toBase58();
        if (this.authorityPatternAtaSubIds.has(ataAddress)) continue;
        const subId = this.connection.onAccountChange(
          ata,
          (accountInfo) => {
            const amount =
              accountInfo.data.length >= 72
                ? accountInfo.data.readBigUInt64LE(64)
                : 0n;
            void this.applyAuthorityPatternAtaBalance(
              state,
              wallet,
              ataAddress,
              amount,
              `ATA subscription update for ${wallet}`,
            ).catch((err) => {
              this.log.warn(
                "Non-similar wallet ATA subscription check failed",
                {
                  mint: state.mint,
                  wallet,
                  error: err instanceof Error ? err.message : String(err),
                },
              );
            });
          },
          "processed",
        );
        this.authorityPatternAtaSubIds.set(ataAddress, subId);
      }
    }
    this.log.info("Subscribed to non-similar wallet ATAs", {
      mint: state.mint,
      wallets: [...state.nonSimilarWallets],
      ataSubscriptionCount: this.authorityPatternAtaSubIds.size,
    });
  }

  private subscribeLargeBuyerAtas(watch: LargeBuyerWatchState): void {
    for (const ata of watch.atas) {
      const ataAddress = ata.toBase58();
      if (this.largeBuyerAtaSubIds.has(ataAddress)) continue;
      const subId = this.connection.onAccountChange(
        ata,
        (accountInfo) => {
          const amount =
            accountInfo.data.length >= 72
              ? accountInfo.data.readBigUInt64LE(64)
              : 0n;
          void this.applyLargeBuyerAtaBalance(
            watch,
            ataAddress,
            amount,
            `ATA subscription update for ${ataAddress}`,
          );
        },
        "processed",
      );
      this.largeBuyerAtaSubIds.set(ataAddress, subId);
    }
    this.log.info("Subscribed to >=$200 authority buyer ATAs", {
      mint: watch.mint,
      wallet: watch.wallet,
      atas: watch.atas.map((ata) => ata.toBase58()),
      currentBalance: watch.currentBalance.toString(),
    });
  }

  private subscribeBundlerFunder(address: string): void {
    if (this.bundlerFunderLogsSubId !== null) return;
    this.bundlerFunderLogsSubId = this.connection.onLogs(
      new PublicKey(address),
      (logInfo) => {
        if (!logInfo.err) {
          this.scheduleBundlerFunderWsSync(logInfo.signature);
        }
      },
      "processed",
    );
    this.log.info("Subscribed to shared bundler funder transactions", {
      address,
      syncLimit: BUNDLER_FUNDER_SYNC_LIMIT,
      wsSyncDelayMs: BUNDLER_FUNDER_WS_SYNC_DELAY_MS,
    });
  }

  private scheduleBundlerFunderWsSync(signature: string): void {
    if (this.bundlerFunderWsSyncTimer) {
      this.bundlerFunderSyncPending = true;
      this.bundlerFunderSyncPendingForce = true;
      return;
    }
    this.bundlerFunderWsSyncTimer = setTimeout(() => {
      this.bundlerFunderWsSyncTimer = null;
      void this.syncBundlerFunderTransactions(true);
    }, BUNDLER_FUNDER_WS_SYNC_DELAY_MS);
    this.log.debug("Scheduled shared feePayer websocket sync", {
      signature,
      delayMs: BUNDLER_FUNDER_WS_SYNC_DELAY_MS,
    });
  }

  private subscribeFunderRecipient(wallet: string): void {
    if (this.recipientLogsSubIds.has(wallet)) return;
    const subId = this.connection.onLogs(
      new PublicKey(wallet),
      (logInfo) => {
        if (!logInfo.err) {
          this.markFunderRecipientDirty(wallet, logInfo.signature);
          void this.syncFunderRecipientBatch();
        }
      },
      "processed",
    );
    this.recipientLogsSubIds.set(wallet, subId);
    this.log.info("Subscribed to valid funder transfer-out recipient", {
      wallet,
    });
  }

  private async stopBundlerFunderMonitoring(): Promise<void> {
    if (this.bundlerFunderLogsSubId !== null) {
      const subId = this.bundlerFunderLogsSubId;
      this.bundlerFunderLogsSubId = null;
      await this.connection.removeOnLogsListener(subId).catch(() => undefined);
    }
    if (this.bundlerFunderWsSyncTimer) {
      clearTimeout(this.bundlerFunderWsSyncTimer);
      this.bundlerFunderWsSyncTimer = null;
    }
    for (const [wallet, subId] of this.recipientLogsSubIds) {
      await this.connection.removeOnLogsListener(subId).catch(() => undefined);
      this.recipientLogsSubIds.delete(wallet);
    }
    this.bundlerFunderWatch = null;
    this.isBundlerFunderSyncing = false;
    this.bundlerFunderSyncPending = false;
    this.bundlerFunderSyncPendingForce = false;
    this.dirtyFunderRecipients.clear();
    this.dirtyFunderRecipientSignatures.clear();
    this.isFunderRecipientBatchSyncing = false;
    this.funderRecipientBatchSyncPending = false;
  }

  private async syncBundlerFunderTransactions(force = false): Promise<void> {
    const state = this.bundlerFunderWatch;
    if (!state || this.positionSellTriggered) return;
    if (this.isBundlerFunderSyncing) {
      this.bundlerFunderSyncPending = true;
      if (force) this.bundlerFunderSyncPendingForce = true;
      return;
    }
    if (
      !force &&
      Date.now() - this.lastBundlerFunderSyncAt <
        BUNDLER_FUNDER_SYNC_MIN_INTERVAL_MS
    ) {
      return;
    }

    this.isBundlerFunderSyncing = true;
    this.lastBundlerFunderSyncAt = Date.now();
    try {
      const txs = await this.withHeliusFallback((client) =>
        client.getAddressTransactionsAsc(
          state.funderWallet,
          state.cursorSignature ?? undefined,
          BUNDLER_FUNDER_SYNC_LIMIT,
        ),
      );
      for (const tx of txs) {
        if (state.processedSignatures.has(tx.signature)) continue;
        state.processedSignatures.add(tx.signature);
        state.cursorSignature = tx.signature;
        await this.resolvePendingBundlerFunderCandidate(state, tx);
        await this.inspectBundlerFunderTransaction(state, tx);
      }
    } catch (err) {
      void this.heliusClient.handlePossibleRateLimitError(err);
      this.log.warn("Shared bundler funder sync failed", {
        mint: state.mint,
        funderWallet: state.funderWallet,
        error: err instanceof Error ? err.message : String(err),
      });
    } finally {
      this.isBundlerFunderSyncing = false;
      if (this.bundlerFunderSyncPending) {
        this.bundlerFunderSyncPending = false;
        const pendingForce = this.bundlerFunderSyncPendingForce;
        this.bundlerFunderSyncPendingForce = false;
        void this.syncBundlerFunderTransactions(pendingForce);
      }
    }
  }

  private async inspectBundlerFunderTransaction(
    state: BundlerFunderWatchState,
    tx: HeliusTransaction,
  ): Promise<void> {
    const transferOut = this.extractSolTransferOutFromWallet(
      tx,
      state.funderWallet,
      state.minTransferOutSol,
    );
    if (!transferOut) return;
    if (this.hasSolIncomingToWallet(tx, state.funderWallet)) {
      this.log.info("Skipping funder transfer-out because same tx also has transfer-in", {
        mint: state.mint,
        funderWallet: state.funderWallet,
        signature: tx.signature,
        amountSol: transferOut.amountSol,
        recipient: transferOut.to,
      });
      return;
    }
    if (
      state.validOutSignatures.has(tx.signature) ||
      state.invalidOutSignatures.has(tx.signature) ||
      state.pendingTransferOut?.signature === tx.signature
    ) {
      return;
    }

    state.pendingTransferOut = {
      signature: tx.signature,
      recipient: transferOut.to,
      amountSol: transferOut.amountSol,
      timestamp: tx.timestamp,
    };

    this.log.warn("Shared feePayer transfer-out candidate pending next-tx check", {
      mint: state.mint,
      funderWallet: state.funderWallet,
      recipient: transferOut.to,
      amountSol: transferOut.amountSol,
      minTransferOutSol: state.minTransferOutSol,
      signature: tx.signature,
      rule:
        "candidate is invalid if the next funder transaction is a SOL transfer-in",
    });

    void this.sendTelegramSafe(
      [
        `<b>🟡 ${this.label} FeePayer Transfer-Out Candidate</b>`,
        `Token: <code>${state.mint}</code>`,
        `FeePayer: <code>${state.funderWallet}</code>`,
        `Recipient: <code>${transferOut.to}</code>`,
        `Amount: <b>${transferOut.amountSol.toFixed(4)} SOL</b>`,
        `Threshold: <b>${state.minTransferOutSol.toFixed(4)} SOL</b>`,
        `Tx: <code>${tx.signature}</code>`,
        "",
        "Waiting for the next funder tx. If it is a SOL transfer-in, this candidate is invalid.",
      ].join("\n"),
      "pending feePayer transfer-out notification",
    );
  }

  private async resolvePendingBundlerFunderCandidate(
    state: BundlerFunderWatchState,
    nextTx: HeliusTransaction,
  ): Promise<void> {
    const pending = state.pendingTransferOut;
    if (!pending || nextTx.signature === pending.signature) return;

    state.pendingTransferOut = null;
    if (this.hasSolIncomingToWallet(nextTx, state.funderWallet)) {
      state.invalidOutSignatures.add(pending.signature);
      this.log.warn(
        "Shared feePayer transfer-out candidate invalidated by immediate next SOL transfer-in",
        {
          mint: state.mint,
          funderWallet: state.funderWallet,
          candidateSignature: pending.signature,
          nextSignature: nextTx.signature,
          recipient: pending.recipient,
          amountSol: pending.amountSol,
        },
      );
      void this.sendTelegramSafe(
        [
          `<b>🔴 ${this.label} FeePayer Candidate Invalid</b>`,
          `Token: <code>${state.mint}</code>`,
          `FeePayer: <code>${state.funderWallet}</code>`,
          `Candidate tx: <code>${pending.signature}</code>`,
          `Next tx: <code>${nextTx.signature}</code>`,
          "Reason: the immediate next funder transaction was a SOL transfer-in.",
          "",
          "Continuing to watch for the next valid transfer-out.",
        ].join("\n"),
        "invalid feePayer transfer-out notification",
      );
      return;
    }

    state.validOutSignatures.add(pending.signature);
    let watch = state.recipientWatches.get(pending.recipient);
    if (!watch) {
      if (state.recipientWatches.size >= BUNDLER_FUNDER_MAX_RECIPIENT_WATCHES) {
        this.log.warn("Shared feePayer recipient watch cap reached; confirmed transfer-out recipient not watched", {
          mint: state.mint,
          funderWallet: state.funderWallet,
          recipient: pending.recipient,
          candidateSignature: pending.signature,
          cap: BUNDLER_FUNDER_MAX_RECIPIENT_WATCHES,
        });
        return;
      }
      watch = {
        wallet: pending.recipient,
        fundingSignature: pending.signature,
        outAmountSol: pending.amountSol,
        heliusPreferredIndex:
          state.recipientWatches.size % Math.max(1, this.heliusPool.length || 1),
        tokenActions: [],
        boughtAmount: 0,
        soldAmount: 0,
        followSellExit: true,
        firstBuySignature: null,
      };
      state.recipientWatches.set(pending.recipient, watch);
      this.subscribeFunderRecipient(pending.recipient);
    }

    this.log.warn("Shared feePayer transfer-out confirmed by next-tx check", {
      mint: state.mint,
      funderWallet: state.funderWallet,
      recipient: pending.recipient,
      amountSol: pending.amountSol,
      minTransferOutSol: state.minTransferOutSol,
      candidateSignature: pending.signature,
      nextSignature: nextTx.signature,
      buySubmitted: this.buySubmitted,
    });

    await this.emitBundlerFunderBuy(state, watch, pending.signature);
    this.markFunderRecipientDirty(pending.recipient);
    await this.syncFunderRecipientBatch(true);
  }

  private extractSolTransferOutFromWallet(
    tx: HeliusTransaction,
    wallet: string,
    minAmountSol: number,
  ): { to: string; amountSol: number } | null {
    const nativeChain = this.extractNativeTransferOutChain(
      tx,
      wallet,
      minAmountSol,
    );
    if (nativeChain) return nativeChain;

    const described = this.parseSolTransferDescription(tx.description);
    if (
      described &&
      described.from === wallet &&
      described.to !== wallet &&
      described.amountSol >= minAmountSol
    ) {
      return {
        to: described.to,
        amountSol: described.amountSol,
      };
    }

    const tokenTransfer = (tx.tokenTransfers ?? [])
      .filter(
        (transfer) =>
          transfer.mint === SOL_MINT &&
          transfer.fromUserAccount === wallet &&
          transfer.toUserAccount !== wallet &&
          (transfer.tokenAmount ?? 0) >= minAmountSol,
      )
      .sort((a, b) => (b.tokenAmount ?? 0) - (a.tokenAmount ?? 0))[0];
    if (tokenTransfer?.toUserAccount) {
      return {
        to: tokenTransfer.toUserAccount,
        amountSol: tokenTransfer.tokenAmount ?? 0,
      };
    }

    const nativeTransfer = (tx.nativeTransfers ?? [])
      .filter(
        (transfer) =>
          transfer.fromUserAccount === wallet &&
          transfer.toUserAccount !== wallet &&
          (transfer.amount ?? 0) / LAMPORTS_PER_SOL >= minAmountSol,
      )
      .sort((a, b) => (b.amount ?? 0) - (a.amount ?? 0))[0];
    if (!nativeTransfer) return null;
    return {
      to: nativeTransfer.toUserAccount,
      amountSol: (nativeTransfer.amount ?? 0) / LAMPORTS_PER_SOL,
    };
  }

  private extractNativeTransferOutChain(
    tx: HeliusTransaction,
    wallet: string,
    minAmountSol: number,
  ): { to: string; amountSol: number } | null {
    const transfers = (tx.nativeTransfers ?? []).filter(
      (transfer) =>
        transfer.fromUserAccount &&
        transfer.toUserAccount &&
        (transfer.amount ?? 0) > 0,
    );
    if (!transfers.length) return null;

    const first = transfers[0];
    const last = transfers[transfers.length - 1];
    if (first.fromUserAccount !== wallet) return null;
    if (last.toUserAccount === wallet) return null;

    const amountLamports = Math.max(
      ...transfers.map((transfer) => transfer.amount ?? 0),
    );
    const amountSol = amountLamports / LAMPORTS_PER_SOL;
    if (amountSol < minAmountSol) return null;

    return {
      to: last.toUserAccount,
      amountSol,
    };
  }

  private hasSolIncomingToWallet(tx: HeliusTransaction, wallet: string): boolean {
    const described = this.parseSolTransferDescription(tx.description);
    if (
      described &&
      described.to === wallet &&
      described.from !== wallet &&
      described.amountSol > 0
    ) {
      return true;
    }

    return (
      (tx.tokenTransfers ?? []).some(
        (transfer) =>
          transfer.mint === SOL_MINT &&
          transfer.toUserAccount === wallet &&
          transfer.fromUserAccount !== wallet,
      ) ||
      (tx.nativeTransfers ?? []).some(
        (transfer) =>
          transfer.toUserAccount === wallet &&
          transfer.fromUserAccount !== wallet &&
          (transfer.amount ?? 0) > 0,
      )
    );
  }

  private parseSolTransferDescription(
    description?: string,
  ): { from: string; to: string; amountSol: number } | null {
    if (!description) return null;
    const match = description.match(
      /^([1-9A-HJ-NP-Za-km-z]{32,44}) transferred ([0-9]+(?:\.[0-9]+)?) SOL to ([1-9A-HJ-NP-Za-km-z]{32,44})\.$/,
    );
    if (!match) return null;
    const amountSol = Number(match[2]);
    if (!Number.isFinite(amountSol)) return null;
    return {
      from: match[1],
      amountSol,
      to: match[3],
    };
  }

  private async emitBundlerFunderBuy(
    state: BundlerFunderWatchState,
    watch: FunderRecipientWatch,
    signature: string,
  ): Promise<void> {
    if (
      this.buySubmitted ||
      this.buyDisabled ||
      this.isBuyExecuting ||
      this.isBuyGateEvaluating
    ) {
      return;
    }
    this.isBuyGateEvaluating = true;
    try {
      const currentMc = await this.gmgnClient.fetchTokenMarketCapUsd(state.mint);
      if (currentMc === null) {
        this.log.warn(
          "Confirmed shared feePayer transfer-out found, but current market cap is unavailable; waiting before buy",
          { mint: state.mint, recipient: watch.wallet, signature },
        );
        return;
      }
      if (currentMc < INSIDER_RUG_MARKET_CAP_USD) {
        this.log.warn(
          "Confirmed shared feePayer transfer-out found, but token is below rug threshold; resetting instead of buying",
          {
            mint: state.mint,
            recipient: watch.wallet,
            currentMc,
            rugThresholdUsd: INSIDER_RUG_MARKET_CAP_USD,
          },
        );
        await this.resetForNewToken(true);
        return;
      }

      const newExitMc = currentMc * (1 + this.exitPercent / 100);
      this.setExitMc(newExitMc);
      this.setEntryMc(currentMc);
      this.setBuyExecuting(true);
      this.buySubmitted = true;
      this.preBuyStopped = true;
      this.emit("buyTrigger", {
        followedWallet: this.followedWallet!,
        mint: state.mint,
        signature,
        buySol: this.buySol,
        entryMc: currentMc,
        monitoredWallet: watch.wallet,
        tradersListStr: [
          "<b>Shared Bundler FeePayer Buy Gate Passed</b>",
          `FeePayer: <code>${state.funderWallet}</code>`,
          `Recipient: <code>${watch.wallet}</code>`,
          `Transfer-out: <b>${watch.outAmountSol.toFixed(4)} SOL</b>`,
          `Threshold: <b>${state.minTransferOutSol.toFixed(4)} SOL</b>`,
          `Trigger tx: <code>${signature}</code>`,
          `Current MC: <b>$${currentMc.toLocaleString()}</b>`,
          "",
          "Next-tx check: confirmed; the immediate next feePayer tx was not a SOL transfer-in.",
          "Sell rule: if this recipient's first token action is a buy, sell when it sells at least 50% of that position. If its second token action is a sell, disable the % MC profit exit and stick to the 50% recipient-sell exit. If its second token action is another buy, use only MC/rug exits.",
        ].join("\n"),
      });
    } finally {
      this.isBuyGateEvaluating = false;
    }
  }

  private async syncFunderRecipientTransactions(
    wallet: string,
    signature?: string,
  ): Promise<void> {
    const state = this.bundlerFunderWatch;
    const watch = state?.recipientWatches.get(wallet);
    if (!state || !watch || this.positionSellTriggered) return;
    try {
      const txs = signature
        ? await this.withHeliusFallback(
            (client) => client.getTransactionsBySignatures([signature]),
            watch.heliusPreferredIndex,
          )
        : await this.withHeliusFallback(
            (client) =>
              client.getWalletTransactionsDesc(wallet, INSIDER_HISTORY_LIMIT),
            watch.heliusPreferredIndex,
          );
      const sorted = [...txs].reverse();
      for (const tx of sorted) {
        if (tx.timestamp < state.earliestFundingTimestamp) continue;
        await this.applyFunderRecipientTransaction(state, watch, tx);
      }
    } catch (err) {
      void this.heliusClient.handlePossibleRateLimitError(err);
      this.log.warn("Valid transfer-out recipient sync failed", {
        mint: state.mint,
        wallet,
        error: err instanceof Error ? err.message : String(err),
      });
    }
  }

  private markFunderRecipientDirty(wallet: string, signature?: string): void {
    const state = this.bundlerFunderWatch;
    if (!state?.recipientWatches.has(wallet)) return;
    this.dirtyFunderRecipients.add(wallet);
    if (signature) {
      let signatures = this.dirtyFunderRecipientSignatures.get(wallet);
      if (!signatures) {
        signatures = new Set<string>();
        this.dirtyFunderRecipientSignatures.set(wallet, signatures);
      }
      signatures.add(signature);
    }
    this.log.debug("Marked shared feePayer recipient for batch sync", {
      mint: state.mint,
      wallet,
      signature,
      dirtyCount: this.dirtyFunderRecipients.size,
    });
  }

  private async syncFunderRecipientBatch(force = false): Promise<void> {
    const state = this.bundlerFunderWatch;
    if (!state || this.positionSellTriggered) return;
    if (state.recipientWatches.size === 0) return;
    if (this.isFunderRecipientBatchSyncing) {
      this.funderRecipientBatchSyncPending = true;
      return;
    }
    if (
      !force &&
      Date.now() - this.lastFunderRecipientBatchSyncAt <
        BUNDLER_FUNDER_RECIPIENT_SYNC_INTERVAL_MS
    ) {
      return;
    }

    this.isFunderRecipientBatchSyncing = true;
    this.lastFunderRecipientBatchSyncAt = Date.now();
    try {
      const dirty = [...this.dirtyFunderRecipients].filter((wallet) =>
        state.recipientWatches.has(wallet),
      );
      if (!force && dirty.length === 0) return;
      const wallets = (dirty.length > 0 ? dirty : [...state.recipientWatches.keys()])
        .slice(0, BUNDLER_FUNDER_RECIPIENT_BATCH_SIZE);
      for (const wallet of wallets) {
        this.dirtyFunderRecipients.delete(wallet);
        const signatures = this.dirtyFunderRecipientSignatures.get(wallet);
        this.dirtyFunderRecipientSignatures.delete(wallet);
        if (signatures?.size) {
          for (const signature of signatures) {
            await this.syncFunderRecipientTransactions(wallet, signature);
          }
        } else {
          await this.syncFunderRecipientTransactions(wallet);
        }
      }
      this.log.info("Shared feePayer recipient batch sync completed", {
        mint: state.mint,
        syncedWallets: wallets,
        remainingDirty: this.dirtyFunderRecipients.size,
        watchedRecipients: state.recipientWatches.size,
        batchSize: BUNDLER_FUNDER_RECIPIENT_BATCH_SIZE,
      });
    } finally {
      this.isFunderRecipientBatchSyncing = false;
      if (this.dirtyFunderRecipients.size > 0) {
        this.funderRecipientBatchSyncPending = false;
        void this.syncFunderRecipientBatch();
      } else if (this.funderRecipientBatchSyncPending) {
        this.funderRecipientBatchSyncPending = false;
      }
    }
  }

  private async applyFunderRecipientTransaction(
    state: BundlerFunderWatchState,
    watch: FunderRecipientWatch,
    tx: HeliusTransaction,
  ): Promise<void> {
    if (!this.isRelevantMintTx(tx, state.mint)) return;
    const action = this.classifyTx(tx, watch.wallet, state.mint);
    if (action !== "buy" && action !== "sell") return;
    if (watch.tokenActions.some((existing) => existing.signature === tx.signature)) return;

    const amount = this.extractTokenAmountForWallet(tx, watch.wallet, state.mint, action);
    watch.tokenActions.push({ kind: action, signature: tx.signature, amount });
    if (action === "buy") {
      if (!watch.firstBuySignature) {
        watch.firstBuySignature = tx.signature;
        watch.boughtAmount += amount;
        this.log.warn("Valid transfer-out recipient bought token; 50% sell watch armed", {
          mint: state.mint,
          wallet: watch.wallet,
          signature: tx.signature,
          boughtAmount: watch.boughtAmount,
        });
        void this.sendTelegramSafe(
          [
            `<b>🟢 ${this.label} Recipient Bought Token</b>`,
            `Token: <code>${state.mint}</code>`,
            `Recipient: <code>${watch.wallet}</code>`,
            `Buy tx: <code>${tx.signature}</code>`,
            `Tracked amount: <b>${watch.boughtAmount.toLocaleString()}</b>`,
            "",
            "50% recipient-sell exit is now armed unless this wallet's next token action is another buy.",
          ].join("\n"),
          "recipient first-buy notification",
        );
      } else {
        watch.followSellExit = false;
        this.log.warn(
          "Valid transfer-out recipient made a second buy; disabling 50% recipient-sell exit for this wallet",
          {
            mint: state.mint,
            wallet: watch.wallet,
            signature: tx.signature,
            firstBuySignature: watch.firstBuySignature,
          },
        );
        void this.sendTelegramSafe(
          [
            `<b>🟠 ${this.label} Recipient Second Buy</b>`,
            `Token: <code>${state.mint}</code>`,
            `Recipient: <code>${watch.wallet}</code>`,
            `Second buy tx: <code>${tx.signature}</code>`,
            "",
            "50% recipient-sell exit disabled for this wallet. MC target and rug exits remain active.",
          ].join("\n"),
          "recipient second-buy notification",
        );
      }
      return;
    }

    if (!watch.firstBuySignature || !watch.followSellExit) return;
    if (watch.tokenActions.length === 2 && !this.profitExitDisabled) {
      this.profitExitDisabled = true;
      this.log.warn(
        "Recipient second token action is a sell; disabling MC profit exit and keeping 50% recipient-sell exit",
        {
          mint: state.mint,
          wallet: watch.wallet,
          firstBuySignature: watch.firstBuySignature,
          secondActionSignature: tx.signature,
        },
      );
      void this.sendTelegramSafe(
        [
          `<b>🔵 ${this.label} Exit Mode Changed</b>`,
          `Token: <code>${state.mint}</code>`,
          `Recipient: <code>${watch.wallet}</code>`,
          `First buy: <code>${watch.firstBuySignature}</code>`,
          `Second token action: <b>SELL</b>`,
          `Sell tx: <code>${tx.signature}</code>`,
          "",
          "% MC profit exit disabled for this position. Bot will wait for this recipient to sell at least 50% of its first tracked buy. Rug protection remains active.",
        ].join("\n"),
        "recipient second-action sell notification",
      );
    }
    watch.soldAmount += amount;
    const soldRatio = watch.boughtAmount > 0 ? watch.soldAmount / watch.boughtAmount : 0;
    this.log.info("Valid transfer-out recipient sell observed", {
      mint: state.mint,
      wallet: watch.wallet,
      signature: tx.signature,
      soldAmount: watch.soldAmount,
      boughtAmount: watch.boughtAmount,
      soldRatio,
    });
    if (soldRatio >= 0.5 && this.phase === "holding") {
      await this.triggerPositionSell(
        state.mint,
        `Shared feePayer recipient ${watch.wallet} sold at least 50% of first buy`,
        [
          "<b>🚨 Shared-Funder Recipient Sold 50%</b>",
          `Token: <code>${state.mint}</code>`,
          `Recipient: <code>${watch.wallet}</code>`,
          `First buy: <code>${watch.firstBuySignature}</code>`,
          `Sell tx: <code>${tx.signature}</code>`,
          `Sold: <b>${(soldRatio * 100).toFixed(2)}%</b> of first tracked position`,
        ],
        tx.signature,
      );
    }
  }

  private extractTokenAmountForWallet(
    tx: HeliusTransaction,
    wallet: string,
    mint: string,
    action: "buy" | "sell",
  ): number {
    return (tx.tokenTransfers ?? [])
      .filter((transfer) => {
        if (transfer.mint !== mint) return false;
        return action === "buy"
          ? transfer.toUserAccount === wallet
          : transfer.fromUserAccount === wallet;
      })
      .reduce((sum, transfer) => sum + (transfer.tokenAmount ?? 0), 0);
  }

  private queueSignature(
    signature: string,
    context: "insider" | "bundler",
    bundlerWallet?: string,
  ): void {
    if (
      this.processedSignatures.has(signature) ||
      this.queuedSignatures.has(signature)
    ) {
      return;
    }
    this.queuedSignatures.add(signature);
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
    for (const signature of signatures) {
      this.queuedSignatures.delete(signature);
    }
    const fresh = signatures.filter((s) => !this.processedSignatures.has(s));
    if (fresh.length === 0) return;

    try {
      const txs = await this.withHeliusFallback((client) =>
        client.getTransactionsBySignatures(fresh),
      );
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
      void this.heliusClient.handlePossibleRateLimitError(err);
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
    await this.stopAuthorityMonitoring();
    await this.stopLargeBuyerMonitoring();
    await this.stopBundlerFunderMonitoring();
    this.axiomTraderWatchActive = false;
    this.clearAxiomWatchedWallets();
    if (this.batchTimer) {
      clearTimeout(this.batchTimer);
      this.batchTimer = null;
    }
    this.processedSignatures.clear();
    this.queuedSignatures.clear();
    this.pendingSignaturesBatch = [];
    this.isSwitchingInsiderWallet = false;
    this.authorityMonitor = null;
    this.largeBuyerWatch = null;
    this.bundlerFunderWatch = null;
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
        if (transfer.mint === mint && transfer.toUserAccount === wallet)
          return "buy";
      }
      for (const transfer of tx.tokenTransfers ?? []) {
        if (transfer.mint === SOL_MINT && transfer.toUserAccount === wallet)
          return "sell";
        if (transfer.mint === mint && transfer.fromUserAccount === wallet)
          return "sell";
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
    const txs = await this.withHeliusFallback((client) =>
      client.getWalletTransactionsDesc(wallet, limit),
    );
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

  private async handleInsiderTransaction(
    tx: HeliusTransaction,
    mint: string,
  ): Promise<void> {
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
      await this.switchToTransferredWallet(
        wallet,
        recipient,
        mint,
        tx.signature,
      );
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
    // independent GMGN/authority discovery alive after it completes.
    this.startPollLoop();
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
    sourceWallet: string,
    newWallet: string,
    mint: string,
    transferSignature: string,
  ): Promise<void> {
    if (this.isSwitchingInsiderWallet) {
      this.log.debug("Ignoring concurrent insider wallet switch", {
        mint,
        sourceWallet,
        newWallet,
        transferSignature,
      });
      return;
    }
    if (this.monitoredWallet !== sourceWallet) {
      this.log.debug(
        "Ignoring stale transfer from a wallet no longer monitored",
        {
          mint,
          sourceWallet,
          currentMonitoredWallet: this.monitoredWallet,
          newWallet,
          transferSignature,
        },
      );
      return;
    }
    if (newWallet === sourceWallet || this.insiderWalletChain.has(newWallet)) {
      this.log.warn("Ignoring self/cyclic insider transfer", {
        mint,
        sourceWallet,
        newWallet,
        transferSignature,
        monitoredWalletChain: [...this.insiderWalletChain],
      });
      return;
    }

    this.isSwitchingInsiderWallet = true;
    try {
      await this.stopInsiderMonitoring();
      this.monitoredWallet = newWallet;
      this.insiderWalletChain.add(newWallet);
      this.insiderState = {
        wallet: newWallet,
        sellCount: 0,
        isTransferred: true,
      };
      this.insiderSellsReady = false;
      this.processedSignatures.add(transferSignature);

      await this.syncWalletHistory(
        newWallet,
        mint,
        transferSignature,
        INSIDER_HISTORY_LIMIT,
        "insider",
      );
      this.startInsiderMonitoring();

      void this.sendTelegramSafe(
        [
          `<b>🔀 ${this.label} Transfer Detected</b>`,
          `Token: <code>${mint}</code>`,
          `From: <code>${sourceWallet}</code>`,
          `Now monitoring: <code>${newWallet}</code>`,
          `Insider sells: <b>${this.insiderState.sellCount}</b> / ${this.requiredInsiderSells}`,
        ]
          .filter(Boolean)
          .join("\n"),
        "transfer notification",
      );
    } finally {
      this.isSwitchingInsiderWallet = false;
    }
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

  private parseBundlerCandidate(
    entry: Record<string, unknown>,
  ): BundlerMatch | null {
    const buyUsd = this.parseBuyVolumeUsd(entry);
    const buyTxCount = this.parseBuyTxCount(entry);
    const address = entry.address as string | undefined;
    if (!address || buyUsd === null || buyTxCount === null) return null;
    if (buyUsd < this.bundlerBuyMinUsd || buyUsd > this.bundlerBuyMaxUsd)
      return null;
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

  private collectAxiomSingleBuyMatches(list: Array<Record<string, unknown>>): {
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
      const tags = Array.isArray(entry.tags) ? (entry.tags as string[]) : [];

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
        cumulativeSkippedMultiBuy: this.axiomSkippedMultiBuyWallets.size,
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
      largestSimilarBalanceGroup?.wallets.filter((wallet) => wallet.soldAny) ??
      [];
    const largestGroupSoldAnyWalletAddresses = new Set(
      largestGroupSoldAnyWalletBalances.map((wallet) => wallet.address),
    );
    const largestGroupSoldAnyWallets = watched.filter((wallet) =>
      largestGroupSoldAnyWalletAddresses.has(wallet.address),
    );
    const largestGroupHoldingCount =
      largestGroupExistingAtaWalletCount - largestGroupSoldAnyWallets.length;
    const largestGroupSoldAnyRatio =
      largestGroupExistingAtaWalletCount > 0
        ? largestGroupSoldAnyWallets.length / largestGroupExistingAtaWalletCount
        : 0;
    const previousMaxLargestSimilarBalanceGroupCount =
      this.maxObservedLargestSimilarBalanceGroupCount;
    this.maxObservedLargestSimilarBalanceGroupCount = Math.max(
      this.maxObservedLargestSimilarBalanceGroupCount,
      largestGroupExistingAtaWalletCount,
    );
    const cumulativeSkippedMultiBuy = this.axiomSkippedMultiBuyWallets.size;
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
        wallet.tokenStatus === "sold_all" && wallet.sellType === "single_sell",
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
            holdingCount: nearZeroWalletBalances.length - nearZeroSoldAnyCount,
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
      AXIOM_AUTHORITY_EARLY_PROBE_GROUP_SIZE
    ) {
      buyGateFailedConditions.push(
        `group_size: ${largestGroupExistingAtaWalletCount} < ${AXIOM_AUTHORITY_EARLY_PROBE_GROUP_SIZE}`,
      );
    }
    if (largestGroupSoldAnyWallets.length > AXIOM_BUY_MAX_SOLD_ANY_WALLETS) {
      buyGateFailedConditions.push(
        `sold_any: ${largestGroupSoldAnyWallets.length} > ${AXIOM_BUY_MAX_SOLD_ANY_WALLETS}`,
      );
    }
    if (
      this.authorityProbeFailedAtTwo &&
      largestGroupExistingAtaWalletCount < AXIOM_AUTHORITY_FALLBACK_GROUP_SIZE
    ) {
      buyGateFailedConditions.push(
        `authority_probe_wait: group_size ${largestGroupExistingAtaWalletCount} < fallback ${AXIOM_AUTHORITY_FALLBACK_GROUP_SIZE}`,
      );
    }
    const sellGateFailedConditions: string[] = [];
    if (
      largestGroupSoldAnyWallets.length < AXIOM_EXIT_SOLD_ANY_WALLET_THRESHOLD
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

    this.log.info(
      `Axiom watched-wallet ATA poll [${options.phase}] — ${largestGroupSoldAnyWallets.length}/${largestGroupExistingAtaWalletCount} sold any in largest similar-SOL group`,
      {
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
                earlyAuthorityProbeGroupSize:
                  AXIOM_AUTHORITY_EARLY_PROBE_GROUP_SIZE,
                fallbackAuthorityProbeGroupSize:
                  AXIOM_AUTHORITY_FALLBACK_GROUP_SIZE,
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
      },
    );

    if (options.phase === "pre_buy") {
      await this.evaluateAxiomAtaBuyGate(
        mint,
        largestSimilarBalanceGroup,
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
      while (sorted[right].solBalanceUsd - sorted[left].solBalanceUsd > 1) {
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
    largestSimilarBalanceGroup: SimilarSolBalanceGroup | null,
    largestGroupSoldAnyCount: number,
    overallExistingAtaWalletCount: number,
    watchedCount: number,
    missingAtaWalletCount: number,
    cumulativeSkippedMultiBuy: number,
  ): Promise<void> {
    const largestGroupExistingAtaWalletCount =
      largestSimilarBalanceGroup?.walletCount ?? 0;
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
    const shouldAttemptEarlyAuthority =
      largestGroupExistingAtaWalletCount >=
        AXIOM_AUTHORITY_EARLY_PROBE_GROUP_SIZE &&
      !this.authorityProbeFailedAtTwo;
    const shouldAttemptFallbackAuthority =
      this.authorityProbeFailedAtTwo &&
      largestGroupExistingAtaWalletCount >= AXIOM_AUTHORITY_FALLBACK_GROUP_SIZE;

    if (
      this.phase !== "pre_buy" ||
      this.preBuyStopped ||
      this.buySubmitted ||
      this.isBuyExecuting ||
      this.isBuyGateEvaluating ||
      this.buyDisabled ||
      this.authorityMonitor !== null ||
      !largestSimilarBalanceGroup ||
      (!shouldAttemptEarlyAuthority && !shouldAttemptFallbackAuthority) ||
      largestGroupSoldAnyCount > AXIOM_BUY_MAX_SOLD_ANY_WALLETS
    ) {
      return;
    }

    this.isBuyGateEvaluating = true;
    try {
      const candidates = largestSimilarBalanceGroup.wallets
        .slice(0, AXIOM_AUTHORITY_CANDIDATE_COUNT)
        .map((groupWallet) => {
          const watchedWallet = this.axiomWatchedWallets.get(
            groupWallet.address,
          );
          if (!watchedWallet) {
            throw new Error(
              `ATA largest-group wallet ${groupWallet.address} is missing from the cumulative watched-wallet map`,
            );
          }
          return {
            address: watchedWallet.address,
            buyUsd: watchedWallet.buyUsd,
            tags: watchedWallet.tags,
          };
        });

      this.log.warn(
        "Axiom ATA largest similar-SOL group passed — starting lookup-table authority trigger flow",
        {
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
          frozenCandidateWallets: candidates,
          largestSimilarSolGroup: {
            balanceUsdRange: `${largestSimilarBalanceGroup.minUsd.toFixed(2)}-${largestSimilarBalanceGroup.maxUsd.toFixed(2)}`,
            spreadUsd: largestSimilarBalanceGroup.spreadUsd,
            wallets: largestSimilarBalanceGroup.wallets.map((wallet) => ({
              address: wallet.address,
              solBalanceUsd: wallet.solBalanceUsd,
              tokenStatus: wallet.tokenStatus,
              soldAny: wallet.soldAny,
            })),
          },
        },
      );

      const isFallbackAttempt = this.authorityProbeFailedAtTwo;
      const started = await this.startAuthorityTriggerFlow(
        mint,
        candidates,
        isFallbackAttempt,
      );
      if (!started && !isFallbackAttempt) {
        this.authorityProbeFailedAtTwo = true;
        this.log.info(
          "Early authority probe failed; waiting for largest similar-SOL group count 5 before retrying",
          {
            mint,
            attemptedGroupCount: largestGroupExistingAtaWalletCount,
            fallbackGroupCount: AXIOM_AUTHORITY_FALLBACK_GROUP_SIZE,
          },
        );
      }
      if (this.authorityMonitor) {
        this.stopAxiomAtaPollLoop();
      }
    } finally {
      this.isBuyGateEvaluating = false;
    }
  }

  private async startAuthorityTriggerFlow(
    mint: string,
    candidates: AuthorityCandidateWallet[],
    resetIfAuthorityMissing: boolean,
  ): Promise<boolean> {
    if (this.authorityMonitor || this.buySubmitted || this.preBuyStopped) {
      return false;
    }

    const candidateAddresses = new Set(
      candidates.map((wallet) => wallet.address),
    );
    let mintTransactions = await this.withHeliusFallback((client) =>
      client.getAddressTransactionsAsc(mint, undefined, 100),
    );
    let firstBuy = mintTransactions.find((tx) =>
      this.getMintBuyActors(tx, mint).some((buy) =>
        candidateAddresses.has(buy.wallet),
      ),
    );

    if (!firstBuy) {
      const walletTransactions = await Promise.all(
        candidates.map((candidate) =>
          this.withHeliusFallback((client) =>
            client.getWalletTransactionsDesc(candidate.address, 50),
          ),
        ),
      );
      mintTransactions = walletTransactions
        .flat()
        .filter((tx) => this.isRelevantMintTx(tx, mint))
        .sort((a, b) => a.slot - b.slot);
      firstBuy = mintTransactions.find((tx) =>
        this.getMintBuyActors(tx, mint).some((buy) =>
          candidateAddresses.has(buy.wallet),
        ),
      );
    }

    if (!firstBuy) {
      if (!resetIfAuthorityMissing) {
        this.log.warn(
          "Early authority probe could not find a candidate buy transaction; waiting for fallback group count",
          {
            mint,
            candidateWallets: candidates.map((candidate) => candidate.address),
            fallbackGroupCount: AXIOM_AUTHORITY_FALLBACK_GROUP_SIZE,
          },
        );
        return false;
      }
      this.log.warn(
        "Fallback authority probe could not find a candidate buy transaction; skipping token",
        {
          mint,
          candidateWallets: candidates.map((candidate) => candidate.address),
        },
      );
      await this.resetForNewToken(true);
      return false;
    }

    let firstBuyTransaction = firstBuy;
    let authority: string | null = null;
    for (
      let attempt = 1;
      attempt <= AXIOM_AUTHORITY_EXTRACTION_MAX_ATTEMPTS;
      attempt += 1
    ) {
      const [enhancedFirstBuy] =
        await this.withHeliusFallback((client) =>
          client.getTransactionsBySignatures([firstBuy.signature]),
        );
      firstBuyTransaction = enhancedFirstBuy ?? firstBuy;
      authority = this.extractLookupTableAuthority(firstBuyTransaction);
      if (authority) break;

      this.log.warn(
        "Lookup-table authority extraction failed; retrying enhanced transaction",
        {
          mint,
          signature: firstBuy.signature,
          attempt,
          maxAttempts: AXIOM_AUTHORITY_EXTRACTION_MAX_ATTEMPTS,
          remainingAttempts: AXIOM_AUTHORITY_EXTRACTION_MAX_ATTEMPTS - attempt,
        },
      );
      if (attempt < AXIOM_AUTHORITY_EXTRACTION_MAX_ATTEMPTS) {
        await new Promise((resolve) => setTimeout(resolve, attempt * 500));
      }
    }

    if (!authority) {
      if (!resetIfAuthorityMissing) {
        this.log.warn(
          "Early lookup-table authority probe failed; token remains active until similar-SOL group reaches fallback count",
          {
            mint,
            signature: firstBuy.signature,
            attempts: AXIOM_AUTHORITY_EXTRACTION_MAX_ATTEMPTS,
            fallbackGroupCount: AXIOM_AUTHORITY_FALLBACK_GROUP_SIZE,
          },
        );
        return false;
      }
      this.log.warn(
        "Token is incompatible with lookup-table authority strategy after three extraction failures; skipping and resetting",
        {
          mint,
          signature: firstBuy.signature,
          attempts: AXIOM_AUTHORITY_EXTRACTION_MAX_ATTEMPTS,
          action:
            "reset token flow and wait for the next followed-wallet token",
        },
      );
      void this.sendTelegramSafe(
        [
          `<b>⏭️ ${this.label} Token Skipped</b>`,
          `Token: <code>${mint}</code>`,
          `First candidate buy: <code>${firstBuy.signature}</code>`,
          `Reason: lookup-table authority was not found after <b>${AXIOM_AUTHORITY_EXTRACTION_MAX_ATTEMPTS}</b> attempts.`,
          "This token is incompatible with the authority strategy. Flow reset for the next token.",
        ].join("\n"),
        "incompatible authority token notification",
      );
      await this.resetForNewToken(true);
      return false;
    }

    this.authorityMonitor = {
      mint,
      candidates,
      authority,
      firstBuySignature: firstBuy.signature,
      firstBuyTransaction,
      initialTransactions: [firstBuyTransaction],
      initialCursorSignature: null,
      cursorSignature: null,
      initialReady: false,
      earlyProbeCompleted: false,
      decisionMode: "pending",
      processedSignatures: new Set([firstBuy.signature]),
      nonSimilarWallets: new Set(),
      patternStates: new Map(),
    };
    await this.retireLegacyInsiderWalletMonitoring(mint, authority);
    this.subscribeAuthority(authority);
    this.log.info("Lookup-table authority trigger flow started", {
      mint,
      candidateWallets: candidates.map((wallet) => wallet.address),
      firstBuyWallets: this.getMintBuyActors(firstBuyTransaction, mint).map(
        (buy) => buy.wallet,
      ),
      firstBuySignature: firstBuy.signature,
      authority,
      initialAfterLimit: AXIOM_AUTHORITY_INITIAL_AFTER_LIMIT,
    });
    await this.syncAuthorityTransactions();
    return true;
  }

  private extractLookupTableAuthority(tx: HeliusTransaction): string | null {
    const lookupInstruction = (tx.instructions ?? []).find(
      (instruction) =>
        instruction.programId === ADDRESS_LOOKUP_TABLE_PROGRAM &&
        (instruction.accounts?.length ?? 0) >= 2,
    );
    const instructionAuthority = lookupInstruction?.accounts?.[1];
    if (instructionAuthority) return instructionAuthority;

    const lookupTransfer = (tx.nativeTransfers ?? []).find(
      (transfer) => transfer.amount === 222_720,
    );
    return lookupTransfer?.fromUserAccount ?? null;
  }

  private getMintBuyActors(
    tx: HeliusTransaction,
    mint: string,
  ): Array<{ wallet: string; amount: number }> {
    const buys = new Map<string, number>();
    for (const transfer of tx.tokenTransfers ?? []) {
      if (transfer.mint !== mint || !transfer.toUserAccount) continue;
      if (tx.feePayer && transfer.toUserAccount !== tx.feePayer) continue;
      buys.set(
        transfer.toUserAccount,
        (buys.get(transfer.toUserAccount) ?? 0) + (transfer.tokenAmount ?? 0),
      );
    }
    return [...buys].map(([wallet, amount]) => ({ wallet, amount }));
  }

  private getMintSellActors(
    tx: HeliusTransaction,
    mint: string,
  ): Array<{ wallet: string; amount: number }> {
    const sells = new Map<string, number>();
    for (const transfer of tx.tokenTransfers ?? []) {
      if (transfer.mint !== mint || !transfer.fromUserAccount) continue;
      if (tx.feePayer && transfer.fromUserAccount !== tx.feePayer) continue;
      sells.set(
        transfer.fromUserAccount,
        (sells.get(transfer.fromUserAccount) ?? 0) +
          (transfer.tokenAmount ?? 0),
      );
    }
    return [...sells].map(([wallet, amount]) => ({ wallet, amount }));
  }

  private async syncAuthorityTransactions(): Promise<void> {
    const state = this.authorityMonitor;
    if (!state) return;
    if (this.isAuthoritySyncing) {
      this.authoritySyncPending = true;
      return;
    }
    if (
      Date.now() - this.lastAuthoritySyncAt <
      AXIOM_AUTHORITY_MIN_SYNC_INTERVAL_MS
    ) {
      return;
    }

    this.isAuthoritySyncing = true;
    this.lastAuthoritySyncAt = Date.now();
    try {
      if (!state.initialReady) {
        // ── Early-probe: check the first 4 txs (tx #1 + 3 more) for a $200+ buy ──
        if (!state.earlyProbeCompleted) {
          const earlyAfter = await this.withHeliusFallback((client) =>
            client.getAddressTransactionsAsc(
              state.authority,
              state.firstBuySignature,
              AXIOM_AUTHORITY_EARLY_PROBE_TX_COUNT,
            ),
          );
          const uniqueEarly = earlyAfter.filter(
            (tx) => !state.processedSignatures.has(tx.signature),
          );
          if (uniqueEarly.length >= AXIOM_AUTHORITY_EARLY_PROBE_TX_COUNT) {
            // We have at least 4 txs — scan them for a $200+ buy
            const earlyWindow = [
              state.firstBuyTransaction,
              ...uniqueEarly.slice(0, AXIOM_AUTHORITY_EARLY_PROBE_TX_COUNT),
            ].sort((a, b) => a.slot - b.slot);

            const solPriceUsd = await this.getCachedSolPriceUsd();
            if (solPriceUsd !== null) {
              for (const tx of earlyWindow) {
                if (
                  state.processedSignatures.has(tx.signature) &&
                  tx.signature !== state.firstBuySignature
                ) {
                  continue;
                }
                for (const buy of this.getMintBuyActors(tx, state.mint)) {
                  const buySol = this.estimateWalletSolSpent(tx, buy.wallet);
                  if (buySol === null) continue;
                  const buyUsd = buySol * solPriceUsd;
                  if (buyUsd < AXIOM_AUTHORITY_LARGE_BUY_MIN_USD) continue;

                  // Found a $200+ buy in the first 4 txs — go direct immediately
                  this.log.warn(
                    "Early 4-tx probe found >=$200 buy — skipping 15-tx window, entering direct_200_buy mode immediately",
                    {
                      mint: state.mint,
                      authority: state.authority,
                      wallet: buy.wallet,
                      signature: tx.signature,
                      buySol,
                      buyUsd: Number(buyUsd.toFixed(2)),
                    },
                  );

                  // Mark all early txs as processed and set cursor
                  for (const etx of earlyWindow) {
                    state.processedSignatures.add(etx.signature);
                  }
                  state.cursorSignature =
                    earlyWindow[earlyWindow.length - 1].signature;
                  state.initialCursorSignature = state.cursorSignature;
                  state.initialTransactions = earlyWindow;
                  state.initialReady = true;
                  state.earlyProbeCompleted = true;
                  state.decisionMode = "direct_200_buy";
                  state.nonSimilarWallets = new Set();

                  this.largeBuyerWatch = await this.createLargeBuyerAtaWatch(
                    state.mint,
                    buy.wallet,
                    tx.signature,
                    buyUsd,
                    buy.amount,
                  );
                  this.subscribeLargeBuyerAtas(this.largeBuyerWatch);
                  await this.emitDirectAuthorityBuyerBuy(
                    state,
                    this.largeBuyerWatch,
                  );
                  return;
                }
              }
            }
            // No $200+ buy found in early window — mark probe done, fall through to full 15-tx wait
            state.earlyProbeCompleted = true;
            this.log.info(
              "Early 4-tx probe complete — no $200+ buy found, waiting for full 15-tx window",
              { mint: state.mint, authority: state.authority },
            );
          } else {
            // Don't have 4 txs yet — wait
            this.log.info("Waiting for early 4-tx authority probe window", {
              mint: state.mint,
              authority: state.authority,
              collected: 1 + uniqueEarly.length,
              needed: 1 + AXIOM_AUTHORITY_EARLY_PROBE_TX_COUNT,
            });
            return;
          }
        }

        // ── Full 15-tx window (only reached if early probe found nothing) ──
        const after = await this.withHeliusFallback((client) =>
          client.getAddressTransactionsAsc(
            state.authority,
            state.firstBuySignature,
            AXIOM_AUTHORITY_INITIAL_AFTER_LIMIT,
          ),
        );
        const uniqueAfter = after.filter(
          (tx) => !state.processedSignatures.has(tx.signature),
        );
        if (uniqueAfter.length < AXIOM_AUTHORITY_INITIAL_AFTER_LIMIT) {
          this.log.info(
            "Waiting for complete 15-transaction authority window",
            {
              mint: state.mint,
              authority: state.authority,
              collected: 1 + uniqueAfter.length,
              required: AXIOM_AUTHORITY_INITIAL_TX_COUNT,
            },
          );
          return;
        }

        state.initialTransactions = [
          state.firstBuyTransaction,
          ...uniqueAfter.slice(0, AXIOM_AUTHORITY_INITIAL_AFTER_LIMIT),
        ].sort((a, b) => a.slot - b.slot);
        for (const tx of state.initialTransactions) {
          state.processedSignatures.add(tx.signature);
        }
        state.cursorSignature =
          state.initialTransactions[
            state.initialTransactions.length - 1
          ].signature;
        state.initialCursorSignature = state.cursorSignature;
        state.initialReady = true;

        await this.evaluateInitialAuthorityWindow(state);
        return;
      }
      if (!state.cursorSignature) {
        return;
      }

      if (this.phase === "pre_buy" && !this.buySubmitted) {
        if (state.decisionMode === "direct_200_buy") {
          if (this.largeBuyerWatch) {
            await this.emitDirectAuthorityBuyerBuy(state, this.largeBuyerWatch);
            return;
          }
          let cursor = state.cursorSignature;
          while (true) {
            const batch = await this.withHeliusFallback((client) =>
              client.getAddressTransactionsAsc(
                state.authority,
                cursor,
                AXIOM_AUTHORITY_BATCH_LIMIT,
              ),
            );
            if (batch.length === 0) break;
            for (const tx of batch) {
              if (state.processedSignatures.has(tx.signature)) continue;
              state.processedSignatures.add(tx.signature);
              const found = await this.inspectDirectAuthorityBuyer(state, tx);
              if (found) return;
            }
            cursor = batch[batch.length - 1].signature;
            state.cursorSignature = cursor;
            if (batch.length < AXIOM_AUTHORITY_BATCH_LIMIT) break;
          }
          return;
        }
        await this.checkAuthorityPatternAtaBalances(
          state,
          "authority transaction or periodic poll",
        );
        return;
      }

      if (
        this.phase !== "holding" ||
        !this.activePosition ||
        this.largeBuyerWatch
      ) {
        return;
      }

      let cursor = state.cursorSignature;
      while (true) {
        const batch = await this.withHeliusFallback((client) =>
          client.getAddressTransactionsAsc(
            state.authority,
            cursor,
            AXIOM_AUTHORITY_BATCH_LIMIT,
          ),
        );
        const fresh = batch.filter(
          (tx) => !state.processedSignatures.has(tx.signature),
        );
        for (const tx of fresh) {
          state.processedSignatures.add(tx.signature);
          await this.inspectAuthorityTransactionForLargeBuyer(state, tx);
          if (this.largeBuyerWatch) break;
        }
        if (batch.length === 0) break;
        cursor = batch[batch.length - 1].signature;
        state.cursorSignature = cursor;
        if (
          this.largeBuyerWatch ||
          batch.length < AXIOM_AUTHORITY_BATCH_LIMIT
        ) {
          break;
        }
      }
    } catch (err) {
      void this.heliusClient.handlePossibleRateLimitError(err);
      this.log.warn("Lookup-table authority transaction sync failed", {
        mint: state.mint,
        authority: state.authority,
        error: err instanceof Error ? err.message : String(err),
      });
    } finally {
      this.isAuthoritySyncing = false;
      if (this.authoritySyncPending) {
        this.authoritySyncPending = false;
        void this.syncAuthorityTransactions();
      }
    }
  }

  private async evaluateInitialAuthorityWindow(
    state: AuthorityMonitorState,
  ): Promise<void> {
    const solPriceUsd = await this.getCachedSolPriceUsd();
    if (solPriceUsd === null) {
      throw new Error(
        "Could not fetch SOL price for initial authority USD-buy grouping",
      );
    }

    const buyUsdByWallet = new Map<string, number>();
    const unpricedBuyWallets = new Set<string>();
    for (const tx of state.initialTransactions) {
      for (const buy of this.getMintBuyActors(tx, state.mint)) {
        const buySol = this.estimateWalletSolSpent(tx, buy.wallet);
        if (buySol === null) {
          unpricedBuyWallets.add(buy.wallet);
          continue;
        }
        buyUsdByWallet.set(
          buy.wallet,
          (buyUsdByWallet.get(buy.wallet) ?? 0) + buySol * solPriceUsd,
        );
      }
    }
    const walletBuys = [...buyUsdByWallet]
      .map(([wallet, buyUsd]) => ({ wallet, buyUsd }))
      .sort((a, b) => a.buyUsd - b.buyUsd);

    let left = 0;
    let largestGroup: typeof walletBuys = [];
    for (let right = 0; right < walletBuys.length; right += 1) {
      while (
        walletBuys[right].buyUsd - walletBuys[left].buyUsd >
        AXIOM_AUTHORITY_SIMILAR_BUY_SPREAD_USD
      ) {
        left += 1;
      }
      const candidate = walletBuys.slice(left, right + 1);
      if (candidate.length > largestGroup.length) largestGroup = candidate;
    }
    const similarWallets = new Set(largestGroup.map((entry) => entry.wallet));
    const nonSimilarWallets = new Set(
      walletBuys
        .map((entry) => entry.wallet)
        .filter((wallet) => !similarWallets.has(wallet)),
    );
    state.decisionMode =
      largestGroup.length < AXIOM_AUTHORITY_NORMAL_BUY_GROUP_MIN_COUNT
        ? "direct_200_buy"
        : "normal_non_similar";
    state.nonSimilarWallets = nonSimilarWallets;
    state.patternStates.clear();
    if (state.decisionMode === "direct_200_buy") {
      this.log.info(
        "Initial authority grouping selected direct $200 buyer path",
        {
          mint: state.mint,
          authority: state.authority,
          largestSimilarUsdBuyGroupCount: largestGroup.length,
          normalFlowMinimum: AXIOM_AUTHORITY_NORMAL_BUY_GROUP_MIN_COUNT,
          afterSignature: state.initialCursorSignature,
          action:
            "wait for first later $200+ authority buy, buy immediately, then sell when that same wallet sells all",
        },
      );
      this.log.info("Initial 15-transaction authority window analyzed", {
        mint: state.mint,
        authority: state.authority,
        transactionSignatures: state.initialTransactions.map(
          (tx) => tx.signature,
        ),
        solPriceUsd: Number(solPriceUsd.toFixed(2)),
        decisionMode: state.decisionMode,
        groupingRule: `largest group whose USD buy values have a maximum spread of $${AXIOM_AUTHORITY_SIMILAR_BUY_SPREAD_USD.toFixed(2)}`,
        largestSimilarUsdBuyGroup: largestGroup.map((entry) => ({
          address: entry.wallet,
          buyUsd: Number(entry.buyUsd.toFixed(2)),
        })),
        allPricedWalletBuys: walletBuys.map((entry) => ({
          address: entry.wallet,
          buyUsd: Number(entry.buyUsd.toFixed(2)),
        })),
        unpricedBuyWallets: [...unpricedBuyWallets],
      });
      return;
    }

    const baselineSnapshot = await this.fetchAuthorityPatternAtaBalances(
      state.mint,
      [...nonSimilarWallets],
    );
    for (const wallet of nonSimilarWallets) {
      const atas = [TOKEN_PROGRAM_ID, TOKEN_2022_PROGRAM_ID].map(
        (tokenProgram) =>
          getAssociatedTokenAddressSync(
            new PublicKey(state.mint),
            new PublicKey(wallet),
            true,
            tokenProgram,
          ),
      );
      const ataBalances = new Map(
        atas.map((ata) => [
          ata.toBase58(),
          baselineSnapshot.byAta.get(ata.toBase58()) ?? 0n,
        ]),
      );
      const baselineBalance = baselineSnapshot.byWallet.get(wallet) ?? 0n;
      state.patternStates.set(wallet, {
        atas,
        ataBalances,
        baselineBalance,
        currentBalance: baselineBalance,
        seenPositiveBalance: baselineBalance > 0n,
        completed: false,
        completedBalance: null,
      });
    }
    this.subscribeAuthorityPatternAtas(state);

    this.log.info("Initial 15-transaction authority window analyzed", {
      mint: state.mint,
      authority: state.authority,
      transactionSignatures: state.initialTransactions.map(
        (tx) => tx.signature,
      ),
      solPriceUsd: Number(solPriceUsd.toFixed(2)),
      decisionMode: state.decisionMode,
      groupingRule: `largest group whose USD buy values have a maximum spread of $${AXIOM_AUTHORITY_SIMILAR_BUY_SPREAD_USD.toFixed(2)}`,
      largestSimilarUsdBuyGroup: largestGroup.map((entry) => ({
        address: entry.wallet,
        buyUsd: Number(entry.buyUsd.toFixed(2)),
      })),
      allPricedWalletBuys: walletBuys.map((entry) => ({
        address: entry.wallet,
        buyUsd: Number(entry.buyUsd.toFixed(2)),
      })),
      unpricedBuyWallets: [...unpricedBuyWallets],
      nonSimilarWallets: [...nonSimilarWallets],
      patternStates: [...state.patternStates].map(([wallet, pattern]) => ({
        wallet,
        baselineBalance: pattern.baselineBalance.toString(),
        ataBalances: Object.fromEntries(
          [...pattern.ataBalances].map(([ata, balance]) => [
            ata,
            balance.toString(),
          ]),
        ),
        currentBalance: pattern.currentBalance.toString(),
        seenPositiveBalance: pattern.seenPositiveBalance,
        completed: pattern.completed,
        completedBalance: pattern.completedBalance?.toString() ?? null,
      })),
      trigger: null,
    });
    this.log.info(
      "Initial authority grouping complete; waiting for every non-similar wallet ATA to sell all",
      {
        mint: state.mint,
        authority: state.authority,
        afterSignature: state.initialCursorSignature,
        nonSimilarWallets: [...state.patternStates].map(
          ([wallet, pattern]) => ({
            address: wallet,
            baselineBalance: pattern.baselineBalance.toString(),
            atas: pattern.atas.map((ata) => ata.toBase58()),
          }),
        ),
      },
    );
  }

  private async fetchAuthorityPatternAtaBalances(
    mint: string,
    wallets: string[],
  ): Promise<{
    byWallet: Map<string, bigint>;
    byAta: Map<string, bigint>;
  }> {
    const lookups = wallets.flatMap((wallet) =>
      [TOKEN_PROGRAM_ID, TOKEN_2022_PROGRAM_ID].map((tokenProgram) => ({
        wallet,
        ata: getAssociatedTokenAddressSync(
          new PublicKey(mint),
          new PublicKey(wallet),
          true,
          tokenProgram,
        ),
      })),
    );
    const byWallet = new Map(wallets.map((wallet) => [wallet, 0n]));
    const byAta = new Map<string, bigint>();
    for (let start = 0; start < lookups.length; start += 100) {
      const chunk = lookups.slice(start, start + 100);
      const infos = await this.connection.getMultipleAccountsInfo(
        chunk.map((lookup) => lookup.ata),
        "processed",
      );
      for (let index = 0; index < chunk.length; index += 1) {
        const info = infos[index];
        if (!info) continue;
        const amount =
          info.data.length >= 72 ? info.data.readBigUInt64LE(64) : 0n;
        const wallet = chunk[index].wallet;
        byAta.set(chunk[index].ata.toBase58(), amount);
        byWallet.set(wallet, (byWallet.get(wallet) ?? 0n) + amount);
      }
    }
    return { byWallet, byAta };
  }

  private async checkAuthorityPatternAtaBalances(
    state: AuthorityMonitorState,
    source: string,
  ): Promise<void> {
    if (
      this.phase !== "pre_buy" ||
      this.buySubmitted ||
      state.patternStates.size === 0
    ) {
      return;
    }
    if (this.isAuthorityAtaChecking) {
      this.authorityAtaCheckPending = true;
      return;
    }

    this.isAuthorityAtaChecking = true;
    try {
      const snapshot = await this.fetchAuthorityPatternAtaBalances(state.mint, [
        ...state.patternStates.keys(),
      ]);
      for (const [wallet, walletState] of state.patternStates) {
        for (const ata of walletState.atas) {
          const ataAddress = ata.toBase58();
          walletState.ataBalances.set(
            ataAddress,
            snapshot.byAta.get(ataAddress) ?? 0n,
          );
        }
        await this.applyAuthorityPatternWalletBalance(
          state,
          wallet,
          snapshot.byWallet.get(wallet) ?? 0n,
          source,
        );
      }

      await this.tryTriggerAllAuthorityWalletsCompleted(state);
    } finally {
      this.isAuthorityAtaChecking = false;
      if (this.authorityAtaCheckPending) {
        this.authorityAtaCheckPending = false;
        void this.checkAuthorityPatternAtaBalances(
          state,
          "queued ATA update",
        ).catch((err) => {
          this.log.warn("Queued non-similar wallet ATA check failed", {
            mint: state.mint,
            error: err instanceof Error ? err.message : String(err),
          });
        });
      }
    }
  }

  private async applyAuthorityPatternAtaBalance(
    state: AuthorityMonitorState,
    wallet: string,
    ataAddress: string,
    amount: bigint,
    source: string,
  ): Promise<void> {
    const walletState = state.patternStates.get(wallet);
    if (!walletState || walletState.completed) return;
    walletState.ataBalances.set(ataAddress, amount);
    const combinedBalance = [...walletState.ataBalances.values()].reduce(
      (total, balance) => total + balance,
      0n,
    );
    await this.applyAuthorityPatternWalletBalance(
      state,
      wallet,
      combinedBalance,
      source,
    );
    await this.tryTriggerAllAuthorityWalletsCompleted(state);
  }

  private async applyAuthorityPatternWalletBalance(
    state: AuthorityMonitorState,
    wallet: string,
    currentBalance: bigint,
    source: string,
  ): Promise<void> {
    const walletState = state.patternStates.get(wallet);
    if (!walletState || walletState.completed) return;

    const previousBalance = walletState.currentBalance;
    walletState.currentBalance = currentBalance;
    if (currentBalance > 0n) {
      walletState.seenPositiveBalance = true;
      return;
    }
    if (
      !walletState.seenPositiveBalance ||
      previousBalance <= 0n ||
      currentBalance !== 0n
    ) {
      return;
    }

    walletState.completed = true;
    walletState.completedBalance = currentBalance;
    const completedWallets = [...state.patternStates].filter(
      ([, pattern]) => pattern.completed,
    );
    this.log.info("Non-similar wallet sold all — wallet frozen as completed", {
      mint: state.mint,
      authority: state.authority,
      wallet,
      source,
      baselineBalance: walletState.baselineBalance.toString(),
      previousBalance: previousBalance.toString(),
      currentBalance: currentBalance.toString(),
      completedCount: completedWallets.length,
      requiredCount: state.patternStates.size,
      completedWallets: completedWallets.map(([address]) => address),
    });
  }

  private async tryTriggerAllAuthorityWalletsCompleted(
    state: AuthorityMonitorState,
  ): Promise<void> {
    const allCompleted =
      state.patternStates.size > 0 &&
      [...state.patternStates.values()].every(
        (walletState) => walletState.completed,
      );
    if (!allCompleted) return;

    await this.emitAuthorityPatternBuy(state, {
      signature: state.cursorSignature ?? "AUTHORITY_ALL_ATAS_SOLD_ALL",
      completedWallets: [...state.patternStates.keys()],
    });
  }

  private async emitAuthorityPatternBuy(
    state: AuthorityMonitorState,
    trigger: {
      signature: string;
      completedWallets: string[];
    },
  ): Promise<void> {
    if (
      this.buySubmitted ||
      this.buyDisabled ||
      this.isBuyExecuting ||
      this.isBuyGateEvaluating
    ) {
      return;
    }
    this.isBuyGateEvaluating = true;
    try {
      const currentMc = await this.gmgnClient.fetchTokenMarketCapUsd(
        state.mint,
      );
      if (currentMc === null) {
        this.log.warn(
          "All non-similar wallets sold all, but current market cap is unavailable; waiting before buy",
          {
            mint: state.mint,
            completedWallets: trigger.completedWallets,
          },
        );
        return;
      }
      if (currentMc < INSIDER_RUG_MARKET_CAP_USD) {
        this.log.warn(
          "All non-similar wallets sold all, but token is below rug threshold; resetting instead of buying",
          {
            mint: state.mint,
            currentMc,
            rugThresholdUsd: INSIDER_RUG_MARKET_CAP_USD,
            completedWallets: trigger.completedWallets,
          },
        );
        void this.sendTelegramSafe(
          [
            `<b>🧹 ${this.label} Rug Reset — Buy Skipped</b>`,
            `Token: <code>${state.mint}</code>`,
            `All non-similar wallets sold all: <b>${trigger.completedWallets.length}</b>`,
            `Current MC: <b>$${currentMc.toLocaleString()}</b>`,
            `Required MC: <b>$${INSIDER_RUG_MARKET_CAP_USD.toLocaleString()}</b> or higher`,
            "Flow reset for the next token.",
          ].join("\n"),
          "all non-similar wallets completed but token rugged",
        );
        await this.resetForNewToken(true);
        return;
      }

      await this.stopPreBuyMonitoring();
      const newExitMc = currentMc * (1 + this.exitPercent / 100);
      this.setExitMc(newExitMc);
      this.setEntryMc(currentMc);
      this.setBuyExecuting(true);
      this.buySubmitted = true;
      const candidateLines = state.candidates.map(
        (candidate, index) =>
          `${index + 1}. <code>${candidate.address}</code> — <b>$${candidate.buyUsd.toFixed(2)}</b>`,
      );

      this.log.warn(
        "Lookup-table authority non-similar wallet pattern passed — triggering buy",
        {
          mint: state.mint,
          authority: state.authority,
          firstBuySignature: state.firstBuySignature,
          cursorSignature: state.cursorSignature,
          triggerSignature: trigger.signature,
          completedWallets: trigger.completedWallets,
          completedCount: trigger.completedWallets.length,
          candidateWallets: state.candidates,
          currentMc,
          exitMc: newExitMc,
        },
      );
      this.emit("buyTrigger", {
        followedWallet: this.followedWallet!,
        mint: state.mint,
        signature: trigger.signature,
        buySol: this.buySol,
        entryMc: currentMc,
        tradersListStr: [
          "<b>Lookup-Table Authority Buy Gate Passed</b>",
          `Authority: <code>${state.authority}</code>`,
          `First candidate buy: <code>${state.firstBuySignature}</code>`,
          `Pattern: <b>All ${trigger.completedWallets.length} non-similar wallets sold all</b>`,
          `Current MC: <b>$${currentMc.toLocaleString()}</b>`,
          "",
          "<b>Frozen completed non-similar wallets:</b>",
          ...trigger.completedWallets.map(
            (wallet, index) => `${index + 1}. <code>${wallet}</code>`,
          ),
          "",
          "<b>Five ATA-group candidate wallets:</b>",
          ...candidateLines,
        ].join("\n"),
      });
    } finally {
      this.isBuyGateEvaluating = false;
    }
  }

  private async inspectDirectAuthorityBuyer(
    state: AuthorityMonitorState,
    tx: HeliusTransaction,
  ): Promise<boolean> {
    const solPriceUsd = await this.getCachedSolPriceUsd();
    if (solPriceUsd === null) return false;

    for (const buy of this.getMintBuyActors(tx, state.mint)) {
      const buySol = this.estimateWalletSolSpent(tx, buy.wallet);
      if (buySol === null) continue;
      const buyUsd = buySol * solPriceUsd;
      if (buyUsd < AXIOM_AUTHORITY_LARGE_BUY_MIN_USD) continue;

      this.largeBuyerWatch = await this.createLargeBuyerAtaWatch(
        state.mint,
        buy.wallet,
        tx.signature,
        buyUsd,
        buy.amount,
      );
      state.cursorSignature = tx.signature;
      this.subscribeLargeBuyerAtas(this.largeBuyerWatch);
      this.log.warn(
        "Direct authority path found >=$200 buy — triggering our buy and reserving wallet for sell-all exit",
        {
          mint: state.mint,
          authority: state.authority,
          wallet: buy.wallet,
          signature: tx.signature,
          buySol,
          buyUsd: Number(buyUsd.toFixed(2)),
          tokenAmount: buy.amount,
          ataBalance: this.largeBuyerWatch.currentBalance.toString(),
        },
      );
      await this.emitDirectAuthorityBuyerBuy(state, this.largeBuyerWatch);
      return true;
    }
    return false;
  }

  private async emitDirectAuthorityBuyerBuy(
    state: AuthorityMonitorState,
    watch: LargeBuyerWatchState,
  ): Promise<void> {
    if (
      this.buySubmitted ||
      this.buyDisabled ||
      this.isBuyExecuting ||
      this.isBuyGateEvaluating
    ) {
      return;
    }
    this.isBuyGateEvaluating = true;
    try {
      const currentMc = await this.gmgnClient.fetchTokenMarketCapUsd(
        state.mint,
      );
      if (currentMc === null) {
        this.log.warn(
          "Direct $200 authority buyer found, but current market cap is unavailable; waiting before buy",
          {
            mint: state.mint,
            wallet: watch.wallet,
            qualifyingSignature: watch.qualifyingSignature,
          },
        );
        return;
      }
      if (currentMc < INSIDER_RUG_MARKET_CAP_USD) {
        this.log.warn(
          "Direct $200 authority buyer found, but token is below rug threshold; resetting instead of buying",
          {
            mint: state.mint,
            wallet: watch.wallet,
            buyUsd: watch.buyUsd,
            currentMc,
            rugThresholdUsd: INSIDER_RUG_MARKET_CAP_USD,
          },
        );
        await this.resetForNewToken(true);
        return;
      }

      await this.stopPreBuyMonitoring();
      const newExitMc = currentMc * (1 + this.exitPercent / 100);
      this.setExitMc(newExitMc);
      this.setEntryMc(currentMc);
      this.setBuyExecuting(true);
      this.buySubmitted = true;
      this.emit("buyTrigger", {
        followedWallet: this.followedWallet!,
        mint: state.mint,
        signature: watch.qualifyingSignature,
        buySol: this.buySol,
        entryMc: currentMc,
        monitoredWallet: watch.wallet,
        tradersListStr: [
          "<b>Direct $200 Authority Buy Gate Passed</b>",
          `Authority: <code>${state.authority}</code>`,
          `Largest similar USD-buy group: <b>less than ${AXIOM_AUTHORITY_NORMAL_BUY_GROUP_MIN_COUNT}</b>`,
          `Trigger wallet: <code>${watch.wallet}</code>`,
          `Trigger buy: <b>$${watch.buyUsd.toFixed(2)}</b>`,
          `Trigger tx: <code>${watch.qualifyingSignature}</code>`,
          `Current MC: <b>$${currentMc.toLocaleString()}</b>`,
          "",
          "Sell rule: sell our position when this same wallet sells all of its tracked token position.",
        ].join("\n"),
      });
    } finally {
      this.isBuyGateEvaluating = false;
    }
  }

  private async inspectAuthorityTransactionForLargeBuyer(
    state: AuthorityMonitorState,
    tx: HeliusTransaction,
  ): Promise<void> {
    const solPriceUsd = await this.getCachedSolPriceUsd();
    if (solPriceUsd === null) return;
    for (const buy of this.getMintBuyActors(tx, state.mint)) {
      const buySol = this.estimateWalletSolSpent(tx, buy.wallet);
      if (buySol === null) continue;
      const buyUsd = buySol * solPriceUsd;
      if (buyUsd < AXIOM_AUTHORITY_LARGE_BUY_MIN_USD) continue;

      this.largeBuyerWatch = await this.createLargeBuyerAtaWatch(
        state.mint,
        buy.wallet,
        tx.signature,
        buyUsd,
        buy.amount,
      );
      this.subscribeLargeBuyerAtas(this.largeBuyerWatch);
      this.log.warn(
        "Authority monitor found >=$200 buy; watching buyer ATA balance until sell-all",
        {
          mint: state.mint,
          authority: state.authority,
          wallet: buy.wallet,
          signature: tx.signature,
          buySol,
          buyUsd: Number(buyUsd.toFixed(2)),
          tokenAmount: buy.amount,
          ataBalance: this.largeBuyerWatch.currentBalance.toString(),
          atas: this.largeBuyerWatch.atas.map((ata) => ata.toBase58()),
        },
      );
      await this.syncLargeBuyerAtaBalances();
      return;
    }
  }

  private async createLargeBuyerAtaWatch(
    mint: string,
    wallet: string,
    qualifyingSignature: string,
    buyUsd: number,
    boughtAmount: number,
  ): Promise<LargeBuyerWatchState> {
    const atas = [TOKEN_PROGRAM_ID, TOKEN_2022_PROGRAM_ID].map((tokenProgram) =>
      getAssociatedTokenAddressSync(
        new PublicKey(mint),
        new PublicKey(wallet),
        true,
        tokenProgram,
      ),
    );
    const infos = await this.connection.getMultipleAccountsInfo(
      atas,
      "processed",
    );
    const ataBalances = new Map<string, bigint>();
    let currentBalance = 0n;
    for (let index = 0; index < atas.length; index += 1) {
      const info = infos[index];
      const amount =
        info && info.data.length >= 72 ? info.data.readBigUInt64LE(64) : 0n;
      ataBalances.set(atas[index].toBase58(), amount);
      currentBalance += amount;
    }
    return {
      mint,
      wallet,
      qualifyingSignature,
      buyUsd,
      boughtAmount,
      atas,
      ataBalances,
      currentBalance,
      // The qualifying transaction proves this wallet held a positive amount,
      // even if the RPC snapshot is briefly behind the indexed transaction.
      seenPositiveBalance: boughtAmount > 0 || currentBalance > 0n,
    };
  }

  private async applyLargeBuyerAtaBalance(
    watch: LargeBuyerWatchState,
    ataAddress: string,
    amount: bigint,
    detection: string,
  ): Promise<void> {
    if (this.largeBuyerWatch !== watch || this.positionSellTriggered) return;
    const previousBalance = watch.currentBalance;
    watch.ataBalances.set(ataAddress, amount);
    watch.currentBalance = [...watch.ataBalances.values()].reduce(
      (total, balance) => total + balance,
      0n,
    );
    if (watch.currentBalance > 0n) watch.seenPositiveBalance = true;
    if (watch.currentBalance !== previousBalance) {
      this.log.info("Tracked >=$200 buyer ATA balance changed", {
        mint: watch.mint,
        wallet: watch.wallet,
        ata: ataAddress,
        previousBalance: previousBalance.toString(),
        currentBalance: watch.currentBalance.toString(),
        detection,
      });
    }
    if (
      watch.seenPositiveBalance &&
      watch.currentBalance === 0n &&
      this.phase === "holding" &&
      this.activePosition?.mint === watch.mint
    ) {
      await this.triggerLargeBuyerSell(watch, detection);
    }
  }

  private async syncLargeBuyerAtaBalances(): Promise<void> {
    const watch = this.largeBuyerWatch;
    if (
      !watch ||
      this.phase !== "holding" ||
      !this.activePosition ||
      this.positionSellTriggered
    ) {
      return;
    }
    if (this.isLargeBuyerSyncing) {
      this.largeBuyerSyncPending = true;
      return;
    }
    if (
      Date.now() - this.lastLargeBuyerSyncAt <
      AXIOM_AUTHORITY_MIN_SYNC_INTERVAL_MS
    ) {
      return;
    }

    this.isLargeBuyerSyncing = true;
    this.lastLargeBuyerSyncAt = Date.now();
    try {
      const infos = await this.connection.getMultipleAccountsInfo(
        watch.atas,
        "processed",
      );
      for (let index = 0; index < watch.atas.length; index += 1) {
        const info = infos[index];
        const amount =
          info && info.data.length >= 72 ? info.data.readBigUInt64LE(64) : 0n;
        watch.ataBalances.set(watch.atas[index].toBase58(), amount);
      }
      const previousBalance = watch.currentBalance;
      watch.currentBalance = [...watch.ataBalances.values()].reduce(
        (total, balance) => total + balance,
        0n,
      );
      if (watch.currentBalance > 0n) watch.seenPositiveBalance = true;
      if (watch.currentBalance !== previousBalance) {
        this.log.info("Tracked >=$200 buyer ATA balance changed", {
          mint: watch.mint,
          wallet: watch.wallet,
          previousBalance: previousBalance.toString(),
          currentBalance: watch.currentBalance.toString(),
          detection: "periodic ATA balance poll",
        });
      }
      if (watch.seenPositiveBalance && watch.currentBalance === 0n) {
        await this.triggerLargeBuyerSell(
          watch,
          "Combined standard SPL and Token-2022 ATA balance reached zero",
        );
      }
    } catch (err) {
      this.log.warn("Large authority buyer ATA balance sync failed", {
        mint: watch.mint,
        wallet: watch.wallet,
        error: err instanceof Error ? err.message : String(err),
      });
    } finally {
      this.isLargeBuyerSyncing = false;
      if (this.largeBuyerSyncPending) {
        this.largeBuyerSyncPending = false;
        void this.syncLargeBuyerAtaBalances();
      }
    }
  }

  private async triggerLargeBuyerSell(
    watch: LargeBuyerWatchState,
    detection: string,
  ): Promise<void> {
    await this.triggerPositionSell(
      watch.mint,
      `Authority >=$200 buyer ${watch.wallet} ATA balance reached zero`,
      [
        "<b>🚨 Lookup-Table Authority Buyer Sold All</b>",
        `Token: <code>${watch.mint}</code>`,
        `Wallet: <code>${watch.wallet}</code>`,
        `Qualifying buy: <b>$${watch.buyUsd.toFixed(2)}</b>`,
        `Buy tx: <code>${watch.qualifyingSignature}</code>`,
        "Combined ATA balance: <code>0</code>",
        `Detection: <b>${detection}</b>`,
      ],
      watch.qualifyingSignature,
    );
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
      this.log.info(
        "Axiom/empty single-buy GMGN scan [pre_buy] — no traders returned",
        {
          mint,
          phase: "pre_buy",
          orderBy: "buy_volume_cur",
          tag: null,
          buyUsdRange: `${this.bundlerBuyMinUsd}-${this.bundlerBuyMaxUsd}`,
          limit: AXIOM_TRADER_SCAN_LIMIT,
          responseShape: this.describeTraderResponseShape(traders),
        },
      );
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
      this.log.info(
        "Axiom/empty single-buy GMGN scan [post_buy] — no traders returned",
        {
          mint,
          phase: "post_buy",
          orderBy: "buy_volume_cur",
          tag: null,
          buyUsdRange: `${this.bundlerBuyMinUsd}-${this.bundlerBuyMaxUsd}`,
          limit: AXIOM_TRADER_SCAN_LIMIT,
          responseShape: this.describeTraderResponseShape(traders),
        },
      );
      return;
    }

    const stats = this.logAxiomSingleBuyTraderScan(mint, "post_buy", list);
    if (!stats || stats.validCount === 0) {
      return;
    }

    this.rememberAxiomWatchedWallets(mint, stats.matchingWallets);
  }

  private extractTraderList(
    traders: Record<string, unknown> | null,
  ): Array<Record<string, unknown>> {
    if (!traders) return [];
    if (Array.isArray(traders))
      return traders as Array<Record<string, unknown>>;
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

  private describeTraderResponseShape(
    traders: Record<string, unknown> | null,
  ): Record<string, unknown> {
    if (!traders) return { type: "null" };
    if (Array.isArray(traders))
      return { type: "array", length: traders.length };

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
      tradersLength: Array.isArray(traders.traders)
        ? traders.traders.length
        : null,
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
      this.log.warn(
        "Bundler wallet transfer-out detected (post-buy) — selling ASAP",
        {
          mint,
          wallet,
          recipient,
          signature: tx.signature,
          totalBuyTxs: this.tokenBuyCount,
          totalSellTxs: this.tokenSellCount,
        },
      );
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

  private async sendTelegramSafe(text: string, context: string): Promise<void> {
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
    await this.stopBundlerFunderMonitoring();
    this.stopAxiomAtaPollLoop();
    this.watchingMint = null;
    this.monitoredWallet = null;
    this.insiderState = null;
    this.clearBundlerAccumulation();
    this.initialInsiderWallets.clear();
    this.insiderWalletChain.clear();
    this.isSwitchingInsiderWallet = false;
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
    this.bundlerFunderWatch = null;
    this.clearBundlerAccumulation();
    this.initialInsiderWallets.clear();
    this.devWallet = null;
    this.axiomTraderWatchActive = false;
    this.clearAxiomWatchedWallets();
    this.preBuyStopped = false;
    this.positionSellTriggered = false;
    this.profitExitDisabled = false;
    this.insiderSellsReady = false;
    this.bundlerMatchesReady = false;
    this.authorityProbeFailedAtTwo = false;
    this.buySubmitted = false;
    this.isBuyGateEvaluating = false;
    this.profitExitDisabled = false;
    this.resetTokenTxCounts();

    this.log.info("InsiderBot reset; resuming followed wallet monitoring");
    if (this.followedWallet && !this.followMonitor) {
      await this.followWallet(this.followedWallet);
    }
  }
}
