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
  HeliusBalanceAtResponse,
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
const MAX_FOLLOW_WALLET_START_MARKET_CAP_USD = 80_000;
const BUNDLER_FUNDER_TRANSFER_LIMIT = 5;
const BUNDLER_FUNDER_REQUIRED_COUNT = 4;
const BUNDLER_FUNDER_FUNDING_RECORD_ATTEMPTS = 3;
const BUNDLER_FUNDER_FUNDING_RECORD_RETRY_DELAY_MS = 500;
const BUNDLER_FUNDER_LOW_FUNDING_SOL = 15;
const BUNDLER_FUNDER_LOW_FUNDING_MAX_TRANSFER_OUT_TXS = 5;
const BUNDLER_FUNDER_LOW_FUNDING_EXIT_PERCENT = 50;
const BUNDLER_FUNDER_LOW_FUNDING_MIN_TRANSFER_OUT_SOL = 3.5;
const BUNDLER_FUNDER_LOW_FUNDING_TINY_GROUP_SECONDS = 10;
const BUNDLER_FUNDER_LOW_FUNDING_TINY_MIN_BUY_USD = 2.5;
const BUNDLER_FUNDER_LOW_FUNDING_TINY_COPYSELL_MIN_USD = 5;
const BUNDLER_FUNDER_NORMAL_TINY_MIN_BUY_USD = 2.5;
const BUNDLER_FUNDER_NORMAL_TINY_MID_MAX_USD = 5;
const BUNDLER_FUNDER_NORMAL_TINY_MID_EXIT_PERCENT = 90;
const BUNDLER_FUNDER_NORMAL_TINY_HIGH_EXIT_PERCENT = 180;
const BUNDLER_FUNDER_NORMAL_TINY_TRANSFER_OUT_MAX_USD = 10;
const BUNDLER_FUNDER_MAX_NORMAL_TRANSFER_OUT_SOL = 100;
const BUNDLER_FUNDER_SYNC_LIMIT = 50;
const BUNDLER_FUNDER_SYNC_MIN_INTERVAL_MS = 1_000;
const BUNDLER_FUNDER_WS_SYNC_DELAY_MS = 50;
const BUNDLER_FUNDER_MAX_RECIPIENT_WATCHES = 2;
const BUNDLER_FUNDER_RECIPIENT_FIRST_TX_WINDOW = 3;
const BUNDLER_FUNDER_RECIPIENT_SWAP_HISTORY_MAX_AGE_MS = 3 * 24 * 60 * 60 * 1_000;
const BUNDLER_FUNDER_RECIPIENT_MIN_BUY_USD = 200;
const BUNDLER_FUNDER_RECIPIENT_SYNC_INTERVAL_MS = 1_500;
const BUNDLER_FUNDER_RECIPIENT_BATCH_SIZE = 2;
const HELIUS_POOL_MAX_CONCURRENT = 2;
const HELIUS_POOL_MIN_TIME_MS = 150;
const HELIUS_POOL_REQUEST_TIMEOUT_MS = 10_000;
const HELIUS_POOL_BASE_BACKOFF_MS = 2_000;
const HELIUS_POOL_MAX_BACKOFF_MS = 60_000;
const HELIUS_POOL_METRICS_INTERVAL_MS = 30_000;
const HELIUS_POOL_MC_RESERVED_INDEX = 3;
const BUNDLER_FUNDER_MAX_QUEUED_TRANSFER_OUT_CANDIDATES = 20;

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
  latestWindowFundingSignature: string;
  latestWindowFundingTimestamp: number;
}

interface FunderRecipientWatch {
  wallet: string;
  fundingSignature: string;
  fundingTimestamp: number;
  outAmountSol: number;
  heliusPreferredIndex: number;
  tokenActions: Array<{
    kind: "buy" | "sell";
    signature: string;
    amount: number;
  }>;
  observedTxSignatures: Set<string>;
  tokenBuyObserved: boolean;
  zeroSolBalanceSignatures: Set<string>;
  buyTriggersEntry: boolean;
  boughtAmount: number;
  soldAmount: number;
  firstBuySignature: string | null;
  firstBuyTimestamp: number | null;
  normalTinyTransferMode: boolean;
  normalTinyExitPercent: number | null;
  lowFundingCopySellOnSellAll: boolean;
  lowFundingTinyUsdBand: "2_5_to_5" | "gt5" | null;
  postEntrySwapSignature: string | null;
  postEntrySwapBaselineSignatures: Set<string>;
}

interface FunderTransferOutCandidate {
  signature: string;
  recipient: string;
  amountSol: number;
  timestamp: number;
  normalTinyTransferMode: boolean;
}

interface BundlerFunderWatchState {
  mint: string;
  funderWallet: string;
  originalFunderWallet: string;
  migrationCount: number;
  lowFundingMode: boolean;
  earliestFundingTimestamp: number;
  earliestFundingSignature: string;
  largestFundingSol: number;
  minTransferOutSol: number;
  cursorSignature: string | null;
  processedSignatures: Set<string>;
  validOutSignatures: Set<string>;
  invalidOutSignatures: Set<string>;
  bundlerWallets: Set<string>;
  recipientWatches: Map<string, FunderRecipientWatch>;
  queuedTransferOuts: FunderTransferOutCandidate[];
  lowFundingFunderTxs: Array<{ signature: string; timestamp: number }>;
  lowFundingTinyTransferOuts: Array<{ signature: string; timestamp: number; recipient: string; amountSol: number; amountUsd: number }>;
  lowFundingTinyBundlerGateSeen: boolean;
  lowFundingTinyEntryTimestamp: number | null;
  lowFundingTinyCandidateWallets: Set<string>;
  lowFundingTinySellGroupSignatures: Set<string>;
  lowFundingTinyBoughtUsdBands: Set<"2_5_to_5" | "gt5">;
  lowFundingTinySoldUsdBands: Set<"2_5_to_5" | "gt5">;
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
  private disableProfitExitAfterBuy = false;
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
  private heliusPoolMetricsMint: string | null = null;
  private heliusPoolMetricsStartedAt = 0;
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

    const monitor = new WalletMonitor(this.config, normalized, {
      enforceMinBuySol: false,
      rpcUrl: this.rpcUrl,
      wsUrl: this.wsUrl,
      logLabel: `WALLET ${this.label.toUpperCase()}`,
    });
    this.followMonitor = monitor;
    monitor.on("newToken", (event) => {
      void this.handleFollowWalletBuy(event.mint, event.signature);
    });

    try {
      await monitor.start();
    } catch (err) {
      monitor.stop();
      this.followMonitor = null;
      throw err;
    }
    for (const mint of monitor.existingMints) {
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
    this.activePosition = null;
    this.watchingMint = null;
    this.monitoredWallet = null;
    this.insiderState = null;
    this.phase = null;
    this.bundlerWatch = null;
    this.bundlerFunderWatch = null;
    this.clearBundlerAccumulation();
    this.clearAxiomWatchedWallets();
    this.log.error(
      "Insider bot reset and stopped because Helius usage is exhausted",
      {
        activePositionCleared: true,
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
    this.profitExitDisabled = this.disableProfitExitAfterBuy;
    this.disableProfitExitAfterBuy = false;
    if (this.authorityMonitor?.initialCursorSignature) {
      this.authorityMonitor.cursorSignature =
        this.authorityMonitor.initialCursorSignature;
    }

    void this.syncAuthorityTransactions();
    void this.syncLargeBuyerAtaBalances();
    void this.syncBundlerFunderTransactions();
    void this.syncFunderRecipientBatch(true);
    void this.auditFunderRecipientsAfterBuy();
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
    this.resetHeliusPoolMetricsForMint(mint);
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
        "Finding each bundler's zero-balance funding window, selecting the latest valid funding transfer in that window, requiring those funding txs to share one feePayer, then watching that feePayer's transfer-outs for recipient buy confirmation.",
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
    const compareEntries = (a: HeliusPoolEntry, b: HeliusPoolEntry) => {
      const aCooling = a.unavailableUntil > now ? 1 : 0;
      const bCooling = b.unavailableUntil > now ? 1 : 0;
      if (aCooling !== bCooling) return aCooling - bCooling;
      if (a.stats.requests !== b.stats.requests) {
        return a.stats.requests - b.stats.requests;
      }
      if (a.stats.rateLimits !== b.stats.rateLimits) {
        return a.stats.rateLimits - b.stats.rateLimits;
      }
      return a.index - b.index;
    };
    const normalPool =
      preferredIndex === HELIUS_POOL_MC_RESERVED_INDEX
        ? pool
        : pool.filter((entry) => entry.index !== HELIUS_POOL_MC_RESERVED_INDEX);
    const candidates = normalPool.length > 0 ? normalPool : pool;
    const preferred = candidates.find((entry) => entry.index === preferredIndex);
    const rest = candidates
      .filter((entry) => entry !== preferred)
      .sort(compareEntries);
    const ordered =
      preferred && preferredIndex !== 0
        ? [preferred, ...rest]
        : [...candidates].sort(compareEntries);
    return ordered[offset] ?? null;
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
      mint: this.heliusPoolMetricsMint,
      elapsedMs:
        this.heliusPoolMetricsStartedAt > 0
          ? now - this.heliusPoolMetricsStartedAt
          : null,
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

  private resetHeliusPoolMetricsForMint(mint: string): void {
    this.heliusPoolMetricsMint = mint;
    this.heliusPoolMetricsStartedAt = Date.now();
    this.lastHeliusPoolMetricsAt = 0;
    for (const entry of this.heliusPool) {
      entry.stats.requests = 0;
      entry.stats.successes = 0;
      entry.stats.fallbacks = 0;
      entry.stats.rateLimits = 0;
      entry.stats.transientFailures = 0;
      entry.stats.permanentFailures = 0;
    }
    this.log.info("Helius pool metrics reset for token", { mint });
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

    let fundingRecords: Array<BundlerFundingRecord | null> = [];
    for (
      let attempt = 1;
      attempt <= BUNDLER_FUNDER_FUNDING_RECORD_ATTEMPTS;
      attempt += 1
    ) {
      fundingRecords = await Promise.all(
        firstFour.map((buy, index) =>
          this.findValidBundlerFundingRecord(mint, buy, index),
        ),
      );

      const missingCount = fundingRecords.filter((record) => !record).length;
      if (missingCount === 0) {
        if (attempt > 1) {
          this.log.warn("Bundler funding records validated after retry", {
            mint,
            attempt,
            maxAttempts: BUNDLER_FUNDER_FUNDING_RECORD_ATTEMPTS,
            fundingRecords,
          });
        }
        break;
      }

      if (attempt < BUNDLER_FUNDER_FUNDING_RECORD_ATTEMPTS) {
        this.log.warn("Could not validate all four bundler funding records; retrying", {
          mint,
          attempt,
          maxAttempts: BUNDLER_FUNDER_FUNDING_RECORD_ATTEMPTS,
          missingCount,
          fundingRecords,
        });
        await new Promise((resolve) =>
          setTimeout(resolve, BUNDLER_FUNDER_FUNDING_RECORD_RETRY_DELAY_MS),
        );
        continue;
      }

      this.log.warn("Could not validate all four bundler funding records after retries; resetting", {
        mint,
        attempts: BUNDLER_FUNDER_FUNDING_RECORD_ATTEMPTS,
        missingCount,
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
    const latest = records.reduce((best, record) =>
      record.timestamp > best.timestamp ? record : best,
    );
    const lowFundingSyncStart = records.reduce((best, record) =>
      record.latestWindowFundingTimestamp > best.latestWindowFundingTimestamp
        ? record
        : best,
    );
    const largestFundingSol = Math.max(...records.map((record) => record.amountSol));
    const lowFundingMode = largestFundingSol < BUNDLER_FUNDER_LOW_FUNDING_SOL;
    const latestBundlerBuyTimestamp = Math.max(
      ...firstFour.map((buy) => buy.timestamp),
    );
    const funderWallet = records[0].fundingFeePayer;
    this.bundlerFunderWatch = {
      mint,
      funderWallet,
      originalFunderWallet: funderWallet,
      migrationCount: 0,
      lowFundingMode,
      earliestFundingTimestamp: earliest.timestamp,
      earliestFundingSignature: earliest.fundingSignature,
      largestFundingSol,
      minTransferOutSol: lowFundingMode
        ? BUNDLER_FUNDER_LOW_FUNDING_MIN_TRANSFER_OUT_SOL
        : largestFundingSol,
      cursorSignature: earliest.fundingSignature,
      processedSignatures: new Set(records.map((record) => record.fundingSignature)),
      validOutSignatures: new Set<string>(),
      invalidOutSignatures: new Set<string>(),
      bundlerWallets: new Set(records.map((record) => record.bundlerWallet)),
      recipientWatches: new Map<string, FunderRecipientWatch>(),
      queuedTransferOuts: [],
      lowFundingFunderTxs: [],
      lowFundingTinyTransferOuts: [],
      lowFundingTinyBundlerGateSeen: false,
      lowFundingTinyEntryTimestamp: null,
      lowFundingTinyCandidateWallets: new Set<string>(),
      lowFundingTinySellGroupSignatures: new Set<string>(),
      lowFundingTinyBoughtUsdBands: new Set<"2_5_to_5" | "gt5">(),
      lowFundingTinySoldUsdBands: new Set<"2_5_to_5" | "gt5">(),
    };

    this.subscribeBundlerFunder(funderWallet);
    if (lowFundingMode) {
      await this.evaluateLowFundingSharedFeePayerBuy({
        state: this.bundlerFunderWatch!,
        syncStart: {
          signature: lowFundingSyncStart.latestWindowFundingSignature,
          timestamp: lowFundingSyncStart.latestWindowFundingTimestamp,
          bundlerWallet: lowFundingSyncStart.bundlerWallet,
        },
        latestBundlerBuyTimestamp,
      });
      if (
        !this.bundlerFunderWatch ||
        this.bundlerFunderWatch.mint !== mint ||
        this.watchingMint !== mint ||
        this.phase !== "pre_buy"
      ) {
        this.log.info(
          "Shared feePayer flow stopped during low-funding evaluation; skipping normal watcher continuation",
          {
            mint,
            reason: "token flow reset or no longer active",
          },
        );
        return;
      }
    }
    if (!this.buySubmitted) {
      await this.syncBundlerFunderTransactions(true);
    }
    const activeFunderWatch = this.bundlerFunderWatch;
    if (
      !activeFunderWatch ||
      activeFunderWatch.mint !== mint ||
      this.watchingMint !== mint ||
      this.phase !== "pre_buy"
    ) {
      this.log.info(
        "Shared feePayer flow stopped before lock notification; skipping stale continuation",
        {
          mint,
          reason: "token flow reset or no longer active",
        },
      );
      return;
    }

    this.log.warn("First-four bundler funding feePayer gate passed; shared feePayer watch started", {
      mint,
      sharedFeePayer: funderWallet,
      earliestFundingTimestamp: earliest.timestamp,
      earliestFundingSignature: earliest.fundingSignature,
      latestFundingTimestamp: latest.timestamp,
      latestFundingSignature: latest.fundingSignature,
      lowFundingSyncStartSignature: lowFundingSyncStart.latestWindowFundingSignature,
      lowFundingSyncStartTimestamp: lowFundingSyncStart.latestWindowFundingTimestamp,
      largestFundingSol,
      minTransferOutSol: activeFunderWatch.minTransferOutSol,
      fundingRecords: records,
    });
    void this.sendTelegramSafe(
      [
        `<b>✅ ${this.label} Shared FeePayer Locked</b>`,
        `Token: <code>${mint}</code>`,
        `FeePayer: <code>${funderWallet}</code>`,
        `Largest bundler funding: <b>${largestFundingSol.toFixed(4)} SOL</b>`,
        `Watching feePayer transfer-outs: <b>${activeFunderWatch.minTransferOutSol.toFixed(4)} SOL+</b>`,
        "",
        "Transfer-outs that pass the filters are watched until the recipient buys this token.",
      ].join("\n"),
      "shared feePayer notification",
    );
  }

  private async evaluateLowFundingSharedFeePayerBuy(args: {
    state: BundlerFunderWatchState;
    syncStart: {
      signature: string;
      timestamp: number;
      bundlerWallet: string;
    };
    latestBundlerBuyTimestamp: number;
  }): Promise<void> {
    const { state, syncStart, latestBundlerBuyTimestamp } = args;
    const txs = await this.withHeliusFallback((client) =>
      client.getAddressTransactionsAsc(
        state.funderWallet,
        syncStart.signature,
        BUNDLER_FUNDER_SYNC_LIMIT,
      ),
    );
    const windowTxs = txs.filter(
      (tx) =>
        tx.timestamp >= syncStart.timestamp &&
        tx.timestamp <= latestBundlerBuyTimestamp,
    );
    const allTransferOuts = windowTxs
      .map((tx) => ({
        tx,
        transferOut: this.extractSolTransferOutFromWallet(
          tx,
          state.funderWallet,
          Number.EPSILON,
        ),
      }))
      .filter(
        (entry): entry is {
          tx: HeliusTransaction;
          transferOut: { to: string; amountSol: number };
        } => Boolean(entry.transferOut),
      );
    const transferOuts = allTransferOuts.filter(
      (entry) =>
        entry.transferOut.amountSol >
          BUNDLER_FUNDER_LOW_FUNDING_MIN_TRANSFER_OUT_SOL &&
        !state.bundlerWallets.has(entry.transferOut.to),
    );
    const largestIncomingSol = Math.max(
      0,
      ...windowTxs.map((tx) =>
        this.extractSolIncomingAmountToWallet(tx, state.funderWallet),
      ),
    );
    const sharedFeePayerBalanceAtSyncStart = await this.getConfirmedWalletBalanceAt(
      state.funderWallet,
      NATIVE_SOL_BALANCE_MINT,
      syncStart.timestamp,
    );
    const sharedFeePayerBalanceSol = Number(sharedFeePayerBalanceAtSyncStart.balance);
    const sharedFeePayerBalanceBelowLowFundingThreshold =
      Number.isFinite(sharedFeePayerBalanceSol) &&
      sharedFeePayerBalanceSol < BUNDLER_FUNDER_LOW_FUNDING_SOL;
    const lowFundingImmediateBuyWindowValid =
      windowTxs.length > 0 &&
      windowTxs.length <= BUNDLER_FUNDER_LOW_FUNDING_MAX_TRANSFER_OUT_TXS;
    const lowFundingImmediateBuyAllowed =
      sharedFeePayerBalanceBelowLowFundingThreshold &&
      lowFundingImmediateBuyWindowValid;
    this.log.warn("Low-funding shared feePayer window evaluated", {
      mint: state.mint,
      sharedFeePayer: state.funderWallet,
      largestFundingSol: state.largestFundingSol,
      lowFundingThresholdSol: BUNDLER_FUNDER_LOW_FUNDING_SOL,
      lowFundingCandidateMinTransferOutSol:
        BUNDLER_FUNDER_LOW_FUNDING_MIN_TRANSFER_OUT_SOL,
      syncStartSignature: syncStart.signature,
      syncStartTimestamp: syncStart.timestamp,
      syncStartBundlerWallet: syncStart.bundlerWallet,
      latestBundlerBuyTimestamp,
      txCount: windowTxs.length,
      minWindowTxsForImmediateBuy: 1,
      maxWindowTxsForImmediateBuy:
        BUNDLER_FUNDER_LOW_FUNDING_MAX_TRANSFER_OUT_TXS,
      transferOutTxCount: transferOuts.length,
      maxTransferOutTxs: BUNDLER_FUNDER_LOW_FUNDING_MAX_TRANSFER_OUT_TXS,
      largestIncomingSol,
      sharedFeePayerBalanceAtSyncStart: sharedFeePayerBalanceAtSyncStart.balance,
      sharedFeePayerBalanceAtSyncStartRaw:
        sharedFeePayerBalanceAtSyncStart.balanceRaw,
      sharedFeePayerBalanceBelowLowFundingThreshold,
      lowFundingImmediateBuyWindowValid,
      lowFundingImmediateBuyAllowed,
      action:
        lowFundingImmediateBuyAllowed
          ? "use low-funding tiny-transfer grouping flow"
          : transferOuts.length > 0
          ? "watch recipients for token buy in first 3 txs"
          : sharedFeePayerBalanceBelowLowFundingThreshold
          ? "skip immediate low-funding buy because window tx count is not between 1 and 5"
          : "waiting for low-funding transfer-out candidate",
      transferOuts: transferOuts.map((entry) => ({
        signature: entry.tx.signature,
        timestamp: entry.tx.timestamp,
        recipient: entry.transferOut.to,
        amountSol: entry.transferOut.amountSol,
      })),
      skippedBundlerRecipients: allTransferOuts
        .filter((entry) => state.bundlerWallets.has(entry.transferOut.to))
        .map((entry) => ({
          signature: entry.tx.signature,
          timestamp: entry.tx.timestamp,
          recipient: entry.transferOut.to,
          amountSol: entry.transferOut.amountSol,
        })),
    });

    this.log.info("Low-funding immediate/>3.5 SOL buy path disabled; using tiny transfer grouping flow", {
      mint: state.mint,
      sharedFeePayer: state.funderWallet,
      tinyTransferMaxUsd: BUNDLER_FUNDER_NORMAL_TINY_TRANSFER_OUT_MAX_USD,
      groupWindowSeconds: BUNDLER_FUNDER_LOW_FUNDING_TINY_GROUP_SECONDS,
      note: "Initial sync will seed the tiny-transfer bundler gate; non-bundler tiny recipients are evaluated after that gate.",
    });
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

    const candidates: Array<{
      tx: HeliusTransaction;
      incoming: { from: string; amountSol: number };
      currentBalance: number;
      effectiveFundingSol: number;
      index: number;
    }> = [];
    let zeroBoundary: {
      signature: string;
      timestamp: number;
      balance: number;
    } | null = null;

    for (let index = 0; index < txs.length; index += 1) {
      const tx = txs[index];
      const currentBalance = await this.fetchSolBalanceAt(
        buy.wallet,
        tx.timestamp,
        preferredClientIndex,
      );
      if (currentBalance < 0) {
        this.log.info("Bundler funding candidate rejected: balance-at timestamp is negative", {
          mint,
          bundlerWallet: buy.wallet,
          candidateSignature: tx.signature,
          index,
          timestamp: tx.timestamp,
          currentBalance,
        });
        continue;
      }
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
        if (currentBalance === 0) {
          zeroBoundary = {
            signature: tx.signature,
            timestamp: tx.timestamp,
            balance: currentBalance,
          };
          this.log.info("Bundler funding zero-balance boundary found", {
            mint,
            bundlerWallet: buy.wallet,
            boundarySignature: tx.signature,
            boundaryTimestamp: tx.timestamp,
            candidateCount: candidates.length,
          });
          break;
        }
        continue;
      }
      if (currentBalance === 0) {
        this.log.info("Bundler funding incoming transfer kept as candidate despite zero balance-at timestamp", {
          mint,
          bundlerWallet: buy.wallet,
          candidateSignature: tx.signature,
          index,
          timestamp: tx.timestamp,
          incomingAmountSol: incoming.amountSol,
          senderWallet: incoming.from,
        });
      }
      candidates.push({
        tx,
        incoming,
        currentBalance,
        effectiveFundingSol: Math.max(incoming.amountSol, currentBalance),
        index,
      });
    }

    if (!zeroBoundary) {
      this.log.warn("No zero-balance boundary found for bundler funding window", {
        mint,
        bundlerWallet: buy.wallet,
        bundlerBuySignature: buy.signature,
        transferCount: txs.length,
        candidateCount: candidates.length,
      });
      return null;
    }

    if (!candidates.length) {
      this.log.warn("No valid funding transfer found above zero-balance boundary for bundler", {
        mint,
        bundlerWallet: buy.wallet,
        bundlerBuySignature: buy.signature,
        zeroBoundary,
        transferCount: txs.length,
      });
      return null;
    }

    const selected = candidates[0];
    const latestWindowFunding = candidates[0];
    this.log.warn("Bundler funding transfer selected from post-zero window", {
      mint,
      bundlerWallet: buy.wallet,
      bundlerBuySignature: buy.signature,
      fundingSignature: selected.tx.signature,
      fundingFeePayer: selected.tx.feePayer,
      senderWallet: selected.incoming.from,
      amountSol: selected.effectiveFundingSol,
      incomingAmountSol: selected.incoming.amountSol,
      timestamp: selected.tx.timestamp,
      latestWindowFundingSignature: latestWindowFunding.tx.signature,
      latestWindowFundingTimestamp: latestWindowFunding.tx.timestamp,
      currentBalance: selected.currentBalance,
      zeroBoundary,
      candidateCount: candidates.length,
      selectionRule:
        "latest funding transfer above zero-balance boundary; effective funding uses wallet balance at that timestamp when higher than incoming amount",
      candidates: candidates.map((candidate) => ({
        signature: candidate.tx.signature,
        fundingFeePayer: candidate.tx.feePayer,
        senderWallet: candidate.incoming.from,
        amountSol: candidate.effectiveFundingSol,
        incomingAmountSol: candidate.incoming.amountSol,
        timestamp: candidate.tx.timestamp,
        currentBalance: candidate.currentBalance,
        index: candidate.index,
      })),
    });
    return {
      bundlerWallet: buy.wallet,
      bundlerBuySignature: buy.signature,
      fundingSignature: selected.tx.signature,
      fundingFeePayer: selected.tx.feePayer!,
      senderWallet: selected.incoming.from,
      amountSol: selected.effectiveFundingSol,
      timestamp: selected.tx.timestamp,
      latestWindowFundingSignature: latestWindowFunding.tx.signature,
      latestWindowFundingTimestamp: latestWindowFunding.tx.timestamp,
    };
  }

  private async fetchSolBalanceAt(
    wallet: string,
    timestamp: number,
    preferredClientIndex: number,
  ): Promise<number> {
    const balance = await this.getConfirmedWalletBalanceAt(
      wallet,
      NATIVE_SOL_BALANCE_MINT,
      timestamp,
      preferredClientIndex,
    );
    const parsed = Number(balance.balance);
    return Number.isFinite(parsed) ? parsed : 0;
  }

  private async getConfirmedWalletBalanceAt(
    wallet: string,
    mint: string,
    timestamp: number,
    preferredClientIndex = 0,
  ): Promise<HeliusBalanceAtResponse> {
    const first = await this.withHeliusFallback(
      (client) => client.getWalletBalanceAt(wallet, mint, timestamp),
      preferredClientIndex,
    );
    const confirmed = await this.withHeliusFallback(
      (client) => client.getWalletBalanceAt(wallet, mint, timestamp),
      preferredClientIndex,
    );
    this.log.debug("Confirmed Helius balance-at with second request", {
      wallet,
      mint,
      timestamp,
      firstBalance: first.balance,
      firstBalanceRaw: first.balanceRaw,
      confirmedBalance: confirmed.balance,
      confirmedBalanceRaw: confirmed.balanceRaw,
      firstAsOf: first.asOf,
      confirmedAsOf: confirmed.asOf,
    });
    return confirmed;
  }

  private async getConfirmedWalletSwapHistory(
    wallet: string,
    limit: number,
    preferredClientIndex = 0,
  ): Promise<HeliusTransaction[]> {
    const first = await this.withHeliusFallback(
      (client) => client.getWalletSwapHistory(wallet, limit),
      preferredClientIndex,
    );
    const confirmed = await this.withHeliusFallback(
      (client) => client.getWalletSwapHistory(wallet, limit),
      preferredClientIndex,
    );
    this.log.debug("Confirmed Helius wallet SWAP history with second request", {
      wallet,
      limit,
      firstCount: first.length,
      confirmedCount: confirmed.length,
      firstNewestSignature: first[0]?.signature ?? null,
      confirmedNewestSignature: confirmed[0]?.signature ?? null,
      firstNewestTimestamp: first[0]?.timestamp ?? null,
      confirmedNewestTimestamp: confirmed[0]?.timestamp ?? null,
    });
    return confirmed;
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

  private async switchBundlerFunderWatchAddress(
    state: BundlerFunderWatchState,
    nextWallet: string,
    cursorSignature: string,
    reason: string,
  ): Promise<void> {
    if (nextWallet === state.funderWallet) return;
    if (this.bundlerFunderLogsSubId !== null) {
      const subId = this.bundlerFunderLogsSubId;
      this.bundlerFunderLogsSubId = null;
      await this.connection.removeOnLogsListener(subId).catch(() => undefined);
    }
    const previousWallet = state.funderWallet;
    state.funderWallet = nextWallet;
    state.migrationCount += 1;
    state.cursorSignature = cursorSignature;
    this.bundlerFunderSyncPending = true;
    this.bundlerFunderSyncPendingForce = true;
    this.subscribeBundlerFunder(nextWallet);
    this.log.warn("Shared feePayer address migrated to a new wallet", {
      mint: state.mint,
      originalFeePayer: state.originalFunderWallet,
      previousWallet,
      nextWallet,
      migrationCount: state.migrationCount,
      cursorSignature,
      reason,
    });
    void this.sendTelegramSafe(
      [
        `<b>🔁 ${this.label} Shared FeePayer Migrated</b>`,
        `Token: <code>${state.mint}</code>`,
        `Original FeePayer: <code>${state.originalFunderWallet}</code>`,
        `Old FeePayer: <code>${previousWallet}</code>`,
        `New FeePayer: <code>${nextWallet}</code>`,
        `Migration #: <b>${state.migrationCount}</b>`,
        `Handoff tx: <code>${cursorSignature}</code>`,
        "",
        reason,
      ].join("\n"),
      "shared feePayer migration notification",
    );
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

  private removeFunderRecipientWatch(wallet: string, reason: string): void {
    const state = this.bundlerFunderWatch;
    state?.recipientWatches.delete(wallet);
    this.dirtyFunderRecipients.delete(wallet);
    this.dirtyFunderRecipientSignatures.delete(wallet);
    const subId = this.recipientLogsSubIds.get(wallet);
    if (subId !== undefined) {
      this.recipientLogsSubIds.delete(wallet);
      void this.connection.removeOnLogsListener(subId).catch(() => undefined);
    }
    this.log.info("Stopped watching shared feePayer recipient", {
      mint: state?.mint,
      wallet,
      reason,
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
    if (this.hasReachedFunderRecipientBuyCap(state)) {
      await this.stopBundlerFunderSourceDiscovery(
        state,
        "recipient buy cap already reached",
      );
      return;
    }
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
    const syncingWallet = state.funderWallet;
    try {
      const txs = await this.withHeliusFallback((client) =>
        client.getAddressTransactionsAsc(
          syncingWallet,
          state.cursorSignature ?? undefined,
          BUNDLER_FUNDER_SYNC_LIMIT,
        ),
      );
      for (const tx of txs) {
        if (state.funderWallet !== syncingWallet) break;
        if (state.processedSignatures.has(tx.signature)) continue;
        state.processedSignatures.add(tx.signature);
        state.cursorSignature = tx.signature;
        const migrated = await this.inspectBundlerFunderTransaction(state, tx);
        if (migrated) break;
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

  private countConfirmedFunderRecipientBuys(
    state: BundlerFunderWatchState,
  ): number {
    return [...state.recipientWatches.values()].filter(
      (watch) => watch.tokenBuyObserved,
    ).length;
  }

  private hasReachedFunderRecipientBuyCap(
    state: BundlerFunderWatchState,
  ): boolean {
    return (
      this.countConfirmedFunderRecipientBuys(state) >=
      BUNDLER_FUNDER_MAX_RECIPIENT_WATCHES
    );
  }

  private isKnownFunderCandidate(
    state: BundlerFunderWatchState,
    signature: string,
  ): boolean {
    return (
      state.validOutSignatures.has(signature) ||
      state.invalidOutSignatures.has(signature) ||
      state.queuedTransferOuts.some((candidate) => candidate.signature === signature)
    );
  }

  private enqueueBundlerFunderCandidate(
    state: BundlerFunderWatchState,
    candidate: FunderTransferOutCandidate,
    reason: string,
  ): boolean {
    if (
      state.invalidOutSignatures.has(candidate.signature) ||
      state.queuedTransferOuts.some((queued) => queued.signature === candidate.signature)
    ) {
      return false;
    }
    if (
      state.queuedTransferOuts.length >=
      BUNDLER_FUNDER_MAX_QUEUED_TRANSFER_OUT_CANDIDATES
    ) {
      this.log.warn("Dropping stacked feePayer transfer-out candidate because queue is full", {
        mint: state.mint,
        funderWallet: state.funderWallet,
        recipient: candidate.recipient,
        signature: candidate.signature,
        amountSol: candidate.amountSol,
        queuedCandidates: state.queuedTransferOuts.length,
        maxQueuedCandidates: BUNDLER_FUNDER_MAX_QUEUED_TRANSFER_OUT_CANDIDATES,
        reason,
      });
      return false;
    }
    state.queuedTransferOuts.push(candidate);
    state.validOutSignatures.add(candidate.signature);
    this.log.info("Stacked feePayer transfer-out candidate for later recipient watch", {
      mint: state.mint,
      funderWallet: state.funderWallet,
      recipient: candidate.recipient,
      signature: candidate.signature,
      amountSol: candidate.amountSol,
      queuedCandidates: state.queuedTransferOuts.length,
      activeRecipientWatches: state.recipientWatches.size,
      confirmedRecipientBuys: this.countConfirmedFunderRecipientBuys(state),
      maxConfirmedRecipientBuys: BUNDLER_FUNDER_MAX_RECIPIENT_WATCHES,
      reason,
    });
    return true;
  }

  private nextRecipientHeliusPreferredIndex(
    state: BundlerFunderWatchState,
  ): number {
    const candidates = this.heliusPool.filter(
      (entry) => entry.index !== HELIUS_POOL_MC_RESERVED_INDEX,
    );
    if (candidates.length === 0) return 0;
    return candidates[state.recipientWatches.size % candidates.length].index;
  }

  private async activateOrQueueBundlerFunderCandidate(
    state: BundlerFunderWatchState,
    candidate: FunderTransferOutCandidate,
    reason: string,
  ): Promise<FunderRecipientWatch | null> {
    if (this.hasReachedFunderRecipientBuyCap(state)) {
      await this.stopBundlerFunderSourceDiscovery(
        state,
        "two recipients already bought the monitored token",
      );
      return null;
    }
    if (state.recipientWatches.size >= BUNDLER_FUNDER_MAX_RECIPIENT_WATCHES) {
      if (candidate.normalTinyTransferMode) {
        await this.stopBundlerFunderSourceDiscovery(
          state,
          "first two normal-mode tiny transfer recipients already selected",
        );
      } else {
        this.enqueueBundlerFunderCandidate(state, candidate, reason);
      }
      return null;
    }
    const watch = this.addBundlerFunderRecipientWatch(state, {
      recipient: candidate.recipient,
      signature: candidate.signature,
      amountSol: candidate.amountSol,
      timestamp: candidate.timestamp,
      buyTriggersEntry: true,
      normalTinyTransferMode: candidate.normalTinyTransferMode,
    });
    if (!watch) {
      this.enqueueBundlerFunderCandidate(state, candidate, reason);
      return null;
    }
    await this.syncThenSubscribeFunderRecipient(state, watch, "accepted candidate");
    if (!state.recipientWatches.has(candidate.recipient)) {
      void this.promoteQueuedBundlerFunderCandidates(
        state,
        "recipient failed first sync",
      );
      return null;
    }
    if (this.buySubmitted || watch.tokenBuyObserved) return watch;
    if (watch.normalTinyTransferMode) return watch;
    const acceptedBySwapHistory = await this.maybeBuyFromRecipientSwapHistory(
      state,
      watch,
    );
    if (!acceptedBySwapHistory) {
      void this.promoteQueuedBundlerFunderCandidates(
        state,
        "recipient missing recent swap history",
      );
      return null;
    }
    return watch;
  }

  private async promoteQueuedBundlerFunderCandidates(
    state: BundlerFunderWatchState,
    reason: string,
  ): Promise<void> {
    if (this.hasReachedFunderRecipientBuyCap(state)) {
      await this.stopBundlerFunderSourceDiscovery(
        state,
        "two recipients bought the monitored token",
      );
      return;
    }
    while (
      state.recipientWatches.size < BUNDLER_FUNDER_MAX_RECIPIENT_WATCHES &&
      state.queuedTransferOuts.length > 0
    ) {
      const candidate = state.queuedTransferOuts.shift()!;
      if (state.invalidOutSignatures.has(candidate.signature)) continue;
      if (state.recipientWatches.has(candidate.recipient)) continue;
      const watch = this.addBundlerFunderRecipientWatch(state, {
        recipient: candidate.recipient,
        signature: candidate.signature,
        amountSol: candidate.amountSol,
        timestamp: candidate.timestamp,
        buyTriggersEntry: true,
        normalTinyTransferMode: candidate.normalTinyTransferMode,
      });
      if (!watch) continue;
      this.log.warn("Promoted stacked feePayer transfer-out candidate into recipient watch", {
        mint: state.mint,
        funderWallet: state.funderWallet,
        recipient: candidate.recipient,
        signature: candidate.signature,
        amountSol: candidate.amountSol,
        queuedCandidates: state.queuedTransferOuts.length,
        activeRecipientWatches: state.recipientWatches.size,
        reason,
      });
      void this.sendTelegramSafe(
        [
          `<b>🟡 ${this.label} Stacked Candidate Promoted</b>`,
          `Token: <code>${state.mint}</code>`,
          `FeePayer: <code>${state.funderWallet}</code>`,
          `Recipient: <code>${candidate.recipient}</code>`,
          `Funding tx: <code>${candidate.signature}</code>`,
          `Amount: <b>${candidate.amountSol.toFixed(4)} SOL</b>`,
          "",
          `Watching its first ${BUNDLER_FUNDER_RECIPIENT_FIRST_TX_WINDOW} post-funding txs for a buy of this token.`,
        ].join("\n"),
        "stacked candidate promoted notification",
      );
      await this.syncThenSubscribeFunderRecipient(state, watch, "promoted stacked candidate");
      if (!state.recipientWatches.has(candidate.recipient)) continue;
      if (this.buySubmitted || watch.tokenBuyObserved) continue;
      if (watch.normalTinyTransferMode) continue;
      const acceptedBySwapHistory = await this.maybeBuyFromRecipientSwapHistory(
        state,
        watch,
      );
      if (!acceptedBySwapHistory) continue;
    }
  }

  private async stopBundlerFunderSourceDiscovery(
    state: BundlerFunderWatchState,
    reason: string,
  ): Promise<void> {
    if (this.bundlerFunderLogsSubId !== null) {
      const subId = this.bundlerFunderLogsSubId;
      this.bundlerFunderLogsSubId = null;
      await this.connection.removeOnLogsListener(subId).catch(() => undefined);
    }
    if (this.bundlerFunderWsSyncTimer) {
      clearTimeout(this.bundlerFunderWsSyncTimer);
      this.bundlerFunderWsSyncTimer = null;
    }
    this.isBundlerFunderSyncing = false;
    this.bundlerFunderSyncPending = false;
    this.bundlerFunderSyncPendingForce = false;
    state.queuedTransferOuts = [];
    this.log.warn("Stopped shared feePayer transfer-out discovery", {
      mint: state.mint,
      funderWallet: state.funderWallet,
      confirmedRecipientBuys: this.countConfirmedFunderRecipientBuys(state),
      activeRecipientWatches: state.recipientWatches.size,
      reason,
    });
  }

  private async inspectBundlerFunderTransaction(
    state: BundlerFunderWatchState,
    tx: HeliusTransaction,
  ): Promise<boolean> {
    this.recordLowFundingFunderTx(state, tx);
    const transferOut = this.extractSolTransferOutFromWallet(
      tx,
      state.funderWallet,
      0,
    );
    if (!transferOut) return false;
    if (transferOut.amountSol > BUNDLER_FUNDER_MAX_NORMAL_TRANSFER_OUT_SOL) {
      const watchedWallet = state.funderWallet;
      const migrated = await this.maybeMoveBundlerFunderWatchAfterLargeDrain(
        state,
        tx,
        transferOut,
      );
      this.log.info("Skipping feePayer transfer-out above normal-mode max amount", {
        mint: state.mint,
        funderWallet: watchedWallet,
        currentFunderWallet: state.funderWallet,
        signature: tx.signature,
        amountSol: transferOut.amountSol,
        maxTransferOutSol: BUNDLER_FUNDER_MAX_NORMAL_TRANSFER_OUT_SOL,
        recipient: transferOut.to,
        migrated,
      });
      return migrated;
    }
    let transferOutUsd: number | null = null;
    const solPriceUsd = await this.getCachedSolPriceUsd();
    transferOutUsd = solPriceUsd !== null ? transferOut.amountSol * solPriceUsd : null;
    if (transferOutUsd === null) {
      this.log.warn("Skipping tiny recipient check because SOL/USD is unavailable", {
        mint: state.mint,
        funderWallet: state.funderWallet,
        signature: tx.signature,
        amountSol: transferOut.amountSol,
        lowFundingMode: state.lowFundingMode,
      });
      return false;
    }
    if (state.lowFundingMode) {
      if (transferOutUsd >= BUNDLER_FUNDER_NORMAL_TINY_TRANSFER_OUT_MAX_USD) return false;
      if (this.hasSolIncomingToWallet(tx, state.funderWallet)) return false;
      await this.handleLowFundingTinyTransferOut(state, tx, transferOut, transferOutUsd);
      return false;
    }
    if (transferOutUsd >= BUNDLER_FUNDER_NORMAL_TINY_TRANSFER_OUT_MAX_USD) {
      this.log.debug("Skipping normal-mode feePayer transfer-out above tiny-recipient USD cap", {
        mint: state.mint,
        funderWallet: state.funderWallet,
        signature: tx.signature,
        amountSol: transferOut.amountSol,
        amountUsd: transferOutUsd,
        maxTinyTransferUsd: BUNDLER_FUNDER_NORMAL_TINY_TRANSFER_OUT_MAX_USD,
        recipient: transferOut.to,
      });
      return false;
    }
    if (this.hasSolIncomingToWallet(tx, state.funderWallet)) {
      this.log.debug("Skipping funder transfer-out because same tx also has transfer-in", {
        mint: state.mint,
        funderWallet: state.funderWallet,
        signature: tx.signature,
        amountSol: transferOut.amountSol,
        recipient: transferOut.to,
      });
      return false;
    }
    if (state.bundlerWallets.has(transferOut.to)) {
      this.log.info("Skipping feePayer transfer-out because recipient is one of the first-four bundlers", {
        mint: state.mint,
        funderWallet: state.funderWallet,
        signature: tx.signature,
        amountSol: transferOut.amountSol,
        recipient: transferOut.to,
        bundlerWallets: [...state.bundlerWallets],
      });
      return false;
    }
    if (
      this.isKnownFunderCandidate(state, tx.signature)
    ) {
      return false;
    }
    if (transferOutUsd < BUNDLER_FUNDER_NORMAL_TINY_MIN_BUY_USD) {
      this.log.info("Skipping normal-mode feePayer tiny transfer below minimum buy USD", {
        mint: state.mint,
        funderWallet: state.funderWallet,
        signature: tx.signature,
        recipient: transferOut.to,
        amountSol: transferOut.amountSol,
        amountUsd: transferOutUsd,
        minBuyUsd: BUNDLER_FUNDER_NORMAL_TINY_MIN_BUY_USD,
      });
      return false;
    }

    const exitPercent = transferOutUsd <= BUNDLER_FUNDER_NORMAL_TINY_MID_MAX_USD
      ? BUNDLER_FUNDER_NORMAL_TINY_MID_EXIT_PERCENT
      : BUNDLER_FUNDER_NORMAL_TINY_HIGH_EXIT_PERCENT;
    const watch = this.addBundlerFunderRecipientWatch(state, {
      recipient: transferOut.to,
      signature: tx.signature,
      amountSol: transferOut.amountSol,
      timestamp: tx.timestamp,
      buyTriggersEntry: false,
      normalTinyTransferMode: true,
    });
    if (!watch) return false;
    watch.normalTinyExitPercent = exitPercent;
    watch.tokenBuyObserved = true;
    watch.firstBuySignature = tx.signature;
    watch.firstBuyTimestamp = tx.timestamp;

    this.log.warn("Normal-mode shared feePayer tiny transfer accepted for immediate buy", {
      mint: state.mint,
      funderWallet: state.funderWallet,
      recipient: transferOut.to,
      amountSol: transferOut.amountSol,
      amountUsd: transferOutUsd,
      exitPercent,
      exitBand: transferOutUsd <= BUNDLER_FUNDER_NORMAL_TINY_MID_MAX_USD ? "2.5-5" : "gt5",
      signature: tx.signature,
    });

    void this.sendTelegramSafe(
      [
        `<b>🟢 ${this.label} Normal FeePayer Tiny Funding Buy Gate</b>`,
        `Token: <code>${state.mint}</code>`,
        `FeePayer: <code>${state.funderWallet}</code>`,
        `Recipient: <code>${transferOut.to}</code>`,
        `Amount: <b>${transferOut.amountSol.toFixed(4)} SOL</b>`,
        `Amount USD: <b>$${transferOutUsd.toFixed(2)}</b>`,
        `Rule: <b>$2.50-$5.00 => +90% MC exit; >$5.00 => +180% MC exit</b>`,
        `Selected exit: <b>+${exitPercent}% MC</b>`,
        `Tx: <code>${tx.signature}</code>`,
      ].join("\n"),
      "normal feePayer tiny funding buy gate notification",
    );

    await this.emitBundlerFunderBuy(
      state,
      watch,
      tx.signature,
      `normal-mode shared feePayer tiny transfer accepted at $${transferOutUsd.toFixed(2)} with +${exitPercent}% MC exit`,
      false,
      tx,
      exitPercent,
    );
    return false;
  }

  private async maybeMoveBundlerFunderWatchAfterLargeDrain(
    state: BundlerFunderWatchState,
    tx: HeliusTransaction,
    transferOut: { to: string; amountSol: number },
  ): Promise<boolean> {
    if (!Number.isFinite(tx.timestamp) || tx.timestamp <= 0) return false;
    if (!transferOut.to || transferOut.to === state.funderWallet) return false;
    try {
      let balance = await this.getConfirmedWalletBalanceAt(
        state.funderWallet,
        NATIVE_SOL_BALANCE_MINT,
        tx.timestamp,
      );
      let balanceRaw = BigInt(balance.balanceRaw || "0");
      let balanceTimestamp = tx.timestamp;
      if (balanceRaw !== 0n) {
        const nextSecondBalance = await this.getConfirmedWalletBalanceAt(
          state.funderWallet,
          NATIVE_SOL_BALANCE_MINT,
          tx.timestamp + 1,
        );
        const nextSecondBalanceRaw = BigInt(nextSecondBalance.balanceRaw || "0");
        if (nextSecondBalanceRaw === 0n) {
          balance = nextSecondBalance;
          balanceRaw = nextSecondBalanceRaw;
          balanceTimestamp = tx.timestamp + 1;
        }
      }
      this.log.info("Checked shared feePayer balance after over-max transfer-out", {
        mint: state.mint,
        watchedWallet: state.funderWallet,
        recipient: transferOut.to,
        signature: tx.signature,
        timestamp: tx.timestamp,
        balanceTimestamp,
        amountSol: transferOut.amountSol,
        balance: balance.balance,
        balanceRaw: balance.balanceRaw,
      });
      if (balanceRaw !== 0n) return false;

      await this.switchBundlerFunderWatchAddress(
        state,
        transferOut.to,
        tx.signature,
        `Over-${BUNDLER_FUNDER_MAX_NORMAL_TRANSFER_OUT_SOL.toFixed(0)} SOL transfer-out drained watched wallet to zero; continuing this token's feePayer monitor from receiver.`,
      );
      return true;
    } catch (err) {
      void this.heliusClient.handlePossibleRateLimitError(err);
      this.log.warn("Failed to check shared feePayer balance after over-max transfer-out", {
        mint: state.mint,
        watchedWallet: state.funderWallet,
        recipient: transferOut.to,
        signature: tx.signature,
        timestamp: tx.timestamp,
        amountSol: transferOut.amountSol,
        error: err instanceof Error ? err.message : String(err),
      });
      return false;
    }
  }

  private addBundlerFunderRecipientWatch(
    state: BundlerFunderWatchState,
    candidate: {
      recipient: string;
      signature: string;
      amountSol: number;
      timestamp: number;
      buyTriggersEntry: boolean;
      normalTinyTransferMode: boolean;
    },
  ): FunderRecipientWatch | null {
    let watch = state.recipientWatches.get(candidate.recipient);
    if (watch) return watch;
    if (state.recipientWatches.size >= BUNDLER_FUNDER_MAX_RECIPIENT_WATCHES) {
      this.log.warn("Shared feePayer recipient watch cap reached; confirmed transfer-out recipient not watched", {
        mint: state.mint,
        funderWallet: state.funderWallet,
        recipient: candidate.recipient,
        candidateSignature: candidate.signature,
        cap: BUNDLER_FUNDER_MAX_RECIPIENT_WATCHES,
      });
      return null;
    }
    state.validOutSignatures.add(candidate.signature);
    watch = {
      wallet: candidate.recipient,
      fundingSignature: candidate.signature,
      fundingTimestamp: candidate.timestamp,
      outAmountSol: candidate.amountSol,
      heliusPreferredIndex: this.nextRecipientHeliusPreferredIndex(state),
      tokenActions: [],
      observedTxSignatures: new Set<string>(),
      tokenBuyObserved: false,
      zeroSolBalanceSignatures: new Set<string>(),
      buyTriggersEntry: candidate.buyTriggersEntry,
      boughtAmount: 0,
      soldAmount: 0,
      firstBuySignature: null,
      firstBuyTimestamp: null,
      normalTinyTransferMode: candidate.normalTinyTransferMode,
      normalTinyExitPercent: null,
      lowFundingCopySellOnSellAll: false,
      lowFundingTinyUsdBand: null,
      postEntrySwapSignature: null,
      postEntrySwapBaselineSignatures: new Set<string>(),
    };
    state.recipientWatches.set(candidate.recipient, watch);
    return watch;
  }

  private async syncThenSubscribeFunderRecipient(
    state: BundlerFunderWatchState,
    watch: FunderRecipientWatch,
    reason: string,
  ): Promise<void> {
    this.log.info("Syncing shared feePayer recipient before websocket subscribe", {
      mint: state.mint,
      wallet: watch.wallet,
      fundingSignature: watch.fundingSignature,
      fundingTimestamp: watch.fundingTimestamp,
      reason,
    });
    this.markFunderRecipientDirty(watch.wallet);
    await this.syncFunderRecipientBatch(true);
    if (!state.recipientWatches.has(watch.wallet)) return;
    if (watch.normalTinyTransferMode && !watch.tokenBuyObserved) {
      this.log.info("Normal tiny recipient skipped: no current-token buy before tiny funding", {
        mint: state.mint,
        wallet: watch.wallet,
        fundingSignature: watch.fundingSignature,
        fundingTimestamp: watch.fundingTimestamp,
        reason,
      });
      state.validOutSignatures.delete(watch.fundingSignature);
      this.removeFunderRecipientWatch(
        watch.wallet,
        "no current-token buy before normal tiny funding",
      );
      return;
    }

    this.subscribeFunderRecipient(watch.wallet);

    this.markFunderRecipientDirty(watch.wallet);
    await this.syncFunderRecipientBatch(true);
    this.log.info("Shared feePayer recipient synced and subscribed", {
      mint: state.mint,
      wallet: watch.wallet,
      fundingSignature: watch.fundingSignature,
      observedTxCount: watch.observedTxSignatures.size,
      tokenBuyObserved: watch.tokenBuyObserved,
      reason,
    });
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
    return this.extractSolIncomingAmountToWallet(tx, wallet) > 0;
  }

  private extractSolIncomingAmountToWallet(
    tx: HeliusTransaction,
    wallet: string,
  ): number {
    const described = this.parseSolTransferDescription(tx.description);
    if (
      described &&
      described.to === wallet &&
      described.from !== wallet &&
      described.amountSol > 0
    ) {
      return described.amountSol;
    }

    const tokenIncoming = (tx.tokenTransfers ?? [])
      .filter(
        (transfer) =>
          transfer.mint === SOL_MINT &&
          transfer.toUserAccount === wallet &&
          transfer.fromUserAccount !== wallet &&
          (transfer.tokenAmount ?? 0) > 0,
      )
      .map((transfer) => transfer.tokenAmount ?? 0);
    const nativeIncoming = (tx.nativeTransfers ?? [])
      .filter(
        (transfer) =>
          transfer.toUserAccount === wallet &&
          transfer.fromUserAccount !== wallet &&
          (transfer.amount ?? 0) > 0,
      )
      .map((transfer) => (transfer.amount ?? 0) / LAMPORTS_PER_SOL);
    return Math.max(0, ...tokenIncoming, ...nativeIncoming);
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

  private recordLowFundingFunderTx(
    state: BundlerFunderWatchState,
    tx: HeliusTransaction,
  ): void {
    if (!state.lowFundingMode) return;
    if (state.lowFundingFunderTxs.some((entry) => entry.signature === tx.signature)) return;
    state.lowFundingFunderTxs.push({ signature: tx.signature, timestamp: tx.timestamp });
    state.lowFundingFunderTxs = state.lowFundingFunderTxs
      .filter((entry) => tx.timestamp - entry.timestamp <= 120)
      .sort((a, b) => a.timestamp - b.timestamp);
  }

  private findCleanLowFundingBundlerTinyGroup(
    state: BundlerFunderWatchState,
    afterTimestamp = 0,
  ): Array<{ signature: string; timestamp: number; recipient: string; amountSol: number; amountUsd: number }> | null {
    const tinyEvents = state.lowFundingTinyTransferOuts
      .filter((entry) => entry.timestamp > afterTimestamp)
      .sort((a, b) => a.timestamp - b.timestamp);
    const bundlers = [...state.bundlerWallets];
    for (let i = 0; i < tinyEvents.length; i += 1) {
      const start = tinyEvents[i].timestamp;
      const end = start + BUNDLER_FUNDER_LOW_FUNDING_TINY_GROUP_SECONDS;
      const group = tinyEvents.filter(
        (entry) => entry.timestamp >= start && entry.timestamp <= end,
      );
      if (group.length !== BUNDLER_FUNDER_REQUIRED_COUNT) continue;
      const recipients = new Set(group.map((entry) => entry.recipient));
      if (recipients.size !== BUNDLER_FUNDER_REQUIRED_COUNT) continue;
      if (!bundlers.every((wallet) => recipients.has(wallet))) continue;

      const groupSignatures = new Set(group.map((entry) => entry.signature));
      const allFunderTxsInWindow = state.lowFundingFunderTxs.filter(
        (entry) => entry.timestamp >= start && entry.timestamp <= end,
      );
      if (allFunderTxsInWindow.length !== BUNDLER_FUNDER_REQUIRED_COUNT) continue;
      if (!allFunderTxsInWindow.every((entry) => groupSignatures.has(entry.signature))) continue;
      return group;
    }
    return null;
  }

  private async findWalletCurrentTokenActivity(
    wallet: string,
    mint: string,
    beforeOrAtTimestamp: number,
    preferredClientIndex: number,
  ): Promise<HeliusTransaction | null> {
    const txs = await this.withHeliusFallback(
      (client) => client.getWalletTransactionsDesc(wallet, INSIDER_HISTORY_LIMIT),
      preferredClientIndex,
    );
    return (
      txs.find((tx) => {
        if (tx.timestamp > beforeOrAtTimestamp) return false;
        if (!this.isRelevantMintTx(tx, mint)) return false;
        const action = this.classifyTx(tx, wallet, mint);
        return action === "buy" || action === "sell";
      }) ?? null
    );
  }

  private getLowFundingTinyUsdBand(amountUsd: number): "lt2_5" | "2_5_to_5" | "gt5" {
    if (amountUsd < BUNDLER_FUNDER_LOW_FUNDING_TINY_MIN_BUY_USD) return "lt2_5";
    if (amountUsd <= BUNDLER_FUNDER_LOW_FUNDING_TINY_COPYSELL_MIN_USD) return "2_5_to_5";
    return "gt5";
  }

  private getLowFundingTinySameBandGroup(
    state: BundlerFunderWatchState,
    timestamp: number,
    band: "lt2_5" | "2_5_to_5" | "gt5",
  ): Array<{ signature: string; timestamp: number; recipient: string; amountSol: number; amountUsd: number }> {
    const start = timestamp - BUNDLER_FUNDER_LOW_FUNDING_TINY_GROUP_SECONDS;
    const group = state.lowFundingTinyTransferOuts.filter(
      (entry) => entry.timestamp >= start && entry.timestamp <= timestamp,
    );
    if (group.length < 2) return [];
    if (group.some((entry) => this.getLowFundingTinyUsdBand(entry.amountUsd) !== band)) {
      return [];
    }
    return group;
  }
  private async handleLowFundingTinyTransferOut(
    state: BundlerFunderWatchState,
    tx: HeliusTransaction,
    transferOut: { to: string; amountSol: number },
    amountUsd: number,
  ): Promise<void> {
    if (state.lowFundingTinyTransferOuts.some((entry) => entry.signature === tx.signature)) return;
    const tinyEvent = {
      signature: tx.signature,
      timestamp: tx.timestamp,
      recipient: transferOut.to,
      amountSol: transferOut.amountSol,
      amountUsd,
    };
    state.lowFundingTinyTransferOuts.push(tinyEvent);
    state.lowFundingTinyTransferOuts = state.lowFundingTinyTransferOuts
      .filter((entry) => tx.timestamp - entry.timestamp <= 180)
      .sort((a, b) => a.timestamp - b.timestamp);

    if (!state.lowFundingTinyBundlerGateSeen) {
      const bundlerGroup = this.findCleanLowFundingBundlerTinyGroup(state);
      if (bundlerGroup) {
        state.lowFundingTinyBundlerGateSeen = true;
        this.log.warn("Low-funding tiny bundler gate passed", {
          mint: state.mint,
          sharedFeePayer: state.funderWallet,
          groupWindowSeconds: BUNDLER_FUNDER_LOW_FUNDING_TINY_GROUP_SECONDS,
          group: bundlerGroup,
        });
        void this.sendTelegramSafe(
          [
            `<b>✅ ${this.label} Low-Funding Tiny Bundler Gate</b>`,
            `Token: <code>${state.mint}</code>`,
            `FeePayer: <code>${state.funderWallet}</code>`,
            `Bundler tiny transfers: <b>${bundlerGroup.length}</b>`,
            `Window: <b>${BUNDLER_FUNDER_LOW_FUNDING_TINY_GROUP_SECONDS}s</b>`,
            "",
            "Waiting for the next under-$10 tiny transfer to a non-bundler wallet with prior activity in this token.",
          ].join("\n"),
          "low-funding tiny bundler gate notification",
        );
      }
      return;
    }

    if (this.buySubmitted || this.phase === "holding") {
      const sellGroup = this.findCleanLowFundingBundlerTinyGroup(
        state,
        state.lowFundingTinyEntryTimestamp ?? 0,
      );
      if (!sellGroup) return;
      const sellGroupKey = sellGroup.map((entry) => entry.signature).join(":");
      if (state.lowFundingTinySellGroupSignatures.has(sellGroupKey)) return;
      state.lowFundingTinySellGroupSignatures.add(sellGroupKey);
      state.lowFundingTinySoldUsdBands.add("2_5_to_5");
      await this.triggerPositionSell(
        state.mint,
        "Low-funding shared feePayer made a clean post-entry 4-bundler tiny-transfer group",
        [
          "<b>🚨 Low-Funding Tiny Bundler Exit</b>",
          `Token: <code>${state.mint}</code>`,
          `FeePayer: <code>${state.funderWallet}</code>`,
          `Window: <b>${BUNDLER_FUNDER_LOW_FUNDING_TINY_GROUP_SECONDS}s</b>`,
          `Bundler txs: <b>${sellGroup.length}</b>`,
          "",
          "A clean post-entry group of 4 tiny transfers to the initial bundlers was detected. Selling position.",
        ],
        tx.signature,
      );
      return;
    }

    if (state.bundlerWallets.has(transferOut.to)) return;
    const tinyUsdBand = this.getLowFundingTinyUsdBand(amountUsd);
    const sameBandGroup = this.getLowFundingTinySameBandGroup(
      state,
      tx.timestamp,
      tinyUsdBand,
    );
    if (sameBandGroup.length < 2) {
      this.log.info("Low-funding tiny transfer skipped: no same-band 2-tx group in 10s", {
        mint: state.mint,
        wallet: transferOut.to,
        fundingSignature: tx.signature,
        amountSol: transferOut.amountSol,
        amountUsd,
        tinyUsdBand,
        groupWindowSeconds: BUNDLER_FUNDER_LOW_FUNDING_TINY_GROUP_SECONDS,
      });
      return;
    }
    if (amountUsd < BUNDLER_FUNDER_LOW_FUNDING_TINY_MIN_BUY_USD) {
      this.log.info("Low-funding tiny transfer skipped: below minimum USD band", {
        mint: state.mint,
        wallet: transferOut.to,
        fundingSignature: tx.signature,
        amountSol: transferOut.amountSol,
        amountUsd,
        tinyUsdBand,
        sameBandGroupCount: sameBandGroup.length,
        minUsd: BUNDLER_FUNDER_LOW_FUNDING_TINY_MIN_BUY_USD,
      });
      return;
    }
    const buyUsdBand = tinyUsdBand === "lt2_5" ? null : tinyUsdBand;
    if (!buyUsdBand) return;
    if (state.lowFundingTinyBoughtUsdBands.has(buyUsdBand)) {
      this.log.info("Low-funding tiny transfer skipped: USD band already used for buy", {
        mint: state.mint,
        wallet: transferOut.to,
        fundingSignature: tx.signature,
        amountUsd,
        tinyUsdBand,
        boughtBands: [...state.lowFundingTinyBoughtUsdBands],
      });
      return;
    }
    if (state.lowFundingTinyCandidateWallets.size >= BUNDLER_FUNDER_MAX_RECIPIENT_WATCHES) return;
    if (state.lowFundingTinyCandidateWallets.has(transferOut.to)) return;
    const copySellOnSellAll = buyUsdBand === "gt5";

    let activityTx: HeliusTransaction | null = null;
    try {
      activityTx = await this.findWalletCurrentTokenActivity(
        transferOut.to,
        state.mint,
        tx.timestamp,
        this.nextRecipientHeliusPreferredIndex(state),
      );
    } catch (err) {
      void this.heliusClient.handlePossibleRateLimitError(err);
      this.log.warn("Low-funding tiny recipient activity check failed", {
        mint: state.mint,
        wallet: transferOut.to,
        fundingSignature: tx.signature,
        error: err instanceof Error ? err.message : String(err),
      });
      return;
    }
    if (!activityTx) {
      this.log.info("Low-funding tiny recipient skipped: no prior current-token activity", {
        mint: state.mint,
        wallet: transferOut.to,
        fundingSignature: tx.signature,
      });
      return;
    }

    state.lowFundingTinyCandidateWallets.add(transferOut.to);
    state.lowFundingTinyEntryTimestamp = tx.timestamp;
    const watch = this.addBundlerFunderRecipientWatch(state, {
      recipient: transferOut.to,
      signature: tx.signature,
      amountSol: transferOut.amountSol,
      timestamp: tx.timestamp,
      buyTriggersEntry: false,
      normalTinyTransferMode: false,
    });
    this.log.warn("Low-funding tiny recipient qualified; triggering buy", {
      mint: state.mint,
      wallet: transferOut.to,
      fundingSignature: tx.signature,
      activitySignature: activityTx.signature,
      candidateCount: state.lowFundingTinyCandidateWallets.size,
      maxCandidates: BUNDLER_FUNDER_MAX_RECIPIENT_WATCHES,
      tinyUsdBand,
      sameBandGroupCount: sameBandGroup.length,
    });
    if (watch) {
      watch.lowFundingCopySellOnSellAll = copySellOnSellAll;
      watch.lowFundingTinyUsdBand = buyUsdBand;
      if (activityTx) {
        const activityAction = this.classifyTx(activityTx, watch.wallet, state.mint);
        watch.firstBuySignature = activityTx.signature;
        watch.firstBuyTimestamp = activityTx.timestamp;
        if (activityAction === "buy") {
          watch.boughtAmount += this.extractTokenAmountForWallet(
            activityTx,
            watch.wallet,
            state.mint,
            "buy",
          );
        }
      }
      if (copySellOnSellAll) {
        this.subscribeFunderRecipient(watch.wallet);
        this.markFunderRecipientDirty(watch.wallet);
        void this.syncFunderRecipientBatch(true);
      }
      state.lowFundingTinyBoughtUsdBands.add(buyUsdBand);
      await this.emitLowFundingRecipientBuy(
        state,
        watch,
        tx.signature,
        copySellOnSellAll
          ? "low-funding under-$10 recipient had prior token activity; >$5 band uses recipient sell-all copy-sell"
          : "low-funding under-$10 recipient had prior token activity; $2.50-$5 band uses clean 4-bundler exit",
        false,
      );
    }
  }
  private async emitLowFundingSharedFeePayerBuy(
    state: BundlerFunderWatchState,
    signature: string,
    details: {
      windowTxCount: number;
      transferOutTxCount: number;
      largestIncomingSol: number;
      syncStart: {
        signature: string;
        timestamp: number;
        bundlerWallet: string;
      };
      latestBundlerBuyTimestamp: number;
      sharedFeePayerBalanceAfterInitialTransfers?: number;
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
      const currentMc = await this.gmgnClient.fetchTokenMarketCapUsd(state.mint);
      if (currentMc === null) {
        this.log.warn(
          "Low-funding shared feePayer condition passed, but current market cap is unavailable; waiting before buy",
          { mint: state.mint, sharedFeePayer: state.funderWallet, signature },
        );
        return;
      }
      if (currentMc < INSIDER_RUG_MARKET_CAP_USD) {
        this.log.warn(
          "Low-funding shared feePayer condition passed, but token is below rug threshold; resetting instead of buying",
          {
            mint: state.mint,
            sharedFeePayer: state.funderWallet,
            currentMc,
            rugThresholdUsd: INSIDER_RUG_MARKET_CAP_USD,
          },
        );
        await this.resetForNewToken(true);
        return;
      }

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
        monitoredWallet: state.funderWallet,
        tradersListStr: [
          "<b>Low-Funding Shared FeePayer Buy Gate Passed</b>",
          `FeePayer: <code>${state.funderWallet}</code>`,
          `Largest bundler funding: <b>${state.largestFundingSol.toFixed(4)} SOL</b>`,
          `Low-funding threshold: <b>${BUNDLER_FUNDER_LOW_FUNDING_SOL.toFixed(2)} SOL</b>`,
          `Window txs: <b>${details.windowTxCount}</b>`,
          `Transfer-out txs in window: <b>${details.transferOutTxCount}/${BUNDLER_FUNDER_LOW_FUNDING_MAX_TRANSFER_OUT_TXS}</b>`,
          `Largest transfer-in to feePayer in window: <b>${details.largestIncomingSol.toFixed(4)} SOL</b>`,
          details.sharedFeePayerBalanceAfterInitialTransfers !== undefined
            ? `FeePayer balance after initial transfers: <b>${details.sharedFeePayerBalanceAfterInitialTransfers.toFixed(4)} SOL</b>`
            : "",
          `Low-funding sync start tx: <code>${details.syncStart.signature}</code>`,
          `Sync-start bundler: <code>${details.syncStart.bundlerWallet}</code>`,
          `Current MC: <b>$${currentMc.toLocaleString()}</b>`,
          "Sell rule: <b>MC profit target disabled</b>; waiting for clean post-entry 4-bundler tiny-transfer exit or rug.",
        ].join("\n"),
      });
    } finally {
      this.isBuyGateEvaluating = false;
    }
  }

  private async emitLowFundingRecipientBuy(
    state: BundlerFunderWatchState,
    watch: FunderRecipientWatch,
    signature: string,
    gateDescription = "recipient bought token",
    disableProfitExitAfterBuy = false,
    triggerTx?: HeliusTransaction,
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
          "Low-funding recipient bought token, but current market cap is unavailable; waiting before buy",
          { mint: state.mint, recipient: watch.wallet, signature },
        );
        return;
      }
      if (currentMc < INSIDER_RUG_MARKET_CAP_USD) {
        this.log.warn(
          "Low-funding recipient bought token, but token is below rug threshold; resetting instead of buying",
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
      if (
        triggerTx &&
        !watch.normalTinyTransferMode &&
        !(await this.ensureRecipientBuyMeetsMinUsd(state, watch, triggerTx))
      ) {
        return;
      }

      this.setEntryMc(currentMc);
      this.setBuyExecuting(true);
      this.buySubmitted = true;
      this.preBuyStopped = true;
      this.disableProfitExitAfterBuy = true;
      this.emit("buyTrigger", {
        followedWallet: this.followedWallet!,
        mint: state.mint,
        signature,
        buySol: this.buySol,
        entryMc: currentMc,
        monitoredWallet: watch.wallet,
        tradersListStr: [
          "<b>Low-Funding Recipient Buy Gate Passed</b>",
          `FeePayer: <code>${state.funderWallet}</code>`,
          `Recipient: <code>${watch.wallet}</code>`,
          `Transfer-out: <b>${watch.outAmountSol.toFixed(4)} SOL</b>`,
          `Low-funding candidate threshold: <b>${BUNDLER_FUNDER_LOW_FUNDING_MIN_TRANSFER_OUT_SOL.toFixed(2)} SOL</b>`,
          `Trigger tx: <code>${signature}</code>`,
          `Buy gate: <b>${gateDescription}</b>`,
          `Current MC: <b>$${currentMc.toLocaleString()}</b>`,
          "",
          "",
          "Sell rule: <b>MC profit target disabled</b>; waiting for clean post-entry 4-bundler tiny-transfer exit or rug.",
        ].join("\n"),
      });
    } finally {
      this.isBuyGateEvaluating = false;
    }
  }

  private async emitBundlerFunderBuy(
    state: BundlerFunderWatchState,
    watch: FunderRecipientWatch,
    signature: string,
    gateDescription = `recipient bought this token within its first ${BUNDLER_FUNDER_RECIPIENT_FIRST_TX_WINDOW} post-funding txs`,
    disableProfitExitAfterBuy = true,
    triggerTx?: HeliusTransaction,
    exitPercentOverride?: number,
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
          "Shared feePayer recipient bought token, but current market cap is unavailable; waiting before buy",
          { mint: state.mint, recipient: watch.wallet, signature },
        );
        return;
      }
      if (currentMc < INSIDER_RUG_MARKET_CAP_USD) {
        this.log.warn(
          "Shared feePayer recipient bought token, but token is below rug threshold; resetting instead of buying",
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
      if (
        triggerTx &&
        !watch.normalTinyTransferMode &&
        !(await this.ensureRecipientBuyMeetsMinUsd(state, watch, triggerTx))
      ) {
        return;
      }

      const exitPercent = exitPercentOverride ?? watch.normalTinyExitPercent ?? this.exitPercent;
      const newExitMc = currentMc * (1 + exitPercent / 100);
      this.setExitMc(newExitMc);
      this.setEntryMc(currentMc);
      this.setBuyExecuting(true);
      this.buySubmitted = true;
      this.preBuyStopped = true;
      this.disableProfitExitAfterBuy = watch.normalTinyTransferMode
        ? false
        : disableProfitExitAfterBuy;
      this.emit("buyTrigger", {
        followedWallet: this.followedWallet!,
        mint: state.mint,
        signature,
        buySol: this.buySol,
        entryMc: currentMc,
        monitoredWallet: watch.wallet,
        tradersListStr: [
          "<b>Shared Bundler Recipient Buy Gate Passed</b>",
          `FeePayer: <code>${state.funderWallet}</code>`,
          `Recipient: <code>${watch.wallet}</code>`,
          `Transfer-out: <b>${watch.outAmountSol.toFixed(4)} SOL</b>`,
          watch.normalTinyTransferMode
            ? `Tiny transfer cap: <b>$${BUNDLER_FUNDER_NORMAL_TINY_TRANSFER_OUT_MAX_USD.toFixed(0)}</b>`
            : `Threshold: <b>${state.minTransferOutSol.toFixed(4)} SOL</b>`,
          `Max valid transfer-out: <b>${BUNDLER_FUNDER_MAX_NORMAL_TRANSFER_OUT_SOL.toFixed(0)} SOL</b>`,
          `Confirmed recipient-buy cap: <b>${BUNDLER_FUNDER_MAX_RECIPIENT_WATCHES}</b>`,
          `Trigger tx: <code>${signature}</code>`,
          `Buy gate: <b>${gateDescription}</b>`,
          `Current MC: <b>$${currentMc.toLocaleString()}</b>`,
          "",
          watch.normalTinyTransferMode
            ? `Sell rule: +${exitPercent}% MC target remains active; rug exits remain active.`
            : disableProfitExitAfterBuy
            ? "Sell rule: MC target is disabled; rug, recipient sell-all, and recipient zero-SOL exits remain active."
            : "Recipient watcher stays active for the current token; MC target remains active until a current-token recipient buy is confirmed.",
        ].join("\n"),
      });
    } finally {
      this.isBuyGateEvaluating = false;
    }
  }

  private async maybeBuyFromRecipientSwapHistory(
    state: BundlerFunderWatchState,
    watch: FunderRecipientWatch,
  ): Promise<boolean> {
    if (this.buySubmitted || watch.tokenBuyObserved) return true;
    try {
      const history = await this.getConfirmedWalletSwapHistory(
        watch.wallet,
        50,
        watch.heliusPreferredIndex,
      );
      const swaps = history.filter((tx) => !tx.type || tx.type === "SWAP");
      const minRecentSwapTimestamp = Math.floor(
        (Date.now() - BUNDLER_FUNDER_RECIPIENT_SWAP_HISTORY_MAX_AGE_MS) / 1_000,
      );
      const recentSwaps = swaps.filter(
        (tx) =>
          Number.isFinite(tx.timestamp) &&
          tx.timestamp >= minRecentSwapTimestamp,
      );
      const currentTokenSwap = recentSwaps.find((tx) =>
        this.isRelevantMintTx(tx, state.mint),
      );
      const newestSwap = swaps.reduce<HeliusTransaction | null>(
        (newest, tx) =>
          !newest || tx.timestamp > newest.timestamp ? tx : newest,
        null,
      );
      this.log.info("Checked recipient Helius SWAP history for buy gate", {
        mint: state.mint,
        wallet: watch.wallet,
        fundingSignature: watch.fundingSignature,
        swapCount: swaps.length,
        recentSwapCount: recentSwaps.length,
        maxSwapAgeDays: 3,
        newestSwapSignature: newestSwap?.signature ?? null,
        newestSwapTimestamp: newestSwap?.timestamp ?? null,
        currentTokenSwapSignature: currentTokenSwap?.signature ?? null,
      });
      if (recentSwaps.length === 0) {
        this.log.warn("Valid transfer-out recipient skipped: no recent swap history found", {
          mint: state.mint,
          wallet: watch.wallet,
          fundingSignature: watch.fundingSignature,
          swapCount: swaps.length,
          recentSwapCount: recentSwaps.length,
          maxSwapAgeDays: 3,
          newestSwapSignature: newestSwap?.signature ?? null,
          newestSwapTimestamp: newestSwap?.timestamp ?? null,
        });
        state.validOutSignatures.delete(watch.fundingSignature);
        void this.sendTelegramSafe(
          [
            `<b>⚪ ${this.label} Recipient Watch Skipped</b>`,
            `Token: <code>${state.mint}</code>`,
            `Recipient: <code>${watch.wallet}</code>`,
            `Funding tx: <code>${watch.fundingSignature}</code>`,
            `Swap history checked: <b>${swaps.length}</b> txs`,
            `Recent swaps within 3 days: <b>${recentSwaps.length}</b>`,
            "",
            "No recent SWAP history was found. Promoting the next stacked candidate.",
          ].join("\n"),
          "recipient recent swap history missing notification",
        );
        this.removeFunderRecipientWatch(
          watch.wallet,
          "no recent swap in Helius wallet history",
        );
        return false;
      }
      if (!currentTokenSwap) {
        this.log.info("Recipient has recent swap history for other token; continuing first-3-tx token-buy watch", {
          mint: state.mint,
          wallet: watch.wallet,
          fundingSignature: watch.fundingSignature,
          swapCount: swaps.length,
          recentSwapCount: recentSwaps.length,
          maxSwapAgeDays: 3,
          newestSwapSignature: newestSwap?.signature ?? null,
          newestSwapTimestamp: newestSwap?.timestamp ?? null,
        });
        return true;
      }

      this.log.warn("Recipient current-token SWAP history found; triggering buy immediately", {
        mint: state.mint,
        wallet: watch.wallet,
        fundingSignature: watch.fundingSignature,
        triggerSignature: currentTokenSwap.signature,
        swapCount: swaps.length,
        recentSwapCount: recentSwaps.length,
        maxSwapAgeDays: 3,
      });
      void this.sendTelegramSafe(
        [
          `<b>🟢 ${this.label} Recipient Swap History Gate</b>`,
          `Token: <code>${state.mint}</code>`,
          `Recipient: <code>${watch.wallet}</code>`,
          `Funding tx: <code>${watch.fundingSignature}</code>`,
          `Swap history tx: <code>${currentTokenSwap.signature}</code>`,
          "Current-token swap found: <b>yes</b>",
          "History age rule: <b>latest accepted swap must be within 3 days</b>",
          "",
          "Buy gate passed from recent Helius wallet SWAP history for this token.",
        ].join("\n"),
        "recipient swap history buy gate notification",
      );
      this.anchorFunderRecipientInitialBuyFromTx(
        state,
        watch,
        currentTokenSwap,
        "recent Helius wallet SWAP history",
      );
      if (state.lowFundingMode) {
        await this.emitLowFundingRecipientBuy(
          state,
          watch,
          currentTokenSwap.signature,
          "recipient Helius SWAP history includes this token within 3 days",
          false,
          currentTokenSwap,
        );
      } else {
        await this.emitBundlerFunderBuy(
          state,
          watch,
          currentTokenSwap.signature,
          "recipient Helius SWAP history includes this token within 3 days",
          true,
          currentTokenSwap,
        );
      }
      return true;
    } catch (err) {
      void this.heliusClient.handlePossibleRateLimitError(err);
      this.log.warn("Recipient Helius SWAP history check failed; continuing first-3-tx watch", {
        mint: state.mint,
        wallet: watch.wallet,
        fundingSignature: watch.fundingSignature,
        error: err instanceof Error ? err.message : String(err),
      });
      return true;
    }
  }

  private anchorFunderRecipientInitialBuyFromTx(
    state: BundlerFunderWatchState,
    watch: FunderRecipientWatch,
    tx: HeliusTransaction,
    source: string,
  ): void {
    if (watch.firstBuySignature) return;
    const action = this.classifyTx(tx, watch.wallet, state.mint);
    if (action !== "buy") return;
    const amount = this.extractTokenAmountForWallet(
      tx,
      watch.wallet,
      state.mint,
      "buy",
    );
    watch.tokenBuyObserved = true;
    watch.firstBuySignature = tx.signature;
    watch.firstBuyTimestamp = tx.timestamp;
    watch.boughtAmount += amount;
    if (!watch.tokenActions.some((entry) => entry.signature === tx.signature)) {
      watch.tokenActions.push({ kind: "buy", signature: tx.signature, amount });
    }
    this.log.warn("Anchored transfer-out recipient initial buy for post-buy audit", {
      mint: state.mint,
      wallet: watch.wallet,
      signature: tx.signature,
      amount,
      source,
    });
  }

  private async ensureRecipientBuyMeetsMinUsd(
    state: BundlerFunderWatchState,
    watch: FunderRecipientWatch,
    tx: HeliusTransaction,
  ): Promise<boolean> {
    const buySol = this.estimateRecipientBuySolSpent(tx, watch.wallet);
    const solPriceUsd = await this.getCachedSolPriceUsd();
    const buyUsd =
      buySol !== null && solPriceUsd !== null ? buySol * solPriceUsd : null;
    const passed =
      buyUsd !== null && buyUsd >= BUNDLER_FUNDER_RECIPIENT_MIN_BUY_USD;
    this.log.warn("Checked recipient buy USD gate before copybuy", {
      mint: state.mint,
      wallet: watch.wallet,
      signature: tx.signature,
      buySol,
      solPriceUsd,
      buyUsd,
      minBuyUsd: BUNDLER_FUNDER_RECIPIENT_MIN_BUY_USD,
      passed,
    });
    if (passed) return true;

    state.validOutSignatures.delete(watch.fundingSignature);
    void this.sendTelegramSafe(
      [
        `<b>⚪ ${this.label} Recipient Buy Too Small</b>`,
        `Token: <code>${state.mint}</code>`,
        `Recipient: <code>${watch.wallet}</code>`,
        `Funding tx: <code>${watch.fundingSignature}</code>`,
        `Buy tx: <code>${tx.signature}</code>`,
        buySol !== null ? `Buy SOL: <b>${buySol.toFixed(6)} SOL</b>` : "Buy SOL: <b>unknown</b>",
        solPriceUsd !== null ? `SOL price: <b>$${solPriceUsd.toFixed(2)}</b>` : "SOL price: <b>unknown</b>",
        buyUsd !== null ? `Buy USD: <b>$${buyUsd.toFixed(2)}</b>` : "Buy USD: <b>unknown</b>",
        `Required: <b>$${BUNDLER_FUNDER_RECIPIENT_MIN_BUY_USD.toFixed(0)}+</b>`,
        "",
        "Candidate skipped. Promoting the next stacked candidate.",
      ].join("\n"),
      "recipient buy below minimum usd notification",
    );
    this.removeFunderRecipientWatch(
      watch.wallet,
      `recipient buy below $${BUNDLER_FUNDER_RECIPIENT_MIN_BUY_USD.toFixed(0)} minimum`,
    );
    void this.promoteQueuedBundlerFunderCandidates(
      state,
      "recipient buy below minimum USD gate",
    );
    return false;
  }

  private estimateRecipientBuySolSpent(
    tx: HeliusTransaction,
    wallet: string,
  ): number | null {
    const nativeSpent = this.estimateWalletSolSpent(tx, wallet);
    if (nativeSpent !== null) return nativeSpent;

    const wrappedSolSpent = (tx.tokenTransfers ?? [])
      .filter(
        (transfer) =>
          transfer.mint === SOL_MINT &&
          transfer.fromUserAccount === wallet &&
          (transfer.tokenAmount ?? 0) > 0,
      )
      .reduce((sum, transfer) => sum + (transfer.tokenAmount ?? 0), 0);
    if (wrappedSolSpent > 0) return Number(wrappedSolSpent.toFixed(6));

    const balanceChangeSpent = (tx.balanceChanges ?? [])
      .filter(
        (change) =>
          (change.mint === NATIVE_SOL_BALANCE_MINT || change.mint === SOL_MINT) &&
          (change.amount ?? 0) < 0,
      )
      .reduce((sum, change) => sum + Math.abs(change.amount ?? 0), 0);
    if (balanceChangeSpent > 0) return Number(balanceChangeSpent.toFixed(6));
    return null;
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
        if (tx.signature === watch.fundingSignature) continue;
        if (!watch.normalTinyTransferMode && tx.timestamp < watch.fundingTimestamp) continue;
        await this.applyFunderRecipientTransaction(
          state,
          watch,
          tx,
          signature ? "notification" : "history",
        );
        if (!state.recipientWatches.has(wallet)) break;
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

  private async auditFunderRecipientsAfterBuy(): Promise<void> {
    const state = this.bundlerFunderWatch;
    if (!state || this.phase !== "holding" || this.positionSellTriggered) return;
    const watched = [...state.recipientWatches.values()].filter(
      (watch) => Boolean(watch.firstBuySignature),
    );
    if (watched.length === 0) return;

    this.log.warn("Post-buy recipient history audit started", {
      mint: state.mint,
      watchedRecipients: watched.map((watch) => watch.wallet),
    });

    for (const watch of watched) {
      if (this.positionSellTriggered) return;
      try {
        const txs = await this.withHeliusFallback(
          (client) =>
            client.getWalletTransactionsDesc(watch.wallet, INSIDER_HISTORY_LIMIT),
          watch.heliusPreferredIndex,
        );
        const sorted = [...txs].reverse();
        let afterInitialBuy = false;
        for (const tx of sorted) {
          if (tx.timestamp < watch.fundingTimestamp) continue;
          if (tx.signature === watch.firstBuySignature) {
            afterInitialBuy = true;
            continue;
          }
          if (!afterInitialBuy) continue;
          await this.sellIfNormalTinyRecipientSwappedAfterEntry(state, watch, tx);
          if (this.positionSellTriggered) return;
          if (!this.isRelevantMintTx(tx, state.mint)) continue;

          const action = this.classifyTx(tx, watch.wallet, state.mint);
          if (action === "buy") {
            this.log.info("Post-buy audit observed additional recipient buy; sell trigger disabled", {
              mint: state.mint,
              wallet: watch.wallet,
              firstBuySignature: watch.firstBuySignature,
              signature: tx.signature,
            });
            continue;
          }

          const remainingAmount = await this.getRecipientTokenBalanceAtTx(
            state,
            watch,
            tx,
          );
          this.log.info("Post-buy recipient token balance audit checked tx", {
            mint: state.mint,
            wallet: watch.wallet,
            signature: tx.signature,
            action,
            remainingAmount,
            lowFundingMode: state.lowFundingMode,
          });
          if (
            !state.lowFundingMode &&
            remainingAmount !== null &&
            remainingAmount <= 0
          ) {
            await this.triggerPositionSell(
              state.mint,
              `Shared feePayer recipient ${watch.wallet} exited token position after initial buy`,
              [
                "<b>🚨 Shared-Funder Recipient Exited Token</b>",
                `Token: <code>${state.mint}</code>`,
                `Recipient: <code>${watch.wallet}</code>`,
                `Initial buy: <code>${watch.firstBuySignature}</code>`,
                `Exit tx: <code>${tx.signature}</code>`,
                `Post-tx token balance: <b>${remainingAmount.toLocaleString()}</b>`,
              ],
              tx.signature,
            );
            return;
          }

          if (!state.lowFundingMode && !watch.normalTinyTransferMode) {
            await this.sellIfRecipientSolBalanceIsZero(state, watch, tx);
            if (this.positionSellTriggered) return;
          }
        }
      } catch (err) {
        void this.heliusClient.handlePossibleRateLimitError(err);
        this.log.warn("Post-buy recipient history audit failed", {
          mint: state.mint,
          wallet: watch.wallet,
          firstBuySignature: watch.firstBuySignature,
          error: err instanceof Error ? err.message : String(err),
        });
      }
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
    source: "history" | "notification" = "history",
  ): Promise<void> {
    const isNewObservedTx = !watch.observedTxSignatures.has(tx.signature);
    if (isNewObservedTx && !watch.tokenBuyObserved) {
      watch.observedTxSignatures.add(tx.signature);
    }

    if (watch.firstBuySignature) {
      if (!state.lowFundingMode && !watch.normalTinyTransferMode) {
        await this.sellIfRecipientSolBalanceIsZero(state, watch, tx);
        if (this.positionSellTriggered) return;
      } else if (
        !state.lowFundingMode &&
        watch.normalTinyTransferMode &&
        source === "notification" &&
        tx.timestamp > watch.fundingTimestamp
      ) {
        await this.sellIfRecipientSolBalanceIsZero(state, watch, tx);
        if (this.positionSellTriggered) return;
      }
      await this.sellIfNormalTinyRecipientSwappedAfterEntry(state, watch, tx);
      if (this.positionSellTriggered) return;
    }

    const isRelevantMintTx = this.isRelevantMintTx(tx, state.mint);
    if (!isRelevantMintTx) {
      this.pruneRecipientWithoutEarlyTokenBuy(state, watch);
      return;
    }
    const action = this.classifyTx(tx, watch.wallet, state.mint);
    if (action !== "buy" && action !== "sell") {
      this.pruneRecipientWithoutEarlyTokenBuy(state, watch);
      return;
    }
    const amount = this.extractTokenAmountForWallet(tx, watch.wallet, state.mint, action);
    if (action === "buy") {
      if (
        watch.normalTinyTransferMode &&
        !watch.firstBuySignature &&
        tx.timestamp > watch.fundingTimestamp
      ) {
        this.log.info("Normal tiny recipient buy ignored because it occurred after tiny funding", {
          mint: state.mint,
          wallet: watch.wallet,
          fundingSignature: watch.fundingSignature,
          fundingTimestamp: watch.fundingTimestamp,
          buySignature: tx.signature,
          buyTimestamp: tx.timestamp,
        });
        return;
      }
      watch.tokenBuyObserved = true;
      if (watch.tokenActions.some((existing) => existing.signature === tx.signature)) return;
      watch.tokenActions.push({ kind: action, signature: tx.signature, amount });
      if (!watch.firstBuySignature) {
        watch.firstBuySignature = tx.signature;
        watch.firstBuyTimestamp = tx.timestamp;
        watch.boughtAmount += amount;
        if (
          !state.lowFundingMode &&
          !watch.normalTinyTransferMode &&
          !this.profitExitDisabled
        ) {
          this.profitExitDisabled = true;
          this.log.warn(
            "Valid transfer-out recipient bought token; disabling MC profit target and using recipient sell-all/zero-SOL exits",
            {
              mint: state.mint,
              wallet: watch.wallet,
              signature: tx.signature,
            },
          );
        }
        this.log.warn(
          watch.normalTinyTransferMode
            ? "Normal tiny recipient bought token; MC target and later-swap exit armed"
            : state.lowFundingMode
            ? "Low-funding recipient bought token; MC profit target disabled"
            : "Valid transfer-out recipient bought token; sell-all/zero-SOL watch armed",
          {
          mint: state.mint,
          wallet: watch.wallet,
          signature: tx.signature,
          boughtAmount: watch.boughtAmount,
          normalTinyTransferMode: watch.normalTinyTransferMode,
          },
        );
        void this.sendTelegramSafe(
          [
            `<b>🟢 ${this.label} Recipient Bought Token</b>`,
            `Token: <code>${state.mint}</code>`,
            `Recipient: <code>${watch.wallet}</code>`,
            `Buy tx: <code>${tx.signature}</code>`,
            `Tracked amount: <b>${watch.boughtAmount.toLocaleString()}</b>`,
            "",
            watch.normalTinyTransferMode
              ? "Exit watch armed: configured % MC target remains active. Bot will also sell on rug, any later recipient swap/buy/sell activity, or recipient SOL balance reaching zero on a new post-funding tx notification."
              : state.lowFundingMode
              ? "Exit watch armed: MC profit target is disabled. Bot will sell on rug or clean post-entry 4-bundler tiny-transfer exit."
              : "Exit watch armed: MC profit target is disabled. Bot will sell on rug, recipient sell-all, or recipient SOL balance reaching zero.",
          ].join("\n"),
          "recipient first-buy notification",
        );
        if (watch.buyTriggersEntry && !this.buySubmitted) {
          if (state.lowFundingMode) {
            await this.emitLowFundingRecipientBuy(
              state,
              watch,
              tx.signature,
              "recipient bought this token within its first 3 post-funding txs",
              false,
              tx,
            );
          } else {
            await this.emitBundlerFunderBuy(
              state,
              watch,
              tx.signature,
              `recipient bought this token within its first ${BUNDLER_FUNDER_RECIPIENT_FIRST_TX_WINDOW} post-funding txs`,
              !watch.normalTinyTransferMode,
              tx,
            );
          }
        }
        if (this.hasReachedFunderRecipientBuyCap(state)) {
          await this.stopBundlerFunderSourceDiscovery(
            state,
            "two recipients bought the monitored token",
          );
        }
      } else {
        watch.boughtAmount += amount;
        const additionalBuyCountAfterFirst = Math.max(
          0,
          watch.tokenActions.filter((entry) => entry.kind === "buy").length - 1,
        );
        this.log.info("Valid transfer-out recipient added to tracked token position", {
          mint: state.mint,
          wallet: watch.wallet,
          signature: tx.signature,
          boughtAmount: watch.boughtAmount,
          firstBuySignature: watch.firstBuySignature,
          additionalBuyCountAfterFirst,
          additionalBuySellTriggerDisabled: true,
        });
      }
      if (!state.lowFundingMode && !watch.normalTinyTransferMode) {
        await this.sellIfRecipientSolBalanceIsZero(state, watch, tx);
      } else if (
        !state.lowFundingMode &&
        watch.normalTinyTransferMode &&
        source === "notification" &&
        tx.timestamp > watch.fundingTimestamp
      ) {
        await this.sellIfRecipientSolBalanceIsZero(state, watch, tx);
      }
      return;
    }

    this.pruneRecipientWithoutEarlyTokenBuy(state, watch);
    if (!watch.firstBuySignature) return;
    if (watch.tokenActions.some((existing) => existing.signature === tx.signature)) return;
    if (state.lowFundingMode && !watch.lowFundingCopySellOnSellAll) {
      this.log.info("Low-funding recipient sell observed; ignoring recipient sell for $2.50-$5 tiny band", {
        mint: state.mint,
        wallet: watch.wallet,
        signature: tx.signature,
        firstBuySignature: watch.firstBuySignature,
      });
      return;
    }
    watch.tokenActions.push({ kind: action, signature: tx.signature, amount });
    watch.soldAmount += amount;
    const remainingAmount = await this.getRecipientTokenBalanceAtTx(
      state,
      watch,
      tx,
    );
    const soldAllByTxBalance = remainingAmount !== null && remainingAmount <= 0;
    const soldAllByTrackedAmount =
      watch.boughtAmount > 0 && watch.soldAmount >= watch.boughtAmount;
    this.log.info(
      state.lowFundingMode && watch.lowFundingCopySellOnSellAll
        ? "Low-funding >$5 tiny recipient sell observed"
        : "Valid transfer-out recipient sell observed",
      {
      mint: state.mint,
      wallet: watch.wallet,
      signature: tx.signature,
      soldAmount: watch.soldAmount,
      boughtAmount: watch.boughtAmount,
      remainingAmount,
      soldAllByTxBalance,
      soldAllByTrackedAmount,
      lowFundingCopySellOnSellAll: watch.lowFundingCopySellOnSellAll,
      },
    );
    if ((soldAllByTxBalance || soldAllByTrackedAmount) && this.phase === "holding") {
      if (state.lowFundingMode && watch.lowFundingTinyUsdBand) {
        state.lowFundingTinySoldUsdBands.add(watch.lowFundingTinyUsdBand);
      }
      await this.triggerPositionSell(
        state.mint,
        state.lowFundingMode && watch.lowFundingCopySellOnSellAll
          ? `Low-funding >$5 tiny recipient ${watch.wallet} sold all token position`
          : `Shared feePayer recipient ${watch.wallet} sold all tracked token position`,
        [
          "<b>🚨 Shared-Funder Recipient Sold All</b>",
          `Token: <code>${state.mint}</code>`,
          `Recipient: <code>${watch.wallet}</code>`,
          `First buy: <code>${watch.firstBuySignature}</code>`,
          `Sell tx: <code>${tx.signature}</code>`,
          `Sold tracked: <b>${watch.soldAmount.toLocaleString()}</b> / <b>${watch.boughtAmount.toLocaleString()}</b>`,
          remainingAmount !== null
            ? `Post-tx token balance: <b>${remainingAmount.toLocaleString()}</b>`
            : "",
        ],
        tx.signature,
      );
      return;
    }
  }

  private pruneRecipientWithoutEarlyTokenBuy(
    state: BundlerFunderWatchState,
    watch: FunderRecipientWatch,
  ): void {
    if (watch.normalTinyTransferMode) return;
    if (watch.tokenBuyObserved) return;
    if (
      watch.observedTxSignatures.size <
      BUNDLER_FUNDER_RECIPIENT_FIRST_TX_WINDOW
    ) {
      return;
    }
    this.log.info(
      "Valid transfer-out recipient skipped: no token buy in first watched transactions",
      {
        mint: state.mint,
        wallet: watch.wallet,
        fundingSignature: watch.fundingSignature,
        observedTxCount: watch.observedTxSignatures.size,
        requiredWindow: BUNDLER_FUNDER_RECIPIENT_FIRST_TX_WINDOW,
      },
    );
    state.validOutSignatures.delete(watch.fundingSignature);
    void this.sendTelegramSafe(
      [
        `<b>⚪ ${this.label} Recipient Watch Skipped</b>`,
        `Token: <code>${state.mint}</code>`,
        `Recipient: <code>${watch.wallet}</code>`,
        `Funding tx: <code>${watch.fundingSignature}</code>`,
        "",
        `No token buy was found in the first ${BUNDLER_FUNDER_RECIPIENT_FIRST_TX_WINDOW} recipient txs. This wallet will not be used for recipient sell-all or zero-SOL exit tracking.`,
      ].join("\n"),
      "recipient first-3 no-buy notification",
    );
    this.removeFunderRecipientWatch(
      watch.wallet,
      `no token buy in first ${BUNDLER_FUNDER_RECIPIENT_FIRST_TX_WINDOW} recipient txs`,
    );
    void this.promoteQueuedBundlerFunderCandidates(
      state,
      "recipient failed first-3-tx token-buy gate",
    );
  }

  private async sellIfNormalTinyRecipientSwappedAfterEntry(
    state: BundlerFunderWatchState,
    watch: FunderRecipientWatch,
    tx: HeliusTransaction,
  ): Promise<void> {
    if (!watch.normalTinyTransferMode || !watch.firstBuySignature) return;
    if (tx.signature === watch.firstBuySignature) return;
    if (!this.isWalletSwapTx(tx, watch.wallet)) return;
    if (watch.postEntrySwapBaselineSignatures.has(tx.signature)) return;

    if (this.phase !== "holding") {
      watch.postEntrySwapBaselineSignatures.add(tx.signature);
      this.log.info("Normal tiny recipient swap observed before bot holding; baselined for later exit checks", {
        mint: state.mint,
        wallet: watch.wallet,
        firstBuySignature: watch.firstBuySignature,
        signature: tx.signature,
      });
      return;
    }

    watch.postEntrySwapBaselineSignatures.add(tx.signature);
    watch.postEntrySwapSignature = tx.signature;
    const qualifiedTinyRecipients = [...state.recipientWatches.values()].filter(
      (candidate) =>
        candidate.normalTinyTransferMode && Boolean(candidate.firstBuySignature),
    );
    const pendingTinyRecipients = qualifiedTinyRecipients.filter(
      (candidate) => !candidate.postEntrySwapSignature,
    );
    if (qualifiedTinyRecipients.length >= 2 && pendingTinyRecipients.length > 0) {
      this.log.warn("Normal tiny recipient post-entry swap observed; waiting for second tiny recipient confirmation", {
        mint: state.mint,
        wallet: watch.wallet,
        signature: tx.signature,
        qualifiedTinyRecipients: qualifiedTinyRecipients.map((candidate) => candidate.wallet),
        pendingTinyRecipients: pendingTinyRecipients.map((candidate) => candidate.wallet),
      });
      void this.sendTelegramSafe(
        [
          `<b>🟡 ${this.label} Normal Tiny Recipient Moved</b>`,
          `Token: <code>${state.mint}</code>`,
          `Recipient: <code>${watch.wallet}</code>`,
          `Trigger tx: <code>${tx.signature}</code>`,
          "",
          "Waiting for the other valid tiny recipient to also make a post-entry buy/sell/swap before selling.",
        ].join("\n"),
        "normal tiny first post-entry swap notification",
      );
      return;
    }

    await this.triggerPositionSell(
      state.mint,
      qualifiedTinyRecipients.length >= 2
        ? "Both normal-mode tiny recipients made a post-entry buy/sell/swap"
        : `Normal-mode tiny recipient ${watch.wallet} made a later swap after entry`,
      [
        "<b>🚨 Normal Tiny Recipient Swapped Again</b>",
        `Token: <code>${state.mint}</code>`,
        `Recipient: <code>${watch.wallet}</code>`,
        `Initial buy: <code>${watch.firstBuySignature}</code>`,
        `Trigger tx: <code>${tx.signature}</code>`,
        qualifiedTinyRecipients.length >= 2
          ? `Confirmed tiny recipients: <b>${qualifiedTinyRecipients
              .map((candidate) => candidate.wallet)
              .join(", ")}</b>`
          : "",
        "",
        qualifiedTinyRecipients.length >= 2
          ? "Both tiny recipients have now made a post-entry buy/sell/swap. Selling position."
          : "Only one valid tiny recipient is active and it made a post-entry buy/sell/swap. Selling position.",
      ],
      tx.signature,
    );
  }

  private isWalletSwapTx(tx: HeliusTransaction, wallet: string): boolean {
    const isSwap =
      tx.type === "SWAP" ||
      /\bswapped\b/i.test(tx.description ?? "");
    if (!isSwap) return false;
    if (tx.feePayer === wallet) return true;
    if (
      (tx.tokenTransfers ?? []).some(
        (transfer) =>
          transfer.fromUserAccount === wallet ||
          transfer.toUserAccount === wallet,
      )
    ) {
      return true;
    }
    return (tx.nativeTransfers ?? []).some(
      (transfer) =>
        transfer.fromUserAccount === wallet ||
        transfer.toUserAccount === wallet,
    );
  }

  private async sellIfRecipientSolBalanceIsZero(
    state: BundlerFunderWatchState,
    watch: FunderRecipientWatch,
    tx: HeliusTransaction,
  ): Promise<void> {
    if (!watch.firstBuySignature || this.phase !== "holding") return;
    if (watch.zeroSolBalanceSignatures.has(tx.signature)) return;
    if (!Number.isFinite(tx.timestamp) || tx.timestamp <= 0) return;
    try {
      const balance = await this.getConfirmedWalletBalanceAt(
        watch.wallet,
        NATIVE_SOL_BALANCE_MINT,
        tx.timestamp,
        watch.heliusPreferredIndex,
      );
      const balanceRaw = BigInt(balance.balanceRaw || "0");
      this.log.info("Checked shared feePayer recipient SOL balance at tx timestamp", {
        mint: state.mint,
        wallet: watch.wallet,
        signature: tx.signature,
        timestamp: tx.timestamp,
        balance: balance.balance,
        balanceRaw: balance.balanceRaw,
      });
      if (balanceRaw > 0n) return;
      watch.zeroSolBalanceSignatures.add(tx.signature);
      await this.triggerPositionSell(
        state.mint,
        `Shared feePayer recipient ${watch.wallet} SOL balance reached zero`,
        [
          "<b>🚨 Shared-Funder Recipient SOL Balance Zero</b>",
          `Token: <code>${state.mint}</code>`,
          `Recipient: <code>${watch.wallet}</code>`,
          `Trigger tx: <code>${tx.signature}</code>`,
          `Timestamp: <b>${tx.timestamp}</b>`,
          `SOL balance at tx: <b>${balance.balance}</b>`,
        ],
        tx.signature,
      );
    } catch (err) {
      void this.heliusClient.handlePossibleRateLimitError(err);
      this.log.warn("Failed to check shared feePayer recipient SOL balance-at", {
        mint: state.mint,
        wallet: watch.wallet,
        signature: tx.signature,
        timestamp: tx.timestamp,
        error: err instanceof Error ? err.message : String(err),
      });
    }
  }

  private async getRecipientTokenBalanceAtTx(
    state: BundlerFunderWatchState,
    watch: FunderRecipientWatch,
    tx: HeliusTransaction,
  ): Promise<number | null> {
    if (!Number.isFinite(tx.timestamp) || tx.timestamp <= 0) return null;
    try {
      const balance = await this.getConfirmedWalletBalanceAt(
        watch.wallet,
        state.mint,
        tx.timestamp,
        watch.heliusPreferredIndex,
      );
      const parsed = Number(balance.balance);
      this.log.info("Checked shared feePayer recipient token balance at tx timestamp", {
        mint: state.mint,
        wallet: watch.wallet,
        signature: tx.signature,
        timestamp: tx.timestamp,
        balance: balance.balance,
        balanceRaw: balance.balanceRaw,
      });
      return Number.isFinite(parsed) ? parsed : null;
    } catch (err) {
      void this.heliusClient.handlePossibleRateLimitError(err);
      this.log.warn("Failed to check shared feePayer recipient token balance-at", {
        mint: state.mint,
        wallet: watch.wallet,
        signature: tx.signature,
        timestamp: tx.timestamp,
        error: err instanceof Error ? err.message : String(err),
      });
      return null;
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
    this.disableProfitExitAfterBuy = false;
    this.insiderSellsReady = false;
    this.bundlerMatchesReady = false;
    this.authorityProbeFailedAtTwo = false;
    this.buySubmitted = false;
    this.isBuyGateEvaluating = false;
    this.profitExitDisabled = false;
    this.disableProfitExitAfterBuy = false;
    this.heliusPoolMetricsMint = null;
    this.heliusPoolMetricsStartedAt = 0;
    this.lastHeliusPoolMetricsAt = 0;
    this.resetTokenTxCounts();

    this.log.info("InsiderBot reset; resuming followed wallet monitoring");
    if (this.followedWallet && !this.followMonitor) {
      await this.followWallet(this.followedWallet);
    }
  }
}
