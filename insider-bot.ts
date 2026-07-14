import {
  Connection,
  LAMPORTS_PER_SOL,
  PublicKey,
} from "@solana/web3.js";
import { EventEmitter } from "events";
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
const LOW_FUNDING_DEV_BUY_SYNC_LIMIT = 10;
const REQUIRED_BUNDLER_MATCHES = 2;
const INSIDER_RUG_MARKET_CAP_USD = 5_000;
const MAX_FOLLOW_WALLET_START_MARKET_CAP_USD = 80_000;
const BUNDLER_FUNDER_TRANSFER_LIMIT = 5;
const BUNDLER_FUNDER_REQUIRED_COUNT = 4;
/** Of the BUNDLER_FUNDER_REQUIRED_COUNT (4) early bundler funding records, at least this many must share the exact same feePayer for the shared-feePayer watch to start. Relaxed from requiring all 4 to match, since a single outlier (e.g. one bundler additionally/separately funded from an unrelated wallet) shouldn't block an otherwise-clear shared-feePayer pattern. The majority feePayer's records are used for the watch; any non-matching outlier record is ignored (its bundler wallet is still tracked as an early buyer, just not as a funding source). */
const BUNDLER_FUNDER_MIN_MATCHING_FEEPAYER_COUNT = 3;
const BUNDLER_FUNDER_FUNDING_RECORD_ATTEMPTS = 3;
const BUNDLER_FUNDER_FUNDING_RECORD_RETRY_DELAY_MS = 500;
const BUNDLER_FUNDER_LOW_FUNDING_SOL = 20;
/** Kill switch for the whole low-funding-mode path. While false, any shared feePayer whose largest funding is below BUNDLER_FUNDER_LOW_FUNDING_SOL is skipped entirely (no watch is even created for it) instead of being handled via low-funding logic. */
const BUNDLER_FUNDER_LOW_FUNDING_MODE_ENABLED = false;
const BUNDLER_FUNDER_LOW_FUNDING_MAX_TRANSFER_OUT_TXS = 5;
const BUNDLER_FUNDER_LOW_FUNDING_EXIT_PERCENT = 50;
const BUNDLER_FUNDER_LOW_FUNDING_MIN_TRANSFER_OUT_SOL = 3.5;
const BUNDLER_FUNDER_LOW_FUNDING_LARGE_EXIT_PERCENT = 180;
const BUNDLER_FUNDER_LOW_FUNDING_TINY_EXIT_MC_USD = 25_000;
const BUNDLER_FUNDER_LOW_FUNDING_LARGE_SWAP_HISTORY_MAX_AGE_MS = 24 * 60 * 60 * 1_000;
const BUNDLER_FUNDER_LOW_FUNDING_TINY_GROUP_SECONDS = 10;
const BUNDLER_FUNDER_LOW_FUNDING_TINY_MIN_BUY_USD = 1;
const BUNDLER_FUNDER_LOW_FUNDING_TINY_COPYSELL_MIN_USD = 5;
const BUNDLER_FUNDER_NORMAL_TINY_MIN_BUY_USD = 1;
/** Floor for what counts as trackable "less than $1" dust (for the $1-$5 band's not-first-group check below) — the effective below-minimum band is $0.10-$0.99; anything under $0.10 is ignored entirely, not just below the buy minimum. */
const BUNDLER_FUNDER_NORMAL_TINY_DUST_FLOOR_USD = 0.1;
const BUNDLER_FUNDER_NORMAL_TINY_MID_MAX_USD = 5;
const BUNDLER_FUNDER_NORMAL_TINY_MID_EXIT_PERCENT = 90;
const BUNDLER_FUNDER_NORMAL_TINY_HIGH_EXIT_PERCENT = 180;
const BUNDLER_FUNDER_NORMAL_TINY_TRANSFER_OUT_MAX_USD = 10;
/** Normal-mode $1-$5 band buy gates are skipped if this long has passed since the shared feePayer was locked. */
const BUNDLER_FUNDER_NORMAL_TINY_MID_BAND_MAX_LOCK_AGE_MS = 30 * 60 * 1_000;
/** Bundler recipient-funding transfers in the buy-triggering $1-$5/>$5-$10 bands are only trusted when their SOL amount is approximately one of the "round" funding sizes valid for that specific band — real gas-funding rounds land on one of these, while incidental/coincidental transfers landing in the same USD band by chance (e.g. ~0.03 SOL) don't and are filtered out. The $1-$5 band ("2_5_to_5") maps to 0.02/0.05 SOL; the >$5-$10 band ("gt5") maps to 0.1 SOL only (0.02/0.05 SOL wouldn't realistically reach >$5 USD anyway, so restricting each band to its own realistic size(s) avoids any ambiguity). Deliberately not applied to the $0.10-$0.99 dust band ("lt2_5", empty here), which uses its own (much smaller, non-round) transfer sizes. */
const BUNDLER_FUNDER_NORMAL_TINY_ROUND_SOL_AMOUNTS_BY_BAND: Record<
  "lt2_5" | "2_5_to_5" | "gt5",
  number[]
> = {
  lt2_5: [],
  "2_5_to_5": [0.02, 0.05],
  gt5: [0.1],
};
/** Tolerance (in SOL) for matching a transfer-out amount against one of the per-band round SOL targets above — kept deliberately slim so it only absorbs fee/rounding/slippage noise, not genuinely different amounts. Ranges stay non-overlapping between 0.02/0.05/0.1 at this tolerance. */
const BUNDLER_FUNDER_NORMAL_TINY_ROUND_SOL_TOLERANCE_SOL = 0.004;
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
  deferProfitExitUntilDevSwap(currentMc: number): Promise<boolean>;
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
  lowFundingLargeTransferMode: boolean;
  postEntrySwapSignature: string | null;
  postEntrySwapBaselineSignatures: Set<string>;
  soldAllSignature: string | null;
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
  normalTinyTransferOuts: Array<{ signature: string; timestamp: number; recipient: string; amountSol: number; amountUsd: number }>;
  /** Sticky flag: set once a same-band group (≥2 recipients within 10s) of $0.10-$0.99 dust transfer-outs has been observed for this token. Once set, the next $1-$5/>$5-$10 band group is routed by sub-band instead of bought unconditionally — see inspectBundlerFunderTransaction. */
  normalTinyDustGroupSeen: boolean;
  lowFundingFunderTxs: Array<{ signature: string; timestamp: number }>;
  lowFundingTinyTransferOuts: Array<{ signature: string; timestamp: number; recipient: string; amountSol: number; amountUsd: number }>;
  lowFundingTinyBundlerGateSeen: boolean;
  lowFundingTinyEntryTimestamp: number | null;
  lowFundingTinyCandidateWallets: Set<string>;
  lowFundingTinySellGroupSignatures: Set<string>;
  lowFundingTinyBoughtUsdBands: Set<"2_5_to_5" | "gt5">;
  lowFundingTinySoldUsdBands: Set<"2_5_to_5" | "gt5">;
  lowFundingPendingTinyBuyWallets: Set<string>;
  lowFundingDevBuySignatures: Set<string>;
  lowFundingDevBuyAfterCreateSignature: string | null;
  lowFundingDevBuyAfterCreateTimestamp: number | null;
  lowFundingTinyMcExitPending: boolean;
  lowFundingTinyMcExitReachedMc: number | null;
  lowFundingTinyDevExitSwapSignature: string | null;
  lowFundingTinyDevExitBaselineSignature: string | null;
  lowFundingTinyDevExitBaselineTimestamp: number | null;
  lowFundingLargeTransferBuyUsed: boolean;
  discoveryStopped: boolean;
  /** Wall-clock time (ms) the shared feePayer was locked (i.e. when the "Shared FeePayer Locked" notification fired). Used to time out stale normal-mode $1-$5 band buy gates. */
  lockedAt: number;
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
  private readonly claimMint: InsiderMintClaimFn | null;
  private readonly releaseMint: InsiderMintReleaseFn | null;
  private readonly rpcUrl: string;
  private readonly wsUrl: string;
  private readonly label: string;

  private followedWallet: string | null = null;
  private buySol: number;
  private normalFundingBuySol: number;
  private lowFundingBuySol: number;
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
  private devCreateSignature: string | null = null;
  private devCreateTimestamp: number | null = null;
  /** Highest market cap observed for the current token across all pre-buy MC fetches — used to skip normal-mode buys that would already be past their own exit target. */
  private highestObservedMarketCapUsd: number | null = null;
  /** Market cap captured at the very start of the current token's flow (the follow-wallet/initial-bundler buy MC). A buy is only allowed if the market cap at buy time is still at or above this — i.e. the token hasn't round-tripped back below where the earliest bundler bought. Null if unknown (fetch failed at flow start), in which case the check is skipped entirely. */
  private initialBundlerMarketCapUsd: number | null = null;
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
  private bundlerFunderWatch: BundlerFunderWatchState | null = null;
  private bundlerFunderLogsSubId: number | null = null;
  private lowFundingDevLogsSubId: number | null = null;
  private recipientLogsSubIds = new Map<string, number>();
  private recipientSolBalanceSubIds = new Map<string, number>();
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
  private stoppedForHeliusCredits = false;
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
  private cachedSolPriceUsd: number | null = null;
  private cachedSolPriceAt = 0;

  constructor(
    config: ServiceConfig,
    rpcUrl: string,
    wsUrl: string,
    gmgnClient: GmgnClient,
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
    this.normalFundingBuySol = config.insiderNormalBuySol;
    this.lowFundingBuySol = config.insiderLowFundingBuySol;
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
    this.phase = "holding";
    this.startPollLoop();
    void this.syncBundlerFunderTransactions();
    void this.syncFunderRecipientBatch();
    this.log.warn(
      "Sell failed; active position retained and shared feePayer monitoring rearmed",
      {
        mint,
        funderWallet: this.bundlerFunderWatch?.funderWallet ?? null,
      },
    );
  }

  clearPreBuyMint(): void {
    void this.resetForNewToken(true);
  }

  private assertBuySol(value: number, label = "Insider buy SOL"): void {
    if (!Number.isFinite(value) || value <= 0) {
      throw new Error(`${label} must be greater than 0`);
    }
  }

  setBuySol(value: number): void {
    this.assertBuySol(value);
    this.buySol = value;
  }

  getBuySol() {
    return this.buySol;
  }

  setNormalFundingBuySol(value: number): void {
    this.assertBuySol(value, "Insider normal-funding buy SOL");
    this.normalFundingBuySol = value;
  }

  getNormalFundingBuySol() {
    return this.normalFundingBuySol;
  }

  setLowFundingBuySol(value: number): void {
    this.assertBuySol(value, "Insider low-funding buy SOL");
    this.lowFundingBuySol = value;
  }

  getLowFundingBuySol() {
    return this.lowFundingBuySol;
  }

  private getBuySolForFundingMode(lowFundingMode: boolean): number {
    return lowFundingMode ? this.lowFundingBuySol : this.normalFundingBuySol;
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

  async deferProfitExitUntilDevSwap(currentMc: number): Promise<boolean> {
    const state = this.bundlerFunderWatch;
    if (!state?.lowFundingMode) return false;
    if (!this.activePosition || this.activePosition.mint !== state.mint) return false;
    const baselineSignature = state.lowFundingTinyDevExitBaselineSignature ?? state.lowFundingDevBuyAfterCreateSignature;
    if (!baselineSignature) return false;
    if (state.lowFundingTinyDevExitSwapSignature) return false;

    this.subscribeLowFundingDevWallet(state);
    const devSwap = await this.findLowFundingTinyDevSwapAfterEntry(state);
    if (devSwap) {
      state.lowFundingTinyDevExitSwapSignature = devSwap.signature;
      this.log.warn("Low-funding tiny MC exit can proceed; dev buy after entry already seen", {
        mint: state.mint,
        devWallet: this.devWallet,
        devExitBaselineSignature: baselineSignature,
        devExitSwapSignature: devSwap.signature,
        currentMc,
      });
      return false;
    }

    state.lowFundingTinyMcExitPending = true;
    state.lowFundingTinyMcExitReachedMc = currentMc;
    this.log.warn("Low-funding tiny MC exit reached; waiting for dev buy before selling", {
      mint: state.mint,
      devWallet: this.devWallet,
      devExitBaselineSignature: baselineSignature,
      currentMc,
      exitMc: this.exitMc,
    });
    void this.sendTelegramSafe(
      [
        `<b>⏳ ${this.label} Low-Funding Tiny MC Exit Pending</b>`,
        `Token: <code>${state.mint}</code>`,
        `Current MC: <b>$${currentMc.toLocaleString()}</b>`,
        `Exit MC: <b>$${this.exitMc.toLocaleString()}</b>`,
        `Dev: <code>${this.devWallet ?? "unknown"}</code>`,
        `Dev exit baseline: <code>${baselineSignature}</code>`,
        "",
        "MC target reached. Waiting for the next dev buy before selling.",
      ].join("\n"),
      "low-funding tiny mc exit pending notification",
    );
    return true;
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
      this.pollTimer !== null
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
    this.activePosition = {
      followedWallet: trigger.followedWallet,
      mint: trigger.mint,
    };
    this.watchingMint = null;
    this.monitoredWallet = null;
    this.insiderState = null;
    this.boughtMints.add(trigger.mint);
    this.phase = "holding";
    this.profitExitDisabled = this.disableProfitExitAfterBuy;
    this.disableProfitExitAfterBuy = false;

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
      // Record this as the token's "initial bundler MC" — the buy gate later
      // requires the market cap at actual buy time to still be at or above
      // this, so the bot never buys into a token that's already dropped back
      // below where the earliest bundler bought.
      this.initialBundlerMarketCapUsd = followWalletBuyMc;

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
    this.highestObservedMarketCapUsd = null;
    this.clearBundlerAccumulation();

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
    this.devCreateSignature = createTx?.signature ?? null;
    this.devCreateTimestamp = createTx?.timestamp ?? null;
    if (this.devWallet) {
      this.log.info("Dev wallet identified for trader-scan exclusions", {
        mint,
        devWallet: this.devWallet,
        devCreateSignature: this.devCreateSignature,
        devCreateTimestamp: this.devCreateTimestamp,
      });
    }
    this.preBuyStopped = false;
    this.positionSellTriggered = false;
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

    const allRecords = fundingRecords as BundlerFundingRecord[];
    const feePayerGroups = new Map<string, BundlerFundingRecord[]>();
    for (const record of allRecords) {
      const group = feePayerGroups.get(record.fundingFeePayer);
      if (group) {
        group.push(record);
      } else {
        feePayerGroups.set(record.fundingFeePayer, [record]);
      }
    }
    const majorityGroup = [...feePayerGroups.values()].reduce((best, group) =>
      group.length > best.length ? group : best,
    );
    if (majorityGroup.length < BUNDLER_FUNDER_MIN_MATCHING_FEEPAYER_COUNT) {
      this.log.warn(
        "Not enough bundler funding tx feePayers matched; resetting",
        {
          mint,
          fundingRecords: allRecords,
          matchingFeePayerCount: majorityGroup.length,
          requiredMatchingFeePayerCount: BUNDLER_FUNDER_MIN_MATCHING_FEEPAYER_COUNT,
          totalCount: allRecords.length,
        },
      );
      await this.resetForNewToken(true);
      return;
    }
    const records = majorityGroup;
    if (records.length < allRecords.length) {
      const outliers = allRecords.filter((record) => !records.includes(record));
      this.log.warn(
        "Majority of bundler funding tx feePayers matched; proceeding with the majority feePayer and ignoring the outlier(s)",
        {
          mint,
          majorityFeePayer: records[0].fundingFeePayer,
          matchingFeePayerCount: records.length,
          totalCount: allRecords.length,
          outliers,
        },
      );
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
    const funderWallet = records[0].fundingFeePayer;
    if (lowFundingMode && !BUNDLER_FUNDER_LOW_FUNDING_MODE_ENABLED) {
      this.log.warn(
        "Low-funding mode is disabled; skipping token because its shared feePayer's largest funding is below the normal-mode threshold",
        {
          mint,
          funderWallet,
          largestFundingSol,
          normalFundingThresholdSol: BUNDLER_FUNDER_LOW_FUNDING_SOL,
        },
      );
      void this.sendTelegramSafe(
        [
          `<b>⏭️ ${this.label} Low-Funding Mode Disabled — Token Skipped</b>`,
          `Token: <code>${mint}</code>`,
          `FeePayer: <code>${funderWallet}</code>`,
          `Largest bundler funding: <b>${largestFundingSol.toFixed(4)} SOL</b> (below the ${BUNDLER_FUNDER_LOW_FUNDING_SOL} SOL normal-mode threshold)`,
          "Low-funding mode is currently disabled — resetting to watch for the next token.",
        ].join("\n"),
        "low-funding mode disabled skip notification",
      );
      await this.resetForNewToken(true);
      return;
    }
    const latestBundlerBuyTimestamp = Math.max(
      ...firstFour.map((buy) => buy.timestamp),
    );
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
        ? 0
        : largestFundingSol,
      cursorSignature: latest.fundingSignature,
      processedSignatures: new Set(records.map((record) => record.fundingSignature)),
      validOutSignatures: new Set<string>(),
      invalidOutSignatures: new Set<string>(),
      bundlerWallets: new Set(firstFour.map((buy) => buy.wallet)),
      recipientWatches: new Map<string, FunderRecipientWatch>(),
      queuedTransferOuts: [],
      normalTinyTransferOuts: [],
      normalTinyDustGroupSeen: false,
      lowFundingFunderTxs: [],
      lowFundingTinyTransferOuts: [],
      lowFundingTinyBundlerGateSeen: false,
      lowFundingTinyEntryTimestamp: null,
      lowFundingTinyCandidateWallets: new Set<string>(),
      lowFundingTinySellGroupSignatures: new Set<string>(),
      lowFundingTinyBoughtUsdBands: new Set<"2_5_to_5" | "gt5">(),
      lowFundingTinySoldUsdBands: new Set<"2_5_to_5" | "gt5">(),
      lowFundingPendingTinyBuyWallets: new Set<string>(),
      lowFundingDevBuySignatures: new Set<string>(),
      lowFundingDevBuyAfterCreateSignature: null,
      lowFundingDevBuyAfterCreateTimestamp: null,
      lowFundingTinyMcExitPending: false,
      lowFundingTinyMcExitReachedMc: null,
      lowFundingTinyDevExitSwapSignature: null,
      lowFundingTinyDevExitBaselineSignature: null,
      lowFundingTinyDevExitBaselineTimestamp: null,
      lowFundingLargeTransferBuyUsed: false,
      discoveryStopped: false,
      lockedAt: Date.now(),
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
        activeFunderWatch.lowFundingMode
          ? `Watching feePayer tiny transfer-outs: <b>$${BUNDLER_FUNDER_LOW_FUNDING_TINY_MIN_BUY_USD.toFixed(2)}-$${BUNDLER_FUNDER_NORMAL_TINY_TRANSFER_OUT_MAX_USD.toFixed(0)}</b>`
          : `Watching feePayer transfer-outs: <b>${activeFunderWatch.minTransferOutSol.toFixed(4)} SOL+</b>`,
        "",
        activeFunderWatch.lowFundingMode
          ? "Low-funding mode uses tiny same-band groups only."
          : "Transfer-outs that pass the filters are watched until the recipient buys this token.",
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
      lowFundingTinyTransferUsdBand: `$${BUNDLER_FUNDER_LOW_FUNDING_TINY_MIN_BUY_USD.toFixed(2)}-$${BUNDLER_FUNDER_NORMAL_TINY_TRANSFER_OUT_MAX_USD.toFixed(0)}`,
      syncStartSignature: syncStart.signature,
      syncStartTimestamp: syncStart.timestamp,
      syncStartBundlerWallet: syncStart.bundlerWallet,
      latestBundlerBuyTimestamp,
      txCount: windowTxs.length,
      minWindowTxsForImmediateBuy: 1,
      maxWindowTxsForImmediateBuy:
        BUNDLER_FUNDER_LOW_FUNDING_MAX_TRANSFER_OUT_TXS,
      legacyLargeTransferOutTxCount: 0,
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
          : sharedFeePayerBalanceBelowLowFundingThreshold
          ? "skip immediate low-funding buy because window tx count is not between 1 and 5"
          : "waiting for low-funding tiny transfer grouping",
      legacyLargeTransferOuts: [],
      skippedBundlerRecipients: allTransferOuts
        .filter((entry) => state.bundlerWallets.has(entry.transferOut.to))
        .map((entry) => ({
          signature: entry.tx.signature,
          timestamp: entry.tx.timestamp,
          recipient: entry.transferOut.to,
          amountSol: entry.transferOut.amountSol,
        })),
    });

    this.log.info("Low-funding large-transfer buy path disabled; using tiny transfer grouping flow", {
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
        }
      }

      if (this.phase === "holding") {
        await this.syncBundlerFunderTransactions();
        await this.syncFunderRecipientBatch();
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

  private subscribeLowFundingDevWallet(state: BundlerFunderWatchState): void {
    if (!state.lowFundingMode || !this.devWallet) return;
    if (this.lowFundingDevLogsSubId !== null) return;
    this.lowFundingDevLogsSubId = this.connection.onLogs(
      new PublicKey(this.devWallet),
      (logInfo) => {
        if (!logInfo.err) {
          void this.maybeTriggerLowFundingPendingTinyBuys(
            state,
            `dev wallet websocket notification ${logInfo.signature}`,
          );
        }
      },
      "processed",
    );
    this.log.info("Subscribed to dev wallet for low-funding tiny buy gate", {
      mint: state.mint,
      devWallet: this.devWallet,
      devCreateSignature: this.devCreateSignature,
      devCreateTimestamp: this.devCreateTimestamp,
    });
  }

  private async handleLowFundingDevWalletNotification(
    state: BundlerFunderWatchState,
    source: string,
  ): Promise<void> {
    const devExitBaselineSignature = state.lowFundingTinyDevExitBaselineSignature ?? state.lowFundingDevBuyAfterCreateSignature;
    if (
      this.activePosition?.mint === state.mint &&
      devExitBaselineSignature
    ) {
      const devSwap = await this.findLowFundingTinyDevSwapAfterEntry(state);
      if (devSwap && !state.lowFundingTinyDevExitSwapSignature) {
        state.lowFundingTinyDevExitSwapSignature = devSwap.signature;
        this.log.warn("Low-funding tiny dev buy after entry observed", {
          mint: state.mint,
          devWallet: this.devWallet,
          devExitBaselineSignature,
          devExitSwapSignature: devSwap.signature,
          mcExitPending: state.lowFundingTinyMcExitPending,
          source,
        });
        if (state.lowFundingTinyMcExitPending) {
          await this.stopLowFundingDevWalletSubscription("low-funding tiny dev buy completed pending MC exit");
          await this.triggerPositionSell(
            state.mint,
            `Low-funding tiny MC exit reached and dev bought after entry`,
            [
              `<b>🚨 ${this.label} Low-Funding Tiny Dev Buy Exit</b>`,
              `Token: <code>${state.mint}</code>`,
              `Dev: <code>${this.devWallet ?? "unknown"}</code>`,
              `Dev exit baseline: <code>${state.lowFundingTinyDevExitBaselineSignature ?? state.lowFundingDevBuyAfterCreateSignature}</code>`,
              `Exit dev buy: <code>${devSwap.signature}</code>`,
              state.lowFundingTinyMcExitReachedMc !== null
                ? `MC when target was reached: <b>$${state.lowFundingTinyMcExitReachedMc.toLocaleString()}</b>`
                : "",
              `Exit MC: <b>$${this.exitMc.toLocaleString()}</b>`,
            ],
            devSwap.signature,
          );
        }
      }
      return;
    }

    await this.maybeTriggerLowFundingPendingTinyBuys(state, source);
  }

  private async findLowFundingTinyDevSwapAfterEntry(
    state: BundlerFunderWatchState,
  ): Promise<HeliusTransaction | null> {
    const baselineSignature = state.lowFundingTinyDevExitBaselineSignature ?? state.lowFundingDevBuyAfterCreateSignature;
    const baselineTimestamp = state.lowFundingTinyDevExitBaselineTimestamp ?? state.lowFundingDevBuyAfterCreateTimestamp;
    if (!this.devWallet || !baselineSignature) return null;
    const txs = await this.withHeliusFallback(
      (client) => client.getWalletTransactionsDesc(this.devWallet!, LOW_FUNDING_DEV_BUY_SYNC_LIMIT),
      HELIUS_POOL_MC_RESERVED_INDEX,
    );
    const sorted = txs
      .filter((tx) => this.isRelevantMintTx(tx, state.mint))
      .filter((tx) => tx.signature !== baselineSignature)
      .filter((tx) =>
        baselineTimestamp === null ||
        tx.timestamp > baselineTimestamp,
      )
      .filter((tx) => {
        const action = this.classifyTx(tx, this.devWallet!, state.mint);
        return action === "buy";
      })
      .sort((a, b) => a.timestamp - b.timestamp || a.slot - b.slot);
    return sorted[0] ?? null;
  }

  private async stopLowFundingDevWalletSubscription(reason: string): Promise<void> {
    if (this.lowFundingDevLogsSubId === null) return;
    const subId = this.lowFundingDevLogsSubId;
    this.lowFundingDevLogsSubId = null;
    await this.connection.removeOnLogsListener(subId).catch(() => undefined);
    this.log.info("Stopped low-funding dev wallet subscription", {
      devWallet: this.devWallet,
      reason,
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
    if (!this.recipientSolBalanceSubIds.has(wallet)) {
      const balanceSubId = this.connection.onAccountChange(
        new PublicKey(wallet),
        (accountInfo) => {
          void this.handleFunderRecipientSolAccountChange(
            wallet,
            BigInt(accountInfo.lamports),
          );
        },
        "processed",
      );
      this.recipientSolBalanceSubIds.set(wallet, balanceSubId);
    }
    this.log.info("Subscribed to valid funder transfer-out recipient", {
      wallet,
      solBalanceSubscription: this.recipientSolBalanceSubIds.has(wallet),
    });
  }
  private isNormalTinyWalletExitDisabled(
    state: BundlerFunderWatchState,
    watch: FunderRecipientWatch,
  ): boolean {
    return (
      !state.lowFundingMode &&
      watch.normalTinyTransferMode
    );
  }
  private async handleFunderRecipientSolAccountChange(
    wallet: string,
    lamports: bigint,
  ): Promise<void> {
    const state = this.bundlerFunderWatch;
    const watch = state?.recipientWatches.get(wallet);
    if (!state || !watch) return;
    if (!watch.firstBuySignature || this.phase !== "holding") return;
    if (this.isNormalTinyWalletExitDisabled(state, watch)) return;
    if (this.positionSellTriggered || lamports > 0n) return;
    const marker = `account-subscribe-sol-zero:${wallet}`;
    if (watch.zeroSolBalanceSignatures.has(marker)) return;
    watch.zeroSolBalanceSignatures.add(marker);
    watch.soldAllSignature = marker;
    const mode = state.lowFundingMode
      ? watch.lowFundingLargeTransferMode
        ? "Low-funding large"
        : "Low-funding tiny"
      : watch.normalTinyTransferMode
      ? "Normal tiny"
      : "Normal";
    this.log.warn("Recipient SOL account subscription reached zero; selling position", {
      mint: state.mint,
      wallet,
      mode,
      lamports: lamports.toString(),
      fundingSignature: watch.fundingSignature,
      firstBuySignature: watch.firstBuySignature,
    });
    await this.triggerPositionSell(
      state.mint,
      `${mode} recipient ${wallet} SOL balance reached zero by account subscription`,
      [
        "<b>🚨 Shared-Funder Recipient SOL Balance Zero</b>",
        `Token: <code>${state.mint}</code>`,
        `Recipient: <code>${wallet}</code>`,
        `Mode: <b>${mode}</b>`,
        `Funding tx: <code>${watch.fundingSignature}</code>`,
        `First buy: <code>${watch.firstBuySignature}</code>`,
        "Source: <b>live account subscription</b>",
        "SOL balance: <b>0</b>",
      ],
      marker,
    );
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
    const balanceSubId = this.recipientSolBalanceSubIds.get(wallet);
    if (balanceSubId !== undefined) {
      this.recipientSolBalanceSubIds.delete(wallet);
      void this.connection.removeAccountChangeListener(balanceSubId).catch(() => undefined);
    }
    this.log.info("Stopped watching shared feePayer recipient", {
      mint: state?.mint,
      wallet,
      reason,
    });
  }

  private async stopBundlerFunderMonitoring(): Promise<void> {
    await this.stopLowFundingDevWalletSubscription("bundler funder monitoring stopped");
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
    for (const [wallet, subId] of this.recipientSolBalanceSubIds) {
      await this.connection.removeAccountChangeListener(subId).catch(() => undefined);
      this.recipientSolBalanceSubIds.delete(wallet);
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
    if (state.discoveryStopped) return;
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
        if (this.hasReachedFunderRecipientBuyCap(state)) break;
        if (state.processedSignatures.has(tx.signature)) continue;
        state.processedSignatures.add(tx.signature);
        state.cursorSignature = tx.signature;
        const migrated = await this.inspectBundlerFunderTransaction(state, tx);
        if (migrated) break;
      }
      await this.maybeTriggerLowFundingPendingTinyBuys(state, "shared feePayer sync");
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
    if (state.discoveryStopped) return;
    state.discoveryStopped = true;
    await this.stopLowFundingDevWalletSubscription("bundler funder monitoring stopped");
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
    if (state.lowFundingMode) {
      if (this.hasSolIncomingToWallet(tx, state.funderWallet)) return false;
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
    if (transferOutUsd < BUNDLER_FUNDER_NORMAL_TINY_DUST_FLOOR_USD) {
      this.log.debug("Skipping normal-mode feePayer transfer-out below the dust floor (too tiny to track at all)", {
        mint: state.mint,
        funderWallet: state.funderWallet,
        signature: tx.signature,
        recipient: transferOut.to,
        amountSol: transferOut.amountSol,
        amountUsd: transferOutUsd,
        dustFloorUsd: BUNDLER_FUNDER_NORMAL_TINY_DUST_FLOOR_USD,
      });
      return false;
    }
    const tinyUsdBand = this.getTinyUsdBand(transferOutUsd);
    // Record every qualifying feePayer transfer-out — including ones below
    // the minimum buy USD ("lt2_5" band, i.e. $0.10-$0.99 dust) — purely so
    // we can tell whether a later $1-$5 band group is genuinely the *first*
    // tiny transfer-out activity seen for this token (see the "not the first
    // group" guard below), rather than a group that just happens to follow
    // some earlier, smaller transfer-out that was itself skipped as too tiny
    // to buy on.
    this.recordNormalTinyTransferOut(state, {
      signature: tx.signature,
      timestamp: tx.timestamp,
      recipient: transferOut.to,
      amountSol: transferOut.amountSol,
      amountUsd: transferOutUsd,
    });

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

    if (tinyUsdBand === "lt2_5") {
      // A single dust transfer-out doesn't mean anything on its own — only a
      // same-band *group* of them (≥2 recipients within the same 10s window,
      // same as $1-$5/>$5-$10 grouping) counts as a genuine dust round. Once
      // that's seen, stick a flag on the state so the next $1-$5/>$5-$10
      // group gets routed by sub-band below instead of just checking raw
      // timestamps (which is unreliable at Solana's 1-second timestamp
      // resolution when dust and a real group land in the same second).
      if (!state.normalTinyDustGroupSeen) {
        const dustGroup = this.getNormalTinySameBandGroup(state, tx.timestamp, "lt2_5");
        if (dustGroup.length >= 2) {
          state.normalTinyDustGroupSeen = true;
          this.log.warn(
            "Normal-mode $0.10-$0.99 dust group observed; will route the next $1-$5/>$5-$10 group by sub-band instead of buying it unconditionally",
            {
              mint: state.mint,
              funderWallet: state.funderWallet,
              signature: tx.signature,
              dustGroupCount: dustGroup.length,
              dustGroupRecipients: dustGroup.map((entry) => entry.recipient),
              groupWindowSeconds: BUNDLER_FUNDER_LOW_FUNDING_TINY_GROUP_SECONDS,
            },
          );
          void this.sendTelegramSafe(
            [
              `<b>🟡 ${this.label} Normal Dust Group Observed</b>`,
              `Token: <code>${state.mint}</code>`,
              `FeePayer: <code>${state.funderWallet}</code>`,
              `Dust transfer-outs: <b>${dustGroup.length}</b> (each $0.10-$0.99), same 10s window`,
              "",
              "Watching for what comes next: $1.00-$2.50 skips the token, >$2.50-$5.00 buys with +90% MC, >$5.00-$10.00 buys with +180% MC.",
            ].join("\n"),
            "normal tiny dust group observed notification",
          );
        }
      }
      return false;
    }
    const sameBandGroup = this.getNormalTinySameBandGroup(
      state,
      tx.timestamp,
      tinyUsdBand,
    );
    if (sameBandGroup.length < 2) {
      this.log.info("Normal tiny transfer waiting for same-band 10s group", {
        mint: state.mint,
        funderWallet: state.funderWallet,
        signature: tx.signature,
        recipient: transferOut.to,
        amountSol: transferOut.amountSol,
        amountUsd: transferOutUsd,
        tinyUsdBand,
        groupWindowSeconds: BUNDLER_FUNDER_LOW_FUNDING_TINY_GROUP_SECONDS,
        isRoundBundlerSolAmount: this.isRoundBundlerTinySolAmount(transferOut.amountSol, tinyUsdBand),
        roundBundlerSolAmountsForBand: BUNDLER_FUNDER_NORMAL_TINY_ROUND_SOL_AMOUNTS_BY_BAND[tinyUsdBand],
      });
      return false;
    }

    if (tinyUsdBand === "2_5_to_5" && state.normalTinyDustGroupSeen) {
      // A dust group was already seen for this token, so this is "what comes
      // next" and gets routed by the group's *round SOL size* instead of
      // bought unconditionally: a ~0.02 SOL group disqualifies the token
      // outright, while a ~0.05 SOL (or larger round) group is trusted and
      // proceeds to the normal +90% MC buy/exit flow below. (Every member
      // already matched one of the round bundler sizes back in
      // getNormalTinySameBandGroup, so this just reads which one.)
      const groupMaxSol = Math.max(...sameBandGroup.map((entry) => entry.amountSol));
      if (this.isNearBundlerTinySolAmount(groupMaxSol, 0.02)) {
        this.log.warn(
          "Skipping normal-mode ~0.02 SOL sub-band buy gate because a dust group was already seen for this token",
          {
            mint: state.mint,
            funderWallet: state.funderWallet,
            signature: tx.signature,
            tinyUsdBand,
            groupMaxSol,
          },
        );
        void this.sendTelegramSafe(
          [
            `<b>⏭️ ${this.label} Normal ~0.02 SOL Sub-Band Buy Skipped — Preceded By Dust Group</b>`,
            `Token: <code>${state.mint}</code>`,
            `FeePayer: <code>${state.funderWallet}</code>`,
            `A $0.10-$0.99 dust group was already seen for this token, and the group that followed it is only ~${groupMaxSol.toFixed(4)} SOL (≈0.02 SOL), so it isn't trusted.`,
            "Skipping this token — resetting to watch for the next one.",
          ].join("\n"),
          "normal tiny ~0.02 sol sub-band dust-group-preceded skip notification",
        );
        await this.resetForNewToken(true);
        // Signal the caller to stop processing this tx batch — `state` is
        // now detached from `this.bundlerFunderWatch` (reset/cleared
        // above), so continuing to inspect further txs against it would be
        // stale work.
        return true;
      }

      this.log.warn(
        "Normal-mode ~0.05 SOL (or larger) sub-band buy gate accepted despite an earlier dust group",
        {
          mint: state.mint,
          funderWallet: state.funderWallet,
          signature: tx.signature,
          tinyUsdBand,
          groupMaxSol,
        },
      );
      // Deliberately not returning here — this group is ~0.05 SOL or larger
      // (not the disqualified ~0.02 SOL size), so despite the earlier dust
      // group it's still trusted and proceeds through the normal buy-gate
      // flow below.
    }

    if (tinyUsdBand === "gt5" && state.normalTinyDustGroupSeen) {
      this.log.info(
        "Normal-mode >$5-$10 sub-band buy gate accepted despite an earlier dust group (this band always buys, with +180% MC exit)",
        {
          mint: state.mint,
          funderWallet: state.funderWallet,
          signature: tx.signature,
          tinyUsdBand,
        },
      );
    }

    if (tinyUsdBand === "2_5_to_5") {
      const lockAgeMs = Date.now() - state.lockedAt;
      if (lockAgeMs >= BUNDLER_FUNDER_NORMAL_TINY_MID_BAND_MAX_LOCK_AGE_MS) {
        this.log.warn(
          "Skipping normal-mode $1-$5 band buy gate because too much time has passed since the shared feePayer was locked",
          {
            mint: state.mint,
            funderWallet: state.funderWallet,
            signature: tx.signature,
            tinyUsdBand,
            lockAgeMs,
            maxLockAgeMs: BUNDLER_FUNDER_NORMAL_TINY_MID_BAND_MAX_LOCK_AGE_MS,
          },
        );
        void this.sendTelegramSafe(
          [
            `<b>⏭️ ${this.label} Normal $1-$5 Band Buy Skipped — Too Stale</b>`,
            `Token: <code>${state.mint}</code>`,
            `FeePayer: <code>${state.funderWallet}</code>`,
            `Time since Shared FeePayer Locked: <b>${Math.round(lockAgeMs / 60_000)} min</b> (limit ${Math.round(BUNDLER_FUNDER_NORMAL_TINY_MID_BAND_MAX_LOCK_AGE_MS / 60_000)} min)`,
            "Skipping this token — resetting to watch for the next one.",
          ].join("\n"),
          "normal tiny $1-5 band stale skip notification",
        );
        await this.resetForNewToken(true);
        // Signal the caller to stop processing this tx batch — `state` is now
        // detached from `this.bundlerFunderWatch` (reset/cleared above), so
        // continuing to inspect further txs against it would be stale work.
        return true;
      }
    }

    const exitPercent = tinyUsdBand === "2_5_to_5"
      ? BUNDLER_FUNDER_NORMAL_TINY_MID_EXIT_PERCENT
      : BUNDLER_FUNDER_NORMAL_TINY_HIGH_EXIT_PERCENT;
    const selectedGroup = sameBandGroup.slice(0, BUNDLER_FUNDER_MAX_RECIPIENT_WATCHES);
    let watch: FunderRecipientWatch | null = null;
    for (const entry of selectedGroup) {
      const entryWatch = this.addBundlerFunderRecipientWatch(state, {
        recipient: entry.recipient,
        signature: entry.signature,
        amountSol: entry.amountSol,
        timestamp: entry.timestamp,
        buyTriggersEntry: false,
        normalTinyTransferMode: true,
      });
      if (!entryWatch) continue;
      entryWatch.normalTinyExitPercent = exitPercent;
      entryWatch.tokenBuyObserved = true;
      entryWatch.firstBuySignature = entry.signature;
      entryWatch.firstBuyTimestamp = entry.timestamp;
      this.subscribeFunderRecipient(entryWatch.wallet);
      this.markFunderRecipientDirty(entryWatch.wallet);
      if (entry.signature === tx.signature) watch = entryWatch;
    }
    watch ??= state.recipientWatches.get(selectedGroup[0]?.recipient ?? "") ?? null;
    if (!watch) return false;
    void this.syncFunderRecipientBatch(true);

    this.log.warn("Normal-mode shared feePayer tiny group accepted for immediate buy", {
      mint: state.mint,
      funderWallet: state.funderWallet,
      amountUsd: transferOutUsd,
      exitPercent,
      tinyUsdBand,
      signature: tx.signature,
      selectedRecipients: selectedGroup.map((entry) => entry.recipient),
      selectedSignatures: selectedGroup.map((entry) => entry.signature),
      sameBandGroupCount: sameBandGroup.length,
    });

    void this.sendTelegramSafe(
      [
        `<b>🟢 ${this.label} Normal FeePayer Tiny Funding Group Buy Gate</b>`,
        `Token: <code>${state.mint}</code>`,
        `FeePayer: <code>${state.funderWallet}</code>`,
        `Group: <b>${selectedGroup.length}/${BUNDLER_FUNDER_MAX_RECIPIENT_WATCHES}</b> same USD band within ${BUNDLER_FUNDER_LOW_FUNDING_TINY_GROUP_SECONDS}s`,
        `Band: <b>${tinyUsdBand === "2_5_to_5" ? "$1.00-$5.00" : ">$5.00-$10.00"}</b>`,
        `Selected exit: <b>+${exitPercent}% MC</b>`,
        ...selectedGroup.map((entry, index) => `${index + 1}. <code>${entry.recipient}</code> — $${entry.amountUsd.toFixed(2)} — <code>${entry.signature}</code>`),
      ].join("\n"),
      "normal feePayer tiny funding group buy gate notification",
    );

    await this.emitBundlerFunderBuy(
      state,
      watch,
      tx.signature,
      `normal-mode shared feePayer same-band tiny group accepted with +${exitPercent}% MC exit`,
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
      lowFundingLargeTransferMode: false,
      postEntrySwapSignature: null,
      postEntrySwapBaselineSignatures: new Set<string>(),
      soldAllSignature: null,
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

  private hasFunderTransferOutToAnyBundler(
    state: BundlerFunderWatchState,
    tx: HeliusTransaction,
  ): boolean {
    return (tx.nativeTransfers ?? []).some(
      (transfer) =>
        transfer.fromUserAccount === state.funderWallet &&
        transfer.toUserAccount !== state.funderWallet &&
        state.bundlerWallets.has(transfer.toUserAccount),
    );
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

  private getTinyUsdBand(amountUsd: number): "lt2_5" | "2_5_to_5" | "gt5" {
    if (amountUsd < BUNDLER_FUNDER_NORMAL_TINY_MIN_BUY_USD) return "lt2_5";
    if (amountUsd <= BUNDLER_FUNDER_NORMAL_TINY_MID_MAX_USD) return "2_5_to_5";
    return "gt5";
  }

  private recordNormalTinyTransferOut(
    state: BundlerFunderWatchState,
    entry: { signature: string; timestamp: number; recipient: string; amountSol: number; amountUsd: number },
  ): void {
    if (state.normalTinyTransferOuts.some((existing) => existing.signature === entry.signature)) return;
    state.normalTinyTransferOuts.push(entry);
    state.normalTinyTransferOuts = state.normalTinyTransferOuts
      .filter((existing) => entry.timestamp - existing.timestamp <= 180)
      .sort((a, b) => a.timestamp - b.timestamp);
  }

  private getNormalTinySameBandGroup(
    state: BundlerFunderWatchState,
    timestamp: number,
    band: "lt2_5" | "2_5_to_5" | "gt5",
  ): Array<{ signature: string; timestamp: number; recipient: string; amountSol: number; amountUsd: number }> {
    const start = timestamp - BUNDLER_FUNDER_LOW_FUNDING_TINY_GROUP_SECONDS;
    const group = state.normalTinyTransferOuts.filter(
      (entry) =>
        entry.timestamp >= start &&
        entry.timestamp <= timestamp &&
        !state.bundlerWallets.has(entry.recipient) &&
        !this.isKnownFunderCandidate(state, entry.signature),
    );
    if (group.length < 2) return [];
    if (group.some((entry) => this.getTinyUsdBand(entry.amountUsd) !== band)) return [];
    if (
      band !== "lt2_5" &&
      group.some((entry) => !this.isRoundBundlerTinySolAmount(entry.amountSol, band))
    ) {
      return [];
    }
    const uniqueRecipients = new Set(group.map((entry) => entry.recipient));
    if (uniqueRecipients.size < 2) return [];
    return group;
  }

  /** True if `amountSol` is within the slim round-amount tolerance of `target`. */
  private isNearBundlerTinySolAmount(amountSol: number, target: number): boolean {
    return Math.abs(amountSol - target) <= BUNDLER_FUNDER_NORMAL_TINY_ROUND_SOL_TOLERANCE_SOL;
  }

  /** True if `amountSol` is approximately one of the round funding sizes valid for `band` (see BUNDLER_FUNDER_NORMAL_TINY_ROUND_SOL_AMOUNTS_BY_BAND), within a slim tolerance. Used to require the $1-$5/>$5-$10 buy-triggering bands to be genuine round-number bundler funding for that specific band, not just any transfer-out that happens to land in the same USD band. */
  private isRoundBundlerTinySolAmount(
    amountSol: number,
    band: "lt2_5" | "2_5_to_5" | "gt5",
  ): boolean {
    return BUNDLER_FUNDER_NORMAL_TINY_ROUND_SOL_AMOUNTS_BY_BAND[band].some((target) =>
      this.isNearBundlerTinySolAmount(amountSol, target),
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
  private async hasRecentAnyTokenSwapHistory(
    wallet: string,
    preferredClientIndex: number,
  ): Promise<boolean> {
    const history = await this.getConfirmedWalletSwapHistory(
      wallet,
      50,
      preferredClientIndex,
    );
    const minTimestamp = Math.floor(
      (Date.now() - BUNDLER_FUNDER_LOW_FUNDING_LARGE_SWAP_HISTORY_MAX_AGE_MS) / 1_000,
    );
    return history.some(
      (tx) =>
        (!tx.type || tx.type === "SWAP") &&
        Number.isFinite(tx.timestamp) &&
        tx.timestamp >= minTimestamp,
    );
  }

  private async isValidLowFundingLargeTransferRecipient(
    state: BundlerFunderWatchState,
    wallet: string,
    timestamp: number,
    preferredClientIndex: number,
  ): Promise<{ valid: boolean; reason: string; activitySignature?: string | null }> {
    const txs = await this.withHeliusFallback(
      (client) => client.getWalletTransactionsDesc(wallet, INSIDER_HISTORY_LIMIT),
      preferredClientIndex,
    );
    const currentTokenBuy = txs.find((tx) => {
      if (tx.timestamp > timestamp) return false;
      if (!this.isRelevantMintTx(tx, state.mint)) return false;
      return this.classifyTx(tx, wallet, state.mint) === "buy";
    });
    if (currentTokenBuy) {
      return {
        valid: true,
        reason: "recipient already has current-token buy activity",
        activitySignature: currentTokenBuy.signature,
      };
    }
    const hasRecentSwap = await this.hasRecentAnyTokenSwapHistory(
      wallet,
      preferredClientIndex,
    );
    return {
      valid: hasRecentSwap,
      reason: hasRecentSwap
        ? "recipient has any-token SWAP history within 1 day"
        : "recipient has no current-token buy and no 1-day SWAP history",
      activitySignature: null,
    };
  }

  private async maybeTriggerLowFundingPendingTinyBuys(
    state: BundlerFunderWatchState,
    source: string,
  ): Promise<void> {
    if (!state.lowFundingMode || this.buyDisabled) return;
    if (this.buySubmitted) return;
    if (!this.devWallet || state.lowFundingPendingTinyBuyWallets.size === 0) {
      await this.stopLowFundingDevWalletSubscription("no pending low-funding tiny wallets");
      return;
    }
    this.subscribeLowFundingDevWallet(state);
    try {
      const txs = await this.withHeliusFallback(
        (client) => client.getWalletTransactionsDesc(this.devWallet!, LOW_FUNDING_DEV_BUY_SYNC_LIMIT),
        HELIUS_POOL_MC_RESERVED_INDEX,
      );
      const devBuys = txs
        .filter((tx) => this.isRelevantMintTx(tx, state.mint))
        .filter((tx) => tx.signature !== this.devCreateSignature)
        .filter((tx) =>
          this.devCreateTimestamp === null || tx.timestamp > this.devCreateTimestamp,
        )
        .filter((tx) => this.classifyTx(tx, this.devWallet!, state.mint) === "buy")
        .sort((a, b) => a.timestamp - b.timestamp || a.slot - b.slot);
      for (const tx of devBuys) state.lowFundingDevBuySignatures.add(tx.signature);
      const devBuyAfterCreate = devBuys[0] ?? null;
      if (!devBuyAfterCreate) {
        this.log.info("Low-funding tiny buy gate waiting for dev buy after create", {
          mint: state.mint,
          devWallet: this.devWallet,
          devBuyCountAfterCreate: devBuys.length,
          syncedDevTxLimit: LOW_FUNDING_DEV_BUY_SYNC_LIMIT,
          devCreateSignature: this.devCreateSignature,
          devCreateTimestamp: this.devCreateTimestamp,
          pendingWallets: [...state.lowFundingPendingTinyBuyWallets],
          source,
        });
        return;
      }
      state.lowFundingDevBuyAfterCreateSignature = devBuyAfterCreate.signature;
      state.lowFundingDevBuyAfterCreateTimestamp = devBuyAfterCreate.timestamp;
      state.lowFundingTinyDevExitBaselineSignature = devBuyAfterCreate.signature;
      state.lowFundingTinyDevExitBaselineTimestamp = devBuyAfterCreate.timestamp;
      const pendingWallets = [...state.lowFundingPendingTinyBuyWallets];
      this.log.warn("Low-funding tiny dev buy-after-create gate passed", {
        mint: state.mint,
        devWallet: this.devWallet,
        devBuyAfterCreateSignature: devBuyAfterCreate.signature,
        syncedDevTxLimit: LOW_FUNDING_DEV_BUY_SYNC_LIMIT,
        devCreateSignature: this.devCreateSignature,
        devCreateTimestamp: this.devCreateTimestamp,
        pendingWallets,
        source,
      });
      void this.sendTelegramSafe(
        [
          `<b>🟢 ${this.label} Low-Funding Dev Buy Gate</b>`,
          `Token: <code>${state.mint}</code>`,
          `Dev: <code>${this.devWallet}</code>`,
          `Mint/create tx: <code>${this.devCreateSignature ?? "unknown"}</code>`,
          `Dev buy after create: <code>${devBuyAfterCreate.signature}</code>`,
          `Pending tiny wallets: <b>${pendingWallets.length}</b>`,
          "",
          "Low-funding tiny candidate is now allowed to buy with fixed $25k MC exit.",
        ].join("\n"),
        "low-funding dev buy-after-create gate notification",
      );
      const watch = pendingWallets
        .map((wallet) => state.recipientWatches.get(wallet))
        .find((entry): entry is FunderRecipientWatch => Boolean(entry));
      if (!watch) return;
      await this.emitLowFundingRecipientBuy(
        state,
        watch,
        devBuyAfterCreate.signature,
        "dev wallet bought again after the mint/create tx and low-funding tiny gate",
        false,
        undefined,
        { fixedExitMc: BUNDLER_FUNDER_LOW_FUNDING_TINY_EXIT_MC_USD, disableProfitExit: false },
      );
    } catch (err) {
      void this.heliusClient.handlePossibleRateLimitError(err);
      this.log.warn("Low-funding dev buy-after-create gate check failed", {
        mint: state.mint,
        devWallet: this.devWallet,
        devCreateSignature: this.devCreateSignature,
        devCreateTimestamp: this.devCreateTimestamp,
        source,
        error: err instanceof Error ? err.message : String(err),
      });
    }
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

    if (this.buySubmitted || this.phase === "holding") {
      return;
    }

    if (state.bundlerWallets.has(transferOut.to)) return;
    const tinyUsdBand = this.getLowFundingTinyUsdBand(amountUsd);
    let sameBandGroup: Array<{ signature: string; timestamp: number; recipient: string; amountSol: number; amountUsd: number }> = [];
    if (amountUsd < BUNDLER_FUNDER_LOW_FUNDING_TINY_MIN_BUY_USD) {
      this.log.info("Low-funding tiny transfer skipped: below minimum USD band", {
        mint: state.mint,
        wallet: transferOut.to,
        fundingSignature: tx.signature,
        amountSol: transferOut.amountSol,
        amountUsd,
        tinyUsdBand,
        sameBandGroupCount: sameBandGroup.length,
        bundlerGateRequired: tinyUsdBand === "2_5_to_5",
        minUsd: BUNDLER_FUNDER_LOW_FUNDING_TINY_MIN_BUY_USD,
      });
      return;
    }

    if (tinyUsdBand === "2_5_to_5" && !state.lowFundingTinyBundlerGateSeen) {
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
            "Waiting for the next $1-$5 tiny transfer to a non-bundler wallet with prior activity in this token.",
          ].join("\n"),
          "low-funding tiny bundler gate notification",
        );
      }
      return;
    }

    sameBandGroup = this.getLowFundingTinySameBandGroup(
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

    if (
      tinyUsdBand === "gt5" &&
      this.hasFunderTransferOutToAnyBundler(state, tx)
    ) {
      this.log.info("Low-funding >$5 tiny transfer skipped: same feePayer tx also funds a bundler", {
        mint: state.mint,
        wallet: transferOut.to,
        fundingSignature: tx.signature,
        amountSol: transferOut.amountSol,
        amountUsd,
        tinyUsdBand,
        bundlers: [...state.bundlerWallets],
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
      bundlerGateRequired: buyUsdBand === "2_5_to_5",
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
      this.subscribeFunderRecipient(watch.wallet);
      this.markFunderRecipientDirty(watch.wallet);
      void this.syncFunderRecipientBatch(true);
      state.lowFundingTinyBoughtUsdBands.add(buyUsdBand);
      this.subscribeLowFundingDevWallet(state);

      if (buyUsdBand === "gt5") {
        state.lowFundingTinyDevExitBaselineSignature = tx.signature;
        state.lowFundingTinyDevExitBaselineTimestamp = tx.timestamp;
        this.log.warn("Low-funding >$5 tiny recipient qualified; buying immediately", {
          mint: state.mint,
          wallet: watch.wallet,
          fundingSignature: tx.signature,
          tinyUsdBand: buyUsdBand,
          devWallet: this.devWallet,
          devExitBaselineSignature: tx.signature,
        });
        await this.emitLowFundingRecipientBuy(
          state,
          watch,
          tx.signature,
          "low-funding >$5 tiny transfer qualified; buying immediately, with $25k MC plus next dev buy as exit gate",
          false,
          undefined,
          { fixedExitMc: BUNDLER_FUNDER_LOW_FUNDING_TINY_EXIT_MC_USD, disableProfitExit: false },
        );
        return;
      }

      state.lowFundingPendingTinyBuyWallets.add(watch.wallet);
      this.log.warn("Low-funding tiny recipient pending dev buy-after-create gate", {
        mint: state.mint,
        wallet: watch.wallet,
        fundingSignature: tx.signature,
        tinyUsdBand: buyUsdBand,
        devWallet: this.devWallet,
        devCreateSignature: this.devCreateSignature,
        devCreateTimestamp: this.devCreateTimestamp,
        pendingWallets: [...state.lowFundingPendingTinyBuyWallets],
      });
      void this.sendTelegramSafe(
        [
          `<b>🟡 ${this.label} Low-Funding Tiny Candidate Pending</b>`,
          `Token: <code>${state.mint}</code>`,
          `Recipient: <code>${watch.wallet}</code>`,
          `Band: <b>$1-$5</b>`,
          `Funding tx: <code>${tx.signature}</code>`,
          `Dev: <code>${this.devWallet ?? "unknown"}</code>`,
          "",
          "Waiting for a dev wallet buy after the mint/create tx before bot buy. Exit will be fixed $25k MC if the dev gate passes.",
        ].join("\n"),
        "low-funding tiny pending dev gate notification",
      );
      await this.maybeTriggerLowFundingPendingTinyBuys(state, "low-funding tiny candidate qualified");
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
      this.recordObservedMarketCapUsd(currentMc);
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
      if (this.isBelowInitialBundlerMarketCap(currentMc)) {
        this.log.warn(
          "Low-funding shared feePayer condition passed, but current market cap is below the token's initial bundler-buy MC; resetting instead of buying",
          {
            mint: state.mint,
            sharedFeePayer: state.funderWallet,
            currentMc,
            initialBundlerMarketCapUsd: this.initialBundlerMarketCapUsd,
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
        buySol: this.getBuySolForFundingMode(state.lowFundingMode),
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
          "Sell rule: <b>MC profit target disabled</b>; rug, recipient sell-all, and recipient zero-SOL exits remain active.",
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
    exitOptions: { fixedExitMc?: number; exitPercent?: number; disableProfitExit?: boolean } = {},
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
      this.recordObservedMarketCapUsd(currentMc);
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
      if (this.isBelowInitialBundlerMarketCap(currentMc)) {
        this.log.warn(
          "Low-funding recipient bought token, but current market cap is below the token's initial bundler-buy MC; resetting instead of buying",
          {
            mint: state.mint,
            recipient: watch.wallet,
            currentMc,
            initialBundlerMarketCapUsd: this.initialBundlerMarketCapUsd,
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

      const exitMc = exitOptions.fixedExitMc ?? (
        exitOptions.exitPercent !== undefined
          ? currentMc * (1 + exitOptions.exitPercent / 100)
          : null
      );
      if (exitMc !== null) this.setExitMc(exitMc);
      this.setEntryMc(currentMc);
      this.setBuyExecuting(true);
      this.buySubmitted = true;
      this.preBuyStopped = true;
      this.disableProfitExitAfterBuy = exitOptions.disableProfitExit ?? true;
      this.emit("buyTrigger", {
        followedWallet: this.followedWallet!,
        mint: state.mint,
        signature,
        buySol: this.getBuySolForFundingMode(state.lowFundingMode),
        entryMc: currentMc,
        monitoredWallet: watch.wallet,
        tradersListStr: [
          "<b>Low-Funding Recipient Buy Gate Passed</b>",
          `FeePayer: <code>${state.funderWallet}</code>`,
          `Recipient: <code>${watch.wallet}</code>`,
          `Transfer-out: <b>${watch.outAmountSol.toFixed(4)} SOL</b>`,
          `Low-funding tiny band: <b>$${BUNDLER_FUNDER_LOW_FUNDING_TINY_MIN_BUY_USD.toFixed(2)}-$${BUNDLER_FUNDER_NORMAL_TINY_TRANSFER_OUT_MAX_USD.toFixed(0)}</b>`,
          `Trigger tx: <code>${signature}</code>`,
          `Buy gate: <b>${gateDescription}</b>`,
          `Current MC: <b>$${currentMc.toLocaleString()}</b>`,
          "",
          "",
          exitMc !== null
            ? `Sell rule: <b>MC exit active at $${exitMc.toLocaleString()}</b>; rug, tiny sell-all, and tiny SOL-zero exits remain active.`
            : "Sell rule: <b>MC profit target disabled</b>; rug, recipient sell-all, and recipient zero-SOL exits remain active.",
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
      this.recordObservedMarketCapUsd(currentMc);
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
      if (this.isBelowInitialBundlerMarketCap(currentMc)) {
        this.log.warn(
          "Shared feePayer recipient bought token, but current market cap is below the token's initial bundler-buy MC; resetting instead of buying",
          {
            mint: state.mint,
            recipient: watch.wallet,
            currentMc,
            initialBundlerMarketCapUsd: this.initialBundlerMarketCapUsd,
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
      if (
        !state.lowFundingMode &&
        watch.normalTinyTransferMode &&
        this.highestObservedMarketCapUsd !== null &&
        this.highestObservedMarketCapUsd >= newExitMc
      ) {
        this.log.warn(
          "Skipping normal-mode tiny-band buy because a previously observed market cap already reached this band's exit target",
          {
            mint: state.mint,
            recipient: watch.wallet,
            currentMc,
            exitPercent,
            wouldBeExitMc: newExitMc,
            highestObservedMarketCapUsd: this.highestObservedMarketCapUsd,
          },
        );
        return;
      }
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
        buySol: this.getBuySolForFundingMode(state.lowFundingMode),
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

  private async sellIfLowFundingLargeRecipientBuyTooSmall(
    state: BundlerFunderWatchState,
    watch: FunderRecipientWatch,
    tx: HeliusTransaction,
  ): Promise<boolean> {
    if (!state.lowFundingMode || !watch.lowFundingLargeTransferMode) return false;
    if (this.phase !== "holding" || this.positionSellTriggered) return false;
    const buySol = this.estimateRecipientBuySolSpent(tx, watch.wallet);
    const solPriceUsd = await this.getCachedSolPriceUsd();
    const buyUsd =
      buySol !== null && solPriceUsd !== null ? buySol * solPriceUsd : null;
    const shouldSell =
      buyUsd !== null && buyUsd < BUNDLER_FUNDER_RECIPIENT_MIN_BUY_USD;
    this.log.warn("Checked low-funding large recipient buy USD after bot entry", {
      mint: state.mint,
      wallet: watch.wallet,
      signature: tx.signature,
      fundingSignature: watch.fundingSignature,
      buySol,
      solPriceUsd,
      buyUsd,
      minBuyUsd: BUNDLER_FUNDER_RECIPIENT_MIN_BUY_USD,
      shouldSell,
    });
    if (!shouldSell) return false;

    await this.triggerPositionSell(
      state.mint,
      `Low-funding large-transfer recipient ${watch.wallet} bought below $${BUNDLER_FUNDER_RECIPIENT_MIN_BUY_USD.toFixed(0)} after bot entry`,
      [
        `<b>🚨 ${this.label} Low-Funding Recipient Buy Too Small</b>`,
        `Token: <code>${state.mint}</code>`,
        `Recipient: <code>${watch.wallet}</code>`,
        `Funding tx: <code>${watch.fundingSignature}</code>`,
        `Buy tx: <code>${tx.signature}</code>`,
        buySol !== null ? `Buy SOL: <b>${buySol.toFixed(6)} SOL</b>` : "Buy SOL: <b>unknown</b>",
        solPriceUsd !== null ? `SOL price: <b>$${solPriceUsd.toFixed(2)}</b>` : "SOL price: <b>unknown</b>",
        buyUsd !== null ? `Buy USD: <b>$${buyUsd.toFixed(2)}</b>` : "Buy USD: <b>unknown</b>",
        `Required: <b>$${BUNDLER_FUNDER_RECIPIENT_MIN_BUY_USD.toFixed(0)}+</b>`,
        "",
        "Bot already entered from the legacy low-funding large-transfer path, so selling now.",
      ],
      tx.signature,
    );
    return true;
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
      const tinyWalletMode = watch.normalTinyTransferMode || state.lowFundingMode;
      if (!tinyWalletMode) {
        await this.sellIfRecipientSolBalanceIsZero(state, watch, tx);
        if (this.positionSellTriggered) return;
      } else if (source === "notification" && tx.timestamp > watch.fundingTimestamp) {
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
              ? this.isNormalTinyWalletExitDisabled(state, watch)
                ? "Exit watch armed: configured MC target remains active; rug exits remain active. Recipient sell-all and SOL-zero exits are disabled for normal tiny paths."
                : "Exit watch armed: configured % MC target remains active. Bot will also sell on rug, recipient sell-all, or recipient SOL balance reaching zero on a new post-funding tx notification."
              : state.lowFundingMode
              ? "Exit watch armed: MC profit target is disabled. Bot will sell on rug or clean post-entry 4-bundler tiny-transfer exit."
              : "Exit watch armed: MC profit target is disabled. Bot will sell on rug, recipient sell-all, or recipient SOL balance reaching zero.",
          ].join("\n"),
          "recipient first-buy notification",
        );
        if (await this.sellIfLowFundingLargeRecipientBuyTooSmall(state, watch, tx)) {
          return;
        }
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
        if (
          watch.lowFundingLargeTransferMode &&
          watch.tokenActions.filter((entry) => entry.kind === "buy").length === 1
        ) {
          if (await this.sellIfLowFundingLargeRecipientBuyTooSmall(state, watch, tx)) {
            return;
          }
        }
      }
      const tinyWalletMode = watch.normalTinyTransferMode || state.lowFundingMode;
      if (!tinyWalletMode) {
        await this.sellIfRecipientSolBalanceIsZero(state, watch, tx);
      } else if (source === "notification" && tx.timestamp > watch.fundingTimestamp) {
        await this.sellIfRecipientSolBalanceIsZero(state, watch, tx);
      }
      return;
    }

    this.pruneRecipientWithoutEarlyTokenBuy(state, watch);
    if (!watch.firstBuySignature) return;
    if (watch.tokenActions.some((existing) => existing.signature === tx.signature)) return;

    if (
      (watch.normalTinyTransferMode || state.lowFundingMode) &&
      tx.timestamp <= watch.fundingTimestamp
    ) {
      this.log.info("Tiny recipient sell ignored because it is not after tiny funding", {
        mint: state.mint,
        wallet: watch.wallet,
        signature: tx.signature,
        fundingSignature: watch.fundingSignature,
        fundingTimestamp: watch.fundingTimestamp,
        txTimestamp: tx.timestamp,
        lowFundingMode: state.lowFundingMode,
        normalTinyTransferMode: watch.normalTinyTransferMode,
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
    const tinySellAllMode = watch.normalTinyTransferMode || state.lowFundingMode;
    this.log.info(
      state.lowFundingMode
        ? "Low-funding tiny recipient sell observed"
        : watch.normalTinyTransferMode
        ? "Normal tiny recipient sell observed"
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
        lowFundingTinyUsdBand: watch.lowFundingTinyUsdBand,
        normalTinyTransferMode: watch.normalTinyTransferMode,
      },
    );
    if (
      (soldAllByTxBalance || soldAllByTrackedAmount) &&
      this.phase === "holding" &&
      !this.isNormalTinyWalletExitDisabled(state, watch)
    ) {
      watch.soldAllSignature = tx.signature;
      if (state.lowFundingMode && watch.lowFundingTinyUsdBand) {
        state.lowFundingTinySoldUsdBands.add(watch.lowFundingTinyUsdBand);
      }
      const tinyWatches = tinySellAllMode
        ? [...state.recipientWatches.values()].filter(
            (entry) =>
              Boolean(entry.firstBuySignature) &&
              (entry.normalTinyTransferMode || state.lowFundingMode),
          )
        : [];
      const tinySoldAllCount = tinyWatches.filter((entry) => entry.soldAllSignature).length;
      if (tinySellAllMode && tinyWatches.length > 0 && tinySoldAllCount < tinyWatches.length) {
        this.log.info("Tiny recipient sold all; waiting for remaining tracked tiny recipients", {
          mint: state.mint,
          wallet: watch.wallet,
          signature: tx.signature,
          soldAllCount: tinySoldAllCount,
          trackedTinyWallets: tinyWatches.length,
          trackedWallets: tinyWatches.map((entry) => entry.wallet),
          soldWallets: tinyWatches
            .filter((entry) => entry.soldAllSignature)
            .map((entry) => entry.wallet),
        });
        return;
      }
      await this.triggerPositionSell(
        state.mint,
        state.lowFundingMode
          ? `Low-funding tiny recipient sell-all threshold reached (${tinySoldAllCount}/${tinyWatches.length || 1})`
          : watch.normalTinyTransferMode
          ? `Normal tiny recipient sell-all threshold reached (${tinySoldAllCount}/${tinyWatches.length || 1})`
          : `Shared feePayer recipient ${watch.wallet} sold all tracked token position`,
        [
          "<b>🚨 Shared-Funder Recipient Sold All</b>",
          `Token: <code>${state.mint}</code>`,
          `Recipient: <code>${watch.wallet}</code>`,
          `First buy: <code>${watch.firstBuySignature}</code>`,
          `Sell tx: <code>${tx.signature}</code>`,
          tinySellAllMode
            ? `Tiny wallets sold all: <b>${tinySoldAllCount}/${tinyWatches.length || 1}</b>`
            : "",
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
    _state: BundlerFunderWatchState,
    _watch: FunderRecipientWatch,
    _tx: HeliusTransaction,
  ): Promise<void> {
    return;
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
    this.log.debug("Skipping recipient SOL balance-at check; live account subscription handles SOL-zero exits", {
      mint: state.mint,
      wallet: watch.wallet,
      signature: tx.signature,
    });
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
    await this.stopPreBuyMonitoring();
    await this.stopInsiderMonitoring();
    await this.stopBundlerMonitoring();
    await this.stopBundlerFunderMonitoring();
    if (this.batchTimer) {
      clearTimeout(this.batchTimer);
      this.batchTimer = null;
    }
    this.processedSignatures.clear();
    this.queuedSignatures.clear();
    this.pendingSignaturesBatch = [];
    this.isSwitchingInsiderWallet = false;
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
    this.log.info("Insider sell threshold reached — insider wallet tracking complete", {
      mint,
      sellCount: this.insiderState?.sellCount,
      required: this.requiredInsiderSells,
      bundlerMatchesReady: this.bundlerMatchesReady,
      phase: this.phase,
    });
    this.startPollLoop();
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

  private recordObservedMarketCapUsd(marketCapUsd: number): void {
    if (
      this.highestObservedMarketCapUsd === null ||
      marketCapUsd > this.highestObservedMarketCapUsd
    ) {
      this.highestObservedMarketCapUsd = marketCapUsd;
    }
  }

  /**
   * Buy filter: the market cap at actual buy time must not be below the
   * market cap captured at the very start of this token's flow (the
   * follow-wallet/initial-bundler buy MC) — i.e. don't buy into a token
   * that's already round-tripped back below where the earliest bundler
   * bought. Returns false (no gate) if the initial MC is unknown.
   */
  private isBelowInitialBundlerMarketCap(currentMc: number): boolean {
    return (
      this.initialBundlerMarketCapUsd !== null &&
      currentMc < this.initialBundlerMarketCapUsd
    );
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
    this.watchingMint = null;
    this.monitoredWallet = null;
    this.insiderState = null;
    this.clearBundlerAccumulation();
    this.initialInsiderWallets.clear();
    this.insiderWalletChain.clear();
    this.isSwitchingInsiderWallet = false;
    this.devWallet = null;
    this.devCreateSignature = null;
    this.devCreateTimestamp = null;
    this.highestObservedMarketCapUsd = null;
    this.initialBundlerMarketCapUsd = null;
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
    this.devCreateSignature = null;
    this.devCreateTimestamp = null;
    this.highestObservedMarketCapUsd = null;
    this.initialBundlerMarketCapUsd = null;
    this.preBuyStopped = false;
    this.positionSellTriggered = false;
    this.profitExitDisabled = false;
    this.disableProfitExitAfterBuy = false;
    this.insiderSellsReady = false;
    this.bundlerMatchesReady = false;
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
