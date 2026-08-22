import {
  AccountInfo,
  Connection,
  LAMPORTS_PER_SOL,
  PublicKey,
} from "@solana/web3.js";
import {
  AccountLayout,
  getAssociatedTokenAddressSync,
  TOKEN_2022_PROGRAM_ID,
  TOKEN_PROGRAM_ID,
} from "@solana/spl-token";
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
import { HeliusEnhancedWsClient } from "./helius-enhanced-ws";
import { isDevRugCloseAccountTx, UNKNOWN_COUNTERPARTY } from "./tx-normalizer";
import { extractFirstUniqueEarlyBundlerBuys } from "./wallet-swap-detector";

const SOL_MINT = "So11111111111111111111111111111111111111112";
const NATIVE_SOL_BALANCE_MINT =
  "So11111111111111111111111111111111111111111";
const INSIDER_HISTORY_LIMIT = 21;
const LOW_FUNDING_DEV_BUY_SYNC_LIMIT = 10;
/** Proceed with bot buy only while dev has fewer than this many mint buys after create (create tx excluded). */
const DEV_BUY_COUNT_AFTER_CREATE_MAX_EXCLUSIVE = 2;
const REQUIRED_BUNDLER_MATCHES = 2;
const INSIDER_RUG_MARKET_CAP_USD = 5_000;
/** Live rug reset/sell when MC drops below this during pre-buy or in-position monitoring. */
const INSIDER_RUG_RESET_MARKET_CAP_USD = 3_000;
const MAX_FOLLOW_WALLET_START_MARKET_CAP_USD = 80_000;
const BUNDLER_FUNDER_TRANSFER_LIMIT = 5;
const BUNDLER_FUNDER_REQUIRED_COUNT = 4;
/** Post-zero bundler funding window: selected tx must have incoming SOL above this threshold (most recent qualifying transfer wins; dust-drain latest tx is skipped). */
const BUNDLER_FUNDER_MIN_SELECTED_FUNDING_SOL = 15;
/** Of the BUNDLER_FUNDER_REQUIRED_COUNT (4) early bundler funding records, at least this many must share the exact same feePayer for the shared-feePayer watch to start. Relaxed from requiring all 4 to match, since a single outlier (e.g. one bundler additionally/separately funded from an unrelated wallet) shouldn't block an otherwise-clear shared-feePayer pattern. The majority feePayer's records are used for the watch; any non-matching outlier record is ignored (its bundler wallet is still tracked as an early buyer, just not as a funding source). */
const BUNDLER_FUNDER_MIN_MATCHING_FEEPAYER_COUNT = 3;
const BUNDLER_FUNDER_FUNDING_RECORD_ATTEMPTS = 3;
const BUNDLER_FUNDER_FUNDING_RECORD_RETRY_DELAY_MS = 500;
/** Funder-first normal-mode minimum bundler funding / transfer-out threshold. */
const BUNDLER_FUNDER_LOW_FUNDING_SOL = 20;
/** Follow-wallet normal-mode minimum bundler funding threshold (tiny round groups only). */
const BUNDLER_FUNDER_FOLLOW_WALLET_NORMAL_FUNDING_MIN_SOL = 3;
/** Kill switch for the whole low-funding-mode path. While false, any shared feePayer whose largest funding is below its flow's normal-mode threshold is skipped entirely (no watch is even created for it) instead of being handled via low-funding logic. */
const BUNDLER_FUNDER_LOW_FUNDING_MODE_ENABLED = false;
/** Max follow wallets monitored concurrently on one insider bot. */
export const MAX_FOLLOW_WALLETS = 4;
const BUNDLER_FUNDER_LOW_FUNDING_MAX_TRANSFER_OUT_TXS = 5;
const BUNDLER_FUNDER_LOW_FUNDING_EXIT_PERCENT = 50;
const BUNDLER_FUNDER_LOW_FUNDING_MIN_TRANSFER_OUT_SOL = 3.5;
const BUNDLER_FUNDER_LOW_FUNDING_LARGE_EXIT_PERCENT = 180;
const BUNDLER_FUNDER_LOW_FUNDING_TINY_EXIT_MC_USD = 25_000;
const BUNDLER_FUNDER_LOW_FUNDING_LARGE_SWAP_HISTORY_MAX_AGE_MS = 24 * 60 * 60 * 1_000;
const BUNDLER_FUNDER_LOW_FUNDING_TINY_GROUP_SECONDS = 10;
const BUNDLER_FUNDER_LOW_FUNDING_TINY_MIN_BUY_USD = 1;
const BUNDLER_FUNDER_LOW_FUNDING_TINY_COPYSELL_MIN_USD = 5;
/** Sub-$0.10 transfer-outs are tracked as dust (not round SOL groups). */
const BUNDLER_FUNDER_NORMAL_TINY_DUST_FLOOR_USD = 0.1;
const BUNDLER_FUNDER_NORMAL_TINY_TRANSFER_OUT_MAX_USD = 10;
/** Normal-mode round sizes subject to the &lt; $10 USD cap. */
const BUNDLER_FUNDER_NORMAL_TINY_USD_CAPPED_ROUND_SOL_AMOUNTS = [0.02, 0.05] as const;
/** Normal-mode round sizes allowed even when USD value exceeds $10. */
const BUNDLER_FUNDER_NORMAL_TINY_USD_EXEMPT_ROUND_SOL_AMOUNTS = [
  0.1, 0.15, 0.2, 0.25, 0.3, 0.35, 0.4, 0.45, 0.5,
] as const;
/** Normal-mode buy only on these exact round sizes (± tolerance) as same-amount 10s groups. */
const BUNDLER_FUNDER_NORMAL_TINY_VALID_ROUND_SOL_AMOUNTS = [
  ...BUNDLER_FUNDER_NORMAL_TINY_USD_CAPPED_ROUND_SOL_AMOUNTS,
  ...BUNDLER_FUNDER_NORMAL_TINY_USD_EXEMPT_ROUND_SOL_AMOUNTS,
] as const;
const BUNDLER_FUNDER_NORMAL_TINY_ROUND_SOL_TOLERANCE_SOL = 0.004;

function formatNormalTinyRoundSolLabel(
  amounts: readonly number[],
): string {
  return amounts.map((amount) => `~${amount}`).join(" / ") + " SOL";
}

interface FollowTokenBundlerAnchorGroup {
  anchorTimestamp: number;
  wallets: string[];
}

type FollowTokenWatchMode = "standard";

const FOLLOW_TOKEN_LARGE_INSIDER_MIN_FEEPAYER_OUT_SOL = 15;
const FOLLOW_TOKEN_LARGE_INSIDER_MIN_CHAIN_OUT_SOL = 8;
const FOLLOW_TOKEN_LARGE_INSIDER_FEEPAYER_WINDOW_SEC = 20 * 60;
const FOLLOW_TOKEN_LARGE_INSIDER_FEEPAYER_SYNC_LIMIT = 100;
const FOLLOW_TOKEN_LARGE_INSIDER_MAX_VALID_WALLETS = 5;
const FOLLOW_TOKEN_LARGE_INSIDER_BUY_AT_VALID_WALLET_COUNT = 4;
/** At buy time (wallet #4), at least one of the first four valid wallets must have Qualified SOL below this. */
const FOLLOW_TOKEN_LARGE_INSIDER_BUY_REQUIRES_ONE_QUALIFIED_SOL_BELOW = 20;
const FOLLOW_TOKEN_LARGE_INSIDER_EXIT_SOLD_FRACTION = 0.25;
const FOLLOW_TOKEN_LARGE_INSIDER_PROFIT_EXIT_PERCENT = 80;
const FOLLOW_TOKEN_LARGE_INSIDER_MAX_CHILDREN_PER_WALLET = 2;
/** Scrape wallets (tier1 or chain) with first buy above this SOL are not valid Large Insider buyers. */
const FOLLOW_TOKEN_LARGE_INSIDER_MAX_VALID_WALLET_FIRST_BUY_SOL = 10;
/** First token buy on a scrape wallet must exceed this USD to count as a valid Large Insider wallet. */
const FOLLOW_TOKEN_LARGE_INSIDER_VALID_WALLET_FIRST_BUY_MIN_USD = 110;
/** Keep dev transfer-out watch active this long after buy submit. */
const DEV_WALLET_TOKEN_OUT_POST_BUY_WATCH_MS = 3 * 60 * 1_000;
const FOLLOW_TOKEN_LARGE_INSIDER_VALID_WALLET_REASON =
  "follow_token_large_insider_valid_wallet";
const FOLLOW_TOKEN_EARLY_BUNDLER_EXIT_SYNC_PAGE_LIMIT = 100;
/** Bundler max cumulative sell USD at or below this → immediate sell once all sold all (if no valid LI ≥25%). */
const FOLLOW_TOKEN_EARLY_BUNDLER_EXIT_LOW_SELL_USD_THRESHOLD = 25_500;
/** Any bundler cumulative sell USD above this → +80% MC TP disabled; valid LI ≥25% only. */
const FOLLOW_TOKEN_EARLY_BUNDLER_EXIT_HIGH_SELL_USD_MC_TP_DISABLE = 35_000;
/** Cumulative-USD exit branches require max sell-tx count across watches ≥ this. */
const FOLLOW_TOKEN_EARLY_BUNDLER_EXIT_MIN_SELL_TX_COUNT_FOR_USD_GATE = 75;
/** Pre–1st-LI-wallet bundler sold-all buy: max single sell token amount on highest cumulative-USD watch (standard tier). */
const FOLLOW_TOKEN_EARLY_BUNDLER_PRE_LI_MAX_SINGLE_SELL_TOKEN_AMOUNT = 8_000_000;
/** Fallback when standard 8M gate fails: max single sell ≤ this → buy with reduced MC TP + dedicated buy SOL. */
const FOLLOW_TOKEN_EARLY_BUNDLER_FALLBACK_MAX_SINGLE_SELL_TOKEN_AMOUNT = 16_000_000;
const FOLLOW_TOKEN_EARLY_BUNDLER_FALLBACK_PROFIT_EXIT_PERCENT = 40;
const FOLLOW_TOKEN_FAST_MIGRATION_PROFIT_EXIT_PERCENT = 60;
/** A sold-all gate watch must have disposed of essentially all tracked holdings. */
const FOLLOW_TOKEN_EARLY_BUNDLER_SOLD_ALL_MIN_SOLD_FRACTION = 0.9;
/** Post-LI 16M fallback buy: skip when token ATH MC (GMGN fetch at buy) ≥ this multiple of calculated exit MC. */
const FOLLOW_TOKEN_16M_FALLBACK_BUY_ATH_EXIT_MC_MULTIPLIER = 2;
const FOLLOW_TOKEN_16M_FALLBACK_ATH_MC_FETCH_RETRIES = 3;
const FOLLOW_TOKEN_16M_FALLBACK_ATH_MC_FETCH_RETRY_DELAY_MS = 500;
/** Post-LI bundler sold-all buy (8M standard or 16M fallback): ≥1 present valid LI wallet must have Qualified SOL below this. */
const FOLLOW_TOKEN_POST_LI_BUNDLER_BUY_REQUIRES_ONE_QUALIFIED_SOL_BELOW = 30;

type FollowTokenMaxSingleSellGateTier = "standard_8m" | "fallback_16m" | "fail";
const FOLLOW_TOKEN_EARLY_BUNDLER_EXIT_SOLD_FRACTION = 0.25;
/** Defer sold-all exit eval when ATA hits zero before the sell tx is processed. */
const FOLLOW_TOKEN_EARLY_BUNDLER_ATA_SOLD_ALL_EVAL_DEFER_MS = 1000;
const PUMP_FUN_TOKEN_RAW_DECIMALS = 6;

interface FollowTokenEarlyBundlerExitWatch {
  wallet: string;
  source: "early_bundler" | "transfer_recipient";
  parentWallet: string | null;
  syncAfterSignature: string;
  boughtAmount: number;
  soldAmount: number;
  sellTxCount: number;
  cumulativeSellUsd: number;
  maxSingleSellTokenAmount: number;
  soldAll: boolean;
  soldAllSignature: string | null;
  reachedTwentyFivePercentSold: boolean;
  observedTxSignatures: Set<string>;
  syncComplete: boolean;
  monitoringActive: boolean;
  observedNonZeroTokenBalance: boolean;
}

interface FollowTokenEarlyBundlerExitState {
  active: boolean;
  mint: string;
  watches: Map<string, FollowTokenEarlyBundlerExitWatch>;
  mcTpReachedPending: boolean;
  allSoldAllComplete: boolean;
  highSellUsdMode: boolean;
  earlyBundlerTransferOutObserved: boolean;
  preBuyBundlerPathTriggered: boolean;
  preLiBundlerSoldAllBuy: boolean;
  preLiExitArmedNotified: boolean;
  preLiWaitingForValidLiNotified: boolean;
  preLiBundlerSoldAllBuyBlockedNotified: boolean;
  postLiBundlerSoldAllBuyBlockedNotified: boolean;
  validWalletTwentyFivePercentDeferred: boolean;
  /** Set when bundler sold-all buy fires — used for LI ≥25% exit logging (8M standard vs 16M fallback). */
  maxSingleSellGateTierAtBuy: FollowTokenMaxSingleSellGateTier | null;
  exitTriggerSignature: string | null;
  enhancedWatchIds: Map<string, number>;
  logsSubIds: Map<string, number>;
  tokenAtaBalanceSubIds: Map<string, Map<string, number>>;
  tokenAtaBalancesByAccount: Map<string, Map<string, bigint>>;
  tokenAtaLiveBalanceRaw: Map<string, bigint>;
  deferredSoldAllEvalTimer: ReturnType<typeof setTimeout> | null;
}

interface FollowTokenLargeInsiderScrapeWatch {
  wallet: string;
  fundingSignature: string;
  qualifiedReceivedSol: number;
  fundedBy: string;
  fundingTimestamp: number;
  tier1DirectFromFeePayer: boolean;
  childWallets: string[];
  firstBuyTimestamp: number | null;
  firstBuySignature: string | null;
  boughtAmount: number;
  soldAmount: number;
  tokenActions: Array<{
    kind: "buy" | "sell";
    signature: string;
    amount: number;
  }>;
  observedTxSignatures: Set<string>;
  soldAllSignature: string | null;
  lastSolTransferOutTo: string | null;
}

interface FollowTokenLargeInsiderState {
  mint: string;
  active: boolean;
  triggerReason: string;
  secondGroup: FollowTokenBundlerAnchorGroup;
  tier1FeePayerRecipients: Set<string>;
  scrapeWatches: Map<string, FollowTokenLargeInsiderScrapeWatch>;
  validWallets: string[];
  validWalletSearchComplete: boolean;
  exitTriggerSignature: string | null;
  bundlerFirstBuyAnchorTimestamp: number;
  feePayerWindowEndsAt: number;
  exitOverrideActive: boolean;
  scrapeEnhancedWatchIds: Map<string, number>;
  scrapeSolBalanceSubIds: Map<string, number>;
  seenFeePayerOutSignatures: Set<string>;
  /** Scrape wallets that bought but first buy USD was ≤ min — would be valid LI if above threshold. */
  firstBuyBelowMinUsdWallets: Map<
    string,
    { firstBuyUsd: number; buySol: number | null; signature: string }
  >;
}

function readHeliusTxFeeLamports(tx: HeliusTransaction): number | null {
  const raw = tx.fee;
  if (typeof raw === "number" && Number.isFinite(raw) && raw >= 0) return raw;
  return null;
}

function formatNormalTinyRoundSolWatchDescription(): string {
  const capped = formatNormalTinyRoundSolLabel(
    BUNDLER_FUNDER_NORMAL_TINY_USD_CAPPED_ROUND_SOL_AMOUNTS,
  );
  const exempt = formatNormalTinyRoundSolLabel(
    BUNDLER_FUNDER_NORMAL_TINY_USD_EXEMPT_ROUND_SOL_AMOUNTS,
  );
  return `<b>${capped}</b> (&lt; $${BUNDLER_FUNDER_NORMAL_TINY_TRANSFER_OUT_MAX_USD}) / <b>${exempt}</b> (may exceed $10)`;
}

function isNormalTinyUsdExemptRoundSolAmount(amountSol: number): boolean {
  return BUNDLER_FUNDER_NORMAL_TINY_USD_EXEMPT_ROUND_SOL_AMOUNTS.some(
    (target) =>
      Math.abs(amountSol - target) <= BUNDLER_FUNDER_NORMAL_TINY_ROUND_SOL_TOLERANCE_SOL,
  );
}
/** Minimum txs in 10s to count as a qualifying sol group (dust or round). */
const BUNDLER_FUNDER_NORMAL_TINY_MIN_SOL_GROUP_TXS = 2;
/** Round buy requires at least this many same-size round SOL outs in 10s. */
const BUNDLER_FUNDER_NORMAL_TINY_MIN_ROUND_GROUP_TXS_FOR_BUY = 15;
/** Cumulative dust txs required to skip (round still uses a 10s window). */
const normalTinyQualifyingDustGroupTxs = (): number =>
  BUNDLER_FUNDER_NORMAL_TINY_MIN_ROUND_GROUP_TXS_FOR_BUY;
/** Low-funding mode still bands by USD with discrete round sizes per band. */
const BUNDLER_FUNDER_NORMAL_TINY_ROUND_SOL_AMOUNTS_BY_BAND: Record<
  "lt2_5" | "2_5_to_5" | "gt5",
  number[]
> = {
  lt2_5: [],
  "2_5_to_5": [0.02, 0.05],
  gt5: [0.1],
};
const BUNDLER_FUNDER_NORMAL_TINY_MID_EXIT_PERCENT = 90;
const BUNDLER_FUNDER_NORMAL_TINY_HIGH_EXIT_PERCENT = 180;
/** Round targets at or above this SOL use HIGH exit; smaller rounds use MID. */
const BUNDLER_FUNDER_NORMAL_TINY_HIGH_EXIT_MIN_ROUND_SOL = 0.1;
const BUNDLER_FUNDER_MAX_NORMAL_TRANSFER_OUT_SOL = 100;
/** Recent feePayer txs scanned at lock when live balance is zero. */
const BUNDLER_FUNDER_STARTUP_HANDOFF_HISTORY_LIMIT = 50;
const BUNDLER_FUNDER_STARTUP_HANDOFF_MAX_CHAIN = 5;
const ZERO_BALANCE_EPSILON_SOL = 1e-6;

function bundlerFundingIncomingQualifies(amountSol: number): boolean {
  return amountSol > BUNDLER_FUNDER_MIN_SELECTED_FUNDING_SOL;
}

/** Latest post-zero tx can show dust incoming while wallet balance reflects prior funding (drain/re-wrap). */
function isBundlerFundingDrainIncomingPattern(
  incomingAmountSol: number,
  currentBalance: number,
): boolean {
  return (
    incomingAmountSol <= ZERO_BALANCE_EPSILON_SOL &&
    currentBalance > BUNDLER_FUNDER_MIN_SELECTED_FUNDING_SOL
  );
}

/** Follow-token: also watch the wallet that funded the shared feePayer if funded within this window. */
const FOLLOW_TOKEN_FEEPAYER_FUNDER_MAX_AGE_SEC = 6 * 60 * 60;
/** REST backfill lookback before Enhanced WSS subscribe (covers connect / reconnect lag). */
const FOLLOW_TOKEN_TOP_BUYER_WATCH_BACKFILL_BUFFER_SEC = 20;
/** Recent tx page size for follow-token top-buyer / second-group watch backfill. */
const FOLLOW_TOKEN_TOP_BUYER_WATCH_BACKFILL_LIMIT = INSIDER_HISTORY_LIMIT;
const BUNDLER_FUNDER_SYNC_LIMIT = 20;
const BUNDLER_FUNDER_SYNC_MIN_INTERVAL_MS = 1_000;
const BUNDLER_FUNDER_MAX_RECIPIENT_WATCHES = 2;
/** Round-group buy requires at least one selected recipient's first token buy to exceed this USD. */
const BUNDLER_FUNDER_ROUND_GROUP_RECIPIENT_FIRST_BUY_MIN_USD = 100;
const BUNDLER_FUNDER_ROUND_GROUP_RECIPIENT_HISTORY_LIMIT = 50;
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
  /** Gate-time MC (rug checks / display); final entry MC is captured after buy submit. */
  entryMc?: number;
  profitExitPercent?: number;
  fixedExitMc?: number;
  tradersListStr?: string;
  monitoredWallet?: string;
}

export interface InsiderSellTrigger {
  followedWallet: string;
  positionMint: string;
  signature: string;
  reason: string;
}

export interface InsiderTokenFlowEndedEvent {
  mint: string | null;
  feePayer: string | null;
  source: "follow" | "funder-first" | "follow-token" | null;
  hadPosition: boolean;
  reason: "reset" | "cycle_complete";
}

export interface InsiderBot {
  on(event: "buyTrigger", listener: (trigger: InsiderBuyTrigger) => void): this;
  on(
    event: "sellTrigger",
    listener: (trigger: InsiderSellTrigger) => void,
  ): this;
  on(event: "mintSeen", listener: (mint: string) => void): this;
  on(
    event: "tokenFlowEnded",
    listener: (event: InsiderTokenFlowEndedEvent) => void,
  ): this;
  on(
    event: "heliusCreditsExhausted",
    listener: (info: HeliusCreditExhaustionInfo) => void,
  ): this;
  on(event: "error", listener: (error: Error) => void): this;
  getActivePosition(): { followedWallet: string; mint: string } | null;
  getPreBuyMint(): string | null;
  markPositionBought(trigger: InsiderBuyTrigger): void;
  clearActivePosition(): void;
  clearActivePositionAfterSuccessfulSell(): void;
  rearmPositionMonitoringAfterSellFailure(mint: string): void;
  armPositionSellTrigger(mint: string): void;
  clearPreBuyMint(): void;
  getEntryMc(): number;
  getExitMc(): number;
  getStopLossMcPercent(): number;
  tryTriggerStopLossSell(currentMc: number): Promise<boolean>;
  tryTriggerRugMarketCapReset(currentMc: number): Promise<boolean>;
  isProfitExitDisabled(): boolean;
  shouldDeferFollowTokenEarlyBundlerMcTp(): boolean;
  notifyFollowTokenEarlyBundlerMcTpReached(currentMc: number): void;
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
  getFollowedWallets(): string[];
  getMonitoredWallet(): string | null;
  getBuySol(): number;
  isBuyDisabled(): boolean;
  setBuyDisabled(value: boolean): void;
  configureFollowWallet(address: string): void;
  removeFollowWallet(address: string): Promise<void>;
  pause(): void;
  stopForHeliusCredits(): Promise<void>;
  isStoppedForHeliusCredits(): boolean;
  isRunning(): boolean;
  isFollowWalletMonitoringActive(): boolean;
  isFollowWalletPaused(): boolean;
  pauseFollowWalletMonitoring(): Promise<void>;
  resumeFollowWalletMonitoring(): Promise<void>;
  isBuyInProgress(): boolean;
  setBuyExecuting(executing: boolean): void;
  isDevTokenOutBuyBlocked(mint: string): boolean;
  triggerDevTokenOutRecoverySell(mint: string, signature: string): void;
  resetBuyAttempt(): void;
  seedSeenMints(mints: Set<string>): void;
  followWallet(address: string): Promise<void>;
  addFollowWallet(
    address: string,
  ): Promise<{ ok: true } | { ok: false; error: string }>;
  setFollowTokenMigrationSuspendDelegate(
    delegate: ((mint: string) => void) | null,
  ): void;
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

/** Bundler first-buy record used when entering a token flow (follow-wallet or funder-first). */
export type FunderFirstEarlyBuy = EarlyInsiderBuy;

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
  /** True once a qualifying same-round SOL group (≥2 in 10s) is found. */
  normalTinyRoundGroupFound: boolean;
  /** One-shot: round 10s group reached buy threshold before cumulative dust skip fired. */
  roundWonDustRaceNotified: boolean;
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
  /** Wall-clock time (ms) the shared feePayer was locked. */
  lockedAt: number;
  /** Follow-token: Helius funded-by wallet for shared feePayer (parallel round/dust watch). */
  parallelFeePayerFunderWallet: string | null;
  parallelFeePayerFunderCursorSignature: string | null;
  parallelFeePayerFunderFundedAtSec: number | null;
}

export class InsiderBot extends EventEmitter {
  private readonly log: Logger;
  private readonly followWalletLog: Logger;
  private readonly followTokenWatchLog: Logger;
  private readonly config: ServiceConfig;
  private readonly connection: Connection;
  private readonly telegramBot: TelegramBot | null;
  private readonly heliusClient: HeliusClient;
  private readonly heliusClients: HeliusClient[] = [];
  private readonly heliusPool: HeliusPoolEntry[] = [];
  /** Enhanced WSS (transactionSubscribe) client — always keyed to the Developer-plan key (config.insiderHeliusApiKey), never a fallback pool entry, since transactionSubscribe requires a Developer+ plan Helius key and only that one is confirmed to be on it. Null (falls back to the pre-existing onLogs + REST fetch pattern everywhere) if that key isn't configured. */
  private readonly enhancedWs: HeliusEnhancedWsClient | null;
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

  private followedWallets: string[] = [];
  /** Follow wallet that started the current follow-sourced flow (may differ when delegated to another bot). */
  private flowFollowWallet: string | null = null;
  private followWalletFlowDelegate:
    | ((
        mint: string,
        signature: string,
        followedWallet: string,
      ) => Promise<boolean>)
    | null = null;
  private followTokenMigrationSuspendDelegate: ((mint: string) => void) | null =
    null;
  /** Whether the current/previous token flow was started from follow-wallet backtrack, funder-first discovery, or follow-token migration. */
  private flowSource: "follow" | "funder-first" | "follow-token" | null = null;
  /** FeePayer locked for the active funder-first flow — emitted on tokenFlowEnded so the orchestrator can cooldown/resume. */
  private funderFirstFeePayer: string | null = null;
  private buySol: number;
  private normalFundingBuySol: number;
  private lowFundingBuySol: number;
  private followToken16mPostLiBuySol: number;
  private entryMc: number;
  private exitMc: number;
  private exitPercent: number;
  private bundlerBuyMinUsd: number;
  private bundlerBuyMaxUsd: number;
  private requiredInsiderSells: number;
  private buyDisabled = false;

  private followMonitors = new Map<string, WalletMonitor>();
  /** User paused follow-wallet via Telegram — blocks auto-resume after token flow reset/complete. */
  private followWalletPaused = false;
  private followWalletTxNotifier: ((tx: HeliusTransaction) => void) | null = null;
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
  /** Set once a dev full-exit CLOSE_ACCOUNT tx has been acted on for the current token, so we don't re-trigger the reset/sell on the next poll tick. */
  private devFullExitHandled = false;
  /** Set when dev token-out is acted on for the current flow (pre-buy reset or post-buy sell). */
  private devTokenOutHandled = false;
  /** Mints blocked from buy completion after dev token-out (race recovery). */
  private devTokenOutBlockedMints = new Set<string>();
  /** Post-buy dev token-out watch expires at this timestamp (ms). */
  private devTokenOutWatchUntilMs: number | null = null;
  private devTokenOutPostBuyWatchTimer: ReturnType<typeof setTimeout> | null =
    null;
  /** Extra exit state used only by the 2s-4s follow-token migration path. */
  private fastMigrationSellMode:
    | "transfer_out_immediate"
    | "transfer_out_mc_retrace"
    | null = null;
  private fastMigrationTransferOutMc: number | null = null;
  /** Highest market cap observed for the current token across all pre-buy MC fetches — used to skip normal-mode buys that would already be past their own exit target. */
  private highestObservedMarketCapUsd: number | null = null;
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
  /** Enhanced WSS watch handles, parallel to the *LogsSubId/*LogsSubIds fields above — used instead of the onLogs+REST pattern whenever this.enhancedWs is available. */
  private insiderEnhancedWatchId: number | null = null;
  private bundlerEnhancedWatchIds = new Map<string, number>();
  private bundlerFunderEnhancedWatchId: number | null = null;
  private bundlerFunderParallelEnhancedWatchId: number | null = null;
  private devFullExitEnhancedWatchId: number | null = null;
  private recipientEnhancedWatchIds = new Map<string, number>();
  /** Per-recipient dedup for Enhanced WSS notifications — a reconnect can resubscribe and redeliver a signature already applied, and unlike the REST batch path (which consumes/clears dirty signatures as it goes) this push path has no other natural at-most-once guarantee. */
  private recipientEnhancedWatchSeenSignatures = new Map<string, Set<string>>();

  private processedSignatures = new Set<string>();
  private queuedSignatures = new Set<string>();
  private pendingSignaturesBatch: string[] = [];
  private batchTimer: ReturnType<typeof setTimeout> | null = null;
  private pollTimer: ReturnType<typeof setInterval> | null = null;
  /** Watched wallet for follow-token post-buy exit (Large Insider valid wallet path). */
  private followTokenTopBuyerWallet: string | null = null;
  private followTokenTopBuyerMint: string | null = null;
  private followTokenWatchMode: FollowTokenWatchMode | null = null;
  /** Post-buy watched-wallet exit reason (Large Insider valid wallet path). */
  private followTokenTopBuyerWatchReason: string | null = null;
  private followTokenTopBuyerEnhancedWatchId: number | null = null;
  private followTokenTopBuyerSeenSignatures = new Set<string>();
  private followTokenTopBuyerWatchConnectStartedAtSec: number | null = null;
  private followTokenTopBuyerWatchLastBackfillAtSec: number | null = null;
  private followTokenTopBuyerWatchBackfillInFlight = false;
  private followTokenTopBuyerWatchBackfillPending = false;
  /** Early bundler buys retained for follow-token large-insider feePayer backtrack. */
  private followTokenEarlyInsiderBuys: EarlyInsiderBuy[] | null = null;
  private followTokenLargeInsiderState: FollowTokenLargeInsiderState | null =
    null;
  private followTokenEarlyBundlerExitState: FollowTokenEarlyBundlerExitState | null =
    null;
  private followTokenLargeInsiderWindowTimer: ReturnType<typeof setTimeout> | null =
    null;
  private bundlerFunderWatch: BundlerFunderWatchState | null = null;
  private bundlerFunderLogsSubId: number | null = null;
  private bundlerFunderParallelLogsSubId: number | null = null;
  /** Active primary shared-feePayer subscription address (dedup vs parallel watch). */
  private bundlerFunderPrimaryWatchAddress: string | null = null;
  /** Active parallel feePayer-funder subscription address (dedup vs primary watch). */
  private bundlerFunderParallelWatchAddress: string | null = null;
  private lowFundingDevLogsSubId: number | null = null;
  /** Websocket log subscription on the dev wallet, used to push-detect a full-exit CLOSE_ACCOUNT tx (see subscribeDevWalletFullExitWatch). */
  private devFullExitLogsSubId: number | null = null;
  /** Native SOL balance subscription on the dev wallet — zero balance is treated as rug. */
  private devSolBalanceSubId: number | null = null;
  /** Dedup set of dev-wallet signatures already checked for the full-exit pattern, so a duplicate log notification doesn't trigger a redundant getTransactionsBySignatures call. */
  private devFullExitSeenSignatures = new Set<string>();
  private recipientLogsSubIds = new Map<string, number>();
  private recipientSolBalanceSubIds = new Map<string, number>();
  private isBundlerFunderSyncing = false;
  private bundlerFunderSyncPending = false;
  private bundlerFunderSyncPendingForce = false;
  private lastBundlerFunderSyncAt = 0;
  private isParallelFeePayerFunderSyncing = false;
  private lastParallelFeePayerFunderSyncAt = 0;
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
    enhancedWs: HeliusEnhancedWsClient | null = null,
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
    const enhancedWsApiKey = config.insiderHeliusApiKey || config.heliusApiKey;
    this.enhancedWs =
      enhancedWs ??
      (enhancedWsApiKey
        ? new HeliusEnhancedWsClient(enhancedWsApiKey, `${label} Enhanced WS`)
        : null);
    if (!this.enhancedWs) {
      createLogger(label.toUpperCase()).warn(
        "No Developer-plan Helius key configured (INSIDER_HELIUS_API_KEY); Enhanced WSS push disabled, falling back to onLogs + REST fetch everywhere",
      );
    }
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
    this.followWalletLog = createLogger('FOLLOW-WALLET');
    this.followTokenWatchLog = createLogger('FOLLOW-TOKEN-WATCH');
    this.buySol = config.insiderBuySol;
    this.normalFundingBuySol = config.insiderNormalBuySol;
    this.lowFundingBuySol = config.insiderLowFundingBuySol;
    this.followToken16mPostLiBuySol = config.insiderFollowToken16mPostLiBuySol;
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

  /** No-op when using the process-wide shared Enhanced WSS client. */
  closeEnhancedWs(): void {
    // Shared client is closed once in index.ts shutdown.
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

  clearActivePositionAfterSuccessfulSell(): void {
    void this.resetForNewToken(true);
  }

  armPositionSellTrigger(mint: string): void {
    if (!this.activePosition || this.activePosition.mint !== mint) return;
    this.positionSellTriggered = true;
  }

  rearmPositionMonitoringAfterSellFailure(mint: string): void {
    if (!this.activePosition || this.activePosition.mint !== mint) return;
    this.positionSellTriggered = false;
    this.phase = "holding";
    this.startPollLoop();
    void this.syncBundlerFunderTransactions(true);
    void this.syncParallelFeePayerFunderTransactions(true);
    void this.syncFunderRecipientBatch(true);
    if (
      this.followTokenEarlyInsiderBuys?.length &&
      !this.followTokenEarlyBundlerExitState?.active
    ) {
      void this.startFollowTokenEarlyBundlerExitMonitoring(mint);
    }
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

  setFollowToken16mFallbackBuySol(value: number): void {
    this.assertBuySol(value, "Insider 16M fallback buy SOL");
    this.followToken16mPostLiBuySol = value;
  }

  getFollowToken16mPostLiBuySol(): number {
    return this.followToken16mPostLiBuySol;
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

  getStopLossMcPercent(): number {
    return 0;
  }

  async tryTriggerStopLossSell(_currentMc: number): Promise<boolean> {
    return false;
  }

  async tryTriggerRugMarketCapReset(currentMc: number): Promise<boolean> {
    const mint = this.watchingMint ?? this.activePosition?.mint;
    if (mint) {
      this.recordObservedMarketCapUsd(currentMc);
    }
    if (currentMc >= INSIDER_RUG_RESET_MARKET_CAP_USD) return false;
    if (!mint) return false;
    await this.handleDevWalletRugSignal(mint, {
      kind: "mc_floor",
      currentMc,
    });
    return true;
  }

  isProfitExitDisabled() {
    return this.profitExitDisabled;
  }

  shouldDeferFollowTokenEarlyBundlerMcTp(): boolean {
    const state = this.followTokenEarlyBundlerExitState;
    if (
      !state?.active ||
      state.allSoldAllComplete ||
      state.exitTriggerSignature
    ) {
      return false;
    }
    if (this.phase !== "holding" || !this.activePosition) return false;
    return (
      state.watches.size > 0 &&
      !this.allFollowTokenEarlyBundlerExitWatchesSoldAll()
    );
  }

  notifyFollowTokenEarlyBundlerMcTpReached(currentMc: number): void {
    const state = this.followTokenEarlyBundlerExitState;
    if (!state?.active || state.mcTpReachedPending) return;
    state.mcTpReachedPending = true;
    this.log.warn(
      "Follow-token early bundler MC TP reached — deferring until all bundlers sold all",
      {
        mint: state.mint,
        currentMc,
        exitMc: this.exitMc,
        watchedWallets: [...state.watches.keys()],
      },
    );
    void this.sendTelegramSafe(
      [
        `<b>⏳ ${this.label} Early Bundler MC TP Deferred</b>`,
        `Token: <code>${state.mint}</code>`,
        `Current MC: <b>$${currentMc.toLocaleString()}</b>`,
        `Target: <b>+${FOLLOW_TOKEN_LARGE_INSIDER_PROFIT_EXIT_PERCENT}% MC TP</b>`,
        "",
        "MC target reached. Waiting for every early bundler / transfer recipient to sell all holdings before exiting.",
      ].join("\n"),
      "follow-token early bundler mc tp deferred",
    );
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
    return this.followedWallets[0] ?? null;
  }

  getFollowedWallets(): string[] {
    return [...this.followedWallets];
  }

  isFollowWallet(address: string): boolean {
    try {
      return this.followedWallets.includes(new PublicKey(address).toBase58());
    } catch {
      return false;
    }
  }

  /** Follow wallet tied to the active follow-sourced token flow (supports delegated flows). */
  private getFlowFollowWallet(): string | null {
    return this.flowFollowWallet ?? this.followedWallets[0] ?? null;
  }

  /** Wallet label stored on buyTrigger/activePosition when no follow wallet is set (follow-token). */
  private getBuyTriggerFollowedWallet(
    state?: BundlerFunderWatchState | null,
  ): string {
    return this.getFlowFollowWallet() ?? state?.funderWallet ?? "follow-token";
  }

  private formatFollowWalletTelegramLine(): string {
    const wallet = this.getFlowFollowWallet();
    if (!wallet || this.flowSource !== "follow") return "";
    return `Follow wallet: <code>${wallet}</code>`;
  }

  private followWalletBackend(
    message: string,
    data?: Record<string, unknown>,
  ): void {
    if (!this.config.insiderFollowWalletVerboseLogs) return;
    if (data) this.followWalletLog.info(message, data);
    else this.followWalletLog.info(message);
  }

  private followTokenTopBuyerWatchBackend(
    message: string,
    data?: Record<string, unknown>,
  ): void {
    if (data) this.followTokenWatchLog.info(message, data);
    else this.followTokenWatchLog.info(message);
  }

  private ensureFollowTokenTopBuyerWatchSubscribed(): void {
    this.syncFollowTokenTopBuyerWatch();
  }

  private markFollowTokenTopBuyerWatchConnectStart(): void {
    const nowSec = Math.floor(Date.now() / 1000);
    this.followTokenTopBuyerWatchConnectStartedAtSec =
      nowSec - FOLLOW_TOKEN_TOP_BUYER_WATCH_BACKFILL_BUFFER_SEC;
    this.followTokenTopBuyerWatchLastBackfillAtSec = null;
  }

  private getFollowTokenTopBuyerWatchedWallets(): string[] {
    if (
      this.followTokenTopBuyerEnhancedWatchId !== null &&
      this.followTokenTopBuyerWallet
    ) {
      return [this.followTokenTopBuyerWallet];
    }
    return [];
  }

  private scheduleFollowTokenTopBuyerWatchBackfill(
    wallets: readonly string[],
    reason: string,
  ): void {
    const uniqueWallets = [...new Set(wallets.filter(Boolean))];
    if (uniqueWallets.length === 0) return;
    void this.runFollowTokenTopBuyerWatchBackfill(uniqueWallets, reason);
  }

  private async runFollowTokenTopBuyerWatchBackfill(
    wallets: string[],
    reason: string,
  ): Promise<void> {
    const mint = this.followTokenTopBuyerMint;
    if (!mint || wallets.length === 0) return;

    if (this.followTokenTopBuyerWatchBackfillInFlight) {
      this.followTokenTopBuyerWatchBackfillPending = true;
      return;
    }

    const floorSec =
      this.followTokenTopBuyerWatchLastBackfillAtSec !== null
        ? this.followTokenTopBuyerWatchLastBackfillAtSec - 2
        : this.followTokenTopBuyerWatchConnectStartedAtSec ??
          Math.floor(Date.now() / 1000) -
            FOLLOW_TOKEN_TOP_BUYER_WATCH_BACKFILL_BUFFER_SEC;

    this.followTokenTopBuyerWatchBackfillInFlight = true;
    try {
      const collected: Array<{ wallet: string; tx: HeliusTransaction }> = [];
      for (const wallet of wallets) {
        let txs: HeliusTransaction[];
        try {
          txs = await this.withHeliusFallback((client) =>
            client.getWalletTransactionsDesc(
              wallet,
              FOLLOW_TOKEN_TOP_BUYER_WATCH_BACKFILL_LIMIT,
            ),
          );
        } catch (err) {
          void this.heliusClient.handlePossibleRateLimitError(err);
          this.followTokenWatchLog.warn(
            "Follow-token top-buyer watch backfill fetch failed",
            {
              mint,
              wallet,
              reason,
              error: err instanceof Error ? err.message : String(err),
            },
          );
          continue;
        }

        for (const tx of txs) {
          if (tx.timestamp < floorSec) continue;
          if (!this.isRelevantMintTx(tx, mint)) continue;
          const action = this.classifyTx(tx, wallet, mint);
          if (action !== "buy" && action !== "sell") continue;
          collected.push({ wallet, tx });
        }
      }

      collected.sort(
        (a, b) =>
          a.tx.timestamp - b.tx.timestamp ||
          a.tx.signature.localeCompare(b.tx.signature),
      );

      this.followTokenTopBuyerWatchBackend(
        "Follow-token top-buyer watch backfill processing",
        {
          mint,
          reason,
          floorSec,
          walletCount: wallets.length,
          txCount: collected.length,
        },
      );

      for (const { wallet, tx } of collected) {
        if (this.followTokenTopBuyerMint !== mint) break;
        await this.handleFollowTokenTopBuyerTransaction(tx, wallet);
      }

      this.followTokenTopBuyerWatchLastBackfillAtSec = Math.floor(
        Date.now() / 1000,
      );
    } finally {
      this.followTokenTopBuyerWatchBackfillInFlight = false;
      if (this.followTokenTopBuyerWatchBackfillPending) {
        this.followTokenTopBuyerWatchBackfillPending = false;
        const retryWallets = this.getFollowTokenTopBuyerWatchedWallets();
        if (retryWallets.length > 0) {
          void this.runFollowTokenTopBuyerWatchBackfill(
            retryWallets,
            "pending_retry",
          );
        }
      }
    }
  }

  private syncFollowTokenTopBuyerWatch(): void {
    const wallet = this.followTokenTopBuyerWallet;
    const mint = this.followTokenTopBuyerMint;
    if (!wallet || !mint) return;

    const prevSingleWatchId = this.followTokenTopBuyerEnhancedWatchId;
    this.ensureFollowTokenTopBuyerSingleWatchSubscribed();

    const watchedWallets = this.getFollowTokenTopBuyerWatchedWallets();
    const watchChanged =
      prevSingleWatchId !== this.followTokenTopBuyerEnhancedWatchId;
    if (watchedWallets.length > 0) {
      this.scheduleFollowTokenTopBuyerWatchBackfill(
        watchedWallets,
        watchChanged ? "watch_subscribed" : "watch_resync",
      );
    }
  }

  private async resubscribeFollowTokenTopBuyerWatch(
    wallet: string,
    mint: string,
    watchMode: FollowTokenWatchMode,
    reason: string,
  ): Promise<void> {
    await this.stopFollowTokenTopBuyerSingleWatch();
    this.followTokenTopBuyerWallet = wallet;
    this.followTokenTopBuyerMint = mint;
    this.followTokenWatchMode = watchMode;
    this.syncFollowTokenTopBuyerWatch();
    this.followTokenTopBuyerWatchBackend("Follow-token watch re-subscribed", {
      mint,
      wallet,
      watchMode,
      reason,
    });
  }

  private ensureFollowTokenTopBuyerSingleWatchSubscribed(): void {
    const wallet = this.followTokenTopBuyerWallet;
    const mint = this.followTokenTopBuyerMint;
    if (!wallet || !mint) return;
    if (this.followTokenTopBuyerEnhancedWatchId !== null) return;

    if (!this.enhancedWs) {
      this.followTokenWatchLog.warn(
        "Follow-token top-buyer watch requires Enhanced WSS; watch not started",
        { wallet, mint },
      );
      return;
    }

    this.markFollowTokenTopBuyerWatchConnectStart();
    this.followTokenTopBuyerEnhancedWatchId = this.enhancedWs.watch(
      wallet,
      (tx) => {
        void this.handleFollowTokenTopBuyerTransaction(tx, wallet);
      },
    );
    this.followTokenTopBuyerWatchBackend("Follow-token top-buyer watch subscribed", {
      wallet,
      mint,
      watchMode: this.followTokenWatchMode,
    });
  }

  private async stopFollowTokenTopBuyerSingleWatch(): Promise<void> {
    if (this.followTokenTopBuyerEnhancedWatchId === null) return;
    const id = this.followTokenTopBuyerEnhancedWatchId;
    this.followTokenTopBuyerEnhancedWatchId = null;
    await this.enhancedWs?.unwatch(id).catch(() => undefined);
  }

  private async stopFollowTokenTopBuyerWatch(reason: string): Promise<void> {
    await this.stopFollowTokenTopBuyerSingleWatch();
    this.followTokenTopBuyerSeenSignatures.clear();
    this.followTokenTopBuyerWallet = null;
    this.followTokenTopBuyerMint = null;
    this.followTokenWatchMode = null;
    this.followTokenTopBuyerWatchReason = null;
    this.followTokenTopBuyerWatchConnectStartedAtSec = null;
    this.followTokenTopBuyerWatchLastBackfillAtSec = null;
    this.followTokenTopBuyerWatchBackfillInFlight = false;
    this.followTokenTopBuyerWatchBackfillPending = false;
    this.followTokenTopBuyerWatchBackend("Follow-token top-buyer watch stopped", {
      reason,
    });
  }

  private async triggerFollowTokenWatchExitSell(
    mint: string,
    wallet: string,
    action: "buy" | "sell",
    tx: HeliusTransaction,
    context: string,
  ): Promise<void> {
    if (
      !this.activePosition ||
      this.activePosition.mint !== mint ||
      this.phase !== "holding" ||
      this.positionSellTriggered
    ) {
      return;
    }

    const actionLabel = action === "sell" ? "sell" : "buy";
    await this.triggerPositionSell(
      mint,
      `Follow-token watched wallet ${wallet} ${actionLabel} on ${mint} (${context})`,
      [
        `<b>🚨 ${this.label} Follow-Token Watch Exit</b>`,
        `Token: <code>${mint}</code>`,
        `Watched wallet: <code>${wallet}</code>`,
        `Watch mode: <b>${this.followTokenWatchMode ?? "unknown"}</b>`,
        `Trigger: <b>${actionLabel}</b> tx`,
        `Tx: <code>${tx.signature}</code>`,
        "",
        `Selling full position (${context}).`,
      ],
      tx.signature,
    );
    if (
      this.followTokenLargeInsiderState?.active &&
      !this.followTokenLargeInsiderState.validWallets.length
    ) {
      await this.stopFollowTokenLargeInsiderFlow(
        "post-buy exit before valid large insider wallet",
      );
    }
  }

  private async resetFollowTokenAfterLargeInsiderStartFailed(
    mint: string,
    triggerReason: string,
    options?: { skipTelegram?: boolean },
  ): Promise<void> {
    this.followTokenTopBuyerWatchBackend(
      "Large Insider failed to start — resetting follow-token flow",
      { mint, triggerReason },
    );
    await this.stopFollowTokenTopBuyerWatch("large insider flow failed to start");
    await this.resetForNewToken(false, {
      reason: "large_insider_feePayer_lock_failed",
      skipTelegram: options?.skipTelegram,
    });
  }

  private clearFollowTokenLargeInsiderWindowTimer(): void {
    if (this.followTokenLargeInsiderWindowTimer) {
      clearTimeout(this.followTokenLargeInsiderWindowTimer);
      this.followTokenLargeInsiderWindowTimer = null;
    }
  }

  private scheduleFollowTokenLargeInsiderWindowCloseCheck(): void {
    this.clearFollowTokenLargeInsiderWindowTimer();
    const li = this.followTokenLargeInsiderState;
    if (!li?.active) return;

    const nowSec = Math.floor(Date.now() / 1000);
    const delayMs = Math.max(0, (li.feePayerWindowEndsAt - nowSec) * 1_000);
    this.followTokenLargeInsiderWindowTimer = setTimeout(() => {
      this.followTokenLargeInsiderWindowTimer = null;
      void this.maybeResetFollowTokenLargeInsiderWindowClosedBeforeBuy();
    }, delayMs);
  }

  private isFollowTokenLargeInsiderFeePayerWindowClosed(): boolean {
    const li = this.followTokenLargeInsiderState;
    if (!li?.active) return false;
    return Math.floor(Date.now() / 1000) >= li.feePayerWindowEndsAt;
  }

  private async maybeResetFollowTokenLargeInsiderWindowClosedBeforeBuy(): Promise<void> {
    const li = this.followTokenLargeInsiderState;
    if (!li?.active || this.flowSource !== "follow-token") return;
    if (this.buySubmitted || this.activePosition) return;
    if (li.validWallets.length >= 1) {
      return;
    }
    if (!this.isFollowTokenLargeInsiderFeePayerWindowClosed()) {
      // The timeout can fire just before the epoch-second boundary. Keep the
      // close check armed instead of leaving the pre-buy flow without a timer.
      this.scheduleFollowTokenLargeInsiderWindowCloseCheck();
      return;
    }

    const resetReason = "large_insider_window_closed_before_fourth_valid_wallet";
    this.followTokenLargeInsiderLog(
      "feePayer window closed before 4 valid wallets — resetting follow-token flow",
      {
        mint: li.mint,
        reason: resetReason,
        validWalletCount: li.validWallets.length,
        bundlerFirstBuyAnchorTimestamp: li.bundlerFirstBuyAnchorTimestamp,
        feePayerWindowEndsAt: li.feePayerWindowEndsAt,
      },
    );
    await this.stopFollowTokenLargeInsiderFlow(resetReason);
    await this.resetForNewToken(false, { reason: resetReason });
  }

  private passesFollowTokenLargeInsiderFirstFourQualifiedSolBuyGate(): boolean {
    const li = this.followTokenLargeInsiderState;
    if (!li) return false;
    const firstFour = li.validWallets.slice(
      0,
      FOLLOW_TOKEN_LARGE_INSIDER_BUY_AT_VALID_WALLET_COUNT,
    );
    if (firstFour.length < FOLLOW_TOKEN_LARGE_INSIDER_BUY_AT_VALID_WALLET_COUNT) {
      return false;
    }
    return firstFour.some((validWallet) => {
      const scrapeWatch = li.scrapeWatches.get(validWallet);
      return (
        scrapeWatch !== undefined &&
        scrapeWatch.qualifiedReceivedSol <
          FOLLOW_TOKEN_LARGE_INSIDER_BUY_REQUIRES_ONE_QUALIFIED_SOL_BELOW
      );
    });
  }

  private summarizeFollowTokenLargeInsiderFirstFourQualifiedSol(): Array<{
    index: number;
    wallet: string;
    qualifiedSol: number | null;
  }> {
    const li = this.followTokenLargeInsiderState;
    if (!li) return [];
    return li.validWallets
      .slice(0, FOLLOW_TOKEN_LARGE_INSIDER_BUY_AT_VALID_WALLET_COUNT)
      .map((validWallet, index) => ({
        index: index + 1,
        wallet: validWallet,
        qualifiedSol: li.scrapeWatches.get(validWallet)?.qualifiedReceivedSol ?? null,
      }));
  }

  private async resetFollowTokenAfterLargeInsiderQualifiedSolBuyGateFailed(
    mint: string,
  ): Promise<void> {
    const summary = this.summarizeFollowTokenLargeInsiderFirstFourQualifiedSol();
    this.followTokenLargeInsiderLog(
      "buy gate failed — all first four valid wallets have Qualified SOL ≥20",
      {
        mint,
        requiredOneBelowSol:
          FOLLOW_TOKEN_LARGE_INSIDER_BUY_REQUIRES_ONE_QUALIFIED_SOL_BELOW,
        wallets: summary,
      },
    );
    void this.sendTelegramSafe(
      [
        `<b>⛔ ${this.label} Follow-Token Large Insider Buy Skipped</b>`,
        `Token: <code>${mint}</code>`,
        `Valid wallet #4 found, but buy gate failed.`,
        `Need ≥1 of first <b>${FOLLOW_TOKEN_LARGE_INSIDER_BUY_AT_VALID_WALLET_COUNT}</b> valid wallets with Qualified SOL <b>&lt;${FOLLOW_TOKEN_LARGE_INSIDER_BUY_REQUIRES_ONE_QUALIFIED_SOL_BELOW}</b> SOL.`,
        "",
        ...summary.map(
          ({ index, wallet, qualifiedSol }) =>
            `${index}. <code>${wallet}</code> — Qualified SOL: <b>${qualifiedSol !== null ? qualifiedSol.toFixed(4) : "?"}</b>`,
        ),
      ].join("\n"),
      "follow-token large insider qualified sol buy gate failed",
    );
    await this.stopFollowTokenLargeInsiderFlow(
      "large insider qualified SOL buy gate failed",
    );
    await this.resetForNewToken(false, {
      reason: "large_insider_qualified_sol_buy_gate_failed",
    });
  }

  private passesFollowTokenPostLiBundlerQualifiedSolBuyGate(): boolean {
    const li = this.followTokenLargeInsiderState;
    if (!li?.active || li.validWallets.length === 0) return false;
    return li.validWallets.some((validWallet) => {
      const scrapeWatch = li.scrapeWatches.get(validWallet);
      return (
        scrapeWatch !== undefined &&
        scrapeWatch.qualifiedReceivedSol <
          FOLLOW_TOKEN_POST_LI_BUNDLER_BUY_REQUIRES_ONE_QUALIFIED_SOL_BELOW
      );
    });
  }

  private summarizeFollowTokenPresentQualifiedSol(): Array<{
    index: number;
    wallet: string;
    qualifiedSol: number | null;
  }> {
    const li = this.followTokenLargeInsiderState;
    if (!li) return [];
    return li.validWallets.map((validWallet, index) => ({
      index: index + 1,
      wallet: validWallet,
      qualifiedSol: li.scrapeWatches.get(validWallet)?.qualifiedReceivedSol ?? null,
    }));
  }

  private async stopFollowTokenLargeInsiderFlow(reason: string): Promise<void> {
    const state = this.followTokenLargeInsiderState;
    if (!state?.active) return;

    this.clearFollowTokenLargeInsiderWindowTimer();

    for (const wallet of [...state.scrapeEnhancedWatchIds.keys()]) {
      this.unsubscribeFollowTokenLargeInsiderScrapeWallet(wallet);
    }

    this.followTokenLargeInsiderLog(reason, {
      mint: state.mint,
      validWallets: state.validWallets,
      exitOverrideActive: state.exitOverrideActive,
    });

    this.followTokenLargeInsiderState = null;

    const funderState = this.bundlerFunderWatch;
    if (
      funderState &&
      this.flowSource === "follow-token" &&
      !this.buySubmitted
    ) {
      funderState.discoveryStopped = true;
    }
  }

  private followTokenLargeInsiderLog(
    message: string,
    meta?: Record<string, unknown>,
  ): void {
    this.log.warn(`Follow-token large insider: ${message}`, meta ?? {});
  }

  private sendFollowTokenLargeInsiderFlowTelegram(
    outcome: "started" | "feePayer_lock_failed",
    mint: string,
    triggerReason: string,
    options: {
      feePayer?: string | null;
      bundlerFirstBuyAnchorTimestamp?: number;
      feePayerWindowEndsAt?: number;
      secondGroupWalletCount?: number;
    } = {},
  ): void {
    if (outcome === "started") {
      void this.sendTelegramSafe(
        [
          `<b>🔎 ${this.label} Follow-Token Large Insider Flow Started</b>`,
          `Token: <code>${mint}</code>`,
          `Trigger: <code>${triggerReason}</code>`,
          options.feePayer
            ? `FeePayer: <code>${options.feePayer}</code>`
            : "",
          options.bundlerFirstBuyAnchorTimestamp !== undefined &&
          options.feePayerWindowEndsAt !== undefined
            ? `Window: first <b>${FOLLOW_TOKEN_LARGE_INSIDER_FEEPAYER_WINDOW_SEC / 60}m</b> after initial bundler first buy · ≥<b>${FOLLOW_TOKEN_LARGE_INSIDER_MIN_FEEPAYER_OUT_SOL} SOL</b> outs`
            : "",
          `Chain: each scrape wallet may spawn ≤${FOLLOW_TOKEN_LARGE_INSIDER_MAX_CHILDREN_PER_WALLET} downstream watches only on **≥${FOLLOW_TOKEN_LARGE_INSIDER_MIN_CHAIN_OUT_SOL} SOL** outs (tier1 → chain → chain…)`,
          options.secondGroupWalletCount !== undefined
            ? `Initial bundlers: <b>${options.secondGroupWalletCount}</b>`
            : "",
          `Buy on valid wallet <b>#${FOLLOW_TOKEN_LARGE_INSIDER_BUY_AT_VALID_WALLET_COUNT}</b> or early bundler sold-all path (up to <b>${FOLLOW_TOKEN_LARGE_INSIDER_MAX_VALID_WALLETS}</b> LI tracked · first buy &gt; $${FOLLOW_TOKEN_LARGE_INSIDER_VALID_WALLET_FIRST_BUY_MIN_USD} · +${FOLLOW_TOKEN_LARGE_INSIDER_PROFIT_EXIT_PERCENT}% MC TP · any valid wallet ≥25% sell early exit).`,
        ]
          .filter(Boolean)
          .join("\n"),
        "follow-token large insider flow started",
      );
      return;
    }

    void this.sendTelegramSafe(
      [
        `<b>⚠️ ${this.label} Follow-Token Large Insider Flow Failed To Start</b>`,
        `Token: <code>${mint}</code>`,
        `Trigger: <code>${triggerReason}</code>`,
        "Reason: <b>shared feePayer lock failed</b> (need ≥3/4 bundlers funded by same feePayer).",
      ].join("\n"),
      "follow-token large insider flow failed to start",
    );
  }

  private buildFollowTokenStubSecondGroupFromInitialBundlers(
    state: BundlerFunderWatchState,
  ): FollowTokenBundlerAnchorGroup {
    const earlyBuys = this.followTokenEarlyInsiderBuys;
    const wallets = earlyBuys?.length
      ? earlyBuys.map((buy) => buy.wallet)
      : [...state.bundlerWallets];
    const anchorTimestamp = earlyBuys?.length
      ? Math.min(...earlyBuys.map((buy) => buy.timestamp))
      : (this.devCreateTimestamp ?? Math.floor(Date.now() / 1_000));
    return { anchorTimestamp, wallets };
  }

  private resolveFollowTokenLargeInsiderBundlerFirstBuyAnchorTimestamp(): number {
    const earlyBuys = this.followTokenEarlyInsiderBuys;
    if (earlyBuys?.length) {
      return Math.min(
        ...earlyBuys
          .slice(0, BUNDLER_FUNDER_REQUIRED_COUNT)
          .map((buy) => buy.timestamp),
      );
    }
    const watch = this.bundlerFunderWatch;
    if (watch?.earliestFundingTimestamp && watch.earliestFundingTimestamp > 0) {
      return watch.earliestFundingTimestamp;
    }
    return this.devCreateTimestamp ?? Math.floor(Date.now() / 1_000);
  }

  private async startFollowTokenLargeInsiderFlow(
    funderState: BundlerFunderWatchState,
    secondGroup: FollowTokenBundlerAnchorGroup,
    triggerReason: string,
  ): Promise<boolean> {
    if (
      this.followTokenLargeInsiderState?.active &&
      this.followTokenLargeInsiderState.mint === funderState.mint
    ) {
      return true;
    }

    const locked = await this.ensureFollowTokenLargeInsiderFeePayerLocked(
      funderState.mint,
    );
    if (!locked) {
      this.followTokenLargeInsiderLog(
        "feePayer lock failed — large insider flow not started",
        { mint: funderState.mint, triggerReason },
      );
      this.sendFollowTokenLargeInsiderFlowTelegram(
        "feePayer_lock_failed",
        funderState.mint,
        triggerReason,
      );
      return false;
    }

    const watchState = this.bundlerFunderWatch;
    if (!watchState || watchState.mint !== funderState.mint) return false;

    const anchorTimestamp =
      this.resolveFollowTokenLargeInsiderBundlerFirstBuyAnchorTimestamp();
    this.followTokenLargeInsiderState = {
      mint: funderState.mint,
      active: true,
      triggerReason,
      secondGroup,
      tier1FeePayerRecipients: new Set<string>(),
      scrapeWatches: new Map<string, FollowTokenLargeInsiderScrapeWatch>(),
      validWallets: [],
      validWalletSearchComplete: false,
      exitTriggerSignature: null,
      bundlerFirstBuyAnchorTimestamp: anchorTimestamp,
      feePayerWindowEndsAt:
        anchorTimestamp + FOLLOW_TOKEN_LARGE_INSIDER_FEEPAYER_WINDOW_SEC,
      exitOverrideActive: false,
      scrapeEnhancedWatchIds: new Map<string, number>(),
      scrapeSolBalanceSubIds: new Map<string, number>(),
      seenFeePayerOutSignatures: new Set<string>(),
      firstBuyBelowMinUsdWallets: new Map(),
    };

    watchState.discoveryStopped = false;
    this.subscribeBundlerFunder(watchState.funderWallet);
    await this.syncFollowTokenLargeInsiderFeePayerTransactions();

    this.followTokenLargeInsiderLog("flow started", {
      mint: funderState.mint,
      triggerReason,
      bundlerFirstBuyAnchorTimestamp: anchorTimestamp,
      feePayerWindowEndsAt:
        anchorTimestamp + FOLLOW_TOKEN_LARGE_INSIDER_FEEPAYER_WINDOW_SEC,
      secondGroupWallets: secondGroup.wallets.length,
    });

    this.sendFollowTokenLargeInsiderFlowTelegram(
      "started",
      funderState.mint,
      triggerReason,
      {
        feePayer: watchState.funderWallet,
        bundlerFirstBuyAnchorTimestamp: anchorTimestamp,
        feePayerWindowEndsAt:
          anchorTimestamp + FOLLOW_TOKEN_LARGE_INSIDER_FEEPAYER_WINDOW_SEC,
        secondGroupWalletCount: secondGroup.wallets.length,
      },
    );
    this.scheduleFollowTokenLargeInsiderWindowCloseCheck();
    return true;
  }

  private async ensureFollowTokenLargeInsiderFeePayerLocked(
    mint: string,
  ): Promise<boolean> {
    const earlyBuys = this.followTokenEarlyInsiderBuys;
    const existing = this.bundlerFunderWatch;
    if (
      existing &&
      existing.mint === mint &&
      existing.earliestFundingTimestamp > 0 &&
      existing.funderWallet !== mint &&
      !existing.bundlerWallets.has(existing.funderWallet)
    ) {
      return true;
    }
    if (!earlyBuys || earlyBuys.length < BUNDLER_FUNDER_REQUIRED_COUNT) {
      return false;
    }

    const firstFour = earlyBuys.slice(0, BUNDLER_FUNDER_REQUIRED_COUNT);
    let fundingRecords: Array<BundlerFundingRecord | null> = [];
    for (
      let attempt = 1;
      attempt <= BUNDLER_FUNDER_FUNDING_RECORD_ATTEMPTS;
      attempt += 1
    ) {
      const resolved = await this.resolveBundlerFundingRecordsSequential(
        mint,
        firstFour,
      );
      if (resolved === null) return false;
      fundingRecords = resolved;
      if (fundingRecords.every((record) => record !== null)) break;
      if (attempt < BUNDLER_FUNDER_FUNDING_RECORD_ATTEMPTS) {
        await new Promise((resolve) =>
          setTimeout(resolve, BUNDLER_FUNDER_FUNDING_RECORD_RETRY_DELAY_MS),
        );
      }
    }
    if (fundingRecords.some((record) => !record)) return false;

    const allRecords = fundingRecords as BundlerFundingRecord[];
    const feePayerGroups = new Map<string, BundlerFundingRecord[]>();
    for (const record of allRecords) {
      const group = feePayerGroups.get(record.fundingFeePayer);
      if (group) group.push(record);
      else feePayerGroups.set(record.fundingFeePayer, [record]);
    }
    const majorityGroup = [...feePayerGroups.values()].reduce((best, group) =>
      group.length > best.length ? group : best,
    );
    if (majorityGroup.length < BUNDLER_FUNDER_MIN_MATCHING_FEEPAYER_COUNT) {
      return false;
    }

    const earliest = majorityGroup.reduce((best, record) =>
      record.timestamp < best.timestamp ? record : best,
    );
    const latest = majorityGroup.reduce((best, record) =>
      record.timestamp > best.timestamp ? record : best,
    );
    const funderWallet = majorityGroup[0]!.fundingFeePayer;
    const largestFundingSol = Math.max(
      ...majorityGroup.map((record) => record.amountSol),
    );

    const base =
      existing && existing.mint === mint
        ? existing
        : this.buildFollowTokenStubBundlerWatch(mint, firstFour);

    this.bundlerFunderWatch = {
      ...base,
      funderWallet,
      originalFunderWallet: funderWallet,
      lowFundingMode: false,
      earliestFundingTimestamp: earliest.timestamp,
      earliestFundingSignature: earliest.fundingSignature,
      largestFundingSol,
      cursorSignature: latest.fundingSignature,
      processedSignatures: new Set(
        majorityGroup.map((record) => record.fundingSignature),
      ),
      discoveryStopped: false,
      lockedAt: Date.now(),
    };
    return true;
  }

  private async resolveFollowTokenLargeInsiderLastBundlerFundingSignature(
    state: BundlerFunderWatchState,
  ): Promise<string | null> {
    const earlyBuys = this.followTokenEarlyInsiderBuys;
    if (earlyBuys && earlyBuys.length >= BUNDLER_FUNDER_REQUIRED_COUNT) {
      const firstFour = earlyBuys.slice(0, BUNDLER_FUNDER_REQUIRED_COUNT);
      const resolved = await this.resolveBundlerFundingRecordsSequential(
        state.mint,
        firstFour,
      );
      if (resolved) {
        const records = resolved.filter(
          (record): record is BundlerFundingRecord => record !== null,
        );
        if (records.length > 0) {
          const latest = records.reduce((best, record) =>
            record.timestamp > best.timestamp ? record : best,
          );
          return latest.fundingSignature;
        }
      }
    }
    if (state.cursorSignature) return state.cursorSignature;
    return state.earliestFundingSignature || null;
  }

  /** Large Insider start only: REST backfill from last bundler-funding tx (after-signature), up to 100 txs. */
  private async syncFollowTokenLargeInsiderFeePayerTransactions(): Promise<void> {
    const state = this.bundlerFunderWatch;
    if (!state || !this.followTokenLargeInsiderState?.active) return;
    if (state.discoveryStopped) return;

    const afterSignature =
      await this.resolveFollowTokenLargeInsiderLastBundlerFundingSignature(state);
    if (!afterSignature) {
      this.followTokenLargeInsiderLog(
        "feePayer sync skipped — no bundler funding anchor signature",
        { mint: state.mint },
      );
      return;
    }

    const syncingWallet = state.funderWallet;
    try {
      const txs = await this.withHeliusFallback((client) =>
        client.getAddressTransactionsAsc(
          syncingWallet,
          afterSignature,
          FOLLOW_TOKEN_LARGE_INSIDER_FEEPAYER_SYNC_LIMIT,
        ),
      );
      let inspected = 0;
      for (const tx of txs) {
        if (state.funderWallet !== syncingWallet) break;
        if (state.processedSignatures.has(tx.signature)) continue;
        state.processedSignatures.add(tx.signature);
        state.cursorSignature = tx.signature;
        const migrated = await this.inspectBundlerFunderTransaction(state, tx);
        inspected += 1;
        if (migrated) break;
        if (state.discoveryStopped || !this.bundlerFunderWatch) break;
      }
      this.followTokenLargeInsiderLog("feePayer REST sync completed", {
        mint: state.mint,
        funderWallet: syncingWallet,
        afterSignature,
        fetchedTxCount: txs.length,
        inspectedTxCount: inspected,
        syncLimit: FOLLOW_TOKEN_LARGE_INSIDER_FEEPAYER_SYNC_LIMIT,
      });
    } catch (err) {
      void this.heliusClient.handlePossibleRateLimitError(err);
      this.followTokenLargeInsiderLog("feePayer REST sync failed", {
        mint: state.mint,
        funderWallet: syncingWallet,
        afterSignature,
        error: err instanceof Error ? err.message : String(err),
      });
    }
  }

  private isFollowTokenLargeInsiderFeePayerWindowOpen(
    txTimestamp: number,
  ): boolean {
    const li = this.followTokenLargeInsiderState;
    if (!li?.active) return false;
    return (
      txTimestamp >= li.bundlerFirstBuyAnchorTimestamp &&
      txTimestamp <= li.feePayerWindowEndsAt
    );
  }

  private isFollowTokenLargeInsiderSharedFeePayer(wallet: string): boolean {
    const state = this.bundlerFunderWatch;
    if (!state) return false;
    return wallet === state.originalFunderWallet;
  }

  private recordFollowTokenLargeInsiderScrapeSolTransferOut(
    watch: FollowTokenLargeInsiderScrapeWatch,
    tx: HeliusTransaction,
    wallet: string,
    funderState: BundlerFunderWatchState,
  ): { to: string; amountSol: number } | null {
    const transferOut = this.extractSolTransferOutFromWallet(tx, wallet, 0);
    if (!transferOut || this.hasSolIncomingToWallet(tx, wallet)) return null;

    watch.lastSolTransferOutTo = transferOut.to;

    if (this.isFollowTokenLargeInsiderSharedFeePayer(transferOut.to)) {
      this.followTokenLargeInsiderLog(
        "scrape wallet SOL return to feePayer — ignored for chain spawn",
        {
          mint: funderState.mint,
          wallet,
          feePayer: transferOut.to,
          amountSol: transferOut.amountSol,
          signature: tx.signature,
        },
      );
    }

    return transferOut;
  }

  private async handleFollowTokenLargeInsiderFeePayerTransferOut(
    state: BundlerFunderWatchState,
    tx: HeliusTransaction,
    transferOut: { to: string; amountSol: number },
  ): Promise<void> {
    const li = this.followTokenLargeInsiderState;
    if (!li?.active || li.mint !== state.mint) return;
    if (li.seenFeePayerOutSignatures.has(tx.signature)) return;
    li.seenFeePayerOutSignatures.add(tx.signature);

    if (tx.timestamp < li.bundlerFirstBuyAnchorTimestamp) {
      return;
    }
    if (tx.timestamp > li.feePayerWindowEndsAt) {
      void this.maybeResetFollowTokenLargeInsiderWindowClosedBeforeBuy();
      return;
    }
    if (transferOut.amountSol < FOLLOW_TOKEN_LARGE_INSIDER_MIN_FEEPAYER_OUT_SOL) {
      return;
    }
    if (state.bundlerWallets.has(transferOut.to)) return;
    if (this.hasSolIncomingToWallet(tx, state.funderWallet)) return;
    if (this.isFollowTokenLargeInsiderSharedFeePayer(transferOut.to)) return;

    this.addFollowTokenLargeInsiderTier1Wallet(
      transferOut.to,
      transferOut.amountSol,
      tx.signature,
      tx.timestamp,
      state.funderWallet,
    );
  }

  private addFollowTokenLargeInsiderTier1Wallet(
    wallet: string,
    receivedSol: number,
    fundingSignature: string,
    fundingTimestamp: number,
    fundedBy: string,
  ): void {
    const li = this.followTokenLargeInsiderState;
    if (!li?.active || li.tier1FeePayerRecipients.has(wallet)) return;

    li.tier1FeePayerRecipients.add(wallet);
    const watch: FollowTokenLargeInsiderScrapeWatch = {
      wallet,
      fundingSignature,
      qualifiedReceivedSol: receivedSol,
      fundedBy,
      fundingTimestamp,
      tier1DirectFromFeePayer: true,
      childWallets: [],
      firstBuyTimestamp: null,
      firstBuySignature: null,
      boughtAmount: 0,
      soldAmount: 0,
      tokenActions: [],
      observedTxSignatures: new Set<string>(),
      soldAllSignature: null,
      lastSolTransferOutTo: null,
    };
    li.scrapeWatches.set(wallet, watch);
    this.subscribeFollowTokenLargeInsiderScrapeWallet(wallet);

    this.followTokenLargeInsiderLog("tier1 15SOL+ recipient added", {
      mint: li.mint,
      wallet,
      receivedSol,
      fundingSignature,
    });
  }

  private addFollowTokenLargeInsiderChainWallet(
    wallet: string,
    receivedSol: number,
    fundedBy: string,
    fundingTimestamp: number,
  ): FollowTokenLargeInsiderScrapeWatch | null {
    const li = this.followTokenLargeInsiderState;
    if (!li?.active) return null;
    if (this.isFollowTokenLargeInsiderSharedFeePayer(wallet)) {
      this.followTokenLargeInsiderLog(
        "chain recipient rejected — target is shared feePayer",
        { mint: li.mint, wallet, fundedBy, receivedSol },
      );
      return null;
    }
    if (receivedSol < FOLLOW_TOKEN_LARGE_INSIDER_MIN_CHAIN_OUT_SOL) {
      this.followTokenLargeInsiderLog("chain recipient rejected — below min SOL", {
        mint: li.mint,
        wallet,
        fundedBy,
        receivedSol,
        minChainOutSol: FOLLOW_TOKEN_LARGE_INSIDER_MIN_CHAIN_OUT_SOL,
      });
      return null;
    }

    const parent = li.scrapeWatches.get(fundedBy);
    if (!parent) {
      this.followTokenLargeInsiderLog(
        "chain recipient rejected — funder not on scrape watch list",
        {
          mint: li.mint,
          wallet,
          fundedBy,
          receivedSol,
        },
      );
      return null;
    }
    if (li.scrapeWatches.has(wallet)) return li.scrapeWatches.get(wallet)!;

    if (
      parent.childWallets.length >=
      FOLLOW_TOKEN_LARGE_INSIDER_MAX_CHILDREN_PER_WALLET
    ) {
      this.followTokenLargeInsiderLog("chain recipient rejected — parent child cap", {
        mint: li.mint,
        wallet,
        fundedBy,
        receivedSol,
        maxChildrenPerWallet: FOLLOW_TOKEN_LARGE_INSIDER_MAX_CHILDREN_PER_WALLET,
      });
      return null;
    }

    const watch: FollowTokenLargeInsiderScrapeWatch = {
      wallet,
      fundingSignature: "",
      qualifiedReceivedSol: receivedSol,
      fundedBy,
      fundingTimestamp,
      tier1DirectFromFeePayer: false,
      childWallets: [],
      firstBuyTimestamp: null,
      firstBuySignature: null,
      boughtAmount: 0,
      soldAmount: 0,
      tokenActions: [],
      observedTxSignatures: new Set<string>(),
      soldAllSignature: null,
      lastSolTransferOutTo: null,
    };
    li.scrapeWatches.set(wallet, watch);
    parent.childWallets.push(wallet);
    this.subscribeFollowTokenLargeInsiderScrapeWallet(wallet);

    this.followTokenLargeInsiderLog("chain 8SOL+ recipient added", {
      mint: li.mint,
      wallet,
      receivedSol,
      fundedBy,
    });
    return watch;
  }

  private resolveFollowTokenLargeInsiderScrapeWatchChainDepth(
    watch: FollowTokenLargeInsiderScrapeWatch,
  ): number {
    const li = this.followTokenLargeInsiderState;
    if (!li || watch.tier1DirectFromFeePayer) return 0;

    let depth = 0;
    let current: FollowTokenLargeInsiderScrapeWatch | undefined = watch;
    const seen = new Set<string>();
    while (current && !current.tier1DirectFromFeePayer) {
      if (seen.has(current.wallet)) break;
      seen.add(current.wallet);
      depth += 1;
      const parent = li.scrapeWatches.get(current.fundedBy);
      if (!parent) break;
      current = parent;
    }
    return Math.max(1, depth);
  }

  private formatFollowTokenLargeInsiderScrapeWatchTierLine(
    watch: FollowTokenLargeInsiderScrapeWatch,
  ): string {
    if (watch.tier1DirectFromFeePayer) {
      return `Large Insider tier: <b>tier1</b> · direct feePayer ≥${FOLLOW_TOKEN_LARGE_INSIDER_MIN_FEEPAYER_OUT_SOL} SOL out`;
    }
    const depth = this.resolveFollowTokenLargeInsiderScrapeWatchChainDepth(watch);
    const parentLabel =
      depth === 1 ? "tier1" : `chain-${depth - 1}`;
    return `Large Insider tier: <b>chain-${depth}</b> · ≥${FOLLOW_TOKEN_LARGE_INSIDER_MIN_CHAIN_OUT_SOL} SOL downstream from ${parentLabel}`;
  }

  private subscribeFollowTokenLargeInsiderScrapeWallet(wallet: string): void {
    const li = this.followTokenLargeInsiderState;
    if (!li?.active || li.scrapeEnhancedWatchIds.has(wallet)) return;
    if (!this.isValidFollowTokenEarlyBundlerWatchWallet(wallet)) {
      this.log.warn("Skipped invalid Large Insider scrape wallet subscription", {
        mint: li.mint,
        wallet,
      });
      return;
    }

    if (this.enhancedWs) {
      const watchId = this.enhancedWs.watch(wallet, (tx) => {
        void this.applyFollowTokenLargeInsiderScrapeNotificationTx(wallet, tx);
      });
      li.scrapeEnhancedWatchIds.set(wallet, watchId);
    }

    if (!li.scrapeSolBalanceSubIds.has(wallet)) {
      const balanceSubId = this.connection.onAccountChange(
        new PublicKey(wallet),
        (accountInfo) => {
          void this.handleFollowTokenLargeInsiderScrapeZeroBalance(
            wallet,
            BigInt(accountInfo.lamports),
          );
        },
        "processed",
      );
      li.scrapeSolBalanceSubIds.set(wallet, balanceSubId);
    }
  }

  private unsubscribeFollowTokenLargeInsiderScrapeWallet(wallet: string): void {
    const li = this.followTokenLargeInsiderState;
    if (!li) return;

    const enhancedId = li.scrapeEnhancedWatchIds.get(wallet);
    if (enhancedId !== undefined && this.enhancedWs) {
      this.enhancedWs.unwatch(enhancedId);
      li.scrapeEnhancedWatchIds.delete(wallet);
    }

    const balanceSubId = li.scrapeSolBalanceSubIds.get(wallet);
    if (balanceSubId !== undefined) {
      void this.connection
        .removeAccountChangeListener(balanceSubId)
        .catch(() => undefined);
      li.scrapeSolBalanceSubIds.delete(wallet);
    }

    li.scrapeWatches.delete(wallet);
    li.tier1FeePayerRecipients.delete(wallet);
  }

  private pruneFollowTokenLargeInsiderScrapeWalletsExcept(
    keepWallets: readonly string[],
  ): void {
    const li = this.followTokenLargeInsiderState;
    if (!li) return;
    const keep = new Set(keepWallets);
    for (const wallet of [...li.scrapeWatches.keys()]) {
      if (!keep.has(wallet)) {
        this.unsubscribeFollowTokenLargeInsiderScrapeWallet(wallet);
      }
    }
  }

  private isFollowTokenLargeInsiderTrackedValidWallet(wallet: string): boolean {
    return (
      this.followTokenLargeInsiderState?.validWallets.includes(wallet) ?? false
    );
  }

  /** Valid LI wallets eligible for ≥25% sell exit — whatever is discovered so far (1–5), including after bundler-path buy. */
  private getFollowTokenLargeInsiderExitValidWallets(): string[] {
    const li = this.followTokenLargeInsiderState;
    if (!li?.active) return [];
    return [...li.validWallets];
  }

  private formatFollowTokenLargeInsiderExitValidWalletsLine(): string {
    const wallets = this.getFollowTokenLargeInsiderExitValidWallets();
    const li = this.followTokenLargeInsiderState;
    const searchComplete = li?.validWalletSearchComplete ?? false;
    const walletList = wallets.length
      ? wallets.map((w) => `<code>${w}</code>`).join(", ")
      : "none yet";
    const pendingLine =
      !searchComplete && wallets.length < FOLLOW_TOKEN_LARGE_INSIDER_MAX_VALID_WALLETS
        ? ` Still discovering up to <b>${FOLLOW_TOKEN_LARGE_INSIDER_MAX_VALID_WALLETS}</b> — new valid wallets join the ≥25% exit pool automatically.`
        : "";
    return `Valid LI ≥25% exit pool: <b>${wallets.length}</b> wallet(s) — ${walletList}.${pendingLine}`;
  }

  private registerFollowTokenLargeInsiderValidWalletForExitMonitoring(
    wallet: string,
  ): void {
    const li = this.followTokenLargeInsiderState;
    if (!li?.active || !li.validWallets.includes(wallet)) return;

    this.subscribeFollowTokenLargeInsiderScrapeWallet(wallet);

    if (!this.isFollowTokenLargeInsiderBuyExitMode()) return;

    this.followTokenLargeInsiderLog(
      "valid wallet registered for ≥25% sell exit monitoring",
      {
        mint: li.mint,
        wallet,
        validWalletCount: li.validWallets.length,
        validWallets: [...li.validWallets],
      },
    );
  }

  private followTokenLargeInsiderEffectiveSoldFraction(
    watch: FollowTokenLargeInsiderScrapeWatch,
    remainingAmount: number | null,
  ): number | null {
    if (watch.soldAmount > 0 && remainingAmount !== null) {
      const baselineHoldings = watch.soldAmount + remainingAmount;
      if (baselineHoldings > 0) {
        return watch.soldAmount / baselineHoldings;
      }
      return null;
    }
    if (watch.boughtAmount <= 0) return null;
    return watch.soldAmount / watch.boughtAmount;
  }

  private followTokenLargeInsiderWatchReachedExitSoldThreshold(
    watch: FollowTokenLargeInsiderScrapeWatch,
    remainingAmount: number | null,
  ): boolean {
    const soldFraction = this.followTokenLargeInsiderEffectiveSoldFraction(
      watch,
      remainingAmount,
    );
    return (
      soldFraction !== null &&
      soldFraction >= FOLLOW_TOKEN_LARGE_INSIDER_EXIT_SOLD_FRACTION
    );
  }

  private async handleFollowTokenLargeInsiderScrapeZeroBalance(
    wallet: string,
    lamports: bigint,
  ): Promise<void> {
    const li = this.followTokenLargeInsiderState;
    const funderState = this.bundlerFunderWatch;
    if (!li?.active || lamports > 0n) return;
    if (li.validWallets.includes(wallet)) return;
    const watch = li.scrapeWatches.get(wallet);
    if (!watch) return;

    const returnedToFeePayer =
      !!funderState &&
      !!watch.lastSolTransferOutTo &&
      watch.lastSolTransferOutTo === funderState.originalFunderWallet;

    this.followTokenLargeInsiderLog(
      returnedToFeePayer
        ? "scrape wallet zero balance after SOL return to feePayer — removing watch (feePayer stays on primary watch only)"
        : "scrape wallet zero balance — removing (no child spawn required)",
      {
        mint: li.mint,
        wallet,
        feePayer: funderState?.originalFunderWallet ?? null,
        lastSolTransferOutTo: watch.lastSolTransferOutTo,
        hadChildren: watch.childWallets.length,
      },
    );
    this.unsubscribeFollowTokenLargeInsiderScrapeWallet(wallet);
  }

  private async applyFollowTokenLargeInsiderScrapeNotificationTx(
    wallet: string,
    tx: HeliusTransaction,
  ): Promise<void> {
    const li = this.followTokenLargeInsiderState;
    const watch = li?.scrapeWatches.get(wallet);
    const funderState = this.bundlerFunderWatch;
    if (!li?.active || !watch || !funderState || li.mint !== funderState.mint) {
      return;
    }
    if (watch.observedTxSignatures.has(tx.signature)) return;
    watch.observedTxSignatures.add(tx.signature);

    const solTransferOut = this.recordFollowTokenLargeInsiderScrapeSolTransferOut(
      watch,
      tx,
      wallet,
      funderState,
    );
    const chainOut =
      solTransferOut &&
      solTransferOut.amountSol >= FOLLOW_TOKEN_LARGE_INSIDER_MIN_CHAIN_OUT_SOL
        ? solTransferOut
        : null;
    if (
      chainOut &&
      !funderState.bundlerWallets.has(chainOut.to) &&
      !this.isFollowTokenLargeInsiderSharedFeePayer(chainOut.to)
    ) {
      this.addFollowTokenLargeInsiderChainWallet(
        chainOut.to,
        chainOut.amountSol,
        wallet,
        tx.timestamp,
      );
    }

    if (!this.isRelevantMintTx(tx, funderState.mint)) return;
    const action = this.classifyTx(tx, wallet, funderState.mint);
    if (action !== "buy" && action !== "sell") return;

    if (action === "buy") {
      if (
        li.validWalletSearchComplete &&
        !this.isFollowTokenLargeInsiderTrackedValidWallet(wallet)
      ) {
        return;
      }

      const amount = this.extractTokenAmountForWallet(
        tx,
        wallet,
        funderState.mint,
        "buy",
      );
      watch.tokenActions.push({ kind: "buy", signature: tx.signature, amount });
      watch.boughtAmount += amount;

      if (!watch.firstBuySignature) {
        watch.firstBuySignature = tx.signature;
        watch.firstBuyTimestamp = tx.timestamp;
        if (
          await this.shouldSkipFollowTokenLargeInsiderValidWalletBuy(
            watch,
            tx,
            wallet,
          )
        ) {
          return;
        }
        if (!this.isFollowTokenLargeInsiderTrackedValidWallet(wallet)) {
          if (
            li.validWallets.length >= FOLLOW_TOKEN_LARGE_INSIDER_MAX_VALID_WALLETS
          ) {
            return;
          }
          await this.onFollowTokenLargeInsiderValidWalletFound(
            wallet,
            watch,
            tx,
          );
        }
      }
      return;
    }

    if (
      !watch.firstBuySignature ||
      !this.isFollowTokenLargeInsiderTrackedValidWallet(wallet)
    ) {
      return;
    }
    const sellAmount = this.extractTokenAmountForWallet(
      tx,
      wallet,
      funderState.mint,
      "sell",
    );
    watch.tokenActions.push({ kind: "sell", signature: tx.signature, amount: sellAmount });
    watch.soldAmount += sellAmount;
    if (this.isFollowTokenLargeInsiderBuyExitMode()) {
      await this.handleFollowTokenLargeInsiderValidWalletTwentyFivePercentSoldExit(
        wallet,
        tx,
        watch,
      );
    } else if (li.exitOverrideActive) {
      await this.handleFollowTokenLargeInsiderValidWalletSellAllExit(
        wallet,
        tx,
        watch,
      );
    }
  }

  private isFollowTokenLargeInsiderBuyExitMode(): boolean {
    return (
      this.followTokenTopBuyerWatchReason ===
      FOLLOW_TOKEN_LARGE_INSIDER_VALID_WALLET_REASON
    );
  }

  private async estimateFollowTokenLargeInsiderValidWalletFirstBuyUsd(
    tx: HeliusTransaction,
    wallet: string,
  ): Promise<number | null> {
    const buySol = this.estimateEarlyBuySol(tx, wallet);
    const solPriceUsd = await this.getCachedSolPriceUsd();
    if (buySol === null || solPriceUsd === null) return null;
    return buySol * solPriceUsd;
  }

  private async shouldSkipFollowTokenLargeInsiderValidWalletBuy(
    watch: FollowTokenLargeInsiderScrapeWatch,
    tx: HeliusTransaction,
    wallet: string,
  ): Promise<boolean> {
    const li = this.followTokenLargeInsiderState;
    const buySol = this.estimateEarlyBuySol(tx, wallet);
    if (
      buySol !== null &&
      buySol > FOLLOW_TOKEN_LARGE_INSIDER_MAX_VALID_WALLET_FIRST_BUY_SOL
    ) {
      this.followTokenLargeInsiderLog(
        "scrape wallet first buy skipped — above max SOL for valid wallet",
        {
          mint: li?.mint ?? null,
          wallet,
          buySol,
          tier1DirectFromFeePayer: watch.tier1DirectFromFeePayer,
          maxValidWalletFirstBuySol:
            FOLLOW_TOKEN_LARGE_INSIDER_MAX_VALID_WALLET_FIRST_BUY_SOL,
          signature: tx.signature,
        },
      );
      return true;
    }

    const firstBuyUsd = await this.estimateFollowTokenLargeInsiderValidWalletFirstBuyUsd(
      tx,
      wallet,
    );
    if (firstBuyUsd === null) {
      this.followTokenLargeInsiderLog(
        "scrape wallet first buy skipped — first buy USD unavailable for valid wallet gate",
        {
          mint: li?.mint ?? null,
          wallet,
          buySol,
          signature: tx.signature,
        },
      );
      return true;
    }
    if (firstBuyUsd <= FOLLOW_TOKEN_LARGE_INSIDER_VALID_WALLET_FIRST_BUY_MIN_USD) {
      this.recordFollowTokenLargeInsiderFirstBuyUsdNearMiss(
        wallet,
        firstBuyUsd,
        buySol,
        tx.signature,
      );
      this.followTokenLargeInsiderLog(
        "scrape wallet first buy skipped — below min USD for valid wallet",
        {
          mint: li?.mint ?? null,
          wallet,
          buySol,
          firstBuyUsd,
          requiredMinUsd:
            FOLLOW_TOKEN_LARGE_INSIDER_VALID_WALLET_FIRST_BUY_MIN_USD,
          signature: tx.signature,
        },
      );
      return true;
    }

    return false;
  }

  private recordFollowTokenLargeInsiderFirstBuyUsdNearMiss(
    wallet: string,
    firstBuyUsd: number,
    buySol: number | null,
    signature: string,
  ): void {
    const li = this.followTokenLargeInsiderState;
    if (!li?.active || li.validWallets.includes(wallet)) return;
    li.firstBuyBelowMinUsdWallets.set(wallet, {
      firstBuyUsd,
      buySol,
      signature,
    });
  }

  private async sendFollowTokenLargeInsiderFirstBuyUsdNearMissTelegram(
    mint: string,
    nearMisses: ReadonlyArray<
      readonly [
        string,
        { firstBuyUsd: number; buySol: number | null; signature: string },
      ]
    >,
  ): Promise<void> {
    const minUsd = FOLLOW_TOKEN_LARGE_INSIDER_VALID_WALLET_FIRST_BUY_MIN_USD;
    const walletLines = nearMisses.map(([wallet, info]) => {
      const solPart =
        info.buySol !== null ? ` · ${info.buySol.toFixed(4)} SOL` : "";
      return `• <code>${wallet}</code> — first buy <b>$${info.firstBuyUsd.toFixed(2)}</b>${solPart}`;
    });
    await this.sendTelegramSafe(
      [
        `<b>🟡 ${this.label} Follow-Token Large Insider — First Buy ≤ $${minUsd}</b>`,
        `Token: <code>${mint}</code>`,
        `These scrape wallets bought the token but first buy was ≤ <b>$${minUsd}</b> — not counted as valid LI:`,
        ...walletLines,
        "",
        `With first buy &gt; <b>$${minUsd}</b>, they would join the valid LI pool (wallet #4 buy, post-LI 8M/16M gates, Qualified SOL gate, ≥25% exit pool).`,
      ].join("\n"),
      "follow-token large insider first buy usd near miss",
    );
  }

  private async handleFollowTokenLargeInsiderValidWalletTwentyFivePercentSoldExit(
    wallet: string,
    tx: HeliusTransaction | null,
    watch: FollowTokenLargeInsiderScrapeWatch,
  ): Promise<void> {
    const li = this.followTokenLargeInsiderState;
    const funderState = this.bundlerFunderWatch;
    const ebState = this.followTokenEarlyBundlerExitState;
    if (
      !li?.active ||
      !this.isFollowTokenLargeInsiderTrackedValidWallet(wallet) ||
      !funderState
    ) {
      return;
    }
    if (li.exitTriggerSignature) return;

    if (this.shouldDeferFollowTokenLargeInsiderValidWalletTwentyFivePercentExit()) {
      if (ebState) {
        ebState.validWalletTwentyFivePercentDeferred = true;
      }
      this.followTokenLargeInsiderLog(
        "valid wallet ≥25% deferred until all early bundlers sold all",
        {
          mint: li.mint,
          wallet,
          signature: tx?.signature ?? null,
        },
      );
      return;
    }

    let remainingAmount: number | null = null;
    if (tx) {
      remainingAmount = await this.getRecipientTokenBalanceAtTx(
        funderState,
        this.buildFollowTokenLargeInsiderRecipientWatchStub(watch),
        tx,
      );
    }
    if (
      !this.followTokenLargeInsiderWatchReachedExitSoldThreshold(
        watch,
        remainingAmount,
      )
    ) {
      return;
    }

    if (this.phase !== "holding" || this.positionSellTriggered) return;

    const signature =
      tx?.signature ??
      [...watch.tokenActions]
        .reverse()
        .find((action) => action.kind === "sell")?.signature ??
      "VALID_LI_25_EXIT";
    li.exitTriggerSignature = signature;
    if (ebState?.active) {
      ebState.exitTriggerSignature = signature;
    }
    const validIndex = li.validWallets.indexOf(wallet) + 1;
    const soldFraction = this.followTokenLargeInsiderEffectiveSoldFraction(
      watch,
      remainingAmount,
    );
    const soldPercent =
      soldFraction !== null ? (soldFraction * 100).toFixed(1) : "?";
    const trackedBoughtPercent =
      watch.boughtAmount > 0
        ? ((watch.soldAmount / watch.boughtAmount) * 100).toFixed(1)
        : null;

    this.followTokenLargeInsiderLog("valid wallet ≥25% sold — exiting", {
      mint: li.mint,
      wallet,
      validIndex,
      signature,
      soldPercent,
      trackedBoughtPercent,
      soldAmount: watch.soldAmount,
      remainingAmount,
      validWallets: li.validWallets,
      highSellUsdMode: ebState?.highSellUsdMode ?? false,
      allBundlersSoldAll: ebState?.allSoldAllComplete ?? false,
      maxSingleSellGateTierAtBuy: ebState?.maxSingleSellGateTierAtBuy ?? null,
    });

    const activeMcTpPercent = this.getFollowTokenActiveProfitExitPercent();
    const gateTierNote =
      ebState?.maxSingleSellGateTierAtBuy === "fallback_16m"
        ? " (16M max-single-sell fallback buy — +40% MC TP tier)"
        : "";
    const exitDetail = ebState?.highSellUsdMode
      ? `Valid wallet ≥25% exit (bundler cumulative sell >$${FOLLOW_TOKEN_EARLY_BUNDLER_EXIT_HIGH_SELL_USD_MC_TP_DISABLE.toLocaleString()} — +${activeMcTpPercent.toFixed(0)}% MC TP disabled).`
      : ebState?.allSoldAllComplete
        ? `All early bundlers sold all; valid wallet ≥25% — selling full position${gateTierNote}.`
        : `Valid wallet holdings below 75% — selling full position (+${activeMcTpPercent.toFixed(0)}% MC TP bypassed).`;

    await this.triggerPositionSell(
      funderState.mint,
      "follow-token large insider valid wallet 25% sold",
      [
        `<b>🚨 ${this.label} Follow-Token Large Insider Exit</b>`,
        `Token: <code>${funderState.mint}</code>`,
        validIndex > 0
          ? `Valid wallet #${validIndex}: <code>${wallet}</code>`
          : `Valid wallet: <code>${wallet}</code>`,
        `Sold: <b>≥25%</b> (${soldPercent}% tracked)`,
        tx ? `Tx: <code>${tx.signature}</code>` : "",
        "",
        exitDetail,
      ].filter(Boolean),
      signature,
    );
  }

  private buildFollowTokenLargeInsiderRecipientWatchStub(
    watch: FollowTokenLargeInsiderScrapeWatch,
  ): FunderRecipientWatch {
    return {
      wallet: watch.wallet,
      fundingSignature: watch.fundingSignature,
      fundingTimestamp: watch.fundingTimestamp,
      outAmountSol: watch.qualifiedReceivedSol,
      heliusPreferredIndex: 0,
      tokenActions: watch.tokenActions,
      observedTxSignatures: watch.observedTxSignatures,
      tokenBuyObserved: true,
      zeroSolBalanceSignatures: new Set<string>(),
      buyTriggersEntry: false,
      boughtAmount: watch.boughtAmount,
      soldAmount: watch.soldAmount,
      firstBuySignature: watch.firstBuySignature,
      firstBuyTimestamp: watch.firstBuyTimestamp,
      normalTinyTransferMode: false,
      normalTinyExitPercent: null,
      lowFundingCopySellOnSellAll: false,
      lowFundingTinyUsdBand: null,
      lowFundingLargeTransferMode: false,
      postEntrySwapSignature: null,
      postEntrySwapBaselineSignatures: new Set<string>(),
      soldAllSignature: watch.soldAllSignature,
    };
  }

  private async handleFollowTokenLargeInsiderValidWalletSellAllExit(
    wallet: string,
    tx: HeliusTransaction,
    watch: FollowTokenLargeInsiderScrapeWatch,
  ): Promise<void> {
    const li = this.followTokenLargeInsiderState;
    const funderState = this.bundlerFunderWatch;
    if (
      !li?.active ||
      !this.isFollowTokenLargeInsiderTrackedValidWallet(wallet) ||
      !funderState
    ) {
      return;
    }
    if (watch.soldAllSignature || li.exitTriggerSignature) return;

    const remainingAmount = await this.getRecipientTokenBalanceAtTx(
      funderState,
      this.buildFollowTokenLargeInsiderRecipientWatchStub(watch),
      tx,
    );
    const soldAll =
      (remainingAmount !== null && remainingAmount <= 0) ||
      (watch.boughtAmount > 0 && watch.soldAmount >= watch.boughtAmount);
    if (!soldAll || this.phase !== "holding" || this.positionSellTriggered) {
      return;
    }

    watch.soldAllSignature = tx.signature;
    li.exitTriggerSignature = tx.signature;
    this.followTokenLargeInsiderLog("valid wallet sold all — exiting (tag-plan override)", {
      mint: li.mint,
      wallet,
      signature: tx.signature,
    });

    await this.triggerPositionSell(
      funderState.mint,
      "follow-token large insider valid wallet sold all (tag-plan override)",
      [
        `<b>🚨 ${this.label} Follow-Token Large Insider Exit</b>`,
        `Token: <code>${funderState.mint}</code>`,
        `Valid wallet: <code>${wallet}</code>`,
        `Tx: <code>${tx.signature}</code>`,
        "",
        "Tag-plan override: valid wallet sold all — selling full position.",
      ],
      tx.signature,
    );
  }

  private extractWalletSolOutflowOnTx(
    wallet: string,
    tx: HeliusTransaction,
  ): number {
    let total = 0;
    for (const transfer of tx.nativeTransfers ?? []) {
      if (transfer.fromUserAccount === wallet && transfer.toUserAccount !== wallet) {
        total += (transfer.amount ?? 0) / LAMPORTS_PER_SOL;
      }
    }
    return total;
  }

  private followTokenEarlyBundlerUsesTransferRecipientSellPath(): boolean {
    return !!this.followTokenEarlyBundlerExitState
      ?.earlyBundlerTransferOutObserved;
  }

  private canTriggerFollowTokenLargeInsiderBuyOnValidWalletFourth(): boolean {
    return !this.followTokenEarlyBundlerUsesTransferRecipientSellPath();
  }

  private applyPreLiBundlerBuyExitModeFromBundlerStats(): {
    mode: "mc_tp_retained_low_stats" | "mc_tp_and_li" | "li_only";
    meetsSellTxGate: boolean;
    maxSellTxCount: number;
    maxCumulativeSellUsd: number;
  } {
    const state = this.followTokenEarlyBundlerExitState;
    const maxSellTxCount = this.getFollowTokenEarlyBundlerExitMaxSellTxCount();
    const maxCumulativeSellUsd =
      this.getFollowTokenEarlyBundlerExitMaxCumulativeSellUsd();
    const meetsSellTxGate =
      this.bundlerExitMeetsMinSellTxCountForCumulativeUsdGate();
    const highSellUsd =
      meetsSellTxGate &&
      this.anyFollowTokenEarlyBundlerExitWatchExceedsHighSellUsdMcTpDisable();
    const wouldPostLiSkip =
      !meetsSellTxGate ||
      this.noFollowTokenEarlyBundlerExitWatchExceedsLowSellUsd();

    if (highSellUsd) {
      if (state?.maxSingleSellGateTierAtBuy === "fallback_16m") {
        if (state) state.highSellUsdMode = false;
        this.profitExitDisabled = false;
        this.disableProfitExitAfterBuy = false;
        return {
          mode: "mc_tp_and_li",
          meetsSellTxGate,
          maxSellTxCount,
          maxCumulativeSellUsd,
        };
      }
      if (state) state.highSellUsdMode = true;
      this.profitExitDisabled = true;
      return {
        mode: "li_only",
        meetsSellTxGate,
        maxSellTxCount,
        maxCumulativeSellUsd,
      };
    }

    if (state) state.highSellUsdMode = false;
    this.profitExitDisabled = false;
    this.disableProfitExitAfterBuy = false;

    if (wouldPostLiSkip) {
      return {
        mode: "mc_tp_retained_low_stats",
        meetsSellTxGate,
        maxSellTxCount,
        maxCumulativeSellUsd,
      };
    }

    return {
      mode: "mc_tp_and_li",
      meetsSellTxGate,
      maxSellTxCount,
      maxCumulativeSellUsd,
    };
  }

  private preservePreLiBundlerBuyMcTpExitAfterFirstValidWallet(): void {
    const state = this.followTokenEarlyBundlerExitState;
    const funderState = this.bundlerFunderWatch;
    const li = this.followTokenLargeInsiderState;
    if (
      !state?.preLiBundlerSoldAllBuy ||
      !state.allSoldAllComplete ||
      !funderState ||
      !li?.active
    ) {
      return;
    }

    const exitMode = this.applyPreLiBundlerBuyExitModeFromBundlerStats();
    const {
      mode,
      meetsSellTxGate,
      maxSellTxCount,
      maxCumulativeSellUsd,
    } = exitMode;

    if (mode === "li_only") {
      const activeMcTpPercent = this.getFollowTokenActiveProfitExitPercent();
      this.followTokenLargeInsiderLog(
        `pre-LI buy — +${activeMcTpPercent.toFixed(0)}% MC TP disabled (bundler cumulative >$35k); valid LI ≥25% only`,
        {
          mint: funderState.mint,
          maxSellTxCount,
          maxCumulativeSellUsd,
        },
      );
      void this.sendTelegramSafe(
        [
          `<b>⏳ ${this.label} Pre-LI Buy — Valid LI ≥25% Only</b>`,
          `Token: <code>${funderState.mint}</code>`,
          `Valid LI wallet #1 found while holding pre-LI bundler buy.`,
          `Bundler cumulative sell > $${FOLLOW_TOKEN_EARLY_BUNDLER_EXIT_HIGH_SELL_USD_MC_TP_DISABLE.toLocaleString()} (max <b>$${maxCumulativeSellUsd.toLocaleString(undefined, { maximumFractionDigits: 0 })}</b>; ≥${FOLLOW_TOKEN_EARLY_BUNDLER_EXIT_MIN_SELL_TX_COUNT_FOR_USD_GATE} sell txs).`,
          `+${activeMcTpPercent.toFixed(0)}% MC TP disabled — same as post-LI LI-only branch.`,
          "Waiting for ≥25% sold on any valid Large Insider wallet.",
        ].join("\n"),
        "follow-token pre-li buy li-only",
      );
      return;
    }

    if (mode !== "mc_tp_retained_low_stats") return;

    const activeMcTpPercent = this.getFollowTokenActiveProfitExitPercent();
    this.followTokenLargeInsiderLog(
      `pre-LI buy — keeping +${activeMcTpPercent.toFixed(0)}% MC TP after 1st valid LI wallet (post-LI skip branches ignored)`,
      {
        mint: funderState.mint,
        maxSellTxCount,
        maxCumulativeSellUsd,
        meetsSellTxGate,
        lowUsdThreshold: FOLLOW_TOKEN_EARLY_BUNDLER_EXIT_LOW_SELL_USD_THRESHOLD,
      },
    );

    void this.sendTelegramSafe(
      [
        `<b>ℹ️ ${this.label} Pre-LI Buy — +${activeMcTpPercent.toFixed(0)}% MC TP Retained</b>`,
        `Token: <code>${funderState.mint}</code>`,
        `Valid LI wallet #1 found while holding pre-LI bundler buy.`,
        !meetsSellTxGate
          ? `Bundler max sell txs &lt; ${FOLLOW_TOKEN_EARLY_BUNDLER_EXIT_MIN_SELL_TX_COUNT_FOR_USD_GATE} (max <b>${maxSellTxCount}</b>) — post-LI would skip token; keeping pre-LI +${activeMcTpPercent.toFixed(0)}% MC TP.`
          : `Bundler max cumulative ≤ $${FOLLOW_TOKEN_EARLY_BUNDLER_EXIT_LOW_SELL_USD_THRESHOLD.toLocaleString()} (max <b>$${maxCumulativeSellUsd.toLocaleString(undefined, { maximumFractionDigits: 0 })}</b>) — post-LI would skip token; keeping pre-LI +${activeMcTpPercent.toFixed(0)}% MC TP.`,
        `Valid LI ≥25% exit also active for discovered wallets.`,
      ].join("\n"),
      "follow-token pre-li buy mc tp retained",
    );
  }

  private async onFollowTokenLargeInsiderValidWalletFound(
    wallet: string,
    watch: FollowTokenLargeInsiderScrapeWatch,
    tx: HeliusTransaction,
  ): Promise<void> {
    const li = this.followTokenLargeInsiderState;
    const funderState = this.bundlerFunderWatch;
    if (!li?.active || !funderState || li.validWalletSearchComplete) return;
    if (li.validWallets.includes(wallet)) return;
    if (li.validWallets.length >= FOLLOW_TOKEN_LARGE_INSIDER_MAX_VALID_WALLETS) {
      return;
    }

    li.validWallets.push(wallet);
    const validIndex = li.validWallets.length;
    if (validIndex >= FOLLOW_TOKEN_LARGE_INSIDER_MAX_VALID_WALLETS) {
      li.validWalletSearchComplete = true;
      this.pruneFollowTokenLargeInsiderScrapeWalletsExcept(li.validWallets);
    }

    this.followTokenLargeInsiderLog("valid wallet found", {
      mint: li.mint,
      wallet,
      validIndex,
      validWalletCount: li.validWallets.length,
      qualifiedSol: watch.qualifiedReceivedSol,
      tier: watch.tier1DirectFromFeePayer
        ? "tier1"
        : `chain-${this.resolveFollowTokenLargeInsiderScrapeWatchChainDepth(watch)}`,
      signature: tx.signature,
    });

    const liExitLine = `Exit: <b>+${FOLLOW_TOKEN_LARGE_INSIDER_PROFIT_EXIT_PERCENT}% MC TP</b> · any valid wallet (up to ${FOLLOW_TOKEN_LARGE_INSIDER_MAX_VALID_WALLETS}) ≥25% sell early exit.`;
    const statusLine =
      validIndex === FOLLOW_TOKEN_LARGE_INSIDER_BUY_AT_VALID_WALLET_COUNT
        ? this.canTriggerFollowTokenLargeInsiderBuyOnValidWalletFourth()
          ? `Valid wallet #4 found — buy trigger armed (or early bundler sold-all path).`
          : `Valid wallet #4 found — buy only via early bundler/recipient sold-all path (transfer-out observed).`
        : validIndex >= FOLLOW_TOKEN_LARGE_INSIDER_MAX_VALID_WALLETS
          ? `Valid wallet #5 found — search complete.`
          : validIndex < FOLLOW_TOKEN_LARGE_INSIDER_BUY_AT_VALID_WALLET_COUNT
            ? this.buySubmitted
              ? `Valid wallet #${validIndex} added while holding — included in ≥25% exit pool.`
              : `Waiting for valid wallet <b>#${validIndex + 1}</b> of <b>${FOLLOW_TOKEN_LARGE_INSIDER_BUY_AT_VALID_WALLET_COUNT}</b> before buy.`
            : `Valid wallet #${validIndex} added while holding — included in ≥25% exit pool; still watching for #5.`;

    void this.sendTelegramSafe(
      [
        `<b>🎯 ${this.label} Follow-Token Large Insider Valid Wallet #${validIndex}</b>`,
        `Token: <code>${li.mint}</code>`,
        `Wallet: <code>${wallet}</code>`,
        this.formatFollowTokenLargeInsiderScrapeWatchTierLine(watch),
        `Qualified SOL: <b>${watch.qualifiedReceivedSol.toFixed(4)}</b>`,
        `Buy tx: <code>${tx.signature}</code>`,
        statusLine,
        validIndex >= FOLLOW_TOKEN_LARGE_INSIDER_BUY_AT_VALID_WALLET_COUNT ||
        validIndex === FOLLOW_TOKEN_LARGE_INSIDER_BUY_AT_VALID_WALLET_COUNT - 1 ||
        this.buySubmitted
          ? liExitLine
          : `Planned exit after buy: ${liExitLine}`,
        this.buySubmitted
          ? this.formatFollowTokenLargeInsiderExitValidWalletsLine()
          : "",
      ]
        .filter(Boolean)
        .join("\n"),
      "follow-token large insider valid wallet",
    );

    this.registerFollowTokenLargeInsiderValidWalletForExitMonitoring(wallet);

    if (validIndex === 1) {
      if (this.buySubmitted && this.followTokenEarlyBundlerExitState?.preLiBundlerSoldAllBuy) {
        this.preservePreLiBundlerBuyMcTpExitAfterFirstValidWallet();
      }
      void this.maybeEvaluateFollowTokenEarlyBundlerExit();
    }

    if (
      this.buySubmitted &&
      this.phase === "holding" &&
      !this.positionSellTriggered &&
      this.isFollowTokenLargeInsiderBuyExitMode()
    ) {
      void this.triggerFollowTokenLargeInsiderValidWalletTwentyFivePercentExitIfReady();
    }

    if (this.buySubmitted) return;

    if (validIndex === FOLLOW_TOKEN_LARGE_INSIDER_BUY_AT_VALID_WALLET_COUNT) {
      if (this.canTriggerFollowTokenLargeInsiderBuyOnValidWalletFourth()) {
        await this.emitFollowTokenLargeInsiderBuy(
          funderState,
          wallet,
          tx.signature,
          tx,
          { triggerSource: "valid_wallet_4" },
        );
      }
    }
  }

  private async emitFollowTokenLargeInsiderBuy(
    state: BundlerFunderWatchState,
    watchedWallet: string,
    signature: string,
    triggerTx: HeliusTransaction,
    options: {
      triggerSource?:
        | "valid_wallet_4"
        | "bundler_sold_all"
        | "migration_2_4s_immediate";
      bundlerExitBranch?:
        | "low_tx_immediate"
        | "low_usd_immediate"
        | "normal_mc_tp"
        | "high_usd_li_only";
      preLiPhase?: boolean;
      profitExitPercent?: number;
      buySolOverride?: number;
      maxSingleSellGateTier?: "standard_8m" | "fallback_16m";
      skipAthGate?: boolean;
    } = {},
  ): Promise<void> {
    if (
      this.buySubmitted ||
      this.buyDisabled ||
      this.isBuyExecuting ||
      this.isBuyGateEvaluating ||
      this.isBuyBlockedByDevTokenOut(state.mint)
    ) {
      return;
    }

    this.isBuyGateEvaluating = true;
    const ebState = this.followTokenEarlyBundlerExitState;
    try {
      if (
        options.triggerSource !== "migration_2_4s_immediate" &&
        this.anyFollowTokenLargeInsiderValidWalletReachedTwentyFivePercentSold()
      ) {
        await this.skipFollowTokenLargeInsiderFromValidWalletTwentyFivePercentAlreadySold(
          state.mint,
          options.triggerSource ?? "valid_wallet_4",
          triggerTx,
        );
        return;
      }

      const currentMc = await this.gmgnClient.fetchTokenMarketCapUsd(state.mint);
      if (currentMc === null) return;
      this.recordObservedMarketCapUsd(currentMc);
      if (currentMc < INSIDER_RUG_MARKET_CAP_USD) {
        await this.resetForNewToken(true, {
          reason: `below_rug_threshold_${INSIDER_RUG_MARKET_CAP_USD}`,
        });
        return;
      }

      this.disableProfitExitAfterBuy = false;
      this.profitExitDisabled = false;
      const profitExitPercent =
        options.profitExitPercent ?? FOLLOW_TOKEN_LARGE_INSIDER_PROFIT_EXIT_PERCENT;
      if (
        options.maxSingleSellGateTier === "fallback_16m" &&
        !options.skipAthGate
      ) {
        const exitMc = currentMc * (1 + profitExitPercent / 100);
        const athSkipThreshold =
          exitMc * FOLLOW_TOKEN_16M_FALLBACK_BUY_ATH_EXIT_MC_MULTIPLIER;
        const tokenAthMc = await this.fetchTokenAthMarketCapUsdFor16mFallbackGate(
          state.mint,
        );
        if (tokenAthMc === null) {
          this.log.warn(
            "16M fallback buy — token ATH MC unavailable after retries; ATH gate skipped",
            {
              mint: state.mint,
              entryMc: currentMc,
              exitMc,
              athSkipThreshold,
              fetchAttempts: 1 + FOLLOW_TOKEN_16M_FALLBACK_ATH_MC_FETCH_RETRIES,
            },
          );
        } else if (tokenAthMc >= athSkipThreshold) {
          await this.skipFollowTokenLargeInsiderFrom16mFallbackAthMcGate(
            state.mint,
            currentMc,
            exitMc,
            profitExitPercent,
            tokenAthMc,
            triggerTx,
          );
          return;
        }
      }
      if (
        options.triggerSource === "bundler_sold_all" &&
        ebState?.highSellUsdMode &&
        !options.preLiPhase &&
        options.maxSingleSellGateTier !== "fallback_16m"
      ) {
        this.disableProfitExitAfterBuy = true;
      }
      if (this.isBuyBlockedByDevTokenOut(state.mint)) {
        return;
      }
      if (
        !(await this.ensureDevBuyCountAllowsBuy(state.mint, {
          signature,
          triggerLabel: options.triggerSource ?? "valid_wallet_4",
        }))
      ) {
        return;
      }
      this.setEntryMc(currentMc);
      this.setExitMc(currentMc * (1 + profitExitPercent / 100));
      this.setBuyExecuting(true);
      this.buySubmitted = true;
      this.preBuyStopped = true;
      this.armDevTokenOutPostBuyWatch(state.mint);
      if (
        ebState?.active &&
        options.triggerSource === "bundler_sold_all" &&
        options.maxSingleSellGateTier
      ) {
        ebState.maxSingleSellGateTierAtBuy = options.maxSingleSellGateTier;
      }
      this.followTokenTopBuyerWallet = watchedWallet;
      this.followTokenTopBuyerMint = state.mint;
      this.followTokenWatchMode = "standard";
      this.followTokenTopBuyerWatchReason =
        FOLLOW_TOKEN_LARGE_INSIDER_VALID_WALLET_REASON;
      this.ensureFollowTokenTopBuyerWatchSubscribed();

      const liWatch =
        this.followTokenLargeInsiderState?.scrapeWatches.get(watchedWallet);
      const triggerSource = options.triggerSource ?? "valid_wallet_4";
      const bundlerBranch = options.bundlerExitBranch;
      const hasDiscoveredLi =
        (this.followTokenLargeInsiderState?.validWallets.length ?? 0) > 0;
      const soldAllAfterFirstLi =
        triggerSource === "bundler_sold_all" && hasDiscoveredLi;
      const displayedPreLiPhase = options.preLiPhase && !soldAllAfterFirstLi;
      const gateTierLine =
        options.maxSingleSellGateTier === "fallback_16m"
          ? `Max-single-sell gate: <b>16M fallback</b> (failed 8M · ≤16M).`
          : "";
      const buySol =
        options.buySolOverride ??
        this.getBuySolForFundingMode(state.lowFundingMode);
      const buyTitle =
        displayedPreLiPhase
          ? options.maxSingleSellGateTier === "fallback_16m"
            ? `<b>🟢 ${this.label} Follow-Token Buy (Pre-LI Bundler Sold-All · 16M Fallback)</b>`
            : `<b>🟢 ${this.label} Follow-Token Buy (Pre-LI Bundler Sold-All)</b>`
          : triggerSource === "bundler_sold_all"
            ? options.maxSingleSellGateTier === "fallback_16m"
              ? `<b>🟢 ${this.label} Follow-Token Large Insider Buy (Bundler Sold-All · 16M Fallback)</b>`
              : `<b>🟢 ${this.label} Follow-Token Large Insider Buy (Bundler Sold-All)</b>`
            : `<b>🟢 ${this.label} Follow-Token Large Insider Buy</b>`;
      const triggerLine =
        displayedPreLiPhase
          ? `Trigger: all early bundlers/recipients sold all before 1st valid LI wallet · +${profitExitPercent}% MC TP`
          : soldAllAfterFirstLi
            ? `Trigger: all early bundlers/recipients sold all after valid LI discovery · +${profitExitPercent}% MC TP`
          : triggerSource === "bundler_sold_all"
            ? `Trigger: all early bundlers/recipients sold all · branch <b>${bundlerBranch ?? "unknown"}</b>`
            : `Valid wallet #4: <code>${watchedWallet}</code>`;
      const postBuyExitLine =
        displayedPreLiPhase
          ? `Post-buy: +${profitExitPercent}% MC TP only (until valid LI wallets join ≥25% pool).`
          : options.maxSingleSellGateTier === "fallback_16m"
            ? `Post-buy: +${profitExitPercent}% MC TP or valid LI ≥25%.`
          : bundlerBranch === "high_usd_li_only"
          ? `Post-buy: +${profitExitPercent}% MC TP disabled; valid LI ≥25% only.`
          : bundlerBranch === "normal_mc_tp"
            ? `Post-buy: +${profitExitPercent}% MC TP or valid LI ≥25%.`
            : "";
      const isFastMigrationBuy =
        triggerSource === "migration_2_4s_immediate";
      const fastMigrationModeLine = isFastMigrationBuy
        ? `Special migration buy: <b>2s–4s immediate path</b> · dev transfer-out watch has no 3-minute expiry · ${this.fastMigrationSellMode === "transfer_out_mc_retrace" ? "4s mode waits for +60% TP or MC retrace to transfer-out baseline." : "2s/3s mode sells immediately on dev transfer-out."}`
        : "";
      void this.sendTelegramSafe(
        [
          isFastMigrationBuy
            ? `<b>🟢 ${this.label} Follow-Token Immediate Migration Buy</b>`
            : buyTitle,
          `Token: <code>${state.mint}</code>`,
          isFastMigrationBuy
            ? "Trigger: Create → migrate time was between 2s and 4s and all migration filters passed."
            : triggerLine,
          gateTierLine,
          fastMigrationModeLine,
          triggerSource === "valid_wallet_4"
            ? liWatch
              ? this.formatFollowTokenLargeInsiderScrapeWatchTierLine(liWatch)
              : ""
            : isFastMigrationBuy
              ? `Reference early bundler: <code>${watchedWallet}</code>`
              : `Reference wallet: <code>${watchedWallet}</code>`,
          `Trigger tx: <code>${signature}</code>`,
          `Buy: <b>${buySol} SOL</b>`,
          isFastMigrationBuy
            ? `Exit: <b>+${profitExitPercent}% MC TP</b> · special dev transfer-out handling applies`
            : "",
          triggerSource === "valid_wallet_4"
            ? `Still watching for valid wallet #5.`
            : "",
          this.formatFollowTokenLargeInsiderExitValidWalletsLine(),
          `Exit: <b>+${profitExitPercent}% MC TP</b> · any discovered valid LI wallet (up to ${FOLLOW_TOKEN_LARGE_INSIDER_MAX_VALID_WALLETS}) ≥25% sell early exit`,
          postBuyExitLine,
          triggerSource === "valid_wallet_4"
            ? `Early bundlers (${this.followTokenEarlyInsiderBuys?.length ?? 0}): parallel pre-buy watch — sold all + exit rules also trigger buy.`
            : "",
        ]
          .filter(Boolean)
          .join("\n"),
        "follow-token large insider buy",
      );

      this.emit("buyTrigger", {
        followedWallet: this.getBuyTriggerFollowedWallet(state),
        mint: state.mint,
        signature,
        buySol,
        entryMc: currentMc,
        profitExitPercent,
        monitoredWallet: watchedWallet,
      });
    } finally {
      this.isBuyGateEvaluating = false;
      if (!this.buySubmitted && ebState?.preBuyBundlerPathTriggered) {
        ebState.preBuyBundlerPathTriggered = false;
      }
    }
  }

  /** True when large-insider exit override should ignore watched-wallet exit rules. */
  private followTokenLargeInsiderExitOverrideActive(): boolean {
    return !!this.followTokenLargeInsiderState?.exitOverrideActive;
  }

  private async handleFollowTokenTopBuyerTransaction(
    tx: HeliusTransaction,
    watchWallet?: string,
  ): Promise<void> {
    const watchedWallet = this.followTokenTopBuyerWallet;
    const mint = this.followTokenTopBuyerMint;
    const wallet = watchWallet ?? watchedWallet;
    if (!wallet || !mint || !watchedWallet || wallet !== watchedWallet) return;
    if (this.followTokenTopBuyerSeenSignatures.has(tx.signature)) return;
    if (!this.isRelevantMintTx(tx, mint)) return;

    this.followTokenTopBuyerSeenSignatures.add(tx.signature);
    const action = this.classifyTx(tx, wallet, mint);
    if (action !== "buy" && action !== "sell") return;

    this.followTokenTopBuyerWatchBackend("Follow-token watched wallet tx observed", {
      mint,
      wallet,
      action,
      signature: tx.signature,
      watchMode: this.followTokenWatchMode,
      phase: this.phase,
      hasActivePosition: !!this.activePosition,
    });

    if (
      this.activePosition?.mint !== mint ||
      this.phase !== "holding" ||
      this.positionSellTriggered
    ) {
      return;
    }

    if (this.followTokenLargeInsiderExitOverrideActive()) {
      return;
    }

    if (
      this.followTokenTopBuyerWatchReason !==
      FOLLOW_TOKEN_LARGE_INSIDER_VALID_WALLET_REASON
    ) {
      return;
    }

    if (action === "buy") {
      return;
    }

    const li = this.followTokenLargeInsiderState;
    const scrapeWatch = li?.scrapeWatches.get(wallet);
    if (
      !scrapeWatch ||
      !this.isFollowTokenLargeInsiderTrackedValidWallet(wallet) ||
      scrapeWatch.observedTxSignatures.has(tx.signature)
    ) {
      return;
    }
    scrapeWatch.observedTxSignatures.add(tx.signature);
    const sellAmount = this.extractTokenAmountForWallet(
      tx,
      wallet,
      mint,
      "sell",
    );
    scrapeWatch.tokenActions.push({
      kind: "sell",
      signature: tx.signature,
      amount: sellAmount,
    });
    scrapeWatch.soldAmount += sellAmount;
    await this.handleFollowTokenLargeInsiderValidWalletTwentyFivePercentSoldExit(
      wallet,
      tx,
      scrapeWatch,
    );
  }

  setFollowWalletFlowDelegate(
    delegate:
      | ((
          mint: string,
          signature: string,
          followedWallet: string,
        ) => Promise<boolean>)
      | null,
  ): void {
    this.followWalletFlowDelegate = delegate;
  }

  setFollowTokenMigrationSuspendDelegate(
    delegate: ((mint: string) => void) | null,
  ): void {
    this.followTokenMigrationSuspendDelegate = delegate;
  }

  /**
   * Starts a follow-wallet token flow on this bot (used for primary or delegated handoff).
   */
  async startFromFollowWalletBuy(
    mint: string,
    signature: string,
    followedWallet: string,
  ): Promise<boolean> {
    if (this.boughtMints.has(mint)) return false;
    if (!this.isIdleForFunderFirst()) return false;
    this.fastMigrationSellMode = null;
    this.fastMigrationTransferOutMc = null;
    if (this.claimMint && !this.claimMint(mint)) {
      this.log.info("Follow-wallet buy delegated but mint is claimed by another bot", {
        mint,
        signature,
        followedWallet,
      });
      return false;
    }

    this.flowFollowWallet = followedWallet;
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
            followedWallet,
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
            `Follow wallet: <code>${followedWallet}</code>`,
            `Follow-wallet buy MC: <b>$${followWalletBuyMc.toLocaleString()}</b>`,
            `Monitoring ceiling: <b>$${MAX_FOLLOW_WALLET_START_MARKET_CAP_USD.toLocaleString()}</b>`,
            "Flow reset — waiting for the next token.",
          ].join("\n"),
          "high-MC skip notification",
        );
        await this.resetForNewToken(true, {
          reason: "follow_wallet_buy_mc_above_ceiling",
          skipTelegram: true,
        });
        return false;
      }
      if (followWalletBuyMc === null) {
        this.log.warn(
          "Could not fetch follow-wallet buy MC; continuing token monitoring",
          { mint, signature, followedWallet },
        );
      } else {
        this.log.info(
          "Follow-wallet buy MC accepted; starting token monitoring",
          {
            mint,
            signature,
            followedWallet,
            followWalletBuyMc,
            maxFollowWalletStartMarketCapUsd:
              MAX_FOLLOW_WALLET_START_MARKET_CAP_USD,
          },
        );
        this.followWalletBackend("Follow-wallet buy MC accepted; starting flow", {
          bot: this.label,
          mint,
          signature,
          followedWallet,
          followWalletBuyMc,
        });
      }
      await this.startInsiderFlowWithIndexingLagRetry(mint, followedWallet);
      return true;
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
      return false;
    }
  }

  /**
   * Starts a follow-token flow from a Pump.fun migration event (no follow wallet).
   */
  async startFromFollowTokenMigration(
    mint: string,
    migrationSignature: string,
    migrationAgeSec?: number,
    migrationDevWallet?: string,
  ): Promise<boolean> {
    if (this.boughtMints.has(mint)) return false;
    if (!this.isIdleForFunderFirst()) return false;
    this.fastMigrationSellMode = null;
    this.fastMigrationTransferOutMc = null;
    if (this.claimMint && !this.claimMint(mint)) {
      this.log.info("Follow-token migration delegated but mint is claimed by another bot", {
        mint,
        migrationSignature,
      });
      return false;
    }

    this.flowFollowWallet = null;
    this.boughtMints.add(mint);
    this.watchingMint = mint;
    this.claimedMint = mint;
    this.emit("mintSeen", mint);

    try {
      const migrationMc = await this.gmgnClient.fetchTokenMarketCapUsd(mint);
      if (
        migrationMc !== null &&
        migrationMc > MAX_FOLLOW_WALLET_START_MARKET_CAP_USD
      ) {
        this.log.warn(
          "Follow-token migration MC above monitoring ceiling; skipping token",
          {
            mint,
            migrationSignature,
            migrationMc,
            maxFollowWalletStartMarketCapUsd:
              MAX_FOLLOW_WALLET_START_MARKET_CAP_USD,
          },
        );
        void this.sendTelegramSafe(
          [
            `<b>⏭️ ${this.label} Follow-Token Skipped</b>`,
            `Token: <code>${mint}</code>`,
            `Migration MC: <b>$${migrationMc.toLocaleString()}</b>`,
            `Monitoring ceiling: <b>$${MAX_FOLLOW_WALLET_START_MARKET_CAP_USD.toLocaleString()}</b>`,
            "Flow reset — waiting for the next migration.",
          ].join("\n"),
          "follow-token high-MC skip notification",
        );
        await this.resetForNewToken(true, {
          reason: "migration_mc_above_ceiling",
          skipTelegram: true,
        });
        return false;
      }
      const flowActive = await this.startInsiderFlowFromMigrationWithIndexingLagRetry(
        mint,
        migrationSignature,
        migrationAgeSec,
        migrationDevWallet,
      );
      return flowActive;
    } catch (err) {
      void this.heliusClient.handlePossibleRateLimitError(err);
      this.releaseMint?.(mint);
      if (err instanceof InsiderMinBuySolFilterError) {
        this.log.info("Follow-token flow skipped by min-buy SOL filter; resetting", {
          mint,
          reason: err.message,
        });
      } else {
        this.log.error("Failed to start follow-token flow; resetting", err);
        this.emit("error", err instanceof Error ? err : new Error(String(err)));
      }
      await this.resetForNewToken(true);
      return false;
    }
  }

  setFollowWalletTxNotifier(
    notifier: ((tx: HeliusTransaction) => void) | null,
  ): void {
    this.followWalletTxNotifier = notifier;
  }

  getFlowSource() {
    return this.flowSource;
  }

  private getNormalFundingMinSol(): number {
    return this.flowSource === "funder-first"
      ? BUNDLER_FUNDER_LOW_FUNDING_SOL
      : BUNDLER_FUNDER_FOLLOW_WALLET_NORMAL_FUNDING_MIN_SOL;
  }

  /** True when the bot is not mid-flow on a token and can accept a funder-first handoff. */
  isIdleForFunderFirst(): boolean {
    return (
      !this.watchingMint &&
      !this.activePosition &&
      !this.isBuyExecuting &&
      !this.buySubmitted
    );
  }

  /**
   * Enters a token flow from funder-first discovery (feePayer already known).
   * Skips follow-wallet backtrack and locks the provided feePayer directly.
   */
  async startFromFunderFirst(
    mint: string,
    feePayer: string,
    earlyBuys: FunderFirstEarlyBuy[],
  ): Promise<boolean> {
    if (!this.isIdleForFunderFirst()) {
      this.log.warn("Funder-first handoff rejected because Insider bot is busy", {
        mint,
        feePayer,
        watchingMint: this.watchingMint,
        activePosition: this.activePosition?.mint ?? null,
      });
      return false;
    }
    if (this.claimMint && !this.claimMint(mint)) {
      this.log.warn("Funder-first handoff rejected because mint is claimed by another bot", {
        mint,
        feePayer,
      });
      return false;
    }
    if (this.boughtMints.has(mint)) {
      this.log.info("Funder-first handoff skipped because mint was already bought this session", {
        mint,
        feePayer,
      });
      return false;
    }

    this.flowSource = "funder-first";
    this.flowFollowWallet = null;
    this.funderFirstFeePayer = feePayer;
    this.watchingMint = mint;
    this.claimedMint = mint;
    this.boughtMints.add(mint);
    this.emit("mintSeen", mint);

    this.resetHeliusPoolMetricsForMint(mint);
    this.resetTokenTxCounts();
    this.insiderSellsReady = false;
    this.bundlerMatchesReady = false;
    this.highestObservedMarketCapUsd = null;
    this.clearBundlerAccumulation();

    const earlyBundlerWallets = this.extractEarlyInsiderWallets(earlyBuys);
    this.initialInsiderWallets.clear();
    for (const wallet of earlyBundlerWallets) this.initialInsiderWallets.add(wallet);

    const createTx = await this.withHeliusFallback((client) =>
      client.getMintCreateTransaction(mint),
    );
    this.devWallet = createTx?.feePayer ?? null;
    this.devCreateSignature = createTx?.signature ?? null;
    this.devCreateTimestamp = createTx?.timestamp ?? null;
    if (this.devWallet) {
      this.subscribeDevWalletFullExitWatch();
    }

    this.preBuyStopped = false;
    this.positionSellTriggered = false;
    this.monitoredWallet = null;
    this.insiderState = null;
    this.phase = "pre_buy";

    void this.sendTelegramSafe(
      [
        `<b>🔍 ${this.label} Funder-First Flow Started</b>`,
        `Token: <code>${mint}</code>`,
        `FeePayer: <code>${feePayer}</code>`,
        `Matched bundlers: <b>${earlyBundlerWallets.length}</b>`,
        "",
        "FeePayer was discovered upstream; watching its transfer-outs for normal-mode buy signals.",
      ].join("\n"),
      "funder-first flow-start notification",
    );

    this.startPollLoop();
    await this.startKnownFeePayerBundlerFlow(mint, feePayer, earlyBuys);
    return true;
  }

  setBuyExecuting(executing: boolean): void {
    this.isBuyExecuting = executing;
  }

  isDevTokenOutBuyBlocked(mint: string): boolean {
    return this.devTokenOutBlockedMints.has(mint);
  }

  triggerDevTokenOutRecoverySell(mint: string, signature: string): void {
    void this.triggerPositionSell(
      mint,
      "Dev wallet transfer_out before buy completed (race) — immediate 100% exit",
      [
        `<b>⛔ ${this.label} Dev Token-Out Race Recovery — Selling 100%</b>`,
        `Token: <code>${mint}</code>`,
        `Tx: <code>${signature}</code>`,
        "",
        "Buy landed after dev transfer-out reset — dumping position immediately.",
      ],
      signature,
    );
  }

  async tryTriggerFastMigrationTransferOutMcSell(
    currentMc: number,
  ): Promise<boolean> {
    if (
      this.fastMigrationSellMode !== "transfer_out_mc_retrace" ||
      this.fastMigrationTransferOutMc === null ||
      !this.activePosition ||
      this.positionSellTriggered ||
      currentMc > this.fastMigrationTransferOutMc
    ) {
      return false;
    }
    await this.triggerPositionSell(
      this.activePosition.mint,
      `4s migration transfer-out MC retrace to $${this.fastMigrationTransferOutMc.toLocaleString()}`,
      [
        `<b>⛔ ${this.label} 4s Migration Transfer-Out MC Retrace — Selling 100%</b>`,
        `Token: <code>${this.activePosition.mint}</code>`,
        `Transfer-out MC: <b>$${this.fastMigrationTransferOutMc.toLocaleString()}</b>`,
        `Current MC: <b>$${currentMc.toLocaleString()}</b>`,
        "The MC returned to the level recorded when Insider 1 transferred tokens out.",
      ],
      "FAST_MIGRATION_TRANSFER_OUT_MC_RETRACE",
    );
    return true;
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
      this.followMonitors.size > 0 ||
      this.insiderLogsSubId !== null ||
      this.insiderEnhancedWatchId !== null ||
      this.bundlerLogsSubIds.size > 0 ||
      this.bundlerEnhancedWatchIds.size > 0 ||
      this.followTokenEarlyBundlerExitState?.active ||
      this.bundlerFunderLogsSubId !== null ||
      this.bundlerFunderEnhancedWatchId !== null ||
      this.bundlerFunderParallelLogsSubId !== null ||
      this.bundlerFunderParallelEnhancedWatchId !== null ||
      this.recipientLogsSubIds.size > 0 ||
      this.recipientEnhancedWatchIds.size > 0 ||
      this.pollTimer !== null
    );
  }

  async addFollowWallet(
    address: string,
  ): Promise<{ ok: true } | { ok: false; error: string }> {
    if (this.stoppedForHeliusCredits) {
      return {
        ok: false,
        error: "Follow-wallet start blocked because Helius credits are exhausted.",
      };
    }

    let normalized: string;
    try {
      normalized = new PublicKey(address.trim()).toBase58();
    } catch {
      return { ok: false, error: "Invalid Solana wallet address." };
    }

    if (this.followedWallets.includes(normalized)) {
      if (!this.followMonitors.has(normalized) && !this.followWalletPaused) {
        await this.startFollowWalletMonitor(normalized);
      }
      return { ok: true };
    }

    if (this.followedWallets.length >= MAX_FOLLOW_WALLETS) {
      return {
        ok: false,
        error: `At most ${MAX_FOLLOW_WALLETS} follow wallets — remove one before adding another.`,
      };
    }

    this.followedWallets.push(normalized);
    if (!this.followWalletPaused) {
      await this.startFollowWalletMonitor(normalized);
    }
    return { ok: true };
  }

  async followWallet(address: string): Promise<void> {
    const result = await this.addFollowWallet(address);
    if (!result.ok) {
      throw new Error(result.error);
    }
  }

  async followAllWallets(): Promise<void> {
    if (this.followWalletPaused) return;
    for (const wallet of this.followedWallets) {
      if (!this.followMonitors.has(wallet)) {
        await this.startFollowWalletMonitor(wallet);
      }
    }
  }

  private async startFollowWalletMonitor(normalized: string): Promise<void> {
    if (this.followMonitors.has(normalized)) {
      return;
    }

    const monitor = new WalletMonitor(this.config, normalized, {
      enforceMinBuySol: false,
      rpcUrl: this.rpcUrl,
      wsUrl: this.wsUrl,
      logLabel: this.config.insiderFollowWalletVerboseLogs
        ? `FOLLOW-WALLET ${normalized.slice(0, 6)}`
        : `WALLET ${this.label.toUpperCase()}`,
      verboseActivityLogs: this.config.insiderFollowWalletVerboseLogs,
      enhancedWs: this.enhancedWs,
    });
    this.followMonitors.set(normalized, monitor);
    monitor.on("transaction", (event: { walletAddress: string; tx: HeliusTransaction }) => {
      this.followWalletTxNotifier?.(event.tx);
    });
    monitor.on("newToken", (event) => {
      void this.handleFollowWalletBuy(
        event.mint,
        event.signature,
        event.walletAddress,
      );
    });

    try {
      await monitor.start();
    } catch (err) {
      this.followMonitors.delete(normalized);
      throw err;
    }
    for (const mint of monitor.existingMints) {
      this.boughtMints.add(mint);
    }

    this.log.info("Insider follow wallet monitoring started", {
      followedWallet: normalized,
      followWalletCount: this.followedWallets.length,
      buySol: this.buySol,
      bundlerUsdRange: `${this.bundlerBuyMinUsd}-${this.bundlerBuyMaxUsd}`,
    });
    this.followWalletBackend("Follow-wallet monitoring started", {
      bot: this.label,
      followedWallet: normalized,
      followWalletCount: this.followedWallets.length,
      enhancedWs: !!this.enhancedWs,
    });
  }

  private stopFollowWalletMonitor(normalized: string): void {
    const monitor = this.followMonitors.get(normalized);
    if (!monitor) return;
    monitor.stop();
    this.followMonitors.delete(normalized);
  }

  private stopAllFollowMonitors(): void {
    for (const address of [...this.followMonitors.keys()]) {
      this.stopFollowWalletMonitor(address);
    }
  }

  isFollowWalletMonitoringActive(): boolean {
    return this.followMonitors.size > 0;
  }

  isFollowWalletPaused(): boolean {
    return this.followWalletPaused;
  }

  async pauseFollowWalletMonitoring(): Promise<void> {
    this.followWalletPaused = true;
    this.stopAllFollowMonitors();
    this.log.info('Follow-wallet monitoring paused', {
      followWallets: this.followedWallets,
    });
    this.followWalletBackend('Follow-wallet monitoring paused', {
      bot: this.label,
      followWallets: this.followedWallets,
    });
  }

  async resumeFollowWalletMonitoring(): Promise<void> {
    this.followWalletPaused = false;
    await this.followAllWallets();
  }

  async removeFollowWallet(address: string): Promise<void> {
    let normalized: string;
    try {
      normalized = new PublicKey(address.trim()).toBase58();
    } catch {
      throw new Error("Invalid Solana wallet address.");
    }
    if (!this.followedWallets.includes(normalized)) {
      return;
    }
    this.stopFollowWalletMonitor(normalized);
    this.followedWallets = this.followedWallets.filter((w) => w !== normalized);
    this.log.info("Insider follow wallet removed", {
      followedWallet: normalized,
      remainingFollowWallets: this.followedWallets,
    });
    this.followWalletBackend("Follow wallet removed", {
      bot: this.label,
      removedWallet: normalized,
      remainingFollowWallets: this.followedWallets,
    });
  }

  configureFollowWallet(address: string): void {
    const normalized = new PublicKey(address).toBase58();
    if (this.followedWallets.includes(normalized)) return;
    if (this.followedWallets.length >= MAX_FOLLOW_WALLETS) return;
    this.followedWallets.push(normalized);
  }

  async stop(): Promise<void> {
    await this.stopFlowMonitoring();
    this.stopAllFollowMonitors();
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
    this.stopAllFollowMonitors();
  }

  async stopForHeliusCredits(): Promise<void> {
    if (this.stoppedForHeliusCredits) return;
    this.stoppedForHeliusCredits = true;
    await this.stopFlowMonitoring();
    this.stopAllFollowMonitors();
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

    if (this.followTokenTopBuyerWallet && this.followTokenTopBuyerMint === trigger.mint) {
      this.ensureFollowTokenTopBuyerWatchSubscribed();
    }

    void this.syncBundlerFunderTransactions(true);
    void this.syncParallelFeePayerFunderTransactions(true);
    void this.syncFunderRecipientBatch(true);
    void this.auditFunderRecipientsAfterBuy();

    void this.executeFollowTokenEarlyBundlerPostBuyExitPlan();
  }

  private async executeFollowTokenEarlyBundlerPostBuyExitPlan(): Promise<void> {
    const state = this.followTokenEarlyBundlerExitState;
    if (!state?.active || !state.allSoldAllComplete || state.exitTriggerSignature) {
      return;
    }
    if (this.phase !== "holding" || this.positionSellTriggered) return;

    await this.maybeEvaluateFollowTokenEarlyBundlerExit();
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
    followedWallet: string,
  ): Promise<void> {
    if (!this.isFollowWallet(followedWallet)) return;
    this.followWalletBackend("Follow-wallet buy detected", {
      bot: this.label,
      mint,
      signature,
      followedWallet,
    });
    if (this.boughtMints.has(mint)) return;
    if (this.claimMint && !this.claimMint(mint)) {
      this.log.info(
        "Mint active on other insider bot; ignoring follow-wallet buy",
        {
          mint,
          signature,
          followedWallet,
        },
      );
      return;
    }

    if (this.activePosition || this.watchingMint) {
      if (this.followWalletFlowDelegate) {
        const delegated = await this.followWalletFlowDelegate(
          mint,
          signature,
          followedWallet,
        );
        if (delegated) return;
      }
      this.log.info(
        "Follow-wallet buy ignored because this bot is already on a token flow",
        {
          mint,
          signature,
          followedWallet,
          watchingMint: this.watchingMint,
          activePosition: this.activePosition?.mint ?? null,
        },
      );
      return;
    }

    await this.startFromFollowWalletBuy(mint, signature, followedWallet);
  }

  private isMintIndexingLagError(error: unknown): boolean {
    const message = error instanceof Error ? error.message : String(error);
    return /No transactions found for mint yet|No SWAP transactions found for mint yet/i.test(
      message,
    );
  }

  private delay(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  /**
   * A followed-wallet buy is a real, valuable signal — it shouldn't be
   * permanently dropped just because Helius hasn't finished indexing/
   * classifying the mint's very first transactions yet (common for tokens
   * that are only seconds old). startInsiderFlow already fans its Helius
   * request out across the whole key pool for this specific error (see
   * isTransientHeliusError), but if every key still comes back empty, retry
   * the whole flow a few more times with real-world delays in between
   * before finally giving up and letting the caller reset for a new token.
   */
  private async startInsiderFlowWithIndexingLagRetry(
    mint: string,
    followedWallet: string,
    maxRetries = 2,
  ): Promise<void> {
    for (let attempt = 0; ; attempt += 1) {
      try {
        await this.startInsiderFlow(mint, followedWallet);
        return;
      } catch (err) {
        if (attempt >= maxRetries || !this.isMintIndexingLagError(err)) {
          throw err;
        }
        const delayMs = (attempt + 1) * 4_000;
        this.log.warn(
          "Followed-wallet mint not indexed by Helius yet; retrying after delay",
          {
            mint,
            followedWallet,
            attempt: attempt + 1,
            maxRetries,
            delayMs,
            error: err instanceof Error ? err.message : String(err),
          },
        );
        await this.delay(delayMs);
      }
    }
  }

  private async startInsiderFlowFromMigrationWithIndexingLagRetry(
    mint: string,
    migrationSignature: string,
    migrationAgeSec?: number,
    migrationDevWallet?: string,
  ): Promise<boolean> {
    const delaysMs = [0, 4_000, 8_000];
    for (let attempt = 0; attempt < delaysMs.length; attempt += 1) {
      const delayMs = delaysMs[attempt] ?? 0;
      if (delayMs > 0) {
        await new Promise((resolve) => setTimeout(resolve, delayMs));
      }
      try {
        return await this.startInsiderFlowFromMigration(
          mint,
          migrationSignature,
          migrationAgeSec,
          migrationDevWallet,
        );
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        const retryable =
          message.includes("No SWAP transactions found") ||
          message.includes("Failed to fetch early insider swaps");
        if (!retryable || attempt === delaysMs.length - 1) {
          throw err;
        }
        this.log.warn("Follow-token insider flow waiting for Helius indexing", {
          mint,
          migrationSignature,
          attempt: attempt + 1,
          error: message,
        });
      }
    }
    return false;
  }

  private isFollowTokenFlowActive(mint: string): boolean {
    return (
      this.flowSource === "follow-token" &&
      (this.watchingMint === mint ||
        this.bundlerFunderWatch?.mint === mint ||
        this.activePosition?.mint === mint)
    );
  }

  private async startInsiderFlowFromMigration(
    mint: string,
    migrationSignature: string,
    migrationAgeSec?: number,
    migrationDevWallet?: string,
  ): Promise<boolean> {
    this.flowSource = "follow-token";
    this.flowFollowWallet = null;
    this.funderFirstFeePayer = null;
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

    if (earlyBundlerWallets.length < BUNDLER_FUNDER_REQUIRED_COUNT) {
      this.log.warn(
        "Follow-token migration has fewer than four first unique bundler buys; resetting",
        {
          mint,
          migrationSignature,
          earlyBundlerCount: earlyBundlerWallets.length,
        },
      );
      await this.resetForNewToken(true);
      return false;
    }

    if (
      await this.trySkipLowFundingDisabledFromEarlyBuys(mint, earlyInsiderBuys)
    ) {
      return false;
    }

    this.preBuyStopped = false;
    this.positionSellTriggered = false;
    this.monitoredWallet = null;
    this.insiderState = null;
    this.phase = "pre_buy";

    void this.sendTelegramSafe(
      [
        `<b>🔍 ${this.label} Follow-Token Large Insider Flow Started</b>`,
        `Token: <code>${mint}</code>`,
        `Migration tx: <code>${migrationSignature}</code>`,
        `First unique bundler wallets: <b>${earlyBundlerWallets.length}</b>`,
        "",
        `Buy trigger: Large Insider flow (valid wallet <b>#${FOLLOW_TOKEN_LARGE_INSIDER_BUY_AT_VALID_WALLET_COUNT}</b> or early bundler sold-all path).`,
        "Early bundler / transfer-recipient watch started (pre–1st-LI sold-all buy gate active).",
        "Bundler feePayer backtrack skipped for follow-token (Helius early-buy anchor).",
      ].join("\n"),
      "follow-token flow-start notification",
    );

    this.startPollLoop();
    await this.startFollowTokenLargeInsiderPreBuyFlow(
      mint,
      earlyInsiderBuys,
      migrationAgeSec,
      migrationDevWallet,
    );
    return this.isFollowTokenFlowActive(mint);
  }

  private buildFollowTokenStubBundlerWatch(
    mint: string,
    firstFour: EarlyInsiderBuy[],
  ): BundlerFunderWatchState {
    const placeholderFunder =
      this.devWallet ?? firstFour[0]?.feePayer ?? firstFour[0]?.wallet ?? mint;
    return {
      mint,
      funderWallet: placeholderFunder,
      originalFunderWallet: placeholderFunder,
      migrationCount: 0,
      lowFundingMode: false,
      earliestFundingTimestamp: 0,
      earliestFundingSignature: "",
      largestFundingSol: 0,
      minTransferOutSol: 0,
      cursorSignature: null,
      processedSignatures: new Set<string>(),
      validOutSignatures: new Set<string>(),
      invalidOutSignatures: new Set<string>(),
      bundlerWallets: new Set(firstFour.map((buy) => buy.wallet)),
      recipientWatches: new Map<string, FunderRecipientWatch>(),
      queuedTransferOuts: [],
      normalTinyTransferOuts: [],
      normalTinyRoundGroupFound: false,
      roundWonDustRaceNotified: false,
      lowFundingFunderTxs: [],
      lowFundingTinyTransferOuts: [],
      lowFundingTinyBundlerGateSeen: false,
      lowFundingTinyEntryTimestamp: null,
      lowFundingTinyCandidateWallets: new Set<string>(),
      lowFundingTinySellGroupSignatures: new Set<string>(),
      lowFundingTinyBoughtUsdBands: new Set(),
      lowFundingTinySoldUsdBands: new Set(),
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
      discoveryStopped: true,
      lockedAt: Date.now(),
      parallelFeePayerFunderWallet: null,
      parallelFeePayerFunderCursorSignature: null,
      parallelFeePayerFunderFundedAtSec: null,
    };
  }

  /**
   * Follow-token pre-buy path: skip bundler funding feePayer backtrack and start
   * Large Insider + early bundler exit monitoring directly from Helius first-four
   * SWAP buys (migrated tokens often lack zero-balance funding windows).
   */
  private async startFollowTokenLargeInsiderPreBuyFlow(
    mint: string,
    earlyBuys: EarlyInsiderBuy[],
    migrationAgeSec?: number,
    migrationDevWallet?: string,
  ): Promise<void> {
    const firstFour = earlyBuys.slice(0, BUNDLER_FUNDER_REQUIRED_COUNT);
    if (firstFour.length < BUNDLER_FUNDER_REQUIRED_COUNT) {
      this.log.warn("Follow-token pre-buy flow has fewer than four early bundler buys; resetting", {
        mint,
        earlyBuyCount: firstFour.length,
      });
      await this.resetForNewToken(true);
      return;
    }

    if (migrationDevWallet) {
      this.devWallet = migrationDevWallet;
    } else {
      await this.ensureDevWalletLoaded(mint);
    }
    if (!this.devCreateTimestamp) {
      this.log.warn(
        "Follow-token pre-buy flow skipped because dev CREATE timestamp is unavailable",
        { mint },
      );
      await this.resetForNewToken(true);
      return;
    }

    this.bundlerFunderWatch = this.buildFollowTokenStubBundlerWatch(mint, firstFour);
    this.followTokenEarlyInsiderBuys = firstFour;
    const immediateMigrationBuy =
      migrationAgeSec !== undefined &&
      migrationAgeSec >= 2 &&
      migrationAgeSec <= 4;
    if (immediateMigrationBuy) {
      this.fastMigrationSellMode =
        migrationAgeSec === 4
          ? "transfer_out_mc_retrace"
          : "transfer_out_immediate";
      const firstBuy = firstFour[0];
      await this.emitFollowTokenLargeInsiderBuy(
        this.bundlerFunderWatch,
        firstBuy.wallet,
        firstBuy.signature,
        {
          signature: firstBuy.signature,
          slot: 0,
          timestamp: firstBuy.timestamp,
          type: "SWAP",
        },
        {
          triggerSource: "migration_2_4s_immediate",
          profitExitPercent: FOLLOW_TOKEN_FAST_MIGRATION_PROFIT_EXIT_PERCENT,
          buySolOverride: this.followToken16mPostLiBuySol,
          skipAthGate: true,
        },
      );
      this.log.warn("Follow-token immediate migration buy emitted before normal LI setup", {
        mint,
        migrationAgeSec,
        buySol: this.followToken16mPostLiBuySol,
        profitExitPercent: FOLLOW_TOKEN_FAST_MIGRATION_PROFIT_EXIT_PERCENT,
        normalLargeInsiderSetup: "deferred",
      });
      void this.startFollowTokenLargeInsiderPreBuyFlow(mint, firstFour);
      return;
    }
    if (this.devWallet) {
      this.subscribeDevWalletFullExitWatch();
    }

    void this.startFollowTokenEarlyBundlerExitMonitoring(mint);

    const watchState = this.bundlerFunderWatch;
    if (!watchState) return;

    const stubSecondGroup =
      this.buildFollowTokenStubSecondGroupFromInitialBundlers(watchState);
    const largeInsiderStarted = await this.startFollowTokenLargeInsiderFlow(
      watchState,
      stubSecondGroup,
      "large_insider_pre_buy_started",
    );
    if (!largeInsiderStarted) {
      await this.resetFollowTokenAfterLargeInsiderStartFailed(
        mint,
        "large_insider_pre_buy_started",
      );
      return;
    }

    this.log.warn("Follow-token Large Insider pre-buy flow started", {
      mint,
      devCreateTimestamp: this.devCreateTimestamp,
      initialBundlers: [...this.bundlerFunderWatch.bundlerWallets],
    });
    void this.sendTelegramSafe(
      [
        `<b>✅ ${this.label} Follow-Token Large Insider Watch Started</b>`,
        `Token: <code>${mint}</code>`,
        `Dev CREATE: <b>${this.devCreateTimestamp}</b>`,
        `Initial bundlers: <b>${firstFour.length}</b>`,
        "",
        `Large Insider active — buy on valid wallet <b>#${FOLLOW_TOKEN_LARGE_INSIDER_BUY_AT_VALID_WALLET_COUNT}</b> or early bundler sold-all.`,
        `Exit: +${FOLLOW_TOKEN_LARGE_INSIDER_PROFIT_EXIT_PERCENT}% MC TP · any valid wallet ≥25% sell early exit.`,
      ].join("\n"),
      "follow-token large insider watch started notification",
    );

  }

  private async startInsiderFlow(
    mint: string,
    followedWallet: string,
  ): Promise<void> {
    this.flowSource = "follow";
    this.flowFollowWallet = followedWallet;
    this.funderFirstFeePayer = null;
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

    if (!earlyBundlerWallets.includes(followedWallet)) {
      this.log.warn(
        "Follow wallet is not one of the first four unique bundler first-buy wallets; resetting token flow",
        {
          mint,
          followedWallet,
          earlyBundlers: earlyBundlerWallets,
        },
      );
      await this.resetForNewToken(true);
      return;
    }

    if (
      await this.trySkipLowFundingDisabledFromEarlyBuys(mint, earlyInsiderBuys)
    ) {
      return;
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
        this.formatFollowWalletTelegramLine() ||
          `Follow wallet: <code>${followedWallet}</code>`,
        `First unique bundler wallets: <b>${earlyBundlerWallets.length}</b>`,
        "",
        "Finding each bundler's zero-balance funding window, selecting the latest post-zero funding transfer above 15 SOL, requiring those funding txs to share one feePayer, then watching that feePayer's transfer-outs for recipient buy confirmation.",
      ].filter(Boolean).join("\n"),
      "flow-start notification",
    );
    this.followWalletBackend("Follow-wallet bundler-funder flow started", {
      bot: this.label,
      mint,
      followedWallet,
      earlyBundlerWallets,
    });

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
    return extractFirstUniqueEarlyBundlerBuys(
      swaps,
      mint,
      BUNDLER_FUNDER_REQUIRED_COUNT,
      (tx, wallet) => this.estimateEarlyBuySol(tx, wallet),
    );
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
    const fromTransfers = this.estimateWalletSolSpent(tx, buyWallet);
    if (fromTransfers !== null) return fromTransfers;

    const accountEntry = tx.accountData?.find((a) => a.account === buyWallet);
    if (
      accountEntry?.nativeBalanceChange !== undefined &&
      accountEntry.nativeBalanceChange < 0
    ) {
      return parseFloat(
        (
          -accountEntry.nativeBalanceChange / LAMPORTS_PER_SOL
        ).toFixed(6),
      );
    }

    return tx.feePayer ? this.estimateWalletSolSpent(tx, tx.feePayer) : null;
  }

  /**
   * When low-funding mode is off, skip tokens whose first-four bundler buys
   * clearly spent less than the normal-mode threshold — avoids ~40+ Helius
   * balance-at / funding-history calls that would reject the token anyway.
   */
  private async trySkipLowFundingDisabledFromEarlyBuys(
    mint: string,
    buys: EarlyInsiderBuy[],
  ): Promise<boolean> {
    if (BUNDLER_FUNDER_LOW_FUNDING_MODE_ENABLED) return false;
    if (buys.length < BUNDLER_FUNDER_REQUIRED_COUNT) return false;

    const buySols = buys.map((buy) => buy.buySol);
    if (buySols.some((sol) => sol === null)) return false;

    const maxBuySol = Math.max(...(buySols as number[]));
    const normalFundingMinSol = this.getNormalFundingMinSol();
    if (maxBuySol >= normalFundingMinSol) return false;

    this.log.warn(
      "Low-funding mode disabled; skipping token from early bundler buy SOL (before funding REST)",
      {
        mint,
        maxBuySol,
        normalFundingThresholdSol: normalFundingMinSol,
        bundlerBuySols: buys.map((buy) => ({
          wallet: buy.wallet,
          buySol: buy.buySol,
          signature: buy.signature,
        })),
      },
    );
    await this.skipLowFundingDisabledToken(mint, null, maxBuySol, normalFundingMinSol);
    return true;
  }

  private async skipLowFundingDisabledToken(
    mint: string,
    funderWallet: string | null,
    largestFundingSol: number,
    normalFundingThresholdSol = this.getNormalFundingMinSol(),
  ): Promise<void> {
    void this.sendTelegramSafe(
      [
        `<b>⏭️ ${this.label} Low-Funding Mode Disabled — Token Skipped</b>`,
        `Token: <code>${mint}</code>`,
        this.formatFollowWalletTelegramLine(),
        funderWallet ? `FeePayer: <code>${funderWallet}</code>` : "",
        `Largest bundler funding: <b>${largestFundingSol.toFixed(4)} SOL</b> (below the ${normalFundingThresholdSol} SOL normal-mode threshold)`,
        "Low-funding mode is currently disabled — resetting to watch for the next token.",
      ]
        .filter(Boolean)
        .join("\n"),
      "low-funding mode disabled skip notification",
    );
    await this.resetForNewToken(true);
  }

  private async ensureDevWalletLoaded(mint: string): Promise<void> {
    if (this.devWallet) return;
    const createTx = await this.withHeliusFallback((client) =>
      client.getMintCreateTransaction(mint),
    );
    this.devWallet = createTx?.feePayer ?? null;
    this.devCreateSignature = createTx?.signature ?? null;
    this.devCreateTimestamp = createTx?.timestamp ?? null;
    if (!this.devWallet) return;
    this.log.info("Dev wallet identified for trader-scan exclusions", {
      mint,
      devWallet: this.devWallet,
      devCreateSignature: this.devCreateSignature,
      devCreateTimestamp: this.devCreateTimestamp,
    });
    this.subscribeDevWalletFullExitWatch();
  }

  private getNativePostBalanceSol(
    tx: HeliusTransaction,
    wallet: string,
  ): number | null {
    const entry = tx.accountData?.find((a) => a.account === wallet);
    if (entry?.nativePostBalance === undefined) return null;
    return entry.nativePostBalance / LAMPORTS_PER_SOL;
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
    // A brand-new mint may not have any transactions indexed/classified by
    // Helius yet — this is an indexing-lag issue, not a permanent failure of
    // this particular API key, so it's worth cycling through the rest of the
    // Helius pool (each key gets its own multi-attempt retry loop inside e.g.
    // getEarlyInsiderSwaps/getEarlyBundlers) rather than giving up after just
    // one key's ~3.7s retry budget.
    if (/No transactions found for mint yet|No SWAP transactions found for mint yet/i.test(message)) {
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
      const resolved = await this.resolveBundlerFundingRecordsSequential(
        mint,
        firstFour,
      );
      if (resolved === null) {
        return;
      }
      fundingRecords = resolved;

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
    const normalFundingMinSol = this.getNormalFundingMinSol();
    const lowFundingMode = largestFundingSol < normalFundingMinSol;
    const funderWallet = records[0].fundingFeePayer;
    if (lowFundingMode && !BUNDLER_FUNDER_LOW_FUNDING_MODE_ENABLED) {
      this.log.warn(
        "Low-funding mode is disabled; skipping token because its shared feePayer's largest funding is below the normal-mode threshold",
        {
          mint,
          funderWallet,
          largestFundingSol,
          normalFundingThresholdSol: normalFundingMinSol,
        },
      );
      await this.skipLowFundingDisabledToken(
        mint,
        funderWallet,
        largestFundingSol,
        normalFundingMinSol,
      );
      return;
    }

    await this.ensureDevWalletLoaded(mint);

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
      minTransferOutSol: 0,
      cursorSignature: latest.fundingSignature,
      processedSignatures: new Set(records.map((record) => record.fundingSignature)),
      validOutSignatures: new Set<string>(),
      invalidOutSignatures: new Set<string>(),
      bundlerWallets: new Set(firstFour.map((buy) => buy.wallet)),
      recipientWatches: new Map<string, FunderRecipientWatch>(),
      queuedTransferOuts: [],
      normalTinyTransferOuts: [],
      normalTinyRoundGroupFound: false,
      roundWonDustRaceNotified: false,
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
      parallelFeePayerFunderWallet: null,
      parallelFeePayerFunderCursorSignature: null,
      parallelFeePayerFunderFundedAtSec: null,
    };

    this.subscribeBundlerFunder(funderWallet);
    await this.maybeHandoffEmptyBundlerFunderAtStartupChain(this.bundlerFunderWatch!);
    if (
      !this.bundlerFunderWatch ||
      this.bundlerFunderWatch.mint !== mint ||
      this.watchingMint !== mint ||
      this.phase !== "pre_buy"
    ) {
      return;
    }
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
    if (this.bundlerFunderWatch) {
      await this.maybeSubscribeFollowTokenFeePayerFunderWatch(this.bundlerFunderWatch);
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
        this.formatFollowWalletTelegramLine(),
        `FeePayer: <code>${funderWallet}</code>`,
        `Largest bundler funding: <b>${largestFundingSol.toFixed(4)} SOL</b>`,
        activeFunderWatch.lowFundingMode
          ? `Watching feePayer tiny transfer-outs: <b>$${BUNDLER_FUNDER_LOW_FUNDING_TINY_MIN_BUY_USD.toFixed(2)}-$${BUNDLER_FUNDER_NORMAL_TINY_TRANSFER_OUT_MAX_USD.toFixed(0)}</b>`
          : `Watching feePayer tiny transfer-outs: ${formatNormalTinyRoundSolWatchDescription()} (≥${normalFundingMinSol} SOL funded)`,
        "",
        activeFunderWatch.lowFundingMode
          ? "Low-funding mode uses tiny same-band groups only."
          : this.flowSource === "follow-token"
            ? `Follow-token buy triggers: Large Insider on valid wallet <b>#${FOLLOW_TOKEN_LARGE_INSIDER_BUY_AT_VALID_WALLET_COUNT}</b> or early bundler sold-all (still watch for #5 · +${FOLLOW_TOKEN_LARGE_INSIDER_PROFIT_EXIT_PERCENT}% MC TP · any valid wallet ≥25% sell early exit). Reset on 20m window close before buy. Large Insider: feePayer ≥${FOLLOW_TOKEN_LARGE_INSIDER_MIN_FEEPAYER_OUT_SOL} SOL outs (${FOLLOW_TOKEN_LARGE_INSIDER_FEEPAYER_WINDOW_SEC / 60}m after initial bundler first buy) → downstream watches only on ≥${FOLLOW_TOKEN_LARGE_INSIDER_MIN_CHAIN_OUT_SOL} SOL outs (tier1→chain→…). Round/dust gates disabled.`
            : `Round groups, dust race-to-${BUNDLER_FUNDER_NORMAL_TINY_MIN_ROUND_GROUP_TXS_FOR_BUY}, and recipient first-buy gates apply.`,
        activeFunderWatch.parallelFeePayerFunderWallet
          ? `Parallel feePayer funder (≤6h): <code>${activeFunderWatch.parallelFeePayerFunderWallet}</code>`
          : "",
      ].filter(Boolean).join("\n"),
      "shared feePayer notification",
    );

  }

  /**
   * Locks a feePayer discovered by funder-first upstream logic — skips the
   * four-bundler funding-record backtrack since the feePayer is already known.
   */
  private async startKnownFeePayerBundlerFlow(
    mint: string,
    feePayer: string,
    earlyBuys: EarlyInsiderBuy[],
  ): Promise<void> {
    const firstFour = earlyBuys.slice(0, BUNDLER_FUNDER_REQUIRED_COUNT);
    if (firstFour.length < BUNDLER_FUNDER_REQUIRED_COUNT) {
      this.log.warn("Funder-first token has fewer than four early bundler buys; resetting", {
        mint,
        feePayer,
        earlyBuyCount: firstFour.length,
      });
      await this.resetForNewToken(true);
      return;
    }

    const largestFundingSol = BUNDLER_FUNDER_LOW_FUNDING_SOL;
    const latestBundlerBuy = firstFour.reduce((best, buy) =>
      buy.timestamp > best.timestamp ? buy : best,
    );

    this.bundlerFunderWatch = {
      mint,
      funderWallet: feePayer,
      originalFunderWallet: feePayer,
      migrationCount: 0,
      lowFundingMode: false,
      earliestFundingTimestamp: latestBundlerBuy.timestamp,
      earliestFundingSignature: latestBundlerBuy.signature,
      largestFundingSol,
      minTransferOutSol: 0,
      cursorSignature: latestBundlerBuy.signature,
      processedSignatures: new Set<string>(),
      validOutSignatures: new Set<string>(),
      invalidOutSignatures: new Set<string>(),
      bundlerWallets: new Set(firstFour.map((buy) => buy.wallet)),
      recipientWatches: new Map<string, FunderRecipientWatch>(),
      queuedTransferOuts: [],
      normalTinyTransferOuts: [],
      normalTinyRoundGroupFound: false,
      roundWonDustRaceNotified: false,
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
      parallelFeePayerFunderWallet: null,
      parallelFeePayerFunderCursorSignature: null,
      parallelFeePayerFunderFundedAtSec: null,
    };

    this.subscribeBundlerFunder(feePayer);
    await this.maybeHandoffEmptyBundlerFunderAtStartupChain(this.bundlerFunderWatch!);
    if (
      !this.bundlerFunderWatch ||
      this.bundlerFunderWatch.mint !== mint ||
      this.watchingMint !== mint ||
      this.phase !== "pre_buy"
    ) {
      return;
    }
    if (!this.buySubmitted) {
      await this.syncBundlerFunderTransactions(true);
    }
    if (this.bundlerFunderWatch) {
      await this.maybeSubscribeFollowTokenFeePayerFunderWatch(this.bundlerFunderWatch);
    }

    const activeFunderWatch = this.bundlerFunderWatch;
    if (
      !activeFunderWatch ||
      activeFunderWatch.mint !== mint ||
      this.watchingMint !== mint ||
      this.phase !== "pre_buy"
    ) {
      return;
    }

    this.log.warn("Funder-first feePayer locked; shared feePayer watch started", {
      mint,
      sharedFeePayer: feePayer,
      largestFundingSol,
      bundlerWallets: [...activeFunderWatch.bundlerWallets],
    });
    void this.sendTelegramSafe(
      [
        `<b>✅ ${this.label} Funder-First FeePayer Locked</b>`,
        `Token: <code>${mint}</code>`,
        `FeePayer: <code>${feePayer}</code>`,
        `Mode: <b>Normal (funder-first)</b>`,
        `Largest bundler funding: <b>≥${BUNDLER_FUNDER_LOW_FUNDING_SOL} SOL</b>`,
        `Watching feePayer tiny transfer-outs: ${formatNormalTinyRoundSolWatchDescription()}`,
        "",
        `Round groups, dust race-to-${BUNDLER_FUNDER_NORMAL_TINY_MIN_ROUND_GROUP_TXS_FOR_BUY}, and recipient first-buy gates apply.`,
      ].join("\n"),
      "funder-first feePayer locked notification",
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

  private async resolveBundlerFundingRecordsSequential(
    mint: string,
    firstFour: EarlyInsiderBuy[],
  ): Promise<Array<BundlerFundingRecord | null> | null> {
    const records: Array<BundlerFundingRecord | null> = [];
    for (let index = 0; index < firstFour.length; index += 1) {
      const buy = firstFour[index]!;
      const record = await this.findValidBundlerFundingRecord(mint, buy, index);
      records.push(record);
      if (!record) continue;
      const normalFundingMinSol = this.getNormalFundingMinSol();
      if (
        !BUNDLER_FUNDER_LOW_FUNDING_MODE_ENABLED &&
        record.amountSol < normalFundingMinSol
      ) {
        this.log.warn(
          "Low-funding mode disabled; aborting remaining bundler funding lookups after first sub-threshold record",
          {
            mint,
            bundlerWallet: buy.wallet,
            fundingSol: record.amountSol,
            resolvedCount: records.filter(Boolean).length,
            remaining: firstFour.length - index - 1,
            normalFundingThresholdSol: normalFundingMinSol,
          },
        );
        await this.skipLowFundingDisabledToken(
          mint,
          record.fundingFeePayer,
          record.amountSol,
          normalFundingMinSol,
        );
        return null;
      }
    }
    return records;
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
      index: number;
    }> = [];
    let zeroBoundary: {
      signature: string;
      timestamp: number;
      balance: number;
    } | null = null;

    for (let index = 0; index < txs.length; index += 1) {
      const tx = txs[index];
      const currentBalance =
        this.getNativePostBalanceSol(tx, buy.wallet) ??
        (await this.fetchSolBalanceAt(
          buy.wallet,
          tx.timestamp,
          preferredClientIndex,
        ));
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
        index,
      });
    }

    if (!zeroBoundary && txs.length > 0) {
      const oldestPreBuyTx = txs[txs.length - 1]!;
      const freshWallet = await this.isWalletFirstTransaction(
        buy.wallet,
        oldestPreBuyTx.signature,
        preferredClientIndex,
      );
      if (freshWallet) {
        zeroBoundary = {
          signature: oldestPreBuyTx.signature,
          timestamp: oldestPreBuyTx.timestamp,
          balance: 0,
        };
        this.log.info(
          "Bundler funding fresh-wallet window — oldest pre-buy tx is wallet first tx (no zero boundary)",
          {
            mint,
            bundlerWallet: buy.wallet,
            bundlerBuySignature: buy.signature,
            oldestPreBuySignature: oldestPreBuyTx.signature,
            transferCount: txs.length,
            candidateCount: candidates.length,
          },
        );
      }
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

    const qualifiedCandidates = candidates.filter((candidate) =>
      bundlerFundingIncomingQualifies(candidate.incoming.amountSol),
    );
    if (!qualifiedCandidates.length) {
      this.log.warn(
        "No post-zero funding transfer above min incoming SOL for bundler",
        {
          mint,
          bundlerWallet: buy.wallet,
          bundlerBuySignature: buy.signature,
          zeroBoundary,
          transferCount: txs.length,
          candidateCount: candidates.length,
          minSelectedFundingSol: BUNDLER_FUNDER_MIN_SELECTED_FUNDING_SOL,
          candidates: candidates.map((candidate) => ({
            signature: candidate.tx.signature,
            incomingAmountSol: candidate.incoming.amountSol,
            currentBalance: candidate.currentBalance,
            timestamp: candidate.tx.timestamp,
            drainIncomingPattern: isBundlerFundingDrainIncomingPattern(
              candidate.incoming.amountSol,
              candidate.currentBalance,
            ),
          })),
        },
      );
      return null;
    }

    const selected = qualifiedCandidates[0];
    const latestWindowFunding = candidates[0];
    const skippedLatestDrainCandidate =
      selected !== latestWindowFunding &&
      isBundlerFundingDrainIncomingPattern(
        latestWindowFunding.incoming.amountSol,
        latestWindowFunding.currentBalance,
      );
    this.log.warn("Bundler funding transfer selected from post-zero window", {
      mint,
      bundlerWallet: buy.wallet,
      bundlerBuySignature: buy.signature,
      fundingSignature: selected.tx.signature,
      fundingFeePayer: selected.tx.feePayer,
      senderWallet: selected.incoming.from,
      amountSol: selected.incoming.amountSol,
      incomingAmountSol: selected.incoming.amountSol,
      timestamp: selected.tx.timestamp,
      latestWindowFundingSignature: latestWindowFunding.tx.signature,
      latestWindowFundingTimestamp: latestWindowFunding.tx.timestamp,
      latestWindowIncomingAmountSol: latestWindowFunding.incoming.amountSol,
      latestWindowCurrentBalance: latestWindowFunding.currentBalance,
      skippedLatestDrainCandidate,
      currentBalance: selected.currentBalance,
      zeroBoundary,
      candidateCount: candidates.length,
      qualifiedCandidateCount: qualifiedCandidates.length,
      minSelectedFundingSol: BUNDLER_FUNDER_MIN_SELECTED_FUNDING_SOL,
      selectionRule:
        "latest post-zero transfer with incomingAmountSol > minSelectedFundingSol; when the newest tx shows dust incoming + high balance (drain pattern), scan older txs in the fetched batch for the latest qualifying incoming transfer",
      candidates: candidates.map((candidate) => ({
        signature: candidate.tx.signature,
        fundingFeePayer: candidate.tx.feePayer,
        senderWallet: candidate.incoming.from,
        incomingAmountSol: candidate.incoming.amountSol,
        timestamp: candidate.tx.timestamp,
        currentBalance: candidate.currentBalance,
        drainIncomingPattern: isBundlerFundingDrainIncomingPattern(
          candidate.incoming.amountSol,
          candidate.currentBalance,
        ),
        index: candidate.index,
      })),
    });
    return {
      bundlerWallet: buy.wallet,
      bundlerBuySignature: buy.signature,
      fundingSignature: selected.tx.signature,
      fundingFeePayer: selected.tx.feePayer!,
      senderWallet: selected.incoming.from,
      amountSol: selected.incoming.amountSol,
      timestamp: selected.tx.timestamp,
      latestWindowFundingSignature: latestWindowFunding.tx.signature,
      latestWindowFundingTimestamp: latestWindowFunding.tx.timestamp,
    };
  }

  private async isWalletFirstTransaction(
    wallet: string,
    signature: string,
    preferredClientIndex: number,
  ): Promise<boolean> {
    const firstTxs = await this.withHeliusFallback(
      (client) => client.getAddressTransactionsAsc(wallet, undefined, 1),
      preferredClientIndex,
    );
    return firstTxs[0]?.signature === signature;
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
    return this.withHeliusFallback(
      (client) => client.getWalletBalanceAt(wallet, mint, timestamp),
      preferredClientIndex,
    );
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
    // Push-only: Enhanced WSS handles tx detection; no REST poll/backstop loop.
  }

  private stopPollLoop(): void {
    if (this.pollTimer) {
      clearInterval(this.pollTimer);
      this.pollTimer = null;
    }
  }

  /** Fed by a fresh Enhanced WSS `transactionSubscribe` notification  /** Fed by a fresh Enhanced WSS `transactionSubscribe` notification for the monitored insider wallet, post-buy bundler wallet, or early-bundler exit wallet — already fully parsed, no REST fetch/batching needed. Mirrors the net effect of queueSignature -> processSignatureBatch for the push path. */
  private handleEnhancedWsMintTx(
    tx: HeliusTransaction,
    context: "insider" | "bundler" | "early_bundler_exit",
    bundlerWallet?: string,
  ): void {
    if (this.processedSignatures.has(tx.signature)) return;
    const mint = this.watchingMint ?? this.activePosition?.mint;
    if (!mint || !this.isRelevantMintTx(tx, mint)) return;
    this.processedSignatures.add(tx.signature);
    if (context === "insider") {
      void this.handleInsiderTransaction(tx, mint);
    } else if (context === "bundler" && bundlerWallet) {
      void this.handleBundlerTransaction(tx, mint, bundlerWallet);
    } else if (context === "early_bundler_exit" && bundlerWallet) {
      void this.applyFollowTokenEarlyBundlerExitTx(tx, mint, bundlerWallet);
    }
  }

  private startInsiderMonitoring(): void {
    if (!this.monitoredWallet) return;
    this.stopInsiderMonitoring();
    if (this.enhancedWs) {
      this.insiderEnhancedWatchId = this.enhancedWs.watch(this.monitoredWallet, (tx) => {
        this.handleEnhancedWsMintTx(tx, "insider");
      });
      this.log.info("Started pre-buy insider wallet monitoring via Enhanced WSS", {
        wallet: this.monitoredWallet,
      });
      return;
    }
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
    if (this.insiderEnhancedWatchId !== null) {
      const id = this.insiderEnhancedWatchId;
      this.insiderEnhancedWatchId = null;
      await this.enhancedWs?.unwatch(id).catch(() => undefined);
    }
    if (this.insiderLogsSubId !== null) {
      const id = this.insiderLogsSubId;
      this.insiderLogsSubId = null;
      await this.connection.removeOnLogsListener(id).catch(() => undefined);
    }
  }

  private followTokenEarlyBundlerExitWatchHasNeverSold(
    watch: FollowTokenEarlyBundlerExitWatch,
  ): boolean {
    return watch.boughtAmount > 0 && watch.soldAmount === 0;
  }

  private healFollowTokenEarlyBundlerExitWatchErroneousSoldAll(
    watch: FollowTokenEarlyBundlerExitWatch,
  ): boolean {
    if (
      !watch.monitoringActive ||
      !watch.soldAll ||
      !this.followTokenEarlyBundlerExitWatchHasNeverSold(watch)
    ) {
      return false;
    }
    watch.soldAll = false;
    watch.soldAllSignature = null;
    return true;
  }

  private followTokenEarlyBundlerExitSoldAllBlockReason(): string | null {
    const state = this.followTokenEarlyBundlerExitState;
    if (!state?.active || state.watches.size === 0) return "inactive";
    const activeWatches = [...state.watches.values()].filter(
      (watch) => watch.monitoringActive,
    );
    if (activeWatches.length === 0) return "no_active_watches";
    for (const watch of activeWatches) {
      if (this.healFollowTokenEarlyBundlerExitWatchErroneousSoldAll(watch)) {
        return "active_watch_never_sold";
      }
      if (!watch.syncComplete) return "sync_incomplete";
      if (this.followTokenEarlyBundlerExitWatchHasNeverSold(watch)) {
        return "active_watch_never_sold";
      }
      if (!watch.soldAll) return "watch_not_sold_all";
    }
    const largestBag = this.getFollowTokenEarlyBundlerExitLargestBagWatch();
    if (
      largestBag &&
      (!largestBag.soldAll ||
        this.followTokenEarlyBundlerExitWatchHasNeverSold(largestBag))
    ) {
      return "largest_bag_not_sold_all";
    }
    return null;
  }

  private allFollowTokenEarlyBundlerExitWatchesSoldAll(): boolean {
    return this.followTokenEarlyBundlerExitSoldAllBlockReason() === null;
  }

  private markFollowTokenEarlyBundlerExitWatchObservedNonZeroBalance(
    watch: FollowTokenEarlyBundlerExitWatch,
    liveRaw: bigint,
  ): void {
    if (liveRaw > 0n) {
      watch.observedNonZeroTokenBalance = true;
    }
  }

  private followTokenEarlyBundlerExitWatchSoldAllByAtaBalance(
    watch: FollowTokenEarlyBundlerExitWatch,
    liveRaw: bigint | null,
  ): boolean {
    if (liveRaw === null || liveRaw > 0n) return false;
    if (watch.source === "early_bundler") return true;
    if (!watch.observedNonZeroTokenBalance) return false;
    return watch.soldAmount > 0;
  }

  private followTokenEarlyBundlerExitWatchReachedTwentyFivePercent(
    watch: FollowTokenEarlyBundlerExitWatch,
    remainingAmount: number | null,
  ): boolean {
    if (watch.boughtAmount <= 0) return false;
    if (
      watch.soldAmount / watch.boughtAmount >=
      FOLLOW_TOKEN_EARLY_BUNDLER_EXIT_SOLD_FRACTION
    ) {
      return true;
    }
    if (remainingAmount === null) return false;
    return (
      remainingAmount <=
      watch.boughtAmount * (1 - FOLLOW_TOKEN_EARLY_BUNDLER_EXIT_SOLD_FRACTION)
    );
  }

  private getFollowTokenEarlyBundlerExitWatchLiveTokenBalanceRaw(
    wallet: string,
  ): bigint | null {
    const state = this.followTokenEarlyBundlerExitState;
    if (!state) return null;
    return state.tokenAtaLiveBalanceRaw.get(wallet) ?? null;
  }

  private applyFollowTokenEarlyBundlerExitWatchSoldAllFromLiveState(
    watch: FollowTokenEarlyBundlerExitWatch,
    signature: string | null,
  ): void {
    if (this.healFollowTokenEarlyBundlerExitWatchErroneousSoldAll(watch)) {
      return;
    }
    if (watch.soldAll) return;
    const liveRaw = this.getFollowTokenEarlyBundlerExitWatchLiveTokenBalanceRaw(
      watch.wallet,
    );
    const soldAllByTrackedAmount =
      watch.boughtAmount > 0 && watch.soldAmount >= watch.boughtAmount;
    const soldAllByAtaBalance =
      watch.syncComplete &&
      this.followTokenEarlyBundlerExitWatchSoldAllByAtaBalance(watch, liveRaw);
    if (soldAllByTrackedAmount || soldAllByAtaBalance) {
      watch.soldAll = true;
      watch.soldAllSignature = signature ?? watch.soldAllSignature;
      watch.reachedTwentyFivePercentSold = true;
      return;
    }
    if (
      this.followTokenEarlyBundlerExitWatchReachedTwentyFivePercent(watch, null)
    ) {
      watch.reachedTwentyFivePercentSold = true;
    }
  }

  private updateFollowTokenEarlyBundlerExitWatchSoldAll(
    watch: FollowTokenEarlyBundlerExitWatch,
    signature: string | null,
  ): void {
    this.applyFollowTokenEarlyBundlerExitWatchSoldAllFromLiveState(
      watch,
      signature,
    );
  }

  private clearFollowTokenEarlyBundlerExitDeferredSoldAllEvalTimer(): void {
    const state = this.followTokenEarlyBundlerExitState;
    if (!state?.deferredSoldAllEvalTimer) return;
    clearTimeout(state.deferredSoldAllEvalTimer);
    state.deferredSoldAllEvalTimer = null;
  }

  private bumpFollowTokenEarlyBundlerExitWatchMaxSingleSellFromAtaBalanceDrop(
    watch: FollowTokenEarlyBundlerExitWatch,
    previousRaw: bigint,
    totalRaw: bigint,
    mint: string,
  ): void {
    if (previousRaw <= totalRaw) return;
    const dropRaw = previousRaw - totalRaw;
    const dropAmount = Number(dropRaw) / 10 ** PUMP_FUN_TOKEN_RAW_DECIMALS;
    if (dropAmount <= 0) return;
    if (dropAmount <= watch.maxSingleSellTokenAmount) return;
    watch.maxSingleSellTokenAmount = dropAmount;
    this.log.info(
      "Follow-token early bundler max single sell bumped from ATA balance drop",
      {
        mint,
        wallet: watch.wallet,
        dropAmount,
        previousLiveTokenBalanceRaw: previousRaw.toString(),
        liveTokenBalanceRaw: totalRaw.toString(),
      },
    );
  }

  private scheduleFollowTokenEarlyBundlerExitSoldAllEvalFromAta(
    mint: string,
    wallet: string,
  ): void {
    const state = this.followTokenEarlyBundlerExitState;
    if (!state?.active) return;
    this.clearFollowTokenEarlyBundlerExitDeferredSoldAllEvalTimer();
    state.deferredSoldAllEvalTimer = setTimeout(() => {
      state.deferredSoldAllEvalTimer = null;
      void this.maybeEvaluateFollowTokenEarlyBundlerExit();
    }, FOLLOW_TOKEN_EARLY_BUNDLER_ATA_SOLD_ALL_EVAL_DEFER_MS);
    this.log.debug(
      "Deferred follow-token early bundler sold-all eval (ATA before sell tx)",
      {
        mint,
        wallet,
        deferMs: FOLLOW_TOKEN_EARLY_BUNDLER_ATA_SOLD_ALL_EVAL_DEFER_MS,
      },
    );
  }

  private getFollowTokenActiveProfitExitPercent(): number {
    const entryMc = this.getEntryMc();
    const exitMc = this.getExitMc();
    if (entryMc > 0 && exitMc > entryMc) {
      return ((exitMc / entryMc) - 1) * 100;
    }
    return FOLLOW_TOKEN_LARGE_INSIDER_PROFIT_EXIT_PERCENT;
  }

  private shouldDeferFollowTokenLargeInsiderValidWalletTwentyFivePercentExit(): boolean {
    const ebState = this.followTokenEarlyBundlerExitState;
    return (
      !!ebState?.active &&
      !ebState.allSoldAllComplete &&
      this.isFollowTokenLargeInsiderBuyExitMode()
    );
  }

  private anyFollowTokenLargeInsiderValidWalletReachedTwentyFivePercentSold(): boolean {
    const li = this.followTokenLargeInsiderState;
    if (!li?.active) return false;
    for (const wallet of this.getFollowTokenLargeInsiderExitValidWallets()) {
      const watch = li.scrapeWatches.get(wallet);
      if (!watch) continue;
      if (
        this.followTokenLargeInsiderWatchReachedExitSoldThreshold(watch, null)
      ) {
        return true;
      }
    }
    return false;
  }

  private summarizeFollowTokenLargeInsiderValidWalletsAtOrAboveExitSoldThreshold(): Array<{
    index: number;
    wallet: string;
    soldPercent: string;
  }> {
    const li = this.followTokenLargeInsiderState;
    if (!li?.active) return [];
    return li.validWallets.flatMap((wallet, index) => {
      const watch = li.scrapeWatches.get(wallet);
      if (
        !watch ||
        !this.followTokenLargeInsiderWatchReachedExitSoldThreshold(watch, null)
      ) {
        return [];
      }
      const soldFraction = this.followTokenLargeInsiderEffectiveSoldFraction(
        watch,
        null,
      );
      return [
        {
          index: index + 1,
          wallet,
          soldPercent:
            soldFraction !== null
              ? (soldFraction * 100).toFixed(1)
              : "?",
        },
      ];
    });
  }

  private async skipFollowTokenLargeInsiderFromValidWalletTwentyFivePercentAlreadySold(
    mint: string,
    triggerSource: "valid_wallet_4" | "bundler_sold_all",
    triggerTx?: HeliusTransaction,
  ): Promise<void> {
    const state = this.followTokenEarlyBundlerExitState;
    const funderState = this.bundlerFunderWatch;
    if (state) {
      state.preBuyBundlerPathTriggered = true;
      state.exitTriggerSignature =
        triggerTx?.signature ??
        "FOLLOW_TOKEN_SKIP_LI_25PCT_ALREADY_SOLD_BEFORE_BUY";
    }
    const reason = "large_insider_valid_wallet_25pct_already_sold_before_buy";
    const wallets =
      this.summarizeFollowTokenLargeInsiderValidWalletsAtOrAboveExitSoldThreshold();
    const triggerLabel =
      triggerSource === "bundler_sold_all"
        ? "bundler sold-all"
        : "valid wallet #4";

    this.followTokenLargeInsiderLog(
      "buy skipped — valid LI wallet already sold ≥25% before buy",
      {
        mint,
        triggerSource,
        wallets,
        signature: state?.exitTriggerSignature ?? null,
      },
    );

    void this.sendTelegramSafe(
      [
        `<b>⛔ ${this.label} Follow-Token Buy Skipped</b>`,
        `Token: <code>${mint}</code>`,
        `Trigger: ${triggerLabel}`,
        "",
        `≥1 valid Large Insider wallet already sold <b>≥25%</b> of holdings before buy — would buy then exit immediately.`,
        "",
        ...wallets.map(
          ({ index, wallet, soldPercent }) =>
            `${index}. <code>${wallet}</code> — sold <b>${soldPercent}%</b> tracked`,
        ),
        "",
        "No buy — token skipped and flow reset.",
      ]
        .filter(Boolean)
        .join("\n"),
      "follow-token li 25pct already sold before buy skip",
    );

    if (funderState && this.followTokenLargeInsiderState?.active) {
      await this.stopFollowTokenLargeInsiderFlow(
        "valid LI ≥25% already sold before buy",
      );
    }
    await this.resetForNewToken(false, { reason });
  }

  private async triggerFollowTokenLargeInsiderValidWalletTwentyFivePercentExitIfReady(
    triggerTx?: HeliusTransaction,
  ): Promise<boolean> {
    const li = this.followTokenLargeInsiderState;
    if (!li?.active) return false;

    for (const wallet of this.getFollowTokenLargeInsiderExitValidWallets()) {
      const watch = li.scrapeWatches.get(wallet);
      if (!watch) continue;
      if (
        !this.followTokenLargeInsiderWatchReachedExitSoldThreshold(watch, null)
      ) {
        continue;
      }
      const lastSell = [...watch.tokenActions]
        .reverse()
        .find((action) => action.kind === "sell");
      const tx =
        triggerTx ??
        (lastSell
          ? ({
              signature: lastSell.signature,
              timestamp: watch.firstBuyTimestamp ?? 0,
              type: "SWAP",
            } as HeliusTransaction)
          : null);
      await this.handleFollowTokenLargeInsiderValidWalletTwentyFivePercentSoldExit(
        wallet,
        tx,
        watch,
      );
      return (
        this.positionSellTriggered || !!li.exitTriggerSignature
      );
    }
    return false;
  }

  private anyFollowTokenEarlyBundlerExitWatchExceedsHighSellUsdMcTpDisable(): boolean {
    return (
      this.getFollowTokenEarlyBundlerExitMaxCumulativeSellUsd() >
      FOLLOW_TOKEN_EARLY_BUNDLER_EXIT_HIGH_SELL_USD_MC_TP_DISABLE
    );
  }

  private noFollowTokenEarlyBundlerExitWatchExceedsLowSellUsd(): boolean {
    return (
      this.getFollowTokenEarlyBundlerExitMaxCumulativeSellUsd() <=
      FOLLOW_TOKEN_EARLY_BUNDLER_EXIT_LOW_SELL_USD_THRESHOLD
    );
  }

  private getFollowTokenEarlyBundlerExitMaxCumulativeSellUsd(): number {
    const state = this.followTokenEarlyBundlerExitState;
    if (!state?.active || state.watches.size === 0) return 0;
    return Math.max(
      0,
      ...[...state.watches.values()].map((watch) => watch.cumulativeSellUsd),
    );
  }

  private getFollowTokenEarlyBundlerExitMaxSellTxCount(): number {
    const state = this.followTokenEarlyBundlerExitState;
    if (!state?.active || state.watches.size === 0) return 0;
    return Math.max(
      0,
      ...[...state.watches.values()].map((watch) => watch.sellTxCount),
    );
  }

  private bundlerExitMeetsMinSellTxCountForCumulativeUsdGate(): boolean {
    return (
      this.getFollowTokenEarlyBundlerExitMaxSellTxCount() >=
      FOLLOW_TOKEN_EARLY_BUNDLER_EXIT_MIN_SELL_TX_COUNT_FOR_USD_GATE
    );
  }

  private async triggerFollowTokenEarlyBundlerLowSellTxCountExit(
    signature: string,
    maxSellTxCount: number,
  ): Promise<void> {
    const state = this.followTokenEarlyBundlerExitState;
    const funderState = this.bundlerFunderWatch;
    if (!state?.active || !funderState || state.exitTriggerSignature) return;
    if (this.phase !== "holding" || this.positionSellTriggered) return;

    state.exitTriggerSignature = signature;

    this.log.warn(
      "Follow-token early bundler low sell-tx count exit — selling immediately",
      {
        mint: funderState.mint,
        maxSellTxCount,
        minSellTxCountForUsdGate:
          FOLLOW_TOKEN_EARLY_BUNDLER_EXIT_MIN_SELL_TX_COUNT_FOR_USD_GATE,
        signature,
      },
    );

    await this.triggerPositionSell(
      funderState.mint,
      "follow-token early bundler low sell-tx count exit",
      [
        `<b>🚨 ${this.label} Follow-Token Early Bundler Exit</b>`,
        `Token: <code>${funderState.mint}</code>`,
        `All bundlers sold all; max sell txs &lt; ${FOLLOW_TOKEN_EARLY_BUNDLER_EXIT_MIN_SELL_TX_COUNT_FOR_USD_GATE} (max <b>${maxSellTxCount}</b> txs).`,
        "",
        "No valid LI ≥25% — selling full position immediately (cumulative-USD rules skipped).",
      ],
      signature,
    );
  }

  private async triggerFollowTokenEarlyBundlerLowCumulativeSellUsdExit(
    signature: string,
  ): Promise<void> {
    const state = this.followTokenEarlyBundlerExitState;
    const funderState = this.bundlerFunderWatch;
    if (!state?.active || !funderState || state.exitTriggerSignature) return;
    if (this.phase !== "holding" || this.positionSellTriggered) return;

    state.exitTriggerSignature = signature;
    const maxCumulativeSellUsd =
      this.getFollowTokenEarlyBundlerExitMaxCumulativeSellUsd();

    this.log.warn(
      "Follow-token early bundler low cumulative sell-usd exit — selling immediately",
      {
        mint: funderState.mint,
        maxCumulativeSellUsd,
        signature,
      },
    );

    await this.triggerPositionSell(
      funderState.mint,
      "follow-token early bundler low cumulative sell-usd exit",
      [
        `<b>🚨 ${this.label} Follow-Token Early Bundler Exit</b>`,
        `Token: <code>${funderState.mint}</code>`,
        `All bundlers sold all; max cumulative sell ≤ $${FOLLOW_TOKEN_EARLY_BUNDLER_EXIT_LOW_SELL_USD_THRESHOLD.toLocaleString()} (max <b>$${maxCumulativeSellUsd.toLocaleString(undefined, { maximumFractionDigits: 0 })}</b>; max <b>${this.getFollowTokenEarlyBundlerExitMaxSellTxCount()}</b> sell txs ≥ ${FOLLOW_TOKEN_EARLY_BUNDLER_EXIT_MIN_SELL_TX_COUNT_FOR_USD_GATE}).`,
        "",
        "No valid LI ≥25% — selling full position immediately.",
      ],
      signature,
    );
  }

  private async estimateWalletSellUsd(
    tx: HeliusTransaction,
    wallet: string,
  ): Promise<number | null> {
    const solReceived = this.extractSolIncomingAmountToWallet(tx, wallet);
    if (solReceived <= 0) return null;
    const solPriceUsd = await this.getCachedSolPriceUsd();
    if (solPriceUsd === null) return null;
    return solReceived * solPriceUsd;
  }

  private async updateFollowTokenEarlyBundlerExitWatchCumulativeSellUsd(
    watch: FollowTokenEarlyBundlerExitWatch,
    tx: HeliusTransaction,
    wallet: string,
  ): Promise<void> {
    const sellUsd = await this.estimateWalletSellUsd(tx, wallet);
    if (sellUsd !== null && sellUsd > 0) {
      watch.cumulativeSellUsd += sellUsd;
    }
  }

  private hasFollowTokenLargeInsiderValidWalletDiscovered(): boolean {
    return (
      (this.followTokenLargeInsiderState?.validWallets.length ?? 0) >= 1
    );
  }

  private getFollowTokenEarlyBundlerExitLargestBagWatch(): FollowTokenEarlyBundlerExitWatch | null {
    const state = this.followTokenEarlyBundlerExitState;
    if (!state?.active || state.watches.size === 0) return null;

    let best: FollowTokenEarlyBundlerExitWatch | null = null;
    for (const watch of state.watches.values()) {
      if (!watch.monitoringActive) continue;
      if (!best || watch.boughtAmount > best.boughtAmount) {
        best = watch;
      }
    }
    return best;
  }

  private getFollowTokenEarlyBundlerExitHighestMaxSingleSellWatch(options?: {
    activeOnly?: boolean;
    source?: "early_bundler" | "transfer_recipient";
  }): FollowTokenEarlyBundlerExitWatch | null {
    const state = this.followTokenEarlyBundlerExitState;
    if (!state?.active || state.watches.size === 0) return null;

    const activeOnly = options?.activeOnly ?? true;
    const sourceFilter = options?.source;

    let best: FollowTokenEarlyBundlerExitWatch | null = null;
    for (const watch of state.watches.values()) {
      if (activeOnly && !watch.monitoringActive) continue;
      if (sourceFilter && watch.source !== sourceFilter) continue;
      if (
        !best ||
        watch.maxSingleSellTokenAmount > best.maxSingleSellTokenAmount
      ) {
        best = watch;
      }
    }
    return best;
  }

  private followTokenEarlyBundlerExitWatchMeetsSoldAllBalanceGate(
    watch: FollowTokenEarlyBundlerExitWatch,
  ): boolean {
    if (watch.boughtAmount <= 0 || watch.soldAmount <= 0) return false;
    if (
      watch.soldAmount / watch.boughtAmount >=
      FOLLOW_TOKEN_EARLY_BUNDLER_SOLD_ALL_MIN_SOLD_FRACTION
    ) {
      return true;
    }
    const liveRaw = this.getFollowTokenEarlyBundlerExitWatchLiveTokenBalanceRaw(
      watch.wallet,
    );
    if (liveRaw === null) return false;
    const remainingAmount = Number(liveRaw) / 10 ** PUMP_FUN_TOKEN_RAW_DECIMALS;
    return (
      remainingAmount <=
      watch.boughtAmount *
        (1 - FOLLOW_TOKEN_EARLY_BUNDLER_SOLD_ALL_MIN_SOLD_FRACTION)
    );
  }

  private formatFollowTokenEarlyBundlerExitWatchRoleLabel(
    source: FollowTokenEarlyBundlerExitWatch["source"],
  ): string {
    return source === "transfer_recipient" ? "transfer recipient" : "early bundler";
  }

  private getFollowTokenEarlyBundlerExitActiveRolesAtSoldAll(): Array<
    "early_bundler" | "transfer_recipient"
  > {
    const state = this.followTokenEarlyBundlerExitState;
    if (!state?.active) return [];
    const roles = new Set<"early_bundler" | "transfer_recipient">();
    for (const watch of state.watches.values()) {
      if (!watch.monitoringActive) continue;
      roles.add(watch.source);
    }
    return [...roles];
  }

  private getBundlerSoldAllMaxSingleSellGateSnapshot(): {
    tier: FollowTokenMaxSingleSellGateTier;
    candidateTier: FollowTokenMaxSingleSellGateTier;
    activeRolesAtSoldAll: Array<"early_bundler" | "transfer_recipient">;
    maxSingleSellTokenAmount: number;
    maxSingleSellWallet: string | null;
    maxSingleSellWatchSource: FollowTokenEarlyBundlerExitWatch["source"] | null;
    earlyBundlerMaxSingleSellTokenAmount: number;
    earlyBundlerMaxSingleSellWallet: string | null;
    transferRecipientMaxSingleSellTokenAmount: number;
    transferRecipientMaxSingleSellWallet: string | null;
    perSourceGateFailure: {
      source: "early_bundler" | "transfer_recipient";
      wallet: string;
      maxSingleSellTokenAmount: number;
      boughtAmount: number;
      soldAmount: number;
      soldFraction: number;
      limit: number;
      candidateTier: "standard_8m" | "fallback_16m";
      reason: "max_single_sell" | "not_sold_all";
    } | null;
    largestBagWallet: string | null;
    largestBagWatchSource: FollowTokenEarlyBundlerExitWatch["source"] | null;
    largestBagAmount: number;
    standardLimit: number;
    fallbackLimit: number;
  } {
    const largestBagWatch = this.getFollowTokenEarlyBundlerExitLargestBagWatch();
    const activeRolesAtSoldAll =
      this.getFollowTokenEarlyBundlerExitActiveRolesAtSoldAll();
    const maxSingleSellWatch =
      this.getFollowTokenEarlyBundlerExitHighestMaxSingleSellWatch({
        activeOnly: true,
      });
    const earlyBundlerMaxSingleSellWatch =
      activeRolesAtSoldAll.includes("early_bundler")
        ? this.getFollowTokenEarlyBundlerExitHighestMaxSingleSellWatch({
            activeOnly: true,
            source: "early_bundler",
          })
        : null;
    const transferRecipientMaxSingleSellWatch =
      activeRolesAtSoldAll.includes("transfer_recipient")
        ? this.getFollowTokenEarlyBundlerExitHighestMaxSingleSellWatch({
            activeOnly: true,
            source: "transfer_recipient",
          })
        : null;
    const standardLimit =
      FOLLOW_TOKEN_EARLY_BUNDLER_PRE_LI_MAX_SINGLE_SELL_TOKEN_AMOUNT;
    const fallbackLimit =
      FOLLOW_TOKEN_EARLY_BUNDLER_FALLBACK_MAX_SINGLE_SELL_TOKEN_AMOUNT;
    const maxSingleSellTokenAmount =
      maxSingleSellWatch?.maxSingleSellTokenAmount ?? 0;
    const earlyBundlerMaxSingleSellTokenAmount =
      earlyBundlerMaxSingleSellWatch?.maxSingleSellTokenAmount ?? 0;
    const transferRecipientMaxSingleSellTokenAmount =
      transferRecipientMaxSingleSellWatch?.maxSingleSellTokenAmount ?? 0;
    let candidateTier: FollowTokenMaxSingleSellGateTier;
    if (maxSingleSellTokenAmount <= standardLimit) {
      candidateTier = "standard_8m";
    } else if (maxSingleSellTokenAmount <= fallbackLimit) {
      candidateTier = "fallback_16m";
    } else {
      candidateTier = "fail";
    }

    let tier = candidateTier;
    let perSourceGateFailure: {
      source: "early_bundler" | "transfer_recipient";
      wallet: string;
      maxSingleSellTokenAmount: number;
      boughtAmount: number;
      soldAmount: number;
      soldFraction: number;
      limit: number;
      candidateTier: "standard_8m" | "fallback_16m";
      reason: "max_single_sell" | "not_sold_all";
    } | null = null;

    if (candidateTier === "standard_8m" || candidateTier === "fallback_16m") {
      const limit =
        candidateTier === "standard_8m" ? standardLimit : fallbackLimit;
      const perSourceChecks: Array<{
        source: "early_bundler" | "transfer_recipient";
        watch: FollowTokenEarlyBundlerExitWatch | null;
        amount: number;
      }> = [];
      if (activeRolesAtSoldAll.includes("early_bundler")) {
        perSourceChecks.push({
          source: "early_bundler",
          watch: earlyBundlerMaxSingleSellWatch,
          amount: earlyBundlerMaxSingleSellTokenAmount,
        });
      }
      if (activeRolesAtSoldAll.includes("transfer_recipient")) {
        perSourceChecks.push({
          source: "transfer_recipient",
          watch: transferRecipientMaxSingleSellWatch,
          amount: transferRecipientMaxSingleSellTokenAmount,
        });
      }
      for (const check of perSourceChecks) {
        const sourceWatches = [...(this.followTokenEarlyBundlerExitState?.watches.values() ?? [])]
          .filter(
            (watch) =>
              watch.monitoringActive && watch.source === check.source,
          );
        const failedWatch = sourceWatches.find(
          (watch) =>
            !this.followTokenEarlyBundlerExitWatchMeetsSoldAllBalanceGate(watch),
        );
        if (!failedWatch && check.amount <= limit) continue;
        const failureWatch = failedWatch ?? check.watch!;
        const soldFraction =
          failureWatch.boughtAmount > 0
            ? failureWatch.soldAmount / failureWatch.boughtAmount
            : 0;
        tier = "fail";
        perSourceGateFailure = {
          source: check.source,
          wallet: failureWatch.wallet,
          maxSingleSellTokenAmount: failureWatch.maxSingleSellTokenAmount,
          boughtAmount: failureWatch.boughtAmount,
          soldAmount: failureWatch.soldAmount,
          soldFraction,
          limit,
          candidateTier,
          reason: failedWatch ? "not_sold_all" : "max_single_sell",
        };
        break;
      }
    }

    return {
      tier,
      candidateTier,
      activeRolesAtSoldAll,
      maxSingleSellTokenAmount,
      maxSingleSellWallet: maxSingleSellWatch?.wallet ?? null,
      maxSingleSellWatchSource: maxSingleSellWatch?.source ?? null,
      earlyBundlerMaxSingleSellTokenAmount,
      earlyBundlerMaxSingleSellWallet:
        earlyBundlerMaxSingleSellWatch?.wallet ?? null,
      transferRecipientMaxSingleSellTokenAmount,
      transferRecipientMaxSingleSellWallet:
        transferRecipientMaxSingleSellWatch?.wallet ?? null,
      perSourceGateFailure,
      largestBagWallet: largestBagWatch?.wallet ?? null,
      largestBagWatchSource: largestBagWatch?.source ?? null,
      largestBagAmount: largestBagWatch?.boughtAmount ?? 0,
      standardLimit,
      fallbackLimit,
    };
  }

  private resolveFollowTokenBundlerSoldAllBuyParams(
    gateTier: "standard_8m" | "fallback_16m",
    preLiPhase: boolean,
    lowFundingMode: boolean,
  ): { profitExitPercent: number; buySol: number } {
    if (gateTier === "fallback_16m") {
      return {
        profitExitPercent: FOLLOW_TOKEN_EARLY_BUNDLER_FALLBACK_PROFIT_EXIT_PERCENT,
        buySol: this.followToken16mPostLiBuySol,
      };
    }
    return {
      profitExitPercent: FOLLOW_TOKEN_LARGE_INSIDER_PROFIT_EXIT_PERCENT,
      buySol: this.getBuySolForFundingMode(lowFundingMode),
    };
  }

  private formatFollowTokenActiveRoleMaxSingleSellGateLimitLine(
    gate: ReturnType<InsiderBot["getBundlerSoldAllMaxSingleSellGateSnapshot"]>,
    limitLabel: string,
  ): string {
    if (gate.activeRolesAtSoldAll.length === 0) return "";
    const roleLabels = gate.activeRolesAtSoldAll.map((role) =>
      this.formatFollowTokenEarlyBundlerExitWatchRoleLabel(role),
    );
    if (roleLabels.length === 1) {
      return `(active ${roleLabels[0]} path at sold-all · ≤ ${limitLabel}).`;
    }
    return `(active ${roleLabels.join(" + ")} paths at sold-all · each ≤ ${limitLabel}).`;
  }

  private formatFollowTokenPerSourceMaxSingleSellGateTelegramLines(
    gate: ReturnType<InsiderBot["getBundlerSoldAllMaxSingleSellGateSnapshot"]>,
  ): string[] {
    const lines: string[] = [];
    if (
      gate.activeRolesAtSoldAll.includes("early_bundler") &&
      gate.earlyBundlerMaxSingleSellWallet
    ) {
      lines.push(
        `Active early bundler max-single-sell: <b>${gate.earlyBundlerMaxSingleSellTokenAmount.toLocaleString()}</b> on <code>${gate.earlyBundlerMaxSingleSellWallet}</code>`,
      );
    }
    if (
      gate.activeRolesAtSoldAll.includes("transfer_recipient") &&
      gate.transferRecipientMaxSingleSellWallet
    ) {
      lines.push(
        `Active transfer recipient max-single-sell: <b>${gate.transferRecipientMaxSingleSellTokenAmount.toLocaleString()}</b> on <code>${gate.transferRecipientMaxSingleSellWallet}</code>`,
      );
    }
    return lines;
  }

  private formatFollowTokenMaxSingleSellGateTelegramLine(
    gate: ReturnType<InsiderBot["getBundlerSoldAllMaxSingleSellGateSnapshot"]>,
  ): string {
    if (!gate.maxSingleSellWallet) return "";
    const maxSellRole = this.formatFollowTokenEarlyBundlerExitWatchRoleLabel(
      gate.maxSingleSellWatchSource ?? "early_bundler",
    );
    const maxSellLine = `Highest max-single-sell across active watches: <b>${gate.maxSingleSellTokenAmount.toLocaleString()}</b> tokens on ${maxSellRole} <code>${gate.maxSingleSellWallet}</code>`;
    const perSourceLines =
      this.formatFollowTokenPerSourceMaxSingleSellGateTelegramLines(gate);
    const largestBagLine =
      gate.largestBagWallet &&
      gate.largestBagWallet !== gate.maxSingleSellWallet
        ? `Largest bag: ${this.formatFollowTokenEarlyBundlerExitWatchRoleLabel(gate.largestBagWatchSource ?? "early_bundler")} <code>${gate.largestBagWallet}</code> (<b>${gate.largestBagAmount.toLocaleString(undefined, { maximumFractionDigits: 3 })}</b> tokens).`
        : gate.largestBagWallet
          ? `Largest bag: same wallet (<b>${gate.largestBagAmount.toLocaleString(undefined, { maximumFractionDigits: 3 })}</b> tokens).`
          : "";
    const perSourceFailureLine = gate.perSourceGateFailure
      ? gate.perSourceGateFailure.reason === "not_sold_all"
        ? `Active ${this.formatFollowTokenEarlyBundlerExitWatchRoleLabel(gate.perSourceGateFailure.source)} <code>${gate.perSourceGateFailure.wallet}</code> is not sold-all: <b>${(gate.perSourceGateFailure.soldFraction * 100).toFixed(1)}%</b> sold from <b>${gate.perSourceGateFailure.boughtAmount.toLocaleString()}</b> bought (minimum <b>${(FOLLOW_TOKEN_EARLY_BUNDLER_SOLD_ALL_MIN_SOLD_FRACTION * 100).toFixed(0)}%</b> or near-zero balance required).`
        : `Active ${this.formatFollowTokenEarlyBundlerExitWatchRoleLabel(gate.perSourceGateFailure.source)} <code>${gate.perSourceGateFailure.wallet}</code> max-single-sell <b>${gate.perSourceGateFailure.maxSingleSellTokenAmount.toLocaleString()}</b> exceeds ${gate.perSourceGateFailure.candidateTier === "standard_8m" ? "8M" : "16M"} limit <b>${gate.perSourceGateFailure.limit.toLocaleString()}</b> (global active tier was <b>${gate.perSourceGateFailure.candidateTier === "standard_8m" ? "8M standard" : "16M fallback"}</b>).`
      : "";
    if (gate.tier === "fail") {
      return [
        maxSellLine,
        ...perSourceLines,
        largestBagLine,
        perSourceFailureLine,
        gate.perSourceGateFailure
          ? ""
          : `(8M limit <b>${gate.standardLimit.toLocaleString()}</b> · 16M limit <b>${gate.fallbackLimit.toLocaleString()}</b>).`,
      ]
        .filter(Boolean)
        .join("\n");
    }
    if (gate.tier === "fallback_16m") {
      return [
        maxSellLine,
        ...perSourceLines,
        largestBagLine,
        `→ <b>16M fallback buy after first LI</b> (+${FOLLOW_TOKEN_EARLY_BUNDLER_FALLBACK_PROFIT_EXIT_PERCENT}% MC TP; exceeds 8M limit <b>${gate.standardLimit.toLocaleString()}</b>${gate.activeRolesAtSoldAll.length > 0 ? `; ${this.formatFollowTokenActiveRoleMaxSingleSellGateLimitLine(gate, "16M")}` : ""}; waits for ≥1 valid LI within the 20m window).`,
      ]
        .filter(Boolean)
        .join("\n");
    }
    return [
      maxSellLine,
      ...perSourceLines,
      largestBagLine,
      `(≤ 8M limit <b>${gate.standardLimit.toLocaleString()}</b> · +${FOLLOW_TOKEN_LARGE_INSIDER_PROFIT_EXIT_PERCENT}% MC TP${gate.activeRolesAtSoldAll.length > 0 ? `; ${this.formatFollowTokenActiveRoleMaxSingleSellGateLimitLine(gate, "8M")}` : ""}).`,
    ]
      .filter(Boolean)
      .join("\n");
  }

  private async triggerFollowTokenBundlerSoldAllBuy(
    funderState: BundlerFunderWatchState,
    triggerWallet: string,
    signature: string,
    tx: HeliusTransaction,
    gate: ReturnType<InsiderBot["getBundlerSoldAllMaxSingleSellGateSnapshot"]>,
    options: {
      preLiPhase: boolean;
      bundlerExitBranch: "normal_mc_tp" | "high_usd_li_only";
      rawBranch?: string;
    },
  ): Promise<void> {
    const state = this.followTokenEarlyBundlerExitState;
    if (!state?.active) return;

    if (
      options.preLiPhase &&
      this.resolveFollowTokenEarlyBundlerPreBuyExitBranch() ===
        "high_usd_li_only"
    ) {
      await this.skipFollowTokenLargeInsiderFromBundlerHighUsdPreLi(tx, gate);
      return;
    }

    if (
      !options.preLiPhase &&
      (gate.tier === "standard_8m" || gate.tier === "fallback_16m") &&
      !this.passesFollowTokenPostLiBundlerQualifiedSolBuyGate()
    ) {
      await this.skipFollowTokenLargeInsiderFromPostLiBundlerQualifiedSolGate(
        tx,
        gate,
      );
      return;
    }

    if (gate.tier === "fail") {
      if (options.preLiPhase) {
        this.notifyBundlerSoldAllMaxSingleSellBuyBlocked("pre_li", gate);
        return;
      }
      if (
        options.rawBranch === "low_tx_immediate" ||
        options.rawBranch === "low_usd_immediate"
      ) {
        await this.skipFollowTokenLargeInsiderFromBundlerMaxSingleSellGate(
          options.rawBranch,
          tx,
          gate,
        );
        return;
      }
      this.notifyBundlerSoldAllMaxSingleSellBuyBlocked("post_li", gate);
      return;
    }

    const buyParams = this.resolveFollowTokenBundlerSoldAllBuyParams(
      gate.tier,
      options.preLiPhase,
      funderState.lowFundingMode,
    );

    state.preBuyBundlerPathTriggered = true;
    state.preLiBundlerSoldAllBuy = options.preLiPhase;

    if (
      options.bundlerExitBranch === "high_usd_li_only" &&
      gate.tier === "standard_8m"
    ) {
      state.highSellUsdMode = true;
    }

    const gateLabel =
      gate.tier === "fallback_16m" ? "16M fallback" : "8M standard";
    this.log.warn(
      `Follow-token bundler sold-all buy (${gateLabel}, +${buyParams.profitExitPercent}% MC TP)`,
      {
        mint: funderState.mint,
        triggerWallet,
        signature,
        gateTier: gate.tier,
        buySol: buyParams.buySol,
        profitExitPercent: buyParams.profitExitPercent,
        bundlerExitBranch: options.bundlerExitBranch,
        rawBranch: options.rawBranch ?? null,
        transferRecipientPath: state.earlyBundlerTransferOutObserved,
        ...gate,
      },
    );

    await this.emitFollowTokenLargeInsiderBuy(
      funderState,
      triggerWallet,
      signature,
      tx,
      {
        triggerSource: "bundler_sold_all",
        bundlerExitBranch: options.bundlerExitBranch,
        preLiPhase: options.preLiPhase,
        profitExitPercent: buyParams.profitExitPercent,
        buySolOverride: buyParams.buySol,
        maxSingleSellGateTier:
          gate.tier === "fallback_16m" ? "fallback_16m" : "standard_8m",
      },
    );
  }

  private notifyBundlerSoldAllMaxSingleSellBuyBlocked(
    phase: "pre_li" | "post_li",
    gate: ReturnType<InsiderBot["getBundlerSoldAllMaxSingleSellGateSnapshot"]>,
  ): void {
    const state = this.followTokenEarlyBundlerExitState;
    const funderState = this.bundlerFunderWatch;
    if (!state?.active || !funderState) return;

    const topWatchLine = this.formatFollowTokenMaxSingleSellGateTelegramLine(gate);

    if (phase === "pre_li") {
      if (state.preLiBundlerSoldAllBuyBlockedNotified) return;
      state.preLiBundlerSoldAllBuyBlockedNotified = true;
      this.log.warn(
        gate.perSourceGateFailure
          ? "Pre-LI bundler sold-all buy blocked — active-role max single sell exceeds gate limit"
          : "Pre-LI bundler sold-all buy blocked — global max single sell exceeds 16M",
        {
          mint: funderState.mint,
          gateTier: gate.tier,
          maxSingleSellWallet: gate.maxSingleSellWallet,
          maxSingleSellWatchSource: gate.maxSingleSellWatchSource,
          maxSingleSellTokenAmount: gate.maxSingleSellTokenAmount,
          candidateTier: gate.candidateTier,
          earlyBundlerMaxSingleSellWallet: gate.earlyBundlerMaxSingleSellWallet,
          earlyBundlerMaxSingleSellTokenAmount:
            gate.earlyBundlerMaxSingleSellTokenAmount,
          transferRecipientMaxSingleSellWallet:
            gate.transferRecipientMaxSingleSellWallet,
          transferRecipientMaxSingleSellTokenAmount:
            gate.transferRecipientMaxSingleSellTokenAmount,
          perSourceGateFailure: gate.perSourceGateFailure,
          largestBagWallet: gate.largestBagWallet,
          largestBagAmount: gate.largestBagAmount,
          standardLimit: gate.standardLimit,
          fallbackLimit: gate.fallbackLimit,
        },
      );
      void this.sendTelegramSafe(
        [
          `<b>⏸️ ${this.label} Pre-LI Bundler Sold-All Buy Blocked</b>`,
          `Token: <code>${funderState.mint}</code>`,
          topWatchLine,
          "",
          gate.perSourceGateFailure
            ? "Per-role max-single-sell check failed — waiting for 1st valid Large Insider wallet; post-LI bundler buy rules apply after that."
            : "Exceeds 16M max-single-sell limit — waiting for 1st valid Large Insider wallet; post-LI bundler buy rules apply after that.",
        ]
          .filter(Boolean)
          .join("\n"),
        "follow-token pre-li bundler sold-all buy blocked",
      );
      return;
    }

    if (state.postLiBundlerSoldAllBuyBlockedNotified) return;
    state.postLiBundlerSoldAllBuyBlockedNotified = true;
    this.log.warn(
      gate.perSourceGateFailure
        ? "Post-LI bundler sold-all buy blocked — active-role max single sell exceeds gate limit"
        : "Post-LI bundler sold-all buy blocked — global max single sell exceeds 16M",
      {
        mint: funderState.mint,
        maxSingleSellWallet: gate.maxSingleSellWallet,
        maxSingleSellWatchSource: gate.maxSingleSellWatchSource,
        maxSingleSellTokenAmount: gate.maxSingleSellTokenAmount,
        candidateTier: gate.candidateTier,
        earlyBundlerMaxSingleSellWallet: gate.earlyBundlerMaxSingleSellWallet,
        earlyBundlerMaxSingleSellTokenAmount:
          gate.earlyBundlerMaxSingleSellTokenAmount,
        transferRecipientMaxSingleSellWallet:
          gate.transferRecipientMaxSingleSellWallet,
        transferRecipientMaxSingleSellTokenAmount:
          gate.transferRecipientMaxSingleSellTokenAmount,
        perSourceGateFailure: gate.perSourceGateFailure,
        largestBagWallet: gate.largestBagWallet,
        largestBagAmount: gate.largestBagAmount,
        standardLimit: gate.standardLimit,
        fallbackLimit: gate.fallbackLimit,
      },
    );
    void this.sendTelegramSafe(
      [
        `<b>⏸️ ${this.label} Post-LI Bundler Sold-All Buy Blocked</b>`,
        `Token: <code>${funderState.mint}</code>`,
        topWatchLine,
        "",
        gate.perSourceGateFailure
          ? "Per-role max-single-sell check failed — waiting for valid wallet #4 (direct-sell) or a later sold-all eval that passes 8M/16M gates."
          : "Exceeds 16M max-single-sell limit — waiting for valid wallet #4 (direct-sell) or a later sold-all eval that passes 8M/16M gates.",
      ]
        .filter(Boolean)
        .join("\n"),
      "follow-token post-li bundler sold-all buy blocked",
    );
  }

  private resolveFollowTokenEarlyBundlerPreBuyExitBranch():
    | "low_tx_immediate"
    | "low_usd_immediate"
    | "normal_mc_tp"
    | "high_usd_li_only"
    | null {
    if (!this.allFollowTokenEarlyBundlerExitWatchesSoldAll()) return null;
    const meetsSellTxGate =
      this.bundlerExitMeetsMinSellTxCountForCumulativeUsdGate();
    if (!meetsSellTxGate) return "low_tx_immediate";
    if (this.anyFollowTokenEarlyBundlerExitWatchExceedsHighSellUsdMcTpDisable()) {
      return "high_usd_li_only";
    }
    if (this.noFollowTokenEarlyBundlerExitWatchExceedsLowSellUsd()) {
      return "low_usd_immediate";
    }
    return "normal_mc_tp";
  }

  private resolveFollowTokenEarlyBundlerPreBuyTriggerWallet(
    triggerTx?: HeliusTransaction,
  ): string | null {
    const state = this.followTokenEarlyBundlerExitState;
    if (triggerTx && state?.active) {
      for (const watch of state.watches.values()) {
        if (watch.observedTxSignatures.has(triggerTx.signature)) {
          return watch.wallet;
        }
      }
    }
    return this.followTokenEarlyInsiderBuys?.[0]?.wallet ?? null;
  }

  private async skipFollowTokenLargeInsiderFromBundlerLowExitBranch(
    branch: "low_tx_immediate" | "low_usd_immediate",
    triggerTx?: HeliusTransaction,
  ): Promise<void> {
    const state = this.followTokenEarlyBundlerExitState;
    const funderState = this.bundlerFunderWatch;
    const li = this.followTokenLargeInsiderState;
    if (!state?.active || !funderState || !li?.active) return;

    state.preBuyBundlerPathTriggered = true;
    state.exitTriggerSignature =
      triggerTx?.signature ?? `BUNDLER_SKIP_${branch.toUpperCase()}`;

    const maxSellTxCount = this.getFollowTokenEarlyBundlerExitMaxSellTxCount();
    const maxCumulativeSellUsd =
      this.getFollowTokenEarlyBundlerExitMaxCumulativeSellUsd();
    const reason =
      branch === "low_tx_immediate"
        ? "large_insider_bundler_low_sell_tx_skip"
        : "large_insider_bundler_low_cumulative_sell_usd_skip";
    const detailLine =
      branch === "low_tx_immediate"
        ? `All bundlers/recipients sold all; max sell txs &lt; ${FOLLOW_TOKEN_EARLY_BUNDLER_EXIT_MIN_SELL_TX_COUNT_FOR_USD_GATE} (max <b>${maxSellTxCount}</b>).`
        : `All bundlers/recipients sold all; max cumulative sell ≤ $${FOLLOW_TOKEN_EARLY_BUNDLER_EXIT_LOW_SELL_USD_THRESHOLD.toLocaleString()} (max <b>$${maxCumulativeSellUsd.toLocaleString(undefined, { maximumFractionDigits: 0 })}</b>; max <b>${maxSellTxCount}</b> sell txs ≥ ${FOLLOW_TOKEN_EARLY_BUNDLER_EXIT_MIN_SELL_TX_COUNT_FOR_USD_GATE}).`;

    this.followTokenLargeInsiderLog("bundler sold-all low exit — skipping token (no buy)", {
      mint: funderState.mint,
      branch,
      maxSellTxCount,
      maxCumulativeSellUsd,
      transferRecipientPath: state.earlyBundlerTransferOutObserved,
      signature: state.exitTriggerSignature,
    });

    void this.sendTelegramSafe(
      [
        `<b>⛔ ${this.label} Follow-Token Large Insider Skipped</b>`,
        `Token: <code>${funderState.mint}</code>`,
        detailLine,
        "",
        "No buy — token skipped and flow reset.",
      ].join("\n"),
      "follow-token large insider bundler low exit skip",
    );

    await this.stopFollowTokenLargeInsiderFlow(`bundler ${branch} skip`);
    await this.resetForNewToken(false, { reason });
  }

  private async skipFollowTokenLargeInsiderFromBundlerMaxSingleSellGate(
    rawBranch: "low_tx_immediate" | "low_usd_immediate",
    triggerTx: HeliusTransaction | undefined,
    gate: ReturnType<InsiderBot["getBundlerSoldAllMaxSingleSellGateSnapshot"]>,
  ): Promise<void> {
    const state = this.followTokenEarlyBundlerExitState;
    const funderState = this.bundlerFunderWatch;
    const li = this.followTokenLargeInsiderState;
    if (!state?.active || !funderState || !li?.active) return;

    state.preBuyBundlerPathTriggered = true;
    state.exitTriggerSignature =
      triggerTx?.signature ?? "BUNDLER_SKIP_MAX_SINGLE_SELL";

    const maxSellTxCount = this.getFollowTokenEarlyBundlerExitMaxSellTxCount();
    const maxCumulativeSellUsd =
      this.getFollowTokenEarlyBundlerExitMaxCumulativeSellUsd();
    const reason = "large_insider_bundler_max_single_sell_skip";
    const lowBranchLine =
      rawBranch === "low_tx_immediate"
        ? `Would qualify for +80% MC TP buy on low sell-tx branch (max <b>${maxSellTxCount}</b> txs &lt; ${FOLLOW_TOKEN_EARLY_BUNDLER_EXIT_MIN_SELL_TX_COUNT_FOR_USD_GATE}).`
        : `Would qualify for +80% MC TP buy on low-USD branch (max cumulative <b>$${maxCumulativeSellUsd.toLocaleString(undefined, { maximumFractionDigits: 0 })}</b> ≤ $${FOLLOW_TOKEN_EARLY_BUNDLER_EXIT_LOW_SELL_USD_THRESHOLD.toLocaleString()}).`;

    this.log.warn(
      gate.perSourceGateFailure
        ? "Post-LI bundler sold-all — active-role max-single-sell gate failed (low branch skip)"
        : "Post-LI bundler sold-all — max-single-sell gates failed (low branch skip)",
      {
        mint: funderState.mint,
        rawBranch,
        maxSellTxCount,
        maxCumulativeSellUsd,
        candidateTier: gate.candidateTier,
        maxSingleSellWallet: gate.maxSingleSellWallet,
        maxSingleSellWatchSource: gate.maxSingleSellWatchSource,
        maxSingleSellTokenAmount: gate.maxSingleSellTokenAmount,
        earlyBundlerMaxSingleSellWallet: gate.earlyBundlerMaxSingleSellWallet,
        earlyBundlerMaxSingleSellTokenAmount:
          gate.earlyBundlerMaxSingleSellTokenAmount,
        transferRecipientMaxSingleSellWallet:
          gate.transferRecipientMaxSingleSellWallet,
        transferRecipientMaxSingleSellTokenAmount:
          gate.transferRecipientMaxSingleSellTokenAmount,
        perSourceGateFailure: gate.perSourceGateFailure,
        largestBagWallet: gate.largestBagWallet,
        largestBagAmount: gate.largestBagAmount,
        standardLimit: gate.standardLimit,
        fallbackLimit: gate.fallbackLimit,
        signature: state.exitTriggerSignature,
      },
    );

    this.followTokenLargeInsiderLog(
      gate.perSourceGateFailure
        ? "bundler sold-all active-role max-single-sell gate failed — skipping token (no buy)"
        : "bundler sold-all max-single-sell gates failed — skipping token (no buy)",
      {
        mint: funderState.mint,
        rawBranch,
        maxSellTxCount,
        maxCumulativeSellUsd,
        candidateTier: gate.candidateTier,
        maxSingleSellWallet: gate.maxSingleSellWallet,
        maxSingleSellWatchSource: gate.maxSingleSellWatchSource,
        maxSingleSellTokenAmount: gate.maxSingleSellTokenAmount,
        earlyBundlerMaxSingleSellWallet: gate.earlyBundlerMaxSingleSellWallet,
        earlyBundlerMaxSingleSellTokenAmount:
          gate.earlyBundlerMaxSingleSellTokenAmount,
        transferRecipientMaxSingleSellWallet:
          gate.transferRecipientMaxSingleSellWallet,
        transferRecipientMaxSingleSellTokenAmount:
          gate.transferRecipientMaxSingleSellTokenAmount,
        perSourceGateFailure: gate.perSourceGateFailure,
        largestBagWallet: gate.largestBagWallet,
        largestBagAmount: gate.largestBagAmount,
        transferRecipientPath: state.earlyBundlerTransferOutObserved,
        signature: state.exitTriggerSignature,
      },
    );

    void this.sendTelegramSafe(
      [
        `<b>⛔ ${this.label} Follow-Token Large Insider Skipped</b>`,
        `Token: <code>${funderState.mint}</code>`,
        this.formatFollowTokenMaxSingleSellGateTelegramLine(gate),
        lowBranchLine,
        "",
        gate.perSourceGateFailure
          ? "No buy — active-role max-single-sell check failed; token skipped and flow reset."
          : "No buy — exceeds 16M max-single-sell limit; token skipped and flow reset.",
      ]
        .filter(Boolean)
        .join("\n"),
      "follow-token large insider bundler max single sell skip",
    );

    await this.stopFollowTokenLargeInsiderFlow("bundler max_single_sell gate skip");
    await this.resetForNewToken(false, { reason });
  }

  private async skipFollowTokenLargeInsiderFromBundlerHighUsdPreLi(
    triggerTx: HeliusTransaction | undefined,
    gate: ReturnType<InsiderBot["getBundlerSoldAllMaxSingleSellGateSnapshot"]>,
  ): Promise<void> {
    const state = this.followTokenEarlyBundlerExitState;
    const funderState = this.bundlerFunderWatch;
    if (!state?.active || !funderState) return;

    state.preBuyBundlerPathTriggered = true;
    state.exitTriggerSignature =
      triggerTx?.signature ?? "BUNDLER_SKIP_HIGH_USD_PRE_LI";
    const reason = "large_insider_bundler_high_usd_pre_li_skip";
    const maxSellTxCount = this.getFollowTokenEarlyBundlerExitMaxSellTxCount();
    const maxCumulativeSellUsd =
      this.getFollowTokenEarlyBundlerExitMaxCumulativeSellUsd();

    this.log.warn(
      "Pre-LI bundler sold-all — high cumulative sell; skipping token (no buy)",
      {
        mint: funderState.mint,
        signature: state.exitTriggerSignature,
        maxSellTxCount,
        maxCumulativeSellUsd,
        highSellUsdThreshold:
          FOLLOW_TOKEN_EARLY_BUNDLER_EXIT_HIGH_SELL_USD_MC_TP_DISABLE,
        transferRecipientPath: state.earlyBundlerTransferOutObserved,
        ...gate,
      },
    );

    void this.sendTelegramSafe(
      [
        `<b>⛔ ${this.label} Follow-Token Large Insider Skipped</b>`,
        `Token: <code>${funderState.mint}</code>`,
        this.formatFollowTokenMaxSingleSellGateTelegramLine(gate),
        "",
        `All bundlers/recipients sold all before 1st valid LI wallet.`,
        `Bundler cumulative sell &gt; $${FOLLOW_TOKEN_EARLY_BUNDLER_EXIT_HIGH_SELL_USD_MC_TP_DISABLE.toLocaleString()} (max <b>$${maxCumulativeSellUsd.toLocaleString(undefined, { maximumFractionDigits: 0 })}</b>; max <b>${maxSellTxCount}</b> sell txs ≥ ${FOLLOW_TOKEN_EARLY_BUNDLER_EXIT_MIN_SELL_TX_COUNT_FOR_USD_GATE}).`,
        "",
        "Pre-LI path has no valid LI wallets for ≥25% exit and +80% MC TP would be disabled — no viable post-buy exit.",
        "No buy — token skipped and flow reset.",
      ]
        .filter(Boolean)
        .join("\n"),
      "follow-token large insider high usd pre-li skip",
    );

    await this.stopFollowTokenLargeInsiderFlow("high USD pre-LI skip");
    await this.resetForNewToken(false, { reason });
  }

  private async skipFollowTokenLargeInsiderFromPostLiBundlerQualifiedSolGate(
    triggerTx: HeliusTransaction | undefined,
    gate: ReturnType<InsiderBot["getBundlerSoldAllMaxSingleSellGateSnapshot"]>,
  ): Promise<void> {
    const state = this.followTokenEarlyBundlerExitState;
    const funderState = this.bundlerFunderWatch;
    const li = this.followTokenLargeInsiderState;
    if (!state?.active || !funderState || !li?.active) return;

    state.preBuyBundlerPathTriggered = true;
    state.exitTriggerSignature =
      triggerTx?.signature ?? "BUNDLER_SKIP_POST_LI_QUALIFIED_SOL";
    const reason = "large_insider_post_li_bundler_qualified_sol_buy_gate_failed";
    const summary = this.summarizeFollowTokenPresentQualifiedSol();
    const gateLabel =
      gate.tier === "fallback_16m" ? "16M fallback" : "8M standard";

    this.log.warn(
      `Post-LI bundler sold-all buy gate failed (${gateLabel}) — all present valid LI wallets have Qualified SOL ≥25`,
      {
        mint: funderState.mint,
        gateTier: gate.tier,
        requiredOneBelowSol:
          FOLLOW_TOKEN_POST_LI_BUNDLER_BUY_REQUIRES_ONE_QUALIFIED_SOL_BELOW,
        wallets: summary,
        signature: state.exitTriggerSignature,
        ...gate,
      },
    );

    void this.sendTelegramSafe(
      [
        `<b>⛔ ${this.label} Follow-Token Post-LI Buy Skipped (${gateLabel})</b>`,
        `Token: <code>${funderState.mint}</code>`,
        this.formatFollowTokenMaxSingleSellGateTelegramLine(gate),
        "",
        `Need ≥1 present valid LI wallet with Qualified SOL <b>&lt;${FOLLOW_TOKEN_POST_LI_BUNDLER_BUY_REQUIRES_ONE_QUALIFIED_SOL_BELOW}</b> SOL.`,
        "",
        ...summary.map(
          ({ index, wallet, qualifiedSol }) =>
            `${index}. <code>${wallet}</code> — Qualified SOL: <b>${qualifiedSol !== null ? qualifiedSol.toFixed(4) : "?"}</b>`,
        ),
        "",
        "No buy — token skipped and flow reset.",
      ]
        .filter(Boolean)
        .join("\n"),
      "follow-token post-li bundler qualified sol buy gate failed",
    );

    await this.stopFollowTokenLargeInsiderFlow(
      "post-LI bundler qualified SOL gate skip",
    );
    await this.resetForNewToken(false, { reason });
  }

  private async fetchTokenAthMarketCapUsdFor16mFallbackGate(
    mint: string,
  ): Promise<number | null> {
    const maxAttempts = 1 + FOLLOW_TOKEN_16M_FALLBACK_ATH_MC_FETCH_RETRIES;
    for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
      const tokenAthMc = await this.gmgnClient.fetchTokenAthMarketCapUsd(mint);
      if (tokenAthMc !== null) {
        if (attempt > 1) {
          this.log.info("16M fallback ATH MC fetch succeeded after retry", {
            mint,
            attempt,
            tokenAthMc,
          });
        }
        return tokenAthMc;
      }
      if (attempt < maxAttempts) {
        await this.delay(FOLLOW_TOKEN_16M_FALLBACK_ATH_MC_FETCH_RETRY_DELAY_MS);
      }
    }
    return null;
  }

  private async skipFollowTokenLargeInsiderFrom16mFallbackAthMcGate(
    mint: string,
    entryMc: number,
    exitMc: number,
    profitExitPercent: number,
    tokenAthMc: number,
    triggerTx?: HeliusTransaction,
  ): Promise<void> {
    const state = this.followTokenEarlyBundlerExitState;
    const funderState = this.bundlerFunderWatch;
    if (state) {
      state.preBuyBundlerPathTriggered = true;
      state.exitTriggerSignature =
        triggerTx?.signature ?? "BUNDLER_SKIP_16M_ATH_MC";
    }
    const reason = "large_insider_16m_fallback_ath_mc_skip";
    const athSkipThreshold =
      exitMc * FOLLOW_TOKEN_16M_FALLBACK_BUY_ATH_EXIT_MC_MULTIPLIER;

    this.log.warn(
      "16M fallback buy skipped — token ATH MC already ≥2× calculated exit MC",
      {
        mint,
        entryMc,
        exitMc,
        profitExitPercent,
        tokenAthMc,
        athSkipThreshold,
        athExitMcMultiplier: FOLLOW_TOKEN_16M_FALLBACK_BUY_ATH_EXIT_MC_MULTIPLIER,
        signature: state?.exitTriggerSignature ?? null,
      },
    );

    void this.sendTelegramSafe(
      [
        `<b>⛔ ${this.label} Follow-Token 16M Fallback Buy Skipped</b>`,
        `Token: <code>${mint}</code>`,
        `Entry MC would be: <b>$${entryMc.toLocaleString(undefined, { maximumFractionDigits: 3 })}</b>`,
        `Exit MC (+${profitExitPercent}%): <b>$${exitMc.toLocaleString(undefined, { maximumFractionDigits: 3 })}</b>`,
        `Token ATH MC: <b>$${tokenAthMc.toLocaleString(undefined, { maximumFractionDigits: 3 })}</b>`,
        `Skip when token ATH ≥ <b>${FOLLOW_TOKEN_16M_FALLBACK_BUY_ATH_EXIT_MC_MULTIPLIER}×</b> exit MC (<b>$${athSkipThreshold.toLocaleString(undefined, { maximumFractionDigits: 3 })}</b>).`,
        "",
        "No buy — token skipped and flow reset.",
      ].join("\n"),
      "follow-token 16m fallback ath mc skip",
    );

    if (funderState && this.followTokenLargeInsiderState?.active) {
      await this.stopFollowTokenLargeInsiderFlow("16M fallback ATH MC gate skip");
    }
    await this.resetForNewToken(false, { reason });
  }

  private async maybeTriggerFollowTokenLargeInsiderPreBuyFromBundlerPath(
    triggerTx?: HeliusTransaction,
  ): Promise<void> {
    const state = this.followTokenEarlyBundlerExitState;
    const funderState = this.bundlerFunderWatch;
    const li = this.followTokenLargeInsiderState;
    if (!state?.active || !funderState || state.mint !== funderState.mint) {
      return;
    }
    if (
      this.buySubmitted ||
      this.buyDisabled ||
      this.isBuyExecuting ||
      this.isBuyGateEvaluating ||
      state.preBuyBundlerPathTriggered
    ) {
      return;
    }

    const triggerWallet = this.resolveFollowTokenEarlyBundlerPreBuyTriggerWallet(
      triggerTx,
    );
    if (!triggerWallet) return;

    const earlyBuy = this.followTokenEarlyInsiderBuys?.[0];
    const signature =
      triggerTx?.signature ?? earlyBuy?.signature ?? "BUNDLER_SOLD_ALL";
    const tx =
      triggerTx ??
      ({
        signature,
        timestamp: earlyBuy?.timestamp ?? 0,
        type: "SWAP",
      } as HeliusTransaction);

    const waitingGate = this.getBundlerSoldAllMaxSingleSellGateSnapshot();
    if (waitingGate.tier === "fail") {
      await this.skipFollowTokenLargeInsiderFromPreLiMaxSingleSellGate(
        tx,
        waitingGate,
      );
      return;
    }

    if (!this.hasFollowTokenLargeInsiderValidWalletDiscovered()) {
      if (
        !state.preLiWaitingForValidLiNotified &&
        (waitingGate.tier === "standard_8m" ||
          waitingGate.tier === "fallback_16m")
      ) {
        state.preLiWaitingForValidLiNotified = true;
        const gateLabel =
          waitingGate.tier === "fallback_16m" ? "16M fallback" : "8M standard";
        const profitExitPercent =
          waitingGate.tier === "fallback_16m"
            ? FOLLOW_TOKEN_EARLY_BUNDLER_FALLBACK_PROFIT_EXIT_PERCENT
            : FOLLOW_TOKEN_LARGE_INSIDER_PROFIT_EXIT_PERCENT;
        const windowRemainingSec = Math.max(
          0,
          (li?.feePayerWindowEndsAt ?? 0) - Math.floor(Date.now() / 1000),
        );
        void this.sendTelegramSafe(
          [
            `<b>⏳ ${this.label} Pre-LI Bundler Sold-All — Waiting for LI</b>`,
            `Token: <code>${funderState.mint}</code>`,
            `Path: <b>${gateLabel}</b> (+${profitExitPercent}% MC TP)`,
            "All early bundlers/transfer recipients sold all before the first valid LI wallet.",
            "No buy yet — waiting for at least 1 valid LI wallet.",
            `Remaining LI window: <b>${Math.ceil(windowRemainingSec / 60)}m</b>`,
            "Rug detection remains active; window close with no valid LI resets the flow.",
          ].join("\n"),
          "follow-token pre-li sold-all waiting for valid LI",
        );
      }
      this.log.info("Pre-LI bundler sold-all — waiting for first valid LI wallet", {
        mint: funderState.mint,
        signature,
      });
      return;
    }

    // Bundler/recipient sold-all is only an armed pre-LI +80% path. Do not
    // buy on the early exit before LI discovery; the first valid LI wallet
    // within the feePayer window releases this path.
    if (!li?.active || li.mint !== funderState.mint) return;

    const firstLiGate = this.getBundlerSoldAllMaxSingleSellGateSnapshot();
    if (firstLiGate.tier === "fail") {
      this.notifyBundlerSoldAllMaxSingleSellBuyBlocked("pre_li", firstLiGate);
      return;
    }

    const gate = this.getBundlerSoldAllMaxSingleSellGateSnapshot();
    const maxSellTxCount = this.getFollowTokenEarlyBundlerExitMaxSellTxCount();
    const maxCumulativeSellUsd =
      this.getFollowTokenEarlyBundlerExitMaxCumulativeSellUsd();

    this.log.warn("Pre-LI bundler sold-all — max-single-sell gate eval after first LI", {
      mint: funderState.mint,
      maxSellTxCount,
      maxCumulativeSellUsd,
      ...gate,
      signature,
    });

    await this.triggerFollowTokenBundlerSoldAllBuy(
      funderState,
      triggerWallet,
      signature,
      tx,
      gate,
      {
        preLiPhase: true,
        bundlerExitBranch: "normal_mc_tp",
      },
    );
  }

  private async skipFollowTokenLargeInsiderFromPreLiMaxSingleSellGate(
    triggerTx: HeliusTransaction,
    gate: ReturnType<InsiderBot["getBundlerSoldAllMaxSingleSellGateSnapshot"]>,
  ): Promise<void> {
    const state = this.followTokenEarlyBundlerExitState;
    const funderState = this.bundlerFunderWatch;
    if (!state?.active || !funderState) return;

    state.preBuyBundlerPathTriggered = true;
    state.exitTriggerSignature = triggerTx.signature;
    const reason = "large_insider_bundler_pre_li_max_single_sell_skip";

    this.log.warn(
      "Pre-LI bundler sold-all — max-single-sell exceeds 16M; skipping token",
      {
        mint: funderState.mint,
        signature: triggerTx.signature,
        transferRecipientPath: state.earlyBundlerTransferOutObserved,
        ...gate,
      },
    );

    void this.sendTelegramSafe(
      [
        `<b>⛔ ${this.label} Follow-Token Pre-LI Bundler Sold-All Skipped</b>`,
        `Token: <code>${funderState.mint}</code>`,
        this.formatFollowTokenMaxSingleSellGateTelegramLine(gate),
        "",
        "All early bundlers/transfer recipients sold all before the first valid LI wallet.",
        "No buy — maximum single sell exceeds the 16M fallback limit; token skipped and flow reset.",
      ]
        .filter(Boolean)
        .join("\n"),
      "follow-token pre-li max single sell skip",
    );

    await this.stopFollowTokenLargeInsiderFlow("pre-LI max single sell gate skip");
    await this.resetForNewToken(false, { reason });
  }

  private async maybeEvaluateFollowTokenEarlyBundlerExit(
    triggerTx?: HeliusTransaction,
  ): Promise<void> {
    const state = this.followTokenEarlyBundlerExitState;
    const funderState = this.bundlerFunderWatch;
    const li = this.followTokenLargeInsiderState;
    if (!state?.active || !funderState || state.exitTriggerSignature) return;
    const soldAllBlockReason = this.followTokenEarlyBundlerExitSoldAllBlockReason();
    if (soldAllBlockReason) {
      const largestBag = this.getFollowTokenEarlyBundlerExitLargestBagWatch();
      const neverSoldWallets = [...state.watches.values()]
        .filter(
          (watch) =>
            watch.monitoringActive &&
            this.followTokenEarlyBundlerExitWatchHasNeverSold(watch),
        )
        .map((watch) => ({
          wallet: watch.wallet,
          source: watch.source,
          boughtAmount: watch.boughtAmount,
          soldAll: watch.soldAll,
          syncComplete: watch.syncComplete,
        }));
      this.log.info("Follow-token early bundler sold-all eval blocked", {
        mint: state.mint,
        reason: soldAllBlockReason,
        triggerSignature: triggerTx?.signature ?? null,
        largestBagWallet: largestBag?.wallet ?? null,
        largestBagAmount: largestBag?.boughtAmount ?? 0,
        largestBagSoldAll: largestBag?.soldAll ?? null,
        neverSoldWallets,
      });
      return;
    }

    state.allSoldAllComplete = true;

    if (!this.buySubmitted) {
      await this.maybeTriggerFollowTokenLargeInsiderPreBuyFromBundlerPath(
        triggerTx,
      );
      return;
    }

    if (this.phase !== "holding" || this.positionSellTriggered) return;

    if (state.preLiBundlerSoldAllBuy) {
      const { mode, maxCumulativeSellUsd, maxSellTxCount } =
        this.applyPreLiBundlerBuyExitModeFromBundlerStats();
      const activeMcTpPercent = this.getFollowTokenActiveProfitExitPercent();
      const fallbackBuyNote =
        state.maxSingleSellGateTierAtBuy === "fallback_16m"
          ? " · 16M fallback buy"
          : "";
      if (
        await this.triggerFollowTokenLargeInsiderValidWalletTwentyFivePercentExitIfReady(
          triggerTx,
        )
      ) {
        return;
      }
      if (!state.preLiExitArmedNotified) {
        state.preLiExitArmedNotified = true;
        if (mode === "li_only") {
          void this.sendTelegramSafe(
            [
              `<b>⏳ ${this.label} Pre-LI Bundler Buy — Valid LI ≥25% Only</b>`,
              `Token: <code>${funderState.mint}</code>`,
              "All bundlers/recipients sold all before 1st valid LI wallet.",
              `Bundler cumulative > $${FOLLOW_TOKEN_EARLY_BUNDLER_EXIT_HIGH_SELL_USD_MC_TP_DISABLE.toLocaleString()} (max <b>$${maxCumulativeSellUsd.toLocaleString(undefined, { maximumFractionDigits: 0 })}</b>; max <b>${maxSellTxCount}</b> sell txs).`,
              `+${activeMcTpPercent.toFixed(0)}% MC TP disabled — waiting for valid LI ≥25%.`,
            ].join("\n"),
            "follow-token pre-li bundler li-only",
          );
        } else {
          void this.sendTelegramSafe(
            [
              `<b>✅ ${this.label} Pre-LI Bundler Buy — +${activeMcTpPercent.toFixed(0)}% MC TP Active${fallbackBuyNote}</b>`,
              `Token: <code>${funderState.mint}</code>`,
              "All bundlers/recipients sold all before 1st valid LI wallet.",
              mode === "mc_tp_and_li"
                ? `Exit: +${activeMcTpPercent.toFixed(0)}% MC TP or valid LI ≥25% (bundler cumulative $${(FOLLOW_TOKEN_EARLY_BUNDLER_EXIT_LOW_SELL_USD_THRESHOLD / 1000).toFixed(1)}k–$${(FOLLOW_TOKEN_EARLY_BUNDLER_EXIT_HIGH_SELL_USD_MC_TP_DISABLE / 1000).toFixed(0)}k zone).`
                : `Exit: +${activeMcTpPercent.toFixed(0)}% MC TP; valid LI ≥25% once discovered.`,
            ].join("\n"),
            "follow-token pre-li bundler mc tp armed",
          );
        }
      }
      return;
    }

    const watches = [...state.watches.values()];
    const maxCumulativeSellUsd =
      this.getFollowTokenEarlyBundlerExitMaxCumulativeSellUsd();
    const maxSellTxCount = this.getFollowTokenEarlyBundlerExitMaxSellTxCount();
    const meetsSellTxGate =
      this.bundlerExitMeetsMinSellTxCountForCumulativeUsdGate();
    const highSellUsd =
      meetsSellTxGate &&
      this.anyFollowTokenEarlyBundlerExitWatchExceedsHighSellUsdMcTpDisable();
    if (highSellUsd) {
      state.highSellUsdMode = true;
      this.profitExitDisabled = true;
    }

    const walletLines = watches.map((watch) => {
      const soldPercent =
        watch.boughtAmount > 0
          ? ((watch.soldAmount / watch.boughtAmount) * 100).toFixed(1)
          : "?";
      return `• <code>${watch.wallet}</code> (${watch.source}): cumulative sell <b>$${watch.cumulativeSellUsd.toLocaleString(undefined, { maximumFractionDigits: 0 })}</b> (${watch.sellTxCount} txs), ${soldPercent}% sold`;
    });

    this.log.warn("Follow-token early bundler all sold all — evaluating LI exit", {
      mint: funderState.mint,
      highSellUsd,
      maxCumulativeSellUsd,
      maxSellTxCount,
      meetsSellTxGate,
      minSellTxCountForUsdGate:
        FOLLOW_TOKEN_EARLY_BUNDLER_EXIT_MIN_SELL_TX_COUNT_FOR_USD_GATE,
      validLiTwentyFive:
        this.anyFollowTokenLargeInsiderValidWalletReachedTwentyFivePercentSold(),
      validWalletTwentyFivePercentDeferred:
        state.validWalletTwentyFivePercentDeferred,
      mcTpReachedPending: state.mcTpReachedPending,
    });

    const postLiBundlerMcTpBuy =
      state.preBuyBundlerPathTriggered &&
      !state.highSellUsdMode &&
      !state.preLiBundlerSoldAllBuy;

    if (
      await this.triggerFollowTokenLargeInsiderValidWalletTwentyFivePercentExitIfReady(
        triggerTx,
      )
    ) {
      return;
    }

    if (!meetsSellTxGate) {
      if (!postLiBundlerMcTpBuy) {
        await this.triggerFollowTokenEarlyBundlerLowSellTxCountExit(
          triggerTx?.signature ?? "EARLY_BUNDLER_LOW_SELL_TX_COUNT",
          maxSellTxCount,
        );
        return;
      }
    }

    if (highSellUsd) {
      void this.sendTelegramSafe(
        [
          `<b>⏳ ${this.label} Early Bundler All Sold All — Valid LI ≥25% Only</b>`,
          `Token: <code>${funderState.mint}</code>`,
          ...walletLines,
          "",
          `A bundler/recipient cumulative sell exceeded $${FOLLOW_TOKEN_EARLY_BUNDLER_EXIT_HIGH_SELL_USD_MC_TP_DISABLE.toLocaleString()} (max <b>$${maxCumulativeSellUsd.toLocaleString(undefined, { maximumFractionDigits: 0 })}</b>; max <b>${maxSellTxCount}</b> sell txs) — +${FOLLOW_TOKEN_LARGE_INSIDER_PROFIT_EXIT_PERCENT}% MC TP disabled.`,
          "Waiting for ≥25% sold on any valid Large Insider wallet.",
          state.validWalletTwentyFivePercentDeferred
            ? "Valid wallet ≥25% was seen during wait — will sell on next qualifying sell."
            : "",
        ]
          .filter(Boolean)
          .join("\n"),
        "follow-token early bundler high sell-usd wait valid li",
      );
      return;
    }

    if (this.noFollowTokenEarlyBundlerExitWatchExceedsLowSellUsd()) {
      if (!postLiBundlerMcTpBuy) {
        await this.triggerFollowTokenEarlyBundlerLowCumulativeSellUsdExit(
          triggerTx?.signature ?? "EARLY_BUNDLER_LOW_CUMULATIVE_SELL_USD",
        );
        return;
      }
    }

    void this.sendTelegramSafe(
      [
        `<b>✅ ${this.label} Early Bundler All Sold All — Normal Exit Active</b>`,
        `Token: <code>${funderState.mint}</code>`,
        ...walletLines,
        "",
        `Max cumulative bundler sell ≤ $${FOLLOW_TOKEN_EARLY_BUNDLER_EXIT_HIGH_SELL_USD_MC_TP_DISABLE.toLocaleString()} (max <b>$${maxCumulativeSellUsd.toLocaleString(undefined, { maximumFractionDigits: 0 })}</b>; max <b>${maxSellTxCount}</b> sell txs).`,
        `Exit: +${FOLLOW_TOKEN_LARGE_INSIDER_PROFIT_EXIT_PERCENT}% MC TP or ≥25% sold on any valid LI wallet.`,
        state.mcTpReachedPending
          ? `MC TP (+${FOLLOW_TOKEN_LARGE_INSIDER_PROFIT_EXIT_PERCENT}%) was deferred — re-armed.`
          : "",
        state.validWalletTwentyFivePercentDeferred
          ? "Valid wallet ≥25% was seen during wait — will sell on next qualifying sell."
          : "Waiting for ≥25% sold on any valid Large Insider wallet.",
      ]
        .filter(Boolean)
        .join("\n"),
      "follow-token early bundler normal exit armed",
    );
  }

  private async applyFollowTokenEarlyBundlerExitTx(
    tx: HeliusTransaction,
    mint: string,
    wallet: string,
  ): Promise<void> {
    const state = this.followTokenEarlyBundlerExitState;
    if (!state?.active || state.mint !== mint || state.exitTriggerSignature) {
      return;
    }
    const preBuyMonitoring =
      this.phase === "pre_buy" &&
      !this.buySubmitted &&
      state.active &&
      state.mint === mint;
    if (this.phase !== "holding" && !preBuyMonitoring) {
      return;
    }

    const watch = state.watches.get(wallet);
    if (!watch || !watch.monitoringActive) return;
    if (watch.observedTxSignatures.has(tx.signature)) return;
    if (!this.isRelevantMintTx(tx, mint)) return;

    watch.observedTxSignatures.add(tx.signature);
    const kind = this.classifyTx(tx, wallet, mint);
    if (!kind) return;

    if (kind === "buy" || kind === "sell" || kind === "transfer_in") {
      this.logTokenTx(
        mint,
        kind === "transfer_in" ? "buy" : kind,
        "early_bundler_exit",
        tx.signature,
        wallet,
      );
    }

    if (kind === "transfer_out") {
      await this.handleFollowTokenEarlyBundlerTransferOut(watch, tx, mint);
      return;
    }

    if (kind === "buy" || kind === "transfer_in") {
      const amount = this.extractTokenAmountForWallet(tx, wallet, mint, "buy");
      this.applyFollowTokenEarlyBundlerExitWatchInboundAmount(
        watch,
        amount,
        tx,
        mint,
        kind,
      );
      return;
    }

    if (kind === "sell") {
      const amount = this.extractTokenAmountForWallet(tx, wallet, mint, "sell");
      if (amount <= 0) return;

      watch.sellTxCount += 1;
      watch.soldAmount += amount;
      if (amount > watch.maxSingleSellTokenAmount) {
        watch.maxSingleSellTokenAmount = amount;
      }

      const liveTokenBalanceRaw =
        this.getFollowTokenEarlyBundlerExitWatchLiveTokenBalanceRaw(wallet);
      this.log.info("Follow-token early bundler sell observed", {
        mint,
        wallet: watch.wallet,
        source: watch.source,
        signature: tx.signature,
        sellAmount: amount,
        soldAmount: watch.soldAmount,
        boughtAmount: watch.boughtAmount,
        expectedRemaining: Math.max(0, watch.boughtAmount - watch.soldAmount),
        liveTokenBalanceRaw: liveTokenBalanceRaw?.toString() ?? null,
      });

      await this.updateFollowTokenEarlyBundlerExitWatchCumulativeSellUsd(
        watch,
        tx,
        wallet,
      );
      this.updateFollowTokenEarlyBundlerExitWatchSoldAll(watch, tx.signature);
      this.clearFollowTokenEarlyBundlerExitDeferredSoldAllEvalTimer();
      await this.maybeEvaluateFollowTokenEarlyBundlerExit(tx);
    }
  }

  private applyFollowTokenEarlyBundlerExitWatchInboundAmount(
    watch: FollowTokenEarlyBundlerExitWatch,
    amount: number,
    tx: HeliusTransaction,
    mint: string,
    kind: "buy" | "transfer_in",
  ): void {
    if (amount <= 0) return;

    watch.boughtAmount += amount;
    if (watch.soldAll && watch.soldAmount < watch.boughtAmount) {
      watch.soldAll = false;
      watch.soldAllSignature = null;
      if (!watch.monitoringActive) {
        watch.monitoringActive = true;
        this.subscribeFollowTokenEarlyBundlerExitWallet(watch.wallet);
      }
    }

    this.log.info(
      kind === "transfer_in"
        ? "Follow-token early bundler watch — transfer-in increased bought amount"
        : "Follow-token early bundler watch — buy increased bought amount",
      {
        mint,
        wallet: watch.wallet,
        source: watch.source,
        amount,
        totalBoughtAmount: watch.boughtAmount,
        signature: tx.signature,
      },
    );
  }

  private isValidFollowTokenEarlyBundlerWatchWallet(address: string): boolean {
    if (!address || address === UNKNOWN_COUNTERPARTY) return false;
    try {
      new PublicKey(address);
      return true;
    } catch {
      return false;
    }
  }

  private getFollowTokenEarlyBundlerTransferRecipientsFromTx(
    tx: HeliusTransaction,
    wallet: string,
    mint: string,
  ): Map<string, number> {
    const recipients = new Map<string, number>();
    const addRecipient = (recipient: string, amount: number) => {
      if (amount <= 0) return;
      if (!this.isValidFollowTokenEarlyBundlerWatchWallet(recipient)) return;
      recipients.set(recipient, (recipients.get(recipient) ?? 0) + amount);
    };

    for (const transfer of tx.tokenTransfers ?? []) {
      if (transfer.mint !== mint || transfer.fromUserAccount !== wallet) continue;
      const recipient = transfer.toUserAccount;
      if (!recipient || recipient === wallet || recipient === UNKNOWN_COUNTERPARTY) {
        continue;
      }
      addRecipient(recipient, transfer.tokenAmount ?? 0);
    }

    // Enhanced WSS delta reconstruction emits wallet→__pool__ outbound legs; pair with
    // __pool__→recipient inbound legs in the same tx (same pattern as SOL transfers).
    const hasPoolOutbound = (tx.tokenTransfers ?? []).some(
      (transfer) =>
        transfer.mint === mint &&
        transfer.fromUserAccount === wallet &&
        transfer.toUserAccount === UNKNOWN_COUNTERPARTY,
    );
    if (hasPoolOutbound) {
      for (const transfer of tx.tokenTransfers ?? []) {
        if (transfer.mint !== mint || transfer.fromUserAccount !== UNKNOWN_COUNTERPARTY) {
          continue;
        }
        addRecipient(transfer.toUserAccount ?? "", transfer.tokenAmount ?? 0);
      }
    }

    return recipients;
  }

  private async ensureFollowTokenEarlyBundlerTransferRecipientWatch(
    parentWatch: FollowTokenEarlyBundlerExitWatch,
    recipient: string,
    transferAmount: number,
    tx: HeliusTransaction,
    mint: string,
  ): Promise<void> {
    const state = this.followTokenEarlyBundlerExitState;
    if (!state?.active || transferAmount <= 0) return;
    if (!this.isValidFollowTokenEarlyBundlerWatchWallet(recipient)) return;

    const transferReceiveSignature = tx.signature;
    const existing = state.watches.get(recipient);
    if (existing) {
      existing.boughtAmount += transferAmount;
      existing.observedTxSignatures.add(transferReceiveSignature);
      if (existing.soldAll) {
        existing.soldAll = false;
        existing.soldAllSignature = null;
        existing.reachedTwentyFivePercentSold = false;
        if (!existing.monitoringActive) {
          existing.monitoringActive = true;
          this.subscribeFollowTokenEarlyBundlerExitWallet(recipient);
        }
      }
      existing.syncComplete = false;
      this.log.info(
        "Follow-token early bundler transfer — added inbound amount to existing recipient watch",
        {
          mint,
          parentWallet: parentWatch.wallet,
          recipient,
          transferAmount,
          totalBoughtAmount: existing.boughtAmount,
          transferReceiveSignature,
        },
      );
      await this.syncFollowTokenEarlyBundlerExitWallet(recipient, mint);
      return;
    }

    const childWatch: FollowTokenEarlyBundlerExitWatch = {
      wallet: recipient,
      source: "transfer_recipient",
      parentWallet: parentWatch.wallet,
      syncAfterSignature: transferReceiveSignature,
      boughtAmount: transferAmount,
      soldAmount: 0,
      sellTxCount: 0,
      cumulativeSellUsd: 0,
      maxSingleSellTokenAmount: 0,
      soldAll: false,
      soldAllSignature: null,
      reachedTwentyFivePercentSold: false,
      observedTxSignatures: new Set([transferReceiveSignature]),
      syncComplete: false,
      monitoringActive: true,
      observedNonZeroTokenBalance: false,
    };
    state.watches.set(recipient, childWatch);

    this.log.info(
      "Follow-token early bundler token transfer — chaining recipient watch",
      {
        mint,
        parentWallet: parentWatch.wallet,
        recipient,
        transferAmount,
        transferReceiveSignature,
      },
    );

    this.subscribeFollowTokenEarlyBundlerExitWallet(recipient);
    await this.syncFollowTokenEarlyBundlerExitWallet(recipient, mint);
  }

  private async handleFollowTokenEarlyBundlerTransferOut(
    parentWatch: FollowTokenEarlyBundlerExitWatch,
    tx: HeliusTransaction,
    mint: string,
  ): Promise<void> {
    const state = this.followTokenEarlyBundlerExitState;
    if (!state?.active) return;

    const recipients = this.getFollowTokenEarlyBundlerTransferRecipientsFromTx(
      tx,
      parentWatch.wallet,
      mint,
    );
    const hasUnresolvedPoolOutbound =
      recipients.size === 0 &&
      (tx.tokenTransfers ?? []).some(
        (transfer) =>
          transfer.mint === mint &&
          transfer.fromUserAccount === parentWatch.wallet &&
          transfer.toUserAccount === UNKNOWN_COUNTERPARTY,
      );
    if (recipients.size === 0 && !hasUnresolvedPoolOutbound) return;

    if (hasUnresolvedPoolOutbound) {
      this.log.info(
        "Follow-token early bundler transfer-out pool leg — no token recipient resolved (not counted as sell on sender)",
        {
          mint,
          wallet: parentWatch.wallet,
          signature: tx.signature,
        },
      );
    }

    if (recipients.size > 0 && parentWatch.source === "early_bundler") {
      state.earlyBundlerTransferOutObserved = true;
    }

    for (const [recipient, transferAmount] of recipients) {
      await this.ensureFollowTokenEarlyBundlerTransferRecipientWatch(
        parentWatch,
        recipient,
        transferAmount,
        tx,
        mint,
      );
    }

    const dropParent = recipients.size > 0;
    if (dropParent) {
      this.dropFollowTokenEarlyBundlerExitWatchAfterTransfer(parentWatch, tx);
    }

    const telegramLines = [
      `<b>🔗 ${this.label} Early Bundler Token Transfer</b>`,
      `Token: <code>${mint}</code>`,
      `From: <code>${parentWatch.wallet}</code>${dropParent ? " (dropped from monitoring)" : hasUnresolvedPoolOutbound ? " (pool leg unresolved — still monitored)" : ""}`,
    ];
    if (recipients.size > 0) {
      telegramLines.push(`Recipients: <b>${recipients.size}</b>`);
      for (const [recipient, amount] of recipients) {
        telegramLines.push(
          `• <code>${recipient}</code>: <b>${amount.toLocaleString()}</b>`,
        );
      }
    } else if (hasUnresolvedPoolOutbound) {
      telegramLines.push(
        "No token recipient resolved from pool leg — sender not counted as a sell; monitoring continues until balance is zero.",
      );
    }
    telegramLines.push(
      `Tx: <code>${tx.signature}</code>`,
      "",
      dropParent
        ? "Sender unsubscribed; every recipient synced and monitored for individual sell txs."
        : "Sender kept until remaining balance is zero.",
    );
    void this.sendTelegramSafe(
      telegramLines.join("\n"),
      "follow-token early bundler transfer chained",
    );

    await this.maybeEvaluateFollowTokenEarlyBundlerExit(tx);
  }

  private subscribeFollowTokenEarlyBundlerExitWallet(wallet: string): void {
    const state = this.followTokenEarlyBundlerExitState;
    if (!state?.active) return;
    if (!this.isValidFollowTokenEarlyBundlerWatchWallet(wallet)) return;

    if (!state.enhancedWatchIds.has(wallet) && !state.logsSubIds.has(wallet)) {
      if (this.enhancedWs) {
        const watchId = this.enhancedWs.watch(wallet, (tx) => {
          this.handleEnhancedWsMintTx(tx, "early_bundler_exit", wallet);
        });
        state.enhancedWatchIds.set(wallet, watchId);
      } else {
        const pubkey = new PublicKey(wallet);
        const subId = this.connection.onLogs(
          pubkey,
          (logInfo) => {
            if (!logInfo.err) {
              this.queueSignature(logInfo.signature, "early_bundler_exit", wallet);
            }
          },
          "processed",
        );
        state.logsSubIds.set(wallet, subId);
      }
    }

    void this.subscribeFollowTokenEarlyBundlerExitTokenAta(wallet, state.mint);
  }

  private async subscribeFollowTokenEarlyBundlerExitTokenAta(
    wallet: string,
    mint: string,
  ): Promise<void> {
    const state = this.followTokenEarlyBundlerExitState;
    if (!state?.active || state.mint !== mint) return;
    if (state.tokenAtaBalanceSubIds.has(wallet)) return;
    if (!this.isValidFollowTokenEarlyBundlerWatchWallet(wallet)) return;

    const owner = new PublicKey(wallet);
    const mintPubkey = new PublicKey(mint);
    const programIds = [TOKEN_PROGRAM_ID, TOKEN_2022_PROGRAM_ID];
    const subIds = new Map<string, number>();
    const balances = new Map<string, bigint>();

    for (const programId of programIds) {
      const ata = getAssociatedTokenAddressSync(mintPubkey, owner, false, programId);
      const ataKey = ata.toBase58();
      try {
        const accountInfo = await this.connection.getAccountInfo(ata, "confirmed");
        balances.set(
          ataKey,
          this.decodeFollowTokenEarlyBundlerExitTokenAtaBalance(accountInfo),
        );
      } catch (err) {
        balances.set(ataKey, 0n);
        this.log.debug("Early bundler ATA seed balance fetch failed — defaulting to zero", {
          mint,
          wallet,
          ata: ataKey,
          error: err instanceof Error ? err.message : String(err),
        });
      }

      const subId = this.connection.onAccountChange(
        ata,
        (accountInfo) => {
          this.handleFollowTokenEarlyBundlerExitTokenAtaChange(
            wallet,
            ataKey,
            accountInfo,
          );
        },
        "confirmed",
      );
      subIds.set(ataKey, subId);
    }

    state.tokenAtaBalanceSubIds.set(wallet, subIds);
    state.tokenAtaBalancesByAccount.set(wallet, balances);
    const totalRaw = [...balances.values()].reduce((sum, value) => sum + value, 0n);
    state.tokenAtaLiveBalanceRaw.set(wallet, totalRaw);

    const watch = state.watches.get(wallet);
    if (watch) {
      this.markFollowTokenEarlyBundlerExitWatchObservedNonZeroBalance(
        watch,
        totalRaw,
      );
      this.applyFollowTokenEarlyBundlerExitWatchSoldAllFromLiveState(watch, null);
    }

    this.log.info("Subscribed follow-token early bundler token ATA balance", {
      mint,
      wallet,
      ataAccounts: [...subIds.keys()],
      liveTokenBalanceRaw: totalRaw.toString(),
    });
  }

  private decodeFollowTokenEarlyBundlerExitTokenAtaBalance(
    accountInfo: AccountInfo<Buffer> | null,
  ): bigint {
    if (!accountInfo || accountInfo.data.length < AccountLayout.span) {
      return 0n;
    }
    try {
      return AccountLayout.decode(accountInfo.data).amount;
    } catch {
      return 0n;
    }
  }

  private handleFollowTokenEarlyBundlerExitTokenAtaChange(
    wallet: string,
    ataKey: string,
    accountInfo: AccountInfo<Buffer>,
  ): void {
    const state = this.followTokenEarlyBundlerExitState;
    if (!state?.active) return;
    const watch = state.watches.get(wallet);
    if (!watch?.monitoringActive) return;

    const balanceRaw =
      this.decodeFollowTokenEarlyBundlerExitTokenAtaBalance(accountInfo);
    let accounts = state.tokenAtaBalancesByAccount.get(wallet);
    if (!accounts) {
      accounts = new Map<string, bigint>();
      state.tokenAtaBalancesByAccount.set(wallet, accounts);
    }
    accounts.set(ataKey, balanceRaw);

    const totalRaw = [...accounts.values()].reduce((sum, value) => sum + value, 0n);
    const previousRaw = state.tokenAtaLiveBalanceRaw.get(wallet) ?? null;
    state.tokenAtaLiveBalanceRaw.set(wallet, totalRaw);
    this.markFollowTokenEarlyBundlerExitWatchObservedNonZeroBalance(
      watch,
      totalRaw,
    );

    this.log.debug("Follow-token early bundler ATA balance updated", {
      mint: state.mint,
      wallet,
      ata: ataKey,
      ataBalanceRaw: balanceRaw.toString(),
      liveTokenBalanceRaw: totalRaw.toString(),
      previousLiveTokenBalanceRaw: previousRaw?.toString() ?? null,
    });

    if (
      totalRaw <= 0n &&
      !watch.soldAll &&
      watch.syncComplete &&
      this.followTokenEarlyBundlerExitWatchSoldAllByAtaBalance(watch, totalRaw)
    ) {
      watch.soldAll = true;
      watch.soldAllSignature = watch.soldAllSignature ?? "ata_zero_balance";
      watch.reachedTwentyFivePercentSold = true;
      if (previousRaw !== null && previousRaw > 0n) {
        this.bumpFollowTokenEarlyBundlerExitWatchMaxSingleSellFromAtaBalanceDrop(
          watch,
          previousRaw,
          totalRaw,
          state.mint,
        );
      }
      this.log.info("Follow-token early bundler sold all — ATA balance zero", {
        mint: state.mint,
        wallet,
        soldAmount: watch.soldAmount,
        boughtAmount: watch.boughtAmount,
        maxSingleSellTokenAmount: watch.maxSingleSellTokenAmount,
      });
      this.scheduleFollowTokenEarlyBundlerExitSoldAllEvalFromAta(
        state.mint,
        wallet,
      );
      return;
    }

    this.applyFollowTokenEarlyBundlerExitWatchSoldAllFromLiveState(watch, null);
  }

  private async unsubscribeFollowTokenEarlyBundlerExitTokenAta(
    wallet: string,
  ): Promise<void> {
    const state = this.followTokenEarlyBundlerExitState;
    if (!state) return;

    const subIds = state.tokenAtaBalanceSubIds.get(wallet);
    if (subIds) {
      for (const subId of subIds.values()) {
        await this.connection
          .removeAccountChangeListener(subId)
          .catch(() => undefined);
      }
      state.tokenAtaBalanceSubIds.delete(wallet);
    }
    state.tokenAtaBalancesByAccount.delete(wallet);
    state.tokenAtaLiveBalanceRaw.delete(wallet);
  }

  private async refreshFollowTokenEarlyBundlerExitWatchTokenBalanceAfterSync(
    wallet: string,
    mint: string,
    options?: { applySoldAll?: boolean },
  ): Promise<void> {
    await this.subscribeFollowTokenEarlyBundlerExitTokenAta(wallet, mint);
    try {
      const raw = await this.gmgnClient.getTokenRawBalance(wallet, mint);
      const state = this.followTokenEarlyBundlerExitState;
      if (!state) return;
      state.tokenAtaLiveBalanceRaw.set(wallet, raw);
      const watch = state.watches.get(wallet);
      if (watch) {
        this.markFollowTokenEarlyBundlerExitWatchObservedNonZeroBalance(
          watch,
          raw,
        );
        if (options?.applySoldAll !== false) {
          this.applyFollowTokenEarlyBundlerExitWatchSoldAllFromLiveState(
            watch,
            null,
          );
        }
      }
    } catch (err) {
      this.log.warn(
        "Failed to refresh follow-token early bundler token balance after sync",
        {
          mint,
          wallet,
          error: err instanceof Error ? err.message : String(err),
        },
      );
    }
  }

  private unsubscribeFollowTokenEarlyBundlerExitWallet(wallet: string): void {
    const state = this.followTokenEarlyBundlerExitState;
    if (!state) return;

    const enhancedWatchId = state.enhancedWatchIds.get(wallet);
    if (enhancedWatchId !== undefined) {
      void this.enhancedWs?.unwatch(enhancedWatchId).catch(() => undefined);
      state.enhancedWatchIds.delete(wallet);
    }

    const logsSubId = state.logsSubIds.get(wallet);
    if (logsSubId !== undefined) {
      void this.connection
        .removeOnLogsListener(logsSubId)
        .catch(() => undefined);
      state.logsSubIds.delete(wallet);
    }

    void this.unsubscribeFollowTokenEarlyBundlerExitTokenAta(wallet);
  }

  private dropFollowTokenEarlyBundlerExitWatchAfterTransfer(
    watch: FollowTokenEarlyBundlerExitWatch,
    tx: HeliusTransaction,
  ): void {
    if (!watch.monitoringActive) return;

    watch.monitoringActive = false;
    watch.soldAll = true;
    watch.soldAllSignature = tx.signature;
    watch.reachedTwentyFivePercentSold = true;
    this.unsubscribeFollowTokenEarlyBundlerExitWallet(watch.wallet);

    this.log.info(
      "Follow-token early bundler dropped from monitoring after token transfer-out",
      {
        wallet: watch.wallet,
        signature: tx.signature,
        parentWallet: watch.parentWallet,
      },
    );
  }

  private async syncFollowTokenEarlyBundlerExitWallet(
    wallet: string,
    mint: string,
  ): Promise<void> {
    const state = this.followTokenEarlyBundlerExitState;
    const watch = state?.watches.get(wallet);
    if (!state?.active || !watch) return;
    if (!this.isValidFollowTokenEarlyBundlerWatchWallet(wallet)) {
      state.watches.delete(wallet);
      watch.syncComplete = true;
      return;
    }

    this.log.info("Follow-token early bundler exit wallet sync started", {
      mint,
      wallet,
      source: watch.source,
      syncAfterSignature: watch.syncAfterSignature,
    });
    let syncSucceeded = false;
    try {
      syncSucceeded = await this.paginateFollowTokenEarlyBundlerExitWalletSync(
        watch,
        mint,
        watch.syncAfterSignature || undefined,
      );
    } catch (err) {
      const error = err instanceof Error ? err.message : String(err);
      this.log.warn("Follow-token early bundler exit wallet sync failed", {
        mint,
        wallet,
        error,
      });
      if (
        watch.syncAfterSignature &&
        error.includes("signature filter is too old")
      ) {
        this.log.warn(
          "Follow-token early bundler exit wallet sync retrying without signature cursor",
          { mint, wallet, syncAfterSignature: watch.syncAfterSignature },
        );
        try {
          syncSucceeded =
            await this.paginateFollowTokenEarlyBundlerExitWalletSync(
              watch,
              mint,
              undefined,
            );
        } catch (retryErr) {
          this.log.warn(
            "Follow-token early bundler exit wallet sync retry failed",
            {
              mint,
              wallet,
              error:
                retryErr instanceof Error ? retryErr.message : String(retryErr),
            },
          );
        }
      }
    }
    if (syncSucceeded) {
      watch.syncComplete = true;
    }
    await this.refreshFollowTokenEarlyBundlerExitWatchTokenBalanceAfterSync(
      wallet,
      mint,
      { applySoldAll: syncSucceeded },
    );
  }

  private async paginateFollowTokenEarlyBundlerExitWalletSync(
    watch: FollowTokenEarlyBundlerExitWatch,
    mint: string,
    cursor: string | undefined,
  ): Promise<boolean> {
    let pageCursor: string | undefined = cursor;
    for (let page = 0; page < 50; page++) {
      const batch = await this.withHeliusFallback((client) =>
        client.getAddressTransactionsAsc(
          watch.wallet,
          pageCursor,
          FOLLOW_TOKEN_EARLY_BUNDLER_EXIT_SYNC_PAGE_LIMIT,
        ),
      );
      if (batch.length === 0) break;

      for (const tx of batch) {
        if (!tx.signature || watch.observedTxSignatures.has(tx.signature)) {
          continue;
        }
        await this.applyFollowTokenEarlyBundlerExitTx(tx, mint, watch.wallet);
      }

      const lastSignature = batch[batch.length - 1]?.signature;
      if (
        !lastSignature ||
        batch.length < FOLLOW_TOKEN_EARLY_BUNDLER_EXIT_SYNC_PAGE_LIMIT
      ) {
        break;
      }
      pageCursor = lastSignature;
    }
    return true;
  }

  /** Paginated sync for every watch, including recipients chained mid-sync. */
  private async syncAllFollowTokenEarlyBundlerExitWatches(
    mint: string,
  ): Promise<void> {
    const state = this.followTokenEarlyBundlerExitState;
    if (!state?.active) return;

    while ([...state.watches.values()].some((watch) => !watch.syncComplete)) {
      for (const [wallet, watch] of [...state.watches.entries()]) {
        if (!watch.syncComplete) {
          await this.syncFollowTokenEarlyBundlerExitWallet(wallet, mint);
        }
      }
    }
  }

  private async startFollowTokenEarlyBundlerExitMonitoring(
    mint: string,
  ): Promise<void> {
    const earlyBuys = this.followTokenEarlyInsiderBuys;
    const funderState = this.bundlerFunderWatch;
    if (!earlyBuys?.length || !funderState || funderState.mint !== mint) {
      return;
    }
    if (
      this.followTokenEarlyBundlerExitState?.active &&
      this.followTokenEarlyBundlerExitState.mint === mint
    ) {
      this.log.info(
        "Follow-token early bundler exit watch already active — skipping restart",
        { mint },
      );
      return;
    }

    await this.stopFollowTokenEarlyBundlerExitMonitoring();

    const watches = new Map<string, FollowTokenEarlyBundlerExitWatch>();
    for (const buy of earlyBuys) {
      watches.set(buy.wallet, {
        wallet: buy.wallet,
        source: "early_bundler",
        parentWallet: null,
        syncAfterSignature: buy.signature,
        boughtAmount: buy.tokenAmount,
        soldAmount: 0,
        sellTxCount: 0,
        cumulativeSellUsd: 0,
        maxSingleSellTokenAmount: 0,
        soldAll: false,
        soldAllSignature: null,
        reachedTwentyFivePercentSold: false,
        observedTxSignatures: new Set(),
        syncComplete: false,
        monitoringActive: true,
        observedNonZeroTokenBalance: false,
      });
    }

    this.followTokenEarlyBundlerExitState = {
      active: true,
      mint,
      watches,
      mcTpReachedPending: false,
      allSoldAllComplete: false,
      highSellUsdMode: false,
      earlyBundlerTransferOutObserved: false,
      preBuyBundlerPathTriggered: false,
      preLiBundlerSoldAllBuy: false,
      preLiExitArmedNotified: false,
      preLiWaitingForValidLiNotified: false,
      preLiBundlerSoldAllBuyBlockedNotified: false,
      postLiBundlerSoldAllBuyBlockedNotified: false,
      validWalletTwentyFivePercentDeferred: false,
      maxSingleSellGateTierAtBuy: null,
      exitTriggerSignature: null,
      enhancedWatchIds: new Map(),
      logsSubIds: new Map(),
      tokenAtaBalanceSubIds: new Map(),
      tokenAtaBalancesByAccount: new Map(),
      tokenAtaLiveBalanceRaw: new Map(),
      deferredSoldAllEvalTimer: null,
    };

    for (const wallet of watches.keys()) {
      this.subscribeFollowTokenEarlyBundlerExitWallet(wallet);
    }

    await this.syncAllFollowTokenEarlyBundlerExitWatches(mint);

    await this.maybeEvaluateFollowTokenEarlyBundlerExit();

    this.log.info("Started follow-token early bundler exit monitoring", {
      mint,
      wallets: [...watches.keys()],
      pushDriven:
        this.followTokenEarlyBundlerExitState.enhancedWatchIds.size > 0,
    });

    void this.sendTelegramSafe(
      [
        `<b>👀 ${this.label} Early Bundler Exit Watch Started</b>`,
        `Token: <code>${mint}</code>`,
        `Wallets: <b>${watches.size}</b> initial bundler(s)`,
        "",
        "Started at follow-token Large Insider pre-buy flow. Synced from each initial buy tx (paginated). Token transfers chain **every** recipient (multi-recipient txs included); sender dropped only after full transfer-out. Recipient sync drains until no new watches remain.",
        this.buySubmitted
          ? `MC TP (+${FOLLOW_TOKEN_LARGE_INSIDER_PROFIT_EXIT_PERCENT}%) deferred until every early bundler/recipient sells all.`
          : this.hasFollowTokenLargeInsiderValidWalletDiscovered()
            ? `Post–1st-LI buy: valid wallet #4 (direct-sell) OR sold-all branches — 8M/16M gate uses largest active watch bag (early bundler buy or transfer received). Transfer-out → sold-all only.`
            : `Pre–1st-LI buy: sold-all + max single sell ≤ ${FOLLOW_TOKEN_EARLY_BUNDLER_PRE_LI_MAX_SINGLE_SELL_TOKEN_AMOUNT.toLocaleString()} only (+${FOLLOW_TOKEN_LARGE_INSIDER_PROFIT_EXIT_PERCENT}% MC TP). 8M–16M pre-LI sold-all → skip+reset. Cumulative &gt; $${FOLLOW_TOKEN_EARLY_BUNDLER_EXIT_HIGH_SELL_USD_MC_TP_DISABLE.toLocaleString()} before 1st valid LI → skip+reset (no LI ≥25% exit; MC TP disabled).`,
        `Post–1st-LI sold-all: 8M (+${FOLLOW_TOKEN_LARGE_INSIDER_PROFIT_EXIT_PERCENT}% MC TP) or 16M fallback (+${FOLLOW_TOKEN_EARLY_BUNDLER_FALLBACK_PROFIT_EXIT_PERCENT}% MC TP) · ≥1 valid LI Qualified SOL &lt;${FOLLOW_TOKEN_POST_LI_BUNDLER_BUY_REQUIRES_ONE_QUALIFIED_SOL_BELOW} SOL · 16M also requires token ATH MC &lt; ${FOLLOW_TOKEN_16M_FALLBACK_BUY_ATH_EXIT_MC_MULTIPLIER}× exit MC (GMGN fetch at buy).`,
        `After buy + all sold all (post–1st-LI): valid LI ≥25% → sell; max sell txs &lt; ${FOLLOW_TOKEN_EARLY_BUNDLER_EXIT_MIN_SELL_TX_COUNT_FOR_USD_GATE} or cumulative ≤ $${FOLLOW_TOKEN_EARLY_BUNDLER_EXIT_LOW_SELL_USD_THRESHOLD.toLocaleString()} → skip unless max single sell ≤ ${FOLLOW_TOKEN_EARLY_BUNDLER_FALLBACK_MAX_SINGLE_SELL_TOKEN_AMOUNT.toLocaleString()} tokens (+${FOLLOW_TOKEN_EARLY_BUNDLER_FALLBACK_PROFIT_EXIT_PERCENT}% MC TP buy on 8M–16M fallback); ≥ ${FOLLOW_TOKEN_EARLY_BUNDLER_EXIT_MIN_SELL_TX_COUNT_FOR_USD_GATE} txs + $${FOLLOW_TOKEN_EARLY_BUNDLER_EXIT_LOW_SELL_USD_THRESHOLD.toLocaleString()}–$${FOLLOW_TOKEN_EARLY_BUNDLER_EXIT_HIGH_SELL_USD_MC_TP_DISABLE.toLocaleString()} → dual exit; cumulative &gt; $${FOLLOW_TOKEN_EARLY_BUNDLER_EXIT_HIGH_SELL_USD_MC_TP_DISABLE.toLocaleString()} → LI-only.`,
      ].join("\n"),
      "follow-token early bundler exit watch started",
    );
  }

  private async stopFollowTokenEarlyBundlerExitMonitoring(): Promise<void> {
    const state = this.followTokenEarlyBundlerExitState;
    if (!state) return;

    this.clearFollowTokenEarlyBundlerExitDeferredSoldAllEvalTimer();

    for (const [wallet, subId] of state.logsSubIds) {
      await this.connection.removeOnLogsListener(subId).catch(() => undefined);
      state.logsSubIds.delete(wallet);
    }
    for (const [wallet, watchId] of state.enhancedWatchIds) {
      await this.enhancedWs?.unwatch(watchId).catch(() => undefined);
      state.enhancedWatchIds.delete(wallet);
    }
    for (const wallet of [...state.tokenAtaBalanceSubIds.keys()]) {
      await this.unsubscribeFollowTokenEarlyBundlerExitTokenAta(wallet);
    }
    this.followTokenEarlyBundlerExitState = null;
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
      if (this.enhancedWs) {
        const watchId = this.enhancedWs.watch(wallet, (tx) => {
          this.handleEnhancedWsMintTx(tx, "bundler", wallet);
        });
        this.bundlerEnhancedWatchIds.set(wallet, watchId);
        continue;
      }
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

    this.log.info("Started post-buy bundler monitoring", {
      mint,
      wallets,
      pushDriven: this.bundlerEnhancedWatchIds.size > 0,
    });
  }

  private async stopBundlerMonitoring(): Promise<void> {
    for (const [wallet, subId] of this.bundlerLogsSubIds) {
      await this.connection.removeOnLogsListener(subId).catch(() => undefined);
      this.bundlerLogsSubIds.delete(wallet);
    }
    for (const [wallet, watchId] of this.bundlerEnhancedWatchIds) {
      await this.enhancedWs?.unwatch(watchId).catch(() => undefined);
      this.bundlerEnhancedWatchIds.delete(wallet);
    }
    this.bundlerWatch = null;
  }

  private isPrimaryBundlerFunderWatchActive(): boolean {
    return (
      this.bundlerFunderLogsSubId !== null ||
      this.bundlerFunderEnhancedWatchId !== null
    );
  }

  private isParallelBundlerFunderWatchActive(): boolean {
    return (
      this.bundlerFunderParallelLogsSubId !== null ||
      this.bundlerFunderParallelEnhancedWatchId !== null
    );
  }

  private isPrimaryBundlerFunderSubscribedTo(address: string): boolean {
    return (
      this.isPrimaryBundlerFunderWatchActive() &&
      this.bundlerFunderPrimaryWatchAddress === address
    );
  }

  private isParallelBundlerFunderSubscribedTo(address: string): boolean {
    return (
      this.isParallelBundlerFunderWatchActive() &&
      this.bundlerFunderParallelWatchAddress === address
    );
  }

  private isBundlerFunderAddressWatched(address: string): boolean {
    return (
      this.isPrimaryBundlerFunderSubscribedTo(address) ||
      this.isParallelBundlerFunderSubscribedTo(address)
    );
  }

  private async clearParallelFeePayerFunderWatch(
    state: BundlerFunderWatchState,
    reason: string,
  ): Promise<void> {
    const hadParallel =
      state.parallelFeePayerFunderWallet !== null ||
      this.isParallelBundlerFunderWatchActive();
    if (!hadParallel) return;
    await this.unsubscribeParallelBundlerFunder();
    state.parallelFeePayerFunderWallet = null;
    state.parallelFeePayerFunderCursorSignature = null;
    state.parallelFeePayerFunderFundedAtSec = null;
    this.log.info("Cleared parallel feePayer-funder watch", {
      mint: state.mint,
      sharedFeePayer: state.funderWallet,
      reason,
    });
  }

  private subscribeParallelBundlerFunder(address: string): void {
    if (this.isParallelBundlerFunderSubscribedTo(address)) {
      return;
    }
    if (this.isPrimaryBundlerFunderSubscribedTo(address)) {
      this.log.debug(
        "Skipping parallel feePayer-funder subscribe — address already on primary watch",
        { address },
      );
      return;
    }
    if (this.isParallelBundlerFunderWatchActive()) {
      this.log.warn(
        "Skipping parallel feePayer-funder subscribe — another parallel watch is already active",
        {
          activeAddress: this.bundlerFunderParallelWatchAddress,
          requestedAddress: address,
        },
      );
      return;
    }
    if (this.enhancedWs) {
      this.bundlerFunderParallelEnhancedWatchId = this.enhancedWs.watch(address, (tx) => {
        void this.applyParallelBundlerFunderNotificationTx(tx);
      });
      this.bundlerFunderParallelWatchAddress = address;
      this.log.info(
        "Subscribed to parallel feePayer-funder transactions via Enhanced WSS",
        { address },
      );
      return;
    }
    this.bundlerFunderParallelLogsSubId = this.connection.onLogs(
      new PublicKey(address),
      (logInfo) => {
        if (!logInfo.err) {
          this.log.debug(
            "Parallel feePayer-funder logs notification ignored — Enhanced WSS required for push detection",
            { signature: logInfo.signature },
          );
        }
      },
      "processed",
    );
    this.bundlerFunderParallelWatchAddress = address;
    this.log.info(
      "Subscribed to parallel feePayer-funder transactions (logs only — Enhanced WSS recommended)",
      { address, syncLimit: BUNDLER_FUNDER_SYNC_LIMIT },
    );
  }

  private async unsubscribeParallelBundlerFunder(): Promise<void> {
    if (this.bundlerFunderParallelEnhancedWatchId !== null) {
      const id = this.bundlerFunderParallelEnhancedWatchId;
      this.bundlerFunderParallelEnhancedWatchId = null;
      await this.enhancedWs?.unwatch(id).catch(() => undefined);
    }
    if (this.bundlerFunderParallelLogsSubId !== null) {
      const subId = this.bundlerFunderParallelLogsSubId;
      this.bundlerFunderParallelLogsSubId = null;
      await this.connection.removeOnLogsListener(subId).catch(() => undefined);
    }
    this.bundlerFunderParallelWatchAddress = null;
  }

  /** Follow-token normal mode: also watch the wallet that funded the shared feePayer if funded ≤6h ago. */
  private async maybeSubscribeFollowTokenFeePayerFunderWatch(
    state: BundlerFunderWatchState,
  ): Promise<void> {
    if (this.flowSource !== "follow-token") return;
    if (state.lowFundingMode) return;
    if (state.parallelFeePayerFunderWallet) return;

    let funding: Awaited<ReturnType<HeliusClient["getWalletFundedBy"]>>;
    try {
      funding = await this.withHeliusFallback((client) =>
        client.getWalletFundedBy(state.funderWallet),
      );
    } catch (err) {
      this.log.warn("Failed to resolve shared feePayer funded-by for parallel watch", {
        mint: state.mint,
        funderWallet: state.funderWallet,
        error: err instanceof Error ? err.message : String(err),
      });
      return;
    }
    if (!funding?.funder) {
      this.log.debug("Shared feePayer has no Helius funded-by record; skipping parallel funder watch", {
        mint: state.mint,
        funderWallet: state.funderWallet,
      });
      return;
    }

    const fundedAtSec = funding.timestamp;
    if (!fundedAtSec || !Number.isFinite(fundedAtSec)) {
      this.log.debug("Shared feePayer funded-by has no timestamp; skipping parallel funder watch", {
        mint: state.mint,
        funderWallet: state.funderWallet,
        funder: funding.funder,
      });
      return;
    }

    const ageSec = Math.floor(Date.now() / 1000) - fundedAtSec;
    if (ageSec > FOLLOW_TOKEN_FEEPAYER_FUNDER_MAX_AGE_SEC) {
      this.log.info("Shared feePayer funder is older than 6h; skipping parallel watch", {
        mint: state.mint,
        funderWallet: state.funderWallet,
        funder: funding.funder,
        fundedAtSec,
        ageSec,
        maxAgeSec: FOLLOW_TOKEN_FEEPAYER_FUNDER_MAX_AGE_SEC,
      });
      return;
    }

    const funderWallet = funding.funder;
    if (
      funderWallet === state.funderWallet ||
      funderWallet === state.originalFunderWallet ||
      state.bundlerWallets.has(funderWallet) ||
      this.isBundlerFunderAddressWatched(funderWallet)
    ) {
      this.log.debug("Shared feePayer funder overlaps feePayer/bundler set or existing watch; skipping parallel watch", {
        mint: state.mint,
        funderWallet: state.funderWallet,
        parallelFunder: funderWallet,
        alreadyWatched: this.isBundlerFunderAddressWatched(funderWallet),
      });
      return;
    }

    this.subscribeParallelBundlerFunder(funderWallet);
    if (!this.isParallelBundlerFunderSubscribedTo(funderWallet)) {
      return;
    }

    state.parallelFeePayerFunderWallet = funderWallet;
    state.parallelFeePayerFunderFundedAtSec = fundedAtSec;
    state.parallelFeePayerFunderCursorSignature = funding.signature ?? null;
    if (funding.signature) {
      state.processedSignatures.add(funding.signature);
    }

    if (!this.buySubmitted) {
      await this.syncParallelFeePayerFunderTransactions(true);
    }

    this.log.warn("Follow-token: parallel feePayer-funder watch started", {
      mint: state.mint,
      sharedFeePayer: state.funderWallet,
      parallelFunder: funderWallet,
      fundedAtSec,
      ageSec,
      fundingSignature: funding.signature ?? null,
    });
  }

  /** Enhanced WSS push path for the parallel feePayer-funder wallet (no migration/handoff). */
  private async applyParallelBundlerFunderNotificationTx(
    tx: HeliusTransaction,
  ): Promise<void> {
    const state = this.bundlerFunderWatch;
    const watchedWallet = state?.parallelFeePayerFunderWallet;
    if (!state || !watchedWallet || this.positionSellTriggered) return;
    if (state.discoveryStopped) return;
    if (state.processedSignatures.has(tx.signature)) return;
    if (
      state.parallelFeePayerFunderFundedAtSec !== null &&
      tx.timestamp < state.parallelFeePayerFunderFundedAtSec
    ) {
      return;
    }
    state.processedSignatures.add(tx.signature);
    state.parallelFeePayerFunderCursorSignature = tx.signature;
    try {
      await this.inspectBundlerFunderTransaction(state, tx, watchedWallet);
    } catch (err) {
      this.log.warn("Failed to apply parallel feePayer-funder Enhanced WSS notification", {
        mint: state.mint,
        parallelFunder: watchedWallet,
        sharedFeePayer: state.funderWallet,
        signature: tx.signature,
        error: err instanceof Error ? err.message : String(err),
      });
    }
  }

  private async syncParallelFeePayerFunderTransactions(force = false): Promise<void> {
    const state = this.bundlerFunderWatch;
    const watchedWallet = state?.parallelFeePayerFunderWallet;
    if (!state || !watchedWallet || this.positionSellTriggered) return;
    if (state.discoveryStopped) return;
    if (state.lowFundingMode) return;
    if (this.hasReachedFunderRecipientBuyCap(state)) return;
    if (this.isParallelFeePayerFunderSyncing) return;
    if (
      !force &&
      Date.now() - this.lastParallelFeePayerFunderSyncAt <
        BUNDLER_FUNDER_SYNC_MIN_INTERVAL_MS
    ) {
      return;
    }

    this.isParallelFeePayerFunderSyncing = true;
    this.lastParallelFeePayerFunderSyncAt = Date.now();
    const syncingWallet = watchedWallet;
    try {
      const txs = await this.withHeliusFallback((client) =>
        client.getAddressTransactionsAsc(
          syncingWallet,
          state.parallelFeePayerFunderCursorSignature ?? undefined,
          BUNDLER_FUNDER_SYNC_LIMIT,
        ),
      );
      for (const tx of txs) {
        if (state.parallelFeePayerFunderWallet !== syncingWallet) break;
        if (this.hasReachedFunderRecipientBuyCap(state)) break;
        if (state.processedSignatures.has(tx.signature)) continue;
        if (
          state.parallelFeePayerFunderFundedAtSec !== null &&
          tx.timestamp < state.parallelFeePayerFunderFundedAtSec
        ) {
          continue;
        }
        state.processedSignatures.add(tx.signature);
        state.parallelFeePayerFunderCursorSignature = tx.signature;
        await this.inspectBundlerFunderTransaction(state, tx, syncingWallet);
        if (state.discoveryStopped || !this.bundlerFunderWatch) break;
      }
    } catch (err) {
      void this.heliusClient.handlePossibleRateLimitError(err);
      this.log.warn("Parallel feePayer-funder sync failed", {
        mint: state.mint,
        parallelFunder: watchedWallet,
        sharedFeePayer: state.funderWallet,
        error: err instanceof Error ? err.message : String(err),
      });
    } finally {
      this.isParallelFeePayerFunderSyncing = false;
    }
  }

  private subscribeBundlerFunder(address: string): void {
    if (this.isPrimaryBundlerFunderSubscribedTo(address)) {
      return;
    }
    if (this.isParallelBundlerFunderSubscribedTo(address)) {
      this.log.warn(
        "subscribeBundlerFunder blocked — address is still on parallel watch; clear parallel first",
        { address },
      );
      return;
    }
    if (this.isPrimaryBundlerFunderWatchActive()) {
      this.log.warn(
        "subscribeBundlerFunder skipped — primary watch already active on a different address",
        {
          activeAddress: this.bundlerFunderPrimaryWatchAddress,
          requestedAddress: address,
        },
      );
      return;
    }
    if (this.enhancedWs) {
      this.bundlerFunderEnhancedWatchId = this.enhancedWs.watch(address, (tx) => {
        void this.applyBundlerFunderNotificationTx(tx);
      });
      this.bundlerFunderPrimaryWatchAddress = address;
      this.log.info("Subscribed to shared bundler funder transactions via Enhanced WSS", {
        address,
      });
      return;
    }
    this.bundlerFunderLogsSubId = this.connection.onLogs(
      new PublicKey(address),
      (logInfo) => {
        if (!logInfo.err) {
          this.log.debug(
            "Shared feePayer logs notification ignored — Enhanced WSS required for push detection",
            { signature: logInfo.signature },
          );
        }
      },
      "processed",
    );
    this.bundlerFunderPrimaryWatchAddress = address;
    this.log.info("Subscribed to shared bundler funder transactions (logs only — Enhanced WSS recommended)", {
      address,
      syncLimit: BUNDLER_FUNDER_SYNC_LIMIT,
    });
  }

  /** Fed by a fresh Enhanced WSS `transactionSubscribe` notification for the watched feePayer — already fully parsed, no REST fetch needed. Mirrors the per-tx body of syncBundlerFunderTransactions' loop. */
  private async applyBundlerFunderNotificationTx(tx: HeliusTransaction): Promise<void> {
    const state = this.bundlerFunderWatch;
    if (!state || this.positionSellTriggered) return;
    if (state.discoveryStopped) return;
    if (state.processedSignatures.has(tx.signature)) return;
    // Enhanced WSS accountInclude also matches txs where the watched address
    // is merely referenced (not necessarily the fee payer) — harmless here,
    // since inspectBundlerFunderTransaction only acts on outgoing transfers
    // *from* state.funderWallet and no-ops otherwise.
    state.processedSignatures.add(tx.signature);
    state.cursorSignature = tx.signature;
    try {
      const migrated = await this.inspectBundlerFunderTransaction(state, tx);
      if (!migrated) {
        await this.maybeTriggerLowFundingPendingTinyBuys(state, "shared feePayer Enhanced WSS notification");
      }
    } catch (err) {
      this.log.warn("Failed to apply shared feePayer Enhanced WSS notification", {
        mint: state.mint,
        funderWallet: state.funderWallet,
        signature: tx.signature,
        error: err instanceof Error ? err.message : String(err),
      });
    }
  }

  private async unsubscribePrimaryBundlerFunder(): Promise<void> {
    if (this.bundlerFunderEnhancedWatchId !== null) {
      const id = this.bundlerFunderEnhancedWatchId;
      this.bundlerFunderEnhancedWatchId = null;
      await this.enhancedWs?.unwatch(id).catch(() => undefined);
    }
    if (this.bundlerFunderLogsSubId !== null) {
      const subId = this.bundlerFunderLogsSubId;
      this.bundlerFunderLogsSubId = null;
      await this.connection.removeOnLogsListener(subId).catch(() => undefined);
    }
    this.bundlerFunderPrimaryWatchAddress = null;
  }

  private async unsubscribeBundlerFunder(): Promise<void> {
    await this.unsubscribeParallelBundlerFunder();
    await this.unsubscribePrimaryBundlerFunder();
  }

  private async switchBundlerFunderWatchAddress(
    state: BundlerFunderWatchState,
    nextWallet: string,
    cursorSignature: string,
    reason: string,
  ): Promise<void> {
    if (nextWallet === state.funderWallet) return;
    if (state.parallelFeePayerFunderWallet === nextWallet) {
      await this.clearParallelFeePayerFunderWatch(
        state,
        "handoff target already watched as parallel feePayer funder",
      );
    }
    await this.unsubscribePrimaryBundlerFunder();
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

  private isBuyBlockedByDevTokenOut(mint?: string | null): boolean {
    if (this.devTokenOutHandled) return true;
    if (mint && this.devTokenOutBlockedMints.has(mint)) return true;
    return false;
  }

  private getDevTokenOutFlowMint(): string | null {
    return (
      this.watchingMint ??
      this.activePosition?.mint ??
      this.followTokenTopBuyerMint ??
      this.bundlerFunderWatch?.mint ??
      null
    );
  }

  private isDevWalletTokenOutWatchActive(mint: string): boolean {
    if (!this.devWallet || this.devTokenOutHandled) return false;
    if (this.activePosition?.mint === mint) {
      if (this.fastMigrationSellMode !== null) return true;
      return (
        this.devTokenOutWatchUntilMs !== null &&
        Date.now() <= this.devTokenOutWatchUntilMs
      );
    }
    if (this.watchingMint === mint && !this.activePosition) return true;
    if (
      this.buySubmitted &&
      !this.activePosition &&
      this.getDevTokenOutFlowMint() === mint
    ) {
      return true;
    }
    return false;
  }

  private armDevTokenOutPostBuyWatch(mint: string): void {
    this.clearDevTokenOutPostBuyWatchTimer();
    if (this.fastMigrationSellMode !== null) {
      this.devTokenOutWatchUntilMs = null;
      this.log.info(
        "Special migration buy dev token-out watch armed without 3-minute expiry",
        {
          mint,
          mode: this.fastMigrationSellMode,
        },
      );
      return;
    }
    this.devTokenOutWatchUntilMs =
      Date.now() + DEV_WALLET_TOKEN_OUT_POST_BUY_WATCH_MS;
    this.devTokenOutPostBuyWatchTimer = setTimeout(() => {
      this.devTokenOutPostBuyWatchTimer = null;
      this.devTokenOutWatchUntilMs = null;
      this.log.info("Dev wallet token-out post-buy watch expired", {
        mint,
        watchMs: DEV_WALLET_TOKEN_OUT_POST_BUY_WATCH_MS,
      });
    }, DEV_WALLET_TOKEN_OUT_POST_BUY_WATCH_MS);
  }

  private clearDevTokenOutPostBuyWatchTimer(): void {
    if (this.devTokenOutPostBuyWatchTimer !== null) {
      clearTimeout(this.devTokenOutPostBuyWatchTimer);
      this.devTokenOutPostBuyWatchTimer = null;
    }
  }

  private async listDevBuysAfterCreateMint(
    mint: string,
  ): Promise<HeliusTransaction[]> {
    if (!this.devWallet) return [];
    const txs = await this.withHeliusFallback(
      (client) =>
        client.getWalletTransactionsDesc(
          this.devWallet!,
          LOW_FUNDING_DEV_BUY_SYNC_LIMIT,
        ),
      HELIUS_POOL_MC_RESERVED_INDEX,
    );
    return txs
      .filter((tx) => this.isRelevantMintTx(tx, mint))
      .filter((tx) => tx.signature !== this.devCreateSignature)
      .filter(
        (tx) =>
          this.devCreateTimestamp === null ||
          tx.timestamp > this.devCreateTimestamp,
      )
      .filter((tx) => this.classifyTx(tx, this.devWallet!, mint) === "buy")
      .sort((a, b) => a.timestamp - b.timestamp || a.slot - b.slot);
  }

  private async evaluateDevBuyCountBeforeBuyGate(mint: string): Promise<{
    pass: boolean;
    deferred: boolean;
    devBuyCountAfterCreate: number;
    devBuys: HeliusTransaction[];
  }> {
    if (!this.devWallet) {
      return {
        pass: true,
        deferred: false,
        devBuyCountAfterCreate: 0,
        devBuys: [],
      };
    }
    try {
      const devBuys = await this.listDevBuysAfterCreateMint(mint);
      const devBuyCountAfterCreate = devBuys.length;
      return {
        pass: devBuyCountAfterCreate < DEV_BUY_COUNT_AFTER_CREATE_MAX_EXCLUSIVE,
        deferred: false,
        devBuyCountAfterCreate,
        devBuys,
      };
    } catch (err) {
      void this.heliusClient.handlePossibleRateLimitError(err);
      this.log.warn("Dev buy count before buy gate check failed; deferring buy", {
        mint,
        devWallet: this.devWallet,
        devCreateSignature: this.devCreateSignature,
        error: err instanceof Error ? err.message : String(err),
      });
      return {
        pass: false,
        deferred: true,
        devBuyCountAfterCreate: -1,
        devBuys: [],
      };
    }
  }

  private async ensureDevBuyCountAllowsBuy(
    mint: string,
    context: { signature?: string; triggerLabel: string },
  ): Promise<boolean> {
    const gate = await this.evaluateDevBuyCountBeforeBuyGate(mint);
    if (gate.pass) return true;
    if (gate.deferred) return false;
    await this.skipFromDevBuyCountAfterCreateGate(
      mint,
      gate.devBuyCountAfterCreate,
      gate.devBuys,
      context,
    );
    return false;
  }

  private async skipFromDevBuyCountAfterCreateGate(
    mint: string,
    devBuyCountAfterCreate: number,
    devBuys: HeliusTransaction[],
    context: { signature?: string; triggerLabel: string },
  ): Promise<void> {
    const ebState = this.followTokenEarlyBundlerExitState;
    const funderState = this.bundlerFunderWatch;
    if (ebState?.active) {
      ebState.preBuyBundlerPathTriggered = true;
      ebState.exitTriggerSignature =
        context.signature ?? "DEV_BUY_COUNT_AFTER_CREATE_GATE";
    }
    const reason = "dev_buy_count_after_create_gate";
    const devBuyLines = devBuys.slice(0, 5).map(
      (tx, index) =>
        `${index + 1}. <code>${tx.signature}</code> · ${new Date(tx.timestamp * 1000).toISOString()}`,
    );

    this.log.warn("Buy skipped — dev wallet made too many buys after mint", {
      mint,
      triggerLabel: context.triggerLabel,
      devWallet: this.devWallet,
      devCreateSignature: this.devCreateSignature,
      devBuyCountAfterCreate,
      maxExclusive: DEV_BUY_COUNT_AFTER_CREATE_MAX_EXCLUSIVE,
      triggerSignature: context.signature ?? null,
      devBuySignatures: devBuys.map((tx) => tx.signature),
    });

    void this.sendTelegramSafe(
      [
        `<b>⛔ ${this.label} Buy Skipped — Dev Buy Count Gate</b>`,
        `Token: <code>${mint}</code>`,
        `Trigger: <b>${context.triggerLabel}</b>`,
        `Dev: <code>${this.devWallet ?? "unknown"}</code>`,
        `Mint/create tx: <code>${this.devCreateSignature ?? "unknown"}</code>`,
        "",
        `Dev buys after mint (excluding create): <b>${devBuyCountAfterCreate}</b> — limit is <b>&lt; ${DEV_BUY_COUNT_AFTER_CREATE_MAX_EXCLUSIVE}</b>.`,
        "",
        ...devBuyLines,
        devBuys.length > devBuyLines.length
          ? `… and ${devBuys.length - devBuyLines.length} more`
          : "",
        "",
        "No buy — token skipped and flow reset.",
      ]
        .filter(Boolean)
        .join("\n"),
      "dev buy count after create gate skip",
    );

    if (funderState && this.followTokenLargeInsiderState?.active) {
      await this.stopFollowTokenLargeInsiderFlow(
        "dev buy count after create gate skip",
      );
    }
    await this.resetForNewToken(false, { reason });
  }

  private isDevWalletTokenTransferOutTx(tx: HeliusTransaction, mint: string): boolean {
    if (!this.devWallet) return false;
    if (!this.isRelevantMintTx(tx, mint)) return false;
    if (
      this.devCreateTimestamp !== null &&
      tx.timestamp <= this.devCreateTimestamp
    ) {
      return false;
    }
    const kind = this.classifyTx(tx, this.devWallet, mint);
    return kind === "transfer_out";
  }

  private async evaluateDevWalletTokenOutTx(
    tx: HeliusTransaction,
  ): Promise<void> {
    if (!this.devWallet || this.devTokenOutHandled) return;
    const mint = this.getDevTokenOutFlowMint();
    if (!mint || !this.isDevWalletTokenOutWatchActive(mint)) return;
    if (!this.isDevWalletTokenTransferOutTx(tx, mint)) return;
    await this.handleDevWalletTokenOut(mint, tx);
  }

  private async syncDevWalletTokenOutHistory(mint: string): Promise<void> {
    if (
      !this.devWallet ||
      this.devTokenOutHandled ||
      !this.isDevWalletTokenOutWatchActive(mint)
    ) {
      return;
    }
    try {
      const txs = await this.withHeliusFallback((client) =>
        client.getWalletTransactionsDesc(this.devWallet!, LOW_FUNDING_DEV_BUY_SYNC_LIMIT),
      );
      for (const tx of [...txs].reverse()) {
        if (this.devFullExitSeenSignatures.has(tx.signature)) continue;
        if (!this.isDevWalletTokenTransferOutTx(tx, mint)) continue;
        this.devFullExitSeenSignatures.add(tx.signature);
        await this.handleDevWalletTokenOut(mint, tx);
        return;
      }
    } catch (err) {
      this.log.warn("Dev wallet token-out history sync failed", {
        mint,
        devWallet: this.devWallet,
        error: err instanceof Error ? err.message : String(err),
      });
    }
  }

  private async handleDevWalletTokenOut(
    mint: string,
    tx: HeliusTransaction,
  ): Promise<void> {
    if (this.devTokenOutHandled || !this.isDevWalletTokenOutWatchActive(mint)) {
      return;
    }
    this.devTokenOutHandled = true;
    this.devTokenOutBlockedMints.add(mint);
    const hasPosition = !!this.activePosition;

    if (hasPosition) {
      if (this.fastMigrationSellMode === "transfer_out_mc_retrace") {
        const transferOutMc = await this.gmgnClient.fetchTokenMarketCapUsd(mint);
        if (transferOutMc !== null) {
          this.fastMigrationTransferOutMc = transferOutMc;
        }
        this.log.warn(
          "4s migration dev token transfer-out observed — waiting for +60% TP or MC retrace",
          { mint, devWallet: this.devWallet, signature: tx.signature, transferOutMc },
        );
        void this.sendTelegramSafe(
          [
            `<b>⚠️ ${this.label} 4s Migration Dev Transfer-Out Observed</b>`,
            `Token: <code>${mint}</code>`,
            `Tx: <code>${tx.signature}</code>`,
            transferOutMc !== null
              ? `Transfer-out MC baseline: <b>$${transferOutMc.toLocaleString()}</b>`
              : "Transfer-out MC baseline unavailable; +60% TP remains active.",
            "No immediate sell. Selling at +60% TP or if MC retraces to this baseline.",
          ].join("\n"),
          "4s migration dev transfer-out observed",
        );
        return;
      }
      this.log.warn("Dev wallet token transfer-out after buy — selling 100%", {
        mint,
        devWallet: this.devWallet,
        signature: tx.signature,
      });
      await this.triggerPositionSell(
        mint,
        `Dev wallet ${this.devWallet} transfer_out within 3m post-buy`,
        [
          `<b>⛔ ${this.label} Dev Wallet Token Transfer-Out After Buy — Selling 100%</b>`,
          `Token: <code>${mint}</code>`,
          `Dev: <code>${this.devWallet}</code>`,
          `Tx: <code>${tx.signature}</code>`,
          `Action: <b>transfer_out</b>`,
          "",
          "Dev transferred tokens out within 3 minutes of buy — selling 100%.",
        ],
        tx.signature,
      );
      return;
    }

    this.log.warn("Dev wallet token transfer-out before buy — skipping token", {
      mint,
      devWallet: this.devWallet,
      signature: tx.signature,
    });
    void this.sendTelegramSafe(
      [
        `<b>⛔ ${this.label} Dev Wallet Token Transfer-Out Before Buy</b>`,
        `Token: <code>${mint}</code>`,
        `Dev: <code>${this.devWallet}</code>`,
        `Tx: <code>${tx.signature}</code>`,
        `Action: <b>transfer_out</b>`,
        "",
        "Dev transferred tokens out before bot buy — skipping token and resetting flow.",
      ].join("\n"),
      "dev wallet token transfer-out before buy",
    );
    // Teardown must not wait on Telegram. The transfer-out is terminal for
    // this pre-buy flow, and pending watcher callbacks must be stopped first.
    await this.resetForNewToken(false, {
      reason: "dev_wallet_token_out_before_buy",
    });
  }

  /**
   * Detects the dev wallet fully cashing out: a CLOSE_ACCOUNT tx (source
   * SOLANA_PROGRAM_LIBRARY) paid for by the dev wallet itself, closing a WSOL
   * token account after unwrapping sell proceeds to native SOL. This is used
   * as the token's rug/reset signal in place of a fixed MC floor — a dev
   * closing out their WSOL account is a much sharper "fully sold" signal
   * than market cap alone, which can lag or bounce around a threshold.
   *
   * Detection is push-based, not polled: a websocket log subscription on the
   * dev wallet (subscribeDevWalletFullExitWatch) notifies us of each new
   * signature, and only that specific signature is fetched/parsed via the
   * getTransactionsBySignatures API — the same subscribe-then-getTx pattern
   * used everywhere else in this file (queueSignature/processSignatureBatch,
   * subscribeLowFundingDevWalletSubscription, subscribeFunderRecipient).
   */
  private subscribeDevWalletFullExitWatch(): void {
    if (!this.devWallet || this.devFullExitHandled) return;
    if (
      this.devFullExitLogsSubId !== null ||
      this.devFullExitEnhancedWatchId !== null ||
      this.devSolBalanceSubId !== null
    ) {
      return;
    }
    if (this.enhancedWs) {
      this.devFullExitEnhancedWatchId = this.enhancedWs.watch(this.devWallet, (tx) => {
        if (this.devFullExitSeenSignatures.has(tx.signature)) return;
        this.devFullExitSeenSignatures.add(tx.signature);
        void this.evaluateDevWalletTokenOutTx(tx);
        void this.evaluateDevWalletFullExitTx(tx);
      });
      this.log.info("Subscribed to dev wallet for full-exit (CLOSE_ACCOUNT) detection via Enhanced WSS", {
        devWallet: this.devWallet,
        devCreateSignature: this.devCreateSignature,
        devCreateTimestamp: this.devCreateTimestamp,
      });
    } else {
      this.devFullExitLogsSubId = this.connection.onLogs(
        new PublicKey(this.devWallet),
        (logInfo) => {
          if (!logInfo.err) {
            void this.checkDevWalletSignatureForFullExit(logInfo.signature);
          }
        },
        "processed",
      );
      this.log.info("Subscribed to dev wallet for full-exit (CLOSE_ACCOUNT) detection", {
        devWallet: this.devWallet,
        devCreateSignature: this.devCreateSignature,
        devCreateTimestamp: this.devCreateTimestamp,
      });
    }
    this.devSolBalanceSubId = this.connection.onAccountChange(
      new PublicKey(this.devWallet),
      (accountInfo) => {
        if (accountInfo.lamports === 0) {
          void this.handleDevWalletZeroBalance();
        }
      },
      "processed",
    );
    this.log.info("Subscribed to dev wallet native SOL balance for zero-balance rug detection", {
      devWallet: this.devWallet,
    });
    void this.checkDevWalletZeroBalanceImmediate();
    const mint = this.watchingMint;
    if (mint) {
      void this.syncDevWalletTokenOutHistory(mint);
    }
  }

  private async stopDevWalletFullExitWatch(reason: string): Promise<void> {
    if (this.devFullExitEnhancedWatchId !== null) {
      const id = this.devFullExitEnhancedWatchId;
      this.devFullExitEnhancedWatchId = null;
      await this.enhancedWs?.unwatch(id).catch(() => undefined);
      this.log.info("Stopped dev wallet full-exit subscription", {
        devWallet: this.devWallet,
        reason,
      });
    }
    if (this.devFullExitLogsSubId !== null) {
      const subId = this.devFullExitLogsSubId;
      this.devFullExitLogsSubId = null;
      await this.connection.removeOnLogsListener(subId).catch(() => undefined);
      this.log.info("Stopped dev wallet full-exit subscription", {
        devWallet: this.devWallet,
        reason,
      });
    }
    if (this.devSolBalanceSubId !== null) {
      const subId = this.devSolBalanceSubId;
      this.devSolBalanceSubId = null;
      await this.connection.removeAccountChangeListener(subId).catch(() => undefined);
      this.log.info("Stopped dev wallet SOL balance subscription", {
        devWallet: this.devWallet,
        reason,
      });
    }
  }

  private async checkDevWalletZeroBalanceImmediate(): Promise<void> {
    if (!this.devWallet || this.devFullExitHandled) return;
    const mint = this.watchingMint ?? this.activePosition?.mint;
    if (!mint) return;
    try {
      const lamports = await this.connection.getBalance(new PublicKey(this.devWallet));
      if (lamports === 0) {
        await this.handleDevWalletZeroBalance();
      }
    } catch (err) {
      this.log.warn("Failed immediate dev wallet zero-balance check", {
        mint,
        devWallet: this.devWallet,
        error: err instanceof Error ? err.message : String(err),
      });
    }
  }

  /**
   * Acts on an already-fetched/normalized dev-wallet transaction, whether it
   * arrived as a fresh Enhanced WSS notification (no REST call at all) or
   * from the REST fallback fetch in checkDevWalletSignatureForFullExit.
   */
  private async evaluateDevWalletFullExitTx(tx: HeliusTransaction): Promise<void> {
    if (!this.devWallet || this.devFullExitHandled) return;
    const mint = this.watchingMint ?? this.activePosition?.mint;
    if (!mint) return;
    if (!isDevRugCloseAccountTx(tx, this.devWallet)) return;
    if (
      this.devCreateTimestamp !== null &&
      tx.timestamp <= this.devCreateTimestamp
    ) {
      return;
    }
    await this.handleDevWalletFullExit(mint, tx);
  }

  private async checkDevWalletSignatureForFullExit(
    signature: string,
  ): Promise<void> {
    if (!this.devWallet || this.devFullExitHandled) return;
    if (this.devFullExitSeenSignatures.has(signature)) return;
    this.devFullExitSeenSignatures.add(signature);

    const mint = this.watchingMint ?? this.activePosition?.mint;
    if (!mint) return;

    try {
      const txs = await this.withHeliusFallback((client) =>
        client.getTransactionsBySignatures([signature]),
      );
      const tx = txs.find((t) => t.signature === signature);
      if (!tx) return;
      await this.evaluateDevWalletTokenOutTx(tx);
      if (this.devTokenOutHandled) return;
      if (!isDevRugCloseAccountTx(tx, this.devWallet)) return;
      if (
        this.devCreateTimestamp !== null &&
        tx.timestamp <= this.devCreateTimestamp
      ) {
        return;
      }
      await this.handleDevWalletFullExit(mint, tx);
    } catch (err) {
      void this.heliusClient.handlePossibleRateLimitError(err);
      this.log.warn("Failed to check dev wallet signature for full exit", {
        mint,
        devWallet: this.devWallet,
        signature,
        error: err instanceof Error ? err.message : String(err),
      });
    }
  }

  /**
   * Acts on a confirmed full-exit CLOSE_ACCOUNT tx: either resets the pre-buy
   * flow or triggers an immediate sell of the held position — replacing the
   * old "MC below $5,000" rug check.
   */
  private async handleDevWalletFullExit(
    mint: string,
    exitTx: HeliusTransaction,
  ): Promise<void> {
    await this.handleDevWalletRugSignal(mint, {
      kind: "close_account",
      signature: exitTx.signature,
      timestamp: exitTx.timestamp,
    });
  }

  /** Dev native SOL hit zero — same rug reset/sell path as CLOSE_ACCOUNT full exit. */
  private async handleDevWalletZeroBalance(): Promise<void> {
    const mint = this.watchingMint ?? this.activePosition?.mint;
    if (!mint) return;
    await this.handleDevWalletRugSignal(mint, { kind: "zero_balance" });
  }

  private async handleDevWalletRugSignal(
    mint: string,
    signal:
      | { kind: "close_account"; signature: string; timestamp: number }
      | { kind: "zero_balance" }
      | { kind: "mc_floor"; currentMc: number },
  ): Promise<void> {
    if (this.devFullExitHandled) return;
    if (this.followTokenTopBuyerMint === mint) {
      await this.stopFollowTokenTopBuyerWatch(`rug signal (${signal.kind})`);
    }
    this.devFullExitHandled = true;
    const stopReason =
      signal.kind === "zero_balance"
        ? "dev zero balance detected"
        : signal.kind === "mc_floor"
          ? "MC below rug reset floor"
          : "full exit detected";
    await this.stopDevWalletFullExitWatch(stopReason);

    if (signal.kind === "close_account") {
      this.log.warn(
        "Dev wallet closed a WSOL token account (SOLANA_PROGRAM_LIBRARY CLOSE_ACCOUNT); treating as full dev exit",
        {
          mint,
          devWallet: this.devWallet,
          signature: signal.signature,
          timestamp: signal.timestamp,
          phase: this.phase,
          hasActivePosition: !!this.activePosition,
        },
      );
    } else if (signal.kind === "zero_balance") {
      this.log.warn("Dev wallet native SOL balance reached zero; treating as full dev exit", {
        mint,
        devWallet: this.devWallet,
        phase: this.phase,
        hasActivePosition: !!this.activePosition,
      });
    } else {
      this.log.warn("Token market cap fell below rug reset floor; treating as rug", {
        mint,
        devWallet: this.devWallet,
        currentMc: signal.currentMc,
        rugResetThresholdUsd: INSIDER_RUG_RESET_MARKET_CAP_USD,
        phase: this.phase,
        hasActivePosition: !!this.activePosition,
      });
    }

    if (this.activePosition) {
      const reason =
        signal.kind === "close_account"
          ? `Dev wallet ${this.devWallet} closed its WSOL account (full exit detected)`
          : signal.kind === "zero_balance"
            ? `Dev wallet ${this.devWallet} native SOL balance reached zero (full exit detected)`
            : `Token MC $${signal.currentMc.toLocaleString()} fell below $${INSIDER_RUG_RESET_MARKET_CAP_USD.toLocaleString()} rug reset floor`;
      const telegramLines =
        signal.kind === "close_account"
          ? [
              "<b>🚨 Dev Full-Exit Detected — Selling ASAP</b>",
              `Token: <code>${mint}</code>`,
              `Dev wallet: <code>${this.devWallet}</code>`,
              `Close-account tx: <code>${signal.signature}</code>`,
              "Dev wallet closed its WSOL token account — treated as a full exit/rug signal.",
            ]
          : signal.kind === "zero_balance"
            ? [
                "<b>🚨 Dev Zero-Balance Exit — Selling ASAP</b>",
                `Token: <code>${mint}</code>`,
                `Dev wallet: <code>${this.devWallet}</code>`,
                "Dev wallet native SOL reached zero — treated as a full exit/rug signal.",
              ]
            : [
                "<b>🚨 MC Rug Reset — Selling ASAP</b>",
                `Token: <code>${mint}</code>`,
                `Current MC: <b>$${signal.currentMc.toLocaleString()}</b>`,
                `Rug reset floor: <b>$${INSIDER_RUG_RESET_MARKET_CAP_USD.toLocaleString()}</b>`,
                "Market cap dropped below the rug reset threshold.",
              ];
      await this.triggerPositionSell(
        mint,
        reason,
        telegramLines,
        signal.kind === "close_account"
          ? signal.signature
          : signal.kind === "mc_floor"
            ? "mc-rug-reset"
            : "dev-zero-balance",
      );
    } else {
      void this.sendTelegramSafe(
        (
          signal.kind === "close_account"
            ? [
                "<b>🧹 Dev Full-Exit Reset — Token Skipped</b>",
                `Token: <code>${mint}</code>`,
                `Dev wallet: <code>${this.devWallet}</code>`,
                `Close-account tx: <code>${signal.signature}</code>`,
                "Dev wallet closed its WSOL token account before we bought — treated as a full exit/rug signal.",
                "Flow reset — waiting for the next token.",
              ]
            : signal.kind === "zero_balance"
              ? [
                  "<b>🧹 Dev Zero-Balance Reset — Token Skipped</b>",
                  `Token: <code>${mint}</code>`,
                  `Dev wallet: <code>${this.devWallet}</code>`,
                  "Dev wallet native SOL reached zero before we bought — treated as a full exit/rug signal.",
                  "Flow reset — waiting for the next token.",
                ]
              : [
                  "<b>🧹 MC Rug Reset — Token Skipped</b>",
                  `Token: <code>${mint}</code>`,
                  `Current MC: <b>$${signal.currentMc.toLocaleString()}</b>`,
                  `Rug reset floor: <b>$${INSIDER_RUG_RESET_MARKET_CAP_USD.toLocaleString()}</b>`,
                  "Market cap dropped below the rug reset threshold before we bought.",
                  "Flow reset — waiting for the next token.",
                ]
        ).join("\n"),
        signal.kind === "close_account"
          ? "dev full-exit reset notification"
          : signal.kind === "mc_floor"
            ? "MC rug reset notification"
            : "dev zero-balance reset notification",
      );
      await this.resetForNewToken(true);
    }
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
    if (this.recipientLogsSubIds.has(wallet) || this.recipientEnhancedWatchIds.has(wallet)) return;
    if (this.enhancedWs) {
      const watchId = this.enhancedWs.watch(wallet, (tx) => {
        void this.applyFunderRecipientNotificationTx(wallet, tx);
      });
      this.recipientEnhancedWatchIds.set(wallet, watchId);
    } else {
      const subId = this.connection.onLogs(
        new PublicKey(wallet),
        (logInfo) => {
          if (!logInfo.err) {
            this.log.debug(
              "Funder recipient logs notification ignored — Enhanced WSS required for push detection",
              { wallet, signature: logInfo.signature },
            );
          }
        },
        "processed",
      );
      this.recipientLogsSubIds.set(wallet, subId);
    }
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
      pushDriven: this.recipientEnhancedWatchIds.has(wallet),
      solBalanceSubscription: this.recipientSolBalanceSubIds.has(wallet),
    });
  }

  /** Fed by a fresh Enhanced WSS `transactionSubscribe` notification for a watched recipient — already fully parsed, no REST fetch needed. Mirrors the per-tx filtering + body of syncFunderRecipientTransactions' loop. */
  private async applyFunderRecipientNotificationTx(
    wallet: string,
    tx: HeliusTransaction,
  ): Promise<void> {
    const state = this.bundlerFunderWatch;
    const watch = state?.recipientWatches.get(wallet);
    if (!state || !watch || this.positionSellTriggered) return;
    let seen = this.recipientEnhancedWatchSeenSignatures.get(wallet);
    if (!seen) {
      seen = new Set<string>();
      this.recipientEnhancedWatchSeenSignatures.set(wallet, seen);
    }
    if (seen.has(tx.signature)) return;
    seen.add(tx.signature);
    if (tx.signature === watch.fundingSignature) return;
    if (!watch.normalTinyTransferMode && tx.timestamp < watch.fundingTimestamp) return;
    try {
      await this.applyFunderRecipientTransaction(state, watch, tx, "notification");
    } catch (err) {
      this.log.warn("Failed to apply funder recipient Enhanced WSS notification", {
        mint: state.mint,
        wallet,
        signature: tx.signature,
        error: err instanceof Error ? err.message : String(err),
      });
    }
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
    this.recipientEnhancedWatchSeenSignatures.delete(wallet);
    const subId = this.recipientLogsSubIds.get(wallet);
    if (subId !== undefined) {
      this.recipientLogsSubIds.delete(wallet);
      void this.connection.removeOnLogsListener(subId).catch(() => undefined);
    }
    const watchId = this.recipientEnhancedWatchIds.get(wallet);
    if (watchId !== undefined) {
      this.recipientEnhancedWatchIds.delete(wallet);
      void this.enhancedWs?.unwatch(watchId).catch(() => undefined);
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
    await this.unsubscribeBundlerFunder();
    for (const [wallet, subId] of this.recipientLogsSubIds) {
      await this.connection.removeOnLogsListener(subId).catch(() => undefined);
      this.recipientLogsSubIds.delete(wallet);
    }
    for (const [wallet, watchId] of this.recipientEnhancedWatchIds) {
      await this.enhancedWs?.unwatch(watchId).catch(() => undefined);
      this.recipientEnhancedWatchIds.delete(wallet);
    }
    this.recipientEnhancedWatchSeenSignatures.clear();
    for (const [wallet, subId] of this.recipientSolBalanceSubIds) {
      await this.connection.removeAccountChangeListener(subId).catch(() => undefined);
      this.recipientSolBalanceSubIds.delete(wallet);
    }
    this.bundlerFunderWatch = null;
    this.isBundlerFunderSyncing = false;
    this.bundlerFunderSyncPending = false;
    this.bundlerFunderSyncPendingForce = false;
    this.isParallelFeePayerFunderSyncing = false;
    this.lastParallelFeePayerFunderSyncAt = 0;
    this.bundlerFunderPrimaryWatchAddress = null;
    this.bundlerFunderParallelWatchAddress = null;
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
        if (state.discoveryStopped || !this.bundlerFunderWatch) break;
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
    await this.unsubscribeBundlerFunder();
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
    watchedWallet: string = state.funderWallet,
  ): Promise<boolean> {
    const largeInsiderActive = !!this.followTokenLargeInsiderState?.active;
    if (state.discoveryStopped && !largeInsiderActive) return false;
    const isPrimaryWatch = watchedWallet === state.funderWallet;
    if (isPrimaryWatch) {
      this.recordLowFundingFunderTx(state, tx);
    }
    const transferOut = this.extractSolTransferOutFromWallet(
      tx,
      watchedWallet,
      0,
    );
    if (!transferOut) return false;

    if (
      largeInsiderActive &&
      isPrimaryWatch &&
      this.flowSource === "follow-token"
    ) {
      await this.handleFollowTokenLargeInsiderFeePayerTransferOut(
        state,
        tx,
        transferOut,
      );
    }

    if (transferOut.amountSol > BUNDLER_FUNDER_MAX_NORMAL_TRANSFER_OUT_SOL) {
      if (
        largeInsiderActive &&
        isPrimaryWatch &&
        this.flowSource === "follow-token"
      ) {
        if (this.isFollowTokenLargeInsiderFeePayerWindowOpen(tx.timestamp)) {
          const migrated = await this.maybeMoveBundlerFunderWatchAfterLargeDrain(
            state,
            tx,
            transferOut,
          );
          this.log.info(
            "Follow-token large insider feePayer over-max transfer-out",
            {
              mint: state.mint,
              funderWallet: watchedWallet,
              signature: tx.signature,
              amountSol: transferOut.amountSol,
              recipient: transferOut.to,
              migrated,
            },
          );
          return migrated;
        }
        return false;
      }
      if (!isPrimaryWatch) {
        this.log.debug("Skipping parallel feePayer-funder transfer-out above normal-mode max (no migration)", {
          mint: state.mint,
          parallelFunder: watchedWallet,
          sharedFeePayer: state.funderWallet,
          signature: tx.signature,
          amountSol: transferOut.amountSol,
          maxTransferOutSol: BUNDLER_FUNDER_MAX_NORMAL_TRANSFER_OUT_SOL,
          recipient: transferOut.to,
        });
        return false;
      }
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
      if (!isPrimaryWatch) return false;
      if (this.hasSolIncomingToWallet(tx, watchedWallet)) return false;
      let transferOutUsd: number | null = null;
      const solPriceUsd = await this.getCachedSolPriceUsd();
      transferOutUsd = solPriceUsd !== null ? transferOut.amountSol * solPriceUsd : null;
      if (transferOutUsd === null) {
        this.log.warn("Skipping tiny recipient check because SOL/USD is unavailable", {
          mint: state.mint,
          funderWallet: watchedWallet,
          signature: tx.signature,
          amountSol: transferOut.amountSol,
          lowFundingMode: state.lowFundingMode,
        });
        return false;
      }
      if (transferOutUsd >= BUNDLER_FUNDER_NORMAL_TINY_TRANSFER_OUT_MAX_USD) return false;
      await this.handleLowFundingTinyTransferOut(state, tx, transferOut, transferOutUsd);
      return false;
    }
    return this.handleNormalModeTinyTransferOut(state, tx, transferOut, watchedWallet);
  }

  private async handleNormalModeTinyTransferOut(
    state: BundlerFunderWatchState,
    tx: HeliusTransaction,
    transferOut: { to: string; amountSol: number },
    sourceWallet: string = state.funderWallet,
  ): Promise<boolean> {
    if (this.flowSource === "follow-token") {
      return false;
    }
    const isParallelSource = sourceWallet !== state.funderWallet;
    const solPriceUsd = await this.getCachedSolPriceUsd();
    const transferOutUsd =
      solPriceUsd !== null ? transferOut.amountSol * solPriceUsd : null;
    if (transferOutUsd === null) {
      this.log.warn("Skipping tiny recipient check because SOL/USD is unavailable", {
        mint: state.mint,
        funderWallet: sourceWallet,
        sharedFeePayer: state.funderWallet,
        parallelFunder: isParallelSource,
        signature: tx.signature,
        amountSol: transferOut.amountSol,
        lowFundingMode: state.lowFundingMode,
      });
      return false;
    }
    if (
      !isNormalTinyUsdExemptRoundSolAmount(transferOut.amountSol) &&
      transferOutUsd >= BUNDLER_FUNDER_NORMAL_TINY_TRANSFER_OUT_MAX_USD
    ) {
      this.log.debug("Skipping normal-mode feePayer transfer-out above tiny-recipient USD cap", {
        mint: state.mint,
        funderWallet: sourceWallet,
        sharedFeePayer: state.funderWallet,
        parallelFunder: isParallelSource,
        signature: tx.signature,
        amountSol: transferOut.amountSol,
        amountUsd: transferOutUsd,
        maxTinyTransferUsd: BUNDLER_FUNDER_NORMAL_TINY_TRANSFER_OUT_MAX_USD,
        recipient: transferOut.to,
      });
      return false;
    }
    if (this.hasSolIncomingToWallet(tx, sourceWallet)) {
      this.log.debug("Skipping funder transfer-out because same tx also has transfer-in", {
        mint: state.mint,
        funderWallet: sourceWallet,
        sharedFeePayer: state.funderWallet,
        parallelFunder: isParallelSource,
        signature: tx.signature,
        amountSol: transferOut.amountSol,
        recipient: transferOut.to,
      });
      return false;
    }
    if (state.bundlerWallets.has(transferOut.to)) {
      this.log.info("Skipping feePayer transfer-out because recipient is one of the first-four bundlers", {
        mint: state.mint,
        funderWallet: sourceWallet,
        sharedFeePayer: state.funderWallet,
        parallelFunder: isParallelSource,
        signature: tx.signature,
        amountSol: transferOut.amountSol,
        recipient: transferOut.to,
        bundlerWallets: [...state.bundlerWallets],
      });
      return false;
    }
    if (this.isKnownFunderCandidate(state, tx.signature)) return false;
    // Record every qualifying feePayer transfer-out for dust counting and round-group detection.
    // Sub-$0.10 outs are tracked as dust. Round buy wins unless cumulative dust reaches the buy threshold before a round 10s group does.
    this.recordNormalTinyTransferOut(state, {
      signature: tx.signature,
      timestamp: tx.timestamp,
      recipient: transferOut.to,
      amountSol: transferOut.amountSol,
      amountUsd: transferOutUsd,
    });

    const roundTarget =
      transferOutUsd < BUNDLER_FUNDER_NORMAL_TINY_DUST_FLOOR_USD
        ? null
        : this.getNormalTinyRoundTarget(transferOut.amountSol);

    if (!state.normalTinyRoundGroupFound) {
      if (roundTarget === null) {
        const cumulativeDustCount = this.countCumulativeNormalTinyDustTransferOuts(state);
        const dustSkipThreshold = normalTinyQualifyingDustGroupTxs();
        if (cumulativeDustCount >= dustSkipThreshold) {
          if (this.hasRoundGroupReachedBuyThresholdBy(state, tx.timestamp)) {
            if (!state.roundWonDustRaceNotified) {
              state.roundWonDustRaceNotified = true;
              void this.sendTelegramSafe(
                [
                  `<b>🏁 ${this.label} Round Group Won Race to ${BUNDLER_FUNDER_NORMAL_TINY_MIN_ROUND_GROUP_TXS_FOR_BUY}</b>`,
                  `Token: <code>${state.mint}</code>`,
                  this.formatFollowWalletTelegramLine(),
                  `FeePayer: <code>${state.funderWallet}</code>`,
                  `Cumulative dust txs: <b>${cumulativeDustCount}</b> (≥${dustSkipThreshold})`,
                  `A ${formatNormalTinyRoundSolLabel(BUNDLER_FUNDER_NORMAL_TINY_VALID_ROUND_SOL_AMOUNTS)} 10s group also reached <b>≥${BUNDLER_FUNDER_NORMAL_TINY_MIN_ROUND_GROUP_TXS_FOR_BUY}</b> txs first — not skipping for dust.`,
                  `Trigger tx: <code>${tx.signature}</code>`,
                  "",
                  "Continuing to round buy gates ($100 first-buy check).",
                ].filter(Boolean).join("\n"),
                "round won dust race notification",
              );
            }
            this.log.info(
              `Normal cumulative dust reached skip threshold but round 10s group hit ${BUNDLER_FUNDER_NORMAL_TINY_MIN_ROUND_GROUP_TXS_FOR_BUY}+ txs first — not skipping`,
              {
                mint: state.mint,
                funderWallet: state.funderWallet,
                signature: tx.signature,
                cumulativeDustTxCount: cumulativeDustCount,
                roundBuyThreshold: BUNDLER_FUNDER_NORMAL_TINY_MIN_ROUND_GROUP_TXS_FOR_BUY,
              },
            );
          } else {
            await this.skipTokenCumulativeDustThreshold(state, cumulativeDustCount, tx);
            return true;
          }
        } else {
          this.log.info(`Normal tiny cumulative dust waiting for ${BUNDLER_FUNDER_NORMAL_TINY_MIN_ROUND_GROUP_TXS_FOR_BUY}+ txs to skip token`, {
            mint: state.mint,
            funderWallet: state.funderWallet,
            signature: tx.signature,
            recipient: transferOut.to,
            amountSol: transferOut.amountSol,
            cumulativeDustTxCount: cumulativeDustCount,
            requiredTxCount: dustSkipThreshold,
          });
        }
        return false;
      }
    }

    if (roundTarget === null) {
      return false;
    }

    if (state.normalTinyRoundGroupFound) {
      return false;
    }

    const sameRoundGroup = this.getNormalTinySameRoundGroup(
      state,
      tx.timestamp,
      roundTarget,
    );
    if (sameRoundGroup.length < BUNDLER_FUNDER_NORMAL_TINY_MIN_ROUND_GROUP_TXS_FOR_BUY) {
      if (sameRoundGroup.length >= BUNDLER_FUNDER_NORMAL_TINY_MIN_SOL_GROUP_TXS) {
        this.log.info(`Normal tiny same-round group waiting for ${BUNDLER_FUNDER_NORMAL_TINY_MIN_ROUND_GROUP_TXS_FOR_BUY}+ txs in 10s window`, {
          mint: state.mint,
          funderWallet: state.funderWallet,
          signature: tx.signature,
          recipient: transferOut.to,
          amountSol: transferOut.amountSol,
          roundTargetSol: roundTarget,
          currentWindowTxCount: sameRoundGroup.length,
          requiredTxCount: BUNDLER_FUNDER_NORMAL_TINY_MIN_ROUND_GROUP_TXS_FOR_BUY,
          groupWindowSeconds: BUNDLER_FUNDER_LOW_FUNDING_TINY_GROUP_SECONDS,
        });
      } else {
        this.log.info(`Normal tiny transfer waiting for same-round 10s group (${formatNormalTinyRoundSolLabel(BUNDLER_FUNDER_NORMAL_TINY_VALID_ROUND_SOL_AMOUNTS)})`, {
          mint: state.mint,
          funderWallet: state.funderWallet,
          signature: tx.signature,
          recipient: transferOut.to,
          amountSol: transferOut.amountSol,
          amountUsd: transferOutUsd,
          roundTargetSol: roundTarget,
          groupWindowSeconds: BUNDLER_FUNDER_LOW_FUNDING_TINY_GROUP_SECONDS,
          minSolGroupTxs: BUNDLER_FUNDER_NORMAL_TINY_MIN_SOL_GROUP_TXS,
        });
      }
      return false;
    }

    if (this.hasPriorCumulativeDustSkipThresholdBefore(state, tx.timestamp)) {
      const priorDustCount = this.countCumulativeNormalTinyDustTransferOuts(
        state,
        tx.timestamp,
      );
      await this.skipTokenCumulativeDustBeforeRoundBuy(
        state,
        roundTarget,
        tx,
        priorDustCount,
        sameRoundGroup.length,
      );
      return true;
    }

    const exitPercent = this.getNormalTinyExitPercent(roundTarget);
    const selectedGroup = this.selectFirstUniqueRoundGroupRecipients(
      sameRoundGroup,
      BUNDLER_FUNDER_MAX_RECIPIENT_WATCHES,
    );
    if (selectedGroup.length < BUNDLER_FUNDER_MAX_RECIPIENT_WATCHES) {
      this.log.info("Normal tiny round group waiting for two unique recipients", {
        mint: state.mint,
        funderWallet: state.funderWallet,
        signature: tx.signature,
        uniqueRecipientCount: selectedGroup.length,
        requiredRecipientCount: BUNDLER_FUNDER_MAX_RECIPIENT_WATCHES,
        sameRoundGroupCount: sameRoundGroup.length,
      });
      return false;
    }
    state.normalTinyRoundGroupFound = true;

    const recipientFirstBuyGate =
      await this.evaluateRoundGroupRecipientFirstBuyUsdGate(state, selectedGroup);
    if (!recipientFirstBuyGate.passed) {
      await this.skipTokenRoundGroupRecipientFirstBuyUsd(
        state,
        roundTarget,
        tx,
        recipientFirstBuyGate,
      );
      return true;
    }

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
    watch ??=
      (recipientFirstBuyGate.qualifyingWallet
        ? state.recipientWatches.get(recipientFirstBuyGate.qualifyingWallet)
        : null) ??
      state.recipientWatches.get(selectedGroup[0]?.recipient ?? "") ??
      null;
    if (!watch) return false;
    void this.syncFunderRecipientBatch(true);

    this.log.warn("Normal-mode shared feePayer round SOL group accepted for immediate buy", {
      mint: state.mint,
      funderWallet: state.funderWallet,
      amountUsd: transferOutUsd,
      amountSol: transferOut.amountSol,
      roundTargetSol: roundTarget,
      exitPercent,
      qualifyingRecipient: recipientFirstBuyGate.qualifyingWallet,
      signature: tx.signature,
      selectedRecipients: selectedGroup.map((entry) => entry.recipient),
      selectedSignatures: selectedGroup.map((entry) => entry.signature),
      sameRoundGroupCount: sameRoundGroup.length,
    });

    void this.sendTelegramSafe(
      [
        `<b>🟢 ${this.label} Normal FeePayer Round SOL Group Buy Gate</b>`,
        `Token: <code>${state.mint}</code>`,
        this.formatFollowWalletTelegramLine(),
        `FeePayer: <code>${state.funderWallet}</code>`,
        `Round size: <b>~${roundTarget} SOL</b> (±${BUNDLER_FUNDER_NORMAL_TINY_ROUND_SOL_TOLERANCE_SOL})`,
        `Group: <b>${selectedGroup.length}/${BUNDLER_FUNDER_MAX_RECIPIENT_WATCHES}</b> selected · <b>${sameRoundGroup.length}</b> ~${roundTarget} SOL txs in ${BUNDLER_FUNDER_LOW_FUNDING_TINY_GROUP_SECONDS}s (first valid group, ≥${BUNDLER_FUNDER_NORMAL_TINY_MIN_ROUND_GROUP_TXS_FOR_BUY})`,
        recipientFirstBuyGate.qualifyingWallet
          ? `Buy triggered by: <code>${recipientFirstBuyGate.qualifyingWallet}</code> (first buy &gt; $${BUNDLER_FUNDER_ROUND_GROUP_RECIPIENT_FIRST_BUY_MIN_USD})`
          : "",
        `Selected exit: <b>+${exitPercent}% MC</b>`,
        ...selectedGroup.map((entry, index) => `${index + 1}. <code>${entry.recipient}</code> — ${entry.amountSol.toFixed(4)} SOL — <code>${entry.signature}</code>`),
      ].filter(Boolean).join("\n"),
      "normal feePayer round sol group buy gate notification",
    );

    await this.emitBundlerFunderBuy(
      state,
      watch,
      tx.signature,
      `normal-mode shared feePayer ~${roundTarget} SOL round group accepted with +${exitPercent}% MC exit`,
      false,
      tx,
      exitPercent,
    );
    return false;
  }

  private async maybeHandoffEmptyBundlerFunderAtStartupChain(
    state: BundlerFunderWatchState,
  ): Promise<number> {
    let migrations = 0;
    for (let attempt = 0; attempt < BUNDLER_FUNDER_STARTUP_HANDOFF_MAX_CHAIN; attempt += 1) {
      const active = this.bundlerFunderWatch;
      if (!active || active.mint !== state.mint) break;
      const migrated = await this.maybeHandoffEmptyBundlerFunderAtStartup(active);
      if (!migrated) break;
      migrations += 1;
    }
    return migrations;
  }

  private async getBundlerFunderLiveBalanceSol(
    wallet: string,
  ): Promise<number | null> {
    try {
      const lamports = await this.connection.getBalance(new PublicKey(wallet));
      return lamports / LAMPORTS_PER_SOL;
    } catch (err) {
      this.log.warn("Failed to fetch shared feePayer live SOL balance", {
        wallet,
        error: err instanceof Error ? err.message : String(err),
      });
      return null;
    }
  }

  private async isFunderWalletDrainedAfterTx(
    wallet: string,
    tx: HeliusTransaction,
  ): Promise<boolean> {
    if (!Number.isFinite(tx.timestamp) || tx.timestamp <= 0) return false;
    try {
      let balance = await this.getConfirmedWalletBalanceAt(
        wallet,
        NATIVE_SOL_BALANCE_MINT,
        tx.timestamp,
      );
      let balanceRaw = BigInt(balance.balanceRaw || "0");
      if (balanceRaw !== 0n) {
        const nextSecondBalance = await this.getConfirmedWalletBalanceAt(
          wallet,
          NATIVE_SOL_BALANCE_MINT,
          tx.timestamp + 1,
        );
        balanceRaw = BigInt(nextSecondBalance.balanceRaw || "0");
      }
      return balanceRaw === 0n;
    } catch (err) {
      void this.heliusClient.handlePossibleRateLimitError(err);
      this.log.warn("Failed to confirm shared feePayer zero balance after transfer-out", {
        wallet,
        signature: tx.signature,
        timestamp: tx.timestamp,
        error: err instanceof Error ? err.message : String(err),
      });
      return false;
    }
  }

  private async maybeHandoffEmptyBundlerFunderAtStartup(
    state: BundlerFunderWatchState,
  ): Promise<boolean> {
    const wallet = state.funderWallet;
    const liveBalanceSol = await this.getBundlerFunderLiveBalanceSol(wallet);
    if (liveBalanceSol === null) {
      return false;
    }
    if (liveBalanceSol > ZERO_BALANCE_EPSILON_SOL) {
      this.log.debug("Startup shared feePayer handoff not needed — wallet still holds SOL", {
        mint: state.mint,
        funderWallet: wallet,
        liveBalanceSol,
      });
      return false;
    }

    this.log.warn(
      "Shared feePayer live balance is zero at lock — searching for latest large drain to hand off",
      {
        mint: state.mint,
        funderWallet: wallet,
        originalFunderWallet: state.originalFunderWallet,
        earliestFundingTimestamp: state.earliestFundingTimestamp,
      },
    );

    let txs: HeliusTransaction[];
    try {
      txs = await this.withHeliusFallback((client) =>
        client.getWalletTransactionsDesc(
          wallet,
          BUNDLER_FUNDER_STARTUP_HANDOFF_HISTORY_LIMIT,
        ),
      );
    } catch (err) {
      void this.heliusClient.handlePossibleRateLimitError(err);
      this.log.warn("Startup shared feePayer handoff failed — could not load recent txs", {
        mint: state.mint,
        funderWallet: wallet,
        error: err instanceof Error ? err.message : String(err),
      });
      return false;
    }

    for (const tx of txs) {
      if (
        Number.isFinite(state.earliestFundingTimestamp) &&
        tx.timestamp < state.earliestFundingTimestamp
      ) {
        continue;
      }
      const transferOut = this.extractSolTransferOutFromWallet(tx, wallet, 0);
      if (
        !transferOut ||
        transferOut.amountSol <= BUNDLER_FUNDER_MAX_NORMAL_TRANSFER_OUT_SOL
      ) {
        continue;
      }
      if (!transferOut.to || transferOut.to === wallet) continue;
      if (transferOut.to === state.originalFunderWallet) {
        this.log.info(
          "Startup shared feePayer handoff skipped — latest large drain returned to original feePayer",
          {
            mint: state.mint,
            funderWallet: wallet,
            originalFunderWallet: state.originalFunderWallet,
            signature: tx.signature,
            amountSol: transferOut.amountSol,
            recipient: transferOut.to,
          },
        );
        return false;
      }

      const drained = await this.isFunderWalletDrainedAfterTx(wallet, tx);
      if (!drained) continue;

      const recipientBalanceSol = await this.getBundlerFunderLiveBalanceSol(
        transferOut.to,
      );
      if (
        recipientBalanceSol === null ||
        recipientBalanceSol <= ZERO_BALANCE_EPSILON_SOL
      ) {
        this.log.info(
          "Startup shared feePayer handoff skipped — large-drain recipient is also at zero",
          {
            mint: state.mint,
            funderWallet: wallet,
            recipient: transferOut.to,
            signature: tx.signature,
            amountSol: transferOut.amountSol,
          },
        );
        continue;
      }

      state.processedSignatures.add(tx.signature);
      await this.switchBundlerFunderWatchAddress(
        state,
        transferOut.to,
        tx.signature,
        `Shared feePayer already at zero SOL at lock; handed off to latest ≥${BUNDLER_FUNDER_MAX_NORMAL_TRANSFER_OUT_SOL} SOL drain recipient.`,
      );
      return true;
    }

    this.log.warn(
      "Shared feePayer at zero at lock but no qualifying large-drain handoff found",
      {
        mint: state.mint,
        funderWallet: wallet,
        scannedTxs: txs.length,
        minDrainSol: BUNDLER_FUNDER_MAX_NORMAL_TRANSFER_OUT_SOL,
      },
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
    if (
      this.followTokenLargeInsiderState?.active &&
      this.flowSource === "follow-token" &&
      this.isFollowTokenLargeInsiderSharedFeePayer(transferOut.to)
    ) {
      this.log.info(
        "Follow-token large insider feePayer handoff skipped — drain returned to original shared feePayer",
        {
          mint: state.mint,
          watchedWallet: state.funderWallet,
          originalFunderWallet: state.originalFunderWallet,
          signature: tx.signature,
          amountSol: transferOut.amountSol,
          recipient: transferOut.to,
        },
      );
      return false;
    }
    try {
      const drained = await this.isFunderWalletDrainedAfterTx(
        state.funderWallet,
        tx,
      );
      this.log.info("Checked shared feePayer balance after over-max transfer-out", {
        mint: state.mint,
        watchedWallet: state.funderWallet,
        recipient: transferOut.to,
        signature: tx.signature,
        timestamp: tx.timestamp,
        amountSol: transferOut.amountSol,
        drained,
      });
      if (!drained) return false;

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

  private selectFirstUniqueRoundGroupRecipients<
    T extends { recipient: string },
  >(group: T[], maxCount: number): T[] {
    const selected: T[] = [];
    const seen = new Set<string>();
    for (const entry of group) {
      if (seen.has(entry.recipient)) continue;
      seen.add(entry.recipient);
      selected.push(entry);
      if (selected.length >= maxCount) break;
    }
    return selected;
  }

  private async findWalletFirstTokenBuy(
    wallet: string,
    mint: string,
    preferredClientIndex = 0,
  ): Promise<HeliusTransaction | null> {
    const txs = await this.withHeliusFallback(
      (client) =>
        client.getWalletTransactionsDesc(
          wallet,
          BUNDLER_FUNDER_ROUND_GROUP_RECIPIENT_HISTORY_LIMIT,
        ),
      preferredClientIndex,
    );
    const buys = txs
      .filter((entry) => this.isRelevantMintTx(entry, mint))
      .filter((entry) => this.classifyTx(entry, wallet, mint) === "buy")
      .sort((a, b) => a.timestamp - b.timestamp || a.slot - b.slot);
    return buys[0] ?? null;
  }

  private async estimateRecipientBuyUsd(
    tx: HeliusTransaction,
    wallet: string,
  ): Promise<number | null> {
    const buySol = this.estimateRecipientBuySolSpent(tx, wallet);
    const solPriceUsd = await this.getCachedSolPriceUsd();
    if (buySol === null || solPriceUsd === null) return null;
    return buySol * solPriceUsd;
  }

  private async evaluateRoundGroupRecipientFirstBuyUsdGate(
    state: BundlerFunderWatchState,
    selectedGroup: Array<{
      signature: string;
      timestamp: number;
      recipient: string;
      amountSol: number;
      amountUsd: number;
    }>,
  ): Promise<{
    passed: boolean;
    qualifyingWallet: string | null;
    recipients: Array<{
      wallet: string;
      firstBuySignature: string | null;
      firstBuyUsd: number | null;
    }>;
  }> {
    const recipients: Array<{
      wallet: string;
      firstBuySignature: string | null;
      firstBuyUsd: number | null;
    }> = [];
    let qualifyingWallet: string | null = null;

    for (const entry of selectedGroup) {
      const firstBuy = await this.findWalletFirstTokenBuy(
        entry.recipient,
        state.mint,
      );
      const firstBuyUsd = firstBuy
        ? await this.estimateRecipientBuyUsd(firstBuy, entry.recipient)
        : null;
      recipients.push({
        wallet: entry.recipient,
        firstBuySignature: firstBuy?.signature ?? null,
        firstBuyUsd,
      });
      if (
        qualifyingWallet === null &&
        firstBuyUsd !== null &&
        firstBuyUsd > BUNDLER_FUNDER_ROUND_GROUP_RECIPIENT_FIRST_BUY_MIN_USD
      ) {
        qualifyingWallet = entry.recipient;
      }
    }

    const passed = qualifyingWallet !== null;

    this.log.warn("Checked round-group selected recipients first-buy USD gate", {
      mint: state.mint,
      funderWallet: state.funderWallet,
      requiredMinUsd: BUNDLER_FUNDER_ROUND_GROUP_RECIPIENT_FIRST_BUY_MIN_USD,
      passed,
      qualifyingWallet,
      recipients,
    });

    return { passed, qualifyingWallet, recipients };
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

  private isNormalTinyValidRoundSolAmount(amountSol: number): boolean {
    return this.getNormalTinyRoundTarget(amountSol) !== null;
  }

  private getNormalTinyRoundTarget(amountSol: number): number | null {
    for (const target of BUNDLER_FUNDER_NORMAL_TINY_VALID_ROUND_SOL_AMOUNTS) {
      if (this.isNearBundlerTinySolAmount(amountSol, target)) {
        return target;
      }
    }
    return null;
  }

  private getNormalTinyExitPercent(roundTargetSol: number): number {
    if (roundTargetSol >= BUNDLER_FUNDER_NORMAL_TINY_HIGH_EXIT_MIN_ROUND_SOL) {
      return BUNDLER_FUNDER_NORMAL_TINY_HIGH_EXIT_PERCENT;
    }
    return BUNDLER_FUNDER_NORMAL_TINY_MID_EXIT_PERCENT;
  }

  private isNormalTinyDustTransferOut(
    state: BundlerFunderWatchState,
    entry: {
      signature: string;
      amountSol: number;
      amountUsd: number;
      recipient: string;
    },
  ): boolean {
    if (state.bundlerWallets.has(entry.recipient)) return false;
    if (this.isKnownFunderCandidate(state, entry.signature)) return false;
    if (entry.amountUsd < BUNDLER_FUNDER_NORMAL_TINY_DUST_FLOOR_USD) return true;
    return !this.isNormalTinyValidRoundSolAmount(entry.amountSol);
  }

  private countCumulativeNormalTinyDustTransferOuts(
    state: BundlerFunderWatchState,
    beforeTimestamp?: number,
  ): number {
    return state.normalTinyTransferOuts.filter(
      (entry) =>
        this.isNormalTinyDustTransferOut(state, entry) &&
        (beforeTimestamp === undefined || entry.timestamp < beforeTimestamp),
    ).length;
  }

  /** True when cumulative dust already reached the skip threshold before `beforeTimestamp`. */
  private hasPriorCumulativeDustSkipThresholdBefore(
    state: BundlerFunderWatchState,
    beforeTimestamp: number,
  ): boolean {
    return (
      this.countCumulativeNormalTinyDustTransferOuts(state, beforeTimestamp) >=
      normalTinyQualifyingDustGroupTxs()
    );
  }

  /** True when any round SOL 10s group already reached the round buy threshold by `byTimestamp`. */
  private hasRoundGroupReachedBuyThresholdBy(
    state: BundlerFunderWatchState,
    byTimestamp: number,
  ): boolean {
    const entries = state.normalTinyTransferOuts.filter(
      (entry) =>
        entry.timestamp <= byTimestamp &&
        !state.bundlerWallets.has(entry.recipient) &&
        !this.isKnownFunderCandidate(state, entry.signature) &&
        entry.amountUsd >= BUNDLER_FUNDER_NORMAL_TINY_DUST_FLOOR_USD,
    );
    for (const entry of entries) {
      const roundTarget = this.getNormalTinyRoundTarget(entry.amountSol);
      if (roundTarget === null) continue;
      if (
        this.getNormalTinySameRoundGroup(state, entry.timestamp, roundTarget).length >=
        BUNDLER_FUNDER_NORMAL_TINY_MIN_ROUND_GROUP_TXS_FOR_BUY
      ) {
        return true;
      }
    }
    return false;
  }

  private async skipTokenCumulativeDustThreshold(
    state: BundlerFunderWatchState,
    cumulativeDustTxCount: number,
    tx: HeliusTransaction,
  ): Promise<void> {
    if (state.discoveryStopped) return;
    await this.stopBundlerFunderSourceDiscovery(
      state,
      "cumulative dust threshold reached before round buy",
    );
    this.log.warn("Skipping token — cumulative dust transfer-outs reached skip threshold before round buy", {
      mint: state.mint,
      funderWallet: state.funderWallet,
      cumulativeDustTxCount,
      signature: tx.signature,
    });
    void this.sendTelegramSafe(
      [
        `<b>⏭️ ${this.label} Token Skipped — Cumulative Dust Threshold</b>`,
        `Token: <code>${state.mint}</code>`,
        this.formatFollowWalletTelegramLine(),
        `FeePayer: <code>${state.funderWallet}</code>`,
        `Cumulative dust txs (not ${formatNormalTinyRoundSolLabel(BUNDLER_FUNDER_NORMAL_TINY_VALID_ROUND_SOL_AMOUNTS)}): <b>${cumulativeDustTxCount}</b> (≥${normalTinyQualifyingDustGroupTxs()})`,
        `Trigger tx: <code>${tx.signature}</code>`,
        "",
        `Cumulative dust reached ≥${normalTinyQualifyingDustGroupTxs()} before any round 10s group did — token skipped; feePayer watch will resume.`,
        `Round group must reach ≥${BUNDLER_FUNDER_NORMAL_TINY_MIN_ROUND_GROUP_TXS_FOR_BUY} txs in 10s before cumulative dust hits ${normalTinyQualifyingDustGroupTxs()} to buy.`,
      ].filter(Boolean).join("\n"),
      "cumulative dust threshold skip notification",
    );
    await this.resetForNewToken(true);
  }

  private async skipTokenCumulativeDustBeforeRoundBuy(
    state: BundlerFunderWatchState,
    roundTargetSol: number,
    tx: HeliusTransaction,
    priorDustCount: number,
    sameRoundGroupCount: number,
  ): Promise<void> {
    if (state.discoveryStopped) return;
    await this.stopBundlerFunderSourceDiscovery(
      state,
      "cumulative dust reached threshold before round buy",
    );
    this.log.warn("Skipping token — cumulative dust reached skip threshold before round 10s buy group", {
      mint: state.mint,
      funderWallet: state.funderWallet,
      roundTargetSol,
      priorDustCount,
      sameRoundGroupCount,
      signature: tx.signature,
    });
    void this.sendTelegramSafe(
      [
        `<b>⏭️ ${this.label} Token Skipped — Cumulative Dust Before Round Buy</b>`,
        `Token: <code>${state.mint}</code>`,
        this.formatFollowWalletTelegramLine(),
        `FeePayer: <code>${state.funderWallet}</code>`,
        `Cumulative dust txs (sub-$0.10 / non-round): <b>${priorDustCount}</b> (≥${normalTinyQualifyingDustGroupTxs()}) reached before this round group.`,
        `Saw ~${roundTargetSol} SOL 10s group with <b>${sameRoundGroupCount}</b> txs (≥${BUNDLER_FUNDER_NORMAL_TINY_MIN_ROUND_GROUP_TXS_FOR_BUY}) but dust hit ${normalTinyQualifyingDustGroupTxs()} first — token skipped.`,
        `Trigger tx: <code>${tx.signature}</code>`,
      ].filter(Boolean).join("\n"),
      "cumulative dust before round buy skip notification",
    );
    await this.resetForNewToken(true);
  }

  private async skipTokenRoundGroupRecipientFirstBuyUsd(
    state: BundlerFunderWatchState,
    roundTargetSol: number,
    tx: HeliusTransaction,
    gate: {
      passed: boolean;
      recipients: Array<{
        wallet: string;
        firstBuySignature: string | null;
        firstBuyUsd: number | null;
      }>;
    },
  ): Promise<void> {
    if (state.discoveryStopped) return;
    await this.stopBundlerFunderSourceDiscovery(
      state,
      "round group recipient first buy below USD threshold",
    );
    this.log.warn(
      "Skipping token — no selected round-group recipient first buy above USD threshold",
      {
        mint: state.mint,
        funderWallet: state.funderWallet,
        roundTargetSol,
        requiredMinUsd: BUNDLER_FUNDER_ROUND_GROUP_RECIPIENT_FIRST_BUY_MIN_USD,
        recipients: gate.recipients,
        signature: tx.signature,
      },
    );
    void this.sendTelegramSafe(
      [
        `<b>⏭️ ${this.label} Token Skipped — Round Group Recipient First Buy Too Small</b>`,
        `Token: <code>${state.mint}</code>`,
        this.formatFollowWalletTelegramLine(),
        `FeePayer: <code>${state.funderWallet}</code>`,
        `Saw ~${roundTargetSol} SOL 10s group (≥${BUNDLER_FUNDER_NORMAL_TINY_MIN_ROUND_GROUP_TXS_FOR_BUY} txs) but neither of the first two unique recipients has a first buy on this token above <b>$${BUNDLER_FUNDER_ROUND_GROUP_RECIPIENT_FIRST_BUY_MIN_USD}</b>.`,
        `Trigger tx: <code>${tx.signature}</code>`,
        "",
        ...gate.recipients.map((entry, index) => {
          const buyUsdLabel =
            entry.firstBuyUsd === null
              ? "unknown"
              : `$${entry.firstBuyUsd.toFixed(2)}`;
          return `${index + 1}. <code>${entry.wallet}</code> — first buy: ${
            entry.firstBuySignature
              ? `<code>${entry.firstBuySignature}</code>`
              : "none found"
          } — <b>${buyUsdLabel}</b>`;
        }),
      ].filter(Boolean).join("\n"),
      "round group recipient first buy usd skip notification",
    );
    await this.resetForNewToken(true);
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

  private getNormalTinySameRoundGroup(
    state: BundlerFunderWatchState,
    timestamp: number,
    roundTargetSol: number,
  ): Array<{ signature: string; timestamp: number; recipient: string; amountSol: number; amountUsd: number }> {
    const start = timestamp - BUNDLER_FUNDER_LOW_FUNDING_TINY_GROUP_SECONDS;
    const group = state.normalTinyTransferOuts.filter(
      (entry) =>
        entry.timestamp >= start &&
        entry.timestamp <= timestamp &&
        !state.bundlerWallets.has(entry.recipient) &&
        !this.isKnownFunderCandidate(state, entry.signature) &&
        this.isNearBundlerTinySolAmount(entry.amountSol, roundTargetSol),
    );
    if (group.length < 2) return [];
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
      this.isBuyGateEvaluating ||
      this.isBuyBlockedByDevTokenOut(state.mint)
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
        await this.resetForNewToken(true, {
          reason: `below_rug_threshold_${INSIDER_RUG_MARKET_CAP_USD}`,
        });
        return;
      }

      if (this.isBuyBlockedByDevTokenOut(state.mint)) {
        return;
      }
      if (
        !(await this.ensureDevBuyCountAllowsBuy(state.mint, {
          signature,
          triggerLabel: "low_funding_shared_feepayer",
        }))
      ) {
        return;
      }

      this.setEntryMc(currentMc);
      this.setBuyExecuting(true);
      this.buySubmitted = true;
      this.preBuyStopped = true;
      this.armDevTokenOutPostBuyWatch(state.mint);
      this.emit("buyTrigger", {
        followedWallet: this.getFlowFollowWallet()!,
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
      this.isBuyGateEvaluating ||
      this.isBuyBlockedByDevTokenOut(state.mint)
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
        await this.resetForNewToken(true, {
          reason: `below_rug_threshold_${INSIDER_RUG_MARKET_CAP_USD}`,
        });
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
      const exitPercent =
        exitOptions.exitPercent ??
        (exitOptions.fixedExitMc === undefined ? this.exitPercent : undefined);
      if (this.isBuyBlockedByDevTokenOut(state.mint)) {
        return;
      }
      if (
        !(await this.ensureDevBuyCountAllowsBuy(state.mint, {
          signature,
          triggerLabel: "low_funding_recipient",
        }))
      ) {
        return;
      }
      if (exitMc !== null) this.setExitMc(exitMc);
      this.setEntryMc(currentMc);
      this.setBuyExecuting(true);
      this.buySubmitted = true;
      this.preBuyStopped = true;
      this.armDevTokenOutPostBuyWatch(state.mint);
      this.disableProfitExitAfterBuy = exitOptions.disableProfitExit ?? true;
      this.emit("buyTrigger", {
        followedWallet: this.getFlowFollowWallet()!,
        mint: state.mint,
        signature,
        buySol: this.getBuySolForFundingMode(state.lowFundingMode),
        entryMc: currentMc,
        profitExitPercent: exitPercent,
        fixedExitMc: exitOptions.fixedExitMc,
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
      this.isBuyGateEvaluating ||
      this.isBuyBlockedByDevTokenOut(state.mint)
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
        await this.resetForNewToken(true, {
          reason: `below_rug_threshold_${INSIDER_RUG_MARKET_CAP_USD}`,
        });
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
      if (this.isBuyBlockedByDevTokenOut(state.mint)) {
        return;
      }
      if (
        !(await this.ensureDevBuyCountAllowsBuy(state.mint, {
          signature,
          triggerLabel: "bundler_funder_recipient",
        }))
      ) {
        return;
      }
      this.setExitMc(newExitMc);
      this.setEntryMc(currentMc);
      this.setBuyExecuting(true);
      this.buySubmitted = true;
      this.preBuyStopped = true;
      this.armDevTokenOutPostBuyWatch(state.mint);
      this.disableProfitExitAfterBuy = watch.normalTinyTransferMode
        ? false
        : disableProfitExitAfterBuy;
      this.emit("buyTrigger", {
        followedWallet: this.getFlowFollowWallet()!,
        mint: state.mint,
        signature,
        buySol: this.getBuySolForFundingMode(state.lowFundingMode),
        entryMc: currentMc,
        profitExitPercent: exitPercent,
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
    context: "insider" | "bundler" | "early_bundler_exit",
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
    context: "insider" | "bundler" | "early_bundler_exit",
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
        } else if (context === "early_bundler_exit" && bundlerWallet) {
          await this.applyFollowTokenEarlyBundlerExitTx(tx, mint, bundlerWallet);
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
    this.clearDevTokenOutPostBuyWatchTimer();
    this.devTokenOutWatchUntilMs = null;
    await this.stopPreBuyMonitoring();
    await this.stopInsiderMonitoring();
    await this.stopBundlerMonitoring();
    await this.stopFollowTokenEarlyBundlerExitMonitoring();
    await this.stopBundlerFunderMonitoring();
    await this.stopDevWalletFullExitWatch("flow monitoring stopped");
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
      `Bundler match threshold reached (${this.bundlerMatchTypeLabel(matchType)}) — bundler match scan complete`,
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
      followedWallet: this.getBuyTriggerFollowedWallet(this.bundlerFunderWatch),
      positionMint: mint,
      signature,
      reason,
    });
  }

  private async maybeAutoResumeFollowWalletMonitoring(source: string): Promise<void> {
    if (this.followWalletPaused) {
      this.followWalletBackend("Follow-wallet auto-resume skipped — user paused", {
        bot: this.label,
        source,
        followWallets: this.followedWallets,
      });
      return;
    }
    if (this.followedWallets.length === 0 || this.followMonitors.size > 0) {
      return;
    }
    this.log.info("InsiderBot resuming followed wallet monitoring", { source });
    this.followWalletBackend("Follow-wallet flow ended; resuming wallet monitoring", {
      bot: this.label,
      source,
      followWallets: this.followedWallets,
    });
    await this.followAllWallets();
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
    const endedMint = this.watchingMint ?? this.activePosition?.mint ?? null;
    const endedFeePayer =
      this.funderFirstFeePayer ?? this.bundlerFunderWatch?.funderWallet ?? null;
    const endedSource = this.flowSource;
    const hadPosition = !!this.activePosition;

    if (this.claimedMint) {
      this.releaseMint?.(this.claimedMint);
      this.claimedMint = null;
    }

    await this.stopBundlerMonitoring();
    await this.stopBundlerFunderMonitoring();
    await this.stopDevWalletFullExitWatch("flow cycle completed");
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
    this.devFullExitHandled = false;
    this.devTokenOutHandled = false;
    this.fastMigrationSellMode = null;
    this.fastMigrationTransferOutMc = null;
    this.clearDevTokenOutPostBuyWatchTimer();
    this.devTokenOutWatchUntilMs = null;
    this.devFullExitSeenSignatures.clear();
    this.highestObservedMarketCapUsd = null;
    this.preBuyStopped = false;
    this.insiderSellsReady = false;
    this.bundlerMatchesReady = false;
    this.phase = this.activePosition ? "holding" : null;

    if (!this.activePosition) {
      this.stopPollLoop();
      this.resetTokenTxCounts();
    }

    this.emit("tokenFlowEnded", {
      mint: endedMint,
      feePayer: endedFeePayer,
      source: endedSource,
      hadPosition,
      reason: "cycle_complete",
    });
    this.flowSource = null;
    this.flowFollowWallet = null;
    this.funderFirstFeePayer = null;

    if (this.followedWallets.length > 0 && this.followMonitors.size === 0) {
      await this.maybeAutoResumeFollowWalletMonitoring("cycle_complete");
    }
  }

  private async resetForNewToken(
    clearPosition: boolean,
    options?: { reason?: string; skipTelegram?: boolean },
  ): Promise<void> {
    const endedMint = this.watchingMint ?? this.activePosition?.mint ?? null;
    const endedFeePayer =
      this.funderFirstFeePayer ?? this.bundlerFunderWatch?.funderWallet ?? null;
    const endedSource = this.flowSource;
    const hadPosition = clearPosition && !!this.activePosition;
    const resetReason = options?.reason ?? "flow_reset";
    const liNearMisses =
      endedSource === "follow-token" &&
      !this.buySubmitted &&
      !this.activePosition &&
      (this.followTokenLargeInsiderState?.firstBuyBelowMinUsdWallets.size ?? 0) > 0
        ? [
            ...this.followTokenLargeInsiderState!.firstBuyBelowMinUsdWallets.entries(),
          ]
        : null;

    if (!options?.skipTelegram && liNearMisses?.length && endedMint) {
      await this.sendFollowTokenLargeInsiderFirstBuyUsdNearMissTelegram(
        endedMint,
        liNearMisses,
      );
    }

    if (!options?.skipTelegram && endedMint) {
      const flowLabel =
        endedSource === "follow-token"
          ? "Follow-Token"
          : endedSource === "funder-first"
            ? "Funder-First"
            : endedSource === "follow"
              ? "Follow-Wallet"
              : (endedSource ?? "Insider");
      await this.sendTelegramSafe(
        [
          `<b>🔄 ${this.label} ${flowLabel} Reset</b>`,
          `Token: <code>${endedMint}</code>`,
          `Reason: <code>${resetReason}</code>`,
          hadPosition ? "Had open position: cleared." : "No position held.",
          "",
          "Flow reset — resuming PumpPortal / idle monitoring.",
        ].join("\n"),
        "token flow reset notification",
      );
    }

    if (this.claimedMint) {
      this.releaseMint?.(this.claimedMint);
      this.claimedMint = null;
    }

    await this.stopFlowMonitoring();
    await this.stopFollowTokenTopBuyerWatch("token flow reset");
    await this.stopFollowTokenLargeInsiderFlow("token flow reset");
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
    this.devFullExitHandled = false;
    this.devTokenOutHandled = false;
    this.clearDevTokenOutPostBuyWatchTimer();
    this.devTokenOutWatchUntilMs = null;
    this.devFullExitSeenSignatures.clear();
    this.highestObservedMarketCapUsd = null;
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

    if (endedSource === "funder-first" && !hadPosition && endedMint) {
      this.boughtMints.delete(endedMint);
    }

    this.emit("tokenFlowEnded", {
      mint: endedMint,
      feePayer: endedFeePayer,
      source: endedSource,
      hadPosition,
      reason: "reset",
    });
    this.followTokenEarlyInsiderBuys = null;
    this.followTokenEarlyBundlerExitState = null;
    this.followTokenLargeInsiderState = null;
    this.flowSource = null;
    this.flowFollowWallet = null;
    this.funderFirstFeePayer = null;

    if (this.followedWallets.length > 0 && this.followMonitors.size === 0) {
      await this.maybeAutoResumeFollowWalletMonitoring("reset");
    }
  }
}
