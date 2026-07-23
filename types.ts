// ─────────────────────────────────────────────────────────────────────────────
//  types.ts  —  Shared TypeScript types for the GMGN monitor service
// ─────────────────────────────────────────────────────────────────────────────

/** Status of a token in our monitoring pipeline */
export type MonitoringStatus = 'active' | 'paused' | 'stopped';

/** A token we are (or were) monitoring */
export interface TrackedToken {
  walletAddress: string;
  mint: string;
  firstSeen: string;          // ISO-8601 UTC
  monitoringStatus: MonitoringStatus;
  detectedAt: number;         // performance.now() epoch for priority scoring
  buySol: number | null;      // estimated original SOL spent, when known
}

/** One sample of GMGN bundler metrics */
export interface BundlerMetrics {
  id?: number;                // assigned by SQLite on insert
  walletAddress?: string;
  mint: string;
  timestamp: string;          // ISO-8601 UTC
  bundlersPercent: number | null;   // bundler_trader_amount_rate × 100
  bundlersCount: number | null;     // bundle_num / bundler_count
  initialBaseReserve: number | null;
  topWallets: number | null;
  top10HolderRate: number | null;   // top_10_holder_rate × 100
  bundledAmountRate: number | null; // raw rate (0-1) from API
  // Any extra fields the API returns — stored as JSON string in DB
  rawData?: string;
}

/** Raw token-security response shape from GMGN OpenAPI */
export interface GmgnSecurityResponse {
  code?: number;
  msg?: string;
  data?: {
    // Known bundler fields — names may vary; we handle both spellings
    bundler_trader_amount_rate?: number;   // fraction 0–1
    bundle_num?: number;                   // count of bundler wallets
    bundler_count?: number;                // alternative field name
    bundled_amount_rate?: number;          // alternative rate field
    // Other security fields (we capture but don't require them)
    [key: string]: unknown;
  };
}

/** Wallet token holding (from @solana/web3.js parsed account) */
export interface TokenHolding {
  mint: string;
  amount: string;    // raw token amount as string
  decimals: number;
  uiAmount: number | null;
}

/** Scheduler tick entry */
export interface SchedulerEntry {
  walletAddress: string;
  mint: string;
  lastFetchedAt: number;       // ms timestamp of last completed fetch
  pendingRequest: boolean;     // guard against duplicate in-flight requests
  priority: number;            // lower = higher priority (ms since detection)
  monitoringStartedAt: number; // ms timestamp when this token was added
  filterAlerted: boolean;
  filterPassed: boolean;
  buySol: number | null;
  matchingWallets: string[];
}

export interface WalletFilterProfileSettings {
  applyAtSample: number;
  minBundlersPercent: number | null;
  maxBundlersPercent: number | null;
  minBundlersCount: number | null;
  maxBundlersCount: number | null;
  maxPctAboveValue: number | null;
  maxPctAboveOccurrences: number | null;
  maxPctBelowValue: number | null;
  maxPctBelowOccurrences: number | null;
  sellIfFirstThreePctZero: boolean;
  sellIfNoTeenOrTwentyPct: boolean;
  sellIfNoPctAbove50: boolean;
}

export interface WalletFilterSettings extends WalletFilterProfileSettings {
  minBundlersCountChange: number | null;
  reverseBuySellTriggerEnabled: boolean;
  minSolBuy: number | null;
}

/** Summary emitted after a token reaches its apply-sample decision */
export interface TokenSummary {
  walletAddress: string;
  mint: string;
  windowMs: number;
  totalSamples: number;
  firstSeen: string;          // ISO-8601 UTC
  lastSeen: string;           // ISO-8601 UTC
  bundlersPercent: {
    first:   number | null;
    last:    number | null;
    min:     number | null;
    max:     number | null;
  };
  bundlersCount: {
    first:   number | null;
    last:    number | null;
    min:     number | null;
    max:     number | null;
  };
  initialBaseReserve: number | null;
  topWallets: {
    first: number | null;
    last: number | null;
    min: number | null;
    max: number | null;
  };
  top10HolderRate: {
    first: number | null;
    last: number | null;
    min: number | null;
    max: number | null;
  };
}

/** Config pulled from environment variables (all validated at startup) */
export interface ServiceConfig {
  walletAddress: string | null;
  tradingWalletAddress: string | null;
  solanaRpcUrl: string;
  solanaWsUrl: string;
  minBuySol: number;
  gmgnApiKey: string;
  gmgnApiKey2: string;
  gmgnApiKey3: string;
  gmgnApiKey4: string;
  gmgnFallbackApiKey: string | null;
  gmgnApiBaseUrl: string;
  gmgnFetchMode: 'auto' | 'direct' | 'cli';
  jupiterSwapBaseUrl: string;
  jupiterApiKey: string;
  jupiterPriceApiKey: string;
  pumpPortalApiKey: string | null;
  pumpPortalWalletAddress: string | null;
  monitorInterval: number;
  monitoringWindowMs: number;  // legacy env setting; linked tokens use apply-sample decisions
  rateLimitMinTime: number;
  rateLimitMaxConcurrent: number;
  dbPath: string;
  heliusApiKey: string;
  receiverHeliusApiKey: string;
  receiverSolanaRpcUrl: string;
  receiverSolanaWsUrl: string;
  f1HeliusApiKey: string;
  f1SolanaRpcUrl: string;
  f1SolanaWsUrl: string;
  insiderHeliusApiKey: string;
  insiderHeliusApiKey2: string;
  insiderHeliusApiKey3: string;
  insiderHeliusApiKey4: string;
  insiderHeliusProjectId: string;
  insiderHeliusProjectId2: string;
  insiderHeliusProjectId3: string;
  insiderHeliusProjectId4: string;
  insiderSolanaRpcUrl: string;
  insiderSolanaWsUrl: string;
  insiderSolanaRpcUrl2: string;
  insiderSolanaWsUrl2: string;
  insiderSolanaRpcUrl3: string;
  insiderSolanaWsUrl3: string;
  insiderSolanaRpcUrl4: string;
  insiderSolanaWsUrl4: string;
  insiderBuySol: number;
  insiderNormalBuySol: number;
  insiderLowFundingBuySol: number;
  insiderEntryMc: number;
  insiderExitMc: number;
  insiderExitPercent: number;
  insiderMinTransferProfit: number;
  insiderBundlerBuyMinUsd: number;
  insiderBundlerBuyMaxUsd: number;
  insiderRequiredSells: number;
  insiderFollowWallet: string | null;
  insiderFollowWallet2: string | null;
  insiderFollowWallet3: string | null;
  insiderFollowWallet4: string | null;
  /** Top-level feePayer funder wallet for the parallel funder-first discovery flow. */
  insiderFeePayerFunderAddress: string | null;
  /** Auto-start follow-wallet monitoring on boot when true. */
  insiderFollowWalletEnabled: boolean;
  /** Emit [FOLLOW-WALLET] backend info logs (wallet txs, buys, flow lifecycle) while testing. */
  insiderFollowWalletVerboseLogs: boolean;
  /** Auto-start funder-first on boot when true (still requires funder address). */
  insiderFunderFirstEnabled: boolean;
  /** Enable follow-token Pump.fun migration listener on startup (still requires Start in Telegram unless auto-resumed). */
  insiderFollowTokenEnabled: boolean;
  /** Log every PumpPortal migration + filter outcome at [FOLLOW-TOKEN] info while testing. */
  insiderFollowTokenVerboseLogs: boolean;
  /** Max seconds between token CREATE and migrate for follow-token filter (default 60). */
  insiderFollowTokenMaxMigrationAgeSec: number;
  logLevel: 'debug' | 'info' | 'warn' | 'error';
  telegramBotToken: string | null;
  telegramChatId: string | null;
  sellPercent: number;
  sellSlippage: number;
  sellAutoSlippage: boolean;
  sellPriorityFeeSol: number;
  sellAntiMev: boolean;
  port: number;
}

/** Internal event types emitted between modules */
export interface NewTokenEvent {
  walletAddress: string;
  mint: string;
  detectedAt: number;
  buySol: number | null;
  signature?: string;
  timestamp?: number;
  matchingWallets?: string[];
}

export interface MonitorSampleEvent {
  walletAddress: string;
  mint: string;
  elapsedSec: number;
  metrics: BundlerMetrics;
  sampleNumber: number;
  matchingWallets: string[];
}

export interface FilterFailEvent {
  walletAddress: string;
  mint: string;
  sampleNumber: number;
  elapsedSec: number;
  reasons: string[];
  settings: WalletFilterSettings;
  metrics: BundlerMetrics;
  buySol: number | null;
  matchingWallets: string[];
  entryMc?: number | null;
  sellMc?: number | null;
  insiderBotIndex?: number;
}

export interface FilterPassEvent {
  walletAddress: string;
  mint: string;
  sampleNumber: number;
  elapsedSec: number;
  settings: WalletFilterSettings;
  metrics: BundlerMetrics;
  buySol: number | null;
  matchingWallets: string[];
}

export interface SellOptions {
  percent: number;
  slippage: number;
  autoSlippage: boolean;
  priorityFeeSol: number;
  antiMev: boolean;
}

export interface BuyOptions {
  solAmount: number;
  slippage: number;
  autoSlippage: boolean;
  priorityFeeSol: number;
}

export interface SellResult {
  orderId: string | null;
  hash: string | null;
  status: string;
  inputToken: string;
  outputToken: string;
  soldPercent: number;
  filledInputAmount: string | null;
  filledOutputAmount: string | null;
  raw: Record<string, unknown>;
}

export interface SellQuote {
  inputToken: string;
  outputToken: string;
  soldPercent: number;
  inputAmount: string;
  outputAmount: string;
  estimatedOutputSol: number;
  raw: Record<string, unknown>;
}

/** Result of a single GMGN fetch attempt */
export type FetchResult =
  | { success: true;  metrics: BundlerMetrics; raw?: any }
  | {
      success: false;
      error: string;
      retryAfterMs?: number;
      nonRetryable?: boolean;
    };

// ─────────────────────────────────────────────────────────────────────────────
//  Early Bundler Bot Types
// ─────────────────────────────────────────────────────────────────────────────

/** Represents an early bundler wallet that bought into a token */
export interface EarlyBundlerWallet {
  id?: number;
  walletAddress: string;
  initialTokenAmount: number;
  signature: string;
  slot: number;
  timestamp: number;
}

/** Represents a trading position being monitored for early bundler activity */
export interface EarlyBundlerPosition {
  id?: number;
  tradingWallet: string;
  mint: string;
  tokenAmount: number;
  buySol: number | null;
  status: 'active' | 'exited';
  createdAt: string;
  bundlerWallets: EarlyBundlerWallet[];
}

/** Event emitted when a bundler wallet performs a transaction */
export interface BundlerTransactionEvent {
  bundlerWalletId: number;
  walletAddress: string;
  mint: string;
  signature: string;
  tokenAmount: number;
  slot: number;
  timestamp: number;
  type: 'buy' | 'sell';
}

/** Sell trigger reason for early bundler bot */
export type BundlerSellReason =
  | { type: 'bundler_buy'; walletAddress: string }
  | { type: 'bundler_sell_40pct'; walletAddress: string; soldPercentage: number }
  | { type: 'position_closed'; reason: string };
