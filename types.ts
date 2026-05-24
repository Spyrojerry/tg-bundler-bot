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
  buySol: number | null;
}

export interface WalletFilterSettings {
  applyAtSample: number;
  minBundlersPercent: number | null;
  maxBundlersPercent: number | null;
  minBundlersCount: number | null;
  maxBundlersCount: number | null;
  minBundlersPercentIncrease: number | null;
  maxBundlersPercentIncrease: number | null;
  maxPctAboveValue: number | null;
  maxPctAboveOccurrences: number | null;
  maxPctBelowValue: number | null;
  maxPctBelowOccurrences: number | null;
  sellIfFirstThreePctZero: boolean;
  sellIfNoTeenOrTwentyPct: boolean;
}

/** Summary emitted after a token's monitoring window expires */
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
}

/** Config pulled from environment variables (all validated at startup) */
export interface ServiceConfig {
  walletAddress: string | null;
  solanaRpcUrl: string;
  solanaWsUrl: string;
  walletPollInterval: number;
  minBuySol: number;
  gmgnApiKey: string;
  gmgnApiBaseUrl: string;
  gmgnFetchMode: 'auto' | 'direct' | 'cli';
  monitorInterval: number;
  monitoringWindowMs: number;  // how long to monitor each token before summarising
  rateLimitMinTime: number;
  rateLimitMaxConcurrent: number;
  dbPath: string;
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
}

export interface MonitorSampleEvent {
  walletAddress: string;
  mint: string;
  elapsedSec: number;
  metrics: BundlerMetrics;
  sampleNumber: number;
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
}

export interface SellOptions {
  percent: number;
  slippage: number;
  autoSlippage: boolean;
  priorityFeeSol: number;
  antiMev: boolean;
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

/** Result of a single GMGN fetch attempt */
export type FetchResult =
  | { success: true;  metrics: BundlerMetrics }
  | {
      success: false;
      error: string;
      retryAfterMs?: number;
      nonRetryable?: boolean;
    };
