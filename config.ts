// ─────────────────────────────────────────────────────────────────────────────
//  config.ts  —  Load and validate all environment variables at startup
// ─────────────────────────────────────────────────────────────────────────────

import { ServiceConfig } from './types';

function required(key: string): string {
  const val = process.env[key];
  if (!val || val.trim() === '') {
    throw new Error(`Missing required environment variable: ${key}`);
  }
  return val.trim();
}

function optional(key: string, defaultVal: string): string {
  return process.env[key]?.trim() || defaultVal;
}

function deriveWsUrl(httpUrl: string): string {
  if (httpUrl.startsWith('https://')) {
    return `wss://${httpUrl.slice('https://'.length)}`;
  }
  if (httpUrl.startsWith('http://')) {
    return `ws://${httpUrl.slice('http://'.length)}`;
  }
  return httpUrl;
}

function optionalInt(key: string, defaultVal: number): number {
  const raw = process.env[key]?.trim();
  if (!raw) return defaultVal;
  const n = parseInt(raw, 10);
  if (isNaN(n) || n <= 0) {
    throw new Error(`${key} must be a positive integer, got: ${raw}`);
  }
  return n;
}

function optionalNumber(key: string, defaultVal: number): number {
  const raw = process.env[key]?.trim();
  if (!raw) return defaultVal;
  const n = Number(raw);
  if (!Number.isFinite(n) || n < 0) {
    throw new Error(`${key} must be a non-negative number, got: ${raw}`);
  }
  return n;
}

function optionalBoolean(key: string, defaultVal: boolean): boolean {
  const raw = process.env[key]?.trim().toLowerCase();
  if (!raw) return defaultVal;
  if (['1', 'true', 'yes', 'on'].includes(raw)) return true;
  if (['0', 'false', 'no', 'off'].includes(raw)) return false;
  throw new Error(`${key} must be true or false, got: ${raw}`);
}

function optionalNullable(key: string): string | null {
  const val = process.env[key]?.trim();
  return val ? val : null;
}

function isValidLogLevel(s: string): s is ServiceConfig['logLevel'] {
  return ['debug', 'info', 'warn', 'error'].includes(s);
}

function isValidGmgnFetchMode(s: string): s is ServiceConfig['gmgnFetchMode'] {
  return ['auto', 'direct', 'cli'].includes(s);
}

export function loadConfig(): ServiceConfig {
  const walletAddress    = optionalNullable('WALLET_ADDRESS');
  const tradingWalletAddress = optionalNullable('TRADING_WALLET_ADDRESS');
  const gmgnApiKey       = required('GMGN_API_KEY');
  const solanaRpcUrl     = optional('SOLANA_RPC_URL', 'https://api.mainnet-beta.solana.com');
  const solanaWsUrl      = optional('SOLANA_WS_URL', deriveWsUrl(solanaRpcUrl));
  const gmgnApiBaseUrl   = optional('GMGN_API_BASE_URL', 'https://gmgn.ai');
  const gmgnFetchMode    = optional('GMGN_FETCH_MODE', 'cli');
  const jupiterSwapBaseUrl = optional('JUPITER_SWAP_BASE_URL', 'https://api.jup.ag/swap/v2');
  const jupiterApiKey    = required('JUPITER_API_KEY');
  const dbPath           = optional('DB_PATH', './data/monitor.db');
  const rawLogLevel      = optional('LOG_LEVEL', 'info');
  const telegramBotToken = optionalNullable('TELEGRAM_BOT_TOKEN');
  const telegramChatId   = optionalNullable('TELEGRAM_CHAT_ID');
  const sellAutoSlippage = optionalBoolean('SELL_AUTO_SLIPPAGE', true);
  const sellAntiMev      = optionalBoolean('SELL_ANTI_MEV', true);

  const walletPollInterval     = optionalInt('WALLET_POLL_INTERVAL', 5_000);
  const minBuySol              = optionalNumber('MIN_BUY_SOL', 8);
  const monitorInterval        = optionalInt('MONITOR_INTERVAL', 2_000);
  const monitoringWindowMs     = optionalInt('MONITOR_WINDOW_MS', 60_000);
  const rateLimitMinTime       = optionalInt('RATE_LIMIT_MIN_TIME', 500);
  const rateLimitMaxConcurrent = optionalInt('RATE_LIMIT_MAX_CONCURRENT', 1);
  const sellPercent            = optionalNumber('SELL_PERCENT', 100);
  const sellSlippage           = optionalNumber('SELL_SLIPPAGE', 0.3);
  const sellPriorityFeeSol     = optionalNumber('SELL_PRIORITY_FEE_SOL', 0.000012);
  const port                   = optionalInt('PORT', 8080);

  if (!isValidLogLevel(rawLogLevel)) {
    throw new Error(`LOG_LEVEL must be one of debug|info|warn|error, got: ${rawLogLevel}`);
  }
  if (!isValidGmgnFetchMode(gmgnFetchMode)) {
    throw new Error(`GMGN_FETCH_MODE must be one of auto|direct|cli, got: ${gmgnFetchMode}`);
  }

  // Sanity checks
  if (monitorInterval < 1_000) {
    throw new Error('MONITOR_INTERVAL must be at least 1000ms');
  }
  if (rateLimitMinTime < 100) {
    throw new Error('RATE_LIMIT_MIN_TIME must be at least 100ms (do not exceed API rate limits)');
  }
  if (monitoringWindowMs < monitorInterval) {
    throw new Error('MONITOR_WINDOW_MS must be >= MONITOR_INTERVAL');
  }
  if (sellPercent <= 0 || sellPercent > 100) {
    throw new Error('SELL_PERCENT must be greater than 0 and at most 100');
  }

  return {
    walletAddress,
    tradingWalletAddress,
    solanaRpcUrl,
    solanaWsUrl,
    walletPollInterval,
    minBuySol,
    gmgnApiKey,
    gmgnApiBaseUrl,
    gmgnFetchMode,
    jupiterSwapBaseUrl,
    jupiterApiKey,
    monitorInterval,
    monitoringWindowMs,
    rateLimitMinTime,
    rateLimitMaxConcurrent,
    dbPath,
    logLevel: rawLogLevel,
    telegramBotToken,
    telegramChatId,
    sellPercent,
    sellSlippage,
    sellAutoSlippage,
    sellPriorityFeeSol,
    sellAntiMev,
    port,
  };
}
