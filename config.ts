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

function heliusRpcUrl(apiKey: string): string {
  return `https://mainnet.helius-rpc.com/?api-key=${apiKey}`;
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
  const gmgnApiKey2      = optional('GMGN_API_KEY_2', gmgnApiKey);
  const gmgnApiKey3      = optional('GMGN_API_KEY_3', gmgnApiKey2);
  const gmgnApiKey4      = optional('GMGN_API_KEY_4', gmgnApiKey3);
  const gmgnFallbackApiKey = optionalNullable('GMGN_FALLBACK_API_KEY');
  const solanaRpcUrl     = optional('SOLANA_RPC_URL', 'https://api.mainnet-beta.solana.com');
  const solanaWsUrl      = optional('SOLANA_WS_URL', deriveWsUrl(solanaRpcUrl));
  const gmgnApiBaseUrl   = optional('GMGN_API_BASE_URL', 'https://gmgn.ai');
  const gmgnFetchMode    = optional('GMGN_FETCH_MODE', 'cli');
  const jupiterSwapBaseUrl = optional('JUPITER_SWAP_BASE_URL', 'https://api.jup.ag/swap/v2');
  const jupiterApiKey    = required('JUPITER_API_KEY');
  const jupiterPriceApiKey = optional('JUPITER_PRICE_API_KEY', jupiterApiKey);
  const pumpPortalApiKey = optionalNullable('PUMPPORTAL_API_KEY');
  const pumpPortalWalletAddress = optionalNullable('PUMPPORTAL_WALLET_ADDRESS');
  const heliusApiKey     = optional('HELIUS_API_KEY', '');
  const receiverHeliusApiKey = optional('RECEIVER_HELIUS_API_KEY', '');
  const receiverSolanaRpcUrl = optional(
    'RECEIVER_SOLANA_RPC_URL',
    receiverHeliusApiKey ? heliusRpcUrl(receiverHeliusApiKey) : solanaRpcUrl
  );
  const receiverSolanaWsUrl = optional(
    'RECEIVER_SOLANA_WS_URL',
    deriveWsUrl(receiverSolanaRpcUrl)
  );
  const f1HeliusApiKey = optional('F1_HELIUS_API_KEY', '');
  const f1SolanaRpcUrl = optional(
    'F1_SOLANA_RPC_URL',
    f1HeliusApiKey ? heliusRpcUrl(f1HeliusApiKey) : solanaRpcUrl
  );
  const f1SolanaWsUrl = optional(
    'F1_SOLANA_WS_URL',
    deriveWsUrl(f1SolanaRpcUrl)
  );
  const insiderHeliusApiKey = optional('INSIDER_HELIUS_API_KEY', '');
  const insiderHeliusApiKey2 = optional('INSIDER_HELIUS_API_KEY_2', '');
  const insiderHeliusApiKey3 = optional('INSIDER_HELIUS_API_KEY_3', '');
  const insiderHeliusApiKey4 = optional('INSIDER_HELIUS_API_KEY_4', '');
  const insiderHeliusProjectId = optional('INSIDER_HELIUS_PROJECT_ID', '');
  const insiderHeliusProjectId2 = optional('INSIDER_HELIUS_PROJECT_ID_2', '');
  const insiderHeliusProjectId3 = optional('INSIDER_HELIUS_PROJECT_ID_3', '');
  const insiderHeliusProjectId4 = optional('INSIDER_HELIUS_PROJECT_ID_4', '');
  const insiderSolanaRpcUrl = optional(
    'INSIDER_SOLANA_RPC_URL',
    insiderHeliusApiKey ? heliusRpcUrl(insiderHeliusApiKey) : solanaRpcUrl
  );
  const insiderSolanaWsUrl = optional(
    'INSIDER_SOLANA_WS_URL',
    deriveWsUrl(insiderSolanaRpcUrl)
  );
  const insiderSolanaRpcUrl2 = optional(
    'INSIDER_SOLANA_RPC_URL_2',
    insiderHeliusApiKey2 ? heliusRpcUrl(insiderHeliusApiKey2) : solanaRpcUrl
  );
  const insiderSolanaWsUrl2 = optional(
    'INSIDER_SOLANA_WS_URL_2',
    deriveWsUrl(insiderSolanaRpcUrl2)
  );
  const insiderSolanaRpcUrl3 = optional(
    'INSIDER_SOLANA_RPC_URL_3',
    insiderHeliusApiKey3 ? heliusRpcUrl(insiderHeliusApiKey3) : solanaRpcUrl
  );
  const insiderSolanaWsUrl3 = optional(
    'INSIDER_SOLANA_WS_URL_3',
    deriveWsUrl(insiderSolanaRpcUrl3)
  );
  const insiderSolanaRpcUrl4 = optional(
    'INSIDER_SOLANA_RPC_URL_4',
    insiderHeliusApiKey4 ? heliusRpcUrl(insiderHeliusApiKey4) : solanaRpcUrl
  );
  const insiderSolanaWsUrl4 = optional(
    'INSIDER_SOLANA_WS_URL_4',
    deriveWsUrl(insiderSolanaRpcUrl4)
  );
  const dbPath           = optional('DB_PATH', './data/monitor.db');
  const insiderFollowWallet = optionalNullable('INSIDER_FOLLOW_WALLET');
  const insiderFollowWallet2 = optionalNullable('INSIDER_FOLLOW_WALLET_2');
  const insiderFollowWallet3 = optionalNullable('INSIDER_FOLLOW_WALLET_3');
  const insiderFollowWallet4 = optionalNullable('INSIDER_FOLLOW_WALLET_4');
  const insiderFeePayerFunderAddress = optionalNullable(
    'INSIDER_FEEPAYER_FUNDER_ADDRESS',
  );
  const rawLogLevel      = optional('LOG_LEVEL', 'info');
  const telegramBotToken = optionalNullable('TELEGRAM_BOT_TOKEN');
  const telegramChatId   = optionalNullable('TELEGRAM_CHAT_ID');
  const sellAutoSlippage = optionalBoolean('SELL_AUTO_SLIPPAGE', false);
  const sellAntiMev      = optionalBoolean('SELL_ANTI_MEV', true);

  const minBuySol              = optionalNumber('MIN_BUY_SOL', 8);
  const monitorInterval        = optionalInt('MONITOR_INTERVAL', 2_000);
  const monitoringWindowMs     = optionalInt('MONITOR_WINDOW_MS', 60_000);
  const rateLimitMinTime       = optionalInt('RATE_LIMIT_MIN_TIME', 500);
  const rateLimitMaxConcurrent = optionalInt('RATE_LIMIT_MAX_CONCURRENT', 1);
  const sellPercent            = optionalNumber('SELL_PERCENT', 100);
  const sellSlippage           = optionalNumber('SELL_SLIPPAGE', 50);
  const sellPriorityFeeSol     = optionalNumber('SELL_PRIORITY_FEE_SOL', 0.000012);
  const insiderBuySol          = optionalNumber('INSIDER_BUY_SOL', 0.01);
  const insiderNormalBuySol    = optionalNumber('INSIDER_NORMAL_BUY_SOL', insiderBuySol);
  const insiderLowFundingBuySol = optionalNumber('INSIDER_LOW_FUNDING_BUY_SOL', insiderBuySol);
  const insiderEntryMc         = optionalNumber('INSIDER_ENTRY_MC', 15_000);
  const insiderExitMc          = optionalNumber('INSIDER_EXIT_MC', 30_000);
  const insiderExitPercent     = optionalNumber('INSIDER_EXIT_PERCENT', 40);
  const insiderMinTransferProfit = optionalNumber('INSIDER_MIN_TRANSFER_PROFIT', 70);
  const insiderBundlerBuyMinUsd = optionalNumber('INSIDER_BUNDLER_BUY_MIN_USD', 100);
  const insiderBundlerBuyMaxUsd = optionalNumber('INSIDER_BUNDLER_BUY_MAX_USD', 200);
  const insiderRequiredSells = optionalInt('INSIDER_REQUIRED_SELLS', 5);
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
  if (sellSlippage < 0 || sellSlippage > 100) {
    throw new Error('SELL_SLIPPAGE must be between 0 and 100');
  }
  return {
    walletAddress,
    tradingWalletAddress,
    solanaRpcUrl,
    solanaWsUrl,
    minBuySol,
    gmgnApiKey,
    gmgnApiKey2,
    gmgnApiKey3,
    gmgnApiKey4,
    gmgnFallbackApiKey,
    gmgnApiBaseUrl,
    gmgnFetchMode,
    jupiterSwapBaseUrl,
    jupiterApiKey,
    jupiterPriceApiKey,
    pumpPortalApiKey,
    pumpPortalWalletAddress,
    monitorInterval,
    monitoringWindowMs,
    rateLimitMinTime,
    rateLimitMaxConcurrent,
    dbPath,
    heliusApiKey,
    receiverHeliusApiKey,
    receiverSolanaRpcUrl,
    receiverSolanaWsUrl,
    f1HeliusApiKey,
    f1SolanaRpcUrl,
    f1SolanaWsUrl,
    insiderHeliusApiKey,
    insiderHeliusApiKey2,
    insiderHeliusApiKey3,
    insiderHeliusApiKey4,
    insiderHeliusProjectId,
    insiderHeliusProjectId2,
    insiderHeliusProjectId3,
    insiderHeliusProjectId4,
    insiderSolanaRpcUrl,
    insiderSolanaWsUrl,
    insiderSolanaRpcUrl2,
    insiderSolanaWsUrl2,
    insiderSolanaRpcUrl3,
    insiderSolanaWsUrl3,
    insiderSolanaRpcUrl4,
    insiderSolanaWsUrl4,
    insiderBuySol,
    insiderNormalBuySol,
    insiderLowFundingBuySol,
    insiderEntryMc,
    insiderExitMc,
    insiderExitPercent,
    insiderMinTransferProfit,
    insiderBundlerBuyMinUsd,
    insiderBundlerBuyMaxUsd,
    insiderRequiredSells,
    insiderFollowWallet,
    insiderFollowWallet2,
    insiderFollowWallet3,
    insiderFollowWallet4,
    insiderFeePayerFunderAddress,
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
