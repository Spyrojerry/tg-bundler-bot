// ─────────────────────────────────────────────────────────────────────────────
//  gmgn-client.ts  —  GMGN OpenAPI REST client
//
//  Authentication: GMGN_API_KEY sent as  X-API-KEY  header.
//
//  Rate limits: Not published. Community observation ~2 req/s.
//  We rely on RateLimiter for all throttling — this module just fires requests.
// ─────────────────────────────────────────────────────────────────────────────

import { createLogger } from './logger';
import { RateLimiter } from './rate-limiter';
import {
  ComputeBudgetProgram,
  Connection,
  Keypair,
  PublicKey,
  TransactionInstruction,
  TransactionMessage,
  VersionedTransaction,
  ParsedAccountData,
} from '@solana/web3.js';
import { exec } from 'child_process';
import { promisify } from 'util';
import { PUMP_SDK, OnlinePumpSdk, bondingCurvePda, getSellSolAmountFromTokenAmount } from '@pump-fun/pump-sdk';
import {
  createAssociatedTokenAccountIdempotentInstruction,
  createTransferInstruction,
  getAssociatedTokenAddressSync,
} from '@solana/spl-token';
import {
  BundlerMetrics,
  FetchResult,
  GmgnSecurityResponse,
  BuyOptions,
  SellOptions,
  SellQuote,
  SellResult,
  ServiceConfig,
} from './types';

const execAsync = promisify(exec);
const bs58 = require('bs58') as { decode(value: string): Buffer };
const BN = require('bn.js');

const log = createLogger('GMGN');

// ── Retry config ──────────────────────────────────────────────────────────────

const MAX_RETRIES      = 3;
const BASE_RETRY_MS    = 1_000;
const REQUEST_TIMEOUT  = 15_000;  // ms
const BLOCKED_RETRY_MS = 60_000;
const JUPITER_ORDER_RETRIES = 5;
const JUPITER_SELL_RETRIES = 5;
const SOL_MINT = 'So11111111111111111111111111111111111111112';
const TOKEN_PROGRAM_ID = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA';
const TOKEN_2022_PROGRAM_ID = 'TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb';
const TOKEN_PROGRAM_IDS = [
  new PublicKey(TOKEN_PROGRAM_ID),
  new PublicKey(TOKEN_2022_PROGRAM_ID),
];
const PUMP_PORTAL_LOCAL_URL = 'https://pumpportal.fun/api/trade-local';
const PUMP_PORTAL_POOLS = ['pump', 'auto'] as const;

// ── Helper: sleep ─────────────────────────────────────────────────────────────

const sleep = (ms: number): Promise<void> =>
  new Promise((r) => setTimeout(r, ms));

// ── GmgnClient ────────────────────────────────────────────────────────────────

export class GmgnClient {
  private readonly baseUrl: string;
  private readonly apiKey:  string;
  private readonly jupiterSwapBaseUrl: string;
  private readonly jupiterApiKey: string;
  private readonly jupiterPriceApiKey: string;
  private readonly connection: Connection;
  private readonly chain  = 'sol';
  private readonly limiter: RateLimiter;
  private readonly fallbackApiKey: string | null;
  private readonly fallbackLimiter: RateLimiter | null;
  private readonly baselineMinTime: number;
  private readonly fetchMode: 'auto' | 'direct' | 'cli';
  private readonly pumpSdk: OnlinePumpSdk;
  private readonly tradingKeypair: Keypair | null = null;

  constructor(
    config: ServiceConfig,
    limiter: RateLimiter,
    rpcUrlOverride?: string,
    fallbackApiKey?: string,
    fallbackLimiter?: RateLimiter,
  ) {
    this.baseUrl          = config.gmgnApiBaseUrl.replace(/\/$/, '');
    this.apiKey           = config.gmgnApiKey;
    this.jupiterSwapBaseUrl = config.jupiterSwapBaseUrl.replace(/\/$/, '');
    this.jupiterApiKey    = config.jupiterApiKey;
    this.jupiterPriceApiKey = config.jupiterPriceApiKey;
    this.connection       = new Connection(rpcUrlOverride || config.solanaRpcUrl, 'confirmed');
    this.pumpSdk          = new OnlinePumpSdk(this.connection);
    this.limiter          = limiter;
    this.fallbackApiKey =
      fallbackApiKey && fallbackApiKey !== this.apiKey ? fallbackApiKey : null;
    this.fallbackLimiter = this.fallbackApiKey
      ? fallbackLimiter ?? limiter
      : null;
    this.baselineMinTime  = config.rateLimitMinTime;
    this.fetchMode        = config.gmgnFetchMode;

    const jupPrivKey = process.env.JUPITER_PRIVATE_KEY;
    if (jupPrivKey) {
      try {
        if (jupPrivKey.startsWith('[')) {
          this.tradingKeypair = Keypair.fromSecretKey(Uint8Array.from(JSON.parse(jupPrivKey)));
        } else {
          this.tradingKeypair = Keypair.fromSecretKey(bs58.decode(jupPrivKey));
        }
        log.info(`Trading keypair loaded for ${this.tradingKeypair.publicKey.toBase58()}`);
      } catch (err) {
        log.error('Failed to load JUPITER_PRIVATE_KEY', err);
      }
    }
  }

  // ── Public: fetch Market Cap (Primary: GMGN API, Secondary: Jupiter + RPC) ──

  async fetchTokenMarketCapUsd(mint: string): Promise<number | null> {
    this.validateSolAddress(mint, 'mint');

    // 1. PRIMARY: GMGN (CLI or API)
    try {
      let data: Record<string, unknown> | null = null;
      if (this.fetchMode !== 'direct') {
        log.debug(`Attempting MC fetch via GMGN CLI: ${mint}`);
        // CLI calls are local and shouldn't be bottlenecked by API rate limits
        data = await this.fetchCliData('token', mint);
      }
      
      if (!data) {
        log.debug(`Attempting MC fetch via GMGN API: ${mint}`);
        data = await this.limiter.schedule(() => this.fetchRawTokenData('v1/token/info', mint));
      }

      if (data) {
        const marketCap = this.extractMarketCapUsd(data);
        if (marketCap !== null) {
          log.debug(`Calculated MC via GMGN ${data.source === 'cli' ? 'CLI' : 'API'} primary: $${marketCap.toLocaleString()}`, { mint });
          return marketCap;
        } else {
          log.debug(`Extracted MC was null from GMGN ${data.source === 'cli' ? 'CLI' : 'API'} data`, { mint });
        }
      }
    } catch (err) {
      log.debug(`GMGN primary MC fetch failed for ${mint}`, { error: String(err) });
    }

    // 2. SECONDARY: Jupiter Price API + RPC Supply
    try {
      log.info(`Using Jupiter + RPC secondary for MC: ${mint}`);
      
      // Fetch Supply from RPC
      const supplyResp = await this.connection.getTokenSupply(new PublicKey(mint), 'confirmed');
      const supply = supplyResp.value.uiAmount;
      if (supply === null) {
        log.warn(`RPC Supply was null for ${mint}`);
        return null;
      }

      // Fetch Price from Jupiter
      const url = `https://api.jup.ag/price/v3?ids=${mint}`;
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), 5000);

      const resp = await fetch(url, {
        method: 'GET',
        headers: { 
          'Accept': 'application/json',
          'x-api-key': this.jupiterPriceApiKey 
        },
        signal: controller.signal
      });
      clearTimeout(timer);

      if (resp.ok) {
        const json = await resp.json() as Record<string, any>;
        const priceData = json[mint];
        if (priceData && priceData.usdPrice) {
          const priceUsd = parseFloat(priceData.usdPrice);
          if (!isNaN(priceUsd)) {
            const marketCap = supply * priceUsd;
            log.info(`Calculated MC via Jupiter + RPC secondary: $${marketCap.toLocaleString()}`, {
              mint,
              supply,
              priceUsd
            });
            return marketCap;
          }
        } else {
          log.warn(`Jupiter Price API returned no data for ${mint}`);
        }
      } else {
        log.warn(`Jupiter Price API failed: ${resp.status} ${resp.statusText}`);
      }
    } catch (err) {
      log.error(`Both MC fetch methods failed for ${mint}`, err);
    }

    return null;
  }

  async fetchTokenAthMarketCapUsd(mint: string): Promise<number | null> {
    this.validateSolAddress(mint, 'mint');

    try {
      let data: Record<string, unknown> | null = null;
      if (this.fetchMode !== 'direct') {
        data = await this.fetchCliData('token', mint);
      }

      if (!data) {
        data = await this.limiter.schedule(() => this.fetchRawTokenData('v1/token/info', mint));
      }

      if (!data) return null;

      const athPrice =
        this.parseNullableNumber(data.ath_price) ??
        this.parseNullableNumber(this.asRecord(data.stat).ath_price) ??
        this.parseNullableNumber(this.asRecord(data.token).ath_price) ??
        this.parseNullableNumber(this.asRecord(data.price).ath_price);

      if (athPrice === null || athPrice <= 0) return null;

      const supply =
        this.parseNullableNumber(data.circulating_supply ?? data.total_supply) ??
        this.parseNullableNumber(
          this.asRecord(data.token).circulating_supply ??
            this.asRecord(data.token).total_supply,
        );

      if (supply === null) return null;

      const athMc = athPrice * supply;
      if (athMc > 0) {
        log.debug(`Calculated ATH MC: $${athMc.toLocaleString()}`, { mint, athPrice, supply });
        return athMc;
      }
    } catch (err) {
      log.debug(`ATH MC fetch failed for ${mint}`, { error: String(err) });
    }

    return null;
  }

  async fetchWalletTokenProfitUsd(
    walletAddress: string,
    mint: string,
  ): Promise<number | null> {
    const traders = await this.fetchTokenTraders(mint, 100, 'profit');
    if (!traders) return null;

    let list = traders.list;
    if (!Array.isArray(list)) {
      list =
        traders.traders ||
        traders.data?.list ||
        traders.data?.traders ||
        traders.items;
    }

    if (!Array.isArray(list)) return null;

    const trader = list.find((entry: { address?: string }) => entry.address === walletAddress);
    if (!trader) return null;

    const profit =
      this.parseNullableNumber(trader.profit) ??
      this.parseNullableNumber(trader.realized_profit) ??
      this.parseNullableNumber(trader.total_profit);

    return profit;
  }

  /**
   * Fetches current SOL price in USD using Jupiter Price API
   */
  async fetchSolPriceUsd(): Promise<number | null> {
    try {
      const solMint = 'So11111111111111111111111111111111111111112';
      const url = `https://api.jup.ag/price/v3?ids=${solMint}`;
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), 5000);

      const resp = await fetch(url, {
        method: 'GET',
        headers: {
          'Accept': 'application/json',
          'x-api-key': this.jupiterPriceApiKey
        },
        signal: controller.signal
      });
      clearTimeout(timer);

      if (resp.ok) {
        const json = await resp.json() as Record<string, any>;
        const priceData = json[solMint];
        if (priceData && priceData.usdPrice) {
          const priceUsd = parseFloat(priceData.usdPrice);
          if (!isNaN(priceUsd)) {
            log.debug(`Fetched SOL price: $${priceUsd.toFixed(2)}`);
            return priceUsd;
          }
        }
      }
      log.warn('Could not fetch SOL price from Jupiter');
      return null;
    } catch (err) {
      log.error('Failed to fetch SOL price', err);
      return null;
    }
  }

  async fetchCreatorHoldRate(mint: string): Promise<number | null> {
    this.validateSolAddress(mint, 'mint');

    try {
      let data: Record<string, unknown> | null = null;
      if (this.fetchMode !== 'direct') {
        data = await this.fetchCliData('token', mint);
      }

      if (!data) {
        data = await this.limiter.schedule(() => this.fetchRawTokenData('v1/token/info', mint));
      }

      if (!data) return null;

      const candidates = [
        data.creator_hold_rate,
        this.asRecord(data.stat).creator_hold_rate,
        this.asRecord(data.dev).creator_hold_rate,
        this.asRecord(data.token).creator_hold_rate,
      ];

      for (const candidate of candidates) {
        const parsed = this.parseNullableNumber(candidate);
        if (parsed !== null) return parsed;
      }

      return null;
    } catch (err) {
      log.warn(`Failed to fetch creator_hold_rate for ${mint}`, err);
      return null;
    }
  }

  // ── Public: fetch top traders by buy volume (no tag filter) ───────────────

  async fetchBuyVolumeTraders(
    mint: string,
    limit: number = 50,
  ): Promise<any> {
    this.validateSolAddress(mint, 'mint');

    try {
      if (this.fetchMode !== 'direct') {
        const data = await this.fetchCliData('traders', mint, {
          limit,
          orderBy: 'buy_volume_cur',
        });
        if (data && this.hasTraderEntries(data)) return data;
        if (data) {
          log.info(`GMGN CLI buy-volume traders returned no trader rows for ${mint}, falling back to API`, {
            shape: this.describeResponseShape(data),
          });
        } else {
          log.debug(`GMGN CLI buy-volume traders returned no data for ${mint}, falling back to API`);
        }
      }

      const endpoint =
        `v1/token/traders/sol/${mint}?limit=${limit}&orderby=buy_volume_cur&direction=desc`;
      const data = await this.limiter.schedule(() => this.fetchRawTokenData(endpoint, mint));
      return data;
    } catch (err) {
      log.error(`Failed to fetch buy-volume traders for ${mint}`, err);
      return null;
    }
  }

  async fetchBundlerTraders(
    mint: string,
    limit: number = 20,
  ): Promise<any> {
    this.validateSolAddress(mint, 'mint');

    try {
      if (this.fetchMode !== 'direct') {
        const data = await this.fetchCliData('traders', mint, {
          limit,
          orderBy: 'buy_volume_cur',
          tag: 'bundler',
        });
        if (data && this.hasTraderEntries(data)) return data;
        if (data) {
          log.info(`GMGN CLI bundler traders returned no trader rows for ${mint}, falling back to API`, {
            shape: this.describeResponseShape(data),
          });
        } else {
          log.debug(`GMGN CLI bundler traders returned no data for ${mint}, falling back to API`);
        }
      }

      const endpoint =
        `v1/token/traders/sol/${mint}?limit=${limit}&tag=bundler&orderby=buy_volume_cur&direction=desc`;
      const data = await this.limiter.schedule(() => this.fetchRawTokenData(endpoint, mint));
      return data;
    } catch (err) {
      log.error(`Failed to fetch bundler traders for ${mint}`, err);
      return null;
    }
  }

  async fetchTokenTraders(
    mint: string,
    limit: number = 50,
    orderBy: 'profit' | 'profit_change' | 'last_active' | 'buy_volume_cur' = 'profit',
    tag: 'all' | 'bundler' = 'all',
  ): Promise<any> {
    this.validateSolAddress(mint, 'mint');
    
    try {
      if (this.fetchMode !== 'direct') {
        // CLI calls are local and shouldn't be bottlenecked by API rate limits
        const data = await this.fetchCliData('traders', mint, { limit, orderBy, tag });
        if (data && this.hasTraderEntries(data)) return data;
        if (data) {
          log.info(`GMGN CLI traders returned no trader rows for ${mint}, falling back to API`, {
            shape: this.describeResponseShape(data),
          });
        } else {
          log.debug(`GMGN CLI traders returned no data for ${mint}, falling back to API`);
        }
      }
      
      const endpoint = `v1/token/traders/sol/${mint}?limit=${limit}&tag=${tag}&orderby=${orderBy}&direction=desc`;
      const data = await this.limiter.schedule(() => this.fetchRawTokenData(endpoint, mint));
      return data;
    } catch (err) {
      log.error(`Failed to fetch token traders via GMGN ${this.fetchMode.toUpperCase()} for ${mint}`, err);
      return null;
    }
  }

  async fetchBundlerMetrics(mint: string): Promise<FetchResult> {
    this.validateSolAddress(mint, 'mint');

    try {
      let data: Record<string, unknown> | null = null;
      if (this.fetchMode !== 'direct') {
        // CLI calls are local and shouldn't be bottlenecked by API rate limits
        data = await this.fetchCliData('security', mint);
      }
      
      if (!data) {
        data = await this.limiter.schedule(() => this.fetchRawTokenData('v1/token/security', mint));
      }

      if (!data) {
        return { success: false, error: `Empty response or error from GMGN ${this.fetchMode.toUpperCase()}` };
      }

      const metrics: BundlerMetrics = {
        mint,
        timestamp: new Date().toISOString(),
        bundlersPercent: this.parsePercentage(data.bundler_trader_amount_rate ?? data.bundled_amount_rate),
        bundlersCount: this.parseNullableNumber(data.bundle_num ?? data.bundler_count),
        initialBaseReserve: this.parseNullableNumber(data.initial_base_reserve),
        topWallets: this.parseNullableNumber(data.top_wallets),
        top10HolderRate: this.parsePercentage(data.top_10_holder_rate),
        bundledAmountRate: this.parseNullableNumber(data.bundled_amount_rate ?? data.bundler_trader_amount_rate),
        rawData: JSON.stringify(data),
      };

      return { success: true, metrics, raw: data };
    } catch (err) {
      return { success: false, error: String(err) };
    }
  }

  private parsePercentage(value: unknown): number | null {
    const n = this.parseNullableNumber(value);
    return n !== null ? n * 100 : null;
  }

  // ── Internal: raw data fetching ───────────────────────────────────────────

  private async fetchCliData(
    type: 'token' | 'traders' | 'security',
    mint: string,
    options: { limit?: number; orderBy?: string; tag?: string } = {}
  ): Promise<Record<string, unknown> | null> {
    // Map internal types to CLI subcommands
    let subcommand = '';
    switch (type) {
      case 'token': subcommand = 'info'; break;
      case 'traders': subcommand = 'traders'; break;
      case 'security': subcommand = 'security'; break;
    }

    let cmd = `gmgn-cli token ${subcommand} --chain ${this.chain} --address ${mint} --raw`;

    // Add extra options if provided
    if (options.limit) {
      cmd += ` --limit ${options.limit}`;
    }
    if (options.orderBy) {
      cmd += ` --order-by ${options.orderBy}`;
    }
    if (options.tag) {
      cmd += ` --tag ${options.tag}`;
    }

    try {
      log.debug(`Executing CLI: ${cmd}`);
      const { stdout, stderr } = await execAsync(cmd, { timeout: REQUEST_TIMEOUT });
      if (stderr) {
        log.debug(`GMGN CLI ${type} stderr for ${mint}: ${stderr}`);
      }
      
      if (!stdout || stdout.trim() === '') {
        log.warn(`GMGN CLI ${type} returned empty stdout for ${mint}`);
        return null;
      }

      // Find the first '{' and last '}' to extract the JSON part in case there's log noise
      const start = stdout.indexOf('{');
      const end = stdout.lastIndexOf('}');
      if (start === -1 || end === -1 || end < start) {
        log.warn(`GMGN CLI ${type} output does not contain valid JSON for ${mint}. Raw: ${stdout.substring(0, 200)}`);
        return null;
      }
      const jsonStr = stdout.substring(start, end + 1);
      const json = JSON.parse(jsonStr);
      const unwrapped = this.unwrapResponseData(json);
      
      if (!unwrapped) {
        log.warn(`GMGN CLI ${type} could not unwrap data for ${mint}. Raw: ${stdout.substring(0, 200)}`);
        return null;
      }

      unwrapped.source = 'cli';
      return unwrapped;
    } catch (err) {
      log.debug(`GMGN CLI ${type} failed for ${mint}`, { error: String(err) });
      return null;
    }
  }

  private async fetchRawTokenData(
    endpoint: string,
    mint: string
  ): Promise<Record<string, unknown> | null> {
    const separator = endpoint.includes('?') ? '&' : '?';
    const url = `${this.baseUrl}/${endpoint}${separator}chain=${this.chain}&address=${mint}`;

    const primary = await this.fetchRawTokenDataWithKey(
      url,
      this.apiKey,
      this.limiter,
      'primary',
    );
    if (primary.data || !primary.shouldFallback) {
      return primary.data;
    }

    if (!this.fallbackApiKey || !this.fallbackLimiter) {
      return null;
    }

    log.warn(`GMGN primary request failed; trying GMGN_FALLBACK_API_KEY`, {
      mint,
      endpoint,
      reason: primary.reason,
    });

    const fallback = await this.fallbackLimiter.schedule(() =>
      this.fetchRawTokenDataWithKey(
        url,
        this.fallbackApiKey!,
        this.fallbackLimiter!,
        'fallback',
      ),
    );
    return fallback.data;
  }

  private async fetchRawTokenDataWithKey(
    url: string,
    apiKey: string,
    limiter: RateLimiter,
    keyRole: 'primary' | 'fallback',
  ): Promise<{
    data: Record<string, unknown> | null;
    shouldFallback: boolean;
    reason: string;
  }> {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), REQUEST_TIMEOUT);

    try {
      const resp = await fetch(url, {
        method: 'GET',
        headers: {
          'X-API-KEY': apiKey,
          Accept: 'application/json',
          'User-Agent': 'gmgn-monitor/1.0',
        },
        signal: controller.signal,
      });

      if (resp.status === 429) {
        const retryMs = this.getRetryDelayMs(resp) ?? BLOCKED_RETRY_MS;
        limiter.onRateLimited(retryMs);
        return {
          data: null,
          shouldFallback: keyRole === 'primary',
          reason: 'HTTP 429 rate limited',
        };
      }

      if (resp.status === 401 || resp.status === 403 || resp.status >= 500) {
        return {
          data: null,
          shouldFallback: keyRole === 'primary',
          reason: `HTTP ${resp.status}`,
        };
      }

      if (!resp.ok) {
        return {
          data: null,
          shouldFallback: false,
          reason: `HTTP ${resp.status}`,
        };
      }

      const json = (await resp.json()) as GmgnSecurityResponse;
      if (json.code !== undefined && json.code !== 0) {
        return {
          data: null,
          shouldFallback: false,
          reason: `GMGN response code ${json.code}`,
        };
      }

      limiter.onSuccess(this.baselineMinTime);
      const unwrapped = this.unwrapResponseData(json);
      if (unwrapped) {
        unwrapped.source = keyRole === 'primary' ? 'api' : 'api-fallback';
      }
      return {
        data: unwrapped,
        shouldFallback: false,
        reason: unwrapped ? 'success' : 'empty response',
      };
    } catch (err) {
      return {
        data: null,
        shouldFallback: keyRole === 'primary',
        reason: err instanceof Error ? err.message : String(err),
      };
    } finally {
      clearTimeout(timer);
    }
  }

  // ── Internal: extract MC from raw data ────────────────────────────────────

  private extractMarketCapUsd(data: Record<string, unknown>): number | null {
    const candidates = [
      data.market_cap,
      data.marketcap,
      data.market_cap_usd,
      data.mkt_cap,
      data.fdv,
      data.fdv_usd,
      data.fully_diluted_valuation,
      data.mc,
      data.usd_market_cap,
      this.asRecord(data.pool).market_cap,
      this.asRecord(data.pool).market_cap_usd,
      this.asRecord(data.pool).fdv,
      this.asRecord(data.stat).market_cap,
      this.asRecord(data.stat).market_cap_usd,
      this.asRecord(data.token).market_cap,
      this.asRecord(data.token).fdv,
    ];

    for (const candidate of candidates) {
      const parsed = this.parseNullableNumber(candidate);
      if (parsed !== null && parsed > 0) return parsed;
    }

    // Fallback: Calculate MC = Price * Supply
    const price = this.parseNullableNumber(this.asRecord(data.price).price) ?? 
                  this.parseNullableNumber(data.price) ??
                  this.parseNullableNumber(this.asRecord(data.token).price);
                  
    const supply = this.parseNullableNumber(data.circulating_supply ?? data.total_supply) ??
                   this.parseNullableNumber(this.asRecord(data.token).circulating_supply ?? this.asRecord(data.token).total_supply);

    if (price !== null && supply !== null) {
      const calculatedMc = price * supply;
      if (calculatedMc > 0) return calculatedMc;
    }

    return null;
  }

  // ── Public: Swaps & Quotes (Jupiter) ──────────────────────────────────────

  async quoteTokenSellForSol(
    walletAddress: string,
    mint: string,
    percent: number
  ): Promise<SellQuote> {
    const balance = await this.getTokenBalance(walletAddress, mint);
    if (balance === 0n) throw new Error(`No token balance found for ${mint}`);

    const amount = (balance * BigInt(Math.round(percent))) / 100n;
    const url = `${this.jupiterSwapBaseUrl}/quote?inputMint=${mint}&outputMint=${SOL_MINT}&amount=${amount}&slippageBps=50`;
    
    const json = await this.fetchJupiterJson(url, 'GET');
    return {
      inputToken: mint,
      outputToken: SOL_MINT,
      soldPercent: percent,
      inputAmount: String(amount),
      outputAmount: String(json.outAmount),
      estimatedOutputSol: Number(json.outAmount) / 1e9,
      raw: json,
    };
  }

  async buyTokenWithSol(
    walletAddress: string,
    mint: string,
    options: BuyOptions
  ): Promise<SellResult> {
    if (!this.tradingKeypair) throw new Error('No JUPITER_PRIVATE_KEY configured');

    const amountLamports = Math.floor(options.solAmount * 1e9);
    
    // 1. GET ORDER (V2 Meta-Aggregator)
    const orderUrl = `${this.jupiterSwapBaseUrl}/order?inputMint=${SOL_MINT}&outputMint=${mint}&amount=${amountLamports}&taker=${walletAddress}`;
    const order = await this.fetchJupiterJson(orderUrl, 'GET');

    if (!order.transaction || !order.requestId) {
      throw new Error(`Invalid Jupiter order response: ${JSON.stringify(order)}`);
    }

    // 2. SIGN TRANSACTION
    const signedTx = this.signVersionedTransaction(order.transaction as string);

    // 3. EXECUTE
    const execute = await this.fetchJupiterJson(`${this.jupiterSwapBaseUrl}/execute`, 'POST', {
      signedTransaction: signedTx,
      requestId: order.requestId,
    });

    return {
      orderId: String(order.requestId),
      hash: String(execute.signature),
      status: 'confirmed',
      inputToken: mint,
      outputToken: SOL_MINT,
      soldPercent: 100,
      filledInputAmount: String(order.inAmount || 0),
      filledOutputAmount: String(order.outAmount || 0),
      raw: { order, execute }
    };
  }

 // REPLACE lines 658-701 with:
async sellTokenForSol(
  walletAddress: string,
  mint: string,
  options: SellOptions & { preFetchedBalance?: bigint }
): Promise<SellResult> {
  if (!this.tradingKeypair) throw new Error('No JUPITER_PRIVATE_KEY configured');

  const balance = options.preFetchedBalance !== undefined
    ? options.preFetchedBalance
    : await this.getTokenBalance(walletAddress, mint);

  if (balance === 0n) throw new Error(`No token balance found for ${mint}`);

  const amountRaw = (balance * BigInt(Math.round(options.percent))) / 100n;
  if (amountRaw <= 0n) throw new Error(`Sell amount is zero for ${mint}`);

  try {
    return await this.sellTokenForSolViaPumpPortalLocal(walletAddress, mint, amountRaw, options);
  } catch (err) {
    log.warn(`PumpPortal local sell path failed for ${mint}; falling back to Pump.fun SDK`, {
      error: err instanceof Error ? err.message : String(err),
    });
  }

  try {
    return await this.sellTokenForSolViaPump(walletAddress, mint, amountRaw, options);
  } catch (err) {
    const errorMessage = err instanceof Error ? err.message : String(err);
    if (this.isPumpOnlySellFailure(errorMessage)) {
      log.warn(`Pump.fun sell path failed for ${mint}; skipping Jupiter fallback`, {
        error: errorMessage,
      });
      throw err;
    }
    log.warn(`Pump.fun sell path failed for ${mint}; falling back to Jupiter`, {
      error: errorMessage,
    });
  }

  // 1. QUOTE (v1 standard aggregator)
  const slippageBps = this.toJupiterSlippageBps(options);
  const slippageParam = slippageBps !== null
    ? `slippageBps=${slippageBps}`
    : `autoSlippage=true&maxAutoSlippageBps=3000`;

  const JUPITER_V1_BASE = 'https://api.jup.ag/swap/v1';
  const quoteUrl = `${JUPITER_V1_BASE}/quote?inputMint=${mint}&outputMint=${SOL_MINT}&amount=${amountRaw.toString()}&${slippageParam}`;
  const quote = await this.fetchJupiterJson(quoteUrl, 'GET');

  if (!quote.outAmount) {
    throw new Error(`Jupiter quote failed: ${JSON.stringify(quote)}`);
  }

  // 2. GET SWAP TRANSACTION (v1 standard)
  const swapBody: Record<string, unknown> = {
    quoteResponse: quote,
    userPublicKey: walletAddress,
    wrapAndUnwrapSol: true,
    prioritizationFeeLamports: options.priorityFeeSol > 0
      ? Math.floor(options.priorityFeeSol * 1e9)
      : 'auto',
  };

  const swapResp = await this.fetchJupiterJson(`${JUPITER_V1_BASE}/swap`, 'POST', swapBody);

  if (!swapResp.swapTransaction) {
    throw new Error(`Jupiter swap response missing transaction: ${JSON.stringify(swapResp)}`);
  }

  // 3. SIGN
  const tx = VersionedTransaction.deserialize(
    Buffer.from(swapResp.swapTransaction as string, 'base64')
  );
  tx.sign([this.tradingKeypair]);
  const rawTx = Buffer.from(tx.serialize());

  // 4. SEND via RPC directly (no Ultra /execute needed)
  const signature = await this.sendRawTransactionAndAssertSuccess(rawTx, mint);

  log.info(`Sell transaction sent: ${signature}`, { mint, amount: amountRaw.toString() });

  return {
    orderId: null,
    hash: signature,
    status: 'confirmed',
    inputToken: mint,
    outputToken: SOL_MINT,
    soldPercent: options.percent,
    filledInputAmount: String(amountRaw),
    filledOutputAmount: String(quote.outAmount),
    raw: { quote, swapResp },
  };
}

private async sellTokenForSolViaPumpPortalLocal(
  walletAddress: string,
  mint: string,
  amountRaw: bigint,
  options: SellOptions
): Promise<SellResult> {
  if (!this.tradingKeypair) throw new Error('No JUPITER_PRIVATE_KEY configured');

  const signerPublicKey = this.tradingKeypair.publicKey.toBase58();
  if (walletAddress !== signerPublicKey) {
    log.warn('Sell wallet differs from trading keypair; using trading keypair for PumpPortal local sell', {
      requestedWallet: walletAddress,
      signerPublicKey,
      mint,
    });
  }

  const sellPercent = Math.min(Math.max(options.percent, 0), 100);
  const amount =
    sellPercent >= 99.999
      ? '100%'
      : `${sellPercent.toFixed(4).replace(/\.?0+$/, '')}%`;
  const slippage = this.toPumpSlippagePercent(options);
  let lastError: unknown = null;

  for (const pool of PUMP_PORTAL_POOLS) {
    try {
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), REQUEST_TIMEOUT);
      let resp: Response;
      try {
        resp = await fetch(PUMP_PORTAL_LOCAL_URL, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            publicKey: signerPublicKey,
            action: 'sell',
            mint,
            amount,
            denominatedInSol: 'false',
            slippage,
            priorityFee: options.priorityFeeSol,
            pool,
          }),
          signal: controller.signal,
        });
      } finally {
        clearTimeout(timer);
      }

      if (!resp.ok) {
        const text = await resp.text().catch(() => '');
        throw new Error(`PumpPortal error: ${resp.status}${text ? ` ${text}` : ''}`);
      }

      const tx = VersionedTransaction.deserialize(
        Buffer.from(await resp.arrayBuffer()),
      );
      tx.sign([this.tradingKeypair]);

      const signature = await this.sendRawTransactionAndAssertSuccess(
        Buffer.from(tx.serialize()),
        mint,
        { maxRetries: 0 },
      );

      log.info(`PumpPortal local sell transaction sent: ${signature}`, {
        mint,
        amount,
        walletTotalRequestedAmount: amountRaw.toString(),
        slippage,
        priorityFeeSol: options.priorityFeeSol,
        pool,
      });

      return {
        orderId: null,
        hash: signature,
        status: 'confirmed',
        inputToken: mint,
        outputToken: SOL_MINT,
        soldPercent: options.percent,
        filledInputAmount: amountRaw.toString(),
        filledOutputAmount: '0',
        raw: {
          route: 'pumpportal-local',
          amount,
          walletTotalRequestedAmount: amountRaw.toString(),
          slippage,
          priorityFeeSol: options.priorityFeeSol,
          pool,
        },
      };
    } catch (err) {
      lastError = err;
      log.warn(`PumpPortal local sell attempt failed with pool ${pool} for ${mint}`, {
        error: err instanceof Error ? err.message : String(err),
      });
    }
  }

  throw lastError ?? new Error(`PumpPortal local sell failed for ${mint}`);
}

private async sellTokenForSolViaPump(
  walletAddress: string,
  mint: string,
  amountRaw: bigint,
  options: SellOptions
): Promise<SellResult> {
  if (!this.tradingKeypair) throw new Error('No JUPITER_PRIVATE_KEY configured');

  const user = new PublicKey(walletAddress);
  const mintPk = new PublicKey(mint);
  const tokenAccounts = (await this.getTokenAccountsWithBalance(user, mintPk))
    .sort((a, b) => (a.balance === b.balance ? 0 : a.balance > b.balance ? -1 : 1));
  if (tokenAccounts.length === 0) {
    throw new Error(`No token accounts with balance found for ${mint}`);
  }

  const [global, feeConfig, supplyResp] = await Promise.all([
    this.pumpSdk.fetchGlobal(),
    this.pumpSdk.fetchFeeConfig().catch(() => null),
    this.connection.getTokenSupply(mintPk, 'confirmed'),
  ]);

  const mintSupply = new BN(supplyResp.value.amount);
  let lastError: unknown = null;

  for (const sourceTokenAccount of tokenAccounts) {
    const tokenProgram = sourceTokenAccount.tokenProgram;
    try {
      const amountRawForProgram =
        (sourceTokenAccount.balance * BigInt(Math.round(options.percent))) / 100n;
      if (amountRawForProgram <= 0n) {
        throw new Error(`No token balance found in ${sourceTokenAccount.account.toBase58()}`);
      }
      const amount = new BN(amountRawForProgram.toString());

      const bondingCurveAccountInfo = await this.connection.getAccountInfo(
        bondingCurvePda(mintPk),
        'confirmed',
      );
      if (!bondingCurveAccountInfo) {
        throw new Error(`Bonding curve account not found for mint: ${mint}`);
      }

      const bondingCurve = PUMP_SDK.decodeBondingCurve(bondingCurveAccountInfo);
      const quotedSolAmount = getSellSolAmountFromTokenAmount({
        global,
        feeConfig,
        mintSupply,
        bondingCurve,
        amount,
      });

      if (quotedSolAmount.lte(new BN(0))) {
        throw new Error('Pump.fun quote returned 0 SOL; token may be migrated or curve has no liquidity');
      }

      const slippage = this.toPumpSlippagePercent(options);
      const associatedUser = getAssociatedTokenAddressSync(
        mintPk,
        user,
        true,
        tokenProgram,
      );
      const bondingCurveAddress = bondingCurvePda(mintPk);
      const associatedBondingCurve = getAssociatedTokenAddressSync(
        mintPk,
        bondingCurveAddress,
        true,
        tokenProgram,
      );
      const setupInstructions: TransactionInstruction[] = [
        createAssociatedTokenAccountIdempotentInstruction(
          user,
          associatedBondingCurve,
          bondingCurveAddress,
          mintPk,
          tokenProgram,
        ),
      ];

      if (sourceTokenAccount && !sourceTokenAccount.account.equals(associatedUser)) {
        setupInstructions.push(
          createAssociatedTokenAccountIdempotentInstruction(
            user,
            associatedUser,
            user,
            mintPk,
            tokenProgram,
          ),
          createTransferInstruction(
            sourceTokenAccount.account,
            associatedUser,
            user,
            amountRawForProgram,
            [],
            tokenProgram,
          ),
        );
      }

      const instructions = [
        ...setupInstructions,
        ...(await PUMP_SDK.sellInstructions({
          global,
          bondingCurveAccountInfo,
          bondingCurve,
          mint: mintPk,
          user,
          amount,
          solAmount: quotedSolAmount,
          slippage,
          tokenProgram,
          mayhemMode: Boolean(bondingCurve.isMayhemMode),
          cashback: Boolean(bondingCurve.isCashbackCoin),
        })),
      ];

      const computeUnitLimit = 300_000;
      const priorityFeeLamports = Math.floor(options.priorityFeeSol * 1e9);
      if (priorityFeeLamports > 0) {
        instructions.unshift(
          ComputeBudgetProgram.setComputeUnitLimit({ units: computeUnitLimit }),
          ComputeBudgetProgram.setComputeUnitPrice({
            microLamports: Math.max(1, Math.floor((priorityFeeLamports * 1_000_000) / computeUnitLimit)),
          }),
        );
      }

      const latestBlockhash = await this.connection.getLatestBlockhash('confirmed');
      const tx = new VersionedTransaction(
        new TransactionMessage({
          payerKey: user,
          recentBlockhash: latestBlockhash.blockhash,
          instructions,
        }).compileToV0Message(),
      );
      tx.sign([this.tradingKeypair]);

      const signature = await this.sendRawTransactionAndAssertSuccess(
        Buffer.from(tx.serialize()),
        mint,
      );

      log.info(`Pump.fun sell transaction sent: ${signature}`, {
        mint,
        amount: amountRawForProgram.toString(),
        walletTotalRequestedAmount: amountRaw.toString(),
        quotedSolLamports: quotedSolAmount.toString(),
        slippage,
        tokenProgram: tokenProgram.toBase58(),
        sourceTokenAccount: sourceTokenAccount?.account.toBase58() ?? associatedUser.toBase58(),
        sourceTokenBalance: sourceTokenAccount.balance.toString(),
        associatedUser: associatedUser.toBase58(),
        associatedBondingCurve: associatedBondingCurve.toBase58(),
        setupInstructionCount: setupInstructions.length,
      });

      return {
        orderId: null,
        hash: signature,
        status: 'confirmed',
        inputToken: mint,
        outputToken: SOL_MINT,
        soldPercent: options.percent,
        filledInputAmount: amountRawForProgram.toString(),
        filledOutputAmount: quotedSolAmount.toString(),
        raw: {
          route: 'pump.fun',
          walletTotalRequestedAmount: amountRaw.toString(),
          quotedSolLamports: quotedSolAmount.toString(),
          slippage,
          tokenProgram: tokenProgram.toBase58(),
          sourceTokenAccount: sourceTokenAccount?.account.toBase58() ?? associatedUser.toBase58(),
          sourceTokenBalance: sourceTokenAccount.balance.toString(),
          associatedUser: associatedUser.toBase58(),
          associatedBondingCurve: associatedBondingCurve.toBase58(),
          setupInstructionCount: setupInstructions.length,
        },
      };
    } catch (err) {
      lastError = err;
      log.warn(`Pump.fun sell attempt failed with token program ${tokenProgram.toBase58()} for ${mint}`, {
        error: err instanceof Error ? err.message : String(err),
      });
    }
  }

  throw lastError ?? new Error(`Pump.fun sell failed for ${mint}`);
}

private isPumpOnlySellFailure(message: string): boolean {
  return (
    message.includes('NotEnoughTokensToSell') ||
    message.includes('IncorrectProgramId') ||
    message.includes('Transaction ') ||
    message.includes('token balance remains') ||
    message.includes('was not confirmed')
  );
}

  // ── Jupiter Helpers ───────────────────────────────────────────────────────

  private signVersionedTransaction(transactionBase64: string): string {
    if (!this.tradingKeypair) throw new Error('No trading keypair');
    
    const tx = VersionedTransaction.deserialize(Buffer.from(transactionBase64, 'base64'));
    tx.sign([this.tradingKeypair]);
    return Buffer.from(tx.serialize()).toString('base64');
  }

  private async executeSwapTransaction(swapTransactionBase64: string): Promise<string> {
    // This method is now legacy as V2 uses /execute with requestId
    throw new Error('Use fetchJupiterJson(/execute) for V2 Meta-Aggregator path');
  }

  private async executeJupiterOrder(signedTransaction: string, requestId: string): Promise<Record<string, unknown>> {
    return this.fetchJupiterJson(`${this.jupiterSwapBaseUrl}/execute`, 'POST', { 
      signedTransaction, 
      requestId 
    });
  }

  private async sendRawTransactionAndAssertSuccess(
    rawTx: Buffer,
    mint: string,
    options: { maxRetries?: number; timeoutMs?: number } = {},
  ): Promise<string> {
    const signature = await this.connection.sendRawTransaction(rawTx, {
      skipPreflight: true,
      maxRetries: options.maxRetries ?? 3,
    });

    const deadline = Date.now() + (options.timeoutMs ?? 12_000);
    while (Date.now() < deadline) {
      const status = await this.connection.getSignatureStatuses([signature], {
        searchTransactionHistory: true,
      });
      const value = status.value[0];
      if (value) {
        if (value.err) {
          throw new Error(
            `Transaction ${signature} failed on-chain for ${mint}: ${JSON.stringify(value.err)}`,
          );
        }
        if (value.confirmationStatus) {
          return signature;
        }
      }
      await sleep(250);
    }

    throw new Error(`Transaction ${signature} was not confirmed for ${mint} before timeout`);
  }

  // REPLACE lines 725-745 with:
private async fetchJupiterJson(url: string, method: 'GET' | 'POST', body?: Record<string, unknown>): Promise<Record<string, unknown>> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), REQUEST_TIMEOUT);
  const headers: Record<string, string> = {
    'Accept': 'application/json',
    'x-api-key': this.jupiterApiKey
  };
  if (method === 'POST') headers['Content-Type'] = 'application/json';

  try {
    const resp = await fetch(url, { method, headers, body: body ? JSON.stringify(body) : undefined, signal: controller.signal });
    const json = await resp.json() as Record<string, unknown>;
    if (!resp.ok) {
      log.error(`Jupiter API Error [${resp.status}]: ${JSON.stringify(json)}`, { url, method });
      throw new Error(`Jupiter API failed: ${resp.status}${json.error ? ` - ${json.error}` : ''}${json.message ? ` - ${json.message}` : ''}`);
    }
    // Jupiter Ultra returns HTTP 200 with errorCode in body on routing failure — catch it here
    if (json.errorCode !== undefined) {
      throw new Error(`Jupiter routing error (${json.errorCode}): ${json.errorMessage ?? json.error ?? JSON.stringify(json)}`);
    }
    return json;
  } finally {
    clearTimeout(timer);
  }
}

  private parseJupiterSellResult(order: Record<string, unknown>, execute: Record<string, unknown>, mint: string, soldPercent: number): SellResult {
    const orderAny = order as any;
    const executeAny = execute as any;
    return {
      orderId: String(orderAny.requestId),
      hash: String(executeAny.signature),
      status: this.normalizeJupiterStatus(String(executeAny.status)),
      inputToken: mint,
      outputToken: SOL_MINT,
      soldPercent,
      filledInputAmount: String(executeAny.inputAmountResult || orderAny.inAmount),
      filledOutputAmount: String(executeAny.outputAmountResult || orderAny.outAmount),
      raw: { order, execute }
    };
  }

  private normalizeJupiterStatus(status: string): string {
    return status.toLowerCase() === 'success' ? 'confirmed' : status.toLowerCase();
  }

  private toJupiterSlippageBps(options: BuyOptions | SellOptions): number | null {
    if (options.autoSlippage) return null;
    // Treat slippage as a percentage (e.g., 0.3 means 0.3%, 10 means 10%)
    const bps = Math.round(options.slippage * 100);
    // Cap at 50% (5000 bps) for safety
    return Math.min(Math.max(bps, 0), 5000);
  }

  private toPumpSlippagePercent(options: SellOptions): number {
    if (options.autoSlippage) return 30;
    // Pump SDK expects percent-style slippage, same as SELL_SLIPPAGE.
    return Math.min(Math.max(options.slippage, 0), 50);
  }

  // ── Utils ─────────────────────────────────────────────────────────────────

  async getParsedTokenAccountsForMint(owner: PublicKey, mint: PublicKey) {
    const accounts = [];
    for (const programId of TOKEN_PROGRAM_IDS) {
      const { value } = await this.connection.getParsedTokenAccountsByOwner(
        owner,
        { programId, mint },
      );
      accounts.push(...value);
    }
    return accounts;
  }

  private async getTokenBalance(wallet: string, mint: string): Promise<bigint> {
    const pubkey = new PublicKey(wallet);
    const mintPubkey = new PublicKey(mint);
    let total = 0n;

    for (const programId of TOKEN_PROGRAM_IDS) {
      const accounts = await this.connection.getParsedTokenAccountsByOwner(
        pubkey,
        { programId, mint: mintPubkey },
      );
      for (const { account } of accounts.value) {
        const parsed = account.data as ParsedAccountData;
        const amount = parsed?.parsed?.info?.tokenAmount?.amount;
        if (typeof amount === 'string' && /^\d+$/.test(amount)) {
          total += BigInt(amount);
        }
      }
    }

    return total;
  }

  private async getTokenProgramsWithBalance(owner: PublicKey, mint: PublicKey): Promise<PublicKey[]> {
    const programs: PublicKey[] = [];
    for (const programId of TOKEN_PROGRAM_IDS) {
      const { value } = await this.connection.getParsedTokenAccountsByOwner(
        owner,
        { programId, mint },
      );
      const hasBalance = value.some(({ account }) => {
        const parsed = account.data as ParsedAccountData;
        const amount = parsed?.parsed?.info?.tokenAmount?.amount;
        return typeof amount === 'string' && /^\d+$/.test(amount) && BigInt(amount) > 0n;
      });
      if (hasBalance) programs.push(programId);
    }
    return programs;
  }

  private async getTokenAccountsWithBalance(
    owner: PublicKey,
    mint: PublicKey,
  ): Promise<Array<{ tokenProgram: PublicKey; account: PublicKey; balance: bigint }>> {
    const tokenAccounts: Array<{ tokenProgram: PublicKey; account: PublicKey; balance: bigint }> = [];
    for (const programId of TOKEN_PROGRAM_IDS) {
      const { value } = await this.connection.getParsedTokenAccountsByOwner(
        owner,
        { programId, mint },
      );
      for (const entry of value) {
        const parsed = entry.account.data as ParsedAccountData;
        const amount = parsed?.parsed?.info?.tokenAmount?.amount;
        if (typeof amount === 'string' && /^\d+$/.test(amount)) {
          const balance = BigInt(amount);
          if (balance > 0n) {
            tokenAccounts.push({
              tokenProgram: programId,
              account: entry.pubkey,
              balance,
            });
          }
        }
      }
    }
    return tokenAccounts;
  }

  private validateSolAddress(value: string, label: string): void {
    if (!/^[1-9A-HJ-NP-Za-km-z]{32,44}$/.test(value)) {
      throw new Error(`Invalid ${label}: ${value}`);
    }
  }

  private unwrapResponseData(json: any): Record<string, unknown> {
    return (json.data ?? json) as Record<string, unknown>;
  }

  private hasTraderEntries(data: unknown): boolean {
    return this.extractTraderEntries(data).length > 0;
  }

  private extractTraderEntries(data: unknown): unknown[] {
    if (Array.isArray(data)) return data;
    if (!data || typeof data !== 'object') return [];

    const record = data as Record<string, unknown>;
    const candidates = [
      record.list,
      record.traders,
      record.items,
      this.asRecord(record.data).list,
      this.asRecord(record.data).traders,
      this.asRecord(record.data).items,
    ];

    const nestedData = record.data;
    if (Array.isArray(nestedData)) candidates.push(nestedData);

    for (const candidate of candidates) {
      if (Array.isArray(candidate)) return candidate;
    }
    return [];
  }

  private describeResponseShape(data: unknown): Record<string, unknown> {
    if (Array.isArray(data)) return { type: 'array', length: data.length };
    if (!data || typeof data !== 'object') return { type: typeof data };

    const record = data as Record<string, unknown>;
    const dataRecord = this.asRecord(record.data);
    return {
      type: 'object',
      keys: Object.keys(record).slice(0, 12),
      listLength: Array.isArray(record.list) ? record.list.length : null,
      tradersLength: Array.isArray(record.traders) ? record.traders.length : null,
      itemsLength: Array.isArray(record.items) ? record.items.length : null,
      dataKeys: Object.keys(dataRecord).slice(0, 12),
      dataIsArray: Array.isArray(record.data),
      dataLength: Array.isArray(record.data) ? record.data.length : null,
    };
  }

  private getRetryDelayMs(resp: Response): number | undefined {
    const retryAfter = resp.headers.get('Retry-After');
    return retryAfter ? parseInt(retryAfter, 10) * 1000 : undefined;
  }

  private asRecord(value: unknown): Record<string, unknown> {
    return (value && typeof value === 'object') ? value as Record<string, unknown> : {};
  }

  private parseNullableNumber(value: unknown): number | null {
    const n = Number(value);
    return Number.isFinite(n) ? n : null;
  }
}
