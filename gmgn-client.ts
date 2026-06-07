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
import { Connection, Keypair, PublicKey, VersionedTransaction } from '@solana/web3.js';
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

const bs58 = require('bs58') as { decode(value: string): Buffer };

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
  private readonly baselineMinTime: number;

  constructor(config: ServiceConfig, limiter: RateLimiter, rpcUrlOverride?: string) {
    this.baseUrl          = config.gmgnApiBaseUrl.replace(/\/$/, '');
    this.apiKey           = config.gmgnApiKey;
    this.jupiterSwapBaseUrl = config.jupiterSwapBaseUrl.replace(/\/$/, '');
    this.jupiterApiKey    = config.jupiterApiKey;
    this.jupiterPriceApiKey = config.jupiterPriceApiKey;
    this.connection       = new Connection(rpcUrlOverride || config.solanaRpcUrl, 'confirmed');
    this.limiter          = limiter;
    this.baselineMinTime  = config.rateLimitMinTime;
  }

  // ── Public: fetch Market Cap (Primary: GMGN API, Secondary: Jupiter + RPC) ──

  async fetchTokenMarketCapUsd(mint: string): Promise<number | null> {
    this.validateSolAddress(mint, 'mint');

    // 1. PRIMARY: GMGN Web API (v1/token/info)
    try {
      const data = await this.limiter.schedule(() => this.fetchRawTokenData('v1/token/info', mint));
      if (data) {
        const marketCap = this.extractMarketCapUsd(data);
        if (marketCap !== null) {
          log.debug(`Calculated MC via GMGN API primary: $${marketCap.toLocaleString()}`, { mint });
          return marketCap;
        }
      }
    } catch (err) {
      log.debug(`GMGN API primary MC fetch failed for ${mint}`, { error: String(err) });
    }

    // 2. SECONDARY: Jupiter Price API + RPC Supply
    try {
      log.debug(`Using Jupiter + RPC secondary for MC: ${mint}`);
      
      // Fetch Supply from RPC
      const supplyResp = await this.connection.getTokenSupply(new PublicKey(mint), 'confirmed');
      const supply = supplyResp.value.uiAmount;
      if (supply === null) return null;

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
            log.debug(`Calculated MC via Jupiter + RPC secondary: $${marketCap.toLocaleString()}`, {
              mint,
              supply,
              priceUsd
            });
            return marketCap;
          }
        }
      }
    } catch (err) {
      log.error(`Both MC fetch methods failed for ${mint}`, err);
    }

    return null;
  }

  // ── Public: fetch top profitable traders for a token ──────────────────────

  async fetchTokenTraders(
    mint: string,
    limit: number = 50,
    orderBy: 'profit' | 'profit_change' | 'last_active' = 'profit'
  ): Promise<any> {
    this.validateSolAddress(mint, 'mint');
    
    try {
      const endpoint = `v1/token/traders/sol/${mint}?limit=${limit}&tag=all&orderby=${orderBy}&direction=desc`;
      const data = await this.limiter.schedule(() => this.fetchRawTokenData(endpoint, mint));
      return data;
    } catch (err) {
      log.error(`Failed to fetch token traders via GMGN API for ${mint}`, err);
      return null;
    }
  }

  // ── Internal: raw data fetching ───────────────────────────────────────────

  private async fetchRawTokenData(
    endpoint: string,
    mint: string
  ): Promise<Record<string, unknown> | null> {
    const url = `${this.baseUrl}/${endpoint}?chain=${this.chain}&address=${mint}`;

    try {
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), REQUEST_TIMEOUT);
      const resp = await fetch(url, {
        method: 'GET',
        headers: {
          'X-API-KEY': this.apiKey,
          Accept: 'application/json',
          'User-Agent': 'gmgn-monitor/1.0',
        },
        signal: controller.signal,
      });
      clearTimeout(timer);

      if (resp.status === 429) {
        const retryMs = this.getRetryDelayMs(resp) ?? BLOCKED_RETRY_MS;
        this.limiter.onRateLimited(retryMs);
        return null;
      }
      if (!resp.ok) return null;

      const json = (await resp.json()) as GmgnSecurityResponse;
      if (json.code !== undefined && json.code !== 0) return null;

      return this.unwrapResponseData(json);
    } catch {
      return null;
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
      this.asRecord(data.pool).market_cap,
      this.asRecord(data.pool).market_cap_usd,
      this.asRecord(data.stat).market_cap,
      this.asRecord(data.stat).market_cap_usd,
    ];

    for (const candidate of candidates) {
      const parsed = this.parseNullableNumber(candidate);
      if (parsed !== null && parsed >= 0) return parsed;
    }

    // Fallback: Calculate MC = Price * Supply
    const price = this.parseNullableNumber(this.asRecord(data.price).price);
    const supply = this.parseNullableNumber(data.circulating_supply ?? data.total_supply);

    if (price !== null && supply !== null) {
      const calculatedMc = price * supply;
      if (calculatedMc >= 0) return calculatedMc;
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
    const amountLamports = Math.round(options.solAmount * 1e9);
    const slippageBps = this.toJupiterSlippageBps(options);
    
    const quoteUrl = `${this.jupiterSwapBaseUrl}/quote?inputMint=${SOL_MINT}&outputMint=${mint}&amount=${amountLamports}${slippageBps ? `&slippageBps=${slippageBps}` : ''}`;
    const quote = await this.fetchJupiterJson(quoteUrl, 'GET');

    const swapResp = await this.fetchJupiterJson(`${this.jupiterSwapBaseUrl}/swap`, 'POST', {
      quoteResponse: quote,
      userPublicKey: walletAddress,
      wrapAndUnwrapSol: true,
    });

    const execute = await this.executeJupiterOrder(swapResp.swapTransaction as string, (quote as any).requestId as string);
    return this.parseJupiterSellResult(quote, execute, mint, 100);
  }

  async sellTokenForSol(
    walletAddress: string,
    mint: string,
    options: SellOptions
  ): Promise<SellResult> {
    const quote = await this.quoteTokenSellForSol(walletAddress, mint, options.percent);
    const swapResp = await this.fetchJupiterJson(`${this.jupiterSwapBaseUrl}/swap`, 'POST', {
      quoteResponse: quote.raw,
      userPublicKey: walletAddress,
      wrapAndUnwrapSol: true,
    });

    const execute = await this.executeJupiterOrder(swapResp.swapTransaction as string, (quote.raw as any).requestId);
    return this.parseJupiterSellResult(quote.raw as any, execute, mint, options.percent);
  }

  // ── Jupiter Helpers ───────────────────────────────────────────────────────

  private async executeJupiterOrder(signedTransaction: string, requestId: string): Promise<Record<string, unknown>> {
    return this.fetchJupiterJson(`${this.jupiterSwapBaseUrl}/execute`, 'POST', { signedTransaction, requestId });
  }

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
      if (!resp.ok) throw new Error(`Jupiter API failed: ${resp.status}`);
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
    return Math.round(options.slippage * 10_000);
  }

  // ── Utils ─────────────────────────────────────────────────────────────────

  async getParsedTokenAccountsForMint(owner: PublicKey, mint: PublicKey) {
    const { value } = await this.connection.getParsedTokenAccountsByOwner(owner, { mint });
    return value;
  }

  private async getTokenBalance(wallet: string, mint: string): Promise<bigint> {
    const pubkey = new PublicKey(wallet);
    const mintPubkey = new PublicKey(mint);
    const accounts = await this.connection.getParsedTokenAccountsByOwner(pubkey, { mint: mintPubkey });
    if (accounts.value.length === 0) return 0n;
    return BigInt(accounts.value[0].account.data.parsed.info.tokenAmount.amount);
  }

  private validateSolAddress(value: string, label: string): void {
    if (!/^[1-9A-HJ-NP-Za-km-z]{32,44}$/.test(value)) {
      throw new Error(`Invalid ${label}: ${value}`);
    }
  }

  private unwrapResponseData(json: any): Record<string, unknown> {
    return (json.data ?? json) as Record<string, unknown>;
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
