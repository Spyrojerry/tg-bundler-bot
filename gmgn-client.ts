// ─────────────────────────────────────────────────────────────────────────────
//  gmgn-client.ts  —  GMGN OpenAPI REST client
//
//  Bundler data comes from two endpoints:
//    1.  GET /v1/token/security?chain=sol&address=<mint>
//        Fields: bundler_trader_amount_rate, bundle_num / bundler_count
//
//    2.  GET /v1/token/info?chain=sol&address=<mint>   (fallback)
//        Some GMGN plan tiers surface bundler fields here instead.
//
//  Authentication: GMGN_API_KEY sent as  X-API-KEY  header.
//  (Ed25519 signing is only required for swap/order endpoints.)
//
//  Rate limits: Not published. Community observation ~2 req/s.
//  We rely on RateLimiter for all throttling — this module just fires requests.
// ─────────────────────────────────────────────────────────────────────────────

import { createLogger } from './logger';
import { RateLimiter } from './rate-limiter';
import { execFile } from 'child_process';
import * as path from 'path';
import { Connection, Keypair, PublicKey, VersionedTransaction } from '@solana/web3.js';
import {
  BundlerMetrics,
  FetchResult,
  GmgnSecurityResponse,
  SellOptions,
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
const SOL_MINT = 'So11111111111111111111111111111111111111112';

// ── Helper: sleep ─────────────────────────────────────────────────────────────

const sleep = (ms: number): Promise<void> =>
  new Promise((r) => setTimeout(r, ms));

function getSpawnEnv(): NodeJS.ProcessEnv {
  const env: NodeJS.ProcessEnv = {};
  for (const [key, value] of Object.entries(process.env)) {
    if (key === 'GMGN_PRIVATE_KEY' && value?.trim() === '') continue;
    if (value !== undefined) env[key] = value;
  }
  return env;
}

// ── GmgnClient ────────────────────────────────────────────────────────────────

export class GmgnClient {
  private readonly baseUrl: string;
  private readonly apiKey:  string;
  private readonly fetchMode: ServiceConfig['gmgnFetchMode'];
  private readonly jupiterSwapBaseUrl: string;
  private readonly jupiterApiKey: string;
  private readonly connection: Connection;
  private readonly chain  = 'sol';
  private readonly limiter: RateLimiter;
  private readonly baselineMinTime: number;

  constructor(config: ServiceConfig, limiter: RateLimiter) {
    this.baseUrl          = config.gmgnApiBaseUrl.replace(/\/$/, '');
    this.apiKey           = config.gmgnApiKey;
    this.fetchMode        = config.gmgnFetchMode;
    this.jupiterSwapBaseUrl = config.jupiterSwapBaseUrl.replace(/\/$/, '');
    this.jupiterApiKey    = config.jupiterApiKey;
    this.connection       = new Connection(config.solanaRpcUrl, 'confirmed');
    this.limiter          = limiter;
    this.baselineMinTime  = config.rateLimitMinTime;
  }

  // ── Public: fetch bundler metrics for one token ───────────────────────────

  async fetchBundlerMetrics(mint: string): Promise<FetchResult> {
    if (this.fetchMode === 'direct') {
      return this.fetchDirect(mint);
    }

    if (this.fetchMode === 'cli') {
      return this.fetchWithCli(mint);
    }

    const direct = await this.fetchDirect(mint);
    if (direct.success && this.hasDecisionData(direct.metrics)) {
      return direct;
    }

    const cli = await this.fetchWithCli(mint);
    if (cli.success) return cli;

    if (direct.success) return direct;

    return {
      success: false,
      error: `direct failed: ${direct.error}; cli failed: ${cli.error}`,
      retryAfterMs: direct.retryAfterMs ?? cli.retryAfterMs,
      nonRetryable: direct.nonRetryable && cli.nonRetryable,
    };
  }

  async sellTokenForSol(
    walletAddress: string,
    mint: string,
    options: SellOptions
  ): Promise<SellResult> {
    this.validateSolAddress(walletAddress, 'wallet address');
    this.validateSolAddress(mint, 'token mint');

    const wallet = this.getJupiterWallet(walletAddress);
    const mintPk = new PublicKey(mint);
    const rawAmount = await this.getTokenSellAmount(wallet.publicKey, mintPk, options.percent);

    log.warn(`Submitting confirmed sell via Jupiter Swap V2`, {
      wallet: walletAddress,
      mint,
      percent: options.percent,
      outputToken: SOL_MINT,
      rawAmount: rawAmount.toString(),
    });

    const order = await this.getJupiterOrder(
      mint,
      SOL_MINT,
      rawAmount,
      walletAddress,
      options.priorityFeeSol
    );
    const transactionBase64 = this.asString(order.transaction);
    const requestId = this.asString(order.requestId);
    if (!transactionBase64 || !requestId) {
      const detail = this.asString(order.errorMessage) ?? this.asString(order.error) ?? 'order returned no transaction';
      throw new Error(`Jupiter Swap V2 order failed: ${detail}`);
    }

    const transaction = VersionedTransaction.deserialize(Buffer.from(transactionBase64, 'base64'));
    transaction.sign([wallet]);
    const signedTransaction = Buffer.from(transaction.serialize()).toString('base64');
    const execute = await this.executeJupiterOrder(signedTransaction, requestId);
    const result = this.parseJupiterSellResult(order, execute, mint, options.percent);

    if (result.status === 'failed') {
      const detail = this.asString(execute.error) ?? this.asString(execute.message) ?? 'execution failed';
      throw new Error(`Jupiter Swap V2 execute failed: ${detail}`);
    }

    return result;
  }

  private getJupiterWallet(expectedAddress: string): Keypair {
    const key = this.readJupiterPrivateKey();
    const wallet = Keypair.fromSecretKey(key);
    const actual = wallet.publicKey.toBase58();
    if (actual !== expectedAddress) {
      throw new Error(
        `Jupiter sell key public address ${actual} does not match trading wallet ${expectedAddress}`
      );
    }
    return wallet;
  }

  private readJupiterPrivateKey(): Uint8Array {
    const candidates = [
      'JUPITER_PRIVATE_KEY',
      'SOLANA_PRIVATE_KEY',
      'TRADING_PRIVATE_KEY',
      'PRIVATE_KEY',
    ];
    const found = candidates
      .map((name) => ({ name, value: process.env[name]?.trim() }))
      .find((item) => item.value);

    if (!found?.value) {
      throw new Error(
        'Missing JUPITER_PRIVATE_KEY. Jupiter Swap V2 sell needs the Solana trading wallet private key; GMGN_PRIVATE_KEY is not used for this.'
      );
    }

    try {
      if (found.value.startsWith('[')) {
        const parsed = JSON.parse(found.value) as unknown;
        if (!Array.isArray(parsed)) {
          throw new Error('JSON value is not an array');
        }
        return Uint8Array.from(parsed.map((n) => {
          if (typeof n !== 'number' || n < 0 || n > 255 || !Number.isInteger(n)) {
            throw new Error('JSON array must contain byte values from 0 to 255');
          }
          return n;
        }));
      }

      if (/^\d+(,\s*\d+)+$/.test(found.value)) {
        return Uint8Array.from(found.value.split(',').map((part) => {
          const n = Number(part.trim());
          if (!Number.isInteger(n) || n < 0 || n > 255) {
            throw new Error('comma-separated private key must contain byte values from 0 to 255');
          }
          return n;
        }));
      }

      return bs58.decode(found.value);
    } catch (err) {
      const detail = err instanceof Error ? err.message : String(err);
      throw new Error(`Invalid ${found.name}: expected base58 or JSON byte array Solana secret key (${detail})`);
    }
  }

  private async getTokenSellAmount(
    owner: PublicKey,
    mint: PublicKey,
    percent: number
  ): Promise<bigint> {
    const accounts = await this.connection.getParsedTokenAccountsByOwner(owner, { mint });
    let total = 0n;

    for (const account of accounts.value) {
      const parsed = account.account.data.parsed as {
        info?: { tokenAmount?: { amount?: string } };
      };
      const raw = parsed.info?.tokenAmount?.amount;
      if (raw && /^\d+$/.test(raw)) {
        total += BigInt(raw);
      }
    }

    if (total <= 0n) {
      throw new Error(`No token balance found to sell for ${mint.toBase58()}`);
    }

    const bps = BigInt(Math.max(1, Math.min(10_000, Math.round(percent * 100))));
    const sellAmount = (total * bps) / 10_000n;
    if (sellAmount <= 0n) {
      throw new Error(`Token balance is too small to sell ${percent}% of ${mint.toBase58()}`);
    }
    return sellAmount;
  }

  private async getJupiterOrder(
    inputMint: string,
    outputMint: string,
    amount: bigint,
    taker: string,
    priorityFeeSol: number
  ): Promise<Record<string, unknown>> {
    const url = new URL(`${this.jupiterSwapBaseUrl}/order`);
    url.searchParams.set('inputMint', inputMint);
    url.searchParams.set('outputMint', outputMint);
    url.searchParams.set('amount', amount.toString());
    url.searchParams.set('taker', taker);
    const priorityFeeLamports = Math.round(priorityFeeSol * 1_000_000_000);
    if (priorityFeeLamports > 0) {
      url.searchParams.set('priorityFeeLamports', String(priorityFeeLamports));
      url.searchParams.set('broadcastFeeType', 'exactFee');
    }
    return this.fetchJupiterJson(url.toString(), 'GET');
  }

  private async executeJupiterOrder(
    signedTransaction: string,
    requestId: string
  ): Promise<Record<string, unknown>> {
    return this.fetchJupiterJson(`${this.jupiterSwapBaseUrl}/execute`, 'POST', {
      signedTransaction,
      requestId,
    });
  }

  private async fetchJupiterJson(
    url: string,
    method: 'GET' | 'POST',
    body?: Record<string, unknown>
  ): Promise<Record<string, unknown>> {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), REQUEST_TIMEOUT);
    const headers: Record<string, string> = {
      Accept: 'application/json',
      'User-Agent': 'gmgn-monitor/1.0',
    };
    if (method === 'POST') {
      headers['Content-Type'] = 'application/json';
    }
    headers['x-api-key'] = this.jupiterApiKey;

    try {
      const resp = await fetch(url, {
        method,
        headers,
        body: body ? JSON.stringify(body) : undefined,
        signal: controller.signal,
      });
      const text = await resp.text();
      const json = text.trim() ? JSON.parse(text) as Record<string, unknown> : {};

      if (!resp.ok) {
        const message = this.asString(json.error)
          ?? this.asString(json.message)
          ?? this.asString(json.errorMessage)
          ?? `HTTP ${resp.status}`;
        throw new Error(`Jupiter Swap V2 ${method} failed: ${message}`);
      }

      return json;
    } catch (err) {
      if (err instanceof Error && err.name === 'AbortError') {
        throw new Error(`Jupiter Swap V2 ${method} timeout after ${REQUEST_TIMEOUT}ms`);
      }
      throw err;
    } finally {
      clearTimeout(timer);
    }
  }

  private async fetchDirect(mint: string): Promise<FetchResult> {
    const endpoints = ['v1/token/security', 'v1/token/info'];
    let firstSuccess: FetchResult | null = null;
    let lastError = '';

    for (const endpoint of endpoints) {
      const result = await this.fetchWithRetry(endpoint, mint);

      if (result.success) {
        firstSuccess ??= result;
        if (this.hasDecisionData(result.metrics)) return result;
        continue;
      }

      lastError = result.error;

      if (result.retryAfterMs !== undefined || result.nonRetryable) {
        return result;
      }
    }

    if (firstSuccess?.success) return firstSuccess;
    return { success: false, error: lastError || 'GMGN direct endpoints failed' };
  }

  // ── Internal: fetch with retry ────────────────────────────────────────────

  private async fetchWithRetry(
    endpoint: string,
    mint: string
  ): Promise<FetchResult> {
    let lastError = '';

    for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
      if (attempt > 0) {
        const delay = BASE_RETRY_MS * 2 ** (attempt - 1);
        log.debug(`Retry ${attempt}/${MAX_RETRIES} for ${mint} in ${delay}ms`);
        await sleep(delay);
      }

      const result = await this.limiter.schedule(() =>
        this.doRequest(endpoint, mint)
      );

      if (result.success) {
        this.limiter.onSuccess(this.baselineMinTime);
        return result;
      }

      lastError = result.error;

      // 429 / blocked → activate backoff but don't retry here.
      if (result.retryAfterMs !== undefined) {
        this.limiter.onRateLimited(result.retryAfterMs);
        return result;
      }

      if (result.nonRetryable) return result;
    }

    return { success: false, error: lastError };
  }

  // ── Internal: single HTTP request ─────────────────────────────────────────

  private async doRequest(
    endpoint: string,
    mint: string
  ): Promise<FetchResult> {
    const url = `${this.baseUrl}/${endpoint}?chain=${this.chain}&address=${mint}`;

    log.debug(`GET ${url}`);

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

      // ── 429 Too Many Requests ─────────────────────────────────────────────
      if (resp.status === 429) {
        const retryMs = this.getRetryDelayMs(resp) ?? BLOCKED_RETRY_MS;
        return {
          success: false,
          error: 'Rate limited (429)',
          retryAfterMs: retryMs,
        };
      }

      // ── Other HTTP errors ─────────────────────────────────────────────────
      if (!resp.ok) {
        const body = await resp.text().catch(() => '');
        const isHtml = body.trimStart().startsWith('<');
        const error = isHtml
          ? `HTTP ${resp.status}: non-JSON response from GMGN host`
          : `HTTP ${resp.status}`;
        log.warn(`${error} from ${endpoint}`, { body: body.slice(0, 200) });

        return {
          success: false,
          error,
          retryAfterMs: resp.status === 403 ? BLOCKED_RETRY_MS : undefined,
          nonRetryable: resp.status >= 400 && resp.status < 500,
        };
      }

      // ── Parse JSON ────────────────────────────────────────────────────────
      const json = (await resp.json()) as GmgnSecurityResponse;

      if (json.code !== undefined && json.code !== 0) {
        return { success: false, error: `GMGN error: ${json.msg}` };
      }

      const data = this.unwrapResponseData(json);
      if (!data) {
        return { success: false, error: 'Empty data payload' };
      }

      return { success: true, metrics: this.parseMetrics(mint, data) };

    } catch (err: unknown) {
      if (err instanceof Error && err.name === 'AbortError') {
        return { success: false, error: `Request timeout after ${REQUEST_TIMEOUT}ms` };
      }
      return { success: false, error: String(err) };
    }
  }

  private async fetchWithCli(mint: string): Promise<FetchResult> {
    const commands = ['info'];
    let firstSuccess: FetchResult | null = null;
    let lastError = '';

    for (const command of commands) {
      const result = await this.limiter.schedule(() =>
        this.doCliRequest(command, mint)
      );

      if (result.success) {
        this.limiter.onSuccess(this.baselineMinTime);
        firstSuccess ??= result;
        if (this.hasDecisionData(result.metrics)) return result;
        continue;
      }

      lastError = result.error;
    }

    if (firstSuccess?.success) return firstSuccess;
    return { success: false, error: lastError || 'GMGN CLI endpoints failed' };
  }

  private hasBundlerData(metrics: BundlerMetrics): boolean {
    return metrics.bundlersPercent !== null || metrics.bundlersCount !== null;
  }

  private hasDecisionData(metrics: BundlerMetrics): boolean {
    return this.hasBundlerData(metrics) && metrics.initialBaseReserve !== null;
  }

  private async doCliRequest(command: string, mint: string): Promise<FetchResult> {
    try {
      this.validateSolAddress(mint, 'mint');
    } catch (err) {
      return { success: false, error: err instanceof Error ? err.message : String(err) };
    }

    log.debug(`gmgn-cli token ${command} ${mint}`);

    try {
      const stdout = await this.execGmgnCli([
        'token',
        command,
        '--chain',
        this.chain,
        '--address',
        mint,
        '--raw',
      ]);

      const data = this.parseCliJson(stdout);
      return { success: true, metrics: this.parseMetrics(mint, data) };
    } catch (err: unknown) {
      const message = err instanceof Error ? err.message : String(err);
      return { success: false, error: `gmgn-cli ${command} failed: ${message}` };
    }
  }

  private validateSolAddress(value: string, label: string): void {
    if (!/^[1-9A-HJ-NP-Za-km-z]{32,44}$/.test(value)) {
      throw new Error(`Invalid ${label} for gmgn-cli: ${value}`);
    }
  }

  private parseCliJson(stdout: string): Record<string, unknown> {
    const jsonStart = stdout.indexOf('{');
    if (jsonStart === -1) {
      throw new Error('gmgn-cli returned no JSON');
    }
    return JSON.parse(stdout.slice(jsonStart)) as Record<string, unknown>;
  }

  private parseJupiterSellResult(
    order: Record<string, unknown>,
    execute: Record<string, unknown>,
    mint: string,
    soldPercent: number
  ): SellResult {
    const status = this.normalizeJupiterStatus(this.asString(execute.status));
    return {
      orderId: this.asString(order.requestId),
      hash: this.asString(execute.signature),
      status,
      inputToken: this.asString(order.inputMint) ?? mint,
      outputToken: this.asString(order.outputMint) ?? SOL_MINT,
      soldPercent,
      filledInputAmount:
        this.asString(execute.inputAmountResult) ??
        this.asString(execute.totalInputAmount) ??
        this.asString(order.inAmount),
      filledOutputAmount:
        this.asString(execute.outputAmountResult) ??
        this.asString(execute.totalOutputAmount) ??
        this.asString(order.outAmount),
      raw: { order, execute },
    };
  }

  private normalizeJupiterStatus(status: string | null): string {
    if (!status) return 'unknown';
    if (status.toLowerCase() === 'success') return 'confirmed';
    if (status.toLowerCase() === 'failed') return 'failed';
    return status.toLowerCase();
  }

  private asString(value: unknown): string | null {
    if (typeof value === 'string' && value.trim() !== '') return value;
    if (typeof value === 'number' && Number.isFinite(value)) return String(value);
    return null;
  }

  private execGmgnCli(args: string[]): Promise<string> {
    const env = getSpawnEnv();
    const localBin = `${process.cwd()}/node_modules/.bin`;
    const pathKey = Object.keys(env).find((key) => key.toLowerCase() === 'path') ?? 'PATH';
    env[pathKey] = env[pathKey] ? `${localBin}${path.delimiter}${env[pathKey]}` : localBin;

    return new Promise((resolve, reject) => {
      execFile('gmgn-cli', args, {
        timeout: REQUEST_TIMEOUT,
        maxBuffer: 1024 * 1024,
        env,
        windowsHide: true,
      }, (err, stdout, stderr) => {
        if (err) {
          const detail = stderr.trim() || err.message;
          reject(new Error(detail));
          return;
        }
        resolve(stdout);
      });
    });
  }

  private getRetryDelayMs(resp: Response): number | undefined {
    const retryAfter = resp.headers.get('Retry-After');
    if (retryAfter) {
      const seconds = parseInt(retryAfter, 10);
      if (!Number.isNaN(seconds) && seconds > 0) return seconds * 1_000;
    }

    const reset = resp.headers.get('X-RateLimit-Reset');
    if (reset) {
      const resetSeconds = parseInt(reset, 10);
      if (!Number.isNaN(resetSeconds) && resetSeconds > 0) {
        return Math.max(resetSeconds * 1_000 - Date.now(), 1_000);
      }
    }

    return undefined;
  }

  // ── Internal: extract bundler fields from raw API response ────────────────

  private unwrapResponseData(json: GmgnSecurityResponse): Record<string, unknown> {
    return (json.data ?? json) as Record<string, unknown>;
  }

  private parseMetrics(
    mint: string,
    d: Record<string, unknown>
  ): BundlerMetrics {
    const stat = this.asRecord(d.stat);
    const walletTagsStat = this.asRecord(d.wallet_tags_stat);
    const pool = this.asRecord(d.pool);

    // GMGN token info exposes this as a 0-1 fraction under stat.
    const rawRate = this.parseNullableNumber(
      stat.top_bundler_trader_percentage ??
      d.top_bundler_trader_percentage ??
      d.bundler_trader_amount_rate ??
      d.bundled_amount_rate
    );
    const bundlersPercent =
      rawRate !== null ? parseFloat((rawRate * 100).toFixed(4)) : null;

    const bundlersCount = this.parseNullableNumber(
      walletTagsStat.bundler_wallets ??
      d.bundler_wallets ??
      d.bundle_num ??
      d.bundler_count
    );
    const initialBaseReserve = this.parseNullableNumber(
      this.isAmmPool(pool.exchange) ? pool.initial_base_reserve : null
    );
    const topWallets = this.parseNullableNumber(
      walletTagsStat.top_wallets ??
      d.top_wallets
    );

    return {
      mint,
      timestamp: new Date().toISOString(),
      bundlersPercent,
      bundlersCount: bundlersCount !== null ? Math.round(bundlersCount) : null,
      initialBaseReserve,
      topWallets: topWallets !== null ? Math.round(topWallets) : null,
      bundledAmountRate: rawRate,
      rawData: JSON.stringify(d),
    };
  }

  private asRecord(value: unknown): Record<string, unknown> {
    return value && typeof value === 'object'
      ? value as Record<string, unknown>
      : {};
  }

  private parseNullableNumber(value: unknown): number | null {
    if (typeof value === 'number' && Number.isFinite(value)) return value;
    if (typeof value === 'string' && value.trim() !== '') {
      const n = Number(value);
      return Number.isFinite(n) ? n : null;
    }
    return null;
  }

  private isAmmPool(exchange: unknown): boolean {
    return typeof exchange === 'string' && exchange.toLowerCase().includes('amm');
  }
}
