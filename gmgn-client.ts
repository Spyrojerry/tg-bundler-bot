// ─────────────────────────────────────────────────────────────────────────────
//  gmgn-client.ts  —  GMGN OpenAPI REST client
//
//  Authentication: GMGN_API_KEY sent as  X-API-KEY  header.
//
//  Rate limits: Not published. Community observation ~2 req/s.
//  We rely on RateLimiter for all throttling — this module just fires requests.
// ─────────────────────────────────────────────────────────────────────────────

import { createLogger } from "./logger";
import { RateLimiter } from "./rate-limiter";
import {
  ComputeBudgetProgram,
  Connection,
  Keypair,
  PublicKey,
  TransactionInstruction,
  TransactionMessage,
  VersionedTransaction,
  ParsedAccountData,
} from "@solana/web3.js";
import { exec } from "child_process";
import { promisify } from "util";
import {
  PUMP_SDK,
  OnlinePumpSdk,
  bondingCurvePda,
  getBuyTokenAmountFromSolAmount,
} from "@pump-fun/pump-sdk";
import {
  OnlinePumpAmmSdk,
  PUMP_AMM_SDK,
  canonicalPumpPoolPda,
} from "@pump-fun/pump-swap-sdk";
import {
  createAssociatedTokenAccountIdempotentInstruction,
  createTransferInstruction,
  getAssociatedTokenAddressSync,
} from "@solana/spl-token";
import {
  BundlerMetrics,
  FetchResult,
  GmgnSecurityResponse,
  BuyOptions,
  SellOptions,
  SellQuote,
  SellResult,
  ServiceConfig,
} from "./types";

const execAsync = promisify(exec);
const bs58 = require("bs58") as { decode(value: string): Buffer };
const BN = require("bn.js");

const log = createLogger("GMGN");

// ── Retry config ──────────────────────────────────────────────────────────────

const MAX_RETRIES = 3;
const BASE_RETRY_MS = 1_000;
const REQUEST_TIMEOUT = 15_000; // ms
const BLOCKED_RETRY_MS = 60_000;
const JUPITER_ORDER_RETRIES = 5;
const JUPITER_SELL_RETRIES = 5;
const SOL_MINT = "So11111111111111111111111111111111111111112";
const PUMPPORTAL_TRADE_URL = "https://pumpportal.fun/api/trade";
const PUMPPORTAL_STATUS_CHECKPOINTS_MS = [300, 800, 1_500, 3_000, 5_000];
const TOKEN_PROGRAM_ID = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const TOKEN_2022_PROGRAM_ID = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";
const TOKEN_PROGRAM_IDS = [
  new PublicKey(TOKEN_PROGRAM_ID),
  new PublicKey(TOKEN_2022_PROGRAM_ID),
];

type PumpTradeVenue = "bonding_curve" | "pump_swap" | "unknown";
type PumpPortalTradeAction = "buy" | "sell";
type PumpPortalSignatureState =
  | { status: "confirmed"; error: null }
  | { status: "failed"; error: unknown }
  | { status: "unknown"; error: null };

// ── Helper: sleep ─────────────────────────────────────────────────────────────

const sleep = (ms: number): Promise<void> =>
  new Promise((r) => setTimeout(r, ms));

// ── GmgnClient ────────────────────────────────────────────────────────────────

export class GmgnClient {
  private readonly baseUrl: string;
  private readonly apiKey: string;
  private readonly jupiterSwapBaseUrl: string;
  private readonly jupiterApiKey: string;
  private readonly jupiterPriceApiKey: string;
  private readonly pumpPortalApiKey: string | null;
  private readonly pumpPortalWalletAddress: string | null;
  private readonly connection: Connection;
  private readonly chain = "sol";
  private readonly limiter: RateLimiter;
  private readonly fallbackApiKey: string | null;
  private readonly fallbackLimiter: RateLimiter | null;
  private readonly baselineMinTime: number;
  private readonly fetchMode: "auto" | "direct" | "cli";
  private readonly pumpSdk: OnlinePumpSdk;
  private readonly pumpAmmSdk: OnlinePumpAmmSdk;
  private readonly tradingKeypair: Keypair | null = null;

  constructor(
    config: ServiceConfig,
    limiter: RateLimiter,
    rpcUrlOverride?: string,
    fallbackApiKey?: string,
    fallbackLimiter?: RateLimiter,
  ) {
    this.baseUrl = config.gmgnApiBaseUrl.replace(/\/$/, "");
    this.apiKey = config.gmgnApiKey;
    this.jupiterSwapBaseUrl = config.jupiterSwapBaseUrl.replace(/\/$/, "");
    this.jupiterApiKey = config.jupiterApiKey;
    this.jupiterPriceApiKey = config.jupiterPriceApiKey;
    this.pumpPortalApiKey = config.pumpPortalApiKey;
    this.pumpPortalWalletAddress = config.pumpPortalWalletAddress;
    this.connection = new Connection(
      rpcUrlOverride || config.solanaRpcUrl,
      "confirmed",
    );
    this.pumpSdk = new OnlinePumpSdk(this.connection);
    this.pumpAmmSdk = new OnlinePumpAmmSdk(this.connection);
    this.limiter = limiter;
    this.fallbackApiKey =
      fallbackApiKey && fallbackApiKey !== this.apiKey ? fallbackApiKey : null;
    this.fallbackLimiter = this.fallbackApiKey
      ? (fallbackLimiter ?? limiter)
      : null;
    this.baselineMinTime = config.rateLimitMinTime;
    this.fetchMode = config.gmgnFetchMode;

    const jupPrivKey = process.env.JUPITER_PRIVATE_KEY;
    if (jupPrivKey) {
      try {
        if (jupPrivKey.startsWith("[")) {
          this.tradingKeypair = Keypair.fromSecretKey(
            Uint8Array.from(JSON.parse(jupPrivKey)),
          );
        } else {
          this.tradingKeypair = Keypair.fromSecretKey(bs58.decode(jupPrivKey));
        }
        log.info(
          `Trading keypair loaded for ${this.tradingKeypair.publicKey.toBase58()}`,
        );
      } catch (err) {
        log.error("Failed to load JUPITER_PRIVATE_KEY", err);
      }
    }
  }

  // ── Public: fetch Market Cap (Primary: GMGN API, Secondary: Jupiter + RPC) ──

  async fetchTokenMarketCapUsd(mint: string): Promise<number | null> {
    this.validateSolAddress(mint, "mint");

    // 1. PRIMARY: GMGN (CLI or API)
    try {
      let data: Record<string, unknown> | null = null;
      if (this.fetchMode !== "direct") {
        log.debug(`Attempting MC fetch via GMGN CLI: ${mint}`);
        // CLI calls are local and shouldn't be bottlenecked by API rate limits
        data = await this.fetchCliData("token", mint);
      }

      if (!data) {
        log.debug(`Attempting MC fetch via GMGN API: ${mint}`);
        data = await this.limiter.schedule(() =>
          this.fetchRawTokenData("v1/token/info", mint),
        );
      }

      if (data) {
        const marketCap = this.extractMarketCapUsd(data);
        if (marketCap !== null) {
          log.debug(
            `Calculated MC via GMGN ${data.source === "cli" ? "CLI" : "API"} primary: $${marketCap.toLocaleString()}`,
            { mint },
          );
          return marketCap;
        } else {
          log.debug(
            `Extracted MC was null from GMGN ${data.source === "cli" ? "CLI" : "API"} data`,
            { mint },
          );
        }
      }
    } catch (err) {
      log.debug(`GMGN primary MC fetch failed for ${mint}`, {
        error: String(err),
      });
    }

    // 2. SECONDARY: Jupiter Price API + RPC Supply
    try {
      log.info(`Using Jupiter + RPC secondary for MC: ${mint}`);

      // Fetch Supply from RPC
      const supplyResp = await this.connection.getTokenSupply(
        new PublicKey(mint),
        "confirmed",
      );
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
        method: "GET",
        headers: {
          Accept: "application/json",
          "x-api-key": this.jupiterPriceApiKey,
        },
        signal: controller.signal,
      });
      clearTimeout(timer);

      if (resp.ok) {
        const json = (await resp.json()) as Record<string, any>;
        const priceData = json[mint];
        if (priceData && priceData.usdPrice) {
          const priceUsd = parseFloat(priceData.usdPrice);
          if (!isNaN(priceUsd)) {
            const marketCap = supply * priceUsd;
            log.info(
              `Calculated MC via Jupiter + RPC secondary: $${marketCap.toLocaleString()}`,
              {
                mint,
                supply,
                priceUsd,
              },
            );
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
    this.validateSolAddress(mint, "mint");

    try {
      let data: Record<string, unknown> | null = null;
      if (this.fetchMode !== "direct") {
        data = await this.fetchCliData("token", mint);
      }

      if (!data) {
        data = await this.limiter.schedule(() =>
          this.fetchRawTokenData("v1/token/info", mint),
        );
      }

      if (!data) return null;

      const athPrice =
        this.parseNullableNumber(data.ath_price) ??
        this.parseNullableNumber(this.asRecord(data.stat).ath_price) ??
        this.parseNullableNumber(this.asRecord(data.token).ath_price) ??
        this.parseNullableNumber(this.asRecord(data.price).ath_price);

      if (athPrice === null || athPrice <= 0) return null;

      const supply =
        this.parseNullableNumber(
          data.circulating_supply ?? data.total_supply,
        ) ??
        this.parseNullableNumber(
          this.asRecord(data.token).circulating_supply ??
            this.asRecord(data.token).total_supply,
        );

      if (supply === null) return null;

      const athMc = athPrice * supply;
      if (athMc > 0) {
        log.debug(`Calculated ATH MC: $${athMc.toLocaleString()}`, {
          mint,
          athPrice,
          supply,
        });
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
    const traders = await this.fetchTokenTraders(mint, 100, "profit");
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

    const trader = list.find(
      (entry: { address?: string }) => entry.address === walletAddress,
    );
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
      const solMint = "So11111111111111111111111111111111111111112";
      const url = `https://api.jup.ag/price/v3?ids=${solMint}`;
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), 5000);

      const resp = await fetch(url, {
        method: "GET",
        headers: {
          Accept: "application/json",
          "x-api-key": this.jupiterPriceApiKey,
        },
        signal: controller.signal,
      });
      clearTimeout(timer);

      if (resp.ok) {
        const json = (await resp.json()) as Record<string, any>;
        const priceData = json[solMint];
        if (priceData && priceData.usdPrice) {
          const priceUsd = parseFloat(priceData.usdPrice);
          if (!isNaN(priceUsd)) {
            log.debug(`Fetched SOL price: $${priceUsd.toFixed(2)}`);
            return priceUsd;
          }
        }
      }
      log.warn("Could not fetch SOL price from Jupiter");
      return null;
    } catch (err) {
      log.error("Failed to fetch SOL price", err);
      return null;
    }
  }

  async fetchCreatorHoldRate(mint: string): Promise<number | null> {
    this.validateSolAddress(mint, "mint");

    try {
      let data: Record<string, unknown> | null = null;
      if (this.fetchMode !== "direct") {
        data = await this.fetchCliData("token", mint);
      }

      if (!data) {
        data = await this.limiter.schedule(() =>
          this.fetchRawTokenData("v1/token/info", mint),
        );
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

  async fetchBuyVolumeTraders(mint: string, limit: number = 50): Promise<any> {
    this.validateSolAddress(mint, "mint");

    try {
      if (this.fetchMode !== "direct") {
        const data = await this.fetchCliData("traders", mint, {
          limit,
          orderBy: "buy_volume_cur",
        });
        if (data && this.hasTraderEntries(data)) return data;
        if (data) {
          log.info(
            `GMGN CLI buy-volume traders returned no trader rows for ${mint}, falling back to API`,
            {
              shape: this.describeResponseShape(data),
            },
          );
        } else {
          log.debug(
            `GMGN CLI buy-volume traders returned no data for ${mint}, falling back to API`,
          );
        }
      }

      const endpoint = `v1/token/traders/sol/${mint}?limit=${limit}&orderby=buy_volume_cur&direction=desc`;
      const data = await this.limiter.schedule(() =>
        this.fetchRawTokenData(endpoint, mint),
      );
      return data;
    } catch (err) {
      log.error(`Failed to fetch buy-volume traders for ${mint}`, err);
      return null;
    }
  }

  async fetchBundlerTraders(mint: string, limit: number = 20): Promise<any> {
    this.validateSolAddress(mint, "mint");

    try {
      if (this.fetchMode !== "direct") {
        const data = await this.fetchCliData("traders", mint, {
          limit,
          orderBy: "buy_volume_cur",
          tag: "bundler",
        });
        if (data && this.hasTraderEntries(data)) return data;
        if (data) {
          log.info(
            `GMGN CLI bundler traders returned no trader rows for ${mint}, falling back to API`,
            {
              shape: this.describeResponseShape(data),
            },
          );
        } else {
          log.debug(
            `GMGN CLI bundler traders returned no data for ${mint}, falling back to API`,
          );
        }
      }

      const endpoint = `v1/token/traders/sol/${mint}?limit=${limit}&tag=bundler&orderby=buy_volume_cur&direction=desc`;
      const data = await this.limiter.schedule(() =>
        this.fetchRawTokenData(endpoint, mint),
      );
      return data;
    } catch (err) {
      log.error(`Failed to fetch bundler traders for ${mint}`, err);
      return null;
    }
  }

  async fetchTokenTraders(
    mint: string,
    limit: number = 50,
    orderBy:
      | "profit"
      | "profit_change"
      | "last_active"
      | "buy_volume_cur" = "profit",
    tag: "all" | "bundler" = "all",
  ): Promise<any> {
    this.validateSolAddress(mint, "mint");

    try {
      if (this.fetchMode !== "direct") {
        // CLI calls are local and shouldn't be bottlenecked by API rate limits
        const data = await this.fetchCliData("traders", mint, {
          limit,
          orderBy,
          tag,
        });
        if (data && this.hasTraderEntries(data)) return data;
        if (data) {
          log.info(
            `GMGN CLI traders returned no trader rows for ${mint}, falling back to API`,
            {
              shape: this.describeResponseShape(data),
            },
          );
        } else {
          log.debug(
            `GMGN CLI traders returned no data for ${mint}, falling back to API`,
          );
        }
      }

      const endpoint = `v1/token/traders/sol/${mint}?limit=${limit}&tag=${tag}&orderby=${orderBy}&direction=desc`;
      const data = await this.limiter.schedule(() =>
        this.fetchRawTokenData(endpoint, mint),
      );
      return data;
    } catch (err) {
      log.error(
        `Failed to fetch token traders via GMGN ${this.fetchMode.toUpperCase()} for ${mint}`,
        err,
      );
      return null;
    }
  }

  async fetchBundlerMetrics(mint: string): Promise<FetchResult> {
    this.validateSolAddress(mint, "mint");

    try {
      let data: Record<string, unknown> | null = null;
      if (this.fetchMode !== "direct") {
        // CLI calls are local and shouldn't be bottlenecked by API rate limits
        data = await this.fetchCliData("security", mint);
      }

      if (!data) {
        data = await this.limiter.schedule(() =>
          this.fetchRawTokenData("v1/token/security", mint),
        );
      }

      if (!data) {
        return {
          success: false,
          error: `Empty response or error from GMGN ${this.fetchMode.toUpperCase()}`,
        };
      }

      const metrics: BundlerMetrics = {
        mint,
        timestamp: new Date().toISOString(),
        bundlersPercent: this.parsePercentage(
          data.bundler_trader_amount_rate ?? data.bundled_amount_rate,
        ),
        bundlersCount: this.parseNullableNumber(
          data.bundle_num ?? data.bundler_count,
        ),
        initialBaseReserve: this.parseNullableNumber(data.initial_base_reserve),
        topWallets: this.parseNullableNumber(data.top_wallets),
        top10HolderRate: this.parsePercentage(data.top_10_holder_rate),
        bundledAmountRate: this.parseNullableNumber(
          data.bundled_amount_rate ?? data.bundler_trader_amount_rate,
        ),
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
    type: "token" | "traders" | "security",
    mint: string,
    options: { limit?: number; orderBy?: string; tag?: string } = {},
  ): Promise<Record<string, unknown> | null> {
    // Map internal types to CLI subcommands
    let subcommand = "";
    switch (type) {
      case "token":
        subcommand = "info";
        break;
      case "traders":
        subcommand = "traders";
        break;
      case "security":
        subcommand = "security";
        break;
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
      const { stdout, stderr } = await execAsync(cmd, {
        timeout: REQUEST_TIMEOUT,
      });
      if (stderr) {
        log.debug(`GMGN CLI ${type} stderr for ${mint}: ${stderr}`);
      }

      if (!stdout || stdout.trim() === "") {
        log.warn(`GMGN CLI ${type} returned empty stdout for ${mint}`);
        return null;
      }

      // Find the first '{' and last '}' to extract the JSON part in case there's log noise
      const start = stdout.indexOf("{");
      const end = stdout.lastIndexOf("}");
      if (start === -1 || end === -1 || end < start) {
        log.warn(
          `GMGN CLI ${type} output does not contain valid JSON for ${mint}. Raw: ${stdout.substring(0, 200)}`,
        );
        return null;
      }
      const jsonStr = stdout.substring(start, end + 1);
      const json = JSON.parse(jsonStr);
      const unwrapped = this.unwrapResponseData(json);

      if (!unwrapped) {
        log.warn(
          `GMGN CLI ${type} could not unwrap data for ${mint}. Raw: ${stdout.substring(0, 200)}`,
        );
        return null;
      }

      unwrapped.source = "cli";
      return unwrapped;
    } catch (err) {
      log.debug(`GMGN CLI ${type} failed for ${mint}`, { error: String(err) });
      return null;
    }
  }

  private async fetchRawTokenData(
    endpoint: string,
    mint: string,
  ): Promise<Record<string, unknown> | null> {
    const separator = endpoint.includes("?") ? "&" : "?";
    const url = `${this.baseUrl}/${endpoint}${separator}chain=${this.chain}&address=${mint}`;

    const primary = await this.fetchRawTokenDataWithKey(
      url,
      this.apiKey,
      this.limiter,
      "primary",
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
        "fallback",
      ),
    );
    return fallback.data;
  }

  private async fetchRawTokenDataWithKey(
    url: string,
    apiKey: string,
    limiter: RateLimiter,
    keyRole: "primary" | "fallback",
  ): Promise<{
    data: Record<string, unknown> | null;
    shouldFallback: boolean;
    reason: string;
  }> {
    let lastReason = "unknown GMGN request failure";

    for (let attempt = 1; attempt <= MAX_RETRIES; attempt += 1) {
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), REQUEST_TIMEOUT);

      try {
        const resp = await fetch(url, {
          method: "GET",
          headers: {
            "X-API-KEY": apiKey,
            Accept: "application/json",
            "User-Agent": "gmgn-monitor/1.0",
          },
          signal: controller.signal,
        });

        if (resp.status === 429) {
          const retryMs = this.getRetryDelayMs(resp) ?? BLOCKED_RETRY_MS;
          limiter.onRateLimited(retryMs);
          return {
            data: null,
            shouldFallback: keyRole === "primary",
            reason: "HTTP 429 rate limited",
          };
        }

        if (resp.status === 401 || resp.status === 403) {
          return {
            data: null,
            shouldFallback: keyRole === "primary",
            reason: `HTTP ${resp.status}`,
          };
        }

        if (!resp.ok) {
          lastReason = `HTTP ${resp.status}`;
          if (resp.status >= 500 && attempt < MAX_RETRIES) {
            log.warn(`GMGN ${keyRole} transient HTTP failure; retrying`, {
              status: resp.status,
              attempt,
              maxAttempts: MAX_RETRIES,
            });
            await sleep(BASE_RETRY_MS * attempt);
            continue;
          }
          return {
            data: null,
            shouldFallback: keyRole === "primary" && resp.status >= 500,
            reason: lastReason,
          };
        }

        const contentType =
          resp.headers.get("content-type")?.toLowerCase() ?? "";
        const body = await resp.text();
        const trimmedBody = body.trimStart();
        const looksLikeHtml =
          contentType.includes("text/html") ||
          /^<!doctype html/i.test(trimmedBody) ||
          /^<html[\s>]/i.test(trimmedBody);

        if (looksLikeHtml) {
          lastReason = `HTML response instead of JSON${contentType ? ` (${contentType})` : ""}`;
          if (attempt < MAX_RETRIES) {
            log.warn(
              `GMGN ${keyRole} returned HTML instead of JSON; retrying`,
              {
                attempt,
                maxAttempts: MAX_RETRIES,
                responsePreview: trimmedBody.slice(0, 120),
              },
            );
            await sleep(BASE_RETRY_MS * attempt);
            continue;
          }
          return {
            data: null,
            shouldFallback: keyRole === "primary",
            reason: lastReason,
          };
        }

        let json: GmgnSecurityResponse;
        try {
          json = JSON.parse(body) as GmgnSecurityResponse;
        } catch {
          lastReason = `Non-JSON response (${contentType || "unknown content-type"})`;
          if (attempt < MAX_RETRIES) {
            log.warn(`GMGN ${keyRole} returned malformed JSON; retrying`, {
              attempt,
              maxAttempts: MAX_RETRIES,
              responsePreview: trimmedBody.slice(0, 120),
            });
            await sleep(BASE_RETRY_MS * attempt);
            continue;
          }
          return {
            data: null,
            shouldFallback: keyRole === "primary",
            reason: lastReason,
          };
        }

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
          unwrapped.source = keyRole === "primary" ? "api" : "api-fallback";
        }
        return {
          data: unwrapped,
          shouldFallback: false,
          reason: unwrapped ? "success" : "empty response",
        };
      } catch (err) {
        lastReason = err instanceof Error ? err.message : String(err);
        const retryable =
          lastReason.includes("AbortError") ||
          lastReason.includes("fetch failed");
        if (retryable && attempt < MAX_RETRIES) {
          log.warn(`GMGN ${keyRole} network request failed; retrying`, {
            attempt,
            maxAttempts: MAX_RETRIES,
            reason: lastReason,
          });
          await sleep(BASE_RETRY_MS * attempt);
          continue;
        }
        return {
          data: null,
          shouldFallback: keyRole === "primary",
          reason: lastReason,
        };
      } finally {
        clearTimeout(timer);
      }
    }

    return {
      data: null,
      shouldFallback: keyRole === "primary",
      reason: lastReason,
    };
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
    const price =
      this.parseNullableNumber(this.asRecord(data.price).price) ??
      this.parseNullableNumber(data.price) ??
      this.parseNullableNumber(this.asRecord(data.token).price);

    const supply =
      this.parseNullableNumber(data.circulating_supply ?? data.total_supply) ??
      this.parseNullableNumber(
        this.asRecord(data.token).circulating_supply ??
          this.asRecord(data.token).total_supply,
      );

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
    percent: number,
  ): Promise<SellQuote> {
    const balance = await this.getTokenBalance(walletAddress, mint);
    if (balance === 0n) throw new Error(`No token balance found for ${mint}`);

    const amount = (balance * BigInt(Math.round(percent))) / 100n;
    const url = `${this.jupiterSwapBaseUrl}/quote?inputMint=${mint}&outputMint=${SOL_MINT}&amount=${amount}&slippageBps=50`;

    const json = await this.fetchJupiterJson(url, "GET");
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
    options: BuyOptions,
  ): Promise<SellResult> {
    const venue = await this.detectPumpTradeVenue(mint);
    return this.executePumpPortalLightningTrade(
      "buy",
      walletAddress,
      mint,
      options,
      venue,
    );
  }

  async sellTokenForSol(
    walletAddress: string,
    mint: string,
    options: SellOptions & { preFetchedBalance?: bigint },
  ): Promise<SellResult> {
    const venue = await this.detectPumpTradeVenue(mint);
    try {
      return await this.executePumpPortalLightningTrade(
        "sell",
        walletAddress,
        mint,
        options,
        venue,
        options.preFetchedBalance,
      );
    } catch (err) {
      const balanceAfterLightningFailure = await this.getTokenBalance(
        walletAddress,
        mint,
      ).catch(() => null);
      if (balanceAfterLightningFailure === 0n) {
        log.warn(
          "PumpPortal Lightning sell failed but wallet token balance is zero; treating sell as completed",
          {
            mint,
            walletAddress,
            venue,
            error: err instanceof Error ? err.message : String(err),
          },
        );
        return this.pumpPortalResult(
          "sell",
          mint,
          null,
          options,
          options.preFetchedBalance ?? 0n,
          0n,
          venue,
          0,
          "lightning-error-balance-zero",
        );
      }

      log.warn("PumpPortal Lightning sell failed; trying direct backup sell", {
        mint,
        walletAddress,
        venue,
        balanceAfterLightningFailure:
          balanceAfterLightningFailure?.toString() ?? null,
        error: err instanceof Error ? err.message : String(err),
      });
      return this.sellTokenForSolViaDirectBackup(
        walletAddress,
        mint,
        options,
        venue,
        err,
        balanceAfterLightningFailure ?? undefined,
      );
    }
  }

  private async executePumpPortalLightningTrade(
    action: PumpPortalTradeAction,
    walletAddress: string,
    mint: string,
    options: BuyOptions | SellOptions,
    venue: PumpTradeVenue,
    preFetchedBalance?: bigint,
  ): Promise<SellResult> {
    if (!this.pumpPortalApiKey) {
      throw new Error(
        "PUMPPORTAL_API_KEY is required for Lightning buy/sell execution",
      );
    }
    if (!this.pumpPortalWalletAddress) {
      throw new Error(
        "PUMPPORTAL_WALLET_ADDRESS is required for Lightning balance verification",
      );
    }
    if (walletAddress !== this.pumpPortalWalletAddress) {
      throw new Error(
        `TRADING_WALLET_ADDRESS must match PUMPPORTAL_WALLET_ADDRESS before Lightning trading (received ${walletAddress})`,
      );
    }

    this.validateSolAddress(walletAddress, "PumpPortal Lightning wallet");
    this.validateSolAddress(mint, "mint");

    const balanceBefore = await this.getTokenBalance(walletAddress, mint).catch(
      (err) => {
        if (action === "buy" && this.isMissingMintTokenAccountLookup(err)) {
          log.warn(
            "PumpPortal buy precheck could not read token balance because mint lookup is not ready; continuing as zero balance",
            {
              mint,
              walletAddress,
              error: err instanceof Error ? err.message : String(err),
            },
          );
          return 0n;
        }
        throw err;
      },
    );
    if (
      preFetchedBalance !== undefined &&
      preFetchedBalance !== balanceBefore
    ) {
      log.info(`Ignoring stale cached token balance before PumpPortal sell`, {
        mint,
        cachedBalance: preFetchedBalance.toString(),
        liveBalance: balanceBefore.toString(),
      });
    }
    if (action === "buy" && balanceBefore > 0n) {
      log.info(`PumpPortal buy skipped because wallet already holds ${mint}`, {
        walletAddress,
        balance: balanceBefore.toString(),
      });
      return this.pumpPortalResult(
        action,
        mint,
        null,
        options,
        balanceBefore,
        balanceBefore,
        venue,
        0,
        "already-held",
      );
    }
    if (action === "sell" && balanceBefore === 0n) {
      log.info(`PumpPortal sell skipped because token balance is already zero`, {
        walletAddress,
        mint,
      });
      return this.pumpPortalResult(
        action,
        mint,
        null,
        options,
        balanceBefore,
        0n,
        venue,
        0,
        "already-sold",
      );
    }

    let lastSignature: string | null = null;
    let lastState: PumpPortalSignatureState = {
      status: "unknown",
      error: null,
    };

    for (let attempt = 1; attempt <= 2; attempt++) {
      try {
        lastSignature = await this.submitPumpPortalLightningTrade(
          action,
          mint,
          options,
          venue,
        );
      } catch (submitError) {
        const balanceAfter = await this.getTokenBalance(walletAddress, mint).catch(
          (err) => {
            if (action === "buy" && this.isMissingMintTokenAccountLookup(err)) {
              return 0n;
            }
            throw err;
          },
        );
        const balanceProvesCompletion =
          action === "buy"
            ? balanceAfter > 0n
            : (options as SellOptions).percent >= 100
              ? balanceAfter === 0n
              : balanceAfter < balanceBefore;
        if (balanceProvesCompletion) {
          log.warn(
            `PumpPortal Lightning ${action} request errored but wallet balance proves completion`,
            {
              mint,
              attempt,
              error:
                submitError instanceof Error
                  ? submitError.message
                  : String(submitError),
              balanceBefore: balanceBefore.toString(),
              balanceAfter: balanceAfter.toString(),
            },
          );
          return this.pumpPortalResult(
            action,
            mint,
            null,
            options,
            balanceBefore,
            balanceAfter,
            venue,
            attempt,
            "request-error-balance-recovered",
          );
        }
        if (attempt === 1) {
          log.warn(`Retrying PumpPortal Lightning ${action} once after request error`, {
            mint,
            error:
              submitError instanceof Error
                ? submitError.message
                : String(submitError),
            balance: balanceAfter.toString(),
          });
          continue;
        }
        throw submitError;
      }
      lastState = await this.waitForPumpPortalSignature(lastSignature);

      if (lastState.status === "confirmed") {
        const balanceAfter = await this.getTokenBalance(
          walletAddress,
          mint,
        ).catch(() => balanceBefore);
        log.info(`PumpPortal Lightning ${action} confirmed`, {
          mint,
          signature: lastSignature,
          attempt,
          pool: this.pumpPortalPoolForVenue(venue),
          balanceBefore: balanceBefore.toString(),
          balanceAfter: balanceAfter.toString(),
        });
        return this.pumpPortalResult(
          action,
          mint,
          lastSignature,
          options,
          balanceBefore,
          balanceAfter,
          venue,
          attempt,
          "signature-confirmed",
        );
      }

      const balanceAfter = await this.getTokenBalance(walletAddress, mint).catch(
        (err) => {
          if (action === "buy" && this.isMissingMintTokenAccountLookup(err)) {
            return 0n;
          }
          throw err;
        },
      );
      const balanceProvesCompletion =
        action === "buy"
          ? balanceAfter > 0n
          : (options as SellOptions).percent >= 100
            ? balanceAfter === 0n
            : balanceAfter < balanceBefore;

      if (balanceProvesCompletion) {
        log.warn(
          `PumpPortal Lightning ${action} recovered from wallet balance`,
          {
            mint,
            signature: lastSignature,
            signatureStatus: lastState.status,
            attempt,
            balanceBefore: balanceBefore.toString(),
            balanceAfter: balanceAfter.toString(),
          },
        );
        return this.pumpPortalResult(
          action,
          mint,
          lastSignature,
          options,
          balanceBefore,
          balanceAfter,
          venue,
          attempt,
          "balance-recovered",
        );
      }

      if (attempt === 1) {
        log.warn(`Retrying PumpPortal Lightning ${action} once`, {
          mint,
          signature: lastSignature,
          signatureStatus: lastState.status,
          signatureError: lastState.error,
          balance: balanceAfter.toString(),
        });
      }
    }

    throw new Error(
      `PumpPortal Lightning ${action} failed after one retry for ${mint}` +
        (lastSignature ? ` (last signature ${lastSignature})` : "") +
        (lastState.status === "failed"
          ? `: ${JSON.stringify(lastState.error)}`
          : ": signature remained unknown and wallet balance did not change"),
    );
  }

  private async submitPumpPortalLightningTrade(
    action: PumpPortalTradeAction,
    mint: string,
    options: BuyOptions | SellOptions,
    venue: PumpTradeVenue,
  ): Promise<string> {
    const amount =
      action === "buy"
        ? (options as BuyOptions).solAmount
        : `${Math.min(Math.max((options as SellOptions).percent, 0), 100)}%`;
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), REQUEST_TIMEOUT);

    try {
      const response = await fetch(
        `${PUMPPORTAL_TRADE_URL}?api-key=${encodeURIComponent(this.pumpPortalApiKey!)}`,
        {
          method: "POST",
          headers: {
            Accept: "application/json",
            "Content-Type": "application/json",
          },
          body: JSON.stringify({
            action,
            mint,
            amount,
            denominatedInSol: action === "buy" ? "true" : "false",
            slippage: options.autoSlippage ? 30 : options.slippage,
            priorityFee: options.priorityFeeSol,
            pool: this.pumpPortalPoolForVenue(venue),
            skipPreflight: "false",
          }),
          signal: controller.signal,
        },
      );
      const text = await response.text();
      let data: unknown = text;
      try {
        data = JSON.parse(text);
      } catch {
        // PumpPortal may return the signature as plain text.
      }

      if (!response.ok) {
        throw new Error(
          `PumpPortal API ${response.status}: ${this.pumpPortalErrorMessage(data)}`,
        );
      }

      const signature = this.extractPumpPortalSignature(data);
      if (!signature) {
        throw new Error(
          `PumpPortal returned no transaction signature: ${this.pumpPortalErrorMessage(data)}`,
        );
      }
      log.info(`PumpPortal Lightning ${action} submitted`, {
        mint,
        signature,
        amount,
        pool: this.pumpPortalPoolForVenue(venue),
      });
      return signature;
    } finally {
      clearTimeout(timer);
    }
  }

  private async waitForPumpPortalSignature(
    signature: string,
  ): Promise<PumpPortalSignatureState> {
    const startedAt = Date.now();
    for (const checkpointMs of PUMPPORTAL_STATUS_CHECKPOINTS_MS) {
      const remainingMs = checkpointMs - (Date.now() - startedAt);
      if (remainingMs > 0) await sleep(remainingMs);

      const response = await this.connection
        .getSignatureStatuses([signature], { searchTransactionHistory: true })
        .catch(() => null);
      const status = response?.value[0] ?? null;
      if (status?.err) return { status: "failed", error: status.err };
      if (
        status?.confirmationStatus === "confirmed" ||
        status?.confirmationStatus === "finalized"
      ) {
        return { status: "confirmed", error: null };
      }
    }
    return { status: "unknown", error: null };
  }

  private pumpPortalPoolForVenue(venue: PumpTradeVenue): "pump" | "auto" {
    return venue === "bonding_curve" ? "pump" : "auto";
  }

  private extractPumpPortalSignature(data: unknown): string | null {
    if (typeof data === "string") {
      const trimmed = data.trim().replace(/^"|"$/g, "");
      return /^[1-9A-HJ-NP-Za-km-z]{64,88}$/.test(trimmed) ? trimmed : null;
    }
    if (!data || typeof data !== "object") return null;
    const record = data as Record<string, unknown>;
    for (const value of [
      record.signature,
      record.txSignature,
      record.transactionSignature,
      record.txid,
    ]) {
      if (
        typeof value === "string" &&
        /^[1-9A-HJ-NP-Za-km-z]{64,88}$/.test(value)
      ) {
        return value;
      }
    }
    return null;
  }

  private pumpPortalErrorMessage(data: unknown): string {
    if (typeof data === "string") return data.slice(0, 500);
    if (!data || typeof data !== "object") return String(data);
    const record = data as Record<string, unknown>;
    const message = record.error ?? record.errors ?? record.message ?? data;
    return typeof message === "string"
      ? message.slice(0, 500)
      : JSON.stringify(message).slice(0, 500);
  }

  private pumpPortalResult(
    action: PumpPortalTradeAction,
    mint: string,
    signature: string | null,
    options: BuyOptions | SellOptions,
    balanceBefore: bigint,
    balanceAfter: bigint,
    venue: PumpTradeVenue,
    attempt: number,
    verification: string,
  ): SellResult {
    const isBuy = action === "buy";
    const filledTokenAmount = isBuy
      ? balanceAfter > balanceBefore
        ? balanceAfter - balanceBefore
        : 0n
      : balanceBefore > balanceAfter
        ? balanceBefore - balanceAfter
        : 0n;
    return {
      orderId: null,
      hash: signature,
      status: "confirmed",
      inputToken: isBuy ? SOL_MINT : mint,
      outputToken: isBuy ? mint : SOL_MINT,
      soldPercent: isBuy ? 100 : (options as SellOptions).percent,
      filledInputAmount: isBuy
        ? String(Math.floor((options as BuyOptions).solAmount * 1e9))
        : filledTokenAmount.toString(),
      filledOutputAmount: isBuy ? filledTokenAmount.toString() : null,
      raw: {
        route: "pumpportal-lightning",
        action,
        pool: this.pumpPortalPoolForVenue(venue),
        venue,
        attempt,
        verification,
        balanceBefore: balanceBefore.toString(),
        balanceAfter: balanceAfter.toString(),
      },
    };
  }

  private async sellTokenForSolViaPumpSwap(
    walletAddress: string,
    mint: string,
    amountRaw: bigint,
    options: SellOptions,
  ): Promise<SellResult> {
    if (!this.tradingKeypair)
      throw new Error("No JUPITER_PRIVATE_KEY configured");

    const user = new PublicKey(walletAddress);
    const mintPk = new PublicKey(mint);
    const tokenAccounts = (
      await this.getTokenAccountsWithBalance(user, mintPk)
    ).sort((a, b) =>
      a.balance === b.balance ? 0 : a.balance > b.balance ? -1 : 1,
    );
    if (tokenAccounts.length === 0) {
      throw new Error(
        `No token accounts with balance found for PumpSwap sell: ${mint}`,
      );
    }

    const pool = canonicalPumpPoolPda(mintPk, new PublicKey(SOL_MINT));
    let lastError: unknown = null;

    for (const sourceTokenAccount of tokenAccounts) {
      try {
        const amountRawForAccount =
          (sourceTokenAccount.balance * BigInt(Math.round(options.percent))) /
          100n;
        if (amountRawForAccount <= 0n) continue;

        const swapState = await this.pumpAmmSdk.swapSolanaState(
          pool,
          user,
          sourceTokenAccount.account,
        );
        const slippage = 100;
        const instructions = await PUMP_AMM_SDK.sellInstructions(
          swapState,
          new BN(amountRawForAccount.toString()),
          new BN(0),
        );

        const computeUnitLimit = 350_000;
        const priorityFeeLamports = Math.floor(options.priorityFeeSol * 1e9);
        if (priorityFeeLamports > 0) {
          instructions.unshift(
            ComputeBudgetProgram.setComputeUnitLimit({
              units: computeUnitLimit,
            }),
            ComputeBudgetProgram.setComputeUnitPrice({
              microLamports: Math.max(
                1,
                Math.floor(
                  (priorityFeeLamports * 1_000_000) / computeUnitLimit,
                ),
              ),
            }),
          );
        }

        const latestBlockhash =
          await this.connection.getLatestBlockhash("confirmed");
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
          { maxRetries: 3, timeoutMs: 6_000 },
        );

        log.info(
          `Custom direct PumpSwap sell transaction confirmed: ${signature}`,
          {
            mint,
            pool: pool.toBase58(),
            amount: amountRawForAccount.toString(),
            walletTotalRequestedAmount: amountRaw.toString(),
            slippage,
            tokenProgram: sourceTokenAccount.tokenProgram.toBase58(),
            sourceTokenAccount: sourceTokenAccount.account.toBase58(),
          },
        );

        return {
          orderId: null,
          hash: signature,
          status: "confirmed",
          inputToken: mint,
          outputToken: SOL_MINT,
          soldPercent: options.percent,
          filledInputAmount: amountRawForAccount.toString(),
          filledOutputAmount: null,
          raw: {
            route: "pump-swap-custom-direct",
            pool: pool.toBase58(),
            walletTotalRequestedAmount: amountRaw.toString(),
            slippage,
            tokenProgram: sourceTokenAccount.tokenProgram.toBase58(),
            sourceTokenAccount: sourceTokenAccount.account.toBase58(),
          },
        };
      } catch (err) {
        lastError = err;
        log.warn(`PumpSwap AMM sell attempt failed for ${mint}`, {
          pool: pool.toBase58(),
          tokenProgram: sourceTokenAccount.tokenProgram.toBase58(),
          sourceTokenAccount: sourceTokenAccount.account.toBase58(),
          slippage: 100,
          minQuoteAmountOut: "0",
          error: err instanceof Error ? err.message : String(err),
        });
      }
    }

    throw lastError ?? new Error(`PumpSwap AMM sell failed for ${mint}`);
  }

  private async sellTokenForSolViaDirectBackup(
    walletAddress: string,
    mint: string,
    options: SellOptions & { preFetchedBalance?: bigint },
    venue: PumpTradeVenue,
    lightningError: unknown,
    knownBalance?: bigint,
  ): Promise<SellResult> {
    const balanceBefore = knownBalance ?? (await this.getTokenBalance(walletAddress, mint));
    if (balanceBefore <= 0n) {
      return this.pumpPortalResult(
        "sell",
        mint,
        null,
        options,
        options.preFetchedBalance ?? balanceBefore,
        0n,
        venue,
        0,
        "direct-backup-already-sold",
      );
    }

    const routes =
      venue === "bonding_curve"
        ? (["pump", "pump_swap"] as const)
        : (["pump_swap", "pump"] as const);
    let lastError: unknown = lightningError;

    for (const route of routes) {
      try {
        const result =
          route === "pump_swap"
            ? await this.sellTokenForSolViaPumpSwap(
                walletAddress,
                mint,
                balanceBefore,
                options,
              )
            : await this.sellTokenForSolViaPump(
                walletAddress,
                mint,
                balanceBefore,
                options,
              );
        result.raw = {
          ...result.raw,
          backupAfterPumpPortalLightningFailure: true,
          lightningError:
            lightningError instanceof Error
              ? lightningError.message
              : String(lightningError),
        };
        log.warn("Direct backup sell confirmed after PumpPortal Lightning failure", {
          mint,
          walletAddress,
          route,
          signature: result.hash,
        });
        return result;
      } catch (err) {
        lastError = err;
        const recoveredBalance = await this.getTokenBalance(
          walletAddress,
          mint,
        ).catch(() => balanceBefore);
        if (recoveredBalance === 0n) {
          log.warn("Direct backup sell errored but token balance is zero; treating sell as completed", {
            mint,
            walletAddress,
            route,
            error: err instanceof Error ? err.message : String(err),
          });
          return {
            orderId: null,
            hash:
              err instanceof Error
                ? (err.message.match(/Transaction ([1-9A-HJ-NP-Za-km-z]+)/)?.[1] ?? null)
                : null,
            status: "confirmed",
            inputToken: mint,
            outputToken: SOL_MINT,
            soldPercent: options.percent,
            filledInputAmount: balanceBefore.toString(),
            filledOutputAmount: null,
            raw: {
              route: `${route}-direct-backup-balance-recovered`,
              backupAfterPumpPortalLightningFailure: true,
              lightningError:
                lightningError instanceof Error
                  ? lightningError.message
                  : String(lightningError),
              backupError: err instanceof Error ? err.message : String(err),
              balanceBefore: balanceBefore.toString(),
              balanceAfter: recoveredBalance.toString(),
            },
          };
        }
        log.warn("Direct backup sell route failed; trying next route if available", {
          mint,
          walletAddress,
          route,
          balanceAfter: recoveredBalance.toString(),
          error: err instanceof Error ? err.message : String(err),
        });
      }
    }

    throw new Error(
      `PumpPortal Lightning sell failed and direct backup sell failed for ${mint}: ${
        lastError instanceof Error ? lastError.message : String(lastError)
      }`,
    );
  }

  private async sellTokenForSolViaPump(
    walletAddress: string,
    mint: string,
    amountRaw: bigint,
    options: SellOptions,
  ): Promise<SellResult> {
    if (!this.tradingKeypair)
      throw new Error("No JUPITER_PRIVATE_KEY configured");

    const user = new PublicKey(walletAddress);
    const mintPk = new PublicKey(mint);
    const tokenAccounts = (
      await this.getTokenAccountsWithBalance(user, mintPk)
    ).sort((a, b) =>
      a.balance === b.balance ? 0 : a.balance > b.balance ? -1 : 1,
    );
    if (tokenAccounts.length === 0) {
      throw new Error(`No token accounts with balance found for ${mint}`);
    }

    const global = await this.pumpSdk.fetchGlobal();
    let lastError: unknown = null;

    for (const sourceTokenAccount of tokenAccounts) {
      const tokenProgram = sourceTokenAccount.tokenProgram;
      try {
        const amountRawForProgram =
          (sourceTokenAccount.balance * BigInt(Math.round(options.percent))) /
          100n;
        if (amountRawForProgram <= 0n) {
          throw new Error(
            `No token balance found in ${sourceTokenAccount.account.toBase58()}`,
          );
        }
        const amount = new BN(amountRawForProgram.toString());

        const bondingCurveAccountInfo = await this.connection.getAccountInfo(
          bondingCurvePda(mintPk),
          "confirmed",
        );
        if (!bondingCurveAccountInfo) {
          throw new Error(`Bonding curve account not found for mint: ${mint}`);
        }

        const bondingCurve = PUMP_SDK.decodeBondingCurve(
          bondingCurveAccountInfo,
        );
        if (bondingCurve.complete) {
          throw new Error(
            `Pump.fun bonding curve is complete for ${mint}; token has migrated`,
          );
        }

        const grossSolAmount = amount
          .mul(bondingCurve.virtualQuoteReserves)
          .div(bondingCurve.virtualTokenReserves.add(amount));
        if (grossSolAmount.lte(new BN(0))) {
          throw new Error(
            "Direct Pump.fun reserve quote returned 0 SOL; curve has no liquidity",
          );
        }

        // Leave room for Pump protocol/creator fees before applying configured slippage.
        const quotedSolAmount = grossSolAmount.muln(9_500).divn(10_000);
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

        if (
          sourceTokenAccount &&
          !sourceTokenAccount.account.equals(associatedUser)
        ) {
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
            ComputeBudgetProgram.setComputeUnitLimit({
              units: computeUnitLimit,
            }),
            ComputeBudgetProgram.setComputeUnitPrice({
              microLamports: Math.max(
                1,
                Math.floor(
                  (priorityFeeLamports * 1_000_000) / computeUnitLimit,
                ),
              ),
            }),
          );
        }

        const latestBlockhash =
          await this.connection.getLatestBlockhash("confirmed");
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

        log.info(
          `Custom direct Pump.fun sell transaction confirmed: ${signature}`,
          {
            mint,
            amount: amountRawForProgram.toString(),
            walletTotalRequestedAmount: amountRaw.toString(),
            grossSolLamports: grossSolAmount.toString(),
            quotedSolLamports: quotedSolAmount.toString(),
            slippage,
            tokenProgram: tokenProgram.toBase58(),
            sourceTokenAccount:
              sourceTokenAccount?.account.toBase58() ??
              associatedUser.toBase58(),
            sourceTokenBalance: sourceTokenAccount.balance.toString(),
            associatedUser: associatedUser.toBase58(),
            associatedBondingCurve: associatedBondingCurve.toBase58(),
            setupInstructionCount: setupInstructions.length,
          },
        );

        return {
          orderId: null,
          hash: signature,
          status: "confirmed",
          inputToken: mint,
          outputToken: SOL_MINT,
          soldPercent: options.percent,
          filledInputAmount: amountRawForProgram.toString(),
          filledOutputAmount: quotedSolAmount.toString(),
          raw: {
            route: "pump.fun-custom-direct",
            walletTotalRequestedAmount: amountRaw.toString(),
            grossSolLamports: grossSolAmount.toString(),
            quotedSolLamports: quotedSolAmount.toString(),
            slippage,
            tokenProgram: tokenProgram.toBase58(),
            sourceTokenAccount:
              sourceTokenAccount?.account.toBase58() ??
              associatedUser.toBase58(),
            sourceTokenBalance: sourceTokenAccount.balance.toString(),
            associatedUser: associatedUser.toBase58(),
            associatedBondingCurve: associatedBondingCurve.toBase58(),
            setupInstructionCount: setupInstructions.length,
          },
        };
      } catch (err) {
        lastError = err;
        log.warn(
          `Pump.fun sell attempt failed with token program ${tokenProgram.toBase58()} for ${mint}`,
          {
            error: err instanceof Error ? err.message : String(err),
          },
        );
      }
    }

    throw lastError ?? new Error(`Pump.fun sell failed for ${mint}`);
  }

  private isPumpOnlySellFailure(message: string): boolean {
    return (
      message.includes("NotEnoughTokensToSell") ||
      message.includes("IncorrectProgramId") ||
      message.includes("Transaction ") ||
      message.includes("token balance remains") ||
      message.includes("was not confirmed")
    );
  }

  // ── Jupiter Helpers ───────────────────────────────────────────────────────

  private signVersionedTransaction(transactionBase64: string): string {
    if (!this.tradingKeypair) throw new Error("No trading keypair");

    const tx = VersionedTransaction.deserialize(
      Buffer.from(transactionBase64, "base64"),
    );
    tx.sign([this.tradingKeypair]);
    return Buffer.from(tx.serialize()).toString("base64");
  }

  private async executeSwapTransaction(
    swapTransactionBase64: string,
  ): Promise<string> {
    // This method is now legacy as V2 uses /execute with requestId
    throw new Error(
      "Use fetchJupiterJson(/execute) for V2 Meta-Aggregator path",
    );
  }

  private async executeJupiterOrder(
    signedTransaction: string,
    requestId: string,
  ): Promise<Record<string, unknown>> {
    return this.fetchJupiterJson(`${this.jupiterSwapBaseUrl}/execute`, "POST", {
      signedTransaction,
      requestId,
    });
  }

// ── Helius Sender endpoint (closest to Ashburn VA = Newark) ──────────────────
private readonly SENDER_URL = "http://ewr-sender.helius-rpc.com/fast";

private async sendRawTransactionAndAssertSuccess(
  rawTx: Buffer,
  mint: string,
  options: { maxRetries?: number; timeoutMs?: number } = {},
): Promise<string> {
  const rawTxBase64 = rawTx.toString("base64");
  let signature: string;

  // ── Send via Helius Sender (with fallback to normal RPC) ─────────────────
  try {
    const res = await fetch(this.SENDER_URL, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        jsonrpc: "2.0",
        id: Date.now().toString(),
        method: "sendTransaction",
        params: [
          rawTxBase64,
          {
            encoding: "base64",
            skipPreflight: true,
            maxRetries: 0,
          },
        ],
      }),
    });

    const json = await res.json() as { result?: string; error?: { message: string } };

    if (json.error) throw new Error(json.error.message);
    if (!json.result) throw new Error("Helius Sender returned no signature");

    signature = json.result;
    log.debug(`Sent via Helius Sender: ${signature}`, { mint });
  } catch (senderErr) {
    // Fallback: 429 / 503 / network timeout → use normal RPC
    log.warn(`Helius Sender failed, falling back to RPC: ${senderErr instanceof Error ? senderErr.message : String(senderErr)}`, { mint });
    signature = await this.connection.sendRawTransaction(rawTx, {
      skipPreflight: true,
      maxRetries: options.maxRetries ?? 3,
    });
  }

  // ── Confirm via normal RPC ────────────────────────────────────────────────
  const deadline = Date.now() + (options.timeoutMs ?? 8_000);
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
    await sleep(100);
  }

  const recentBlockhash =
    VersionedTransaction.deserialize(rawTx).message.recentBlockhash;
  throw new Error(
    `Transaction ${signature} was not confirmed for ${mint} before timeout (blockhash ${recentBlockhash})`,
  );
}

  // REPLACE lines 725-745 with:
  private async fetchJupiterJson(
    url: string,
    method: "GET" | "POST",
    body?: Record<string, unknown>,
  ): Promise<Record<string, unknown>> {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), REQUEST_TIMEOUT);
    const headers: Record<string, string> = {
      Accept: "application/json",
      "x-api-key": this.jupiterApiKey,
    };
    if (method === "POST") headers["Content-Type"] = "application/json";

    try {
      const resp = await fetch(url, {
        method,
        headers,
        body: body ? JSON.stringify(body) : undefined,
        signal: controller.signal,
      });
      const json = (await resp.json()) as Record<string, unknown>;
      if (!resp.ok) {
        log.error(
          `Jupiter API Error [${resp.status}]: ${JSON.stringify(json)}`,
          { url, method },
        );
        throw new Error(
          `Jupiter API failed: ${resp.status}${json.error ? ` - ${json.error}` : ""}${json.message ? ` - ${json.message}` : ""}`,
        );
      }
      // Jupiter Ultra returns HTTP 200 with errorCode in body on routing failure — catch it here
      if (json.errorCode !== undefined) {
        throw new Error(
          `Jupiter routing error (${json.errorCode}): ${json.errorMessage ?? json.error ?? JSON.stringify(json)}`,
        );
      }
      return json;
    } finally {
      clearTimeout(timer);
    }
  }

  private parseJupiterSellResult(
    order: Record<string, unknown>,
    execute: Record<string, unknown>,
    mint: string,
    soldPercent: number,
  ): SellResult {
    const orderAny = order as any;
    const executeAny = execute as any;
    return {
      orderId: String(orderAny.requestId),
      hash: String(executeAny.signature),
      status: this.normalizeJupiterStatus(String(executeAny.status)),
      inputToken: mint,
      outputToken: SOL_MINT,
      soldPercent,
      filledInputAmount: String(
        executeAny.inputAmountResult || orderAny.inAmount,
      ),
      filledOutputAmount: String(
        executeAny.outputAmountResult || orderAny.outAmount,
      ),
      raw: { order, execute },
    };
  }

  private normalizeJupiterStatus(status: string): string {
    return status.toLowerCase() === "success"
      ? "confirmed"
      : status.toLowerCase();
  }

  private toJupiterSlippageBps(
    options: BuyOptions | SellOptions,
  ): number | null {
    if (options.autoSlippage) return null;
    // Treat slippage as a percentage (e.g., 0.3 means 0.3%, 10 means 10%)
    const bps = Math.round(options.slippage * 100);
    return Math.min(Math.max(bps, 0), 10_000);
  }

  private toPumpSlippagePercent(options: BuyOptions | SellOptions): number {
    if (options.autoSlippage) return 30;
    // Pump SDK expects percent-style slippage, same as SELL_SLIPPAGE.
    return Math.min(Math.max(options.slippage, 0), 100);
  }

  private async detectPumpTradeVenue(mint: string): Promise<PumpTradeVenue> {
    const mintPk = new PublicKey(mint);
    const bondingCurve = bondingCurvePda(mintPk);
    const pumpSwapPool = canonicalPumpPoolPda(mintPk, new PublicKey(SOL_MINT));

    try {
      const [bondingCurveInfo, pumpSwapPoolInfo] =
        await this.connection.getMultipleAccountsInfo(
          [bondingCurve, pumpSwapPool],
          "confirmed",
        );

      if (bondingCurveInfo) {
        const decoded = PUMP_SDK.decodeBondingCurve(bondingCurveInfo);
        const venue: PumpTradeVenue = decoded.complete
          ? "pump_swap"
          : "bonding_curve";
        log.info(`Pump trade venue detected for ${mint}`, {
          mint,
          venue,
          bondingCurveComplete: decoded.complete,
          bondingCurve: bondingCurve.toBase58(),
          pumpSwapPool: pumpSwapPool.toBase58(),
          pumpSwapPoolExists: pumpSwapPoolInfo !== null,
        });
        return venue;
      }

      if (pumpSwapPoolInfo) {
        log.info(`Pump trade venue detected for ${mint}`, {
          mint,
          venue: "pump_swap",
          bondingCurve: bondingCurve.toBase58(),
          bondingCurveExists: false,
          pumpSwapPool: pumpSwapPool.toBase58(),
          pumpSwapPoolExists: true,
        });
        return "pump_swap";
      }

      log.warn(`Could not identify Pump trade venue for ${mint}`, {
        mint,
        bondingCurve: bondingCurve.toBase58(),
        pumpSwapPool: pumpSwapPool.toBase58(),
      });
      return "unknown";
    } catch (err) {
      log.warn(
        `Pump trade venue detection failed for ${mint}; using route fallback order`,
        {
          mint,
          error: err instanceof Error ? err.message : String(err),
        },
      );
      return "unknown";
    }
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

  async getTokenRawBalance(wallet: string, mint: string): Promise<bigint> {
    return this.getTokenBalance(wallet, mint);
  }

  async getSubmittedBuyReconciliationState(
    wallet: string,
    mint: string,
    signature: string,
    recentBlockhash: string | null,
    callCount: number = 0, // ← add this
  ): Promise<{
    tokenBalance: bigint;
    signatureStatus: "confirmed" | "failed" | "pending";
    signatureError: unknown;
    blockhashValid: boolean | null;
  }> {
    // Check token balance every 4th call (every ~12 s in slow phase, every ~2 s in fast phase)
    const checkBalance = callCount % 4 === 0;
    // Check blockhash every 8th call (~24 s in slow phase) — it takes 90 s to expire
    const checkBlockhash = callCount % 8 === 0;

    const [tokenBalance, statusResponse, blockhashValid] = await Promise.all([
      checkBalance
        ? this.getTokenRawBalance(wallet, mint).catch(() => 0n)
        : Promise.resolve(0n),
      this.connection
        .getSignatureStatuses([signature], { searchTransactionHistory: true })
        .catch(() => null),
      recentBlockhash && checkBlockhash
        ? this.connection
            .isBlockhashValid(recentBlockhash, { commitment: "processed" })
            .catch(() => null)
        : Promise.resolve(null),
    ]);
    const status = statusResponse?.value[0] ?? null;
    return {
      tokenBalance,
      signatureStatus: status?.err
        ? "failed"
        : status?.confirmationStatus
          ? "confirmed"
          : "pending",
      signatureError: status?.err ?? null,
      blockhashValid: blockhashValid?.value ?? null,
    };
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
        if (typeof amount === "string" && /^\d+$/.test(amount)) {
          total += BigInt(amount);
        }
      }
    }

    return total;
  }

  private isMissingMintTokenAccountLookup(error: unknown): boolean {
    const message = error instanceof Error ? error.message : String(error);
    return /could not find mint|invalid param|token program id and mint/i.test(
      message,
    );
  }

  private async getTokenProgramsWithBalance(
    owner: PublicKey,
    mint: PublicKey,
  ): Promise<PublicKey[]> {
    const programs: PublicKey[] = [];
    for (const programId of TOKEN_PROGRAM_IDS) {
      const { value } = await this.connection.getParsedTokenAccountsByOwner(
        owner,
        { programId, mint },
      );
      const hasBalance = value.some(({ account }) => {
        const parsed = account.data as ParsedAccountData;
        const amount = parsed?.parsed?.info?.tokenAmount?.amount;
        return (
          typeof amount === "string" &&
          /^\d+$/.test(amount) &&
          BigInt(amount) > 0n
        );
      });
      if (hasBalance) programs.push(programId);
    }
    return programs;
  }

  private async getTokenAccountsWithBalance(
    owner: PublicKey,
    mint: PublicKey,
  ): Promise<
    Array<{ tokenProgram: PublicKey; account: PublicKey; balance: bigint }>
  > {
    const tokenAccounts: Array<{
      tokenProgram: PublicKey;
      account: PublicKey;
      balance: bigint;
    }> = [];
    for (const programId of TOKEN_PROGRAM_IDS) {
      const { value } = await this.connection.getParsedTokenAccountsByOwner(
        owner,
        { programId, mint },
      );
      for (const entry of value) {
        const parsed = entry.account.data as ParsedAccountData;
        const amount = parsed?.parsed?.info?.tokenAmount?.amount;
        if (typeof amount === "string" && /^\d+$/.test(amount)) {
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
    if (!data || typeof data !== "object") return [];

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
    if (Array.isArray(data)) return { type: "array", length: data.length };
    if (!data || typeof data !== "object") return { type: typeof data };

    const record = data as Record<string, unknown>;
    const dataRecord = this.asRecord(record.data);
    return {
      type: "object",
      keys: Object.keys(record).slice(0, 12),
      listLength: Array.isArray(record.list) ? record.list.length : null,
      tradersLength: Array.isArray(record.traders)
        ? record.traders.length
        : null,
      itemsLength: Array.isArray(record.items) ? record.items.length : null,
      dataKeys: Object.keys(dataRecord).slice(0, 12),
      dataIsArray: Array.isArray(record.data),
      dataLength: Array.isArray(record.data) ? record.data.length : null,
    };
  }

  private async buyTokenWithSolViaPump(
    walletAddress: string,
    mint: string,
    options: BuyOptions,
  ): Promise<SellResult> {
    if (!this.tradingKeypair)
      throw new Error("No JUPITER_PRIVATE_KEY configured");

    const user = new PublicKey(walletAddress);
    const mintPk = new PublicKey(mint);
    const signerPublicKey = this.tradingKeypair.publicKey.toBase58();
    if (walletAddress !== signerPublicKey) {
      log.warn(
        "Buy wallet differs from trading keypair; using trading keypair for custom Pump.fun buy",
        {
          requestedWallet: walletAddress,
          signerPublicKey,
          mint,
        },
      );
    }

    const amountLamports = Math.floor(options.solAmount * 1e9);
    if (amountLamports <= 0) {
      throw new Error(`Custom Pump.fun buy amount is zero for ${mint}`);
    }

    const mintAccountInfo = await this.connection.getAccountInfo(
      mintPk,
      "confirmed",
    );
    if (!mintAccountInfo) {
      throw new Error(`Mint account not found for ${mint}`);
    }
    const tokenProgram = mintAccountInfo.owner;
    if (
      !TOKEN_PROGRAM_IDS.some((programId) => programId.equals(tokenProgram))
    ) {
      throw new Error(
        `Unsupported token program ${tokenProgram.toBase58()} for ${mint}`,
      );
    }

    const [global, feeConfig, supplyResp, buyState] = await Promise.all([
      this.pumpSdk.fetchGlobal(),
      this.pumpSdk.fetchFeeConfig().catch(() => null),
      this.connection.getTokenSupply(mintPk, "confirmed"),
      this.pumpSdk.fetchBuyState(mintPk, user, tokenProgram),
    ]);

    const solAmount = new BN(amountLamports);
    const mintSupply = new BN(supplyResp.value.amount);
    if (buyState.bondingCurve.complete) {
      throw new Error(
        `Pump.fun bonding curve is complete for ${mint}; token has migrated`,
      );
    }
    const tokenAmount = getBuyTokenAmountFromSolAmount({
      global,
      feeConfig,
      mintSupply,
      bondingCurve: buyState.bondingCurve,
      amount: solAmount,
      quoteMint: new PublicKey(SOL_MINT),
    });
    if (tokenAmount.lte(new BN(0))) {
      throw new Error(
        `Custom Pump.fun quote returned 0 tokens for ${mint}; token may be migrated or curve has no liquidity`,
      );
    }

    const slippage = this.toPumpSlippagePercent(options);
    const instructions = await PUMP_SDK.buyInstructions({
      global,
      bondingCurveAccountInfo: buyState.bondingCurveAccountInfo,
      bondingCurve: buyState.bondingCurve,
      associatedUserAccountInfo: buyState.associatedUserAccountInfo,
      mint: mintPk,
      user,
      amount: tokenAmount,
      solAmount,
      slippage,
      tokenProgram,
    });

    const computeUnitLimit = 300_000;
    const priorityFeeLamports = Math.floor(options.priorityFeeSol * 1e9);
    if (priorityFeeLamports > 0) {
      instructions.unshift(
        ComputeBudgetProgram.setComputeUnitLimit({ units: computeUnitLimit }),
        ComputeBudgetProgram.setComputeUnitPrice({
          microLamports: Math.max(
            1,
            Math.floor((priorityFeeLamports * 1_000_000) / computeUnitLimit),
          ),
        }),
      );
    }

    const latestBlockhash =
      await this.connection.getLatestBlockhash("confirmed");
    const tx = new VersionedTransaction(
      new TransactionMessage({
        payerKey: user,
        recentBlockhash: latestBlockhash.blockhash,
        instructions,
      }).compileToV0Message(),
    );
    tx.sign([this.tradingKeypair]);

    try {
      const signature = await this.sendRawTransactionAndAssertSuccess(
        Buffer.from(tx.serialize()),
        mint,
        { maxRetries: 3, timeoutMs: 8_000 },
      );

      log.info(`Custom Pump.fun buy transaction confirmed: ${signature}`, {
        mint,
        solAmount: options.solAmount,
        amountLamports,
        quotedTokenAmount: tokenAmount.toString(),
        slippage,
        priorityFeeSol: options.priorityFeeSol,
        tokenProgram: tokenProgram.toBase58(),
      });

      return {
        orderId: null,
        hash: signature,
        status: "confirmed",
        inputToken: SOL_MINT,
        outputToken: mint,
        soldPercent: 100,
        filledInputAmount: amountLamports.toString(),
        filledOutputAmount: tokenAmount.toString(),
        raw: {
          route: "pump.fun-custom",
          solAmount: options.solAmount,
          amountLamports,
          quotedTokenAmount: tokenAmount.toString(),
          slippage,
          priorityFeeSol: options.priorityFeeSol,
          tokenProgram: tokenProgram.toBase58(),
        },
      };
    } catch (err) {
      const errorMessage = err instanceof Error ? err.message : String(err);
      if (errorMessage.includes("was not confirmed")) {
        const recoveredBalance = await this.getTokenBalance(
          signerPublicKey,
          mint,
        ).catch(() => 0n);
        if (recoveredBalance > 0n) {
          const signature =
            errorMessage.match(/Transaction ([1-9A-HJ-NP-Za-km-z]+)/)?.[1] ??
            null;
          log.warn(
            "Custom Pump.fun buy confirmation timed out but token balance is present; treating buy as confirmed",
            {
              mint,
              signature,
              recoveredBalance: recoveredBalance.toString(),
            },
          );
          return {
            orderId: null,
            hash: signature,
            status: "confirmed",
            inputToken: SOL_MINT,
            outputToken: mint,
            soldPercent: 100,
            filledInputAmount: amountLamports.toString(),
            filledOutputAmount: recoveredBalance.toString(),
            raw: {
              route: "pump.fun-custom-balance-recovered",
              solAmount: options.solAmount,
              amountLamports,
              recoveredBalance: recoveredBalance.toString(),
              slippage,
              priorityFeeSol: options.priorityFeeSol,
              tokenProgram: tokenProgram.toBase58(),
            },
          };
        }
      }
      throw err;
    }
  }

  private async buyTokenWithSolViaPumpSwap(
    walletAddress: string,
    mint: string,
    options: BuyOptions,
  ): Promise<SellResult> {
    if (!this.tradingKeypair)
      throw new Error("No JUPITER_PRIVATE_KEY configured");

    const user = new PublicKey(walletAddress);
    const mintPk = new PublicKey(mint);
    const amountLamports = Math.floor(options.solAmount * 1e9);
    if (amountLamports <= 0) {
      throw new Error(`Custom PumpSwap buy amount is zero for ${mint}`);
    }

    const pool = canonicalPumpPoolPda(mintPk, new PublicKey(SOL_MINT));
    const balanceBefore = await this.getTokenBalance(walletAddress, mint);
    const swapState = await this.pumpAmmSdk.swapSolanaState(pool, user);
    const slippage = 100;
    const instructions = await PUMP_AMM_SDK.buyQuoteInput(
      swapState,
      new BN(amountLamports),
      slippage,
    );

    const computeUnitLimit = 350_000;
    const priorityFeeLamports = Math.floor(options.priorityFeeSol * 1e9);
    if (priorityFeeLamports > 0) {
      instructions.unshift(
        ComputeBudgetProgram.setComputeUnitLimit({ units: computeUnitLimit }),
        ComputeBudgetProgram.setComputeUnitPrice({
          microLamports: Math.max(
            1,
            Math.floor((priorityFeeLamports * 1_000_000) / computeUnitLimit),
          ),
        }),
      );
    }

    const latestBlockhash =
      await this.connection.getLatestBlockhash("confirmed");
    const tx = new VersionedTransaction(
      new TransactionMessage({
        payerKey: user,
        recentBlockhash: latestBlockhash.blockhash,
        instructions,
      }).compileToV0Message(),
    );
    tx.sign([this.tradingKeypair]);

    try {
      const signature = await this.sendRawTransactionAndAssertSuccess(
        Buffer.from(tx.serialize()),
        mint,
        { maxRetries: 3, timeoutMs: 8_000 },
      );
      const balanceAfter = await this.getTokenBalance(
        walletAddress,
        mint,
      ).catch(() => balanceBefore);
      const filledOutputAmount =
        balanceAfter > balanceBefore ? balanceAfter - balanceBefore : 0n;

      log.info(
        `Custom direct PumpSwap buy transaction confirmed: ${signature}`,
        {
          mint,
          pool: pool.toBase58(),
          solAmount: options.solAmount,
          amountLamports,
          filledOutputAmount: filledOutputAmount.toString(),
          slippage,
        },
      );

      return {
        orderId: null,
        hash: signature,
        status: "confirmed",
        inputToken: SOL_MINT,
        outputToken: mint,
        soldPercent: 100,
        filledInputAmount: amountLamports.toString(),
        filledOutputAmount: filledOutputAmount.toString(),
        raw: {
          route: "pump-swap-custom-direct",
          pool: pool.toBase58(),
          solAmount: options.solAmount,
          amountLamports,
          filledOutputAmount: filledOutputAmount.toString(),
          slippage,
        },
      };
    } catch (err) {
      const errorMessage = err instanceof Error ? err.message : String(err);
      if (errorMessage.includes("was not confirmed")) {
        const recoveredBalance = await this.getTokenBalance(
          walletAddress,
          mint,
        ).catch(() => balanceBefore);
        if (recoveredBalance > balanceBefore) {
          const signature =
            errorMessage.match(/Transaction ([1-9A-HJ-NP-Za-km-z]+)/)?.[1] ??
            null;
          const filledOutputAmount = recoveredBalance - balanceBefore;
          log.warn(
            "Custom PumpSwap buy confirmation timed out but token balance increased; treating buy as confirmed",
            {
              mint,
              pool: pool.toBase58(),
              signature,
              filledOutputAmount: filledOutputAmount.toString(),
            },
          );
          return {
            orderId: null,
            hash: signature,
            status: "confirmed",
            inputToken: SOL_MINT,
            outputToken: mint,
            soldPercent: 100,
            filledInputAmount: amountLamports.toString(),
            filledOutputAmount: filledOutputAmount.toString(),
            raw: {
              route: "pump-swap-custom-direct-balance-recovered",
              pool: pool.toBase58(),
              solAmount: options.solAmount,
              amountLamports,
              filledOutputAmount: filledOutputAmount.toString(),
              slippage,
            },
          };
        }
      }
      throw err;
    }
  }

  private getRetryDelayMs(resp: Response): number | undefined {
    const retryAfter = resp.headers.get("Retry-After");
    return retryAfter ? parseInt(retryAfter, 10) * 1000 : undefined;
  }

  private asRecord(value: unknown): Record<string, unknown> {
    return value && typeof value === "object"
      ? (value as Record<string, unknown>)
      : {};
  }

  private parseNullableNumber(value: unknown): number | null {
    const n = Number(value);
    return Number.isFinite(n) ? n : null;
  }
}
