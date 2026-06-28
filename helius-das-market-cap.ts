import { createLogger } from "./logger";

const log = createLogger("HELIUS-DAS-MC");
const REQUEST_TIMEOUT_MS = 4_000;

type CachedSupply = {
  supply: number;
  supplyRaw: string;
  decimals: number;
};

export type HeliusDasMarketCapResult =
  | {
      ok: true;
      marketCap: number;
      price: number;
      supply: number;
      source: "Helius DAS";
    }
  | {
      ok: false;
      reason: string;
    };

export class HeliusDasMarketCapClient {
  private readonly endpoint: string | null;
  private readonly supplyCache = new Map<string, CachedSupply>();

  constructor(apiKey: string | null | undefined) {
    const key = apiKey?.trim();
    this.endpoint = key
      ? `https://mainnet.helius-rpc.com/?api-key=${encodeURIComponent(key)}`
      : null;
  }

  isConfigured(): boolean {
    return this.endpoint !== null;
  }

  async fetchMarketCapUsd(mint: string): Promise<HeliusDasMarketCapResult> {
    if (!this.endpoint) return { ok: false, reason: "Helius DAS key not configured" };

    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
    try {
      const resp = await fetch(this.endpoint, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          jsonrpc: "2.0",
          id: "insider-token-mc",
          method: "getAsset",
          params: { id: mint },
        }),
        signal: controller.signal,
      });

      const text = await resp.text();
      if (!resp.ok) {
        throw new Error(`Helius DAS getAsset HTTP ${resp.status}: ${text.slice(0, 300)}`);
      }

      let json: unknown;
      try {
        json = JSON.parse(text);
      } catch {
        return { ok: false, reason: "Helius DAS returned malformed JSON" };
      }

      if (this.hasRpcError(json)) {
        const code = json.error.code;
        const message = json.error.message;
        throw new Error(`Helius DAS getAsset RPC error ${code}: ${message}`);
      }

      const tokenInfo = this.getRecord(this.getRecord(json).result)?.token_info;
      const tokenInfoRecord = this.getRecord(tokenInfo);
      const priceInfo = this.getRecord(tokenInfoRecord.price_info);
      const price = Number(priceInfo.price_per_token);
      if (!Number.isFinite(price) || price <= 0) {
        return { ok: false, reason: "No Helius DAS price_info for this token yet" };
      }

      const cached = this.getCachedOrResponseSupply(mint, tokenInfoRecord);
      if (!cached) {
        return { ok: false, reason: "No valid Helius DAS token supply" };
      }

      const marketCap = price * cached.supply;
      if (!Number.isFinite(marketCap) || marketCap <= 0) {
        return { ok: false, reason: "Invalid Helius DAS market cap result" };
      }

      return {
        ok: true,
        marketCap,
        price,
        supply: cached.supply,
        source: "Helius DAS",
      };
    } catch (err) {
      if (err instanceof Error && err.name === "AbortError") {
        throw new Error(`Helius DAS getAsset timed out after ${REQUEST_TIMEOUT_MS}ms`);
      }
      throw err;
    } finally {
      clearTimeout(timer);
    }
  }

  private getCachedOrResponseSupply(
    mint: string,
    tokenInfo: Record<string, unknown>,
  ): CachedSupply | null {
    const cached = this.supplyCache.get(mint);
    if (cached) return cached;

    const supplyRawValue = tokenInfo.supply;
    const decimalsValue = tokenInfo.decimals;
    const supplyRaw =
      typeof supplyRawValue === "string" || typeof supplyRawValue === "number"
        ? String(supplyRawValue)
        : "";
    const decimals = Number(decimalsValue);
    const supplyRawNumber = Number(supplyRaw);
    if (
      !supplyRaw ||
      !Number.isFinite(supplyRawNumber) ||
      supplyRawNumber <= 0 ||
      !Number.isInteger(decimals) ||
      decimals < 0
    ) {
      return null;
    }

    const supply = supplyRawNumber / 10 ** decimals;
    if (!Number.isFinite(supply) || supply <= 0) return null;

    const next = { supply, supplyRaw, decimals };
    this.supplyCache.set(mint, next);
    log.debug("Cached Helius DAS token supply", {
      mint,
      supply,
      supplyRaw,
      decimals,
    });
    return next;
  }

  private getRecord(value: unknown): Record<string, unknown> {
    return value && typeof value === "object"
      ? (value as Record<string, unknown>)
      : {};
  }

  private hasRpcError(
    value: unknown,
  ): value is { error: { code: number | string; message: string } } {
    const error = this.getRecord(value).error;
    const record = this.getRecord(error);
    return (
      "code" in record &&
      typeof record.message === "string" &&
      record.message.length > 0
    );
  }
}
