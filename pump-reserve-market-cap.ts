import { AccountInfo, Connection, PublicKey } from "@solana/web3.js";
import { AccountLayout } from "@solana/spl-token";
import {
  PUMP_SDK,
  bondingCurveMarketCap,
  bondingCurvePda,
} from "@pump-fun/pump-sdk";
import {
  OnlinePumpAmmSdk,
  canonicalPumpPoolPda,
} from "@pump-fun/pump-swap-sdk";
import { createLogger } from "./logger";

const BN = require("bn.js");

const log = createLogger("PUMP-RESERVE-MC");
const SOL_MINT = "So11111111111111111111111111111111111111112";
const SOL_DECIMALS = 9;
const REQUEST_TIMEOUT_MS = 1_000;
const SOL_PRICE_CACHE_MS = 15_000;

type PumpReserveMarketCapSource =
  | "Pump bonding curve reserves"
  | "PumpSwap vault reserves";

export type PumpReserveMarketCapResult =
  | {
      ok: true;
      marketCap: number;
      priceUsd: number;
      priceSol: number;
      supply: number;
      source: PumpReserveMarketCapSource;
    }
  | {
      ok: false;
      reason: string;
    };

type MintWatch =
  | {
      venue: "bonding_curve";
      bondingCurve: PublicKey;
      subscriptionIds: number[];
      latestMarketCapSol: number | null;
      supply: number | null;
      source: "Pump bonding curve reserves";
    }
  | {
      venue: "pump_swap";
      pool: PublicKey;
      baseVault: PublicKey;
      quoteVault: PublicKey;
      baseDecimals: number;
      subscriptionIds: number[];
      baseReserve: number | null;
      quoteReserve: number | null;
      supply: number | null;
      source: "PumpSwap vault reserves";
    };

export class PumpReserveMarketCapClient {
  private readonly connection: Connection;
  private readonly pumpAmmSdk: OnlinePumpAmmSdk;
  private readonly watches = new Map<string, Promise<MintWatch>>();
  private solPrice: { value: number; expiresAt: number } | null = null;

  constructor(
    rpcUrl: string,
    wsUrl: string,
    heliusApiKey: string | null | undefined,
  ) {
    this.connection = new Connection(rpcUrl, {
      wsEndpoint: wsUrl,
      commitment: "processed",
    });
    this.pumpAmmSdk = new OnlinePumpAmmSdk(this.connection);
    const key = heliusApiKey?.trim();
    this.heliusDasEndpoint = key
      ? `https://mainnet.helius-rpc.com/?api-key=${encodeURIComponent(key)}`
      : null;
  }

  private readonly heliusDasEndpoint: string | null;

  async fetchMarketCapUsd(
    mint: string,
  ): Promise<PumpReserveMarketCapResult> {
    try {
      const watch = await this.ensureWatch(mint);
      const solPrice = await this.fetchSolPriceUsd();
      if (solPrice === null) {
        return { ok: false, reason: "No SOL/USD price available" };
      }

      if (watch.venue === "bonding_curve") {
        if (watch.latestMarketCapSol === null || watch.supply === null) {
          return {
            ok: false,
            reason: "No Pump bonding-curve reserve value yet",
          };
        }
        const marketCap = watch.latestMarketCapSol * solPrice;
        return {
          ok: true,
          marketCap,
          priceUsd: marketCap / watch.supply,
          priceSol: watch.latestMarketCapSol / watch.supply,
          supply: watch.supply,
          source: watch.source,
        };
      }

      if (
        watch.baseReserve === null ||
        watch.quoteReserve === null ||
        watch.supply === null ||
        watch.baseReserve <= 0 ||
        watch.quoteReserve <= 0
      ) {
        return { ok: false, reason: "No PumpSwap reserve value yet" };
      }

      const priceSol = watch.quoteReserve / watch.baseReserve;
      const marketCapSol = priceSol * watch.supply;
      return {
        ok: true,
        marketCap: marketCapSol * solPrice,
        priceUsd: priceSol * solPrice,
        priceSol,
        supply: watch.supply,
        source: watch.source,
      };
    } catch (err) {
      return {
        ok: false,
        reason: err instanceof Error ? err.message : String(err),
      };
    }
  }

  private ensureWatch(mint: string): Promise<MintWatch> {
    const existing = this.watches.get(mint);
    if (existing) return existing;

    const watchPromise = this.createWatch(mint).catch((err) => {
      this.watches.delete(mint);
      throw err;
    });
    this.watches.set(mint, watchPromise);
    return watchPromise;
  }

  private async createWatch(mint: string): Promise<MintWatch> {
    const mintPk = new PublicKey(mint);
    const bondingCurve = bondingCurvePda(mintPk);
    const pumpSwapPool = canonicalPumpPoolPda(mintPk, new PublicKey(SOL_MINT));
    const [bondingCurveInfo, poolInfo] =
      await this.connection.getMultipleAccountsInfo(
        [bondingCurve, pumpSwapPool],
        "processed",
      );

    if (bondingCurveInfo) {
      const decoded = PUMP_SDK.decodeBondingCurve(bondingCurveInfo);
      if (!decoded.complete) {
        const supply = this.bnToNumber(decoded.tokenTotalSupply, 6);
        const watch: MintWatch = {
          venue: "bonding_curve",
          bondingCurve,
          subscriptionIds: [],
          latestMarketCapSol: this.marketCapSolFromBondingCurve(decoded),
          supply,
          source: "Pump bonding curve reserves",
        };
        const subId = this.connection.onAccountChange(
          bondingCurve,
          (accountInfo) =>
            this.updateBondingCurveWatch(mint, watch, accountInfo),
          "processed",
        );
        watch.subscriptionIds.push(subId);
        log.info("Subscribed to Pump bonding-curve reserve changes", {
          mint,
          bondingCurve: bondingCurve.toBase58(),
          supply,
        });
        return watch;
      }
    }

    if (!poolInfo) {
      throw new Error("No Pump bonding curve or PumpSwap pool account found");
    }

    const swapState = await this.pumpAmmSdk.swapSolanaState(
      pumpSwapPool,
      PublicKey.default,
    );
    const supplyResp = await this.connection.getTokenSupply(mintPk, "processed");
    const supply = supplyResp.value.uiAmount;
    if (!supply || supply <= 0) {
      throw new Error("No valid token supply for PumpSwap market cap");
    }

    const watch: MintWatch = {
      venue: "pump_swap",
      pool: pumpSwapPool,
      baseVault: swapState.pool.poolBaseTokenAccount,
      quoteVault: swapState.pool.poolQuoteTokenAccount,
      baseDecimals: swapState.baseMintAccount.decimals,
      subscriptionIds: [],
      baseReserve: this.bnToNumber(
        swapState.poolBaseAmount,
        swapState.baseMintAccount.decimals,
      ),
      quoteReserve: this.bnToNumber(swapState.poolQuoteAmount, SOL_DECIMALS),
      supply,
      source: "PumpSwap vault reserves",
    };

    const baseSubId = this.connection.onAccountChange(
      watch.baseVault,
      (accountInfo) =>
        this.updatePumpSwapVaultReserve(watch, "base", accountInfo),
      "processed",
    );
    const quoteSubId = this.connection.onAccountChange(
      watch.quoteVault,
      (accountInfo) =>
        this.updatePumpSwapVaultReserve(watch, "quote", accountInfo),
      "processed",
    );
    watch.subscriptionIds.push(baseSubId, quoteSubId);
    log.info("Subscribed to PumpSwap vault reserve changes", {
      mint,
      pool: pumpSwapPool.toBase58(),
      baseVault: watch.baseVault.toBase58(),
      quoteVault: watch.quoteVault.toBase58(),
      baseReserve: watch.baseReserve,
      quoteReserve: watch.quoteReserve,
      supply,
    });
    return watch;
  }

  private updateBondingCurveWatch(
    mint: string,
    watch: Extract<MintWatch, { venue: "bonding_curve" }>,
    accountInfo: AccountInfo<Buffer>,
  ): void {
    try {
      const decoded = PUMP_SDK.decodeBondingCurve(accountInfo);
      if (decoded.complete) {
        this.watches.delete(mint);
        void Promise.all(
          watch.subscriptionIds.map((id) =>
            this.connection.removeAccountChangeListener(id).catch(() => undefined),
          ),
        );
        return;
      }
      watch.latestMarketCapSol = this.marketCapSolFromBondingCurve(decoded);
      watch.supply = this.bnToNumber(decoded.tokenTotalSupply, 6);
    } catch (err) {
      log.warn("Failed to decode Pump bonding-curve reserve update", {
        mint,
        error: err instanceof Error ? err.message : String(err),
      });
    }
  }

  private updatePumpSwapVaultReserve(
    watch: Extract<MintWatch, { venue: "pump_swap" }>,
    side: "base" | "quote",
    accountInfo: AccountInfo<Buffer>,
  ): void {
    try {
      const decoded = AccountLayout.decode(accountInfo.data);
      const rawAmount = new BN(decoded.amount.toString());
      if (side === "base") {
        watch.baseReserve = this.bnToNumber(rawAmount, watch.baseDecimals);
      } else {
        watch.quoteReserve = this.bnToNumber(rawAmount, SOL_DECIMALS);
      }
    } catch (err) {
      log.warn("Failed to decode PumpSwap vault reserve update", {
        pool: watch.pool.toBase58(),
        side,
        error: err instanceof Error ? err.message : String(err),
      });
    }
  }

  private marketCapSolFromBondingCurve(bondingCurve: {
    tokenTotalSupply: typeof BN;
    virtualQuoteReserves: typeof BN;
    virtualTokenReserves: typeof BN;
  }): number {
    const marketCapLamports = bondingCurveMarketCap({
      mintSupply: bondingCurve.tokenTotalSupply,
      virtualQuoteReserves: bondingCurve.virtualQuoteReserves,
      virtualTokenReserves: bondingCurve.virtualTokenReserves,
    });
    return this.bnToNumber(marketCapLamports, SOL_DECIMALS);
  }

  private async fetchSolPriceUsd(): Promise<number | null> {
    const now = Date.now();
    if (this.solPrice && this.solPrice.expiresAt > now) {
      return this.solPrice.value;
    }

    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
    try {
      if (!this.heliusDasEndpoint) return null;
      const resp = await fetch(this.heliusDasEndpoint, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          jsonrpc: "2.0",
          id: "pump-reserve-sol-price",
          method: "getAsset",
          params: { id: SOL_MINT },
        }),
        signal: controller.signal,
      });
      if (!resp.ok) return null;
      const json = (await resp.json()) as Record<string, any>;
      const price = Number(
        json.result?.token_info?.price_info?.price_per_token,
      );
      if (!Number.isFinite(price) || price <= 0) return null;
      this.solPrice = { value: price, expiresAt: now + SOL_PRICE_CACHE_MS };
      return price;
    } catch {
      return null;
    } finally {
      clearTimeout(timer);
    }
  }

  private bnToNumber(value: typeof BN, decimals: number): number {
    return Number(value.toString()) / 10 ** decimals;
  }
}
