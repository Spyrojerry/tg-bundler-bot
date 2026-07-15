// ─────────────────────────────────────────────────────────────────────────────
//  index.ts  —  Service entry point
//
//  Boot sequence:
//    1. Load + validate environment config
//    2. Open SQLite database
//    3. Initialise RateLimiter + GmgnClient
//    4. Initialise FunderFirstOrchestrator + insider bot listeners
//    5. Initialise TelegramBot
//    6. Register SIGINT/SIGTERM for graceful shutdown
// ─────────────────────────────────────────────────────────────────────────────

import "dotenv/config";
import { createLogger, setLogLevel } from "./logger";
import { loadConfig } from "./config";
import { MonitorDatabase } from "./database";
import { RateLimiter } from "./rate-limiter";
import { GmgnClient } from "./gmgn-client";
import { WalletMonitor } from "./wallet-monitor";
import {
  InlineKeyboardMarkup,
  TelegramBot,
  TelegramReply,
} from "./telegram-bot";
import { startHealthServer } from "./health-server";
import {
  FilterFailEvent,
  NewTokenEvent,
  SellResult,
  SellQuote,
} from "./types";
import { FunderFirstOrchestrator } from "./funder-first-orchestrator";
import { InsiderBot } from "./insider-bot";
import type { InsiderMintClaimFn } from "./insider-bot";
import { HeliusDasMarketCapClient } from "./helius-das-market-cap";
import { PumpReserveMarketCapClient } from "./pump-reserve-market-cap";
import { HeliusClient } from "./helius-client";
import { HeliusEnhancedWsClient } from "./helius-enhanced-ws";
import type { HeliusCreditExhaustionInfo, HeliusProjectUsage } from "./helius-client";
import { PublicKey } from "@solana/web3.js";
import { randomBytes } from "crypto";

const log = createLogger("MAIN");
const sleep = (ms: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, ms));
const MCAP_CHECK_INTERVAL_MS = 2_000;
const MCAP_FETCH_GRACE_MS = 1_000;
const INSIDER_DAS_NO_PRICE_COOLDOWN_MS = 10_000;
const isHeliusUsageExhaustionError = (err: unknown): boolean => {
  const message =
    err instanceof Error
      ? `${err.name}\n${err.message}\n${err.stack ?? ""}`
      : String(err);
  return (
    /(?:\b429\b|too many requests)/i.test(message) &&
    /(?:max usage reached|-32429)/i.test(message)
  );
};

async function main(): Promise<void> {
  // ── 1. Config ──────────────────────────────────────────────────────────────
  const config = loadConfig();
  setLogLevel(config.logLevel);
  const hasEnvValue = (key: string): boolean =>
    Boolean(process.env[key]?.trim());
  const insiderBotDefinitions = [
    {
      botNumber: 1,
      enabled:
        hasEnvValue("GMGN_API_KEY") &&
        hasEnvValue("INSIDER_HELIUS_API_KEY") &&
        hasEnvValue("INSIDER_SOLANA_RPC_URL") &&
        hasEnvValue("INSIDER_SOLANA_WS_URL"),
      gmgnApiKey: config.gmgnApiKey,
      heliusApiKey: config.insiderHeliusApiKey || config.heliusApiKey,
      heliusProjectId: config.insiderHeliusProjectId,
      rpcUrl: config.insiderSolanaRpcUrl,
      wsUrl: config.insiderSolanaWsUrl,
      followWallet: config.insiderFollowWallet,
    },
    {
      botNumber: 2,
      enabled:
        hasEnvValue("GMGN_API_KEY_2") &&
        hasEnvValue("INSIDER_HELIUS_API_KEY_2") &&
        hasEnvValue("INSIDER_SOLANA_RPC_URL_2") &&
        hasEnvValue("INSIDER_SOLANA_WS_URL_2"),
      gmgnApiKey: config.gmgnApiKey2,
      heliusApiKey: config.insiderHeliusApiKey2 || config.heliusApiKey,
      heliusProjectId: config.insiderHeliusProjectId2,
      rpcUrl: config.insiderSolanaRpcUrl2,
      wsUrl: config.insiderSolanaWsUrl2,
      followWallet: null,
    },
    {
      botNumber: 3,
      enabled:
        hasEnvValue("GMGN_API_KEY_3") &&
        hasEnvValue("INSIDER_HELIUS_API_KEY_3") &&
        hasEnvValue("INSIDER_SOLANA_RPC_URL_3") &&
        hasEnvValue("INSIDER_SOLANA_WS_URL_3"),
      gmgnApiKey: config.gmgnApiKey3,
      heliusApiKey: config.insiderHeliusApiKey3 || config.heliusApiKey,
      heliusProjectId: config.insiderHeliusProjectId3,
      rpcUrl: config.insiderSolanaRpcUrl3,
      wsUrl: config.insiderSolanaWsUrl3,
      followWallet: null,
    },
    {
      botNumber: 4,
      enabled:
        hasEnvValue("GMGN_API_KEY_4") &&
        hasEnvValue("INSIDER_HELIUS_API_KEY_4") &&
        hasEnvValue("INSIDER_SOLANA_RPC_URL_4") &&
        hasEnvValue("INSIDER_SOLANA_WS_URL_4"),
      gmgnApiKey: config.gmgnApiKey4,
      heliusApiKey: config.insiderHeliusApiKey4 || config.heliusApiKey,
      heliusProjectId: config.insiderHeliusProjectId4,
      rpcUrl: config.insiderSolanaRpcUrl4,
      wsUrl: config.insiderSolanaWsUrl4,
      followWallet: null,
    },
  ].filter((definition) => definition.enabled);

  const getInsiderBotNumber = (index: number): number =>
    insiderBotDefinitions[index]?.botNumber ?? index + 1;

  log.info("═══════════════════════════════════════");
  log.info("  GMGN Bundler Monitor  — starting up");
  log.info("═══════════════════════════════════════");
  log.info("Config", {
    wallet: config.walletAddress,
    tradingWallet: config.tradingWalletAddress,

    // Global solana endpoints (used by wallet monitors / default gmgn client)
    rpc: config.solanaRpcUrl,
    ws: config.solanaWsUrl,

    receiverRpc: config.receiverSolanaRpcUrl,
    receiverWs: config.receiverSolanaWsUrl,

    f1Rpc: config.f1SolanaRpcUrl,
    f1Ws: config.f1SolanaWsUrl,

    insiderBots: insiderBotDefinitions.map((definition) => ({
      bot: definition.botNumber,
      rpc: definition.rpcUrl,
      ws: definition.wsUrl,
      followWallet: definition.followWallet,
    })),

    insiderEntryMc: config.insiderEntryMc,
    insiderExitMc: config.insiderExitMc,

    minBuySol: config.minBuySol,
    gmgnFetchMode: config.gmgnFetchMode,
    monitorInterval: config.monitorInterval,
    rateLimitMinTime: config.rateLimitMinTime,
    dbPath: config.dbPath,
  });

  // ── 2. Database ────────────────────────────────────────────────────────────
  const db = await MonitorDatabase.create(config.dbPath);

  // ── 3. Per-bot rate limiters + GMGN clients ────────────────────────────────
  const gmgnLimiters = insiderBotDefinitions.map(
    () =>
      new RateLimiter(config.rateLimitMinTime, config.rateLimitMaxConcurrent),
  );
  const gmgnFallbackLimiter = new RateLimiter(
    config.rateLimitMinTime,
    config.rateLimitMaxConcurrent,
  );
  const gmgnClients = insiderBotDefinitions.map(
    (definition, index) =>
      new GmgnClient(
        { ...config, gmgnApiKey: definition.gmgnApiKey },
        gmgnLimiters[index],
        definition.rpcUrl,
        config.gmgnFallbackApiKey ?? undefined,
        gmgnFallbackLimiter,
      ),
  );
  const insiderDasMarketCapClient = new HeliusDasMarketCapClient(
    config.insiderHeliusApiKey3 ||
      insiderBotDefinitions[2]?.heliusApiKey ||
      config.insiderHeliusApiKey ||
      config.heliusApiKey,
  );
  const insiderPumpReserveMarketCapClient = new PumpReserveMarketCapClient(
    insiderBotDefinitions[2]?.rpcUrl ||
      insiderBotDefinitions[0]?.rpcUrl ||
      config.insiderSolanaRpcUrl ||
      config.solanaRpcUrl,
    insiderBotDefinitions[2]?.wsUrl ||
      insiderBotDefinitions[0]?.wsUrl ||
      config.insiderSolanaWsUrl ||
      config.solanaWsUrl,
    config.insiderHeliusApiKey3 ||
      insiderBotDefinitions[2]?.heliusApiKey ||
      config.insiderHeliusApiKey ||
      config.heliusApiKey,
  );

  const sharedEnhancedWsApiKey =
    config.insiderHeliusApiKey || config.heliusApiKey;
  const sharedEnhancedWs = sharedEnhancedWsApiKey
    ? new HeliusEnhancedWsClient(
        sharedEnhancedWsApiKey,
        "Shared Helius Enhanced WS",
      )
    : null;

  let telegramBot: TelegramBot | null = null;
  const insiderBots: InsiderBot[] = [];
  let activeInsiderIndex = 0; // Insider UI is pinned to bot 1; keys 2-4 provide API capacity.

  // ── 4. Orchestrators ──────────────────────────────────────────────────────
  let funderFirstOrchestrator: FunderFirstOrchestrator;

  const healthServer = startHealthServer(config.port);
  const walletMonitors = new Map<string, WalletMonitor>();
  const pendingTradingBuys = new Map<string, NewTokenEvent>();
  type PendingTelegramAction =
    | { type: "addwallet" | "removewallet" }
    | { type: "minSol"; walletAddress: string }
    | {
        type:
          | "insiderFollowWallet"
          | "insiderBuySol"
          | "insiderNormalBuySol"
          | "insiderLowFundingBuySol"
          | "insiderExitPercent"
          | "insiderBundlerMinUsd"
          | "insiderBundlerMaxUsd";
        index: number;
      }
    | { type: "funderFirstFunderAddress" };
  const pendingTelegramActions = new Map<string, PendingTelegramAction>();

  const pendingSells = new Map<
    string,
    { event: FilterFailEvent; createdAt: number; executing: boolean }
  >();
  const pausedWallets = new Set<string>();
  const walletAliasesByChat = new Map<string, string[]>();
  const activePositionCache = new Map<
    string,
    { balance: bigint; quote: SellQuote | null; timestamp: number }
  >();
  const activePositionRefreshes = new Set<string>();
  const insiderDasNoPriceUntil = new Map<string, number>();
  const handledHeliusUsageStops = new Set<number>();
  let heliusUsageProcessStopRequested = false;
  let requestFullShutdown: ((signal: string) => void) | null = null;

  function html(value: string): string {
    return value
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;");
  }
  type HeliusUsageReportEntry = {
    label: string;
    projectId: string;
    exhausted: boolean;
    usage: HeliusProjectUsage | null;
    remainingPercent: number | null;
    error: string | null;
  };

  function getHeliusRemainingPercent(usage: HeliusProjectUsage): number | null {
    const creditsRemaining = Number(usage.creditsRemaining);
    const prepaidCreditsRemaining = Number(usage.prepaidCreditsRemaining);
    const creditsLimit = Number(usage.subscriptionDetails?.creditsLimit);
    const prepaidCreditsUsed = Number(usage.prepaidCreditsUsed);
    const totalRemaining =
      (Number.isFinite(creditsRemaining) ? creditsRemaining : 0) +
      (Number.isFinite(prepaidCreditsRemaining) ? prepaidCreditsRemaining : 0);
    const totalLimit =
      (Number.isFinite(creditsLimit) && creditsLimit > 0 ? creditsLimit : 0) +
      (Number.isFinite(prepaidCreditsUsed) && prepaidCreditsUsed > 0 ? prepaidCreditsUsed : 0) +
      (Number.isFinite(prepaidCreditsRemaining) && prepaidCreditsRemaining > 0 ? prepaidCreditsRemaining : 0);
    if (totalLimit <= 0) return null;
    return Math.max(0, Math.min(100, (totalRemaining / totalLimit) * 100));
  }

  function formatHeliusUsageReportLine(entry: HeliusUsageReportEntry): string {
    if (!entry.projectId) {
      return `• <b>${html(entry.label)}</b>: project ID missing`;
    }
    if (entry.error || !entry.usage) {
      return `• <b>${html(entry.label)}</b>: usage unavailable (${html(entry.error ?? "unknown error")})`;
    }
    const usage = entry.usage;
    const remainingPercent = entry.remainingPercent === null
      ? "unknown"
      : `${entry.remainingPercent.toFixed(2)}%`;
    const marker = entry.exhausted ? " <b>USED UP</b>" : "";
    return [
      `• <b>${html(entry.label)}</b>${marker}`,
      `Project: <code>${html(entry.projectId)}</code>`,
      `Plan: <b>${html(usage.subscriptionDetails?.plan ?? "Unknown")}</b>`,
      `Credits remaining: <b>${Number(usage.creditsRemaining).toLocaleString()}</b>`,
      `Prepaid remaining: <b>${Number(usage.prepaidCreditsRemaining).toLocaleString()}</b>`,
      `Remaining: <b>${remainingPercent}</b>`,
    ].join("\n  ");
  }

  async function collectInsiderHeliusUsageReport(
    exhaustedInfo?: HeliusCreditExhaustionInfo,
  ): Promise<HeliusUsageReportEntry[]> {
    const definitions = [
      {
        label: "Insider Helius 1",
        apiKey: config.insiderHeliusApiKey || config.heliusApiKey,
        projectId: config.insiderHeliusProjectId,
      },
      {
        label: "Insider Helius 2",
        apiKey: config.insiderHeliusApiKey2,
        projectId: config.insiderHeliusProjectId2,
      },
      {
        label: "Insider Helius 3 / MC",
        apiKey: config.insiderHeliusApiKey3,
        projectId: config.insiderHeliusProjectId3,
      },
      {
        label: "Insider Helius 4",
        apiKey: config.insiderHeliusApiKey4,
        projectId: config.insiderHeliusProjectId4,
      },
    ];

    return await Promise.all(
      definitions.map(async (definition) => {
        if (!definition.apiKey || !definition.projectId) {
          return {
            label: definition.label,
            projectId: definition.projectId,
            exhausted: false,
            usage: null,
            remainingPercent: null,
            error: !definition.apiKey ? "API key missing" : "project ID missing",
          };
        }
        try {
          const usage = await HeliusClient.fetchProjectUsageForProject(
            definition.apiKey,
            definition.projectId,
          );
          const creditsRemaining = Number(usage.creditsRemaining);
          const prepaidCreditsRemaining = Number(usage.prepaidCreditsRemaining);
          const exhausted =
            definition.projectId === exhaustedInfo?.projectId ||
            (Number.isFinite(creditsRemaining) &&
              Number.isFinite(prepaidCreditsRemaining) &&
              creditsRemaining <= 0 &&
              prepaidCreditsRemaining <= 0);
          return {
            label: definition.label,
            projectId: definition.projectId,
            exhausted,
            usage,
            remainingPercent: getHeliusRemainingPercent(usage),
            error: null,
          };
        } catch (err) {
          return {
            label: definition.label,
            projectId: definition.projectId,
            exhausted: definition.projectId === exhaustedInfo?.projectId,
            usage: null,
            remainingPercent: null,
            error: err instanceof Error ? err.message : String(err),
          };
        }
      }),
    );
  }

  function requestHeliusUsageProcessStop(): void {
    if (heliusUsageProcessStopRequested) return;
    heliusUsageProcessStopRequested = true;
    log.error("Helius usage exhausted; stopping whole bot process");
    if (requestFullShutdown) {
      setTimeout(() => requestFullShutdown?.("HELIUS_USAGE_EXHAUSTED"), 250);
      return;
    }
    setTimeout(() => process.exit(1), 1_500);
  }

  async function stopInsiderForHeliusUsageExhaustion(
    botIndex: number,
    err: unknown,
    source: string,
    exhaustedInfo?: HeliusCreditExhaustionInfo,
  ): Promise<void> {
    const bot = insiderBots[botIndex];
    if (!bot) return;
    const botNumber = getInsiderBotNumber(botIndex);
    const firstNotice = !handledHeliusUsageStops.has(botIndex);
    const activePosition = bot.getActivePosition();
    const preBuyMint = bot.getPreBuyMint();
    handledHeliusUsageStops.add(botIndex);
    if (activePosition) {
      log.error(
        `[INSIDER ${botNumber}] Helius usage exhausted while holding; triggering emergency sell before stop`,
        {
          source,
          mint: activePosition.mint,
          followedWallet: activePosition.followedWallet,
          error: err instanceof Error ? err.message : String(err),
        },
      );
      bot.emit("sellTrigger", {
        followedWallet: activePosition.followedWallet,
        positionMint: activePosition.mint,
        signature: "HELIUS_USAGE_EXHAUSTED",
        reason:
          "Emergency sell: Helius key usage exhausted (429 max usage reached)",
      });
    } else if (preBuyMint) {
      log.error(
        `[INSIDER ${botNumber}] Helius usage exhausted during pre-buy/session; resetting and stopping`,
        {
          source,
          mint: preBuyMint,
          error: err instanceof Error ? err.message : String(err),
        },
      );
    }
    await bot.stopForHeliusCredits();
    log.error(`[INSIDER ${botNumber}] Helius usage exhausted`, {
      source,
      activePositionMint: activePosition?.mint ?? null,
      preBuyMint,
      emergencySellTriggered: !!activePosition,
      error: err instanceof Error ? err.message : String(err),
    });

    const usageReport = firstNotice
      ? await collectInsiderHeliusUsageReport(exhaustedInfo)
      : [];
    if (!firstNotice) {
      requestHeliusUsageProcessStop();
      return;
    }

    await telegramBot?.sendDefault(
      [
        `<b>🛑 Insider ${botNumber} Stopped — Helius Usage Exhausted</b>`,
        `Source: <b>${html(source)}</b>`,
        activePosition
          ? `Action: <b>Emergency sell submitted, then bot reset/stopped</b>`
          : preBuyMint
            ? `Action: <b>Session reset, then bot stopped before buy</b>`
            : `Action: <b>Bot stopped</b>`,
        activePosition
          ? `Position token: <code>${html(activePosition.mint)}</code>`
          : preBuyMint
            ? `Session token: <code>${html(preBuyMint)}</code>`
            : "",
        "",
        "A Helius request returned <code>429 max usage reached</code> or the project credits are exhausted.",
        "The bot was reset and stopped to avoid repeated failed requests.",
        "The whole bot process will now shut down.",
        "",
        "<b>Helius API usage</b>",
        ...usageReport.map(formatHeliusUsageReportLine),
        "",
        "Rotate or top up the used-up Helius key, then restart the bot process.",
      ].filter(Boolean).join("\n"),
      { pin: true },
    );
    log.error(`[INSIDER ${botNumber}] Helius usage report before process stop`, {
      source,
      exhaustedProjectId: exhaustedInfo?.projectId ?? null,
      usageReport: usageReport.map((entry) => ({
        label: entry.label,
        projectId: entry.projectId,
        exhausted: entry.exhausted,
        remainingPercent: entry.remainingPercent,
        creditsRemaining: entry.usage?.creditsRemaining ?? null,
        prepaidCreditsRemaining: entry.usage?.prepaidCreditsRemaining ?? null,
        error: entry.error,
      })),
    });
    requestHeliusUsageProcessStop();
  }
  async function followInsiderWalletWithUsageGuard(
    botIndex: number,
    address: string,
    source: string,
  ): Promise<boolean> {
    try {
      await insiderBots[botIndex].followWallet(address);
      return true;
    } catch (err) {
      if (!isHeliusUsageExhaustionError(err)) throw err;
      await stopInsiderForHeliusUsageExhaustion(botIndex, err, source);
      return false;
    }
  }

  async function fetchInsiderMarketCapUsd(
    botIndex: number,
    mint: string,
  ): Promise<{ marketCap: number; source: string } | null> {
    const startedAt = Date.now();
    const remainingGraceMs = (): number =>
      Math.max(1, MCAP_FETCH_GRACE_MS - (Date.now() - startedAt));
    const withGrace = async <T>(task: Promise<T>): Promise<T | null> => {
      let timer: NodeJS.Timeout | null = null;
      try {
        return await Promise.race([
          task,
          new Promise<null>((resolve) => {
            timer = setTimeout(resolve, remainingGraceMs(), null);
          }),
        ]);
      } finally {
        if (timer) clearTimeout(timer);
      }
    };

    const reserve = await withGrace(
      insiderPumpReserveMarketCapClient.fetchMarketCapUsd(mint),
    );
    if (reserve === null) {
      log.debug(
        `[INSIDER ${getInsiderBotNumber(botIndex)} MC] Pump reserve MC missed ${MCAP_FETCH_GRACE_MS}ms grace for ${mint}; skipping this MC tick`,
      );
      return null;
    }
    if (reserve.ok) {
      return { marketCap: reserve.marketCap, source: reserve.source };
    }
    log.debug(
      `[INSIDER ${getInsiderBotNumber(botIndex)} MC] Pump reserve MC unavailable for ${mint}; trying Helius DAS`,
      { reason: reserve.reason },
    );

    const now = Date.now();
    const dasNoPriceUntil = insiderDasNoPriceUntil.get(mint) ?? 0;
    if (insiderDasMarketCapClient.isConfigured() && now >= dasNoPriceUntil) {
      try {
        const das = await withGrace(
          insiderDasMarketCapClient.fetchMarketCapUsd(mint),
        );
        if (das === null) {
          log.debug(
            `[INSIDER ${getInsiderBotNumber(botIndex)} MC] Helius DAS missed ${MCAP_FETCH_GRACE_MS}ms grace for ${mint}; skipping this MC tick`,
          );
          return null;
        }
        if (das.ok) {
          insiderDasNoPriceUntil.delete(mint);
          return { marketCap: das.marketCap, source: das.source };
        }
        if (das.reason.includes("price_info")) {
          insiderDasNoPriceUntil.set(
            mint,
            Date.now() + INSIDER_DAS_NO_PRICE_COOLDOWN_MS,
          );
        }
        log.debug(
          `[INSIDER ${getInsiderBotNumber(botIndex)} MC] Helius DAS unavailable for ${mint}; skipping this MC tick`,
          { reason: das.reason },
        );
        return null;
      } catch (err) {
        if (isHeliusUsageExhaustionError(err)) {
          await stopInsiderForHeliusUsageExhaustion(
            botIndex,
            err,
            "Helius DAS market-cap checker",
          );
          return null;
        }
        log.debug(
          `[INSIDER ${getInsiderBotNumber(botIndex)} MC] Helius DAS MC fetch failed for ${mint}; skipping this MC tick`,
          { error: err instanceof Error ? err.message : String(err) },
        );
        return null;
      }
    }
    return null;
  }

  const handleProcessHeliusUsageExhaustion = (
    err: unknown,
    source: string,
  ): boolean => {
    if (!isHeliusUsageExhaustionError(err)) return false;
    void (async () => {
      const runningIndexes = insiderBots
        .map((bot, index) => ({ bot, index }))
        .filter(({ bot }) => bot.isRunning())
        .map(({ index }) => index);
      const targetIndexes = runningIndexes.length > 0 ? runningIndexes : [0];
      for (const botIndex of targetIndexes) {
        await stopInsiderForHeliusUsageExhaustion(botIndex, err, source);
      }
    })().catch((notifyErr) =>
      log.error("Failed to stop Insider bot after Helius usage exhaustion", {
        source,
        error:
          notifyErr instanceof Error ? notifyErr.message : String(notifyErr),
      }),
    );
    return true;
  };
  process.on("unhandledRejection", (reason) => {
    if (handleProcessHeliusUsageExhaustion(reason, "unhandled rejection")) {
      return;
    }
    log.error("Unhandled promise rejection", reason);
  });
  process.on("uncaughtException", (err) => {
    if (handleProcessHeliusUsageExhaustion(err, "uncaught exception")) return;
    log.error("Uncaught exception", err);
    process.exit(1);
  });

  function hasPendingSellForMint(walletAddress: string, mint: string): boolean {
    for (const pending of pendingSells.values()) {
      if (
        pending.event.walletAddress === walletAddress &&
        pending.event.mint === mint
      ) {
        return true;
      }
    }
    return false;
  }

  async function handleTelegramCommand(
    _chatId: string,
    text: string,
  ): Promise<string | TelegramReply> {
    const chatId = _chatId;
    const [command] = text.split(/\s+/, 1);

    try {
      if (command === "/callback") {
        const [, data] = text.split(/\s+/, 2);
        const parts = data?.split(":") ?? [];
        const [callbackKind, callbackAction, callbackAddress] = parts;

        if (data === "menu:addwallet") {
          pendingTelegramActions.set(chatId, { type: "addwallet" });
          return {
            text: "Send the Solana wallet address to add.",
            trackPrompt: true,
            editCurrent: true,
          };
        }
        if (data === "menu:removewallet") {
          pendingTelegramActions.set(chatId, { type: "removewallet" });
          return {
            text: "Send the Solana wallet address to remove.",
            trackPrompt: true,
            editCurrent: true,
          };
        }
        if (data === "menu:refresh") return homeReply(true);
        if (data.startsWith("r:m:")) {
          const [, , mint, contextCode] = parts;
          let resolvedBotIndex: number | null = null;

          if (contextCode === "i" || contextCode?.startsWith("i")) {
            if (contextCode && contextCode.length > 1) {
              const parsed = parseInt(contextCode.slice(1), 10);
              if (Number.isFinite(parsed)) resolvedBotIndex = parsed;
            }
          }

          let currentMarketCapUsd: number | null = null;
          let athMarketCapUsd: number | null = null;
          let currentPrice: SellQuote | null = null;
          let tokenBalance: bigint | null = null;
          let balanceIsZero = false;
          let quoteError: string | null = null;

          const botIndex =
            resolvedBotIndex ??
            insiderBots.findIndex((b) => b.getActivePosition()?.mint === mint);
          const effectiveBotIndex = botIndex !== -1 ? botIndex : 0;
          const client = gmgnClients[effectiveBotIndex];

          try {
            const cached = activePositionCache.get(mint);
            if (cached) {
              tokenBalance = cached.balance;
              balanceIsZero = cached.balance <= 0n;
            }

            const balancePromise =
              config.tradingWalletAddress && tokenBalance === null
                ? getTokenRawBalance(
                    new PublicKey(config.tradingWalletAddress),
                    new PublicKey(mint),
                  )
                    .then((balance) => {
                      tokenBalance = balance;
                      balanceIsZero = balance <= 0n;
                      return balance;
                    })
                    .catch((err) => {
                      quoteError =
                        err instanceof Error ? err.message : String(err);
                      return null;
                    })
                : Promise.resolve(tokenBalance);

            const quotePromise = config.tradingWalletAddress
              ? client
                  .quoteTokenSellForSol(config.tradingWalletAddress, mint, 100)
                  .catch(async (err) => {
                    const message =
                      err instanceof Error ? err.message : String(err);
                    if (message.includes("No token balance found")) {
                      balanceIsZero = true;
                    } else {
                      quoteError = message;
                    }
                    return null;
                  })
              : Promise.resolve(null);

            [currentMarketCapUsd, athMarketCapUsd, currentPrice] =
              await Promise.all([
                client.fetchTokenMarketCapUsd(mint),
                client.fetchTokenAthMarketCapUsd(mint),
                quotePromise,
              ]);
            await balancePromise;

            if (config.tradingWalletAddress && tokenBalance !== null) {
              activePositionCache.set(mint, {
                balance: tokenBalance,
                quote: currentPrice,
                timestamp: Date.now(),
              });
            }
          } catch (err) {
            log.error(`Failed to refresh position data for ${mint}`, err);
            quoteError = err instanceof Error ? err.message : String(err);
          }

          const bot =
            botIndex !== -1 ? insiderBots[botIndex] : insiderBots[0];
          const entryMc = bot.getEntryMc();
          const exitMc = bot.getExitMc();
          const insiderProfitExitDisabled = bot.isProfitExitDisabled();

          const marketCapPnlPct =
            entryMc !== null && entryMc > 0 && currentMarketCapUsd !== null
              ? ((currentMarketCapUsd - entryMc) / entryMc) * 100
              : null;
          const profitDisplay =
            marketCapPnlPct !== null
              ? `P/L by MC: <b>${marketCapPnlPct >= 0 ? "+" : ""}${marketCapPnlPct.toFixed(2)}%</b>`
              : "P/L by MC: <b>N/A</b>";

          const tokenBalanceLine =
            tokenBalance !== null
              ? `Token Balance: <code>${html(tokenBalance.toString())}</code>`
              : null;

          const refreshContextCode = `i${effectiveBotIndex}`;

          const lines = [
            `<b>Insider ${getInsiderBotNumber(effectiveBotIndex)} Position Update</b>`,
            `Token: <code>${html(mint)}</code>`,
            entryMc !== null
              ? `Entry MC: <b>$${entryMc.toLocaleString()}</b>`
              : null,
            `Market Cap: <b>$${currentMarketCapUsd?.toLocaleString() ?? "Unknown"}</b>`,
            athMarketCapUsd !== null
              ? `ATH MC: <b>$${athMarketCapUsd.toLocaleString()}</b>`
              : null,
            exitMc !== null
              ? `Exit MC: <b>$${exitMc.toLocaleString()}</b>`
              : null,
            insiderProfitExitDisabled
              ? "Exit MC status: <b>Disabled — waiting for recipient sell-all/zero-SOL exit</b>"
              : null,
            profitDisplay,
            tokenBalanceLine,
            "",
            `Last Updated: ${new Date().toISOString()}`,
          ].filter(Boolean) as string[];

          const buttons = [];

          if (!balanceIsZero) {
            buttons.push({
              text: "🔴 Sell Position",
              callback_data: `sell:insider:${mint}:${effectiveBotIndex}`,
            });
            buttons.push({
              text: "🔄 Refresh",
              callback_data: `r:m:${mint}:${refreshContextCode}`,
            });
          } else {
            buttons.push({
              text: "🔄 Refresh",
              callback_data: `r:m:${mint}:${refreshContextCode}`,
            });
          }

          return {
            text: lines.join("\n"),
            replyMarkup: {
              inline_keyboard: [buttons],
            },
            editCurrent: true,
          };
        }
        if (data.startsWith("sell:insider:")) {
          const [, , mint, botIndexStr] = parts;
          const botIndex = parseInt(botIndexStr);
          const bot = insiderBots[botIndex];
          if (!bot) return "Invalid bot index.";

          const activePos = bot.getActivePosition();
          // Even if activePos is missing (state reset), we allow manual sell if it's the right mint
          // or if the user is forcing a sell from an insider card.
          const followedWallet =
            activePos?.followedWallet || bot.getFollowedWallet() || "UNKNOWN";

          // Trigger manual sell
          bot.emit("sellTrigger", {
            followedWallet,
            positionMint: mint,
            signature: "MANUAL",
            reason: "Manual sell requested via Telegram button",
          });

          return "Sell signal sent.";
        }
        if (data === "menu:wallets") return walletsReply(chatId, true);
        if (data === "menu:status") return statusReply(true);
        if (data.startsWith("insider:select:")) {
          activeInsiderIndex = 0;
          return homeReply(true);
        }
        if (data === "insider:follow") {
          activeInsiderIndex = 0;
          pendingTelegramActions.set(chatId, {
            type: "insiderFollowWallet",
            index: 0,
          });
          return {
            text: "Send the one wallet address for Insider Bot to follow.",
            trackPrompt: true,
            editCurrent: true,
          };
        }
        if (data === "funderfirst:funder") {
          pendingTelegramActions.set(chatId, { type: "funderFirstFunderAddress" });
          return {
            text: "Send the top-level feePayer <b>funder</b> wallet address to watch for SOL transfer-outs.",
            trackPrompt: true,
            editCurrent: true,
          };
        }
        if (data === "funderfirst:start") {
          try {
            await funderFirstOrchestrator.start();
          } catch (err) {
            return {
              text: html(err instanceof Error ? err.message : String(err)),
              editCurrent: true,
            };
          }
          return homeReply(true);
        }
        if (data === "funderfirst:stop") {
          funderFirstOrchestrator.stop("Stopped from Telegram");
          return homeReply(true);
        }
        if (data === "insider:buysol") {
          activeInsiderIndex = 0;
          pendingTelegramActions.set(chatId, {
            type: "insiderBuySol",
            index: 0,
          });
          return {
            text: "Send the SOL amount Insider Bot should buy with.",
            trackPrompt: true,
            editCurrent: true,
          };
        }
        if (data === "insider:normalbuysol") {
          activeInsiderIndex = 0;
          pendingTelegramActions.set(chatId, {
            type: "insiderNormalBuySol",
            index: 0,
          });
          return {
            text: "Send the SOL amount Insider Bot should buy with in normal-funding mode.",
            trackPrompt: true,
            editCurrent: true,
          };
        }
        if (data === "insider:lowfundingbuysol") {
          activeInsiderIndex = 0;
          pendingTelegramActions.set(chatId, {
            type: "insiderLowFundingBuySol",
            index: 0,
          });
          return {
            text: "Send the SOL amount Insider Bot should buy with in low-funding mode.",
            trackPrompt: true,
            editCurrent: true,
          };
        }
        if (data === "insider:exitpercent") {
          activeInsiderIndex = 0;
          pendingTelegramActions.set(chatId, {
            type: "insiderExitPercent",
            index: 0,
          });
          return {
            text: "Send the Exit profit percentage increase.\nExample: <code>40</code> for a 40% current MC increase from your entry point.",
            trackPrompt: true,
            editCurrent: true,
          };
        }
        if (data === "insider:bundlermin") {
          return homeReply(true);
        }
        if (data === "insider:bundlermax") {
          return homeReply(true);
        }
        if (data === "insider:togglebuy") {
          activeInsiderIndex = 0;
          const bot = insiderBots[0];
          bot.setBuyDisabled(!bot.isBuyDisabled());
          return homeReply(true);
        }
        if (data === "insider:stop") {
          activeInsiderIndex = 0;
          await insiderBots[0].stop();
          return homeReply(true);
        }
        if (data === "insider:resume") {
          activeInsiderIndex = 0;
          const bot = insiderBots[0];
          const followedWallet = bot.getFollowedWallet();
          if (!followedWallet) {
            pendingTelegramActions.set(chatId, {
              type: "insiderFollowWallet",
              index: 0,
            });
            return {
              text: "Send the one wallet address for Insider Bot to follow.",
              trackPrompt: true,
              editCurrent: true,
            };
          }
          const started = await followInsiderWalletWithUsageGuard(
            0,
            followedWallet,
            "telegram resume",
          );
          if (!started) {
            return "Insider bot stopped because its Helius RPC/WS usage is exhausted.";
          }
          return homeReply(true);
        }

        if (callbackKind === "sell" && callbackAction && callbackAddress) {
          const pending = pendingSells.get(callbackAddress);
          if (!pending) return "This sell request is no longer available.";
          if (callbackAction === "ignore") {
            pendingSells.delete(callbackAddress);
            return "Sell ignored.";
          }
          if (callbackAction === "confirm") {
            if (pending.executing) return "Sell is already being submitted.";
            pending.executing = true;
            if (telegramBot) {
              void executeSellAndNotify(chatId, callbackAddress, telegramBot);
            }
            return [
              "<b>Sell submission started</b>",
              `Token: <code>${html(pending.event.mint)}</code>`,
              `Selling: <b>${config.sellPercent}%</b> for SOL`,
              `Slippage: <b>${config.sellAutoSlippage ? "auto" : config.sellSlippage}</b>`,
              `Priority fee: <b>${config.sellPriorityFeeSol} SOL</b>`,
              `Anti-MEV: <b>${config.sellAntiMev ? "on" : "off"}</b>`,
              "",
              "I will send the receipt here when GMGN returns the order result.",
            ].join("\n");
          }
          return "Invalid sell action.";
        }

        if (
          callbackKind === "set" &&
          callbackAction === "minSol" &&
          callbackAddress
        ) {
          const normalized = new PublicKey(callbackAddress).toBase58();
          pendingTelegramActions.set(chatId, {
            type: "minSol",
            walletAddress: normalized,
          });
          return {
            text: [
              `Send a minimum SOL value for <code>${html(normalized)}</code>.`,
              "Use a number (e.g., 0.01), or send <code>off</code> to use default.",
            ].join("\n"),
            trackPrompt: true,
            editCurrent: true,
          };
        }
        if (callbackKind === "reverse" && callbackAction) {
          if (callbackAddress) {
            const normalized = new PublicKey(callbackAddress).toBase58();
            if (callbackAction === "add") {
              db.addReverseBuyWallet(normalized);
              return settingsReply(normalized, true);
            }
            if (callbackAction === "remove") {
              db.removeReverseBuyWallet(normalized);
              return settingsReply(normalized, true);
            }
          }
          return "Invalid reverse-buy action.";
        }
        if (
          callbackKind === "settings" &&
          callbackAction === "refresh" &&
          callbackAddress
        ) {
          return settingsReply(callbackAddress, true);
        }

        const [kind, action, address, context] = [
          callbackKind,
          callbackAction,
          callbackAddress,
          parts[3],
        ];
        if (kind !== "wallet" || !address) return "Invalid button action.";
        if (action === "add") {
          await startWallet(address);
          return walletSummaryReply(address, true);
        }
        if (action === "remove") {
          stopWallet(address);
          return walletSummaryReply(address, true);
        }
        if (action === "pause") {
          pauseWallet(address);
          return context === "settings"
            ? settingsReply(address, true)
            : walletSummaryReply(address, true);
        }
        if (action === "resume") {
          await resumeWallet(address);
          return context === "settings"
            ? settingsReply(address, true)
            : walletSummaryReply(address, true);
        }
        if (action === "settings") {
          return settingsReply(address, true);
        }
        if (action === "refresh") {
          return walletSummaryReply(address, true);
        }
        return "Invalid wallet action.";
      }

      if (command === "/start" || command === "/help") {
        return homeReply();
      }
      if (command === "/wallets") {
        return walletsReply(chatId);
      }
      if (command.startsWith("/w_")) {
        const index = parseInt(command.substring(3));
        const wallet =
          walletAliasesByChat.get(chatId)?.[index] ??
          walletAliasesByChat.get("__default__")?.[index];
        if (!wallet)
          return "Wallet shortcut not found. Send /wallets to refresh the list.";
        return walletSummaryReply(wallet);
      }
      if (command === "/status") {
        return statusReply();
      }

      if (!text.startsWith("/")) {
        const pendingAction = pendingTelegramActions.get(chatId);
        if (pendingAction) {
          pendingTelegramActions.delete(chatId);
          if (pendingAction.type === "addwallet") {
            return await startWallet(text);
          }
          if (pendingAction.type === "removewallet") {
            return stopWallet(text);
          }
          if (pendingAction.type === "minSol") {
            const message = updateMinSol(pendingAction.walletAddress, text);
            if (!message.startsWith("Updated ")) return message;
            return settingsReply(pendingAction.walletAddress);
          }
          if (pendingAction.type === "insiderFollowWallet") {
            const started = await followInsiderWalletWithUsageGuard(
              pendingAction.index,
              text,
              "telegram follow-wallet setup",
            );
            if (!started) {
              return "Insider bot stopped because its Helius RPC/WS usage is exhausted.";
            }
            return homeReply();
          }
          if (pendingAction.type === "insiderBuySol") {
            const bot = insiderBots[pendingAction.index];
            const value = Number(text.trim());
            if (!Number.isFinite(value) || value <= 0)
              return "Send a SOL amount greater than 0.";
            bot.setBuySol(value);
            log.info(
              `[SETTINGS] Insider ${getInsiderBotNumber(pendingAction.index)} default buy SOL set to ${value}`,
            );
            return homeReply();
          }
          if (pendingAction.type === "insiderNormalBuySol") {
            const bot = insiderBots[pendingAction.index];
            const value = Number(text.trim());
            if (!Number.isFinite(value) || value <= 0)
              return "Send a SOL amount greater than 0.";
            bot.setNormalFundingBuySol(value);
            log.info(
              `[SETTINGS] Insider ${getInsiderBotNumber(pendingAction.index)} normal-funding buy SOL set to ${value}`,
            );
            return homeReply();
          }
          if (pendingAction.type === "insiderLowFundingBuySol") {
            const bot = insiderBots[pendingAction.index];
            const value = Number(text.trim());
            if (!Number.isFinite(value) || value <= 0)
              return "Send a SOL amount greater than 0.";
            bot.setLowFundingBuySol(value);
            log.info(
              `[SETTINGS] Insider ${getInsiderBotNumber(pendingAction.index)} low-funding buy SOL set to ${value}`,
            );
            return homeReply();
          }
          if (pendingAction.type === "insiderExitPercent") {
            const bot = insiderBots[pendingAction.index];
            const value = Number(text.trim());
            if (!Number.isFinite(value) || value < 0)
              return "Send a valid percentage.";
            bot.setExitPercent(value);
            log.info(
              `[SETTINGS] Insider ${getInsiderBotNumber(pendingAction.index)} exit percent set to ${value}%`,
            );
            return homeReply();
          }
          if (pendingAction.type === "insiderBundlerMinUsd") {
            const bot = insiderBots[pendingAction.index];
            const value = Number(text.trim());
            if (!Number.isFinite(value) || value < 0)
              return "Send a valid USD amount.";
            bot.setBundlerBuyMinUsd(value);
            log.info(
              `[SETTINGS] Insider ${getInsiderBotNumber(pendingAction.index)} bundler min USD set to ${value}`,
            );
            return homeReply();
          }
          if (pendingAction.type === "insiderBundlerMaxUsd") {
            const bot = insiderBots[pendingAction.index];
            const value = Number(text.trim());
            if (!Number.isFinite(value) || value < 0)
              return "Send a valid USD amount.";
            bot.setBundlerBuyMaxUsd(value);
            log.info(
              `[SETTINGS] Insider ${getInsiderBotNumber(pendingAction.index)} bundler max USD set to ${value}`,
            );
            return homeReply();
          }
          if (pendingAction.type === "funderFirstFunderAddress") {
            try {
              funderFirstOrchestrator.setFunderAddress(text.trim());
              log.info(
                `[SETTINGS] Funder-first funder address set to ${funderFirstOrchestrator.getFunderAddress()}`,
              );
            } catch (err) {
              return html(err instanceof Error ? err.message : String(err));
            }
            return homeReply();
          }
        }
        const started = await followInsiderWalletWithUsageGuard(
          activeInsiderIndex,
          text,
          "telegram follow-wallet setup",
        );
        if (!started) {
          return "Insider bot stopped because its Helius RPC/WS usage is exhausted.";
        }
        return homeReply();
      }

      return "Unknown command. Send /help.";
    } catch (err) {
      return html(err instanceof Error ? err.message : String(err));
    }
  }

  telegramBot = config.telegramBotToken
    ? new TelegramBot(config, handleTelegramCommand)
    : null;

  function claimInsiderMint(botIndex: number, mint: string): boolean {
    return insiderBots.every((bot, index) => {
      if (index === botIndex) return true;
      if (bot.getPreBuyMint() === mint) return false;
      if (bot.getActivePosition()?.mint === mint) return false;
      return true;
    });
  }

  const makeClaimFn =
    (botIndex: number): InsiderMintClaimFn =>
    (mint: string) =>
      claimInsiderMint(botIndex, mint);

  insiderBotDefinitions.forEach((definition, index) => {
    const bot = new InsiderBot(
      config,
      definition.rpcUrl,
      definition.wsUrl,
      gmgnClients[index],
      definition.heliusApiKey,
      definition.heliusProjectId,
      telegramBot,
      makeClaimFn(index),
      () => undefined,
      `Insider ${definition.botNumber}`,
      sharedEnhancedWs,
    );
    bot.on("heliusCreditsExhausted", (info) => {
      void (async () => {
        log.error(`[INSIDER ${definition.botNumber}] Helius Admin API confirmed credit exhaustion`, {
          projectId: info.projectId,
          plan: info.usage.subscriptionDetails?.plan ?? "Unknown",
          creditsRemaining: info.usage.creditsRemaining,
          prepaidCreditsRemaining: info.usage.prepaidCreditsRemaining,
        });
        await stopInsiderForHeliusUsageExhaustion(
          index,
          new Error(
            `429 max usage reached: Helius credits exhausted for project ${info.projectId}`,
          ),
               "Helius REST credit check",
          info,
        );
      })().catch((err) =>
        log.error(
          `[INSIDER ${definition.botNumber}] Failed to stop or notify after Helius credit exhaustion`,
          err,
        ),
      );
    });
    insiderBots.push(bot);
  });

  async function resumePrimaryInsiderBot(): Promise<void> {
    const bot = insiderBots[0];
    if (!bot) return;
    const wallet = bot.getFollowedWallet();
    if (
      wallet &&
      !bot.isStoppedForHeliusCredits() &&
      !bot.isRunning() &&
      !bot.getActivePosition() &&
      !bot.getPreBuyMint()
    ) {
      const started = await followInsiderWalletWithUsageGuard(
        0,
        wallet,
        "startup resume",
      );
      if (!started) return;
      log.info("[INSIDER] Resumed primary follow-wallet monitoring", {
        wallet,
        apiPoolBots: insiderBots.length,
      });
    }
  }

  // ── Seed boughtMints from DB so restarts don't re-buy previous tokens ──────
  if (config.tradingWalletAddress) {
    const persistedBoughts = db.getSeenMints(config.tradingWalletAddress);
    for (const bot of insiderBots) {
      bot.seedSeenMints(persistedBoughts);
    }
    log.info(
      `Seeded ${persistedBoughts.size} previously-bought mint(s) from DB`,
    );
  }

  // Load default wallets from config as paused saved wallets. They do not start
  // monitoring until the user resumes or switches into the active mode.
  insiderBotDefinitions.forEach((definition, index) => {
    if (!definition.followWallet) return;
    try {
      insiderBots[index].configureFollowWallet(definition.followWallet);
      log.info(
        `[INSIDER ${definition.botNumber}] Loaded default follow wallet in paused state: ${definition.followWallet}`,
      );
    } catch (err) {
      log.error(
        `[INSIDER ${definition.botNumber}] Failed to load default follow wallet`,
        err,
      );
    }
  });

  // Telegram polling is intentionally NOT started yet — it's started further
  // down, once every orchestrator/bot event listener is wired up. Otherwise
  // an incoming /start (or a queued update from before a restart) could be
  // handled while insider buyTrigger/sellTrigger listeners don't exist yet,
  // silently dropping that update.

  funderFirstOrchestrator = new FunderFirstOrchestrator(
    config,
    insiderBots[0]!,
    telegramBot,
    sharedEnhancedWs,
  );

  insiderBots.forEach((bot, index) => {
    const client = gmgnClients[index];
    const botNumber = getInsiderBotNumber(index);

    bot.on("mintSeen", (mint: string) => {
      if (config.tradingWalletAddress) {
        db.addSeenMint(config.tradingWalletAddress, mint);
      }
    });

    bot.on("buyTrigger", (trigger) => {
      if (!config.tradingWalletAddress) {
        log.warn(
          `[INSIDER ${botNumber} BUY SKIP] No trading wallet configured`,
          trigger,
        );
        return;
      }

      log.warn(`[INSIDER ${botNumber} BUY TRIGGER]`, trigger);

      void (async () => {
        const tradersListStr = trigger.tradersListStr || "";
        void telegramBot
          ?.sendDefault(
            [
              `<b>🚀 Insider ${botNumber} Buy Executing</b>`,
              `Token: <code>${html(trigger.mint)}</code>`,
              `Buying: <b>${html(String(trigger.buySol))} SOL</b>`,
              `Entry MC: <b>$${html(trigger.entryMc?.toLocaleString() ?? "Unknown")}</b>`,
              `Exit MC: <b>$${html(bot.getExitMc().toLocaleString())}</b>`,
              "",
              tradersListStr,
              "",
              "Submitting swap...",
            ]
              .filter(Boolean)
              .join("\n"),
          )
          .catch((err) =>
            log.warn(
              `[INSIDER ${botNumber}] Buy-executing Telegram notification failed; continuing buy`,
              { error: err instanceof Error ? err.message : String(err) },
            ),
          );

        try {
          // Mark that we are executing a buy to prevent cleanup logic from wiping state
          bot.setBuyExecuting(true);

          const result = await client.buyTokenWithSol(
            config.tradingWalletAddress!,
            trigger.mint,
            {
              solAmount: trigger.buySol,
              slippage: config.sellSlippage,
              autoSlippage: config.sellAutoSlippage,
              priorityFeeSol: config.sellPriorityFeeSol,
            },
          );

          bot.markPositionBought(trigger);
          // ── Persist to DB so this mint is skipped on restart ─────────────
          if (config.tradingWalletAddress) {
            db.addSeenMint(config.tradingWalletAddress, trigger.mint);
          }
          bot.setBuyExecuting(false); // Clear execution flag after state is saved

          void telegramBot
            ?.sendDefault(
              [
                `<b>✅ Insider ${botNumber} Buy Completed</b>`,
                `Token: <code>${html(trigger.mint)}</code>`,
                `Entry MC: <b>$${html(trigger.entryMc?.toLocaleString() ?? "Unknown")}</b>`,
                `Status: <b>${html(result.status)}</b>`,
                result.hash
                  ? `Tx: https://solscan.io/tx/${html(result.hash)}`
                  : "",
                "",
                "<b>Strategy: Current MC Exit</b>",
                `Exit when current MC reaches: <b>$${html(bot.getExitMc().toLocaleString())}</b>`,
              ]
                .filter(Boolean)
                .join("\n"),
              {
                pin: true,
                replyMarkup: {
                  inline_keyboard: [
                    [
                      {
                        text: "🔴 Sell Position",
                        callback_data: `sell:insider:${trigger.mint}:${index}`,
                      },
                      {
                        text: "🔄 Refresh P/L & MC",
                        callback_data: `r:m:${trigger.mint}:i${index}`,
                      },
                    ],
                  ],
                },
              },
            )
            .catch((err) =>
              log.warn(
                `[INSIDER ${botNumber}] Buy-completed Telegram notification failed`,
                { error: err instanceof Error ? err.message : String(err) },
              ),
            );
        } catch (err) {
          const errorMessage = err instanceof Error ? err.message : String(err);
          if (errorMessage.includes("was not confirmed")) {
            bot.setBuyExecuting(false);
            const submittedSignature =
              errorMessage.match(/Transaction ([1-9A-HJ-NP-Za-km-z]+)/)?.[1] ??
              null;
            const submittedBlockhash =
              errorMessage.match(/blockhash ([1-9A-HJ-NP-Za-km-z]+)/)?.[1] ??
              null;
            log.warn(
              `[INSIDER ${botNumber}] Buy confirmation uncertain; locking buy gate while reconciling token balance`,
              {
                mint: trigger.mint,
                signature: submittedSignature,
                blockhash: submittedBlockhash,
                fastReconcileWindowMs: 5_000,
              },
            );

            void (async () => {
              if (!submittedSignature) {
                log.error(
                  `[INSIDER ${botNumber}] Timed-out buy had no signature; reopening buy gate`,
                  { mint: trigger.mint },
                );
                bot.resetBuyAttempt();
                return;
              }

              const fastDeadline = Date.now() + 5_000;
              let fastWindowElapsed = false;
              let reconcileCallCount = 0;
              while (true) {
                const state = await client.getSubmittedBuyReconciliationState(
                  config.tradingWalletAddress!,
                  trigger.mint,
                  submittedSignature,
                  submittedBlockhash,
                    reconcileCallCount,    
                );
                reconcileCallCount++;
                if (state.tokenBalance > 0n) {
                  bot.markPositionBought(trigger);
                  db.addSeenMint(config.tradingWalletAddress!, trigger.mint);
                  log.warn(
                    `[INSIDER ${botNumber}] Uncertain PumpPortal buy recovered from token balance`,
                    {
                      mint: trigger.mint,
                      signature: submittedSignature,
                      signatureStatus: state.signatureStatus,
                      tokenBalance: state.tokenBalance.toString(),
                    },
                  );
                  void telegramBot
                    ?.sendDefault(
                      [
                        `<b>✅ Insider ${botNumber} Buy Confirmed by Balance</b>`,
                        `Token: <code>${html(trigger.mint)}</code>`,
                        `Entry MC: <b>$${html(trigger.entryMc?.toLocaleString() ?? "Unknown")}</b>`,
                        submittedSignature
                          ? `Tx: https://solscan.io/tx/${html(submittedSignature)}`
                          : "",
                        `Token balance: <code>${state.tokenBalance.toString()}</code>`,
                      ]
                        .filter(Boolean)
                        .join("\n"),
                      { pin: true },
                    )
                    .catch((notifyErr) =>
                      log.warn(
                        `[INSIDER ${botNumber}] Recovered-buy Telegram notification failed`,
                        {
                          error:
                            notifyErr instanceof Error
                              ? notifyErr.message
                              : String(notifyErr),
                        },
                      ),
                    );
                  return;
                }

                if (state.signatureStatus === "failed") {
                  log.error(
                    `[INSIDER ${botNumber}] Submitted buy failed on-chain; reopening buy gate`,
                    {
                      mint: trigger.mint,
                      signature: submittedSignature,
                      error: state.signatureError,
                    },
                  );
                  bot.resetBuyAttempt();
                  return;
                }

                if (
                  state.blockhashValid === false &&
                  state.signatureStatus !== "confirmed"
                ) {
                  log.error(
                    `[INSIDER ${botNumber}] Submitted buy blockhash expired with zero token balance; reopening buy gate`,
                    {
                      mint: trigger.mint,
                      signature: submittedSignature,
                      blockhash: submittedBlockhash,
                    },
                  );
                  bot.resetBuyAttempt();
                  return;
                }

                if (!fastWindowElapsed && Date.now() >= fastDeadline) {
                  fastWindowElapsed = true;
                  log.warn(
                    `[INSIDER ${botNumber}] Five-second reconciliation elapsed; switching to slow poll`,
                    {
                      mint: trigger.mint,
                      signature: submittedSignature,
                      signatureStatus: state.signatureStatus,
                      blockhashValid: state.blockhashValid,
                    },
                  );
                }
                await sleep(fastWindowElapsed ? 3_000 : 500);
              }
            })();
            return;
          }

          bot.resetBuyAttempt();
          log.error(`Insider ${botNumber} buy failed`, err);
          void telegramBot
            ?.sendDefault(
              [
                `<b>❌ Insider ${botNumber} Buy Failed</b>`,
                `Token: <code>${html(trigger.mint)}</code>`,
                `Error: ${html(err instanceof Error ? err.message : String(err))}`,
              ].join("\n"),
            )
            .catch((notifyErr) =>
              log.warn(
                `[INSIDER ${botNumber}] Buy-failed Telegram notification also failed`,
                {
                  error:
                    notifyErr instanceof Error
                      ? notifyErr.message
                      : String(notifyErr),
                },
              ),
            );
        }
      })();
    });

    bot.on("sellTrigger", (trigger) => {
      if (!config.tradingWalletAddress) {
        log.warn(
          `[INSIDER ${botNumber} SELL SKIP] No trading wallet configured`,
          trigger,
        );
        return;
      }
      if (
        hasPendingSellForMint(config.tradingWalletAddress, trigger.positionMint)
      ) {
        log.info(
          `[INSIDER ${botNumber} SELL SKIP] Sell already pending for ${trigger.positionMint}`,
        );
        return;
      }

      const entryMc = bot.getEntryMc();

      const event: FilterFailEvent = {
        walletAddress: config.tradingWalletAddress,
        mint: trigger.positionMint,
        sampleNumber: 0,
        elapsedSec: 0,
        reasons: [trigger.reason],
        settings: db.getWalletSettings(config.tradingWalletAddress),
        metrics: {
          mint: trigger.positionMint,
          timestamp: new Date().toISOString(),
          bundlersPercent: null,
          bundlersCount: null,
          initialBaseReserve: null,
          topWallets: null,
          top10HolderRate: null,
          bundledAmountRate: null,
        },
        buySol: bot.getBuySol(),
        matchingWallets: [],
        entryMc,
        insiderBotIndex: index,
      };

      const sellId = randomBytes(5).toString("hex");
      pendingSells.set(sellId, {
        event,
        createdAt: Date.now(),
        executing: true,
      });

      telegramBot
        ?.sendDefault(
          [
            `<b>🚨 Insider ${botNumber} Sell Triggered</b>`,
            `Token: <code>${html(trigger.positionMint)}</code>`,
            `Reason: <b>${trigger.reason}</b>`,
            `Action: submit sell for <b>${config.sellPercent}%</b>.`,
          ].join("\n"),
          {
            replyMarkup: {
              inline_keyboard: [
                [
                  {
                    text: "🔄 Refresh P/L & MC",
                    callback_data: `r:m:${trigger.positionMint}:i${index}`,
                  },
                ],
              ],
            },
          },
        )
        .catch((err) =>
          log.warn(`Telegram insider ${botNumber} sell alert failed`, err),
        );

      void executeSellAndNotify(config.telegramChatId, sellId, telegramBot);
    });

    bot.on("error", (err) => {
      log.error(`Insider ${botNumber} error:`, err);
      telegramBot
        ?.sendDefault(
          `<b>⚠️ Insider Bot ${botNumber} Error</b>\n${html(err.message)}`,
        )
        .catch((e) => log.warn("Telegram error alert failed", e));
    });
  });

  // ── 5. Bot Logic ──────────────────────────────────────────────────────────

  function queueWatchedWalletSell(
    watchedBuy: NewTokenEvent,
    tradingPosition: NewTokenEvent,
  ): void {
    if (
      hasPendingSellForMint(tradingPosition.walletAddress, tradingPosition.mint)
    ) {
      log.info(`[SELL SKIP] Sell already pending for ${tradingPosition.mint}`);
      return;
    }

    const event: FilterFailEvent = {
      walletAddress: tradingPosition.walletAddress,
      mint: tradingPosition.mint,
      sampleNumber: 0,
      elapsedSec: 0,
      reasons: [
        `Reverse-buy wallet ${watchedBuy.walletAddress} bought this token while your trading-wallet position was open.`,
        "Configured action triggered: sell immediately on reverse-buy wallet buy signal.",
      ],
      settings: db.getWalletSettings(watchedBuy.walletAddress),
      metrics: {
        mint: tradingPosition.mint,
        timestamp: new Date().toISOString(),
        bundlersPercent: null,
        bundlersCount: null,
        initialBaseReserve: null,
        topWallets: null,
        top10HolderRate: null,
        bundledAmountRate: null,
      },
      buySol:
        tradingPosition.buySol ??
        db.getToken(tradingPosition.walletAddress, tradingPosition.mint)
          ?.buySol ??
        null,
      matchingWallets: [watchedBuy.walletAddress],
    };
    const sellId = randomBytes(5).toString("hex");
    pendingSells.set(sellId, {
      event,
      createdAt: Date.now(),
      executing: true,
    });
    telegramBot
      ?.sendDefault(
        [
          "<b>Reverse-Buy Wallet Triggered Sell</b>",
          `Reverse-buy wallet: <code>${html(watchedBuy.walletAddress)}</code>`,
          `Trading wallet: <code>${html(tradingPosition.walletAddress)}</code>`,
          `Token: <code>${html(tradingPosition.mint)}</code>`,
          `Action: submit sell for <b>${config.sellPercent}%</b> immediately.`,
        ].join("\n"),
      )
      .catch((err) =>
        log.warn("Telegram watched-wallet sell trigger alert failed", err),
      );
    void executeSellAndNotify(config.telegramChatId, sellId, telegramBot);
  }

  function wireWatchedWalletMonitor(walletMonitor: WalletMonitor): void {
    walletMonitor.on("newToken", (event: NewTokenEvent) => {
      log.info(
        `[WATCHED BUY] Wallet: ${event.walletAddress} Mint: ${event.mint}`,
      );
      startWatchedWalletSummary(event);
      if (!db.isReverseBuyWallet(event.walletAddress)) {
        log.info(
          `[REVERSE BUY NOT CONFIGURED] Wallet: ${event.walletAddress} Mint: ${event.mint}`,
        );
        return;
      }
      const tradingPosition = pendingTradingBuys.get(event.mint);
      if (tradingPosition) {
        pendingTradingBuys.delete(event.mint);
        queueWatchedWalletSell(event, tradingPosition);
      }
    });
  }

  async function checkInsiderMcapFlow(
    index: number,
    preFetchedMc?: number | null,
    preFetchedSource?: string,
  ): Promise<void> {
    const bot = insiderBots[index];
    const botNumber = getInsiderBotNumber(index);
    if (bot.isStoppedForHeliusCredits()) return;
    const preBuyMint = bot.getPreBuyMint();
    const activePos = bot.getActivePosition();

    if (!preBuyMint && !activePos) return;

    const mint = preBuyMint || activePos!.mint;

    try {
      const fetched =
        preFetchedMc !== undefined
          ? {
              marketCap: preFetchedMc,
              source: preFetchedSource ?? "Prefetched",
            }
          : await fetchInsiderMarketCapUsd(index, mint);
      const currentMc = fetched?.marketCap ?? null;

      if (currentMc === null) {
        log.debug(
          `[INSIDER ${botNumber} MC SKIP] Could not fetch market cap for ${mint}`,
        );
        return;
      }

      log.info(
        `[INSIDER ${botNumber} MC CHECK] Token: ${mint} MC: $${currentMc.toLocaleString()} (Source: ${fetched?.source ?? "Unknown"})`,
      );

      // Note: the old "MC below $5,000 (Rug)" reset/sell check that used to
      // live here has been replaced by dev-wallet full-exit detection inside
      // InsiderBot itself (a CLOSE_ACCOUNT/SOLANA_PROGRAM_LIBRARY tx from the
      // dev wallet — see checkDevWalletFullExit in insider-bot.ts), which runs
      // continuously in the bot's own poll loop and triggers resetForNewToken
      // (pre-buy) or a sellTrigger (post-buy) directly.

      if (activePos) {
        const exitMc = bot.getExitMc();
        if (bot.isProfitExitDisabled()) {
          log.info(
            `[INSIDER ${botNumber} MC EXIT SKIP] Profit MC exit disabled for ${activePos.mint}; waiting for recipient sell-all or zero-SOL exit. Current MC $${currentMc.toLocaleString()}, target $${exitMc.toLocaleString()}.`,
          );
          return;
        }
        if (currentMc >= exitMc) {
          if (await bot.deferProfitExitUntilDevSwap(currentMc)) {
            log.warn(
              `[INSIDER ${botNumber} MC EXIT PENDING] Current MC $${currentMc.toLocaleString()} reached Exit MC $${exitMc.toLocaleString()}, waiting for dev swap confirmation.`,
            );
            return;
          }
          log.warn(
            `[INSIDER ${botNumber} EXIT] Current MC $${currentMc.toLocaleString()} reached Exit MC $${exitMc.toLocaleString()}. Triggering SELL.`,
          );
          bot.emit("sellTrigger", {
            followedWallet: bot.getFollowedWallet()!,
            positionMint: activePos.mint,
            signature: "MC_TRIGGER",
            reason: `Current MC $${currentMc.toLocaleString()} reached target $${exitMc.toLocaleString()}`,
          });
        }
      }
    } catch (err) {
      log.error(
        `Failed to check Insider MC flow for ${mint} (Bot ${botNumber})`,
        err,
      );
    }
  }

  function startMarketCapChecker(): void {
    log.info(
      `Starting independent market cap checkers (interval: ${MCAP_CHECK_INTERVAL_MS}ms)`,
    );

    // 1. Insider MC flow. Keep these loops alive across mode changes; an
    // inactive bot simply has no pre-buy mint or active position to inspect.
    insiderBots.forEach((bot, i) => {
      let isChecking = false;
      setInterval(async () => {
        if (isChecking) return;
        isChecking = true;

        try {
          const preBuyMint = bot.getPreBuyMint();
          const activePos = bot.getActivePosition();

          // Monitor both pre-buy and active position concurrently if they exist
          const tasks: Promise<void>[] = [];

          if (preBuyMint) {
            tasks.push(
              (async () => {
                const currentMc = await fetchInsiderMarketCapUsd(i, preBuyMint);
                if (currentMc !== null) {
                  await checkInsiderMcapFlow(
                    i,
                    currentMc.marketCap,
                    currentMc.source,
                  );
                }
              })(),
            );
          }

          if (activePos) {
            // Background refresh balance and quote if missing or old (> 30s)
            const cached = activePositionCache.get(activePos.mint);
            if (
              config.tradingWalletAddress &&
              (!cached || Date.now() - cached.timestamp > 30_000) &&
              !activePositionRefreshes.has(activePos.mint)
            ) {
              activePositionRefreshes.add(activePos.mint);
              const client = gmgnClients[i];
              const owner = new PublicKey(config.tradingWalletAddress);
              const mintPk = new PublicKey(activePos.mint);

              log.debug(
                `[INSIDER ${getInsiderBotNumber(i)} BACKGROUND] Refreshing balance/quote for ${activePos.mint}`,
              );
              Promise.all([
                getTokenRawBalance(owner, mintPk),
                client
                  .quoteTokenSellForSol(
                    config.tradingWalletAddress,
                    activePos.mint,
                    100,
                  )
                  .catch(() => null),
              ])
                .then(([balance, quote]) => {
                  activePositionCache.set(activePos.mint, {
                    balance,
                    quote,
                    timestamp: Date.now(),
                  });
                })
                .catch((e) => {
                  // Right after a fresh buy, the RPC node may not have indexed
                  // the new token account yet ("could not find mint" / -32602).
                  // This is a transient condition that resolves itself within a
                  // few seconds once indexing catches up, so it's logged as a
                  // warning rather than an error to avoid alarming noise.
                  const message = e instanceof Error ? e.message : String(e);
                  const isTransientIndexingLag =
                    message.includes("could not find mint") ||
                    (e as { code?: number } | undefined)?.code === -32602;
                  if (isTransientIndexingLag) {
                    log.warn(
                      `Background balance/quote refresh temporarily failed for ${activePos.mint} (RPC still indexing new token account, will retry)`,
                    );
                  } else {
                    log.error(
                      `Background balance/quote refresh failed for ${activePos.mint}`,
                      e,
                    );
                  }
                })
                .finally(() => {
                  activePositionRefreshes.delete(activePos.mint);
                });
            }

            tasks.push(
              (async () => {
                const currentMc = await fetchInsiderMarketCapUsd(
                  i,
                  activePos.mint,
                );
                if (currentMc !== null) {
                  // checkInsiderMcapFlow handles Exit MC and Flow v2 for active positions
                  await checkInsiderMcapFlow(
                    i,
                    currentMc.marketCap,
                    currentMc.source,
                  );
                }
              })(),
            );
          }

          await Promise.all(tasks);
        } catch (err) {
          log.error(
            `Error in Insider Bot ${getInsiderBotNumber(i)} MC loop`,
            err,
          );
        } finally {
          isChecking = false;
        }
      }, MCAP_CHECK_INTERVAL_MS);
    });
  }

  function startWatchedWalletSummary(event: NewTokenEvent): void {
    if (!db.tokenExists(event.walletAddress, event.mint)) {
      db.insertToken({
        walletAddress: event.walletAddress,
        mint: event.mint,
        firstSeen: new Date().toISOString(),
        monitoringStatus: "active",
        detectedAt: event.detectedAt,
        buySol: event.buySol,
      });
    }

    telegramBot
      ?.sendDefault(
        [
          "<b>Watched Wallet Monitoring Started</b>",
          `Wallet: <code>${html(event.walletAddress)}</code>`,
          `Token: <code>${html(event.mint)}</code>`,
          `Buy SOL: <b>${event.buySol ?? "unknown"}</b>`,
          "Sell trigger only happens if this wallet is explicitly added to reverse-buy trigger list in settings.",
        ].join("\n"),
      )
      .catch((err) =>
        log.warn("Telegram watched-wallet monitor alert failed", err),
      );
  }

  async function startWallet(address: string): Promise<string> {
    const normalized = new PublicKey(address).toBase58();
    if (walletMonitors.has(normalized)) {
      return `Already monitoring <code>${normalized}</code>`;
    }

    db.addWallet(normalized);
    const settings = db.getWalletSettings(normalized);
    const minBuySol =
      settings.minSolBuy !== null && settings.minSolBuy > 0
        ? settings.minSolBuy
        : undefined;
    const monitor = new WalletMonitor(config, normalized, {
      enforceMinBuySol: true,
      minBuySol,
      logLabel: `WALLET WATCHED ${normalized.slice(0, 6)}`,
    });
    wireWatchedWalletMonitor(monitor);
    pausedWallets.delete(normalized);
    await monitor.start();
    walletMonitors.set(normalized, monitor);
    log.info(`[WALLET ADD] Started monitoring ${normalized}`);
    return `Monitoring wallet <code>${normalized}</code>`;
  }

  function stopWallet(address: string): string {
    const normalized = new PublicKey(address).toBase58();
    const monitor = walletMonitors.get(normalized);
    if (!monitor) {
      db.removeWallet(normalized);
      return `Wallet was not running: <code>${normalized}</code>`;
    }

    monitor.stop();
    walletMonitors.delete(normalized);
    pausedWallets.delete(normalized);
    db.removeWallet(normalized);
    log.info(`[WALLET REMOVE] Stopped monitoring ${normalized}`);
    return `Stopped monitoring <code>${normalized}</code>`;
  }

  function pauseWallet(address: string): string {
    const normalized = new PublicKey(address).toBase58();

    const monitor = walletMonitors.get(normalized);
    if (!monitor) return `Wallet is not monitored: <code>${normalized}</code>`;
    if (pausedWallets.has(normalized))
      return `Wallet is already paused: <code>${normalized}</code>`;
    monitor.stop();
    pausedWallets.add(normalized);
    log.info(`[WALLET PAUSE] Paused monitoring ${normalized}`);
    return `Paused monitoring <code>${normalized}</code>`;
  }

  async function resumeWallet(address: string): Promise<string> {
    const normalized = new PublicKey(address).toBase58();

    const monitor = walletMonitors.get(normalized);
    if (!monitor) return `Wallet is not monitored: <code>${normalized}</code>`;
    if (!pausedWallets.has(normalized))
      return `Wallet is already running: <code>${normalized}</code>`;
    pausedWallets.delete(normalized);
    await monitor.start();
    log.info(`[WALLET RESUME] Resumed monitoring ${normalized}`);
    return `Continued monitoring <code>${normalized}</code>`;
  }

  async function startFunderFirstModeServices(): Promise<void> {
    if (!funderFirstOrchestrator.getFunderAddress()) return;
    try {
      await funderFirstOrchestrator.start();
      log.info("Funder-first mode auto-started from configured funder address", {
        funderAddress: funderFirstOrchestrator.getFunderAddress(),
      });
    } catch (err) {
      log.warn("Funder-first mode failed to auto-start", {
        error: err instanceof Error ? err.message : String(err),
      });
    }
  }

  function updateMinSol(walletAddress: string, rawValue: string): string {
    const normalized = new PublicKey(walletAddress).toBase58();
    const isTrading = normalized === config.tradingWalletAddress;
    const trimmed = rawValue.trim().toLowerCase();
    const settings = db.getWalletSettings(normalized);

    if (
      trimmed === "off" ||
      trimmed === "any" ||
      trimmed === "none" ||
      trimmed === "default"
    ) {
      settings.minSolBuy = isTrading ? 0 : null;
    } else {
      const value = Number(trimmed);
      if (!Number.isFinite(value) || value < 0) {
        return `Invalid value. Send a non-negative number, or "off".`;
      }
      settings.minSolBuy = value;
    }

    db.updateWalletSettings(normalized, settings);
    return `Updated Min SOL buy for <code>${html(normalized)}</code>.`;
  }

  function walletSummaryReply(
    address: string,
    editCurrent = false,
  ): TelegramReply {
    const normalized = new PublicKey(address).toBase58();
    const isTrading = normalized === config.tradingWalletAddress;
    const isMonitoring = isTrading ? false : walletMonitors.has(normalized);
    const isPaused = pausedWallets.has(normalized);
    const reverseBuyEnabled = !isTrading && db.isReverseBuyWallet(normalized);
    const settings = db.getWalletSettings(normalized);
    const minSolBuy = settings.minSolBuy ?? (isTrading ? 0 : config.minBuySol);
    const status = !isMonitoring
      ? "Not active"
      : isPaused
        ? "Paused"
        : "Active";

    const pauseButton = isPaused
      ? {
          text: "Continue monitoring",
          callback_data: `wallet:resume:${normalized}`,
        }
      : {
          text: "Pause monitoring",
          callback_data: `wallet:pause:${normalized}`,
        };

    const keyboard = [];

    // Row 1: Min SOL Button
    keyboard.push([
      {
        text: `Min SOL: ${minSolBuy}`,
        callback_data: `set:minSol:${normalized}`,
      },
    ]);

    // Row 2: Pause and Action Buttons
    const row2 = [pauseButton];
    if (isTrading) {
      // No extra action button for trading wallet
    } else {
      row2.push(
        !isMonitoring
          ? { text: "Add wallet", callback_data: `wallet:add:${normalized}` }
          : {
              text: "Remove wallet",
              callback_data: `wallet:remove:${normalized}`,
            },
      );
    }
    keyboard.push(row2);

    // Row 3: Reverse Buy (only for watched wallets)
    if (!isTrading && isMonitoring) {
      keyboard.push([
        {
          text: `${reverseBuyEnabled ? "Remove" : "Add"} reverse-buy trigger`,
          callback_data: `${reverseBuyEnabled ? "reverse:remove" : "reverse:add"}:${normalized}`,
        },
      ]);
    }

    // Row 4: Navigation
    keyboard.push([
      { text: "Back", callback_data: "menu:refresh" },
      { text: "Refresh", callback_data: `wallet:refresh:${normalized}` },
    ]);

    const flowDesc = isTrading
      ? [
          "<b>Flow Description</b>",
          "• When this wallet buys a token, early bundlers are detected.",
          "• Monitoring begins for identified bundler wallets.",
          "• If a bundler sells 40%, an immediate sell is triggered.",
        ]
      : [
          "<b>Flow Description</b>",
          "• When your trading wallet buys a token, early bundlers are detected.",
          "• If this wallet buys the same token, an immediate sell is triggered.",
          "• If a bundler sells 40% of holdings, an immediate sell is triggered.",
        ];

    return {
      text: [
        isTrading ? "<b>💳 Trading Wallet</b>" : "<b>✅ Watched Wallet</b>",
        `<code>${html(normalized)}</code>`,
        "",
        `Status: <b>${status}</b>`,
        `Min SOL: <b>${minSolBuy}</b>`,
        !isTrading && isMonitoring
          ? `Reverse-buy: <b>${reverseBuyEnabled ? "ENABLED" : "DISABLED"}</b>`
          : "",
        "",
        ...flowDesc,
      ]
        .filter(Boolean)
        .join("\n"),
      replyMarkup: {
        inline_keyboard: keyboard,
      },
      editCurrent,
    };
  }

  function settingsReply(address: string, editCurrent = false): TelegramReply {
    return walletSummaryReply(address, editCurrent);
  }

  function homeReply(editCurrent = false): TelegramReply {
    activeInsiderIndex = 0;
    const bot = insiderBots[0];
    const followedWallet = bot.getFollowedWallet();
    const insiderRunning = bot.isRunning();
    const preBuyMint = bot.getPreBuyMint();
    const activePos = bot.getActivePosition();

    let status = "Idle";
    if (insiderRunning) {
      if (activePos)
        status = `Holding token ${html(activePos.mint.slice(0, 8))}...`;
      else if (preBuyMint)
        status = `Watching token ${html(preBuyMint.slice(0, 8))}...`;
      else status = "Running";
    } else if (followedWallet) {
      status = "Paused";
    }

    const stopResumeButton =
      followedWallet && !insiderRunning
        ? { text: "Resume", callback_data: "insider:resume" }
        : { text: "Stop", callback_data: "insider:stop" };

    const buyDisabled = bot.isBuyDisabled();
    const disableBuyButton = {
      text: buyDisabled ? "✅ Enable Buy" : "❌ Disable Buy",
      callback_data: "insider:togglebuy",
    };

    const monitoredWallet = bot.getMonitoredWallet();
    const funderAddress = funderFirstOrchestrator.getFunderAddress();
    const funderFirstRunning = funderFirstOrchestrator.isRunning();
    const watchedPotential = funderFirstOrchestrator.getWatchedPotentialFeePayers();
    const potentialLines =
      watchedPotential.length > 0
        ? watchedPotential.map(
            (w) =>
              `  • <code>${html(w.address.slice(0, 8))}…</code> — <b>${w.status}</b>${w.mint ? ` (${html(w.mint.slice(0, 8))}…)` : ""}`,
          )
        : ["  • <i>none yet</i>"];

    const funderFirstStartStop = funderFirstRunning
      ? { text: "Stop Funder-First", callback_data: "funderfirst:stop" }
      : { text: "Start Funder-First", callback_data: "funderfirst:start" };

    return {
      text: [
        "<b>Insider Bot</b>",
        "",
        `Status: <b>${status}</b>`,
        `Follow wallet: ${followedWallet ? `<code>${html(followedWallet)}</code>` : "<b>Not set</b>"}`,
        monitoredWallet
          ? `Insider wallet: <code>${html(monitoredWallet)}</code>`
          : "",
        `FeePayer funder: ${funderAddress ? `<code>${html(funderAddress)}</code>` : "<b>Not set</b>"}`,
        `Funder-first: <b>${funderFirstRunning ? "Running" : "Stopped"}</b>`,
        "<b>Watched potential feePayers:</b>",
        ...potentialLines,
        `Default Buy SOL: <b>${html(String(bot.getBuySol()))}</b>`,
        `Normal Funding Buy SOL: <b>${html(String(bot.getNormalFundingBuySol()))}</b>`,
        `Low-Funding Buy SOL: <b>${html(String(bot.getLowFundingBuySol()))}</b>`,
        `Exit Strategy: <b>+${html(String(bot.getExitPercent()))}% Current MC from Entry</b>`,
        `Auto Buy: <b>${buyDisabled ? "Disabled ❌" : "Enabled ✅"}</b>`,
        "",
        "<b>Flows (run in parallel)</b>",
        "<b>A) Follow-wallet</b> — backtrack feePayer from a followed wallet buy.",
        "<b>B) Funder-first</b> — watch a feePayer funder; detect potential feePayers funding bundlers.",
        "",
        "<b>Follow-wallet steps</b>",
        "1. Bot 1 follows one wallet; Helius keys 1-4 are the fallback pool.",
        "2. Skip if follow-wallet buy MC is above $80,000.",
        "3. First four bundler buys must include the follow wallet.",
        "4. Shared feePayer locked → normal-mode tiny transfer-out buy signals.",
        "",
        "<b>Funder-first steps</b>",
        "1. Funder sends SOL → potential feePayer watches open.",
        "2. ≥20 SOL post-balance to 4 wallets in 10s → normal candidate → buy.",
        "3. Sub-20 SOL bundler sends are ignored (no alerts).",
        "4. FeePayer watch continues until SOL → zero; follow drain recipient (skip if funder).",
        "• Rug: dev CLOSE_ACCOUNT resumes feePayer watch after a trade.",
      ].join("\n"),
      replyMarkup: {
        inline_keyboard: [
          [
            { text: "Follow wallet", callback_data: "insider:follow" },
            { text: "FeePayer funder", callback_data: "funderfirst:funder" },
          ],
          [funderFirstStartStop],
          [
            { text: "Default Buy SOL", callback_data: "insider:buysol" },
            { text: "Normal Buy SOL", callback_data: "insider:normalbuysol" },
          ],
          [
            { text: "Low-Funding Buy SOL", callback_data: "insider:lowfundingbuysol" },
            { text: "Set Exit %", callback_data: "insider:exitpercent" },
          ],
          [disableBuyButton],
          [
            { text: "Status", callback_data: "menu:status" },
            { text: "Refresh", callback_data: "menu:refresh" },
          ],
          [stopResumeButton],
        ],
      },
      editCurrent,
    };
  }

  function walletsReply(chatId: string, editCurrent = false): TelegramReply {
    const wallets = [...walletMonitors.keys()];
    const tradingWallet = config.tradingWalletAddress;
    const allWallets = tradingWallet ? [tradingWallet, ...wallets] : wallets;
    walletAliasesByChat.set(chatId, allWallets);

    const lines = allWallets.map((w, i) => {
      const isTrading = w === tradingWallet;
      const isPaused = pausedWallets.has(w);
      const label = isTrading ? "💳" : "✅";
      const status = isPaused ? " (PAUSED)" : "";
      return `${label} /w_${i} <code>${html(w)}</code>${status}`;
    });

    return {
      text: lines.length
        ? `Monitored wallets:\n${lines.join("\n")}`
        : "No wallets are being monitored.",
      replyMarkup: {
        inline_keyboard: lines.length
          ? [[{ text: "Back", callback_data: "menu:refresh" }]]
          : [[{ text: "Add wallet", callback_data: "menu:addwallet" }]],
      },
      editCurrent,
    };
  }

  function statusReply(editCurrent = false): TelegramReply {
    const bot = insiderBots[0];
    const followed = bot?.getFollowedWallet();
    const status = bot?.isRunning()
      ? "Running"
      : followed
        ? "Paused"
        : "Idle";
    const primaryInfo = [
      "<b>Primary Insider Bot</b>",
      `Status: ${status}`,
      `Follow: ${followed ?? "not set"}`,
      `Default Buy: ${bot?.getBuySol() ?? config.insiderBuySol} SOL`,
      `Normal Buy: ${bot?.getNormalFundingBuySol() ?? config.insiderNormalBuySol} SOL`,
      `Low-Funding Buy: ${bot?.getLowFundingBuySol() ?? config.insiderLowFundingBuySol} SOL`,
      `Helius key pool: ${insiderBots.length} key${insiderBots.length === 1 ? "" : "s"}`,
    ].join("\n");

    const text = [
      "<b>Bot Status</b>",
      "Flow: one follow wallet, four-key Helius pool, shared bundler-funder confirmation.",
      "Candidate rule: transfer-out confirms only when the immediate next funder tx is not a SOL transfer-in.",
      "API guard: queued Helius pool, transient-only fallback, per-key backoff, capped recipient batch sync.",
      "",
      primaryInfo,
    ].join("\n");

    return {
      text,
      replyMarkup: {
        inline_keyboard: [[{ text: "Back", callback_data: "menu:refresh" }]],
      },
      editCurrent,
    };
  }

  function lamportsToSol(raw: string | null): number | null {
    if (!raw || !/^\d+$/.test(raw)) return null;
    return Number(BigInt(raw)) / 1_000_000_000;
  }

  function sellReceipt(event: FilterFailEvent, result: SellResult): string {
    const receivedSol = lamportsToSol(result.filledOutputAmount);
    const pnlPct =
      event.entryMc !== null &&
      event.entryMc !== undefined &&
      event.entryMc > 0 &&
      event.sellMc !== null &&
      event.sellMc !== undefined
        ? ((event.sellMc - event.entryMc) / event.entryMc) * 100
        : null;
    const fmtSol = (value: number | null): string =>
      value === null ? "N/A" : `${parseFloat(value.toFixed(6))} SOL`;
    const pnlLine =
      pnlPct === null
        ? "P/L by MC: <b>N/A</b>"
        : `P/L by MC: <b>${pnlPct >= 0 ? "+" : ""}${pnlPct.toFixed(2)}%</b>`;

    return [
      result.status === "confirmed"
        ? "<b>Sell Confirmed</b>"
        : "<b>Sell Submitted</b>",
      `Wallet: <code>${html(event.walletAddress)}</code>`,
      `Token: <code>${html(event.mint)}</code>`,
      `Status: <b>${html(result.status)}</b>`,
      `Sold: <b>${result.soldPercent}%</b>`,
      `Matched watched wallets: <b>${event.matchingWallets.length}</b>`,
      `Token amount sold: <code>${html(result.filledInputAmount ?? "pending")}</code>`,
      `Received: <b>${fmtSol(receivedSol)}</b>`,
      event.entryMc !== null && event.entryMc !== undefined
        ? `Entry MC: <b>$${event.entryMc.toLocaleString()}</b>`
        : null,
      event.sellMc !== null && event.sellMc !== undefined
        ? `Sell MC: <b>$${event.sellMc.toLocaleString()}</b>`
        : null,
      pnlLine,
      result.hash ? `Tx: https://solscan.io/tx/${html(result.hash)}` : "",
      result.orderId ? `Order ID: <code>${html(result.orderId)}</code>` : "",
      "",
      "<b>Why it sold</b>",
      ...event.reasons.map((reason) => `- ${html(reason)}`),
    ]
      .filter(Boolean)
      .join("\n");
  }

  function sellFailedReply(event: FilterFailEvent, err: unknown): string {
    return [
      "<b>Sell Failed</b>",
      `Wallet: <code>${html(event.walletAddress)}</code>`,
      `Token: <code>${html(event.mint)}</code>`,
      `Matched watched wallets: <b>${event.matchingWallets.length}</b>`,
      `Error: ${html(err instanceof Error ? err.message : String(err))}`,
      "",
      "<b>Why sell was requested</b>",
      ...event.reasons.map((reason) => `- ${html(reason)}`),
    ].join("\n");
  }

  async function getTokenRawBalance(
    owner: PublicKey,
    mint: PublicKey,
  ): Promise<bigint> {
    const accounts = await gmgnClients[0].getParsedTokenAccountsForMint(
      owner,
      mint,
    );
    let total = 0n;
    for (const account of accounts) {
      const parsed = account.account.data.parsed as {
        info?: { tokenAmount?: { amount?: string } };
      };
      const raw = parsed.info?.tokenAmount?.amount;
      if (raw && /^\d+$/.test(raw)) {
        total += BigInt(raw);
      }
    }
    return total;
  }

  async function executeSellAndNotify(
    chatId: string | null,
    sellId: string,
    telegramBot: TelegramBot | null,
  ): Promise<void> {
    const pending = pendingSells.get(sellId);
    if (!pending) return;

    try {
      const owner = new PublicKey(pending.event.walletAddress);
      const mintPk = new PublicKey(pending.event.mint);

      // Check Cache for balance first
      const cached = activePositionCache.get(pending.event.mint);
      let startingBalance: bigint | null = cached ? cached.balance : null;

      if (startingBalance === null) {
        startingBalance = await getTokenRawBalance(owner, mintPk).catch(
          () => null,
        );
      }

      if (startingBalance !== null && startingBalance <= 0n) {
        log.info(
          `[SELL ABORT] No token accounts found for ${pending.event.mint}, assuming already sold.`,
        );
        return;
      }

      const currentPending = pendingSells.get(sellId);
      if (!currentPending) return;

      let lastResult: SellResult | null = null;

      log.info(
        `[SELL EXECUTE] Starting sell for ${currentPending.event.mint} (Initial Balance: ${startingBalance ?? "unknown"})`,
      );

      lastResult = await gmgnClients[0].sellTokenForSol(
        currentPending.event.walletAddress,
        currentPending.event.mint,
        {
          percent: config.sellPercent,
          slippage: config.sellSlippage,
          autoSlippage: config.sellAutoSlippage,
          priorityFeeSol: config.sellPriorityFeeSol,
          antiMev: config.sellAntiMev,
          preFetchedBalance: startingBalance ?? undefined,
        },
      );
      if (lastResult.status !== "confirmed") {
        throw new Error(
          `PumpPortal sell returned unexpected status: ${lastResult.status}`,
        );
      }

      // Cleanup cache
      activePositionCache.delete(pending.event.mint);
      if (pending.event.insiderBotIndex !== undefined) {
        insiderBots[pending.event.insiderBotIndex]?.clearActivePosition();
      }

      const receiptResult = lastResult;
      if (
        currentPending.event.entryMc !== null &&
        currentPending.event.entryMc !== undefined
      ) {
        const sellMcClient =
          currentPending.event.insiderBotIndex !== undefined
            ? gmgnClients[currentPending.event.insiderBotIndex]
            : gmgnClients[0];
        currentPending.event.sellMc = await sellMcClient
          .fetchTokenMarketCapUsd(currentPending.event.mint)
          .catch(() => null);
      }
      const receipt = sellReceipt(currentPending.event, receiptResult);
      if (chatId && telegramBot) {
        await telegramBot.sendChat(chatId, receipt);
      } else {
        log.info("Sell completed without Telegram receipt chat", {
          mint: currentPending.event.mint,
          status: receiptResult.status,
          hash: receiptResult.hash,
          orderId: receiptResult.orderId,
        });
      }
    } catch (err) {
      // Clear the failed attempt before rearming the Insider trigger.
      // Otherwise an immediate authority sell retry can be discarded by
      // hasPendingSellForMint() as a duplicate of the failed attempt.
      pendingSells.delete(sellId);
      if (pending.event.insiderBotIndex !== undefined) {
        insiderBots[
          pending.event.insiderBotIndex
        ]?.rearmPositionMonitoringAfterSellFailure(pending.event.mint);
      }
      if (chatId && telegramBot) {
        await telegramBot.sendChat(chatId, sellFailedReply(pending.event, err));
      } else {
        log.error("Sell failed without Telegram receipt chat", err);
      }
    } finally {
      pendingSells.delete(sellId);
    }
  }

  // ── 6. Start active services ─────────────────────────────────────────────
  await startFunderFirstModeServices();
  await resumePrimaryInsiderBot();

  // Only now, with every orchestrator/bot fully wired up, do we start
  // draining Telegram's update queue — see the note above
  // `funderFirstOrchestrator`'s construction for why this was moved down
  // from right after `new TelegramBot(...)`.
  if (telegramBot) {
    telegramBot.start();
  }
  startMarketCapChecker();

  log.info(
    `Service fully started — insider bots=${insiderBots.length}, funder-first running=${funderFirstOrchestrator.isRunning()}`,
  );

  // Send a one-off Telegram summary so it's obvious the process actually
  // came up (and with what config) without having to check server logs.
  await telegramBot
    ?.sendDefault(
      [
        "<b>🟢 Bot Started</b>",
        "",
        "<b>Insider</b>",
        `Enabled bots: <b>${insiderBots.length}</b> (Helius key pool)`,
        `Bot 1 follow wallet: ${insiderBots[0]?.getFollowedWallet() ? `<code>${html(insiderBots[0]!.getFollowedWallet()!)}</code>` : "not set"}`,
        `Bot 1 running: <b>${insiderBots[0]?.isRunning() ? "yes" : "no"}</b>`,
        "",
        "<b>Funder-First</b>",
        `Funder address: ${funderFirstOrchestrator.getFunderAddress() ? `<code>${html(funderFirstOrchestrator.getFunderAddress()!)}</code>` : "not set"}`,
        `Running: <b>${funderFirstOrchestrator.isRunning() ? "yes" : "no"}</b>`,
        `Watched potential feePayers: <b>${funderFirstOrchestrator.getWatchedPotentialFeePayers().length}</b>`,
        "",
        `Trading wallet: ${config.tradingWalletAddress ? `<code>${html(config.tradingWalletAddress)}</code>` : "not set"}`,
        `Watched wallets: <b>${walletMonitors.size}</b>`,
        `Health server port: <b>${config.port}</b>`,
        "",
        "Send /start or /status any time for a live status card.",
      ].join("\n"),
    )
    .catch((err) => log.warn("Telegram startup summary failed to send", err));

  // ── 7. Graceful shutdown ──────────────────────────────────────────────────
  let shutting_down = false;
  requestFullShutdown = (signal: string): void => {
    void shutdown(signal);
  };

  async function shutdown(signal: string): Promise<void> {
    if (shutting_down) return;
    shutting_down = true;

    log.info(`Received ${signal} — shutting down gracefully`);

    for (const monitor of walletMonitors.values()) {
      monitor.stop();
    }
    telegramBot?.stop();
    healthServer.close();

    await funderFirstOrchestrator.shutdown();
    sharedEnhancedWs?.close();

    await Promise.all(
      [...gmgnLimiters, gmgnFallbackLimiter].map((limiter, index) =>
        limiter
          .drain()
          .catch((e) =>
            log.warn(
              index < gmgnLimiters.length
                ? `GMGN limiter ${index + 1} drain error`
                : "GMGN fallback limiter drain error",
              e,
            ),
          ),
      ),
    );
    db.close();

    log.info("Shutdown complete");
    process.exit(0);
  }

  process.on("SIGINT", () => void shutdown("SIGINT"));
  process.on("SIGTERM", () => void shutdown("SIGTERM"));

  // Keep the process alive
  await new Promise<never>(() => {});
}

main().catch((err) => {
  console.error("[FATAL]", err);
  process.exit(1);
});
