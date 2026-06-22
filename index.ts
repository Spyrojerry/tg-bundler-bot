// ─────────────────────────────────────────────────────────────────────────────
//  index.ts  —  Service entry point
//
//  Boot sequence:
//    1. Load + validate environment config
//    2. Open SQLite database
//    3. Initialise RateLimiter + GmgnClient
//    4. Initialise EarlyBundlerOrchestrator
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
  TokenExitEvent,
} from "./types";
import { EarlyBundlerOrchestrator } from "./early-bundler-orchestrator";
import { ReverseCopySellOrchestrator } from "./reverse-copysell-orchestrator";
import { InsiderBot } from "./insider-bot";
import type { InsiderMintClaimFn } from "./insider-bot";
import { PublicKey } from "@solana/web3.js";
import { randomBytes } from "crypto";

const log = createLogger("MAIN");
const sleep = (ms: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, ms));
const INSIDER_MIN_MARKET_CAP_USD = 5_000;
const MCAP_CHECK_INTERVAL_MS = 1000; // Increased frequency from 1500ms to 1000ms

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
      followWallet: config.insiderFollowWallet2,
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
      followWallet: config.insiderFollowWallet3,
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
      followWallet: config.insiderFollowWallet4,
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

  let telegramBot: TelegramBot | null = null;
  const insiderBots: InsiderBot[] = [];
  let activeInsiderIndex = 0; // Which insider bot we are currently viewing/configuring in Telegram

  // ── 4. Early Bundler Orchestrator ─────────────────────────────────────────
  let earlyBundlerOrchestrator: EarlyBundlerOrchestrator;
  let reverseCopySellOrchestrator: ReverseCopySellOrchestrator;
  let botMode: "insider" | "bundler" | "reverse_copysell" =
    config.defaultBotMode;
  let bundlerBuyInProgress = false;

  const healthServer = startHealthServer(config.port);
  const walletMonitors = new Map<string, WalletMonitor>();
  let tradingWalletMonitor: WalletMonitor | null = null;
  const pendingTradingBuys = new Map<string, NewTokenEvent>();
  type PendingTelegramAction =
    | { type: "addwallet" | "removewallet" }
    | { type: "minSol"; walletAddress: string }
    | {
        type:
          | "insiderFollowWallet"
          | "insiderBuySol"
          | "insiderExitPercent"
          | "insiderBundlerMinUsd"
          | "insiderBundlerMaxUsd";
        index: number;
      }
    | { type: "bundlerFollowWallet" | "bundlerBuySol" | "bundlerExitPercent" }
    | { type: "reverseTargetWallet" };
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
          let context: "insider" | "bundler" | "reverse_copysell" = "bundler";
          let resolvedBotIndex: number | null = null;

          if (contextCode === "b") {
            context = "bundler";
          } else if (contextCode === "r") {
            context = "reverse_copysell";
          } else if (contextCode === "i" || contextCode?.startsWith("i")) {
            context = "insider";
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
          const effectiveBotIndex =
            context === "insider" && botIndex !== -1 ? botIndex : 0;
          const client =
            context === "insider"
              ? gmgnClients[effectiveBotIndex]
              : gmgnClients[0];

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
                context === "insider"
                  ? client.fetchTokenAthMarketCapUsd(mint)
                  : Promise.resolve(null),
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

          let buySol = 0;
          let entryMc: number | null = null;
          let exitMc: number | null = null;

          if (context === "insider") {
            const bot =
              botIndex !== -1 ? insiderBots[botIndex] : insiderBots[0];
            buySol = bot.getBuySol();
            entryMc = bot.getEntryMc();
            exitMc = bot.getExitMc();
          } else if (context === "bundler") {
            const position = earlyBundlerOrchestrator.getActivePosition();
            buySol = position?.buySol ?? earlyBundlerOrchestrator.getBuySol();
            entryMc = position?.entryMc ?? null;
            exitMc =
              entryMc !== null
                ? entryMc *
                  (1 + earlyBundlerOrchestrator.getExitPercent() / 100)
                : null;
          } else if (context === "reverse_copysell") {
            buySol =
              reverseCopySellOrchestrator.getActivePosition()?.buySol ?? 0;
          }

          if (buySol === 0 && config.tradingWalletAddress) {
            const dbPosition = db.getActiveEarlyBundlerPosition(
              config.tradingWalletAddress,
              mint,
            );
            if (dbPosition?.buySol) {
              buySol = dbPosition.buySol;
            }
          }

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

          const refreshContextCode =
            context === "insider"
              ? `i${effectiveBotIndex}`
              : context === "reverse_copysell"
                ? "r"
                : "b";

          const lines = [
            context === "insider"
              ? `<b>Insider ${getInsiderBotNumber(effectiveBotIndex)} Position Update</b>`
              : context === "reverse_copysell"
                ? "<b>Reverse CopySell Update</b>"
                : "<b>Bundler Position Update</b>",
            `Token: <code>${html(mint)}</code>`,
            entryMc !== null
              ? `Entry MC: <b>$${entryMc.toLocaleString()}</b>`
              : null,
            `Market Cap: <b>$${currentMarketCapUsd?.toLocaleString() ?? "Unknown"}</b>`,
            context === "insider" && athMarketCapUsd !== null
              ? `ATH MC: <b>$${athMarketCapUsd.toLocaleString()}</b>`
              : null,
            context === "insider" && exitMc !== null
              ? `Exit MC: <b>$${exitMc.toLocaleString()}</b>`
              : context === "bundler" && exitMc !== null
                ? `Exit MC: <b>$${exitMc.toLocaleString()}</b>`
                : null,
            profitDisplay,
            tokenBalanceLine,
            "",
            `Last Updated: ${new Date().toISOString()}`,
          ].filter(Boolean) as string[];

          const buttons = [];

          if (context === "insider" && !balanceIsZero) {
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
        if (data === "mode:insider") {
          await stopBundlerModeServices("Switched to Insider mode");
          await stopReverseCopySellModeServices("Switched to Insider mode");
          botMode = "insider";
          await resumeAllInsiderBots();
          return homeReply(true);
        }
        if (data === "mode:bundler") {
          for (const bot of insiderBots) bot.pause();
          await stopReverseCopySellModeServices("Switched to Bundler mode");
          botMode = "bundler";
          await startBundlerModeServices();
          return homeReply(true);
        }
        if (data === "bundler:follow") {
          pendingTelegramActions.set(chatId, { type: "bundlerFollowWallet" });
          return {
            text: "Send the wallet address for Bundler mode to follow.",
            trackPrompt: true,
            editCurrent: true,
          };
        }
        if (data === "bundler:buysol") {
          pendingTelegramActions.set(chatId, { type: "bundlerBuySol" });
          return {
            text: "Send the SOL amount Bundler mode should buy with.",
            trackPrompt: true,
            editCurrent: true,
          };
        }
        if (data === "bundler:exitpercent") {
          pendingTelegramActions.set(chatId, { type: "bundlerExitPercent" });
          return {
            text: "Send the Bundler exit profit percentage increase.\nExample: <code>50</code> for a 50% increase from entry market cap.",
            trackPrompt: true,
            editCurrent: true,
          };
        }
        if (data === "bundler:stop") {
          await earlyBundlerOrchestrator.stopActiveMonitoring(
            "Stopped from Telegram",
          );
          return homeReply(true);
        }
        if (data === "bundler:resume") {
          const followedWallet = earlyBundlerOrchestrator.getFollowedWallet();
          if (!followedWallet) {
            pendingTelegramActions.set(chatId, { type: "bundlerFollowWallet" });
            return {
              text: "Send the wallet address for Bundler mode to follow.",
              trackPrompt: true,
              editCurrent: true,
            };
          }
          earlyBundlerOrchestrator.setEnabled(true);
          await earlyBundlerOrchestrator.followWallet(followedWallet);
          return homeReply(true);
        }
        if (data.startsWith("insider:select:")) {
          const selectedIndex = parseInt(data.split(":")[2], 10);
          if (
            !Number.isInteger(selectedIndex) ||
            selectedIndex < 0 ||
            selectedIndex >= insiderBots.length
          ) {
            return "Invalid Insider bot selection.";
          }
          activeInsiderIndex = selectedIndex;
          return homeReply(true);
        }
        if (data === "insider:follow") {
          pendingTelegramActions.set(chatId, {
            type: "insiderFollowWallet",
            index: activeInsiderIndex,
          });
          return {
            text: `[Bot ${getInsiderBotNumber(activeInsiderIndex)}] Send the wallet address for Insider Bot to follow.`,
            trackPrompt: true,
            editCurrent: true,
          };
        }
        if (data === "insider:buysol") {
          pendingTelegramActions.set(chatId, {
            type: "insiderBuySol",
            index: activeInsiderIndex,
          });
          return {
            text: `[Bot ${getInsiderBotNumber(activeInsiderIndex)}] Send the SOL amount Insider Bot should buy with.`,
            trackPrompt: true,
            editCurrent: true,
          };
        }
        if (data === "insider:exitpercent") {
          pendingTelegramActions.set(chatId, {
            type: "insiderExitPercent",
            index: activeInsiderIndex,
          });
          return {
            text: `[Bot ${getInsiderBotNumber(activeInsiderIndex)}] Send the Exit profit percentage increase.\nExample: <code>40</code> for a 40% ATH MC increase from your entry point.`,
            trackPrompt: true,
            editCurrent: true,
          };
        }
        if (data === "insider:bundlermin") {
          pendingTelegramActions.set(chatId, {
            type: "insiderBundlerMinUsd",
            index: activeInsiderIndex,
          });
          return {
            text: `[Bot ${getInsiderBotNumber(activeInsiderIndex)}] Send the minimum bundler buy USD.\nExample: <code>100</code>`,
            trackPrompt: true,
            editCurrent: true,
          };
        }
        if (data === "insider:bundlermax") {
          pendingTelegramActions.set(chatId, {
            type: "insiderBundlerMaxUsd",
            index: activeInsiderIndex,
          });
          return {
            text: `[Bot ${getInsiderBotNumber(activeInsiderIndex)}] Send the maximum bundler buy USD.\nExample: <code>150</code>`,
            trackPrompt: true,
            editCurrent: true,
          };
        }
        if (data === "insider:togglebuy") {
          const bot = insiderBots[activeInsiderIndex];
          bot.setBuyDisabled(!bot.isBuyDisabled());
          return homeReply(true);
        }
        if (data === "insider:stop") {
          await insiderBots[activeInsiderIndex].stop();
          return homeReply(true);
        }
        if (data === "insider:resume") {
          const bot = insiderBots[activeInsiderIndex];
          const followedWallet = bot.getFollowedWallet();
          if (!followedWallet) {
            pendingTelegramActions.set(chatId, {
              type: "insiderFollowWallet",
              index: activeInsiderIndex,
            });
            return {
              text: `[Bot ${getInsiderBotNumber(activeInsiderIndex)}] Send the wallet address for Insider Bot to follow.`,
              trackPrompt: true,
              editCurrent: true,
            };
          }
          await bot.followWallet(followedWallet);
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
          if (callbackAction === "set_target") {
            pendingTelegramActions.set(chatId, { type: "reverseTargetWallet" });
            return {
              text: "Send the Solana wallet address for Reverse CopySell to watch.",
              trackPrompt: true,
              editCurrent: true,
            };
          }
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
            const bot = insiderBots[pendingAction.index];
            await bot.followWallet(text);
            return homeReply();
          }
          if (pendingAction.type === "insiderBuySol") {
            const bot = insiderBots[pendingAction.index];
            const value = Number(text.trim());
            if (!Number.isFinite(value) || value <= 0)
              return "Send a SOL amount greater than 0.";
            bot.setBuySol(value);
            return homeReply();
          }
          if (pendingAction.type === "insiderExitPercent") {
            const bot = insiderBots[pendingAction.index];
            const value = Number(text.trim());
            if (!Number.isFinite(value) || value < 0)
              return "Send a valid percentage.";
            bot.setExitPercent(value);
            return homeReply();
          }
          if (pendingAction.type === "insiderBundlerMinUsd") {
            const bot = insiderBots[pendingAction.index];
            const value = Number(text.trim());
            if (!Number.isFinite(value) || value < 0)
              return "Send a valid USD amount.";
            bot.setBundlerBuyMinUsd(value);
            return homeReply();
          }
          if (pendingAction.type === "insiderBundlerMaxUsd") {
            const bot = insiderBots[pendingAction.index];
            const value = Number(text.trim());
            if (!Number.isFinite(value) || value < 0)
              return "Send a valid USD amount.";
            bot.setBundlerBuyMaxUsd(value);
            return homeReply();
          }
          if (pendingAction.type === "bundlerFollowWallet") {
            await earlyBundlerOrchestrator.followWallet(text);
            return homeReply();
          }
          if (pendingAction.type === "bundlerBuySol") {
            const value = Number(text.trim());
            if (!Number.isFinite(value) || value <= 0)
              return "Send a SOL amount greater than 0.";
            earlyBundlerOrchestrator.setBuySol(value);
            return homeReply();
          }
          if (pendingAction.type === "bundlerExitPercent") {
            const value = Number(text.trim());
            if (!Number.isFinite(value) || value < 0)
              return "Send a valid percentage.";
            earlyBundlerOrchestrator.setExitPercent(value);
            return homeReply();
          }
          if (pendingAction.type === "reverseTargetWallet") {
            const trimmed = text.trim().toLowerCase();
            if (
              trimmed === "off" ||
              trimmed === "none" ||
              trimmed === "clear"
            ) {
              config.reverseCopySellTargetWallet = null;
              return homeReply();
            }
            try {
              const pubkey = new PublicKey(text.trim());
              config.reverseCopySellTargetWallet = pubkey.toBase58();
              return homeReply();
            } catch {
              return 'Invalid Solana wallet address. Send a valid address, or "off" to clear.';
            }
          }
        }
        if (botMode === "insider") {
          await insiderBots[activeInsiderIndex].followWallet(text);
          return homeReply();
        }
        return walletSummaryReply(text);
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
      gmgnClients[index],
      gmgnClients[index],
      definition.heliusApiKey,
      definition.heliusProjectId,
      telegramBot,
      makeClaimFn(index),
      () => undefined,
      `Insider ${definition.botNumber}`,
    );
    bot.on("heliusCreditsExhausted", (info) => {
      void (async () => {
        await bot.stopForHeliusCredits();
        await telegramBot?.sendDefault(
          [
            `<b>🛑 Insider ${definition.botNumber} Stopped — Helius Credits Exhausted</b>`,
            `Project: <code>${html(info.projectId)}</code>`,
            `Plan: <b>${html(info.usage.subscriptionDetails?.plan ?? "Unknown")}</b>`,
            `Credits remaining: <b>${html(String(info.usage.creditsRemaining))}</b>`,
            `Prepaid credits remaining: <b>${html(String(info.usage.prepaidCreditsRemaining))}</b>`,
            "",
            "A Helius request returned 429 and the Admin API confirmed that this project has no remaining credits.",
            "This bot has been stopped. Other Insider bots continue running.",
          ].join("\n"),
          { pin: true },
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

  async function resumeAllInsiderBots(): Promise<void> {
    for (let i = 0; i < insiderBots.length; i++) {
      const bot = insiderBots[i];
      const wallet = bot.getFollowedWallet();
      if (
        wallet &&
        !bot.isStoppedForHeliusCredits() &&
        !bot.isRunning() &&
        !bot.getActivePosition() &&
        !bot.getPreBuyMint()
      ) {
        await bot.followWallet(wallet);
        log.info(
          `[INSIDER ${getInsiderBotNumber(i)}] Resumed follow-wallet monitoring`,
          {
            wallet,
          },
        );
      }
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

  if (telegramBot) {
    telegramBot.start();
  }

  earlyBundlerOrchestrator = new EarlyBundlerOrchestrator(
    config,
    db,
    telegramBot,
    gmgnClients[0],
  );
  reverseCopySellOrchestrator = new ReverseCopySellOrchestrator(
    config,
    db,
    telegramBot,
  );

  earlyBundlerOrchestrator.on("buyTrigger", (trigger) => {
    if (botMode !== "bundler") {
      log.info(
        "[BUNDLER BUY SKIP] Ignoring buy trigger because Bundler mode is inactive",
        {
          mint: trigger.position.mint,
          mode: botMode,
        },
      );
      return;
    }

    if (!config.tradingWalletAddress) {
      log.warn("[BUNDLER BUY SKIP] No trading wallet configured", trigger);
      earlyBundlerOrchestrator.clearActivePosition();
      return;
    }

    void (async () => {
      const { position, matchedWallet } = trigger;
      bundlerBuyInProgress = true;
      try {
        const result = await gmgnClients[0].buyTokenWithSol(
          config.tradingWalletAddress!,
          position.mint,
          {
            solAmount: earlyBundlerOrchestrator.getBuySol(),
            slippage: config.sellSlippage,
            autoSlippage: config.sellAutoSlippage,
            priorityFeeSol: config.sellPriorityFeeSol,
          },
        );

        const entryMc = await gmgnClients[0]
          .fetchTokenMarketCapUsd(position.mint)
          .catch(() => null);
        earlyBundlerOrchestrator.markPositionBought(
          {
            ...position,
            tradingWallet: config.tradingWalletAddress!,
            buySol: earlyBundlerOrchestrator.getBuySol(),
          },
          entryMc,
        );

        const exitMc =
          entryMc !== null
            ? entryMc * (1 + earlyBundlerOrchestrator.getExitPercent() / 100)
            : null;

        await telegramBot?.sendDefault(
          [
            "<b>✅ Bundler Buy Completed</b>",
            `Token: <code>${html(position.mint)}</code>`,
            `Repeated feePayer: <code>${html(matchedWallet)}</code>`,
            `Actual Entry MC: <b>$${html(entryMc?.toLocaleString() ?? "Unknown")}</b>`,
            exitMc !== null
              ? `Exit MC: <b>$${html(exitMc.toLocaleString())}</b>`
              : `Exit: <b>+${html(String(earlyBundlerOrchestrator.getExitPercent()))}% after entry MC is known</b>`,
            `Status: <b>${html(result.status)}</b>`,
            result.hash ? `Tx: https://solscan.io/tx/${html(result.hash)}` : "",
          ]
            .filter(Boolean)
            .join("\n"),
          {
            replyMarkup: {
              inline_keyboard: [
                [
                  {
                    text: "🔄 Refresh P/L & MC",
                    callback_data: `r:m:${position.mint}:b`,
                  },
                ],
              ],
            },
          },
        );
      } catch (err) {
        log.error("Bundler buy failed", err);
        earlyBundlerOrchestrator.clearActivePosition();
        await telegramBot?.sendDefault(
          [
            "<b>❌ Bundler Buy Failed</b>",
            `Token: <code>${html(position.mint)}</code>`,
            `Error: ${html(err instanceof Error ? err.message : String(err))}`,
          ].join("\n"),
        );
      } finally {
        bundlerBuyInProgress = false;
      }
    })();
  });

  earlyBundlerOrchestrator.on("sellTrigger", (trigger) => {
    if (botMode !== "bundler") {
      log.info(
        "[BUNDLER SELL SKIP] Ignoring sell trigger because Bundler mode is inactive",
        {
          mint: trigger.position.mint,
          mode: botMode,
          reason: trigger.reason,
        },
      );
      return;
    }

    if (!config.tradingWalletAddress) {
      log.warn("[BUNDLER SELL SKIP] No trading wallet configured", trigger);
      return;
    }

    if (
      hasPendingSellForMint(config.tradingWalletAddress, trigger.position.mint)
    ) {
      log.info(
        `[BUNDLER SELL SKIP] Sell already pending for ${trigger.position.mint}`,
      );
      return;
    }

    void (async () => {
      const owner = new PublicKey(config.tradingWalletAddress!);
      const mintPk = new PublicKey(trigger.position.mint);
      const balance = await getTokenRawBalance(owner, mintPk).catch(() => 0n);

      if (balance <= 0n) {
        log.info(
          "[BUNDLER SELL SKIP] No position held; resetting orchestrator",
          {
            mint: trigger.position.mint,
            reason: trigger.reason,
          },
        );
        earlyBundlerOrchestrator.clearActivePosition();
        return;
      }

      const event: FilterFailEvent = {
        walletAddress: config.tradingWalletAddress!,
        mint: trigger.position.mint,
        sampleNumber: 0,
        elapsedSec: 0,
        reasons: [
          trigger.reason ??
            "Bundler creator_hold_rate is 0; selling existing position.",
        ],
        settings: db.getWalletSettings(config.tradingWalletAddress!),
        metrics: {
          mint: trigger.position.mint,
          timestamp: new Date().toISOString(),
          bundlersPercent: null,
          bundlersCount: null,
          initialBaseReserve: null,
          topWallets: null,
          top10HolderRate: null,
          bundledAmountRate: null,
        },
        buySol: trigger.position.buySol,
        matchingWallets: trigger.position.matchedWallet
          ? [trigger.position.matchedWallet]
          : [],
      };

      const sellId = randomBytes(5).toString("hex");
      activePositionCache.set(trigger.position.mint, {
        balance,
        quote: null,
        timestamp: Date.now(),
      });
      pendingSells.set(sellId, {
        event,
        createdAt: Date.now(),
        executing: true,
      });

      telegramBot
        ?.sendDefault(
          [
            "<b>🚨 Bundler Creator Hold Sell Triggered</b>",
            `Token: <code>${html(trigger.position.mint)}</code>`,
            `Reason: <b>${html(trigger.reason ?? "creator_hold_rate is 0")}</b>`,
            `Action: submit sell for <b>${config.sellPercent}%</b>.`,
          ].join("\n"),
        )
        .catch((err) =>
          log.warn("Telegram bundler creator-hold sell alert failed", err),
        );

      void executeSellAndNotify(config.telegramChatId, sellId, telegramBot);
    })();
  });

  reverseCopySellOrchestrator.on("sellTrigger", (data) => {
    if (botMode !== "reverse_copysell") return;

    const { position, targetWallet } = data;

    // Check if sell already pending
    if (hasPendingSellForMint(position.tradingWallet, position.mint)) {
      log.info(
        `[REVERSE-COPYSELL SELL SKIP] Sell already pending for ${position.mint}`,
      );
      return;
    }

    pendingTradingBuys.delete(position.mint);

    const event: FilterFailEvent = {
      walletAddress: position.tradingWallet,
      mint: position.mint,
      sampleNumber: 0,
      elapsedSec: 0,
      reasons: [
        `Reverse CopySell trigger: Target wallet ${targetWallet} bought the token`,
      ],
      settings: db.getWalletSettings(position.tradingWallet),
      metrics: {
        mint: position.mint,
        timestamp: new Date().toISOString(),
        bundlersPercent: null,
        bundlersCount: null,
        initialBaseReserve: null,
        topWallets: null,
        top10HolderRate: null,
        bundledAmountRate: null,
      },
      buySol: position.buySol,
      matchingWallets: [targetWallet],
    };

    const sellId = randomBytes(5).toString("hex");
    pendingSells.set(sellId, {
      event,
      createdAt: Date.now(),
      executing: true,
    });

    log.info(
      `[REVERSE-COPYSELL SELL TRIGGER] Target ${targetWallet} for ${position.mint}`,
      {
        sellId,
      },
    );

    void executeSellAndNotify(config.telegramChatId, sellId, telegramBot);
  });

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
                "<b>Strategy: ATH MC Exit</b>",
                `Exit when ATH MC reaches: <b>$${html(bot.getExitMc().toLocaleString())}</b>`,
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

  function wireTradingWalletMonitor(walletMonitor: WalletMonitor): void {
    walletMonitor.on("newToken", (event: NewTokenEvent) => {
      if (botMode === "reverse_copysell") {
        log.info(
          `[REVERSE-COPYSELL POSITION OPEN] Wallet: ${event.walletAddress} Mint: ${event.mint}`,
        );
        reverseCopySellOrchestrator
          .handleTradingWalletBuy(event)
          .catch((err) => {
            log.error("Failed to trigger reverse-copysell detection", err);
          });
      }
    });

    walletMonitor.on("tokenExited", (event: TokenExitEvent) => {
      if (botMode === "reverse_copysell") {
        log.info(
          `[REVERSE-COPYSELL POSITION EXITED] Wallet: ${event.walletAddress} Mint: ${event.mint}`,
        );
        reverseCopySellOrchestrator
          .handleTradingWalletExit(event)
          .catch((err) => {
            log.error(
              "Failed to notify reverse-copysell orchestrator of exit",
              err,
            );
          });
      }
    });
  }

  function wireWatchedWalletMonitor(walletMonitor: WalletMonitor): void {
    walletMonitor.on("newToken", (event: NewTokenEvent) => {
      if (botMode !== "bundler") return;
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

  async function checkAndSellIfLowMcap(
    mint: string,
    context: "insider" | "bundler" | "reverse_copysell",
    botIndex?: number,
    preFetchedMc?: number | null,
  ): Promise<void> {
    if (!config.tradingWalletAddress) return;
    if (hasPendingSellForMint(config.tradingWalletAddress, mint)) return;

    try {
      const client =
        context === "insider" && botIndex !== undefined
          ? gmgnClients[botIndex]
          : gmgnClients[0];
      const currentMc =
        preFetchedMc !== undefined
          ? preFetchedMc
          : await client.fetchTokenMarketCapUsd(mint);
      if (currentMc !== null && currentMc < INSIDER_MIN_MARKET_CAP_USD) {
        log.warn(
          `[MCAP SELL TRIGGER] Market cap $${currentMc.toLocaleString()} below $${INSIDER_MIN_MARKET_CAP_USD.toLocaleString()} for ${mint}`,
          { context },
        );

        // Check Cache for balance first
        const cached = activePositionCache.get(mint);
        let balance: bigint | null = cached ? cached.balance : null;

        if (balance === null && config.tradingWalletAddress) {
          const owner = new PublicKey(config.tradingWalletAddress);
          const mintPk = new PublicKey(mint);
          balance = await getTokenRawBalance(owner, mintPk).catch(() => 0n);
        }

        if (balance !== null && balance <= 0n) {
          log.info(`[MCAP SELL SKIP] No balance found for ${mint}`);
          return;
        }

        const event: FilterFailEvent = {
          walletAddress: config.tradingWalletAddress,
          mint,
          sampleNumber: 0,
          elapsedSec: 0,
          reasons: [
            `Market cap $${currentMc.toLocaleString()} fell below $${INSIDER_MIN_MARKET_CAP_USD.toLocaleString()}.`,
            `Context: ${context} position.`,
            "Periodic market cap checker triggered automatic sell.",
          ],
          settings: db.getWalletSettings(config.tradingWalletAddress),
          metrics: {
            mint,
            timestamp: new Date().toISOString(),
            bundlersPercent: null,
            bundlersCount: null,
            initialBaseReserve: null,
            topWallets: null,
            top10HolderRate: null,
            bundledAmountRate: null,
          },
          buySol:
            context === "insider"
              ? (
                  insiderBots.find(
                    (b) => b.getActivePosition()?.mint === mint,
                  ) || insiderBots[0]
                ).getBuySol()
              : context === "reverse_copysell"
                ? (reverseCopySellOrchestrator.getActivePosition()?.buySol ??
                  null)
                : (earlyBundlerOrchestrator.getActivePosition()?.buySol ??
                  null),
          matchingWallets: [],
        };

        const sellId = randomBytes(5).toString("hex");
        pendingSells.set(sellId, {
          event,
          createdAt: Date.now(),
          executing: true,
        });

        if (context === "insider") {
          // Keep the active position until the sell is confirmed. The pending
          // sell map suppresses duplicate MC-triggered attempts meanwhile.
        } else if (context === "bundler") {
          earlyBundlerOrchestrator.clearActivePosition();
          pendingTradingBuys.delete(mint);
        } else if (context === "reverse_copysell") {
          reverseCopySellOrchestrator.stopActiveMonitoring(
            "Market cap fell below minimum",
          );
        }

        telegramBot
          ?.sendDefault(
            [
              "<b>🚨 Market Cap Sell Triggered</b>",
              `Token: <code>${html(mint)}</code>`,
              `Market Cap: <b>$${currentMc.toLocaleString()}</b>`,
              `Threshold: <b>$${INSIDER_MIN_MARKET_CAP_USD.toLocaleString()}</b>`,
              `Action: submit sell for <b>${config.sellPercent}%</b>.`,
            ].join("\n"),
          )
          .catch((err) => log.warn("Telegram mcap sell alert failed", err));

        void executeSellAndNotify(config.telegramChatId, sellId, telegramBot);
      }
    } catch (err) {
      log.error(`Failed to check market cap for ${mint}`, err);
    }
  }

  async function checkBundlerMcapFlow(
    preFetchedMc?: number | null,
  ): Promise<void> {
    const position = earlyBundlerOrchestrator.getActivePosition();
    if (bundlerBuyInProgress) return;
    if (!position || !config.tradingWalletAddress) return;
    if (hasPendingSellForMint(config.tradingWalletAddress, position.mint))
      return;

    try {
      const currentMc =
        preFetchedMc !== undefined
          ? preFetchedMc
          : await gmgnClients[0].fetchTokenMarketCapUsd(position.mint);
      if (currentMc === null) return;

      await checkAndSellIfLowMcap(
        position.mint,
        "bundler",
        undefined,
        currentMc,
      );
      if (!earlyBundlerOrchestrator.getActivePosition()) return;

      let entryMc = position.entryMc ?? null;
      if (entryMc === null) {
        entryMc = currentMc;
        earlyBundlerOrchestrator.markPositionBought(position, entryMc);
      }

      const exitMc =
        entryMc * (1 + earlyBundlerOrchestrator.getExitPercent() / 100);
      if (currentMc < exitMc) return;

      const event: FilterFailEvent = {
        walletAddress: config.tradingWalletAddress,
        mint: position.mint,
        sampleNumber: 0,
        elapsedSec: 0,
        reasons: [
          `Bundler exit market cap $${currentMc.toLocaleString()} reached target $${exitMc.toLocaleString()}.`,
          `Exit percentage: ${earlyBundlerOrchestrator.getExitPercent()}% from entry MC $${entryMc.toLocaleString()}.`,
        ],
        settings: db.getWalletSettings(config.tradingWalletAddress),
        metrics: {
          mint: position.mint,
          timestamp: new Date().toISOString(),
          bundlersPercent: null,
          bundlersCount: null,
          initialBaseReserve: null,
          topWallets: null,
          top10HolderRate: null,
          bundledAmountRate: null,
        },
        buySol: earlyBundlerOrchestrator.getBuySol(),
        matchingWallets: position.matchedWallet ? [position.matchedWallet] : [],
      };

      const owner = new PublicKey(config.tradingWalletAddress);
      const mintPk = new PublicKey(position.mint);
      const balance = await getTokenRawBalance(owner, mintPk).catch(() => 0n);

      if (balance <= 0n) {
        log.info(
          "[BUNDLER MCAP SELL SKIP] No position held; resetting orchestrator",
          {
            mint: position.mint,
          },
        );
        earlyBundlerOrchestrator.clearActivePosition();
        return;
      }

      const sellId = randomBytes(5).toString("hex");
      pendingSells.set(sellId, {
        event,
        createdAt: Date.now(),
        executing: true,
      });

      earlyBundlerOrchestrator.clearActivePosition();

      telegramBot
        ?.sendDefault(
          [
            "<b>🚨 Bundler Exit Triggered</b>",
            `Token: <code>${html(position.mint)}</code>`,
            `Market Cap: <b>$${html(currentMc.toLocaleString())}</b>`,
            `Exit MC: <b>$${html(exitMc.toLocaleString())}</b>`,
            `Action: submit sell for <b>${config.sellPercent}%</b>.`,
          ].join("\n"),
        )
        .catch((err) => log.warn("Telegram bundler exit alert failed", err));

      void executeSellAndNotify(config.telegramChatId, sellId, telegramBot);
    } catch (err) {
      log.error(`Failed to check Bundler MC flow for ${position.mint}`, err);
    }
  }

  async function checkInsiderMcapFlow(
    index: number,
    preFetchedMc?: number | null,
  ): Promise<void> {
    const bot = insiderBots[index];
    const client = gmgnClients[index];
    const botNumber = getInsiderBotNumber(index);
    if (bot.isStoppedForHeliusCredits()) return;
    const preBuyMint = bot.getPreBuyMint();
    const activePos = bot.getActivePosition();

    if (!preBuyMint && !activePos) return;

    const mint = preBuyMint || activePos!.mint;

    try {
      const currentMc =
        preFetchedMc !== undefined
          ? preFetchedMc
          : await client.fetchTokenMarketCapUsd(mint);

      if (currentMc === null) {
        log.debug(
          `[INSIDER ${botNumber} MC SKIP] Could not fetch market cap for ${mint}`,
        );
        return;
      }

      log.info(
        `[INSIDER ${botNumber} MC CHECK] Token: ${mint} MC: $${currentMc.toLocaleString()} (Source: ${preFetchedMc !== undefined ? "Prefetched" : "Client"})`,
      );

      if (currentMc < INSIDER_MIN_MARKET_CAP_USD) {
        const reason = `Market cap $${currentMc.toLocaleString()} below $${INSIDER_MIN_MARKET_CAP_USD.toLocaleString()} (Rug)`;
        if (
          activePos &&
          config.tradingWalletAddress &&
          hasPendingSellForMint(config.tradingWalletAddress, mint)
        ) {
          log.info(
            `[INSIDER ${botNumber} RUG] Sell already pending for ${mint}; MC monitoring remains active`,
            { currentMc },
          );
          return;
        }
        log.warn(
          `[INSIDER ${botNumber} RUG] ${reason} for ${mint}. ${activePos ? "Triggering sell." : "Resetting pre-buy state."}`,
        );

        const preBuyOnly = !!preBuyMint && !activePos;

        if (activePos) {
          bot.emit("sellTrigger", {
            followedWallet: bot.getFollowedWallet()!,
            positionMint: mint,
            signature: "MC_TRIGGER",
            reason: `Rug protection: ${reason}`,
          });
        }

        if (preBuyOnly) {
          telegramBot
            ?.sendDefault(
              [
                "<b>🧹 Rug Reset — Token Skipped</b>",
                `Bot: <b>${botNumber}</b>`,
                `Token: <code>${html(mint)}</code>`,
                `Market cap: <b>$${currentMc.toLocaleString()}</b>`,
                `Rug threshold: <b>$${INSIDER_MIN_MARKET_CAP_USD.toLocaleString()}</b>`,
                "Token rugged during insider/bundler monitoring before buy.",
                "Flow reset — waiting for next follow-wallet buy.",
              ].join("\n"),
            )
            .catch((err) =>
              log.warn("Telegram insider rug reset alert failed", err),
            );
        }

        if (preBuyOnly) {
          bot.clearPreBuyMint();
        }
        return;
      }

      if (activePos) {
        const exitMc = bot.getExitMc();
        const athMc = await client.fetchTokenAthMarketCapUsd(mint);
        if (athMc !== null && athMc >= exitMc) {
          log.warn(
            `[INSIDER ${botNumber} EXIT] ATH MC $${athMc.toLocaleString()} reached Exit MC $${exitMc.toLocaleString()}. Triggering SELL.`,
          );
          bot.emit("sellTrigger", {
            followedWallet: bot.getFollowedWallet()!,
            positionMint: activePos.mint,
            signature: "MC_TRIGGER",
            reason: `ATH MC $${athMc.toLocaleString()} reached target $${exitMc.toLocaleString()}`,
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
                const client = gmgnClients[i];
                const currentMc =
                  await client.fetchTokenMarketCapUsd(preBuyMint);
                if (currentMc !== null) {
                  await checkInsiderMcapFlow(i, currentMc);
                }
              })(),
            );
          }

          if (activePos) {
            // Background refresh balance and quote if missing or old (> 30s)
            const cached = activePositionCache.get(activePos.mint);
            if (
              config.tradingWalletAddress &&
              (!cached || Date.now() - cached.timestamp > 30_000)
            ) {
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
                .catch((e) =>
                  log.error(
                    `Background balance/quote refresh failed for ${activePos.mint}`,
                    e,
                  ),
                );
            }

            tasks.push(
              (async () => {
                const client = gmgnClients[i];
                const currentMc = await client.fetchTokenMarketCapUsd(
                  activePos.mint,
                );
                if (currentMc !== null) {
                  // checkInsiderMcapFlow handles Exit MC and Flow v2 for active positions
                  await checkInsiderMcapFlow(i, currentMc);
                  await checkAndSellIfLowMcap(
                    activePos.mint,
                    "insider",
                    i,
                    currentMc,
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

    // 2. Bundler / Reverse Mode loop
    let isBundlerChecking = false;
    setInterval(async () => {
      if (isBundlerChecking) return;
      isBundlerChecking = true;

      try {
        const checkPromises: Promise<void>[] = [];

        if (config.tradingWalletAddress) {
          // Bundler Mode positions
          if (botMode === "bundler") {
            const bundlerPos = earlyBundlerOrchestrator.getActivePosition();
            if (bundlerPos) {
              checkPromises.push(checkBundlerMcapFlow());
            }
          }

          // Reverse CopySell positions
          if (botMode === "reverse_copysell") {
            const revPos = reverseCopySellOrchestrator.getActivePosition();
            if (revPos) {
              checkPromises.push(
                checkAndSellIfLowMcap(revPos.mint, "reverse_copysell"),
              );
            }
          }
        }

        await Promise.all(checkPromises);
      } catch (err) {
        log.error("Error in Bundler/Reverse MC loop", err);
      } finally {
        isBundlerChecking = false;
      }
    }, MCAP_CHECK_INTERVAL_MS);
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
    return `Stopped monitoring <code>${normalized}</code>`;
  }

  function pauseWallet(address: string): string {
    const normalized = new PublicKey(address).toBase58();

    if (normalized === config.tradingWalletAddress) {
      if (!tradingWalletMonitor) return `Trading wallet is not active.`;
      if (pausedWallets.has(normalized))
        return `Trading wallet is already paused.`;
      tradingWalletMonitor.stop();
      pausedWallets.add(normalized);
      return `Paused monitoring your TRADING wallet <code>${normalized}</code>`;
    }

    const monitor = walletMonitors.get(normalized);
    if (!monitor) return `Wallet is not monitored: <code>${normalized}</code>`;
    if (pausedWallets.has(normalized))
      return `Wallet is already paused: <code>${normalized}</code>`;
    monitor.stop();
    pausedWallets.add(normalized);
    return `Paused monitoring <code>${normalized}</code>`;
  }

  async function resumeWallet(address: string): Promise<string> {
    const normalized = new PublicKey(address).toBase58();

    if (normalized === config.tradingWalletAddress) {
      if (!tradingWalletMonitor) return `Trading wallet is not active.`;
      if (!pausedWallets.has(normalized))
        return `Trading wallet is already running.`;
      pausedWallets.delete(normalized);
      await tradingWalletMonitor.start();
      return `Continued monitoring your TRADING wallet <code>${normalized}</code>`;
    }

    const monitor = walletMonitors.get(normalized);
    if (!monitor) return `Wallet is not monitored: <code>${normalized}</code>`;
    if (!pausedWallets.has(normalized))
      return `Wallet is already running: <code>${normalized}</code>`;
    pausedWallets.delete(normalized);
    await monitor.start();
    return `Continued monitoring <code>${normalized}</code>`;
  }

  async function startBundlerModeServices(): Promise<void> {
    earlyBundlerOrchestrator.setEnabled(true);
    const followedWallet = earlyBundlerOrchestrator.getFollowedWallet();
    if (followedWallet) {
      await earlyBundlerOrchestrator.followWallet(followedWallet);
    } else {
      log.info(
        "Bundler mode started without follow wallet; set one from Telegram.",
      );
    }

    log.info("Bundler mode services started", {
      followedWallet,
      running: earlyBundlerOrchestrator.isRunning(),
    });
  }

  async function stopBundlerModeServices(
    reason = "Bundler mode stopped",
  ): Promise<void> {
    for (const monitor of walletMonitors.values()) {
      monitor.stop();
    }
    walletMonitors.clear();

    tradingWalletMonitor?.stop();
    tradingWalletMonitor = null;
    pendingTradingBuys.clear();
    pausedWallets.clear();

    await earlyBundlerOrchestrator.stopActiveMonitoring(reason);

    log.info("Bundler mode services stopped", { reason });
  }

  async function startReverseCopySellModeServices(): Promise<void> {
    reverseCopySellOrchestrator.setEnabled(true);

    if (config.tradingWalletAddress && !tradingWalletMonitor) {
      tradingWalletMonitor = new WalletMonitor(
        config,
        config.tradingWalletAddress,
        {
          enforceMinBuySol: false,
          logLabel: "WALLET TRADING 1",
        },
      );
      wireTradingWalletMonitor(tradingWalletMonitor);
      await tradingWalletMonitor.start();
    } else if (!config.tradingWalletAddress) {
      log.warn(
        "No TRADING_WALLET_ADDRESS configured; reverse-copysell flow cannot detect your buys.",
      );
    }

    log.info("Reverse CopySell mode services started", {
      tradingWalletActive: !!tradingWalletMonitor,
    });
  }

  async function stopReverseCopySellModeServices(
    reason = "Reverse CopySell mode stopped",
  ): Promise<void> {
    tradingWalletMonitor?.stop();
    tradingWalletMonitor = null;
    pendingTradingBuys.clear();
    pausedWallets.clear();

    await reverseCopySellOrchestrator.stopActiveMonitoring(reason);
    reverseCopySellOrchestrator.setEnabled(false);

    log.info("Reverse CopySell mode services stopped", { reason });
  }

  const html = (value: string): string =>
    value.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");

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
    const isMonitoring = isTrading
      ? !!tradingWalletMonitor
      : walletMonitors.has(normalized);
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
    if (botMode === "insider") {
      const bot = insiderBots[activeInsiderIndex];
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

      const botSelectionRows = insiderBots.reduce<
        Array<Array<{ text: string; callback_data: string }>>
      >((rows, _insiderBot, index) => {
        const rowIndex = Math.floor(index / 2);
        const botNumber = getInsiderBotNumber(index);
        rows[rowIndex] ??= [];
        rows[rowIndex].push({
          text:
            activeInsiderIndex === index
              ? `🟢 Bot ${botNumber}`
              : `Bot ${botNumber}`,
          callback_data: `insider:select:${index}`,
        });
        return rows;
      }, []);

      return {
        text: [
          `<b>Insider Bot ${getInsiderBotNumber(activeInsiderIndex)}</b>`,
          "",
          `Mode: <b>Insider</b>`,
          `Status: <b>${status}</b>`,
          `Follow wallet: ${followedWallet ? `<code>${html(followedWallet)}</code>` : "<b>Not set</b>"}`,
          monitoredWallet
            ? `Insider wallet: <code>${html(monitoredWallet)}</code>`
            : "",
          `Buy SOL: <b>${html(String(bot.getBuySol()))}</b>`,
          `Exit Strategy: <b>+${html(String(bot.getExitPercent()))}% ATH MC from Entry</b>`,
          `Auto Buy: <b>${buyDisabled ? "Disabled ❌" : "Enabled ✅"}</b>`,
          "",
          "<b>Flow</b>",
          "1. Bots 1–4 run in parallel on their own follow wallets (same mint blocked).",
          "2. Skip immediately when the follow-wallet buy MC is above $50,000.",
          "3. GMGN discovers cumulative axiom/empty single-buy wallets; their ATAs are polled independently.",
          "4. Buy when the largest similar-SOL balance group has at least 10 existing ATA wallets and no more than 3 group wallets have reduced their token balance.",
          "5. After buy: continue Axiom discovery and independent ATA polling.",
          "6. Sell when at least 5 wallets and at least 20% of the largest similar-SOL group have reduced their token balance, the largest group collapses to 2, on ATH MC target, rug threshold, or manual sell.",
          `• Rug: MC below $${INSIDER_MIN_MARKET_CAP_USD.toLocaleString()} resets flow.`,
        ].join("\n"),
        replyMarkup: {
          inline_keyboard: [
            [
              { text: "Insider", callback_data: "mode:insider" },
              { text: "Bundler", callback_data: "mode:bundler" },
            ],
            ...botSelectionRows,
            [
              { text: "Follow wallet", callback_data: "insider:follow" },
              { text: "Buy SOL", callback_data: "insider:buysol" },
            ],
            [
              { text: "Set Exit %", callback_data: "insider:exitpercent" },
              { text: "Bundler Min $", callback_data: "insider:bundlermin" },
            ],
            [{ text: "Bundler Max $", callback_data: "insider:bundlermax" }],
            [
              disableBuyButton,
              { text: "Refresh", callback_data: "menu:refresh" },
            ],
            [stopResumeButton],
          ],
        },
        editCurrent,
      };
    }

    if (botMode === "reverse_copysell") {
      const targetWallet = config.reverseCopySellTargetWallet;
      const activePosition = reverseCopySellOrchestrator.getActivePosition();
      return {
        text: [
          "<b>Reverse CopySell Bot</b>",
          "",
          `Mode: <b>Reverse CopySell</b>`,
          `Target: ${targetWallet ? `<code>${html(targetWallet)}</code>` : "<b>Not set</b>"}`,
          activePosition
            ? `Monitoring: <code>${html(activePosition.mint)}</code>`
            : "Status: <b>Waiting for buy...</b>",
          "",
          "<b>Flow</b>",
          "• Watches your trading wallet for new buys.",
          "• Once you buy, I watch the target wallet for the same token.",
          "• When the target wallet buys, I sell 100% of your position.",
        ].join("\n"),
        replyMarkup: {
          inline_keyboard: [
            [
              { text: "Insider", callback_data: "mode:insider" },
              { text: "Bundler", callback_data: "mode:bundler" },
            ],
            [
              {
                text: "Set Target Wallet",
                callback_data: "reverse:set_target",
              },
              { text: "Refresh", callback_data: "menu:refresh" },
            ],
          ],
        },
        editCurrent,
      };
    }

    const followedWallet = earlyBundlerOrchestrator.getFollowedWallet();
    const activePosition = earlyBundlerOrchestrator.getActivePosition();
    const watchingMint = earlyBundlerOrchestrator.getWatchingMint();
    const bundlerRunning = earlyBundlerOrchestrator.isRunning();

    let status = "Idle";
    if (activePosition)
      status = `Holding token ${html(activePosition.mint.slice(0, 8))}...`;
    else if (watchingMint)
      status = `Checking token ${html(watchingMint.slice(0, 8))}...`;
    else if (bundlerRunning) status = "Running";
    else if (followedWallet) status = "Paused";

    const stopResumeButton =
      followedWallet && !bundlerRunning && !activePosition && !watchingMint
        ? { text: "Resume", callback_data: "bundler:resume" }
        : { text: "Stop", callback_data: "bundler:stop" };

    return {
      text: [
        "<b>Bundler Bot</b>",
        "",
        `Mode: <b>Bundler</b>`,
        `Status: <b>${status}</b>`,
        `Follow wallet: ${followedWallet ? `<code>${html(followedWallet)}</code>` : "<b>Not set</b>"}`,
        `Buy SOL: <b>${html(String(earlyBundlerOrchestrator.getBuySol()))}</b>`,
        `Exit Strategy: <b>+${html(String(earlyBundlerOrchestrator.getExitPercent()))}% from Entry MC</b>`,
        activePosition?.entryMc
          ? `Entry MC: <b>$${html(activePosition.entryMc.toLocaleString())}</b>`
          : "",
        activePosition?.matchedWallet
          ? `Matched feePayer: <code>${html(activePosition.matchedWallet)}</code>`
          : "",
        "",
        "<b>Flow</b>",
        "1. Set a follow wallet.",
        "2. Bot waits for that wallet to buy a new token within 10 minutes of creation.",
        "3. Bot checks Helius system transfers every 2s for up to 2 minutes, waiting for 10 records.",
        "4. creator_hold_rate must be greater than 0; if it is 0 and you already hold, bot sells.",
        "5. Once 10 records are available, bot buys only if a repeated feePayer has buy + buy as its first two actions.",
        `6. Bot exits at your % MC increase or sells on rug below $${INSIDER_MIN_MARKET_CAP_USD.toLocaleString()}.`,
      ]
        .filter(Boolean)
        .join("\n"),
      replyMarkup: {
        inline_keyboard: [
          [
            { text: "Insider", callback_data: "mode:insider" },
            { text: "Bundler", callback_data: "mode:bundler" },
          ],
          [
            { text: "Follow wallet", callback_data: "bundler:follow" },
            { text: "Buy SOL", callback_data: "bundler:buysol" },
          ],
          [
            { text: "Set Exit %", callback_data: "bundler:exitpercent" },
            { text: "Status", callback_data: "menu:status" },
          ],
          [
            { text: "Refresh", callback_data: "menu:refresh" },
            stopResumeButton,
          ],
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
    let text = "";
    if (botMode === "insider") {
      const botsInfo = insiderBots
        .map((bot, i) => {
          const followed = bot.getFollowedWallet();
          const status = bot.isRunning()
            ? "Running"
            : followed
              ? "Paused"
              : "Idle";
          return [
            `<b>Insider Bot ${getInsiderBotNumber(i)}</b>`,
            `Status: ${status}`,
            `Follow: ${followed ?? "not set"}`,
            `Buy: ${bot.getBuySol()} SOL`,
          ].join("\n");
        })
        .join("\n\n");

      text = ["<b>Bot Status</b>", "Mode: Insider", "", botsInfo].join("\n");
    } else if (botMode === "reverse_copysell") {
      const targetWallet = config.reverseCopySellTargetWallet;
      const activePosition = reverseCopySellOrchestrator.getActivePosition();
      text = [
        "<b>Bot Status</b>",
        "Mode: Reverse CopySell",
        `Target: ${targetWallet ?? "not set"}`,
        `Monitoring: ${activePosition ? activePosition.mint : "none"}`,
      ].join("\n");
    } else {
      const activePosition = earlyBundlerOrchestrator.getActivePosition();
      text = [
        "<b>Bot Status</b>",
        "Mode: Bundler",
        `Follow: ${earlyBundlerOrchestrator.getFollowedWallet() ?? "not set"}`,
        `Running: ${earlyBundlerOrchestrator.isRunning() ? "yes" : "no"}`,
        `Watching: ${earlyBundlerOrchestrator.getWatchingMint() ?? "none"}`,
        `Holding: ${activePosition ? activePosition.mint : "none"}`,
        `Buy: ${earlyBundlerOrchestrator.getBuySol()} SOL`,
        `Exit: +${earlyBundlerOrchestrator.getExitPercent()}%`,
      ].join("\n");
    }

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
      let lastError: unknown = null;
      let sold = false;

      log.info(
        `[SELL EXECUTE] Starting sell for ${currentPending.event.mint} (Initial Balance: ${startingBalance ?? "unknown"})`,
      );

      for (let attempt = 1; attempt <= 5; attempt++) {
        try {
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
          lastError = null;

          if (lastResult.status === "confirmed") {
            await sleep(250);
            const remainingBalance = await getTokenRawBalance(
              owner,
              mintPk,
            ).catch(() => null);
            if (remainingBalance !== null && remainingBalance <= 0n) {
              sold = true;
              break;
            }
            lastError = new Error(
              `Sell transaction confirmed but token balance remains ${remainingBalance ?? "unknown"}`,
            );
            log.warn(
              `Sell transaction confirmed but balance not cleared for ${currentPending.event.mint}`,
              {
                hash: lastResult.hash,
                remainingBalance: remainingBalance?.toString() ?? "unknown",
              },
            );
          }
        } catch (err) {
          lastError = err;
          log.warn(
            `Sell attempt ${attempt}/5 failed for ${currentPending.event.mint}`,
            {
              error: err instanceof Error ? err.message : String(err),
            },
          );
        }

        if (attempt < 5) {
          await sleep(100);
          const remainingBalance = await getTokenRawBalance(
            owner,
            mintPk,
          ).catch(() => null);
          if (remainingBalance !== null && remainingBalance <= 0n) {
            sold = true;
            break;
          }
        }
      }

      if (!sold) {
        throw (
          lastError ??
          new Error(`Sell did not clear token balance after 5 attempts`)
        );
      }

      // Cleanup cache
      activePositionCache.delete(pending.event.mint);
      if (pending.event.insiderBotIndex !== undefined) {
        insiderBots[pending.event.insiderBotIndex]?.clearActivePosition();
      }

      const receiptResult = lastResult
        ? {
            ...lastResult,
            status:
              lastResult.status === "failed" ? "confirmed" : lastResult.status,
          }
        : {
            orderId: null,
            hash: null,
            status: "confirmed",
            inputToken: currentPending.event.mint,
            outputToken: "So11111111111111111111111111111111111111112",
            soldPercent: config.sellPercent,
            filledInputAmount: null,
            filledOutputAmount: null,
            raw: {},
          };
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

  // ── 6. Start active mode ──────────────────────────────────────────────────
  if (botMode === "bundler") {
    for (const bot of insiderBots) bot.pause();
    await startBundlerModeServices();
  } else {
    await stopBundlerModeServices("Service started in Insider mode");
    await resumeAllInsiderBots();
  }
  startMarketCapChecker();

  log.info(
    `Service fully started — mode=${botMode}, bundler running=${earlyBundlerOrchestrator.isRunning()}`,
  );

  // ── 7. Graceful shutdown ──────────────────────────────────────────────────
  let shutting_down = false;

  async function shutdown(signal: string): Promise<void> {
    if (shutting_down) return;
    shutting_down = true;

    log.info(`Received ${signal} — shutting down gracefully`);

    for (const monitor of walletMonitors.values()) {
      monitor.stop();
    }
    tradingWalletMonitor?.stop();
    telegramBot?.stop();
    healthServer.close();

    await earlyBundlerOrchestrator.shutdown();
    await reverseCopySellOrchestrator.shutdown();

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
