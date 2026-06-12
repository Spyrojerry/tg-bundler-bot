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

  log.info("═══════════════════════════════════════");
  log.info("  GMGN Bundler Monitor  — starting up");
  log.info("═══════════════════════════════════════");
  log.info("Config", {
    wallet: config.walletAddress,
    tradingWallet: config.tradingWalletAddress,
    rpc: config.solanaRpcUrl,
    ws: config.solanaWsUrl,
    receiverRpc: config.receiverSolanaRpcUrl,
    receiverWs: config.receiverSolanaWsUrl,
    f1Rpc: config.f1SolanaRpcUrl,
    f1Ws: config.f1SolanaWsUrl,
    minBuySol: config.minBuySol,
    gmgnFetchMode: config.gmgnFetchMode,
    monitorInterval: config.monitorInterval,
    rateLimitMinTime: config.rateLimitMinTime,
    dbPath: config.dbPath,
    insiderEntryMc: config.insiderEntryMc,
    insiderExitMc: config.insiderExitMc,
  });

  // ── 2. Database ────────────────────────────────────────────────────────────
  const db = await MonitorDatabase.create(config.dbPath);

  // ── 3. Rate limiter + GMGN client ─────────────────────────────────────────
  const limiter = new RateLimiter(
    config.rateLimitMinTime,
    config.rateLimitMaxConcurrent,
  );
  const gmgnClients = [
    new GmgnClient(config, limiter, config.insiderSolanaRpcUrl),
    new GmgnClient(
      { ...config, gmgnApiKey: config.gmgnApiKey2 },
      limiter,
      config.insiderSolanaRpcUrl2,
    ),
  ];

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
          const context =
            contextCode === "i"
              ? "insider"
              : contextCode === "r"
                ? "reverse_copysell"
                : "bundler";

          let currentMarketCapUsd: number | null = null;
          let currentPrice: SellQuote | null = null;
          let balanceIsZero = false;

          const botIndex = insiderBots.findIndex(
            (b) => b.getActivePosition()?.mint === mint,
          );
          const effectiveBotIndex = botIndex !== -1 ? botIndex : 0;
          const client =
            context === "insider"
              ? gmgnClients[effectiveBotIndex]
              : gmgnClients[0];

          try {
            const quotePromise = config.tradingWalletAddress
              ? client
                  .quoteTokenSellForSol(config.tradingWalletAddress, mint, 100)
                  .catch(async (err) => {
                    if (err.message.includes("No token balance found")) {
                      balanceIsZero = true;
                    }
                    return null;
                  })
              : Promise.resolve(null);

            [currentMarketCapUsd, currentPrice] = await Promise.all([
              client.fetchTokenMarketCapUsd(mint),
              quotePromise,
            ]);
          } catch (err) {
            log.error(`Failed to refresh position data for ${mint}`, err);
          }

          let buySol = 0;
          if (context === "insider") {
            const bot =
              botIndex !== -1 ? insiderBots[botIndex] : insiderBots[0];
            buySol = bot.getBuySol();
          } else if (context === "bundler") {
            buySol = earlyBundlerOrchestrator.getActivePosition()?.buySol ?? 0;
          } else if (context === "reverse_copysell") {
            buySol =
              reverseCopySellOrchestrator.getActivePosition()?.buySol ?? 0;
          }

          // Fallback to DB if buySol is 0
          if (buySol === 0 && config.tradingWalletAddress) {
            const dbToken = db.getToken(config.tradingWalletAddress, mint);
            if (dbToken && dbToken.buySol) {
              buySol = dbToken.buySol;
            }
          }

          const profitSol = currentPrice
            ? currentPrice.estimatedOutputSol - buySol
            : null;
          const profitPct =
            profitSol !== null && buySol > 0
              ? (profitSol / buySol) * 100
              : null;

          let profitDisplay = "Profit/Loss: <b>Calculating...</b>";
          if (profitSol !== null) {
            profitDisplay = `Profit/Loss: <b>${profitSol.toFixed(4)} SOL</b> (${profitPct?.toFixed(2)}%)`;
          } else if (balanceIsZero) {
            profitDisplay = "Profit/Loss: <b>Position Closed (0 balance)</b>";
          } else if (!config.tradingWalletAddress) {
            profitDisplay = "Profit/Loss: <b>N/A (No trading wallet)</b>";
          }

          const lines = [
            context === "insider"
              ? "<b>Insider Position Update</b>"
              : context === "reverse_copysell"
                ? "<b>Reverse CopySell Update</b>"
                : "<b>Bundler Position Update</b>",
            `Token: <code>${html(mint)}</code>`,
            `Market Cap: <b>$${currentMarketCapUsd?.toLocaleString() ?? "Unknown"}</b>`,
            profitDisplay,
            "",
            `Last Updated: ${new Date().toLocaleTimeString()}`,
          ];

          const buttons = [];

          if (context === "insider" && !balanceIsZero) {
            buttons.push({
              text: "🔴 Sell Position",
              callback_data: `sell:insider:${mint}:${effectiveBotIndex}`,
            });
            buttons.push({
              text: "🔄 Refresh",
              callback_data: `r:m:${mint}:${contextCode}`,
            });
          } else {
            // If it's not insider context, or position is closed, just show refresh
            buttons.push({
              text: "🔄 Refresh",
              callback_data: `r:m:${mint}:${contextCode}`,
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
          activeInsiderIndex = parseInt(data.split(":")[2]);
          return homeReply(true);
        }
        if (data === "insider:follow") {
          pendingTelegramActions.set(chatId, {
            type: "insiderFollowWallet",
            index: activeInsiderIndex,
          });
          return {
            text: `[Bot ${activeInsiderIndex + 1}] Send the wallet address for Insider Bot to follow.`,
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
            text: `[Bot ${activeInsiderIndex + 1}] Send the SOL amount Insider Bot should buy with.`,
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
            text: `[Bot ${activeInsiderIndex + 1}] Send the Exit profit percentage increase.\nExample: <code>40</code> for a 40% ATH MC increase from your entry point.`,
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
            text: `[Bot ${activeInsiderIndex + 1}] Send the minimum bundler buy USD.\nExample: <code>110</code>`,
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
            text: `[Bot ${activeInsiderIndex + 1}] Send the maximum bundler buy USD.\nExample: <code>120</code>`,
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
              text: `[Bot ${activeInsiderIndex + 1}] Send the wallet address for Insider Bot to follow.`,
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

  const insiderHeliusKeys = [
    config.insiderHeliusApiKey || config.heliusApiKey,
    config.insiderHeliusApiKey2 || config.insiderHeliusApiKey || config.heliusApiKey,
  ];

  const bundlerGmgnClient = gmgnClients[1];

  function claimInsiderMint(botIndex: number, mint: string): boolean {
    if (botIndex > 1) return true;
    const otherIndex = botIndex === 0 ? 1 : 0;
    const otherBot = insiderBots[otherIndex];
    if (!otherBot) return true;
    if (otherBot.getPreBuyMint() === mint) return false;
    if (otherBot.getActivePosition()?.mint === mint) return false;
    return true;
  }

  const makeClaimFn =
    (botIndex: number): InsiderMintClaimFn =>
    (mint: string) =>
      claimInsiderMint(botIndex, mint);

  insiderBots.push(
    new InsiderBot(
      config,
      config.insiderSolanaRpcUrl,
      config.insiderSolanaWsUrl,
      gmgnClients[0],
      bundlerGmgnClient,
      insiderHeliusKeys[0],
      telegramBot,
      makeClaimFn(0),
      () => undefined,
    ),
  );
  insiderBots.push(
    new InsiderBot(
      config,
      config.insiderSolanaRpcUrl2,
      config.insiderSolanaWsUrl2,
      gmgnClients[1],
      bundlerGmgnClient,
      insiderHeliusKeys[1],
      telegramBot,
      makeClaimFn(1),
      () => undefined,
    ),
  );

  insiderBots.push(
    new InsiderBot(
      config,
      config.insiderSolanaRpcUrl,
      config.insiderSolanaWsUrl,
      gmgnClients[0],
      bundlerGmgnClient,
      insiderHeliusKeys[0],
      telegramBot,
    ),
  );
  insiderBots.push(
    new InsiderBot(
      config,
      config.insiderSolanaRpcUrl2,
      config.insiderSolanaWsUrl2,
      gmgnClients[1],
      bundlerGmgnClient,
      insiderHeliusKeys[1],
      telegramBot,
      makeClaimFn(1),
      () => undefined,
    ),
  );

  async function resumeAllInsiderBots(): Promise<void> {
    for (let i = 0; i < 2; i++) {
      const bot = insiderBots[i];
      const wallet = bot.getFollowedWallet();
      if (
        wallet &&
        !bot.isRunning() &&
        !bot.getActivePosition() &&
        !bot.getPreBuyMint()
      ) {
        await bot.followWallet(wallet);
        log.info(`[INSIDER ${i + 1}] Resumed follow-wallet monitoring`, {
          wallet,
        });
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
  if (config.insiderFollowWallet) {
    try {
      insiderBots[0].configureFollowWallet(config.insiderFollowWallet);
      log.info(
        `[INSIDER 1] Loaded default follow wallet in paused state: ${config.insiderFollowWallet}`,
      );
    } catch (err) {
      log.error("[INSIDER 1] Failed to load default follow wallet", err);
    }
  }
  if (config.insiderFollowWallet2) {
    try {
      insiderBots[1].configureFollowWallet(config.insiderFollowWallet2);
      log.info(
        `[INSIDER 2] Loaded default follow wallet in paused state: ${config.insiderFollowWallet2}`,
      );
    } catch (err) {
      log.error("[INSIDER 2] Failed to load default follow wallet", err);
    }
  }

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

    bot.on("mintSeen", (mint: string) => {
      if (config.tradingWalletAddress) {
        db.addSeenMint(config.tradingWalletAddress, mint);
      }
    });

    bot.on("buyTrigger", (trigger) => {
      if (!config.tradingWalletAddress) {
        log.warn(
          `[INSIDER ${index + 1} BUY SKIP] No trading wallet configured`,
          trigger,
        );
        return;
      }

      log.warn(`[INSIDER ${index + 1} BUY TRIGGER]`, trigger);

      void (async () => {
        try {
          const tradersListStr = trigger.tradersListStr || "";

          await telegramBot?.sendDefault(
            [
              `<b>🚀 Insider ${index + 1} Buy Executing</b>`,
              `Token: <code>${html(trigger.mint)}</code>`,
              `Buying: <b>${html(String(trigger.buySol))} SOL</b>`,
              `Entry MC: <b>$${html(trigger.entryMc?.toLocaleString() ?? "Unknown")}</b>`,
              `Exit MC: <b>$${html(bot.getExitMc().toLocaleString())}</b>`,
              "",
              tradersListStr ? tradersListStr : "",
              "",
              "Submitting swap...",
            ]
              .filter(Boolean)
              .join("\n"),
          );

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

          // Fetch actual MC after purchase for accurate tracking
          let actualEntryMc = trigger.entryMc;
          try {
            const freshMc = await client.fetchTokenMarketCapUsd(trigger.mint);
            if (freshMc !== null) {
              actualEntryMc = freshMc;
              bot.setEntryMc(freshMc);
              // Recalculate Exit MC based on actual entry point
              const exitPercent = bot.getExitPercent();
              const newExitMc = freshMc * (1 + exitPercent / 100);
              bot.setExitMc(newExitMc);
              log.warn(
                `[INSIDER ${index + 1} ACTUAL MC] Buy completed. Actual Entry MC: $${freshMc.toLocaleString()}. New Exit MC: $${newExitMc.toLocaleString()}`,
              );
            }
          } catch (mcErr) {
            log.error(
              `Failed to fetch actual entry MC for ${trigger.mint}`,
              mcErr,
            );
          }

          bot.markPositionBought(trigger);
          // ── Persist to DB so this mint is skipped on restart ─────────────
          if (config.tradingWalletAddress) {
            db.addSeenMint(config.tradingWalletAddress, trigger.mint);
          }
          bot.setBuyExecuting(false); // Clear execution flag after state is saved

          await telegramBot?.sendDefault(
            [
              `<b>✅ Insider ${index + 1} Buy Completed</b>`,
              `Token: <code>${html(trigger.mint)}</code>`,
              `Actual Entry MC: <b>$${html(actualEntryMc?.toLocaleString() ?? "Unknown")}</b>`,
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
              replyMarkup: {
                inline_keyboard: [
                  [
                    {
                      text: "🔴 Sell Position",
                      callback_data: `sell:insider:${trigger.mint}:${index}`,
                    },
                    {
                      text: "🔄 Refresh P/L & MC",
                      callback_data: `r:m:${trigger.mint}:i`,
                    },
                  ],
                ],
              },
            },
          );
        } catch (err) {
          bot.resetBuyAttempt();
          log.error(`Insider ${index + 1} buy failed`, err);
          await telegramBot?.sendDefault(
            [
              `<b>❌ Insider ${index + 1} Buy Failed</b>`,
              `Token: <code>${html(trigger.mint)}</code>`,
              `Error: ${html(err instanceof Error ? err.message : String(err))}`,
            ].join("\n"),
          );
        }
      })();
    });

    bot.on("sellTrigger", (trigger) => {
      if (!config.tradingWalletAddress) {
        log.warn(
          `[INSIDER ${index + 1} SELL SKIP] No trading wallet configured`,
          trigger,
        );
        return;
      }
      if (
        hasPendingSellForMint(config.tradingWalletAddress, trigger.positionMint)
      ) {
        log.info(
          `[INSIDER ${index + 1} SELL SKIP] Sell already pending for ${trigger.positionMint}`,
        );
        return;
      }

      // Clear active position immediately on sell trigger to prevent checker from firing
      bot.clearActivePosition();

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
            `<b>🚨 Insider ${index + 1} Sell Triggered</b>`,
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
                    callback_data: `r:m:${trigger.positionMint}:i`,
                  },
                ],
              ],
            },
          },
        )
        .catch((err) =>
          log.warn(`Telegram insider ${index + 1} sell alert failed`, err),
        );

      void executeSellAndNotify(config.telegramChatId, sellId, telegramBot);
    });

    bot.on("error", (err) => {
      log.error(`Insider ${index + 1} error:`, err);
      telegramBot
        ?.sendDefault(
          `<b>⚠️ Insider Bot ${index + 1} Error</b>\n${html(err.message)}`,
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

        // Clear active position immediately on sell trigger to prevent checker from firing again
        if (context === "insider") {
          const bot = insiderBots.find(
            (b) => b.getActivePosition()?.mint === mint,
          );
          if (bot) bot.clearActivePosition();
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
          `[INSIDER ${index + 1} MC SKIP] Could not fetch market cap for ${mint}`,
        );
        return;
      }

      log.info(
        `[INSIDER ${index + 1} MC CHECK] Token: ${mint} MC: $${currentMc.toLocaleString()} (Source: ${preFetchedMc !== undefined ? "Prefetched" : "Client"})`,
      );

      if (currentMc < INSIDER_MIN_MARKET_CAP_USD) {
        const reason = `Market cap $${currentMc.toLocaleString()} below $${INSIDER_MIN_MARKET_CAP_USD.toLocaleString()} (Rug)`;
        log.warn(
          `[INSIDER ${index + 1} RUG] ${reason} for ${mint}. Resetting state.`,
        );

        if (activePos) {
          bot.emit("sellTrigger", {
            followedWallet: bot.getFollowedWallet()!,
            positionMint: mint,
            signature: "MC_TRIGGER",
            reason: `Rug protection: ${reason}`,
          });
        }

        bot.clearActivePosition();
        bot.clearPreBuyMint();
        return;
      }

      if (activePos) {
        const exitMc = bot.getExitMc();
        const athMc = await client.fetchTokenAthMarketCapUsd(mint);
        if (athMc !== null && athMc >= exitMc) {
          log.warn(
            `[INSIDER ${index + 1} EXIT] ATH MC $${athMc.toLocaleString()} reached Exit MC $${exitMc.toLocaleString()}. Triggering SELL.`,
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
        `Failed to check Insider MC flow for ${mint} (Bot ${index + 1})`,
        err,
      );
    }
  }

  function startMarketCapChecker(): void {
    log.info(
      `Starting independent market cap checkers (interval: ${MCAP_CHECK_INTERVAL_MS}ms)`,
    );

    // 1. Insider Mode MC Flow (Independent loops per bot)
    if (botMode === "insider") {
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
                  `[INSIDER ${i + 1} BACKGROUND] Refreshing balance/quote for ${activePos.mint}`,
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
            log.error(`Error in Insider Bot ${i + 1} MC loop`, err);
          } finally {
            isChecking = false;
          }
        }, MCAP_CHECK_INTERVAL_MS);
      });
    }

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
        { enforceMinBuySol: false },
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

      const botSelectionRow = [
        {
          text: activeInsiderIndex === 0 ? "🟢 Bot 1" : "Bot 1",
          callback_data: "insider:select:0",
        },
        {
          text: activeInsiderIndex === 1 ? "🟢 Bot 2" : "Bot 2",
          callback_data: "insider:select:1",
        },
      ];

      return {
        text: [
          `<b>Insider Bot ${activeInsiderIndex + 1}</b>`,
          "",
          `Mode: <b>Insider</b>`,
          `Status: <b>${status}</b>`,
          `Follow wallet: ${followedWallet ? `<code>${html(followedWallet)}</code>` : "<b>Not set</b>"}`,
          monitoredWallet
            ? `Insider wallet: <code>${html(monitoredWallet)}</code>`
            : "",
          `Buy SOL: <b>${html(String(bot.getBuySol()))}</b>`,
          `Bundler buy USD: <b>$${html(String(bot.getBundlerBuyMinUsd()))} – $${html(String(bot.getBundlerBuyMaxUsd()))}</b>`,
          `Exit Strategy: <b>+${html(String(bot.getExitPercent()))}% ATH MC from Entry</b>`,
          `Auto Buy: <b>${buyDisabled ? "Disabled ❌" : "Enabled ✅"}</b>`,
          "",
          "<b>Flow</b>",
          "1. Bot 1 & 2 run in parallel on their own follow wallets (same mint blocked).",
          "2. After lowest insider found: monitor insider + GMGN bundler scan in parallel.",
          `3. Buy when BOTH ${html(String(bot.getRequiredInsiderSells()))} insider sells AND 2 bundlers in USD range are found (each monitor stops when its target is met).`,
          "4. After buy: watch both bundlers; sell when each has sold once.",
          "5. ATH MC % exit and rug protection remain active.",
          `• Rug: MC below $${INSIDER_MIN_MARKET_CAP_USD.toLocaleString()} resets flow.`,
        ].join("\n"),
        replyMarkup: {
          inline_keyboard: [
            [
              { text: "Insider", callback_data: "mode:insider" },
              { text: "Bundler", callback_data: "mode:bundler" },
            ],
            botSelectionRow,
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
            `<b>Insider Bot ${i + 1}</b>`,
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
    const costBasis =
      event.buySol !== null ? event.buySol * (result.soldPercent / 100) : null;
    const pnl =
      receivedSol !== null && costBasis !== null
        ? receivedSol - costBasis
        : null;
    const pnlPct =
      pnl !== null && costBasis !== null && costBasis > 0
        ? (pnl / costBasis) * 100
        : null;
    const fmtSol = (value: number | null): string =>
      value === null ? "N/A" : `${parseFloat(value.toFixed(6))} SOL`;
    const pnlLine =
      pnl === null
        ? "P/L: N/A (original buy SOL unknown)"
        : `P/L: <b>${fmtSol(pnl)}</b> (${parseFloat((pnlPct ?? 0).toFixed(2))}%)`;

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
      `Cost basis sold: ${fmtSol(costBasis)}`,
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

          // If Jupiter returns success, we still verify balance quickly
          if (lastResult.status === "confirmed") {
            sold = true;
            break;
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
          // Faster retry delay for sells (500ms instead of 1500ms)
          await sleep(500);
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

    await limiter.drain().catch((e) => log.warn("Limiter drain error", e));
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
