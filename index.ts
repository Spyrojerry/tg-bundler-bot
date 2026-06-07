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

import 'dotenv/config';
import { createLogger, setLogLevel } from './logger';
import { loadConfig } from './config';
import { MonitorDatabase } from './database';
import { RateLimiter } from './rate-limiter';
import { GmgnClient } from './gmgn-client';
import { WalletMonitor } from './wallet-monitor';
import { InlineKeyboardMarkup, TelegramBot, TelegramReply } from './telegram-bot';
import { startHealthServer } from './health-server';
import {
  FilterFailEvent,
  NewTokenEvent,
  SellResult,
  SellQuote,
  TokenExitEvent,
  WalletFilterSettings,
} from './types';
import { HeliusClient, EarlyBundlerInfo } from './helius-client';
import { BundlerMonitor, BundlerWallet, BundlerTransaction } from './bundler-monitor';
import { EarlyBundlerOrchestrator, BundlerSellReason } from './early-bundler-orchestrator';
import { ReverseCopySellOrchestrator } from './reverse-copysell-orchestrator';
import { InsiderBot } from './insider-bot';
import { PublicKey } from '@solana/web3.js';
import { randomBytes } from 'crypto';

const log = createLogger('MAIN');
const sleep = (ms: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, ms));
const INSIDER_MIN_MARKET_CAP_USD = 1_000;
const MCAP_CHECK_INTERVAL_MS = 1000;

async function main(): Promise<void> {
  // ── 1. Config ──────────────────────────────────────────────────────────────
  const config = loadConfig();
  setLogLevel(config.logLevel);

  log.info('═══════════════════════════════════════');
  log.info('  GMGN Bundler Monitor  — starting up');
  log.info('═══════════════════════════════════════');
  log.info('Config', {
    wallet:          config.walletAddress,
    tradingWallet:   config.tradingWalletAddress,
    rpc:             config.solanaRpcUrl,
    ws:              config.solanaWsUrl,
    receiverRpc:     config.receiverSolanaRpcUrl,
    receiverWs:      config.receiverSolanaWsUrl,
    f1Rpc:           config.f1SolanaRpcUrl,
    f1Ws:            config.f1SolanaWsUrl,
    minBuySol:       config.minBuySol,
    gmgnFetchMode:   config.gmgnFetchMode,
    monitorInterval: config.monitorInterval,
    rateLimitMinTime: config.rateLimitMinTime,
    dbPath:          config.dbPath,
    insiderEntryMc:  config.insiderEntryMc,
    insiderExitMc:   config.insiderExitMc,
  });

  // ── 2. Database ────────────────────────────────────────────────────────────
  const db = await MonitorDatabase.create(config.dbPath);

  // ── 3. Rate limiter + GMGN client ─────────────────────────────────────────
  const limiter = new RateLimiter(
    config.rateLimitMinTime,
    config.rateLimitMaxConcurrent
  );
  const gmgnClients = [
    new GmgnClient(config, limiter, config.insiderSolanaRpcUrl),
    new GmgnClient({ ...config, gmgnApiKey: config.gmgnApiKey2 }, limiter, config.insiderSolanaRpcUrl2),
  ];

  let telegramBot: TelegramBot | null = null;
  const insiderBots: InsiderBot[] = [];
  let activeInsiderIndex = 0; // Which insider bot we are currently viewing/configuring in Telegram

  // ── 4. Early Bundler Orchestrator ─────────────────────────────────────────
  let earlyBundlerOrchestrator: EarlyBundlerOrchestrator;
  let reverseCopySellOrchestrator: ReverseCopySellOrchestrator;
  let botMode: 'insider' | 'bundler' | 'reverse_copysell' = 'insider';

  const healthServer = startHealthServer(config.port);
  const walletMonitors = new Map<string, WalletMonitor>();
  let tradingWalletMonitor: WalletMonitor | null = null;
  const pendingTradingBuys = new Map<string, NewTokenEvent>();
  type PendingTelegramAction =
    | { type: 'addwallet' | 'removewallet' }
    | { type: 'minSol'; walletAddress: string }
    | { type: 'insiderFollowWallet' | 'insiderBuySol' | 'insiderEntryMc' | 'insiderExitPercent' | 'insiderMinProfit'; index: number }
    | { type: 'reverseTargetWallet' };
  const pendingTelegramActions = new Map<string, PendingTelegramAction>();
  const pendingSells = new Map<string, { event: FilterFailEvent; createdAt: number; executing: boolean }>();
  const insiderCheckState = new Map<string, number>(); // key: "botIndex:mint", value: checkCount
  const pausedWallets = new Set<string>();
  const walletAliasesByChat = new Map<string, string[]>();

  function hasPendingSellForMint(walletAddress: string, mint: string): boolean {
    for (const pending of pendingSells.values()) {
      if (pending.event.walletAddress === walletAddress && pending.event.mint === mint) {
        return true;
      }
    }
    return false;
  }

  async function handleTelegramCommand(_chatId: string, text: string): Promise<string | TelegramReply> {
    const chatId = _chatId;
    const [command] = text.split(/\s+/, 1);

    try {
      if (command === '/callback') {
        const [, data] = text.split(/\s+/, 2);
        const parts = data?.split(':') ?? [];
        const [callbackKind, callbackAction, callbackAddress] = parts;

        if (data === 'menu:addwallet') {
          pendingTelegramActions.set(chatId, { type: 'addwallet' });
          return { text: 'Send the Solana wallet address to add.', trackPrompt: true, editCurrent: true };
        }
        if (data === 'menu:removewallet') {
          pendingTelegramActions.set(chatId, { type: 'removewallet' });
          return { text: 'Send the Solana wallet address to remove.', trackPrompt: true, editCurrent: true };
        }
        if (data === 'menu:refresh') return homeReply(true);
        if (data.startsWith('r:m:')) {
          const [, , mint, contextCode] = parts;
          const context = contextCode === 'i' ? 'insider' : (contextCode === 'r' ? 'reverse_copysell' : 'bundler');
          
          let currentMarketCapUsd: number | null = null;
          let currentPrice: SellQuote | null = null;
          let balanceIsZero = false;

          const botIndex = insiderBots.findIndex(b => b.getActivePosition()?.mint === mint);
          const effectiveBotIndex = botIndex !== -1 ? botIndex : 0;
          const client = context === 'insider' ? gmgnClients[effectiveBotIndex] : gmgnClients[0];

          try {
            const quotePromise = config.tradingWalletAddress 
              ? client.quoteTokenSellForSol(config.tradingWalletAddress, mint, 100).catch(async (err) => {
                  if (err.message.includes('No token balance found')) {
                    balanceIsZero = true;
                  }
                  return null;
                })
              : Promise.resolve(null);

            [currentMarketCapUsd, currentPrice] = await Promise.all([
              client.fetchTokenMarketCapUsd(mint),
              quotePromise
            ]);
          } catch (err) {
            log.error(`Failed to refresh position data for ${mint}`, err);
          }
          
          let buySol = 0;
          if (context === 'insider') {
            const bot = botIndex !== -1 ? insiderBots[botIndex] : insiderBots[0];
            buySol = bot.getBuySol();
          } else if (context === 'bundler') {
            buySol = earlyBundlerOrchestrator.getActivePosition()?.buySol ?? 0;
          } else if (context === 'reverse_copysell') {
            buySol = reverseCopySellOrchestrator.getActivePosition()?.buySol ?? 0;
          }

          // Fallback to DB if buySol is 0
          if (buySol === 0 && config.tradingWalletAddress) {
            const dbToken = db.getToken(config.tradingWalletAddress, mint);
            if (dbToken && dbToken.buySol) {
              buySol = dbToken.buySol;
            }
          }

          const profitSol = currentPrice ? currentPrice.estimatedOutputSol - buySol : null;
          const profitPct = profitSol !== null && buySol > 0 ? (profitSol / buySol) * 100 : null;

          let profitDisplay = 'Profit/Loss: <b>Calculating...</b>';
          if (profitSol !== null) {
            profitDisplay = `Profit/Loss: <b>${profitSol.toFixed(4)} SOL</b> (${profitPct?.toFixed(2)}%)`;
          } else if (balanceIsZero) {
            profitDisplay = 'Profit/Loss: <b>Position Closed (0 balance)</b>';
          } else if (!config.tradingWalletAddress) {
            profitDisplay = 'Profit/Loss: <b>N/A (No trading wallet)</b>';
          }

          const lines = [
            context === 'insider' ? '<b>Insider Position Update</b>' : (context === 'reverse_copysell' ? '<b>Reverse CopySell Update</b>' : '<b>Bundler Position Update</b>'),
            `Token: <code>${html(mint)}</code>`,
            `Market Cap: <b>$${currentMarketCapUsd?.toLocaleString() ?? 'Unknown'}</b>`,
            profitDisplay,
            '',
            `Last Updated: ${new Date().toLocaleTimeString()}`,
          ];

          const buttons = [];
          
          if (context === 'insider' && !balanceIsZero) {
            buttons.push({ text: '🔴 Sell Position', callback_data: `sell:insider:${mint}:${effectiveBotIndex}` });
            buttons.push({ text: '🔄 Refresh', callback_data: `r:m:${mint}:${contextCode}` });
          } else {
            // If it's not insider context, or position is closed, just show refresh
            buttons.push({ text: '🔄 Refresh', callback_data: `r:m:${mint}:${contextCode}` });
          }

          return {
            text: lines.join('\n'),
            replyMarkup: {
              inline_keyboard: [buttons],
            },
            editCurrent: true,
          };
        }
        if (data.startsWith('sell:insider:')) {
          const [, , mint, botIndexStr] = parts;
          const botIndex = parseInt(botIndexStr);
          const bot = insiderBots[botIndex];
          if (!bot) return 'Invalid bot index.';

          const activePos = bot.getActivePosition();
          // Even if activePos is missing (state reset), we allow manual sell if it's the right mint
          // or if the user is forcing a sell from an insider card.
          const followedWallet = activePos?.followedWallet || bot.getFollowedWallet() || 'UNKNOWN';

          // Trigger manual sell
          bot.emit('sellTrigger', {
            followedWallet,
            positionMint: mint,
            signature: 'MANUAL',
            reason: 'Manual sell requested via Telegram button',
          });

          return 'Sell signal sent.';
        }
        if (data === 'menu:wallets') return walletsReply(chatId, true);
        if (data === 'menu:status') return statusReply(true);
        if (data === 'mode:insider') {
          await stopBundlerModeServices('Switched to Insider mode');
          await stopReverseCopySellModeServices('Switched to Insider mode');
          botMode = 'insider';
          return homeReply(true);
        }
        if (data === 'mode:bundler') {
          for (const bot of insiderBots) await bot.stop();
          await stopReverseCopySellModeServices('Switched to Bundler mode');
          botMode = 'bundler';
          await startBundlerModeServices();
          return homeReply(true);
        }
        if (data === 'mode:reverse_copysell') {
          for (const bot of insiderBots) await bot.stop();
          await stopBundlerModeServices('Switched to Reverse CopySell mode');
          botMode = 'reverse_copysell';
          await startReverseCopySellModeServices();
          return homeReply(true);
        }
        if (data.startsWith('insider:select:')) {
          activeInsiderIndex = parseInt(data.split(':')[2]);
          return homeReply(true);
        }
        if (data === 'insider:follow') {
          pendingTelegramActions.set(chatId, { type: 'insiderFollowWallet', index: activeInsiderIndex });
          return { text: `[Bot ${activeInsiderIndex + 1}] Send the wallet address for Insider Bot to follow.`, trackPrompt: true, editCurrent: true };
        }
        if (data === 'insider:buysol') {
          pendingTelegramActions.set(chatId, { type: 'insiderBuySol', index: activeInsiderIndex });
          return { text: `[Bot ${activeInsiderIndex + 1}] Send the SOL amount Insider Bot should buy with.`, trackPrompt: true, editCurrent: true };
        }
        if (data === 'insider:entrymc') {
          pendingTelegramActions.set(chatId, { type: 'insiderEntryMc', index: activeInsiderIndex });
          return { text: `[Bot ${activeInsiderIndex + 1}] Send the Entry Market Cap in <b>thousands (k)</b>.\nExample: <code>50</code> for $50,000.`, trackPrompt: true, editCurrent: true };
        }
        if (data === 'insider:exitpercent') {
          pendingTelegramActions.set(chatId, { type: 'insiderExitPercent', index: activeInsiderIndex });
          return { text: `[Bot ${activeInsiderIndex + 1}] Send the Exit profit percentage increase.\nExample: <code>50</code> for a 50% increase from your entry point.`, trackPrompt: true, editCurrent: true };
        }
        if (data === 'insider:minprofit') {
          pendingTelegramActions.set(chatId, { type: 'insiderMinProfit', index: activeInsiderIndex });
          return { text: `[Bot ${activeInsiderIndex + 1}] Send the minimum profit threshold for transfer wallets (USD).\nExample: <code>70</code> for $70.`, trackPrompt: true, editCurrent: true };
        }
        if (data === 'insider:togglebuy') {
          const bot = insiderBots[activeInsiderIndex];
          bot.setBuyDisabled(!bot.isBuyDisabled());
          return homeReply(true);
        }
        if (data === 'insider:toggleprofit') {
          const bot = insiderBots[activeInsiderIndex];
          const current = bot.getProfitType();
          let next: 'both' | 'total' | 'realized';
          if (current === 'both') next = 'total';
          else if (current === 'total') next = 'realized';
          else next = 'both';
          bot.setProfitType(next);
          return homeReply(true);
        }
        if (data === 'insider:stop') {
          await insiderBots[activeInsiderIndex].stop();
          return homeReply(true);
        }
        if (data === 'insider:resume') {
          const bot = insiderBots[activeInsiderIndex];
          const followedWallet = bot.getFollowedWallet();
          if (!followedWallet) {
            pendingTelegramActions.set(chatId, { type: 'insiderFollowWallet', index: activeInsiderIndex });
            return { text: `[Bot ${activeInsiderIndex + 1}] Send the wallet address for Insider Bot to follow.`, trackPrompt: true, editCurrent: true };
          }
          await bot.followWallet(followedWallet);
          return homeReply(true);
        }

        if (callbackKind === 'sell' && callbackAction && callbackAddress) {
          const pending = pendingSells.get(callbackAddress);
          if (!pending) return 'This sell request is no longer available.';
          if (callbackAction === 'ignore') {
            pendingSells.delete(callbackAddress);
            return 'Sell ignored.';
          }
          if (callbackAction === 'confirm') {
            if (pending.executing) return 'Sell is already being submitted.';
            pending.executing = true;
            if (telegramBot) {
              void executeSellAndNotify(chatId, callbackAddress, telegramBot);
            }
            return [
              '<b>Sell submission started</b>',
              `Token: <code>${html(pending.event.mint)}</code>`,
              `Selling: <b>${config.sellPercent}%</b> for SOL`,
              `Slippage: <b>${config.sellAutoSlippage ? 'auto' : config.sellSlippage}</b>`,
              `Priority fee: <b>${config.sellPriorityFeeSol} SOL</b>`,
              `Anti-MEV: <b>${config.sellAntiMev ? 'on' : 'off'}</b>`,
              '',
              'I will send the receipt here when GMGN returns the order result.',
            ].join('\n');
          }
          return 'Invalid sell action.';
        }

        if (callbackKind === 'set' && callbackAction === 'minSol' && callbackAddress) {
          const normalized = new PublicKey(callbackAddress).toBase58();
          pendingTelegramActions.set(chatId, {
            type: 'minSol',
            walletAddress: normalized,
          });
          return {
            text: [
              `Send a minimum SOL value for <code>${html(normalized)}</code>.`,
              'Use a number (e.g., 0.01), or send <code>off</code> to use default.',
            ].join('\n'),
            trackPrompt: true,
            editCurrent: true,
          };
        }
        if (callbackKind === 'reverse' && callbackAction) {
          if (callbackAction === 'set_target') {
            pendingTelegramActions.set(chatId, { type: 'reverseTargetWallet' });
            return { text: 'Send the Solana wallet address for Reverse CopySell to watch.', trackPrompt: true, editCurrent: true };
          }
          if (callbackAddress) {
            const normalized = new PublicKey(callbackAddress).toBase58();
            if (callbackAction === 'add') {
              db.addReverseBuyWallet(normalized);
              return settingsReply(normalized, true);
            }
            if (callbackAction === 'remove') {
              db.removeReverseBuyWallet(normalized);
              return settingsReply(normalized, true);
            }
          }
          return 'Invalid reverse-buy action.';
        }
        if (callbackKind === 'settings' && callbackAction === 'refresh' && callbackAddress) {
          return settingsReply(callbackAddress, true);
        }

        const [kind, action, address, context] = [callbackKind, callbackAction, callbackAddress, parts[3]];
        if (kind !== 'wallet' || !address) return 'Invalid button action.';
        if (action === 'add') {
          await startWallet(address);
          return walletSummaryReply(address, true);
        }
        if (action === 'remove') {
          stopWallet(address);
          return walletSummaryReply(address, true);
        }
        if (action === 'pause') {
          pauseWallet(address);
          return context === 'settings' ? settingsReply(address, true) : walletSummaryReply(address, true);
        }
        if (action === 'resume') {
          await resumeWallet(address);
          return context === 'settings' ? settingsReply(address, true) : walletSummaryReply(address, true);
        }
        if (action === 'settings') {
          return settingsReply(address, true);
        }
        if (action === 'refresh') {
          return walletSummaryReply(address, true);
        }
        return 'Invalid wallet action.';
      }

      if (command === '/start' || command === '/help') {
        return homeReply();
      }
      if (command === '/wallets') {
        return walletsReply(chatId);
      }
      if (command.startsWith('/w_')) {
        const index = parseInt(command.substring(3));
        const wallet = walletAliasesByChat.get(chatId)?.[index] ?? walletAliasesByChat.get('__default__')?.[index];
        if (!wallet) return 'Wallet shortcut not found. Send /wallets to refresh the list.';
        return walletSummaryReply(wallet);
      }
      if (command === '/status') {
        return statusReply();
      }

      if (!text.startsWith('/')) {
        const pendingAction = pendingTelegramActions.get(chatId);
        if (pendingAction) {
          pendingTelegramActions.delete(chatId);
          if (pendingAction.type === 'addwallet') {
            return await startWallet(text);
          }
          if (pendingAction.type === 'removewallet') {
            return stopWallet(text);
          }
          if (pendingAction.type === 'minSol') {
            const message = updateMinSol(
              pendingAction.walletAddress,
              text
            );
            if (!message.startsWith('Updated ')) return message;
            return settingsReply(pendingAction.walletAddress);
          }
          if (pendingAction.type === 'insiderFollowWallet') {
            const bot = insiderBots[pendingAction.index];
            await bot.followWallet(text);
            return homeReply();
          }
          if (pendingAction.type === 'insiderBuySol') {
            const bot = insiderBots[pendingAction.index];
            const value = Number(text.trim());
            if (!Number.isFinite(value) || value <= 0) return 'Send a SOL amount greater than 0.';
            bot.setBuySol(value);
            return homeReply();
          }
          if (pendingAction.type === 'insiderEntryMc') {
            const bot = insiderBots[pendingAction.index];
            const value = Number(text.trim().replace(/,/g, ''));
            if (!Number.isFinite(value) || value < 0) return 'Send a valid number in thousands (k).';
            bot.setEntryMc(value * 1000);
            return homeReply();
          }
          if (pendingAction.type === 'insiderExitPercent') {
            const bot = insiderBots[pendingAction.index];
            const value = Number(text.trim());
            if (!Number.isFinite(value) || value < 0) return 'Send a valid percentage.';
            bot.setExitPercent(value);
            return homeReply();
          }
          if (pendingAction.type === 'insiderMinProfit') {
            const bot = insiderBots[pendingAction.index];
            const value = Number(text.trim().replace(/,/g, ''));
            if (!Number.isFinite(value) || value < 0) return 'Send a valid number.';
            bot.setMinTransferProfit(value);
            return homeReply();
          }
          if (pendingAction.type === 'reverseTargetWallet') {
            const trimmed = text.trim().toLowerCase();
            if (trimmed === 'off' || trimmed === 'none' || trimmed === 'clear') {
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
        if (botMode === 'insider') {
          await insiderBots[activeInsiderIndex].followWallet(text);
          return homeReply();
        }
        return walletSummaryReply(text);
      }

      return 'Unknown command. Send /help.';
    } catch (err) {
      return html(err instanceof Error ? err.message : String(err));
    }
  }

  telegramBot = config.telegramBotToken
    ? new TelegramBot(config, handleTelegramCommand)
    : null;

  insiderBots.push(new InsiderBot(config, config.insiderSolanaRpcUrl, config.insiderSolanaWsUrl, telegramBot));
  insiderBots.push(new InsiderBot(config, config.insiderSolanaRpcUrl2, config.insiderSolanaWsUrl2, telegramBot));

  if (telegramBot) {
    telegramBot.start();
  }

  earlyBundlerOrchestrator = new EarlyBundlerOrchestrator(config, db, telegramBot, gmgnClients[0]);
  reverseCopySellOrchestrator = new ReverseCopySellOrchestrator(config, db, telegramBot);

  earlyBundlerOrchestrator.on('sellTrigger', (trigger) => {
      if (botMode !== 'bundler') {
        log.info('[EARLY BUNDLER SELL SKIP] Ignoring sell trigger because Bundler mode is inactive', {
          mint: trigger.position.mint,
          mode: botMode,
        });
        return;
      }

      const { position, type, walletAddress, soldPercentage, reason } = trigger;
      
      // Check if sell already pending
      if (hasPendingSellForMint(position.tradingWallet, position.mint)) {
        log.info(`[EARLY BUNDLER SELL SKIP] Sell already pending for ${position.mint}`);
        return;
      }

      // Clear active position immediately on sell trigger to prevent checker from firing
      earlyBundlerOrchestrator.clearActivePosition();
      pendingTradingBuys.delete(position.mint);

      const sellReasons = [
      `Early bundler activity detected for token ${position.mint}`,
      reason || `Trigger type: ${type}`,
    ];
    if (walletAddress) sellReasons.push(`Bundler wallet: ${walletAddress}`);
    if (soldPercentage) sellReasons.push(`Bundler sold: ${soldPercentage.toFixed(2)}%`);

    const event: FilterFailEvent = {
      walletAddress: position.tradingWallet,
      mint: position.mint,
      sampleNumber: 0,
      elapsedSec: 0,
      reasons: sellReasons,
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
      matchingWallets: walletAddress ? [walletAddress] : [],
    };

    const sellId = randomBytes(5).toString('hex');
    pendingSells.set(sellId, {
      event,
      createdAt: Date.now(),
      executing: true,
    });

    log.info(`[EARLY BUNDLER SELL TRIGGER] ${type} for ${position.mint}`, {
      sellId,
      reason,
    });

    void executeSellAndNotify(config.telegramChatId, sellId, telegramBot);
  });

  reverseCopySellOrchestrator.on('sellTrigger', (data) => {
    if (botMode !== 'reverse_copysell') return;
    
    const { position, targetWallet } = data;

    // Check if sell already pending
    if (hasPendingSellForMint(position.tradingWallet, position.mint)) {
      log.info(`[REVERSE-COPYSELL SELL SKIP] Sell already pending for ${position.mint}`);
      return;
    }

    pendingTradingBuys.delete(position.mint);

    const event: FilterFailEvent = {
      walletAddress: position.tradingWallet,
      mint: position.mint,
      sampleNumber: 0,
      elapsedSec: 0,
      reasons: [`Reverse CopySell trigger: Target wallet ${targetWallet} bought the token`],
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

    const sellId = randomBytes(5).toString('hex');
    pendingSells.set(sellId, {
      event,
      createdAt: Date.now(),
      executing: true,
    });

    log.info(`[REVERSE-COPYSELL SELL TRIGGER] Target ${targetWallet} for ${position.mint}`, {
      sellId,
    });

    void executeSellAndNotify(config.telegramChatId, sellId, telegramBot);
  });

  insiderBots.forEach((bot, index) => {
    const client = gmgnClients[index];

    bot.on('buyTrigger', (trigger) => {
      if (!config.tradingWalletAddress) {
        log.warn(`[INSIDER ${index + 1} BUY SKIP] No trading wallet configured`, trigger);
        return;
      }

      log.warn(`[INSIDER ${index + 1} BUY TRIGGER]`, trigger);

      void (async () => {
        try {
            // Fetch top 5 profitable traders (only if not already provided)
            let tradersListStr = trigger.tradersListStr || '';
            if (!tradersListStr) {
              try {
                const summary = await getTraderSummary(trigger.mint, bot, client, 5);
                tradersListStr = summary.tradersListStr;
                if (summary.tradersListStr) {
                  log.warn(`Top 5 ${summary.profitType}-profitable wallets for ${trigger.mint} (Bot ${index + 1}):\n${summary.logTraders.join('\n')}`);
                }
              } catch (err) {
                log.error(`Failed to fetch top traders for message (Bot ${index + 1})`, err);
              }
            }

            await telegramBot?.sendDefault([
              `<b>🚀 Insider ${index + 1} Buy Executing</b>`,
              `Token: <code>${html(trigger.mint)}</code>`,
              `Buying: <b>${html(String(trigger.buySol))} SOL</b>`,
              `Entry MC: <b>$${html(trigger.entryMc?.toLocaleString() ?? 'Unknown')}</b>`,
              `Exit MC: <b>$${html(bot.getExitMc().toLocaleString())}</b>`,
              '',
              tradersListStr ? tradersListStr : '',
              '',
              'Submitting swap...',
            ].filter(Boolean).join('\n'));

            const result = await client.buyTokenWithSol(
              config.tradingWalletAddress!,
              trigger.mint,
              {
                solAmount: trigger.buySol,
                slippage: config.sellSlippage,
                autoSlippage: config.sellAutoSlippage,
                priorityFeeSol: config.sellPriorityFeeSol,
              }
            );
            bot.markPositionBought(trigger);

            await telegramBot?.sendDefault([
              `<b>✅ Insider ${index + 1} Buy Completed</b>`,
              `Token: <code>${html(trigger.mint)}</code>`,
              `Entry MC: <b>$${html(trigger.entryMc?.toLocaleString() ?? 'Unknown')}</b>`,
              `Status: <b>${html(result.status)}</b>`,
              result.hash ? `Tx: https://solscan.io/tx/${html(result.hash)}` : '',
              '',
              '<b>Strategy: MC-Based Exit</b>',
              `Watching for Exit MC: <b>$${html(bot.getExitMc().toLocaleString())}</b>`,
            ].filter(Boolean).join('\n'), {
            replyMarkup: {
              inline_keyboard: [
                [
                  { text: '🔴 Sell Position', callback_data: `sell:insider:${trigger.mint}:${index}` },
                  { text: '🔄 Refresh P/L & MC', callback_data: `r:m:${trigger.mint}:i` }
                ]
              ],
            },
          });
        } catch (err) {
          log.error(`Insider ${index + 1} buy failed`, err);
          await telegramBot?.sendDefault([
            `<b>❌ Insider ${index + 1} Buy Failed</b>`,
            `Token: <code>${html(trigger.mint)}</code>`,
            `Error: ${html(err instanceof Error ? err.message : String(err))}`,
          ].join('\n'));
        }
      })();
    });

    bot.on('sellTrigger', (trigger) => {
      if (!config.tradingWalletAddress) {
        log.warn(`[INSIDER ${index + 1} SELL SKIP] No trading wallet configured`, trigger);
        return;
      }
      if (hasPendingSellForMint(config.tradingWalletAddress, trigger.positionMint)) {
        log.info(`[INSIDER ${index + 1} SELL SKIP] Sell already pending for ${trigger.positionMint}`);
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

      const sellId = randomBytes(5).toString('hex');
      pendingSells.set(sellId, {
        event,
        createdAt: Date.now(),
        executing: true,
      });

      telegramBot?.sendDefault([
        `<b>🚨 Insider ${index + 1} Sell Triggered</b>`,
        `Token: <code>${html(trigger.positionMint)}</code>`,
        `Reason: <b>${trigger.reason}</b>`,
        `Action: submit sell for <b>${config.sellPercent}%</b>.`,
      ].join('\n'), {
        replyMarkup: {
          inline_keyboard: [[{ text: '🔄 Refresh P/L & MC', callback_data: `r:m:${trigger.positionMint}:i` }]],
        },
      }).catch((err) => log.warn(`Telegram insider ${index + 1} sell alert failed`, err));

      void executeSellAndNotify(config.telegramChatId, sellId, telegramBot);
    });

    bot.on('error', (err) => {
      log.error(`Insider ${index + 1} error:`, err);
      telegramBot?.sendDefault(`<b>⚠️ Insider Bot ${index + 1} Error</b>\n${html(err.message)}`).catch((e) => log.warn('Telegram error alert failed', e));
    });
  });

  // ── 5. Bot Logic ──────────────────────────────────────────────────────────

  function queueWatchedWalletSell(
    watchedBuy: NewTokenEvent,
    tradingPosition: NewTokenEvent
  ): void {
    if (hasPendingSellForMint(tradingPosition.walletAddress, tradingPosition.mint)) {
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
        'Configured action triggered: sell immediately on reverse-buy wallet buy signal.',
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
      buySol: tradingPosition.buySol ?? db.getToken(tradingPosition.walletAddress, tradingPosition.mint)?.buySol ?? null,
      matchingWallets: [watchedBuy.walletAddress],
    };
    const sellId = randomBytes(5).toString('hex');
    pendingSells.set(sellId, {
      event,
      createdAt: Date.now(),
      executing: true,
    });
    telegramBot?.sendDefault([
      '<b>Reverse-Buy Wallet Triggered Sell</b>',
      `Reverse-buy wallet: <code>${html(watchedBuy.walletAddress)}</code>`,
      `Trading wallet: <code>${html(tradingPosition.walletAddress)}</code>`,
      `Token: <code>${html(tradingPosition.mint)}</code>`,
      `Action: submit sell for <b>${config.sellPercent}%</b> immediately.`,
    ].join('\n')).catch((err) => log.warn('Telegram watched-wallet sell trigger alert failed', err));
    void executeSellAndNotify(config.telegramChatId, sellId, telegramBot);
  }

  function wireTradingWalletMonitor(walletMonitor: WalletMonitor): void {
    walletMonitor.on('newToken', (event: NewTokenEvent) => {
      if (botMode === 'bundler') {
        pendingTradingBuys.set(event.mint, event);
        log.info(`[TRADING POSITION OPEN] Wallet: ${event.walletAddress} Mint: ${event.mint}`);
        
        // Also trigger early bundler detection
        earlyBundlerOrchestrator.handleTradingWalletBuy(event).catch((err) => {
          log.error('Failed to trigger early bundler detection', err);
        });

        telegramBot?.sendDefault([
          '<b>Trading Wallet Position Opened</b>',
          `Wallet: <code>${html(event.walletAddress)}</code>`,
          `Token: <code>${html(event.mint)}</code>`,
          `Buy SOL: <b>${event.buySol ?? 'unknown'}</b>`,
          'Watching this token for buys from wallets explicitly added to reverse-buy trigger list in settings.',
          'If one of those wallets buys it while this position is still open, sell submits immediately.',
          '',
          '<b>Early Bundler Bot</b>: Detection started. I will notify you if early bundler activity is detected.',
        ].join('\n')).catch((err) => log.warn('Telegram trading buy alert failed', err));
      } else if (botMode === 'reverse_copysell') {
        log.info(`[REVERSE-COPYSELL POSITION OPEN] Wallet: ${event.walletAddress} Mint: ${event.mint}`);
        reverseCopySellOrchestrator.handleTradingWalletBuy(event).catch((err) => {
          log.error('Failed to trigger reverse-copysell detection', err);
        });
      }
    });

    walletMonitor.on('tokenExited', (event: TokenExitEvent) => {
      if (botMode === 'bundler') {
        if (!pendingTradingBuys.has(event.mint)) return;
        pendingTradingBuys.delete(event.mint);
        log.info(`[TRADING POSITION EXITED] Wallet: ${event.walletAddress} Mint: ${event.mint} — stopped watched-wallet trigger watch`);
        
        // Also notify orchestrator
        earlyBundlerOrchestrator.handleTradingWalletExit(event).catch((err) => {
          log.error('Failed to notify early bundler orchestrator of exit', err);
        });

        telegramBot?.sendDefault([
          '<b>Trading Wallet Position Closed</b>',
          `Wallet: <code>${html(event.walletAddress)}</code>`,
          `Token: <code>${html(event.mint)}</code>`,
          'Stopped waiting for watched-wallet buy triggers for this token.',
          '',
          '<b>Early Bundler Bot</b>: Monitoring stopped for this token.',
        ].join('\n')).catch((err) => log.warn('Telegram trading exit alert failed', err));
      } else if (botMode === 'reverse_copysell') {
        log.info(`[REVERSE-COPYSELL POSITION EXITED] Wallet: ${event.walletAddress} Mint: ${event.mint}`);
        reverseCopySellOrchestrator.handleTradingWalletExit(event).catch((err) => {
          log.error('Failed to notify reverse-copysell orchestrator of exit', err);
        });
      }
    });
  }

  function wireWatchedWalletMonitor(walletMonitor: WalletMonitor): void {
    walletMonitor.on('newToken', (event: NewTokenEvent) => {
      if (botMode !== 'bundler') return;
      log.info(`[WATCHED BUY] Wallet: ${event.walletAddress} Mint: ${event.mint}`);
      startWatchedWalletSummary(event);
      if (!db.isReverseBuyWallet(event.walletAddress)) {
        log.info(`[REVERSE BUY NOT CONFIGURED] Wallet: ${event.walletAddress} Mint: ${event.mint}`);
        return;
      }
      const tradingPosition = pendingTradingBuys.get(event.mint);
      if (tradingPosition) {
        pendingTradingBuys.delete(event.mint);
        queueWatchedWalletSell(event, tradingPosition);
      }
    });
  }

  async function checkAndSellIfLowMcap(mint: string, context: 'insider' | 'bundler' | 'reverse_copysell', botIndex?: number): Promise<void> {
    if (!config.tradingWalletAddress) return;
    if (hasPendingSellForMint(config.tradingWalletAddress, mint)) return;

    try {
      const client = (context === 'insider' && botIndex !== undefined) ? gmgnClients[botIndex] : gmgnClients[0];
      const currentMc = await client.fetchTokenMarketCapUsd(mint);
      if (currentMc !== null && currentMc < INSIDER_MIN_MARKET_CAP_USD) {
        log.warn(`[MCAP SELL TRIGGER] Market cap $${currentMc.toLocaleString()} below $${INSIDER_MIN_MARKET_CAP_USD.toLocaleString()} for ${mint}`, { context });

        const event: FilterFailEvent = {
          walletAddress: config.tradingWalletAddress,
          mint,
          sampleNumber: 0,
          elapsedSec: 0,
          reasons: [
            `Market cap $${currentMc.toLocaleString()} fell below $${INSIDER_MIN_MARKET_CAP_USD.toLocaleString()}.`,
            `Context: ${context} position.`,
            'Periodic market cap checker triggered automatic sell.',
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
          buySol: context === 'insider'
            ? (insiderBots.find(b => b.getActivePosition()?.mint === mint) || insiderBots[0]).getBuySol()
            : (context === 'reverse_copysell'
              ? (reverseCopySellOrchestrator.getActivePosition()?.buySol ?? null)
              : (earlyBundlerOrchestrator.getActivePosition()?.buySol ?? null)),
          matchingWallets: [],
        };

        const sellId = randomBytes(5).toString('hex');
        pendingSells.set(sellId, {
          event,
          createdAt: Date.now(),
          executing: true,
        });

        // Clear active position immediately on sell trigger to prevent checker from firing again
        if (context === 'insider') {
          const bot = insiderBots.find(b => b.getActivePosition()?.mint === mint);
          if (bot) bot.clearActivePosition();
        } else if (context === 'bundler') {
          earlyBundlerOrchestrator.clearActivePosition();
          pendingTradingBuys.delete(mint);
        } else if (context === 'reverse_copysell') {
          reverseCopySellOrchestrator.stopActiveMonitoring('Market cap fell below minimum');
        }

        telegramBot?.sendDefault([
          '<b>🚨 Market Cap Sell Triggered</b>',
          `Token: <code>${html(mint)}</code>`,
          `Market Cap: <b>$${currentMc.toLocaleString()}</b>`,
          `Threshold: <b>$${INSIDER_MIN_MARKET_CAP_USD.toLocaleString()}</b>`,
          `Action: submit sell for <b>${config.sellPercent}%</b>.`,
        ].join('\n')).catch((err) => log.warn('Telegram mcap sell alert failed', err));

        void executeSellAndNotify(config.telegramChatId, sellId, telegramBot);
      }
    } catch (err) {
      log.error(`Failed to check market cap for ${mint}`, err);
    }
  }

  async function getTraderSummary(mint: string, bot: InsiderBot, client: GmgnClient, limit: number = 5): Promise<{
    maxTransferProfit: number;
    tradersListStr: string;
    logTraders: string[];
    profitLabel: string;
    profitType: string;
  }> {
    const profitType = bot.getProfitType();
    const followedWallet = bot.getFollowedWallet();
    
    // Fetch traders only (removed metrics call for holders %)
    const traders = await client.fetchTokenTraders(mint, 50, 'profit');

    if (!traders || !Array.isArray(traders.list)) {
      return { maxTransferProfit: -1, tradersListStr: '', logTraders: [], profitLabel: '', profitType };
    }

    const formatTraders = (list: any[], type: 'realized' | 'total') => {
      const sorted = [...list].sort((a, b) => {
        const valA = type === 'realized' ? (a.realized_profit ?? 0) : (a.profit ?? 0);
        const valB = type === 'realized' ? (b.realized_profit ?? 0) : (b.profit ?? 0);
        return valB - valA;
      }).slice(0, limit);

      let maxTP = -1;
      const formatted = sorted.map((t: any, i: number) => {
        const isFollowed = followedWallet && t.address === followedWallet;
        const hasTransfer = t.token_transfer_in?.tx_hash || t.token_transfer?.type === 'transfer_in' || (t.maker_token_tags && t.maker_token_tags.includes('transfer_in'));
        const profit = type === 'realized' ? (t.realized_profit ?? 0) : (t.profit ?? 0);
        
        if (hasTransfer && profit > maxTP) maxTP = profit;

        let tags = [];
        if (isFollowed) tags.push('👤 <b>FOLLOWED</b>');
        if (hasTransfer) tags.push('🟢 <b>Transfer</b>');
        const tagsStr = tags.length > 0 ? ` [${tags.join(', ')}]` : '';

        return `${i + 1}. <code>${html(t.address)}</code>: <b>$${html(profit.toLocaleString())}</b>${tagsStr}`;
      });

      const logLines = sorted.map((t: any, i: number) => {
        const isFollowed = followedWallet && t.address === followedWallet;
        const hasTransfer = t.token_transfer_in?.tx_hash || t.token_transfer?.type === 'transfer_in' || (t.maker_token_tags && t.maker_token_tags.includes('transfer_in'));
        const profit = type === 'realized' ? (t.realized_profit ?? 0) : (t.profit ?? 0);
        let logTags = [];
        if (isFollowed) logTags.push('FOLLOWED');
        if (hasTransfer) logTags.push('Transfer');
        const logTagsStr = logTags.length > 0 ? ` [${logTags.join(', ')}]` : '';
        return `${i + 1}. ${t.address}: $${profit.toLocaleString()}${logTagsStr}`;
      });

      return { maxTP, formatted, logLines };
    };

    if (profitType === 'both') {
      const total = formatTraders(traders.list, 'total');
      const realized = formatTraders(traders.list, 'realized');
      
      const tradersListStr = [
        '<b>Top 5 Total-Profitable:</b>',
        ...total.formatted,
        '',
        '<b>Top 5 Realized-Profitable:</b>',
        ...realized.formatted
      ].join('\n');

      return {
          maxTransferProfit: Math.max(total.maxTP, realized.maxTP),
          tradersListStr,
          logTraders: [...total.logLines, '---', ...realized.logLines],
          profitLabel: 'Combined (Total/Realized)',
          profitType,
        };
      } else {
        const result = formatTraders(traders.list, profitType);
        const label = profitType === 'realized' ? 'Realized' : 'Total';
        return {
          maxTransferProfit: result.maxTP,
          tradersListStr: `<b>Top 5 ${label}-Profitable:</b>\n` + result.formatted.join('\n'),
          logTraders: result.logLines,
          profitLabel: `${label} Profit`,
          profitType,
        };
      }
    }

  async function checkInsiderMcapFlow(index: number): Promise<void> {
    const bot = insiderBots[index];
    const client = gmgnClients[index];
    const preBuyMint = bot.getPreBuyMint();
    const activePos = bot.getActivePosition();

    if (!preBuyMint && !activePos) return;

    const mint = preBuyMint || activePos!.mint;
    const checkKey = `${index}:${mint}`;
    const checkCount = insiderCheckState.get(checkKey) || 0;

    try {
      // 1. Fetch current Market Cap
      const currentMc = await client.fetchTokenMarketCapUsd(mint);

      if (currentMc === null) return;

      log.debug(`[INSIDER ${index + 1} MC CHECK] Token: ${mint} MC: $${currentMc.toLocaleString()}`);

      // 2. Rug protection: MC < $1k
      if (currentMc < 1000) {
        const reason = `Market cap $${currentMc.toLocaleString()} below $1,000 (Rug)`;
        log.warn(`[INSIDER ${index + 1} RUG] ${reason} for ${mint}. Resetting.`);
        
        if (activePos && config.tradingWalletAddress) {
          // Trigger immediate sell if we have a position
          const event: FilterFailEvent = {
            walletAddress: config.tradingWalletAddress,
            mint,
            sampleNumber: 0,
            elapsedSec: 0,
            reasons: [reason],
            settings: db.getWalletSettings(config.tradingWalletAddress),
            metrics: { mint, timestamp: new Date().toISOString(), bundlersPercent: null, bundlersCount: null, initialBaseReserve: null, topWallets: null, top10HolderRate: null, bundledAmountRate: null },
            buySol: bot.getBuySol(),
            matchingWallets: [],
          };
          const sellId = randomBytes(5).toString('hex');
          pendingSells.set(sellId, { event, createdAt: Date.now(), executing: true });
          void executeSellAndNotify(config.telegramChatId, sellId, telegramBot);
          
          telegramBot?.sendDefault([
            `<b>🚨 Insider ${index + 1} Rug Protection Triggered</b>`,
            `Token: <code>${html(mint)}</code>`,
            `Market Cap: <b>$${html(currentMc.toLocaleString())}</b>`,
            `Reason: <b>${reason}</b>`,
            'Action: Selling immediately and resetting.',
          ].join('\n')).catch((err) => log.warn('Telegram rug alert failed', err));
        } else if (activePos && !config.tradingWalletAddress) {
          log.warn(`[INSIDER ${index + 1} RUG SKIP SELL] Cannot sell rugged token ${mint} because no TRADING_WALLET_ADDRESS is configured.`);
          telegramBot?.sendDefault([
            `<b>⚠️ Insider ${index + 1} Rug Detected</b>`,
            `Token: <code>${html(mint)}</code>`,
            'Reason: Rug detected, but no trading wallet configured to sell.',
          ].join('\n')).catch((err) => log.warn('Telegram rug alert failed', err));
        } else {
          telegramBot?.sendDefault([
            `<b>⚠️ Insider ${index + 1} Watch Reset (Rug)</b>`,
            `Token: <code>${html(mint)}</code>`,
            `Market Cap: <b>$${html(currentMc.toLocaleString())}</b>`,
            `Reason: ${reason}. Aborting entry sequence.`,
          ].join('\n')).catch((err) => log.warn('Telegram rug alert failed', err));
        }
        
        bot.clearActivePosition();
        insiderCheckState.delete(checkKey);
        return;
      }

      // 2. Entry / Flow Check
      // We check entry conditions if we have a pre-buy mint, OR if we just bought (checkCount 1) and want the v2 flow card.
      if (preBuyMint || (activePos && checkCount === 1)) {
        const entryMc = bot.getEntryMc();
        if (currentMc >= entryMc || activePos) {
          const nextCount = checkCount + 1;
          insiderCheckState.set(checkKey, nextCount);

          log.warn(`[INSIDER ${index + 1} FLOW] Token: ${mint} MC: $${currentMc.toLocaleString()} Check: ${nextCount}/2`);
          
          try {
            const summary = await getTraderSummary(mint, bot, client, 5);
            const { maxTransferProfit, tradersListStr, logTraders, profitLabel, profitType } = summary;

            if (nextCount === 1) {
              log.info(`[INSIDER ${index + 1} v1] First threshold hit for ${mint}. Evaluating action.`);
              
              const minProfit = bot.getMinTransferProfit();
              const isBuyDisabled = bot.isBuyDisabled();
              let actionTitle = "Entry v1";

              if (maxTransferProfit > minProfit) {
                if (isBuyDisabled) {
                  actionTitle = "Buy Requirements Met (SKIP v1)";
                  log.info(`[INSIDER ${index + 1} SKIP v1] Buy requirements met (${profitLabel}: $${maxTransferProfit.toLocaleString()} > $${minProfit}), but Auto Buy is DISABLED.`);
                } else {
                  actionTitle = "Entry Triggered (BUY v1)";
                  log.warn(`[INSIDER ${index + 1} ENTRY v1] Top transfer wallet ${profitLabel} profit: $${maxTransferProfit.toLocaleString()} (> $${minProfit}). Triggering BUY.`);
                  
                  // Set Exit MC
                  const exitPercent = bot.getExitPercent();
                  const newExitMc = currentMc * (1 + exitPercent / 100);
                  bot.setExitMc(newExitMc);
                  log.warn(`[INSIDER ${index + 1} EXIT SET] Exit MC set to $${newExitMc.toLocaleString()} (${exitPercent}% increase from $${currentMc.toLocaleString()})`);

                  bot.emit('buyTrigger', {
                    followedWallet: bot.getFollowedWallet()!,
                    mint: mint,
                    signature: 'MC_TRIGGER',
                    buySol: bot.getBuySol(),
                    entryMc: currentMc,
                    tradersListStr: tradersListStr,
                  });
                  // Note: preBuyMint will be cleared by the bot's internal markPositionBought
                }
              } else {
                actionTitle = "Entry Condition Not Met (v1)";
                const reason = maxTransferProfit === -1 
                  ? "No transfer wallets found."
                  : `Highest transfer wallet ${profitLabel} profit is $${maxTransferProfit.toLocaleString()} (<= $${minProfit}).`;
                log.info(`[INSIDER ${index + 1} NO-BUY v1] MC reached but ${reason}`);
              }

              telegramBot?.sendDefault([
                `<b>🚀 Insider ${index + 1} ${actionTitle}</b>`,
                `Token: <code>${html(mint)}</code>`,
                `Market Cap: <b>$${html(currentMc.toLocaleString())}</b>`,
                `Threshold MC: <b>$${html(entryMc.toLocaleString())}</b>`,
                `Exit MC: <b>$${html(bot.getExitMc().toLocaleString())}</b>`,
                `Buy SOL: <b>${html(String(bot.getBuySol()))} SOL</b>`,
                '',
                tradersListStr ? tradersListStr : '',
              ].filter(Boolean).join('\n'), { pin: true }).catch((err) => log.warn(`Telegram insider ${index + 1} v1 alert failed`, err));
              
              if (tradersListStr) {
                log.warn(`Top 5 ${profitType}-profitable wallets for ${mint} (Bot ${index + 1} v1):\n${logTraders.join('\n')}`);
              }
              return; // Wait for next check interval (v2)
            }

            if (nextCount === 2) {
              log.info(`[INSIDER ${index + 1} v2] Second check for ${mint}. Sending flow card.`);
              
              telegramBot?.sendDefault([
                `<b>🚀 Insider ${index + 1} Flow v2</b>`,
                `Token: <code>${html(mint)}</code>`,
                `Market Cap: <b>$${html(currentMc.toLocaleString())}</b>`,
                `Exit MC: <b>$${html(bot.getExitMc().toLocaleString())}</b>`,
                '',
                tradersListStr ? tradersListStr : '',
              ].filter(Boolean).join('\n')).catch((err) => log.warn(`Telegram insider ${index + 1} v2 alert failed`, err));

              if (tradersListStr) {
                log.warn(`Top 5 ${profitType}-profitable wallets for ${mint} (Bot ${index + 1} v2):\n${logTraders.join('\n')}`);
              }

              insiderCheckState.delete(checkKey);
              if (preBuyMint) {
                // If it wasn't bought at v1, clear it now after v2 flow msg
                bot.clearPreBuyMint();
              }
              return;
            }
          } catch (err) {
            log.error(`Failed to fetch top traders for entry flow (Bot ${index + 1})`, err);
          }
        }
      } 
      // 3. Exit Check
      // Only run if we are not in the middle of the 2-step flow
      else if (activePos && (checkCount === 0 || checkCount >= 2)) {
        const exitMc = bot.getExitMc();
        if (currentMc >= exitMc) {
          log.warn(`[INSIDER ${index + 1} EXIT] MC $${currentMc.toLocaleString()} reached Exit MC $${exitMc.toLocaleString()}. Triggering SELL.`);
          bot.emit('sellTrigger', {
            followedWallet: bot.getFollowedWallet()!,
            positionMint: activePos.mint,
            signature: 'MC_TRIGGER',
            reason: `Target Exit MC $${exitMc.toLocaleString()} reached`,
          });
        }
      }
    } catch (err) {
      log.error(`Failed to check Insider MC flow for ${mint} (Bot ${index + 1})`, err);
    }
  }

  function startMarketCapChecker(): void {
    log.info(`Starting periodic market cap checker (interval: ${MCAP_CHECK_INTERVAL_MS}ms)`);
    let isChecking = false;
    setInterval(async () => {
      if (isChecking) return;
      isChecking = true;
      try {
        // 1. Insider Mode MC Flow
        if (botMode === 'insider') {
          for (let i = 0; i < insiderBots.length; i++) {
            await checkInsiderMcapFlow(i);
            const pos = insiderBots[i].getActivePosition();
            if (pos) {
              await checkAndSellIfLowMcap(pos.mint, 'insider', i);
            }
          }
        }

        if (!config.tradingWalletAddress) return;

        // 2. Bundler Mode positions
        if (botMode === 'bundler') {
          const bundlerPos = earlyBundlerOrchestrator.getActivePosition();
          if (bundlerPos) {
            await checkAndSellIfLowMcap(bundlerPos.mint, 'bundler');
          }
          for (const mint of pendingTradingBuys.keys()) {
            if (bundlerPos?.mint === mint) continue;
            await checkAndSellIfLowMcap(mint, 'bundler');
          }
        }

        // 3. Reverse CopySell positions
        if (botMode === 'reverse_copysell') {
          const revPos = reverseCopySellOrchestrator.getActivePosition();
          if (revPos) {
            await checkAndSellIfLowMcap(revPos.mint, 'reverse_copysell');
          }
        }
      } finally {
        isChecking = false;
      }
    }, MCAP_CHECK_INTERVAL_MS);
  }

  function startWatchedWalletSummary(event: NewTokenEvent): void {
    if (!db.tokenExists(event.walletAddress, event.mint)) {
      db.insertToken({
        walletAddress: event.walletAddress,
        mint: event.mint,
        firstSeen: new Date().toISOString(),
        monitoringStatus: 'active',
        detectedAt: event.detectedAt,
        buySol: event.buySol,
      });
    }

    telegramBot?.sendDefault([
      '<b>Watched Wallet Monitoring Started</b>',
      `Wallet: <code>${html(event.walletAddress)}</code>`,
      `Token: <code>${html(event.mint)}</code>`,
      `Buy SOL: <b>${event.buySol ?? 'unknown'}</b>`,
      'Sell trigger only happens if this wallet is explicitly added to reverse-buy trigger list in settings.',
    ].join('\n')).catch((err) => log.warn('Telegram watched-wallet monitor alert failed', err));
  }

  async function startWallet(address: string): Promise<string> {
    const normalized = new PublicKey(address).toBase58();
    if (walletMonitors.has(normalized)) {
      return `Already monitoring <code>${normalized}</code>`;
    }

    db.addWallet(normalized);
    const settings = db.getWalletSettings(normalized);
    const minBuySol = settings.minSolBuy !== null && settings.minSolBuy > 0 ? settings.minSolBuy : undefined;
    const monitor = new WalletMonitor(config, normalized, { enforceMinBuySol: true, minBuySol });
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
      if (pausedWallets.has(normalized)) return `Trading wallet is already paused.`;
      tradingWalletMonitor.stop();
      pausedWallets.add(normalized);
      return `Paused monitoring your TRADING wallet <code>${normalized}</code>`;
    }

    const monitor = walletMonitors.get(normalized);
    if (!monitor) return `Wallet is not monitored: <code>${normalized}</code>`;
    if (pausedWallets.has(normalized)) return `Wallet is already paused: <code>${normalized}</code>`;
    monitor.stop();
    pausedWallets.add(normalized);
    return `Paused monitoring <code>${normalized}</code>`;
  }

  async function resumeWallet(address: string): Promise<string> {
    const normalized = new PublicKey(address).toBase58();

    if (normalized === config.tradingWalletAddress) {
      if (!tradingWalletMonitor) return `Trading wallet is not active.`;
      if (!pausedWallets.has(normalized)) return `Trading wallet is already running.`;
      pausedWallets.delete(normalized);
      await tradingWalletMonitor.start();
      return `Continued monitoring your TRADING wallet <code>${normalized}</code>`;
    }

    const monitor = walletMonitors.get(normalized);
    if (!monitor) return `Wallet is not monitored: <code>${normalized}</code>`;
    if (!pausedWallets.has(normalized)) return `Wallet is already running: <code>${normalized}</code>`;
    pausedWallets.delete(normalized);
    await monitor.start();
    return `Continued monitoring <code>${normalized}</code>`;
  }

  async function startBundlerModeServices(): Promise<void> {
    earlyBundlerOrchestrator.setEnabled(true);

    if (config.tradingWalletAddress && !tradingWalletMonitor) {
      tradingWalletMonitor = new WalletMonitor(config, config.tradingWalletAddress, { enforceMinBuySol: false });
      wireTradingWalletMonitor(tradingWalletMonitor);
      await tradingWalletMonitor.start();

      for (const mint of tradingWalletMonitor.existingMints) {
        pendingTradingBuys.set(mint, {
          walletAddress: config.tradingWalletAddress,
          mint,
          detectedAt: Date.now(),
          buySol: db.getToken(config.tradingWalletAddress, mint)?.buySol ?? null,
        });
      }

      if (tradingWalletMonitor.existingMints.size > 0) {
        telegramBot?.sendDefault([
          '<b>Trading Wallet Positions Loaded</b>',
          `Wallet: <code>${html(config.tradingWalletAddress)}</code>`,
          `Positions watched for watched-wallet buy triggers: <b>${tradingWalletMonitor.existingMints.size}</b>`,
        ].join('\n')).catch((err) => log.warn('Telegram trading positions bootstrap alert failed', err));
      }
    } else if (!config.tradingWalletAddress) {
      log.warn('No TRADING_WALLET_ADDRESS configured; bundler sell flow cannot detect your buys.');
    }

    const wallets = db.getActiveWallets();
    for (const wallet of wallets) {
      if (wallet === config.tradingWalletAddress || walletMonitors.has(wallet)) continue;
      await startWallet(wallet);
    }

    log.info('Bundler mode services started', {
      watchedWallets: walletMonitors.size,
      tradingWalletActive: !!tradingWalletMonitor,
    });
  }

  async function stopBundlerModeServices(reason = 'Bundler mode stopped'): Promise<void> {
    for (const monitor of walletMonitors.values()) {
      monitor.stop();
    }
    walletMonitors.clear();

    tradingWalletMonitor?.stop();
    tradingWalletMonitor = null;
    pendingTradingBuys.clear();
    pausedWallets.clear();

    await earlyBundlerOrchestrator.stopActiveMonitoring(reason);

    log.info('Bundler mode services stopped', { reason });
  }

  async function startReverseCopySellModeServices(): Promise<void> {
    reverseCopySellOrchestrator.setEnabled(true);

    if (config.tradingWalletAddress && !tradingWalletMonitor) {
      tradingWalletMonitor = new WalletMonitor(config, config.tradingWalletAddress, { enforceMinBuySol: false });
      wireTradingWalletMonitor(tradingWalletMonitor);
      await tradingWalletMonitor.start();
    } else if (!config.tradingWalletAddress) {
      log.warn('No TRADING_WALLET_ADDRESS configured; reverse-copysell flow cannot detect your buys.');
    }

    log.info('Reverse CopySell mode services started', {
      tradingWalletActive: !!tradingWalletMonitor,
    });
  }

  async function stopReverseCopySellModeServices(reason = 'Reverse CopySell mode stopped'): Promise<void> {
    tradingWalletMonitor?.stop();
    tradingWalletMonitor = null;
    pendingTradingBuys.clear();
    pausedWallets.clear();

    await reverseCopySellOrchestrator.stopActiveMonitoring(reason);
    reverseCopySellOrchestrator.setEnabled(false);

    log.info('Reverse CopySell mode services stopped', { reason });
  }

  const html = (value: string): string =>
    value
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;');

  const fmtSetting = (value: number | null): string =>
    value === null ? 'Any' : String(value);
  const enabledMark = (enabled: boolean): string => enabled ? '✅' : '❌';

  function updateMinSol(
    walletAddress: string,
    rawValue: string
  ): string {
    const normalized = new PublicKey(walletAddress).toBase58();
    const isTrading = normalized === config.tradingWalletAddress;
    const trimmed = rawValue.trim().toLowerCase();
    const settings = db.getWalletSettings(normalized);

    if (trimmed === 'off' || trimmed === 'any' || trimmed === 'none' || trimmed === 'default') {
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

  function walletSummaryReply(address: string, editCurrent = false): TelegramReply {
    const normalized = new PublicKey(address).toBase58();
    const isTrading = normalized === config.tradingWalletAddress;
    const isMonitoring = isTrading ? !!tradingWalletMonitor : walletMonitors.has(normalized);
    const isPaused = pausedWallets.has(normalized);
    const reverseBuyEnabled = !isTrading && db.isReverseBuyWallet(normalized);
    const settings = db.getWalletSettings(normalized);
    const minSolBuy = settings.minSolBuy ?? (isTrading ? 0 : config.minBuySol);
    const status = !isMonitoring ? 'Not active' : isPaused ? 'Paused' : 'Active';
    
    const pauseButton = isPaused
      ? { text: 'Continue monitoring', callback_data: `wallet:resume:${normalized}` }
      : { text: 'Pause monitoring', callback_data: `wallet:pause:${normalized}` };

    const keyboard = [];
    
    // Row 1: Min SOL Button
    keyboard.push([{
      text: `Min SOL: ${minSolBuy}`,
      callback_data: `set:minSol:${normalized}`,
    }]);

    // Row 2: Pause and Action Buttons
    const row2 = [pauseButton];
    if (isTrading) {
      // No extra action button for trading wallet
    } else {
      row2.push(!isMonitoring
        ? { text: 'Add wallet', callback_data: `wallet:add:${normalized}` }
        : { text: 'Remove wallet', callback_data: `wallet:remove:${normalized}` }
      );
    }
    keyboard.push(row2);

    // Row 3: Reverse Buy (only for watched wallets)
    if (!isTrading && isMonitoring) {
      keyboard.push([{
        text: `${reverseBuyEnabled ? 'Remove' : 'Add'} reverse-buy trigger`,
        callback_data: `${reverseBuyEnabled ? 'reverse:remove' : 'reverse:add'}:${normalized}`,
      }]);
    }

    // Row 4: Navigation
    keyboard.push([
      { text: 'Back', callback_data: 'menu:refresh' },
      { text: 'Refresh', callback_data: `wallet:refresh:${normalized}` },
    ]);

    const flowDesc = isTrading
      ? [
          '<b>Flow Description</b>',
          '• When this wallet buys a token, early bundlers are detected.',
          '• Monitoring begins for identified bundler wallets.',
          '• If a bundler sells 40%, an immediate sell is triggered.',
        ]
      : [
          '<b>Flow Description</b>',
          '• When your trading wallet buys a token, early bundlers are detected.',
          '• If this wallet buys the same token, an immediate sell is triggered.',
          '• If a bundler sells 40% of holdings, an immediate sell is triggered.',
        ];

    return {
      text: [
        isTrading ? '<b>💳 Trading Wallet</b>' : '<b>✅ Watched Wallet</b>',
        `<code>${html(normalized)}</code>`,
        '',
        `Status: <b>${status}</b>`,
        `Min SOL: <b>${minSolBuy}</b>`,
        !isTrading && isMonitoring ? `Reverse-buy: <b>${reverseBuyEnabled ? 'ENABLED' : 'DISABLED'}</b>` : '',
        '',
        ...flowDesc,
      ].filter(Boolean).join('\n'),
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
    if (botMode === 'insider') {
      const bot = insiderBots[activeInsiderIndex];
      const followedWallet = bot.getFollowedWallet();
      const insiderRunning = bot.isRunning();
      const preBuyMint = bot.getPreBuyMint();
      const activePos = bot.getActivePosition();
      
      let status = 'Idle';
      if (insiderRunning) {
        if (activePos) status = `Holding token ${html(activePos.mint.slice(0, 8))}...`;
        else if (preBuyMint) status = `Watching token ${html(preBuyMint.slice(0, 8))}...`;
        else status = 'Running';
      } else if (followedWallet) {
        status = 'Paused';
      }

      const stopResumeButton = followedWallet && !insiderRunning
        ? { text: 'Resume', callback_data: 'insider:resume' }
        : { text: 'Stop', callback_data: 'insider:stop' };

      const buyDisabled = bot.isBuyDisabled();
      const disableBuyButton = {
        text: buyDisabled ? '✅ Enable Buy' : '❌ Disable Buy',
        callback_data: 'insider:togglebuy',
      };

      const profitType = bot.getProfitType();
      let profitLabel = 'Both (Total + Realized)';
      if (profitType === 'total') profitLabel = 'Total Profit';
      else if (profitType === 'realized') profitLabel = 'Realized Profit';

      const toggleProfitButton = {
        text: profitType === 'both' ? '📈📊 Mode: Both' : (profitType === 'realized' ? '📊 Mode: Realized' : '📈 Mode: Total'),
        callback_data: 'insider:toggleprofit',
      };

      const botSelectionRow = [
        { text: activeInsiderIndex === 0 ? '🟢 Bot 1' : 'Bot 1', callback_data: 'insider:select:0' },
        { text: activeInsiderIndex === 1 ? '🟢 Bot 2' : 'Bot 2', callback_data: 'insider:select:1' },
      ];

      return {
        text: [
          `<b>Insider Bot ${activeInsiderIndex + 1}</b>`,
          '',
          `Mode: <b>Insider</b>`,
          `Status: <b>${status}</b>`,
          `Follow wallet: ${followedWallet ? `<code>${html(followedWallet)}</code>` : '<b>Not set</b>'}`,
          `Buy SOL: <b>${html(String(bot.getBuySol()))}</b>`,
          `Entry MC: <b>$${html(bot.getEntryMc().toLocaleString())}</b>`,
          `Exit Strategy: <b>+${html(String(bot.getExitPercent()))}% from Entry</b>`,
          `Min Transfer Profit: <b>$${html(bot.getMinTransferProfit().toLocaleString())}</b>`,
          `Ranking: <b>${profitLabel}</b>`,
          `Auto Buy: <b>${buyDisabled ? 'Disabled ❌' : 'Enabled ✅'}</b>`,
          '',
          '<b>Flow</b>',
          '1. Set follow wallet, entry MC, and exit % increase.',
          '2. Bot waits for the followed wallet to buy a new token.',
          '3. Once detected, bot watches that token\'s Market Cap.',
          '4. Bot buys when MC >= Entry MC.',
          '5. Bot sets Exit MC at your chosen % increase from entry MC.',
          '6. Bot sells when MC >= Exit MC.',
          '• Rug Protection: Bot resets if MC is below $1,000.',
        ].join('\n'),
        replyMarkup: {
          inline_keyboard: [
            [
              { text: 'Insider', callback_data: 'mode:insider' },
              { text: 'Bundler', callback_data: 'mode:bundler' },
              { text: 'Rev CopySell', callback_data: 'mode:reverse_copysell' },
            ],
            botSelectionRow,
            [
              { text: 'Follow wallet', callback_data: 'insider:follow' },
              { text: 'Buy SOL', callback_data: 'insider:buysol' },
            ],
            [
              { text: 'Set Entry MC', callback_data: 'insider:entrymc' },
              { text: 'Set Exit %', callback_data: 'insider:exitpercent' },
            ],
            [
              { text: 'Set Min Profit', callback_data: 'insider:minprofit' },
              toggleProfitButton,
            ],
            [
              disableBuyButton,
              { text: 'Refresh', callback_data: 'menu:refresh' },
            ],
            [
              stopResumeButton,
            ],
          ],
        },
        editCurrent,
      };
    }

    if (botMode === 'reverse_copysell') {
      const targetWallet = config.reverseCopySellTargetWallet;
      const activePosition = reverseCopySellOrchestrator.getActivePosition();
      return {
        text: [
          '<b>Reverse CopySell Bot</b>',
          '',
          `Mode: <b>Reverse CopySell</b>`,
          `Target: ${targetWallet ? `<code>${html(targetWallet)}</code>` : '<b>Not set</b>'}`,
          activePosition ? `Monitoring: <code>${html(activePosition.mint)}</code>` : 'Status: <b>Waiting for buy...</b>',
          '',
          '<b>Flow</b>',
          '• Watches your trading wallet for new buys.',
          '• Once you buy, I watch the target wallet for the same token.',
          '• When the target wallet buys, I sell 100% of your position.',
        ].join('\n'),
        replyMarkup: {
          inline_keyboard: [
            [
              { text: 'Insider', callback_data: 'mode:insider' },
              { text: 'Bundler', callback_data: 'mode:bundler' },
              { text: 'Rev CopySell', callback_data: 'mode:reverse_copysell' },
            ],
            [
              { text: 'Set Target Wallet', callback_data: 'reverse:set_target' },
              { text: 'Refresh', callback_data: 'menu:refresh' },
            ],
          ],
        },
        editCurrent,
      };
    }

    const wallets = [...walletMonitors.keys()];
    const tradingWallet = config.tradingWalletAddress;
    
    // Add trading wallet to aliases so it can be navigated to via /w_T or similar
    // Actually, let's just add it to the list if it exists
    const allWallets = tradingWallet ? [tradingWallet, ...wallets] : wallets;
    walletAliasesByChat.set('__default__', allWallets);
    
    const walletLines = allWallets.map((wallet, index) => {
      const isTrading = wallet === tradingWallet;
      const isPaused = pausedWallets.has(wallet);
      const label = isTrading ? '💳 TRADING' : '✅ WATCHED';
      const status = isPaused ? ' (PAUSED)' : '';
      return `${label} /w_${index} <code>${html(wallet)}</code>${status}`;
    });

    if (walletLines.length === 0) {
      walletLines.push('No wallets are currently monitored.');
    }

    const runningWallets = allWallets.length - pausedWallets.size;

    return {
      text: [
        '<b>Early Bundler Bot</b>',
        '',
        `Mode: <b>Bundler</b>`,
        '',
        `Wallets total: <b>${allWallets.length}</b>`,
        `Running: <b>${runningWallets}</b>`,
        `Paused: <b>${pausedWallets.size}</b>`,
        '',
        '<b>Status</b>',
        'Filter: early bundler & reverse-buy',
        `Interval: ${config.monitorInterval}ms`,
        `Min buy: ${config.minBuySol} SOL`,
        '',
        '<b>Wallets List</b>',
        ...walletLines,
        '',
        'Send any Solana wallet address to preview it, then add or remove it with one tap.',
      ].join('\n'),
      replyMarkup: {
        inline_keyboard: [
          [
            { text: 'Insider', callback_data: 'mode:insider' },
            { text: 'Bundler', callback_data: 'mode:bundler' },
            { text: 'Rev CopySell', callback_data: 'mode:reverse_copysell' },
          ],
          [
            { text: 'Add wallet', callback_data: 'menu:addwallet' },
            { text: 'Remove wallet', callback_data: 'menu:removewallet' },
          ],
          [
            { text: 'Wallets', callback_data: 'menu:wallets' },
            { text: 'Status', callback_data: 'menu:status' },
          ],
          [
            { text: 'Refresh', callback_data: 'menu:refresh' },
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
      const label = isTrading ? '💳' : '✅';
      const status = isPaused ? ' (PAUSED)' : '';
      return `${label} /w_${i} <code>${html(w)}</code>${status}`;
    });

    return {
      text: lines.length
        ? `Monitored wallets:\n${lines.join('\n')}`
        : 'No wallets are being monitored.',
      replyMarkup: {
        inline_keyboard: lines.length
          ? [[{ text: 'Back', callback_data: 'menu:refresh' }]]
          : [[{ text: 'Add wallet', callback_data: 'menu:addwallet' }]],
      },
      editCurrent,
    };
  }

  function statusReply(editCurrent = false): TelegramReply {
    let text = '';
    if (botMode === 'insider') {
      const botsInfo = insiderBots.map((bot, i) => {
        const followed = bot.getFollowedWallet();
        const status = bot.isRunning() ? 'Running' : followed ? 'Paused' : 'Idle';
        return [
          `<b>Insider Bot ${i + 1}</b>`,
          `Status: ${status}`,
          `Follow: ${followed ?? 'not set'}`,
          `Buy: ${bot.getBuySol()} SOL`,
        ].join('\n');
      }).join('\n\n');

      text = [
        '<b>Bot Status</b>',
        'Mode: Insider',
        '',
        botsInfo,
      ].join('\n');
    } else if (botMode === 'reverse_copysell') {
      const targetWallet = config.reverseCopySellTargetWallet;
      const activePosition = reverseCopySellOrchestrator.getActivePosition();
      text = [
        '<b>Bot Status</b>',
        'Mode: Reverse CopySell',
        `Target: ${targetWallet ?? 'not set'}`,
        `Monitoring: ${activePosition ? activePosition.mint : 'none'}`,
      ].join('\n');
    } else {
      text = [
        '<b>Bot Status</b>',
        'Mode: Bundler',
        `Wallets: ${walletMonitors.size}`,
        'Filter: early bundler & reverse-buy',
        `Interval: ${config.monitorInterval}ms`,
      ].join('\n');
    }

    return {
      text,
      replyMarkup: {
        inline_keyboard: [[{ text: 'Back', callback_data: 'menu:refresh' }]],
      },
      editCurrent,
    };
  }

  function sellAlertMarkup(sellId: string): InlineKeyboardMarkup {
    return {
      inline_keyboard: [
        [
          {
            text: `Sell ${config.sellPercent}% for SOL`,
            callback_data: `sell:confirm:${sellId}`,
          },
        ],
        [
          { text: 'Ignore', callback_data: `sell:ignore:${sellId}` },
        ],
      ],
    };
  }

  function lamportsToSol(raw: string | null): number | null {
    if (!raw || !/^\d+$/.test(raw)) return null;
    return Number(BigInt(raw)) / 1_000_000_000;
  }

  function sellReceipt(event: FilterFailEvent, result: SellResult): string {
    const receivedSol = lamportsToSol(result.filledOutputAmount);
    const costBasis = event.buySol !== null
      ? event.buySol * (result.soldPercent / 100)
      : null;
    const pnl = receivedSol !== null && costBasis !== null
      ? receivedSol - costBasis
      : null;
    const pnlPct = pnl !== null && costBasis !== null && costBasis > 0
      ? (pnl / costBasis) * 100
      : null;
    const fmtSol = (value: number | null): string =>
      value === null ? 'N/A' : `${parseFloat(value.toFixed(6))} SOL`;
    const pnlLine = pnl === null
      ? 'P/L: N/A (original buy SOL unknown)'
      : `P/L: <b>${fmtSol(pnl)}</b> (${parseFloat((pnlPct ?? 0).toFixed(2))}%)`;

    return [
      result.status === 'confirmed' ? '<b>Sell Confirmed</b>' : '<b>Sell Submitted</b>',
      `Wallet: <code>${html(event.walletAddress)}</code>`,
      `Token: <code>${html(event.mint)}</code>`,
      `Status: <b>${html(result.status)}</b>`,
      `Sold: <b>${result.soldPercent}%</b>`,
      `Matched watched wallets: <b>${event.matchingWallets.length}</b>`,
      `Token amount sold: <code>${html(result.filledInputAmount ?? 'pending')}</code>`,
      `Received: <b>${fmtSol(receivedSol)}</b>`,
      `Cost basis sold: ${fmtSol(costBasis)}`,
      pnlLine,
      result.hash ? `Tx: https://solscan.io/tx/${html(result.hash)}` : '',
      result.orderId ? `Order ID: <code>${html(result.orderId)}</code>` : '',
      '',
      '<b>Why it sold</b>',
      ...event.reasons.map((reason) => `- ${html(reason)}`),
    ].filter(Boolean).join('\n');
  }

  function sellFailedReply(event: FilterFailEvent, err: unknown): string {
    return [
      '<b>Sell Failed</b>',
      `Wallet: <code>${html(event.walletAddress)}</code>`,
      `Token: <code>${html(event.mint)}</code>`,
      `Matched watched wallets: <b>${event.matchingWallets.length}</b>`,
      `Error: ${html(err instanceof Error ? err.message : String(err))}`,
      '',
      '<b>Why sell was requested</b>',
      ...event.reasons.map((reason) => `- ${html(reason)}`),
    ].join('\n');
  }

  async function getTokenRawBalance(owner: PublicKey, mint: PublicKey): Promise<bigint> {
    const accounts = await gmgnClients[0].getParsedTokenAccountsForMint(owner, mint);
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
    telegramBot: TelegramBot | null
  ): Promise<void> {
    const pending = pendingSells.get(sellId);
    if (!pending) return;

    try {
      const owner = new PublicKey(pending.event.walletAddress);
      const mintPk = new PublicKey(pending.event.mint);

      const startingBalance = await getTokenRawBalance(owner, mintPk).catch(() => null);
      if (startingBalance !== null && startingBalance <= 0n) {
        log.info(`[SELL ABORT] No token accounts found for ${pending.event.mint}, assuming already sold.`);
        return;
      }

      const currentPending = pendingSells.get(sellId);
      if (!currentPending) return;

      let lastResult: SellResult | null = null;
      let lastError: unknown = null;
      let sold = false;

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
            }
          );
          lastError = null;
        } catch (err) {
          lastError = err;
          log.warn(`Sell attempt ${attempt}/5 failed`, {
            mint: currentPending.event.mint,
            error: err instanceof Error ? err.message : String(err),
          });
        }

        await sleep(1_500);
        const remainingBalance = await getTokenRawBalance(owner, mintPk).catch(() => null);
        if (remainingBalance !== null && remainingBalance <= 0n) {
          sold = true;
          break;
        }

        if (lastResult?.status === 'confirmed' && remainingBalance === null) {
          sold = true;
          break;
        }
      }

      if (!sold) {
        throw lastError ?? new Error(`Sell did not clear token balance after 5 attempts`);
      }

      const receiptResult = lastResult
        ? { ...lastResult, status: lastResult.status === 'failed' ? 'confirmed' : lastResult.status }
        : {
            orderId: null,
            hash: null,
            status: 'confirmed',
            inputToken: currentPending.event.mint,
            outputToken: 'So11111111111111111111111111111111111111112',
            soldPercent: config.sellPercent,
            filledInputAmount: null,
            filledOutputAmount: null,
            raw: {},
          };
      const receipt = sellReceipt(currentPending.event, receiptResult);
      if (chatId && telegramBot) {
        await telegramBot.sendChat(chatId, receipt);
      } else {
        log.info('Sell completed without Telegram receipt chat', {
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
        log.error('Sell failed without Telegram receipt chat', err);
      }
    } finally {
      pendingSells.delete(sellId);
    }
  }

  // ── 6. Start active mode ──────────────────────────────────────────────────
  await stopBundlerModeServices('Service started in Insider mode');
  startMarketCapChecker();

  log.info(`Service fully started — mode=${botMode}, bundler wallets active=${walletMonitors.size}`);

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

    await limiter.drain().catch((e) => log.warn('Limiter drain error', e));
    db.close();

    log.info('Shutdown complete');
    process.exit(0);
  }

  process.on('SIGINT',  () => void shutdown('SIGINT'));
  process.on('SIGTERM', () => void shutdown('SIGTERM'));

  // Keep the process alive
  await new Promise<never>(() => {});
}

main().catch((err) => {
  console.error('[FATAL]', err);
  process.exit(1);
});
