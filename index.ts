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
  SellQuote,
  SellResult,
  TokenExitEvent,
  WalletFilterSettings,
} from './types';
import { HeliusClient, EarlyBundlerInfo } from './helius-client';
import { BundlerMonitor, BundlerWallet, BundlerTransaction } from './bundler-monitor';
import { EarlyBundlerOrchestrator, BundlerSellReason } from './early-bundler-orchestrator';
import { PublicKey } from '@solana/web3.js';
import { randomBytes } from 'crypto';

const log = createLogger('MAIN');
const sleep = (ms: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, ms));

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
    minBuySol:       config.minBuySol,
    gmgnFetchMode:   config.gmgnFetchMode,
    monitorInterval: config.monitorInterval,
    rateLimitMinTime: config.rateLimitMinTime,
    dbPath:          config.dbPath,
  });

  // ── 2. Database ────────────────────────────────────────────────────────────
  const db = await MonitorDatabase.create(config.dbPath);

  // ── 3. Rate limiter + GMGN client ─────────────────────────────────────────
  const limiter = new RateLimiter(
    config.rateLimitMinTime,
    config.rateLimitMaxConcurrent
  );
  const gmgnClient = new GmgnClient(config, limiter);

  let telegramBot: TelegramBot | null = null;

  // ── 4. Early Bundler Orchestrator ─────────────────────────────────────────
  let earlyBundlerOrchestrator: EarlyBundlerOrchestrator;

  const healthServer = startHealthServer(config.port);
  const walletMonitors = new Map<string, WalletMonitor>();
  let tradingWalletMonitor: WalletMonitor | null = null;
  const pendingTradingBuys = new Map<string, NewTokenEvent>();
  type PendingTelegramAction =
    | { type: 'addwallet' | 'removewallet' }
    | { type: 'minSol'; walletAddress: string };
  const pendingTelegramActions = new Map<string, PendingTelegramAction>();
  const pendingSells = new Map<string, { event: FilterFailEvent; createdAt: number; executing: boolean }>();
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
          return { text: 'Send the Solana wallet address to add.', trackPrompt: true };
        }
        if (data === 'menu:removewallet') {
          pendingTelegramActions.set(chatId, { type: 'removewallet' });
          return { text: 'Send the Solana wallet address to remove.', trackPrompt: true };
        }
        if (data === 'menu:refresh') return homeReply(true);
        if (data === 'menu:wallets') return walletsReply(chatId);
        if (data === 'menu:status') return statusReply();

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
          };
        }
        if (callbackKind === 'reverse' && callbackAction && callbackAddress) {
          const normalized = new PublicKey(callbackAddress).toBase58();
          if (callbackAction === 'add') {
            db.addReverseBuyWallet(normalized);
            return settingsReply(normalized, true);
          }
          if (callbackAction === 'remove') {
            db.removeReverseBuyWallet(normalized);
            return settingsReply(normalized, true);
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

  if (telegramBot) {
    telegramBot.start();
  }

  earlyBundlerOrchestrator = new EarlyBundlerOrchestrator(config, db, telegramBot);

  earlyBundlerOrchestrator.on('sellTrigger', (trigger) => {
    const { position, type, walletAddress, soldPercentage, reason } = trigger;
    
    // Check if sell already pending
    if (hasPendingSellForMint(position.tradingWallet, position.mint)) {
      log.info(`[EARLY BUNDLER SELL SKIP] Sell already pending for ${position.mint}`);
      return;
    }

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
    });

    walletMonitor.on('tokenExited', (event: TokenExitEvent) => {
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
    });
  }

  function wireWatchedWalletMonitor(walletMonitor: WalletMonitor): void {
    walletMonitor.on('newToken', (event: NewTokenEvent) => {
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

  function walletsReply(chatId: string): TelegramReply {
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
    };
  }

  function statusReply(): string {
    return [
      '<b>Bot Status</b>',
      `Wallets: ${walletMonitors.size}`,
      'Filter: early bundler & reverse-buy',
      `Interval: ${config.monitorInterval}ms`,
    ].join('\n');
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

  function profitGateStatusReply(
    event: FilterFailEvent,
    quote: SellQuote,
    costBasis: number,
    pnl: number,
    pnlPct: number
  ): string {
    const fmtSol = (value: number): string => `${parseFloat(value.toFixed(6))} SOL`;
    return [
      '<b>Sell Waiting For Profit</b>',
      `Wallet: <code>${html(event.walletAddress)}</code>`,
      `Token: <code>${html(event.mint)}</code>`,
      `Target P/L: <b>+${config.sellMinProfitPct}%</b>`,
      `Current P/L: <b>${parseFloat(pnlPct.toFixed(3))}%</b> (${fmtSol(pnl)})`,
      `Estimated receive: <b>${fmtSol(quote.estimatedOutputSol)}</b>`,
      `Cost basis sold: ${fmtSol(costBasis)}`,
      `Next check: ${Math.round(config.sellProfitCheckIntervalMs / 1000)}s`,
      '',
      '<b>Why sell is pending</b>',
      ...event.reasons.map((reason) => `- ${html(reason)}`),
    ].join('\n');
  }

  function profitGateReachedReply(
    event: FilterFailEvent,
    quote: SellQuote,
    costBasis: number,
    pnl: number,
    pnlPct: number
  ): string {
    const fmtSol = (value: number): string => `${parseFloat(value.toFixed(6))} SOL`;
    return [
      '<b>Profit Target Reached</b>',
      `Token: <code>${html(event.mint)}</code>`,
      `Current P/L: <b>${parseFloat(pnlPct.toFixed(3))}%</b> (${fmtSol(pnl)})`,
      `Estimated receive: <b>${fmtSol(quote.estimatedOutputSol)}</b>`,
      `Cost basis sold: ${fmtSol(costBasis)}`,
      '',
      `Submitting Jupiter sell for <b>${config.sellPercent}%</b>.`,
    ].join('\n');
  }

  async function waitForProfitBeforeSell(
    chatId: string | null,
    sellId: string,
    telegramBot: TelegramBot | null
  ): Promise<boolean> {
    const pending = pendingSells.get(sellId);
    if (!pending) return false;
    if (config.sellMinProfitPct <= 0) return true;

    const buySol = pending.event.buySol;
    if (buySol === null || buySol <= 0) {
      log.warn('Profit gate skipped because buy SOL is unknown', {
        mint: pending.event.mint,
        wallet: pending.event.walletAddress,
      });
      return true;
    }

    const costBasis = buySol * (config.sellPercent / 100);
    let notifiedWaiting = false;
    let lastLogAt = 0;
    let quoteRetryCount = 0;
    const MAX_QUOTE_RETRIES = 5;

    while (pendingSells.has(sellId)) {
      let quote: SellQuote;
      try {
        quote = await gmgnClient.quoteTokenSellForSol(
          pending.event.walletAddress,
          pending.event.mint,
          config.sellPercent
        );
        // Reset retry count on success
        quoteRetryCount = 0;
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        const isNoBalance = message.includes('No token balance found');
        
        const now = Date.now();
        if (now - lastLogAt >= 30_000 || isNoBalance) {
          lastLogAt = now;
          log.warn(isNoBalance ? 'Profit gate stopped: no balance found' : 'Profit gate quote failed; will retry', {
            mint: pending.event.mint,
            error: message,
            attempt: isNoBalance ? undefined : `${quoteRetryCount + 1}/${MAX_QUOTE_RETRIES}`,
            nextCheckMs: config.sellProfitCheckIntervalMs,
          });
        }

        if (isNoBalance) {
          return false;
        }

        quoteRetryCount++;
        if (quoteRetryCount >= MAX_QUOTE_RETRIES) {
          log.error(`Profit gate aborted after ${MAX_QUOTE_RETRIES} quote failures`, {
            mint: pending.event.mint,
            error: message,
          });
          return false;
        }

        if (!notifiedWaiting && chatId && telegramBot) {
          notifiedWaiting = true;
          await telegramBot.sendChat(chatId, [
            '<b>Sell Waiting For Profit Quote</b>',
            `Wallet: <code>${html(pending.event.walletAddress)}</code>`,
            `Token: <code>${html(pending.event.mint)}</code>`,
            `Target P/L: <b>+${config.sellMinProfitPct}%</b>`,
            'Jupiter quote is not ready yet. I will keep retrying instead of failing the sell.',
            `Next check: ${Math.round(config.sellProfitCheckIntervalMs / 1000)}s`,
            '',
            '<b>Why sell is pending</b>',
            ...pending.event.reasons.map((reason) => `- ${html(reason)}`),
          ].join('\n'));
        }
        await sleep(config.sellProfitCheckIntervalMs);
        continue;
      }

      const pnl = quote.estimatedOutputSol - costBasis;
      const pnlPct = costBasis > 0 ? (pnl / costBasis) * 100 : 0;

      if (pnlPct >= config.sellMinProfitPct) {
        log.info('Profit gate reached; submitting sell', {
          mint: pending.event.mint,
          pnlPct,
          estimatedOutputSol: quote.estimatedOutputSol,
          costBasis,
        });
        if (chatId && telegramBot && notifiedWaiting) {
          await telegramBot.sendChat(chatId, profitGateReachedReply(
            pending.event,
            quote,
            costBasis,
            pnl,
            pnlPct
          ));
        }
        return true;
      }

      const now = Date.now();
      if (!notifiedWaiting && chatId && telegramBot) {
        notifiedWaiting = true;
        await telegramBot.sendChat(chatId, profitGateStatusReply(
          pending.event,
          quote,
          costBasis,
          pnl,
          pnlPct
        ));
      }
      if (now - lastLogAt >= 30_000) {
        lastLogAt = now;
        log.info('Waiting for profit before sell', {
          mint: pending.event.mint,
          pnlPct,
          targetPct: config.sellMinProfitPct,
          estimatedOutputSol: quote.estimatedOutputSol,
          costBasis,
        });
      }

      await sleep(config.sellProfitCheckIntervalMs);
    }

    return false;
  }

  async function executeSellAndNotify(
    chatId: string | null,
    sellId: string,
    telegramBot: TelegramBot | null
  ): Promise<void> {
    const pending = pendingSells.get(sellId);
    if (!pending) return;

    try {
      // Preliminary balance check to avoid entering profit gate if already sold
      const owner = new PublicKey(pending.event.walletAddress);
      const mintPk = new PublicKey(pending.event.mint);
      
      let accounts;
      try {
        accounts = await gmgnClient.getParsedTokenAccountsForMint(owner, mintPk);
      } catch (err) {
        // If we can't check balance, we'll let the profit gate try once
        accounts = null;
      }

      if (accounts && accounts.length === 0) {
        log.info(`[SELL ABORT] No token accounts found for ${pending.event.mint}, assuming already sold.`);
        return;
      }

      const readyToSell = await waitForProfitBeforeSell(chatId, sellId, telegramBot);
      if (!readyToSell) return;
      const currentPending = pendingSells.get(sellId);
      if (!currentPending) return;

      const result = await gmgnClient.sellTokenForSol(
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
      const receipt = sellReceipt(currentPending.event, result);
      if (chatId && telegramBot) {
        await telegramBot.sendChat(chatId, receipt);
      } else {
        log.info('Sell completed without Telegram receipt chat', {
          mint: currentPending.event.mint,
          status: result.status,
          hash: result.hash,
          orderId: result.orderId,
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

  // ── 6. Start everything ───────────────────────────────────────────────────
  if (config.tradingWalletAddress) {
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
  } else {
    log.warn('No TRADING_WALLET_ADDRESS configured; sell flow cannot detect your buys.');
  }
  const wallets = db.getActiveWallets();
  for (const wallet of wallets) {
    if (wallet === config.tradingWalletAddress) continue;
    await startWallet(wallet);
  }

  log.info(`Service fully started — monitoring ${walletMonitors.size} wallet(s) for new tokens`);

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
