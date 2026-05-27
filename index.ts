// ─────────────────────────────────────────────────────────────────────────────
//  index.ts  —  Service entry point
//
//  Boot sequence:
//    1. Load + validate environment config
//    2. Open SQLite database
//    3. Initialise RateLimiter + GmgnClient + Scheduler
//    4. Start WalletMonitor → snapshot existing tokens
//    5. Wire newToken events → Scheduler + DB
//    6. Start Scheduler
//    7. Register SIGINT/SIGTERM for graceful shutdown
// ─────────────────────────────────────────────────────────────────────────────

import 'dotenv/config';
import { createLogger, setLogLevel } from './logger';
import { loadConfig } from './config';
import { MonitorDatabase } from './database';
import { RateLimiter } from './rate-limiter';
import { GmgnClient } from './gmgn-client';
import { Scheduler } from './scheduler';
import { WalletMonitor } from './wallet-monitor';
import { InlineKeyboardMarkup, TelegramBot, TelegramReply } from './telegram-bot';
import { startHealthServer } from './health-server';
import {
  FilterFailEvent,
  FilterPassEvent,
  MonitorSampleEvent,
  NewTokenEvent,
  SellQuote,
  SellResult,
  TokenSummary,
  WalletFilterSettings,
} from './types';
import { PublicKey } from '@solana/web3.js';
import { randomBytes } from 'crypto';

const log = createLogger('MAIN');
const TRADING_LINK_WINDOW_MS = 5 * 60 * 1000;
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

  // ── 4. Scheduler ──────────────────────────────────────────────────────────
  const scheduler = new Scheduler(config, gmgnClient, db, limiter);

  const healthServer = startHealthServer(config.port);
  const walletMonitors = new Map<string, WalletMonitor>();
  let tradingWalletMonitor: WalletMonitor | null = null;
  const pendingTradingBuys = new Map<string, NewTokenEvent>();
  const pendingTradingBuyTimers = new Map<string, NodeJS.Timeout>();
  const recentWatchedBuys = new Map<string, Set<string>>();
  const activeTradingMints = new Set<string>();
  type PendingTelegramAction =
    | { type: 'addwallet' | 'removewallet' }
    | { type: 'setting'; walletAddress: string; field: SettingField };
  const pendingTelegramActions = new Map<string, PendingTelegramAction>();
  const pendingSells = new Map<string, { event: FilterFailEvent; createdAt: number; executing: boolean }>();
  const pausedWallets = new Set<string>();
  const walletAliasesByChat = new Map<string, string[]>();

  type SettingField = keyof Pick<
    WalletFilterSettings,
    | 'applyAtSample'
    | 'minBundlersPercent'
    | 'maxBundlersPercent'
    | 'minBundlersCount'
    | 'maxBundlersCount'
    | 'minBundlersCountChange'
  >;
  const settingFieldByCode: Record<string, SettingField> = {
    apply: 'applyAtSample',
    minPct: 'minBundlersPercent',
    maxPct: 'maxBundlersPercent',
    minCnt: 'minBundlersCount',
    maxCnt: 'maxBundlersCount',
    minInc: 'minBundlersCountChange',
  };
  const settingLabel: Record<SettingField, string> = {
    applyAtSample: 'Apply at sample #',
    minBundlersPercent: 'Min bundlers %',
    maxBundlersPercent: 'Max bundlers %',
    minBundlersCount: 'Min bundlers count',
    maxBundlersCount: 'Max bundlers count',
    minBundlersCountChange: 'Min count change to sell',
  };
  const settingHint = (field: SettingField): string => {
    if (field === 'applyAtSample') return 'Use a positive whole number.';
    return 'Use a number, or send <code>off</code> to disable.';
  };

  function walletHasSellFilters(walletAddress: string): boolean {
    return db.getWalletSettings(walletAddress) !== null;
  }

  function linkedWalletModeLines(wallet: string): string[] {
    return [
      `- <code>${html(wallet)}</code>`,
      `  Sell filter: top wallets must start with 0 or 1, then 1 -> 1 or 3 -> 3`,
      `  Monitoring continues through sample #${20}`,
    ];
  }

  function wireTradingWalletMonitor(walletMonitor: WalletMonitor): void {
    walletMonitor.on('newToken', (event: NewTokenEvent) => {
      const existingTimer = pendingTradingBuyTimers.get(event.mint);
      if (existingTimer) clearTimeout(existingTimer);
      pendingTradingBuys.set(event.mint, event);
      const expireTimer = setTimeout(() => {
        pendingTradingBuyTimers.delete(event.mint);
        if (activeTradingMints.has(event.mint)) return;
        pendingTradingBuys.delete(event.mint);
        log.info(`[TRADING BUY EXPIRED] No watched-wallet match within 5 minutes for ${event.mint}`);
        telegramBot?.sendDefault([
          '<b>Trading Wallet Buy Expired</b>',
          `Token: <code>${html(event.mint)}</code>`,
          'No watched wallet matched this token within 5 minutes.',
        ].join('\n')).catch((err) => log.warn('Telegram trading buy expiry alert failed', err));
      }, TRADING_LINK_WINDOW_MS);
      pendingTradingBuyTimers.set(event.mint, expireTimer);

      const matchedWallets = [...(recentWatchedBuys.get(event.mint) ?? new Set<string>())];

      log.info(`[TRADING BUY] Wallet: ${event.walletAddress} Mint: ${event.mint}`);
      telegramBot?.sendDefault([
        '<b>Trading Wallet Buy Detected</b>',
        `Wallet: <code>${html(event.walletAddress)}</code>`,
        `Token: <code>${html(event.mint)}</code>`,
        `Buy SOL: <b>${event.buySol ?? 'unknown'}</b>`,
        matchedWallets.length
          ? `Already matched by watched wallet(s): <b>${matchedWallets.length}</b>`
          : 'Waiting for a watched wallet to buy this token before monitoring starts.',
      ].join('\n')).catch((err) => log.warn('Telegram trading buy alert failed', err));

      if (matchedWallets.length > 0) {
        activateTradingToken(event.mint, matchedWallets);
      }
    });
  }

  function wireWatchedWalletMonitor(walletMonitor: WalletMonitor): void {
    walletMonitor.on('newToken', (event: NewTokenEvent) => {
      log.info(`[WATCHED BUY] Wallet: ${event.walletAddress} Mint: ${event.mint}`);
      if (!walletHasSellFilters(event.walletAddress)) {
        log.info(`[WATCHED BUY MONITOR-ONLY] Wallet ${event.walletAddress} has no bundler count-change setting`);
      }
      const wallets = recentWatchedBuys.get(event.mint) ?? new Set<string>();
      wallets.add(event.walletAddress);
      recentWatchedBuys.set(event.mint, wallets);
      startWatchedWalletSummary(event);

      if (pendingTradingBuys.has(event.mint)) {
        activateTradingToken(event.mint, [...wallets]);
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

    scheduler.addToken({ ...event, matchingWallets: [] });
    telegramBot?.sendDefault([
      '<b>Watched Wallet Monitoring Started</b>',
      `Wallet: <code>${html(event.walletAddress)}</code>`,
      `Token: <code>${html(event.mint)}</code>`,
      `Buy SOL: <b>${event.buySol ?? 'unknown'}</b>`,
      'Mode: monitor-only summary at sample #20. Sell decisions are disabled for watched-wallet-only monitoring.',
    ].join('\n')).catch((err) => log.warn('Telegram watched-wallet monitor alert failed', err));
  }

  function activateTradingToken(mint: string, matchingWallets: string[]): void {
    if (activeTradingMints.has(mint)) return;
    const tradingBuy = pendingTradingBuys.get(mint);
    if (!tradingBuy) return;
    matchingWallets = [...new Set(matchingWallets)];
    if (matchingWallets.length === 0) return;
    activeTradingMints.add(mint);
    const pendingTimer = pendingTradingBuyTimers.get(mint);
    if (pendingTimer) clearTimeout(pendingTimer);
    pendingTradingBuyTimers.delete(mint);
    pendingTradingBuys.delete(mint);

    if (!db.tokenExists(tradingBuy.walletAddress, mint)) {
      db.insertToken({
        walletAddress: tradingBuy.walletAddress,
        mint,
        firstSeen: new Date().toISOString(),
        monitoringStatus: 'active',
        detectedAt: tradingBuy.detectedAt,
        buySol: tradingBuy.buySol,
      });
    }

    scheduler.addToken({ ...tradingBuy, matchingWallets });
    telegramBot?.sendDefault([
      '<b>Watched Wallet Match Found</b>',
      `Trading wallet token: <code>${html(mint)}</code>`,
      `Matched watched wallets: <b>${matchingWallets.length}</b>`,
      ...matchingWallets.slice(0, 5).flatMap((wallet) => linkedWalletModeLines(wallet)),
      matchingWallets.length > 5 ? `...and ${matchingWallets.length - 5} more` : '',
      '',
      'Monitoring is now running on your trading-wallet position. Sample cards and the sample #20 summary will continue.',
    ].filter(Boolean).join('\n')).catch((err) => log.warn('Telegram match alert failed', err));
  }

  async function startWallet(address: string): Promise<string> {
    const normalized = new PublicKey(address).toBase58();
    if (walletMonitors.has(normalized)) {
      return `Already monitoring <code>${normalized}</code>`;
    }

    db.addWallet(normalized);
    const monitor = new WalletMonitor(config, normalized, { enforceMinBuySol: true });
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
    const monitor = walletMonitors.get(normalized);
    if (!monitor) return `Wallet is not monitored: <code>${normalized}</code>`;
    if (pausedWallets.has(normalized)) return `Wallet is already paused: <code>${normalized}</code>`;
    monitor.stop();
    pausedWallets.add(normalized);
    return `Paused monitoring <code>${normalized}</code>`;
  }

  async function resumeWallet(address: string): Promise<string> {
    const normalized = new PublicKey(address).toBase58();
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

  function updateSetting(
    walletAddress: string,
    field: SettingField,
    rawValue: string
  ): string {
    const normalized = new PublicKey(walletAddress).toBase58();
    const trimmed = rawValue.trim().toLowerCase();
    const settings = db.getWalletSettings(normalized);
    const targetRecord = settings as unknown as Record<string, number | boolean | null>;

    if (trimmed === 'off' || trimmed === 'any' || trimmed === 'none') {
      if (field === 'applyAtSample') {
        return 'Apply sample cannot be disabled. Send a positive whole number.';
      }
      targetRecord[field] = null;
    } else {
      const value = Number(trimmed);
      if (!Number.isFinite(value) || value < 0) {
        return `Invalid value. Send a non-negative number, or "off".`;
      }
      if (field === 'applyAtSample') {
        const n = Math.trunc(value);
        if (n <= 0) return 'Apply sample must be at least 1.';
        targetRecord[field] = n;
      } else {
        targetRecord[field] = value;
      }
    }

    db.updateWalletSettings(normalized, settings);
    return `Updated ${settingLabel[field]} for <code>${html(normalized)}</code>.`;
  }

  function walletSummaryReply(address: string, editCurrent = false): TelegramReply {
    const normalized = new PublicKey(address).toBase58();
    const isMonitoring = walletMonitors.has(normalized);
    const isPaused = pausedWallets.has(normalized);
    const status = !isMonitoring ? 'Not monitoring' : isPaused ? 'Paused' : 'Monitoring';
    const actionButton = !isMonitoring
      ? { text: 'Add wallet', callback_data: `wallet:add:${normalized}` }
      : { text: 'Remove wallet', callback_data: `wallet:remove:${normalized}` };
    const pauseButton = isPaused
      ? { text: 'Continue monitoring', callback_data: `wallet:resume:${normalized}` }
      : { text: 'Pause monitoring', callback_data: `wallet:pause:${normalized}` };

    return {
      text: [
        '<b>Wallet</b>',
        `<code>${html(normalized)}</code>`,
        '',
        `Status: <b>${status}</b>`,
        `Tokens seen: ${db.tokenCountForWallet(normalized)}`,
        `Samples stored: ${db.sampleCountForWallet(normalized)}`,
      ].join('\n'),
      replyMarkup: {
        inline_keyboard: isMonitoring
          ? [
              [{ text: 'Settings', callback_data: `wallet:settings:${normalized}` }],
              [pauseButton, actionButton],
              [
                { text: 'Back', callback_data: 'menu:refresh' },
                { text: 'Refresh', callback_data: `wallet:refresh:${normalized}` },
              ],
            ]
          : [
              [actionButton],
              [
                { text: 'Back', callback_data: 'menu:refresh' },
                { text: 'Refresh', callback_data: `wallet:refresh:${normalized}` },
              ],
            ],
      },
      editCurrent,
    };
  }

  function settingsReply(address: string, editCurrent = false): TelegramReply {
    const normalized = new PublicKey(address).toBase58();

    return {
      text: [
        '<b>Filter Settings</b>',
        `<code>${html(normalized)}</code>`,
        '',
        '<b>Current Flow</b>',
        'Linked trading-wallet tokens are monitored from sample <b>#1</b>.',
        'No reserve-based sell rule is active.',
        'Top wallets sample #1 must be <b>0</b> or <b>1</b>, then samples #2-#3 must be <b>1 -> 1</b> or <b>3 -> 3</b>.',
        'If sample #2 is not 1 or 3, it sells at sample #2. If sample #3 does not match sample #2, it sells at sample #3.',
        '',
        'Samples, logs, Telegram sample cards, and the summary continue through sample <b>#20</b> either way.',
      ].join('\n'),
      replyMarkup: {
        inline_keyboard: [
          [
            { text: 'Back', callback_data: `wallet:refresh:${normalized}` },
            { text: 'Refresh', callback_data: `settings:refresh:${normalized}` },
          ],
        ],
      },
      editCurrent,
    };
  }

  function homeReply(editCurrent = false): TelegramReply {
    const wallets = [...walletMonitors.keys()];
    walletAliasesByChat.set('__default__', wallets);
    const walletLines = wallets.length
      ? wallets.map((wallet, index) => `✅ /w_${index} <code>${html(wallet)}</code>`)
      : ['No wallets are currently monitored.'];
    const runningWallets = wallets.length - pausedWallets.size;

    return {
      text: [
        '<b>GMGN Bundler Monitor</b>',
        '',
        `Wallets monitored: <b>${walletMonitors.size}</b>`,
        `Running wallets: <b>${runningWallets}</b>`,
        `Paused wallets: <b>${pausedWallets.size}</b>`,
        `Active linked tokens: <b>${scheduler.activeCount}</b>`,
        'Filter timing: apply-sample driven',
        `Poll interval: ${config.monitorInterval}ms`,
        `Min buy: ${config.minBuySol} SOL`,
        '',
        '<b>Monitored Wallets</b>',
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
    walletAliasesByChat.set(chatId, wallets);
    return {
      text: wallets.length
        ? `Monitoring ${wallets.length} wallet(s):\n${wallets.map((w, i) => `✅ /w_${i} <code>${html(w)}</code>`).join('\n')}`
        : 'No wallets are being monitored.',
      replyMarkup: {
        inline_keyboard: wallets.length
          ? [[{ text: 'Back', callback_data: 'menu:refresh' }]]
          : [[{ text: 'Add wallet', callback_data: 'menu:addwallet' }]],
      },
    };
  }

  function statusReply(): string {
    return [
      `Wallets: ${walletMonitors.size}`,
      `Active tokens: ${scheduler.activeCount}`,
      'Filter timing: apply-sample driven',
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

    while (pendingSells.has(sellId)) {
      const quote = await gmgnClient.quoteTokenSellForSol(
        pending.event.walletAddress,
        pending.event.mint,
        config.sellPercent
      );
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

  let telegramBot: TelegramBot | null = null;

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

        if (callbackKind === 'set' && callbackAction && callbackAddress) {
          const field = settingFieldByCode[callbackAction];
          if (!field) return 'Invalid setting.';
          const normalized = new PublicKey(callbackAddress).toBase58();
          pendingTelegramActions.set(chatId, {
            type: 'setting',
            walletAddress: normalized,
            field,
          });
          return {
            text: [
              `Send a value for <b>${settingLabel[field]}</b>.`,
              settingHint(field),
            ].join('\n'),
            trackPrompt: true,
          };
        }
        if (callbackKind === 'toggle' && callbackAction && callbackAddress) {
          const normalized = new PublicKey(callbackAddress).toBase58();
          const settings = db.getWalletSettings(normalized);
          if (callbackAction === 'first0') {
            settings.sellIfFirstThreePctZero = !settings.sellIfFirstThreePctZero;
          } else if (callbackAction === 'teen20') {
            settings.sellIfNoTeenOrTwentyPct = !settings.sellIfNoTeenOrTwentyPct;
          } else {
            return 'Invalid setting.';
          }
          db.updateWalletSettings(normalized, settings);
          return settingsReply(normalized, true);
        }
        if (callbackKind === 'settings' && callbackAction === 'refresh' && callbackAddress) {
          return settingsReply(callbackAddress, true);
        }

        const [kind, action, address] = [callbackKind, callbackAction, callbackAddress];
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
          return walletSummaryReply(address, true);
        }
        if (action === 'resume') {
          await resumeWallet(address);
          return walletSummaryReply(address, true);
        }
        if (action === 'settings') return settingsReply(address, true);
        if (action === 'refresh') return walletSummaryReply(address, true);
        return 'Invalid button action.';
      }

      const pendingAction = pendingTelegramActions.get(chatId);
      if (pendingAction && !text.startsWith('/')) {
        pendingTelegramActions.delete(chatId);
        if (pendingAction.type === 'addwallet') {
          await startWallet(text);
          return walletSummaryReply(text);
        }
        if (pendingAction.type === 'removewallet') {
          stopWallet(text);
          return homeReply();
        }
        if (pendingAction.type === 'setting') {
          const message = updateSetting(
            pendingAction.walletAddress,
            pendingAction.field,
            text
          );
          if (!message.startsWith('Updated ')) return message;
          return settingsReply(pendingAction.walletAddress);
        }
      }

      if (command === '/cancel') {
        pendingTelegramActions.delete(chatId);
        return 'Cancelled.';
      }
      if (command === '/start' || command === '/help') {
        return homeReply();
      }
      if (command === '/addwallet') {
        pendingTelegramActions.set(chatId, { type: 'addwallet' });
        return { text: 'Send the Solana wallet address to add.', trackPrompt: true };
      }
      if (command === '/removewallet') {
        pendingTelegramActions.set(chatId, { type: 'removewallet' });
        return { text: 'Send the Solana wallet address to remove.', trackPrompt: true };
      }
      if (command === '/wallets') {
        return walletsReply(chatId);
      }
      const walletAlias = command.match(/^\/w_(\d+)$/);
      if (walletAlias) {
        const wallets = walletAliasesByChat.get(chatId) ?? walletAliasesByChat.get('__default__') ?? [...walletMonitors.keys()];
        const wallet = wallets[Number(walletAlias[1])];
        if (!wallet) return 'Wallet shortcut not found. Send /wallets to refresh the list.';
        return walletSummaryReply(wallet);
      }
      if (command === '/status') {
        return statusReply();
      }

      if (!text.startsWith('/')) {
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
    scheduler.on('sample', (event: MonitorSampleEvent) => {
      telegramBot.sendSampleCard(event).catch((err) =>
        log.warn('Telegram sample send failed', err)
      );
    });
    scheduler.on('summary', (summary: TokenSummary) => {
      telegramBot.sendSummaryCard(summary).catch((err) =>
        log.warn('Telegram summary send failed', err)
      );
    });
    telegramBot.start();
  }

  scheduler.on('filterFail', (event: FilterFailEvent) => {
    const sellId = randomBytes(5).toString('hex');
    pendingSells.set(sellId, {
      event: {
        ...event,
        buySol: event.buySol ?? db.getToken(event.walletAddress, event.mint)?.buySol ?? null,
      },
      createdAt: Date.now(),
      executing: true,
    });
    telegramBot?.sendFilterFailCard(event).catch((err) =>
      log.warn('Telegram filter alert send failed', err)
    );
    void executeSellAndNotify(config.telegramChatId, sellId, telegramBot);
  });

  // ── 6. Start everything ───────────────────────────────────────────────────
  if (config.tradingWalletAddress) {
    tradingWalletMonitor = new WalletMonitor(config, config.tradingWalletAddress, { enforceMinBuySol: false });
    wireTradingWalletMonitor(tradingWalletMonitor);
    await tradingWalletMonitor.start();
  } else {
    log.warn('No TRADING_WALLET_ADDRESS configured; sell flow cannot detect your buys.');
  }
  const wallets = db.getActiveWallets();
  for (const wallet of wallets) {
    if (wallet === config.tradingWalletAddress) continue;
    await startWallet(wallet);
  }
  scheduler.start();

  log.info(`Service fully started — monitoring ${walletMonitors.size} wallet(s) for new tokens`);
  log.info(`Safe token capacity at current rate limit: ${scheduler.safeTokenCapacity} tokens`);

  // ── 7. Graceful shutdown ──────────────────────────────────────────────────
  let shutting_down = false;

  async function shutdown(signal: string): Promise<void> {
    if (shutting_down) return;
    shutting_down = true;

    log.info(`Received ${signal} — shutting down gracefully`);

    for (const monitor of walletMonitors.values()) {
      monitor.stop();
    }
    for (const timer of pendingTradingBuyTimers.values()) {
      clearTimeout(timer);
    }
    pendingTradingBuyTimers.clear();
    tradingWalletMonitor?.stop();
    telegramBot?.stop();
    scheduler.stop();
    healthServer.close();

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
