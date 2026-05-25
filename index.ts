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
  FilterProgressEvent,
  MonitorSampleEvent,
  NewTokenEvent,
  SellResult,
  TokenSummary,
  WalletFilterSettings,
} from './types';
import { PublicKey } from '@solana/web3.js';
import { randomBytes } from 'crypto';

const log = createLogger('MAIN');
const TRADING_LINK_WINDOW_MS = 5 * 60 * 1000;

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
    | { type: 'setting'; walletAddress: string; field: SettingField; mode: ProfileMode };
  const pendingTelegramActions = new Map<string, PendingTelegramAction>();
  const pendingSells = new Map<string, { event: FilterFailEvent; createdAt: number; executing: boolean }>();
  const pausedWallets = new Set<string>();
  const walletAliasesByChat = new Map<string, string[]>();

  type ProfileMode = 'global' | 'massive' | 'minimal';
  type SettingField = keyof Pick<
    WalletFilterSettings,
    | 'applyAtSample'
    | 'minBundlersPercent'
    | 'maxBundlersPercent'
    | 'minBundlersCount'
    | 'maxBundlersCount'
    | 'minBundlersCountChange'
    | 'maxBundlersCountChange'
    | 'maxPctAboveValue'
    | 'maxPctAboveOccurrences'
    | 'maxPctBelowValue'
    | 'maxPctBelowOccurrences'
  >;
  const settingFieldByCode: Record<string, SettingField> = {
    apply: 'applyAtSample',
    minPct: 'minBundlersPercent',
    maxPct: 'maxBundlersPercent',
    minCnt: 'minBundlersCount',
    maxCnt: 'maxBundlersCount',
    minInc: 'minBundlersCountChange',
    maxInc: 'maxBundlersCountChange',
    hiPct: 'maxPctAboveValue',
    hiOcc: 'maxPctAboveOccurrences',
    loPct: 'maxPctBelowValue',
    loOcc: 'maxPctBelowOccurrences',
  };
  const settingLabel: Record<SettingField, string> = {
    applyAtSample: 'Apply at sample #',
    minBundlersPercent: 'Min bundlers %',
    maxBundlersPercent: 'Max bundlers %',
    minBundlersCount: 'Min bundlers count',
    maxBundlersCount: 'Max bundlers count',
    minBundlersCountChange: 'Minimal bundler wallet count change',
    maxBundlersCountChange: 'Massive bundler wallet count change',
    maxPctAboveValue: 'Above-% threshold',
    maxPctAboveOccurrences: 'Max above-% occurrences',
    maxPctBelowValue: 'Below-% threshold',
    maxPctBelowOccurrences: 'Max below-% occurrences',
  };
  const settingHint = (field: SettingField): string => {
    if (field === 'applyAtSample') return 'Use a positive whole number.';
    if (field === 'maxPctAboveValue') return 'Use a number, or send <code>off</code> to disable. Above-% also needs Above Times set.';
    if (field === 'maxPctAboveOccurrences') return 'Use a number, or send <code>off</code> to disable. Above Times also needs Above % set.';
    if (field === 'maxPctBelowValue') return 'Use a number, or send <code>off</code> to disable. Below-% also needs Below Times set.';
    if (field === 'maxPctBelowOccurrences') return 'Use a number, or send <code>off</code> to disable. Below Times also needs Below % set.';
    return 'Use a number, or send <code>off</code> to disable.';
  };
  const profileTitle: Record<Exclude<ProfileMode, 'global'>, string> = {
    massive: 'Massive Count Change',
    minimal: 'Minimal Count Change',
  };

  function walletHasCoreCountChangeMode(walletAddress: string): boolean {
    const settings = db.getWalletSettings(walletAddress);
    return settings.maxBundlersCountChange !== null || settings.minBundlersCountChange !== null;
  }

  function linkedWalletModeLines(wallet: string): string[] {
    const settings = db.getWalletSettings(wallet);
    const lines = [`- <code>${html(wallet)}</code>`];
    if (settings.maxBundlersCountChange !== null) {
      lines.push(
        `  Massive: count change >= <b>${settings.maxBundlersCountChange}</b>, apply #${settings.massive.applyAtSample}`
      );
    }
    if (settings.minBundlersCountChange !== null) {
      lines.push(
        `  Minimal: count change &lt; <b>${settings.minBundlersCountChange}</b>, apply #${settings.minimal.applyAtSample}`
      );
    }
    if (!walletHasCoreCountChangeMode(wallet)) {
      lines.push('  Monitor-only: no Massive/Minimal Count Change set, so sell decisions are disabled.');
    }
    return lines;
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
          : 'Waiting for a watched wallet to buy this token before filters start.',
      ].join('\n')).catch((err) => log.warn('Telegram trading buy alert failed', err));

      if (matchedWallets.length > 0) {
        activateTradingToken(event.mint, matchedWallets);
      }
    });
  }

  function wireWatchedWalletMonitor(walletMonitor: WalletMonitor): void {
    walletMonitor.on('newToken', (event: NewTokenEvent) => {
      log.info(`[WATCHED BUY] Wallet: ${event.walletAddress} Mint: ${event.mint}`);
      if (!walletHasCoreCountChangeMode(event.walletAddress)) {
        log.info(`[WATCHED BUY MONITOR-ONLY] Wallet ${event.walletAddress} has no massive/minimal bundler count change setting`);
      }
      const wallets = recentWatchedBuys.get(event.mint) ?? new Set<string>();
      wallets.add(event.walletAddress);
      recentWatchedBuys.set(event.mint, wallets);

      if (pendingTradingBuys.has(event.mint)) {
        activateTradingToken(event.mint, [...wallets]);
      }
    });
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
      'Monitoring is now running on your trading-wallet position. Sell decisions run only for linked wallets with Massive or Minimal Count Change enabled.',
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
    mode: ProfileMode,
    rawValue: string
  ): string {
    const normalized = new PublicKey(walletAddress).toBase58();
    const trimmed = rawValue.trim().toLowerCase();
    const settings = db.getWalletSettings(normalized);
    const target = mode === 'global' ? settings : settings[mode];
    const targetRecord = target as unknown as Record<string, number | boolean | null>;

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

    if (field === 'maxBundlersCountChange') {
      settings.massive = {
        ...settings.massive,
        applyAtSample: settings.massive.applyAtSample || settings.applyAtSample,
      };
    }
    if (field === 'minBundlersCountChange') {
      settings.minimal = {
        ...settings.minimal,
        applyAtSample: settings.minimal.applyAtSample || settings.applyAtSample,
      };
    }

    db.updateWalletSettings(normalized, settings);
    const scope = mode === 'global' ? '' : ` (${profileTitle[mode]})`;
    return `Updated ${settingLabel[field]}${scope} for <code>${html(normalized)}</code>.`;
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
    const settings = db.getWalletSettings(normalized);
    const range = (min: number | null, max: number | null, suffix = ''): string =>
      `${fmtSetting(min)}${suffix} - ${fmtSetting(max)}${suffix}`;
    const pairLine = (
      occurrences: number | null,
      value: number | null,
      label: 'above' | 'below'
    ): string => {
      const disabled = occurrences === null || value === null
        ? ' <i>(disabled until both values are set)</i>'
        : '';
      return `No more than <b>${fmtSetting(occurrences)}</b> sample(s) ${label} <b>${fmtSetting(value)}%</b>${disabled}`;
    };
    const profileText = (mode: Exclude<ProfileMode, 'global'>): string[] => {
      const profile = settings[mode];
      const threshold = mode === 'massive'
        ? settings.maxBundlersCountChange
        : settings.minBundlersCountChange;
      if (threshold === null) return [];
      const rule = mode === 'massive'
        ? `Core rule: fail if bundler wallet count change is at least <b>${threshold}</b>.`
        : `Core rule: fail if bundler wallet count change is under <b>${threshold}</b>.`;
      return [
        '',
        `<b>${profileTitle[mode]} Settings</b>`,
        rule,
        `Apply filters at sample: <b>#${profile.applyAtSample}</b>`,
        `Observed bundlers % min/max: <b>${range(profile.minBundlersPercent, profile.maxBundlersPercent, '%')}</b>`,
        `Observed bundler wallet count min/max: <b>${range(profile.minBundlersCount, profile.maxBundlersCount)}</b>`,
        pairLine(profile.maxPctAboveOccurrences, profile.maxPctAboveValue, 'above'),
        pairLine(profile.maxPctBelowOccurrences, profile.maxPctBelowValue, 'below'),
        `${enabledMark(profile.sellIfNoPctAbove50)} At least one valid bundlers % sample is above 50%`,
        `${enabledMark(profile.sellIfFirstThreePctZero)} First 3 bundlers % samples are all 0%`,
        `${enabledMark(profile.sellIfNoTeenOrTwentyPct)} No valid sample appears in the 10%-29.99% range`,
      ];
    };
    const shortMode = (mode: Exclude<ProfileMode, 'global'>): 'm' | 'n' =>
      mode === 'massive' ? 'm' : 'n';
    const profileButtons = (mode: Exclude<ProfileMode, 'global'>) => {
      const profile = settings[mode];
      const title = profileTitle[mode];
      return [
        [{ text: `${title} Apply #: ${profile.applyAtSample}`, callback_data: `setp:${shortMode(mode)}:apply:${normalized}` }],
        [
          { text: `${title} Min %: ${fmtSetting(profile.minBundlersPercent)}`, callback_data: `setp:${shortMode(mode)}:minPct:${normalized}` },
          { text: `${title} Max %: ${fmtSetting(profile.maxBundlersPercent)}`, callback_data: `setp:${shortMode(mode)}:maxPct:${normalized}` },
        ],
        [
          { text: `${title} Min Count: ${fmtSetting(profile.minBundlersCount)}`, callback_data: `setp:${shortMode(mode)}:minCnt:${normalized}` },
          { text: `${title} Max Count: ${fmtSetting(profile.maxBundlersCount)}`, callback_data: `setp:${shortMode(mode)}:maxCnt:${normalized}` },
        ],
        [
          { text: `${title} Above %: ${fmtSetting(profile.maxPctAboveValue)}`, callback_data: `setp:${shortMode(mode)}:hiPct:${normalized}` },
          { text: `${title} Above Times: ${fmtSetting(profile.maxPctAboveOccurrences)}`, callback_data: `setp:${shortMode(mode)}:hiOcc:${normalized}` },
        ],
        [
          { text: `${title} Below %: ${fmtSetting(profile.maxPctBelowValue)}`, callback_data: `setp:${shortMode(mode)}:loPct:${normalized}` },
          { text: `${title} Below Times: ${fmtSetting(profile.maxPctBelowOccurrences)}`, callback_data: `setp:${shortMode(mode)}:loOcc:${normalized}` },
        ],
        [
          { text: `${enabledMark(profile.sellIfNoPctAbove50)} ${title} Any >50%`, callback_data: `togglep:${shortMode(mode)}:gt50:${normalized}` },
        ],
        [
          { text: `${enabledMark(profile.sellIfFirstThreePctZero)} ${title} First 3=0`, callback_data: `togglep:${shortMode(mode)}:first0:${normalized}` },
        ],
        [
          { text: `${enabledMark(profile.sellIfNoTeenOrTwentyPct)} ${title} 10s/20s`, callback_data: `togglep:${shortMode(mode)}:teen20:${normalized}` },
        ],
      ];
    };
    const modeButton = (mode: Exclude<ProfileMode, 'global'>) => [
      {
        text: mode === 'massive'
          ? `${enabledMark(settings.maxBundlersCountChange !== null)} Massive Count Change: ${fmtSetting(settings.maxBundlersCountChange)}`
          : `${enabledMark(settings.minBundlersCountChange !== null)} Minimal Count Change: ${fmtSetting(settings.minBundlersCountChange)}`,
        callback_data: mode === 'massive'
          ? `set:maxInc:${normalized}`
          : `set:minInc:${normalized}`,
      },
    ];
    const groupedModeButtons = [
      modeButton('massive'),
      ...(settings.maxBundlersCountChange !== null ? profileButtons('massive') : []),
      modeButton('minimal'),
      ...(settings.minBundlersCountChange !== null ? profileButtons('minimal') : []),
    ];

    return {
      text: [
        '<b>Filter Settings</b>',
        `<code>${html(normalized)}</code>`,
        '',
        '<b>Core Modes</b>',
        `${enabledMark(settings.maxBundlersCountChange !== null)} Massive Count Change: enables Massive sell decisions.`,
        `${enabledMark(settings.minBundlersCountChange !== null)} Minimal Count Change: enables Minimal sell decisions.`,
        'If neither mode is enabled, linked tokens are monitor-only and still summarize at sample #20.',
        'Count change is measured as highest bundler wallet count minus lowest bundler wallet count.',
        'Valid bundlers % samples are 1% or higher.',
        ...profileText('massive'),
        ...profileText('minimal'),
        '',
        '<i>For min/max filters, Any disables only that side of the range. Above/below sample rules need both values. Normal % filters ignore samples below 1%.</i>',
      ].join('\n'),
      replyMarkup: {
        inline_keyboard: [
          ...groupedModeButtons,
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

  async function executeSellAndNotify(
    chatId: string,
    sellId: string,
    telegramBot: TelegramBot
  ): Promise<void> {
    const pending = pendingSells.get(sellId);
    if (!pending) return;

    try {
      const result = await gmgnClient.sellTokenForSol(
        pending.event.walletAddress,
        pending.event.mint,
        {
          percent: config.sellPercent,
          slippage: config.sellSlippage,
          autoSlippage: config.sellAutoSlippage,
          priorityFeeSol: config.sellPriorityFeeSol,
          antiMev: config.sellAntiMev,
        }
      );
      if (result.status === 'confirmed') {
        scheduler.removeToken(pending.event.mint);
        db.updateTokenStatus(pending.event.walletAddress, pending.event.mint, 'stopped');
      }
      await telegramBot.sendChat(chatId, sellReceipt(pending.event, result));
    } catch (err) {
      await telegramBot.sendChat(chatId, sellFailedReply(pending.event, err));
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
            mode: 'global',
          });
          return {
            text: [
              `Send a value for <b>${settingLabel[field]}</b>.`,
              settingHint(field),
            ].join('\n'),
            trackPrompt: true,
          };
        }
        if (callbackKind === 'setp' && parts.length >= 4) {
          const [, rawMode, rawField, rawAddress] = parts;
          const mode = rawMode === 'm' ? 'massive' : rawMode === 'n' ? 'minimal' : rawMode;
          if (mode !== 'massive' && mode !== 'minimal') return 'Invalid setting group.';
          const field = settingFieldByCode[rawField];
          if (!field || field === 'minBundlersCountChange' || field === 'maxBundlersCountChange') {
            return 'Invalid setting.';
          }
          const normalized = new PublicKey(rawAddress).toBase58();
          pendingTelegramActions.set(chatId, {
            type: 'setting',
            walletAddress: normalized,
            field,
            mode,
          });
          return {
            text: [
              `Send a value for <b>${profileTitle[mode]} ${settingLabel[field]}</b>.`,
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
        if (callbackKind === 'togglep' && parts.length >= 4) {
          const [, rawMode, rawAction, rawAddress] = parts;
          const mode = rawMode === 'm' ? 'massive' : rawMode === 'n' ? 'minimal' : rawMode;
          if (mode !== 'massive' && mode !== 'minimal') return 'Invalid setting group.';
          const normalized = new PublicKey(rawAddress).toBase58();
          const settings = db.getWalletSettings(normalized);
          const profile = settings[mode];
          if (rawAction === 'gt50') {
            profile.sellIfNoPctAbove50 = !profile.sellIfNoPctAbove50;
          } else if (rawAction === 'first0') {
            profile.sellIfFirstThreePctZero = !profile.sellIfFirstThreePctZero;
          } else if (rawAction === 'teen20') {
            profile.sellIfNoTeenOrTwentyPct = !profile.sellIfNoTeenOrTwentyPct;
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
            pendingAction.mode,
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
    scheduler.on('filterProgress', (event: FilterProgressEvent) => {
      telegramBot.sendFilterProgressCard(event).catch((err) =>
        log.warn('Telegram filter progress send failed', err)
      );
    });
    scheduler.on('summary', (summary: TokenSummary) => {
      telegramBot.sendSummaryCard(summary).catch((err) =>
        log.warn('Telegram summary send failed', err)
      );
    });
    scheduler.on('filterFail', (event: FilterFailEvent) => {
      const sellId = randomBytes(5).toString('hex');
      pendingSells.set(sellId, {
        event: {
          ...event,
          buySol: event.buySol ?? db.getToken(event.walletAddress, event.mint)?.buySol ?? null,
        },
        createdAt: Date.now(),
        executing: false,
      });
      telegramBot.sendFilterFailCard(event, sellAlertMarkup(sellId)).catch((err) =>
        log.warn('Telegram filter alert send failed', err)
      );
    });
    scheduler.on('filterPass', (event: FilterPassEvent) => {
      telegramBot.sendFilterPassCard(event).catch((err) =>
        log.warn('Telegram filter pass send failed', err)
      );
    });
    telegramBot.start();
  }

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
