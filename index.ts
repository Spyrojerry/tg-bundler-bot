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
import { TelegramBot, TelegramReply } from './telegram-bot';
import { startHealthServer } from './health-server';
import {
  FilterFailEvent,
  MonitorSampleEvent,
  NewTokenEvent,
  TokenSummary,
  WalletFilterSettings,
} from './types';
import { PublicKey } from '@solana/web3.js';

const log = createLogger('MAIN');

async function main(): Promise<void> {
  // ── 1. Config ──────────────────────────────────────────────────────────────
  const config = loadConfig();
  setLogLevel(config.logLevel);

  log.info('═══════════════════════════════════════');
  log.info('  GMGN Bundler Monitor  — starting up');
  log.info('═══════════════════════════════════════');
  log.info('Config', {
    wallet:          config.walletAddress,
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
  type PendingTelegramAction =
    | { type: 'addwallet' | 'removewallet' }
    | { type: 'setting'; walletAddress: string; field: SettingField };
  const pendingTelegramActions = new Map<string, PendingTelegramAction>();
  const pausedWallets = new Set<string>();

  type SettingField = keyof Pick<
    WalletFilterSettings,
    | 'applyAtSample'
    | 'minBundlersPercent'
    | 'maxBundlersPercent'
    | 'minBundlersCount'
    | 'maxBundlersCount'
    | 'maxBundlersPercentIncrease'
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
    pctInc: 'maxBundlersPercentIncrease',
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
    maxBundlersPercentIncrease: 'Max % increase',
    maxPctAboveValue: 'Above-% threshold',
    maxPctAboveOccurrences: 'Max above-% occurrences',
    maxPctBelowValue: 'Below-% threshold',
    maxPctBelowOccurrences: 'Max below-% occurrences',
  };

  function wireWalletMonitor(walletMonitor: WalletMonitor): void {
    walletMonitor.on('newToken', (event: NewTokenEvent) => {
    const { mint, detectedAt } = event;

    log.info(`[NEW TOKEN] Wallet: ${event.walletAddress} Mint: ${mint}`);

    // Persist to DB if not already there
    if (!db.tokenExists(event.walletAddress, mint)) {
      db.insertToken({
        walletAddress: event.walletAddress,
        mint,
        firstSeen: new Date().toISOString(),
        monitoringStatus: 'active',
        detectedAt,
      });
    }

    // Add to scheduler
    scheduler.addToken(event);
    });
  }

  async function startWallet(address: string): Promise<string> {
    const normalized = new PublicKey(address).toBase58();
    if (walletMonitors.has(normalized)) {
      return `Already monitoring <code>${normalized}</code>`;
    }

    db.addWallet(normalized);
    const monitor = new WalletMonitor(config, normalized);
    wireWalletMonitor(monitor);
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

  function updateSetting(
    walletAddress: string,
    field: SettingField,
    rawValue: string
  ): string {
    const normalized = new PublicKey(walletAddress).toBase58();
    const trimmed = rawValue.trim().toLowerCase();
    const settings = db.getWalletSettings(normalized);

    if (trimmed === 'off' || trimmed === 'any' || trimmed === 'none') {
      if (field === 'applyAtSample') {
        return 'Apply sample cannot be disabled. Send a positive whole number.';
      }
      settings[field] = null as never;
    } else {
      const value = Number(trimmed);
      if (!Number.isFinite(value) || value < 0) {
        return `Invalid value. Send a non-negative number, or "off".`;
      }
      if (field === 'applyAtSample') {
        const n = Math.trunc(value);
        if (n <= 0) return 'Apply sample must be at least 1.';
        settings[field] = n as never;
      } else {
        settings[field] = value as never;
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
              [actionButton],
              [pauseButton],
              [{ text: 'Settings', callback_data: `wallet:settings:${normalized}` }],
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
    const mark = (enabled: boolean): string => enabled ? '✅' : '❌';
    const range = (min: number | null, max: number | null, suffix = ''): string =>
      `${fmtSetting(min)}${suffix} - ${fmtSetting(max)}${suffix}`;

    return {
      text: [
        '<b>Filter Settings</b>',
        `<code>${html(normalized)}</code>`,
        '',
        '<b>When To Check</b>',
        `Apply filters at sample: <b>#${settings.applyAtSample}</b>`,
        '',
        '<b>Allowed Ranges</b>',
        `Bundlers %: <b>${range(settings.minBundlersPercent, settings.maxBundlersPercent, '%')}</b>`,
        `Bundler wallets: <b>${range(settings.minBundlersCount, settings.maxBundlersCount)}</b>`,
        `Max bundlers % increase: <b>${fmtSetting(settings.maxBundlersPercentIncrease)}%</b>`,
        '',
        '<b>Occurrence Limits</b>',
        `No more than <b>${fmtSetting(settings.maxPctAboveOccurrences)}</b> sample(s) above <b>${fmtSetting(settings.maxPctAboveValue)}%</b>`,
        `No more than <b>${fmtSetting(settings.maxPctBelowOccurrences)}</b> sample(s) below <b>${fmtSetting(settings.maxPctBelowValue)}%</b>`,
        '',
        '<b>Pattern Checks</b>',
        `${mark(settings.sellIfFirstThreePctZero)} First 3 bundlers % samples are all 0%`,
        `${mark(settings.sellIfNoTeenOrTwentyPct)} No valid sample appears in the 10%-29.99% range`,
        '',
        '<i>Any = disabled. Normal % filters ignore samples below 1%.</i>',
      ].join('\n'),
      replyMarkup: {
        inline_keyboard: [
          [
            { text: 'Apply #', callback_data: `set:apply:${normalized}` },
            { text: 'Min %', callback_data: `set:minPct:${normalized}` },
            { text: 'Max %', callback_data: `set:maxPct:${normalized}` },
          ],
          [
            { text: 'Min Count', callback_data: `set:minCnt:${normalized}` },
            { text: 'Max Count', callback_data: `set:maxCnt:${normalized}` },
          ],
          [
            { text: 'Max % Increase', callback_data: `set:pctInc:${normalized}` },
          ],
          [
            { text: 'Above % Limit', callback_data: `set:hiPct:${normalized}` },
            { text: 'Above Max Times', callback_data: `set:hiOcc:${normalized}` },
          ],
          [
            { text: 'Below % Limit', callback_data: `set:loPct:${normalized}` },
            { text: 'Below Max Times', callback_data: `set:loOcc:${normalized}` },
          ],
          [
            { text: `${mark(settings.sellIfFirstThreePctZero)} First 3 = 0%`, callback_data: `toggle:first0:${normalized}` },
          ],
          [
            { text: `${mark(settings.sellIfNoTeenOrTwentyPct)} Require 10s/20s`, callback_data: `toggle:teen20:${normalized}` },
          ],
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
    const walletLines = wallets.length
      ? wallets.map((wallet, index) => `${index + 1}. <code>${html(wallet)}</code>`)
      : ['No wallets are currently monitored.'];
    const runningWallets = wallets.length - pausedWallets.size;

    return {
      text: [
        '<b>GMGN Bundler Monitor</b>',
        '',
        `Wallets monitored: <b>${walletMonitors.size}</b>`,
        `Running wallets: <b>${runningWallets}</b>`,
        `Paused wallets: <b>${pausedWallets.size}</b>`,
        `Active token windows: <b>${scheduler.activeCount}</b>`,
        `Monitor window: ${Math.round(config.monitoringWindowMs / 1_000)}s`,
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

  function walletsReply(): string {
    const wallets = [...walletMonitors.keys()];
    return wallets.length
      ? `Monitoring ${wallets.length} wallet(s):\n${wallets.map((w) => `- <code>${html(w)}</code>`).join('\n')}`
      : 'No wallets are being monitored.';
  }

  function statusReply(): string {
    return [
      `Wallets: ${walletMonitors.size}`,
      `Active tokens: ${scheduler.activeCount}`,
      `Window: ${Math.round(config.monitoringWindowMs / 1_000)}s`,
      `Interval: ${config.monitorInterval}ms`,
    ].join('\n');
  }

  async function handleTelegramCommand(_chatId: string, text: string): Promise<string | TelegramReply> {
    const chatId = _chatId;
    const [command] = text.split(/\s+/, 1);

    try {
      if (command === '/callback') {
        const [, data] = text.split(/\s+/, 2);
        const [callbackKind, callbackAction, callbackAddress] = data?.split(':') ?? [];

        if (data === 'menu:addwallet') {
          pendingTelegramActions.set(chatId, { type: 'addwallet' });
          return 'Send the Solana wallet address to add.';
        }
        if (data === 'menu:removewallet') {
          pendingTelegramActions.set(chatId, { type: 'removewallet' });
          return 'Send the Solana wallet address to remove.';
        }
        if (data === 'menu:refresh') return homeReply(true);
        if (data === 'menu:wallets') return walletsReply();
        if (data === 'menu:status') return statusReply();

        if (callbackKind === 'set' && callbackAction && callbackAddress) {
          const field = settingFieldByCode[callbackAction];
          if (!field) return 'Invalid setting.';
          const normalized = new PublicKey(callbackAddress).toBase58();
          pendingTelegramActions.set(chatId, {
            type: 'setting',
            walletAddress: normalized,
            field,
          });
          return [
            `Send a value for <b>${settingLabel[field]}</b>.`,
            field === 'applyAtSample' ? 'Use a positive whole number.' : 'Use a number, or send <code>off</code> to disable.',
          ].join('\n');
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
          return await startWallet(text);
        }
        if (pendingAction.type === 'removewallet') {
          return stopWallet(text);
        }
        if (pendingAction.type === 'setting') {
          const message = updateSetting(
            pendingAction.walletAddress,
            pendingAction.field,
            text
          );
          return `${message}\n\nSend the wallet again or open Settings to review.`;
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
        return 'Send the Solana wallet address to add.';
      }
      if (command === '/removewallet') {
        pendingTelegramActions.set(chatId, { type: 'removewallet' });
        return 'Send the Solana wallet address to remove.';
      }
      if (command === '/wallets') {
        return walletsReply();
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

  const telegramBot = config.telegramBotToken
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
    scheduler.on('filterFail', (event: FilterFailEvent) => {
      telegramBot.sendFilterFailCard(event).catch((err) =>
        log.warn('Telegram filter alert send failed', err)
      );
    });
    telegramBot.start();
  }

  // ── 6. Start everything ───────────────────────────────────────────────────
  if (config.walletAddress) {
    db.addWallet(config.walletAddress);
  }
  const wallets = db.getActiveWallets();
  for (const wallet of wallets) {
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
