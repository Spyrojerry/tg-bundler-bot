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
import { MonitorSampleEvent, NewTokenEvent, TokenSummary } from './types';
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
  const pendingTelegramActions = new Map<string, 'addwallet' | 'removewallet'>();
  let monitoringPaused = false;

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
    if (!monitoringPaused) {
      await monitor.start();
    }
    walletMonitors.set(normalized, monitor);
    return monitoringPaused
      ? `Added wallet <code>${normalized}</code>; monitoring is paused`
      : `Monitoring wallet <code>${normalized}</code>`;
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
    db.removeWallet(normalized);
    return `Stopped monitoring <code>${normalized}</code>`;
  }

  async function pauseMonitoring(): Promise<string> {
    if (monitoringPaused) return 'Monitoring is already paused.';
    monitoringPaused = true;
    for (const monitor of walletMonitors.values()) {
      monitor.stop();
    }
    return `Paused monitoring for ${walletMonitors.size} wallet(s).`;
  }

  async function resumeMonitoring(): Promise<string> {
    if (!monitoringPaused) return 'Monitoring is already running.';
    monitoringPaused = false;
    for (const monitor of walletMonitors.values()) {
      await monitor.start();
    }
    return `Continued monitoring for ${walletMonitors.size} wallet(s).`;
  }

  const html = (value: string): string =>
    value
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;');

  function walletSummaryReply(address: string): TelegramReply {
    const normalized = new PublicKey(address).toBase58();
    const isMonitoring = walletMonitors.has(normalized);
    const action = isMonitoring ? 'remove' : 'add';
    const buttonText = isMonitoring ? 'Remove wallet' : 'Add wallet';

    return {
      text: [
        '<b>Wallet</b>',
        `<code>${html(normalized)}</code>`,
        '',
        `Status: <b>${isMonitoring ? 'Monitoring' : 'Not monitoring'}</b>`,
        `Tokens seen: ${db.tokenCountForWallet(normalized)}`,
        `Samples stored: ${db.sampleCountForWallet(normalized)}`,
      ].join('\n'),
      replyMarkup: {
        inline_keyboard: [[
          {
            text: buttonText,
            callback_data: `wallet:${action}:${normalized}`,
          },
        ]],
      },
    };
  }

  function homeReply(): TelegramReply {
    const wallets = [...walletMonitors.keys()];
    const walletLines = wallets.length
      ? wallets.map((wallet, index) => `${index + 1}. <code>${html(wallet)}</code>`)
      : ['No wallets are currently monitored.'];

    return {
      text: [
        '<b>GMGN Bundler Monitor</b>',
        '',
        `Wallets monitored: <b>${walletMonitors.size}</b>`,
        `Monitoring: <b>${monitoringPaused ? 'Paused' : 'Running'}</b>`,
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
            monitoringPaused
              ? { text: 'Continue monitoring', callback_data: 'menu:resume' }
              : { text: 'Pause monitoring', callback_data: 'menu:pause' },
          ],
          [
            { text: 'Refresh', callback_data: 'menu:refresh' },
          ],
        ],
      },
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
        if (data === 'menu:addwallet') {
          pendingTelegramActions.set(chatId, 'addwallet');
          return 'Send the Solana wallet address to add.';
        }
        if (data === 'menu:removewallet') {
          pendingTelegramActions.set(chatId, 'removewallet');
          return 'Send the Solana wallet address to remove.';
        }
        if (data === 'menu:pause') return await pauseMonitoring();
        if (data === 'menu:resume') return await resumeMonitoring();
        if (data === 'menu:refresh') return homeReply();
        if (data === 'menu:wallets') return walletsReply();
        if (data === 'menu:status') return statusReply();

        const [kind, action, address] = data?.split(':') ?? [];
        if (kind !== 'wallet' || !address) return 'Invalid button action.';
        if (action === 'add') return await startWallet(address);
        if (action === 'remove') return stopWallet(address);
        return 'Invalid button action.';
      }

      const pendingAction = pendingTelegramActions.get(chatId);
      if (pendingAction && !text.startsWith('/')) {
        pendingTelegramActions.delete(chatId);
        if (pendingAction === 'addwallet') {
          return await startWallet(text);
        }
        return stopWallet(text);
      }

      if (command === '/cancel') {
        pendingTelegramActions.delete(chatId);
        return 'Cancelled.';
      }
      if (command === '/start' || command === '/help') {
        return homeReply();
      }
      if (command === '/addwallet') {
        pendingTelegramActions.set(chatId, 'addwallet');
        return 'Send the Solana wallet address to add.';
      }
      if (command === '/removewallet') {
        pendingTelegramActions.set(chatId, 'removewallet');
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
