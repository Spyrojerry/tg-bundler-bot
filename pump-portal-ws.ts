// ─────────────────────────────────────────────────────────────────────────────
//  pump-portal-ws.ts — PumpPortal real-time WebSocket (subscribeMigration)
// ─────────────────────────────────────────────────────────────────────────────

import WebSocket from 'ws';
import { createLogger, Logger } from './logger';

const PUMPPORTAL_WS_BASE = 'wss://pumpportal.fun/api/data';
const BASE_RECONNECT_DELAY_MS = 1_000;
const MAX_RECONNECT_DELAY_MS = 30_000;
const CONNECT_TIMEOUT_MS = 10_000;
const HEARTBEAT_INTERVAL_MS = 20_000;
const HEARTBEAT_TIMEOUT_MS = 12_000;
const STATE_LOG_INTERVAL_MS = 60_000;
const WATCHDOG_INTERVAL_MS = 30_000;

export interface PumpPortalMigrationEvent {
  mint: string;
  signature: string;
  timestamp: number;
  raw: unknown;
}

type MigrationCallback = (event: PumpPortalMigrationEvent) => void;

export interface PumpPortalWsStatus {
  connected: boolean;
  connecting: boolean;
  migrationSubscribed: boolean;
  migrationFeedSuspended: boolean;
  lastMessageAt: number | null;
  lastMigrationMessageAt: number | null;
  lastPongAt: number | null;
  lastActivityAt: number | null;
  reconnectAttempts: number;
  reconnectScheduled: boolean;
  stale: boolean;
}

export class PumpPortalWsClient {
  private readonly url: string;
  private readonly log: Logger;
  private ws: WebSocket | null = null;
  private connected = false;
  private connecting = false;
  private closedByUser = false;
  private reconnectAttempts = 0;
  private reconnectTimer: NodeJS.Timeout | null = null;
  private connectTimeoutTimer: NodeJS.Timeout | null = null;
  private heartbeatTimer: NodeJS.Timeout | null = null;
  private watchdogTimer: NodeJS.Timeout | null = null;
  private stateLogTimer: NodeJS.Timeout | null = null;
  private lastPongAt: number | null = null;
  private lastMessageAt: number | null = null;
  private lastMigrationMessageAt: number | null = null;
  private lastActivityAt: number | null = null;
  private migrationCallback: MigrationCallback | null = null;
  private migrationSubscribed = false;
  /** When true, migration feed is torn down (unsubscribe + disconnect) until resumed. */
  private migrationFeedSuspended = false;

  constructor(apiKey: string, label = 'PumpPortal WS') {
    const key = apiKey.trim();
    this.url = `${PUMPPORTAL_WS_BASE}?api-key=${encodeURIComponent(key)}`;
    this.log = createLogger(label.toUpperCase());
  }

  onMigration(callback: MigrationCallback): void {
    this.migrationCallback = callback;
  }

  getStatus(): PumpPortalWsStatus {
    const now = Date.now();
    return {
      connected: this.connected,
      connecting: this.connecting,
      migrationSubscribed: this.migrationSubscribed,
      migrationFeedSuspended: this.migrationFeedSuspended,
      lastMessageAt: this.lastMessageAt,
      lastMigrationMessageAt: this.lastMigrationMessageAt,
      lastPongAt: this.lastPongAt,
      lastActivityAt: this.lastActivityAt,
      reconnectAttempts: this.reconnectAttempts,
      reconnectScheduled: this.reconnectTimer !== null,
      stale:
        !this.migrationFeedSuspended &&
        (!this.connected ||
          !this.lastPongAt ||
          now - this.lastPongAt > HEARTBEAT_INTERVAL_MS + HEARTBEAT_TIMEOUT_MS),
    };
  }

  connect(): void {
    if (this.connecting) return;
    this.closedByUser = false;
    this.connecting = true;
    this.log.info('Connecting to PumpPortal WebSocket', {
      reconnectAttempt: this.reconnectAttempts,
    });
    this.startWatchdog();
    const ws = new WebSocket(this.url);
    this.ws = ws;
    this.connectTimeoutTimer = setTimeout(() => {
      if (this.ws !== ws || !this.connecting) return;
      this.log.warn('PumpPortal WebSocket connection timed out, forcing reconnect', {
        timeoutMs: CONNECT_TIMEOUT_MS,
      });
      this.connecting = false;
      ws.terminate();
    }, CONNECT_TIMEOUT_MS);

    ws.on('open', () => {
      this.clearConnectTimeout();
      this.connecting = false;
      this.connected = true;
      this.reconnectAttempts = 0;
      this.markActivity('open');
      this.migrationSubscribed = false;
      this.log.info('Connected to PumpPortal WebSocket');
      this.startHeartbeat();
      if (!this.migrationFeedSuspended) {
        this.sendSubscribeMigration();
      }
    });

    ws.on('message', (data: WebSocket.Data) => {
      this.lastMessageAt = Date.now();
      this.markActivity('message');
      this.handleMessage(data);
    });

    ws.on('pong', () => {
      this.lastPongAt = Date.now();
      this.markActivity('pong');
    });

    ws.on('error', (err: Error) => {
      this.log.error('PumpPortal WebSocket error', {
        error: err.message,
        ...this.getStatus(),
      });
    });

    ws.on('close', (code: number, reason: Buffer) => {
      this.clearConnectTimeout();
      this.connecting = false;
      this.connected = false;
      this.migrationSubscribed = false;
      this.log.warn('PumpPortal WebSocket close received', {
        code,
        reason: reason?.toString?.() || undefined,
        ...this.getStatus(),
      });
      if (this.heartbeatTimer) {
        clearInterval(this.heartbeatTimer);
        this.heartbeatTimer = null;
      }
      if (!this.closedByUser && !this.migrationFeedSuspended) {
        this.log.warn('PumpPortal WebSocket closed, will reconnect', {
          code,
          reason: reason?.toString?.() || undefined,
        });
        this.scheduleReconnect();
      } else if (this.migrationFeedSuspended) {
        this.log.info('PumpPortal WebSocket closed — migration feed suspended (no reconnect)');
      }
    });
  }

  close(): void {
    this.closedByUser = true;
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    this.clearConnectTimeout();
    if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);
    if (this.watchdogTimer) clearInterval(this.watchdogTimer);
    if (this.stateLogTimer) clearInterval(this.stateLogTimer);
    this.watchdogTimer = null;
    this.stateLogTimer = null;
    this.ws?.close();
    this.ws = null;
  }

  get isConnected(): boolean {
    return this.connected;
  }

  isMigrationFeedSuspended(): boolean {
    return this.migrationFeedSuspended;
  }

  /** Unsubscribe from migration events and disconnect until resumeMigrationFeed(). */
  suspendMigrationFeed(reason: string): void {
    if (this.migrationFeedSuspended) return;
    this.migrationFeedSuspended = true;
    this.log.info('Unsubscribing from PumpPortal migration feed', { reason });

    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }

    this.sendUnsubscribeMigration();
    this.migrationSubscribed = false;

    if (this.ws) {
      const ws = this.ws;
      this.ws = null;
      this.connecting = false;
      this.connected = false;
      this.log.info('PumpPortal migration feed suspended; WebSocket state reset', {
        reason,
        ...this.getStatus(),
      });
      this.clearConnectTimeout();
      if (this.heartbeatTimer) {
        clearInterval(this.heartbeatTimer);
        this.heartbeatTimer = null;
      }
      try {
        ws.close();
      } catch {
        // ignore
      }
    }
  }

  /** Reconnect to PumpPortal and subscribe to migration events again. */
  resumeMigrationFeed(reason: string): void {
    if (!this.migrationFeedSuspended) return;
    this.migrationFeedSuspended = false;
    this.log.info('Resubscribing to PumpPortal migration feed', { reason });
    if (!this.connected && !this.connecting) {
      this.connect();
      return;
    }
    this.sendSubscribeMigration();
  }

  private scheduleReconnect(): void {
    if (this.closedByUser || this.reconnectTimer) return;
    const attempt = this.reconnectAttempts;
    this.reconnectAttempts += 1;
    const delay = Math.min(
      MAX_RECONNECT_DELAY_MS,
      BASE_RECONNECT_DELAY_MS * 2 ** attempt,
    );
    const jitter = Math.floor(Math.random() * 250);
    const delayMs = delay + jitter;
    this.log.warn('Scheduling PumpPortal WebSocket reconnect', {
      reconnectAttempt: attempt + 1,
      delayMs,
    });
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      this.connect();
    }, delayMs);
  }

  private clearConnectTimeout(): void {
    if (!this.connectTimeoutTimer) return;
    clearTimeout(this.connectTimeoutTimer);
    this.connectTimeoutTimer = null;
  }

  private startHeartbeat(): void {
    if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);
    this.heartbeatTimer = setInterval(() => {
      if (!this.ws || this.ws.readyState !== WebSocket.OPEN) return;
      if (
        !this.lastPongAt ||
        Date.now() - this.lastPongAt > HEARTBEAT_INTERVAL_MS + HEARTBEAT_TIMEOUT_MS
      ) {
        this.log.warn('PumpPortal WebSocket heartbeat timed out, forcing reconnect', {
          ...this.getStatus(),
        });
        this.ws.terminate();
        return;
      }
      try {
        this.ws.ping();
      } catch (err) {
        this.log.warn('PumpPortal WebSocket heartbeat ping failed', {
          error: err instanceof Error ? err.message : String(err),
        });
      }
    }, HEARTBEAT_INTERVAL_MS);
  }

  private startWatchdog(): void {
    if (this.watchdogTimer) clearInterval(this.watchdogTimer);
    if (this.stateLogTimer) clearInterval(this.stateLogTimer);
    this.watchdogTimer = setInterval(() => {
      if (this.migrationFeedSuspended || this.closedByUser) return;
      const status = this.getStatus();
      if (status.stale && this.ws && this.ws.readyState === WebSocket.OPEN) {
        this.log.warn('PumpPortal WebSocket transport is stale, forcing reconnect', status);
        this.ws.terminate();
      }
    }, WATCHDOG_INTERVAL_MS);
    this.stateLogTimer = setInterval(() => {
      this.log.info('PumpPortal WebSocket state', this.getStatus());
    }, STATE_LOG_INTERVAL_MS);
  }

  private markActivity(source: 'open' | 'message' | 'pong'): void {
    const now = Date.now();
    this.lastActivityAt = now;
    if (source === 'open') this.lastPongAt = now;
  }

  private sendUnsubscribeMigration(): void {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) return;
    try {
      this.ws.send(JSON.stringify({ method: 'unsubscribeMigration' }));
      this.log.info('Sent PumpPortal unsubscribeMigration');
    } catch (err) {
      this.log.warn('Failed to send PumpPortal unsubscribeMigration', {
        error: err instanceof Error ? err.message : String(err),
      });
    }
  }

  private sendSubscribeMigration(): void {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) return;
    if (this.migrationFeedSuspended) return;
    try {
      this.ws.send(JSON.stringify({ method: 'subscribeMigration' }));
      this.migrationSubscribed = true;
      this.lastMigrationMessageAt = Date.now();
      this.markActivity('message');
      this.log.info('Sent PumpPortal subscribeMigration');
    } catch (err) {
      this.log.warn('Failed to send PumpPortal subscribeMigration', {
        error: err instanceof Error ? err.message : String(err),
      });
    }
  }

  private handleMessage(data: WebSocket.Data): void {
    let parsed: unknown;
    try {
      parsed = JSON.parse(data.toString());
    } catch {
      this.log.debug('PumpPortal WS message was not JSON');
      return;
    }

    if (this.migrationFeedSuspended) return;

    const event = parsePumpPortalMigrationEvent(parsed);
    if (!event) return;
    this.lastMigrationMessageAt = Date.now();

    try {
      this.migrationCallback?.(event);
    } catch (err) {
      this.log.error('PumpPortal migration callback threw', {
        error: err instanceof Error ? err.message : String(err),
      });
    }
  }
}

export function parsePumpPortalMigrationEvent(
  data: unknown,
): PumpPortalMigrationEvent | null {
  if (!data || typeof data !== 'object') return null;
  const obj = data as Record<string, unknown>;

  if (obj.method && typeof obj.method === 'string') return null;
  if (obj.message === 'subscribed' || obj.status === 'subscribed') return null;

  const nested =
    obj.data && typeof obj.data === 'object'
      ? (obj.data as Record<string, unknown>)
      : null;
  const source = nested ?? obj;

  const mint =
    pickString(source, ['mint', 'token', 'mintAddress', 'tokenAddress']) ??
    pickString(obj, ['mint', 'token', 'mintAddress', 'tokenAddress']);
  if (!mint) return null;

  const signature =
    pickString(source, ['signature', 'tx', 'transactionSignature']) ??
    pickString(obj, ['signature', 'tx', 'transactionSignature']) ??
    `pumpportal-migration:${mint}:${pickTimestamp(source) ?? pickTimestamp(obj) ?? Date.now()}`;

  const timestamp = pickTimestamp(source) ?? pickTimestamp(obj) ?? Math.floor(Date.now() / 1000);

  return { mint, signature, timestamp, raw: data };
}

function pickString(obj: Record<string, unknown>, keys: string[]): string | null {
  for (const key of keys) {
    const value = obj[key];
    if (typeof value === 'string' && value.length > 0) return value;
  }
  return null;
}

function pickTimestamp(obj: Record<string, unknown>): number | null {
  const raw = obj.timestamp ?? obj.blockTime ?? obj.time;
  if (typeof raw === 'number' && Number.isFinite(raw)) {
    return raw > 1_000_000_000_000 ? Math.floor(raw / 1000) : Math.floor(raw);
  }
  if (typeof raw === 'string') {
    const asNum = Number(raw);
    if (Number.isFinite(asNum)) {
      return asNum > 1_000_000_000_000 ? Math.floor(asNum / 1000) : Math.floor(asNum);
    }
    const parsed = Date.parse(raw);
    if (Number.isFinite(parsed)) return Math.floor(parsed / 1000);
  }
  return null;
}
