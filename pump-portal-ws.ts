// ─────────────────────────────────────────────────────────────────────────────
//  pump-portal-ws.ts — PumpPortal real-time WebSocket (subscribeMigration)
// ─────────────────────────────────────────────────────────────────────────────

import WebSocket from 'ws';
import { createLogger, Logger } from './logger';

const PUMPPORTAL_WS_BASE = 'wss://pumpportal.fun/api/data';
const BASE_RECONNECT_DELAY_MS = 1_000;
const MAX_RECONNECT_DELAY_MS = 30_000;
const HEARTBEAT_INTERVAL_MS = 20_000;
const HEARTBEAT_TIMEOUT_MS = 12_000;

export interface PumpPortalMigrationEvent {
  mint: string;
  signature: string;
  timestamp: number;
  raw: unknown;
}

type MigrationCallback = (event: PumpPortalMigrationEvent) => void;

export class PumpPortalWsClient {
  private readonly url: string;
  private readonly log: Logger;
  private ws: WebSocket | null = null;
  private connected = false;
  private connecting = false;
  private closedByUser = false;
  private reconnectAttempts = 0;
  private reconnectTimer: NodeJS.Timeout | null = null;
  private heartbeatTimer: NodeJS.Timeout | null = null;
  private lastPongAt = Date.now();
  private migrationCallback: MigrationCallback | null = null;
  private migrationSubscribed = false;

  constructor(apiKey: string, label = 'PumpPortal WS') {
    const key = apiKey.trim();
    this.url = `${PUMPPORTAL_WS_BASE}?api-key=${encodeURIComponent(key)}`;
    this.log = createLogger(label.toUpperCase());
  }

  onMigration(callback: MigrationCallback): void {
    this.migrationCallback = callback;
  }

  connect(): void {
    if (this.connecting) return;
    this.closedByUser = false;
    this.connecting = true;
    this.log.info('Connecting to PumpPortal WebSocket');
    const ws = new WebSocket(this.url);
    this.ws = ws;

    ws.on('open', () => {
      this.connecting = false;
      this.connected = true;
      this.reconnectAttempts = 0;
      this.lastPongAt = Date.now();
      this.migrationSubscribed = false;
      this.log.info('Connected to PumpPortal WebSocket');
      this.startHeartbeat();
      this.sendSubscribeMigration();
    });

    ws.on('message', (data: WebSocket.Data) => {
      this.handleMessage(data);
    });

    ws.on('pong', () => {
      this.lastPongAt = Date.now();
    });

    ws.on('error', (err: Error) => {
      this.log.warn('PumpPortal WebSocket error', { error: err.message });
    });

    ws.on('close', (code: number, reason: Buffer) => {
      this.connecting = false;
      this.connected = false;
      this.migrationSubscribed = false;
      if (this.heartbeatTimer) {
        clearInterval(this.heartbeatTimer);
        this.heartbeatTimer = null;
      }
      if (!this.closedByUser) {
        this.log.warn('PumpPortal WebSocket closed, will reconnect', {
          code,
          reason: reason?.toString?.() || undefined,
        });
        this.scheduleReconnect();
      }
    });
  }

  close(): void {
    this.closedByUser = true;
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);
    this.ws?.close();
    this.ws = null;
  }

  get isConnected(): boolean {
    return this.connected;
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
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      this.connect();
    }, delay + jitter);
  }

  private startHeartbeat(): void {
    if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);
    this.heartbeatTimer = setInterval(() => {
      if (!this.ws || this.ws.readyState !== WebSocket.OPEN) return;
      if (Date.now() - this.lastPongAt > HEARTBEAT_INTERVAL_MS + HEARTBEAT_TIMEOUT_MS) {
        this.log.warn('PumpPortal WebSocket heartbeat timed out, forcing reconnect');
        this.ws.terminate();
        return;
      }
      try {
        this.ws.ping();
      } catch {
        // close handler will reconnect
      }
    }, HEARTBEAT_INTERVAL_MS);
  }

  private sendSubscribeMigration(): void {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) return;
    this.ws.send(JSON.stringify({ method: 'subscribeMigration' }));
    this.migrationSubscribed = true;
    this.log.info('Subscribed to PumpPortal migration events');
  }

  private handleMessage(data: WebSocket.Data): void {
    let parsed: unknown;
    try {
      parsed = JSON.parse(data.toString());
    } catch {
      this.log.debug('PumpPortal WS message was not JSON');
      return;
    }

    const event = parsePumpPortalMigrationEvent(parsed);
    if (!event) return;

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
