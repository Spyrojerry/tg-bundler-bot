// ─────────────────────────────────────────────────────────────────────────────
//  helius-enhanced-ws.ts  —  Raw WebSocket JSON-RPC client for Helius's
//  Developer-plan-only `transactionSubscribe` extension.
//
//  @solana/web3.js's `Connection` class does not expose `transactionSubscribe`
//  — it's a Helius-only extension of the standard Solana pubsub protocol, not
//  part of web3.js. This is a small hand-rolled client that speaks the same
//  subscribe/notify/unsubscribe JSON-RPC convention as the standard
//  `logsSubscribe`/`accountSubscribe` methods `Connection` already uses
//  under the hood, just for `transactionSubscribe`/`transactionUnsubscribe`.
//
//  IMPORTANT: `transactionSubscribe` requires a Developer-plan (or higher)
//  Helius API key. Only one key in this project's pool is confirmed to be on
//  that plan (INSIDER_HELIUS_API_KEY, see config.insiderHeliusApiKey) — every
//  call site that wants to use this client MUST be constructed with that key
//  specifically, not with whichever key that call site's REST fallback pool
//  happens to use.
//
//  Design:
//   - One WebSocket connection per process (shared across Insider and
//     funder-first) watching addresses via
//     `transactionSubscribe`.
//   - Single-address `watch()` and multi-address `watchMulti()` both supported;
//     the latter batches `accountInclude: [addr1, addr2, …]` into one server
//     subscription to reduce subscribe count and reconnect churn.
//   - Automatic reconnect with exponential backoff + jitter, and full
//     re-subscription of every still-active watch on reconnect. A dropped
//     WS connection is the main failure mode once REST polling is demoted to
//     a rare backstop, so this path is deliberately defensive.
//   - A ping/pong heartbeat detects "dead" connections that never emit a
//     close event (common with some proxies/load balancers).
//   - Every notification is normalized (via tx-normalizer.ts) into the same
//     HeliusTransaction shape the rest of the codebase already consumes.
//
//  NOT runtime-verified against a live Helius connection in the environment
//  this was authored in (no verified network egress to
//  wss://mainnet.helius-rpc.com from that sandbox). If notifications aren't
//  arriving after deploying, check for `[HELIUS-WS]` warn/error logs first —
//  they're intentionally verbose around connect/subscribe/parse failures.
// ─────────────────────────────────────────────────────────────────────────────

import WebSocket from 'ws';
import { createLogger, Logger } from './logger';
import { HeliusTransaction } from './helius-client';
import {
  normalizeEnhancedWsTransaction,
  RawEnhancedWsTransactionResult,
} from './tx-normalizer';

const BASE_RECONNECT_DELAY_MS = 1_000;
const MAX_RECONNECT_DELAY_MS = 30_000;
const HEARTBEAT_INTERVAL_MS = 20_000;
const HEARTBEAT_TIMEOUT_MS = 12_000;
const RPC_ACK_TIMEOUT_MS = 15_000;

type TxCallback = (tx: HeliusTransaction) => void;
type MultiTxCallback = (matchedAddress: string, tx: HeliusTransaction) => void;

interface Watch {
  localId: number;
  /** One entry for single-address watches; N entries for `watchMulti`. */
  addresses: string[];
  addressSet: Set<string>;
  callback: TxCallback | MultiTxCallback;
  isMulti: boolean;
  serverSubId: number | null;
}

interface PendingRpc {
  kind: 'subscribe' | 'unsubscribe';
  localId: number;
  resolve: (value: unknown) => void;
  reject: (err: Error) => void;
  timer: NodeJS.Timeout;
}

export class HeliusEnhancedWsClient {
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
  private nextLocalId = 1;
  private nextRpcId = 1;
  private readonly watches = new Map<number, Watch>();
  private readonly watchesByServerSubId = new Map<number, Watch>();
  private readonly pendingRpc = new Map<number, PendingRpc>();

  constructor(apiKey: string, label = 'Helius Enhanced WS') {
    this.url = `wss://mainnet.helius-rpc.com/?api-key=${apiKey}`;
    this.log = createLogger(label.toUpperCase());
    this.connect();
  }

  /** Watches one address for any confirmed transaction touching it. Returns a local handle; the underlying `transactionSubscribe` call happens immediately if connected, or once the connection (re)establishes otherwise. Mirrors `connection.onLogs(pubkey, cb)`. */
  watch(address: string, callback: TxCallback): number {
    const localId = this.nextLocalId;
    this.nextLocalId += 1;
    const watchEntry: Watch = {
      localId,
      addresses: [address],
      addressSet: new Set([address]),
      callback,
      isMulti: false,
      serverSubId: null,
    };
    this.watches.set(localId, watchEntry);
    if (this.connected) this.sendSubscribe(watchEntry);
    return localId;
  }

  /**
   * Watches several addresses via one `transactionSubscribe` (`accountInclude`
   * array). The callback receives the matched address from the batch plus the
   * normalized tx. Use `updateWatchAddresses` to change the set in place.
   */
  watchMulti(addresses: readonly string[], callback: MultiTxCallback): number {
    const unique = [...new Set(addresses.filter(Boolean))];
    const localId = this.nextLocalId;
    this.nextLocalId += 1;
    const watchEntry: Watch = {
      localId,
      addresses: unique,
      addressSet: new Set(unique),
      callback,
      isMulti: true,
      serverSubId: null,
    };
    this.watches.set(localId, watchEntry);
    if (this.connected && unique.length > 0) this.sendSubscribe(watchEntry);
    return localId;
  }

  /** Replaces the address set on an existing `watchMulti` subscription. */
  async updateWatchAddresses(
    localId: number,
    addresses: readonly string[],
  ): Promise<void> {
    const watchEntry = this.watches.get(localId);
    if (!watchEntry || !watchEntry.isMulti) return;
    const unique = [...new Set(addresses.filter(Boolean))];
    watchEntry.addresses = unique;
    watchEntry.addressSet = new Set(unique);
    if (watchEntry.serverSubId !== null) {
      const oldSubId = watchEntry.serverSubId;
      watchEntry.serverSubId = null;
      this.watchesByServerSubId.delete(oldSubId);
      if (this.connected) {
        await this.sendUnsubscribe(oldSubId).catch(() => undefined);
      }
    }
    if (this.connected && unique.length > 0) {
      this.sendSubscribe(watchEntry);
    }
  }

  /** Stops watching a previously-`watch()`ed address. Mirrors `connection.removeOnLogsListener(id)`. */
  async unwatch(localId: number): Promise<void> {
    const watchEntry = this.watches.get(localId);
    if (!watchEntry) return;
    this.watches.delete(localId);
    if (watchEntry.serverSubId !== null) {
      this.watchesByServerSubId.delete(watchEntry.serverSubId);
      if (this.connected) {
        await this.sendUnsubscribe(watchEntry.serverSubId).catch((err) => {
          this.log.debug('transactionUnsubscribe failed (connection likely already gone)', {
            addresses: watchEntry.addresses,
            error: err instanceof Error ? err.message : String(err),
          });
        });
      }
    }
  }

  /** Closes the connection and stops all reconnect attempts. Call on process shutdown only — there's normally no need to call this mid-run. */
  close(): void {
    this.closedByUser = true;
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);
    this.ws?.close();
  }

  get isConnected(): boolean {
    return this.connected;
  }

  private connect(): void {
    if (this.connecting || this.closedByUser) return;
    this.connecting = true;
    let ws: WebSocket;
    try {
      ws = new WebSocket(this.url);
    } catch (err) {
      this.connecting = false;
      this.log.error('Failed to construct WebSocket', {
        error: err instanceof Error ? err.message : String(err),
      });
      this.scheduleReconnect();
      return;
    }
    this.ws = ws;

    ws.on('open', () => {
      this.connecting = false;
      this.connected = true;
      this.reconnectAttempts = 0;
      this.lastPongAt = Date.now();
      this.log.info('Connected to Helius Enhanced WebSocket', {
        activeWatches: this.watches.size,
      });
      this.startHeartbeat();
      this.resubscribeAll();
    });

    ws.on('message', (data: WebSocket.Data) => {
      this.handleMessage(data);
    });

    ws.on('pong', () => {
      this.lastPongAt = Date.now();
    });

    ws.on('error', (err: Error) => {
      this.log.warn('Helius Enhanced WebSocket error', { error: err.message });
    });

    ws.on('close', (code: number, reason: Buffer) => {
      this.connecting = false;
      this.connected = false;
      if (this.heartbeatTimer) {
        clearInterval(this.heartbeatTimer);
        this.heartbeatTimer = null;
      }
      this.failAllPendingRpc(new Error('WebSocket closed'));
      for (const watchEntry of this.watches.values()) watchEntry.serverSubId = null;
      this.watchesByServerSubId.clear();
      if (!this.closedByUser) {
        this.log.warn('Helius Enhanced WebSocket closed, will reconnect', {
          code,
          reason: reason?.toString?.() || undefined,
        });
        this.scheduleReconnect();
      }
    });
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
        this.log.warn('Helius Enhanced WebSocket heartbeat timed out, forcing reconnect');
        this.ws.terminate();
        return;
      }
      try {
        this.ws.ping();
      } catch {
        // socket already going away; the close handler will run and reconnect.
      }
    }, HEARTBEAT_INTERVAL_MS);
  }

  private resubscribeAll(): void {
    for (const watchEntry of this.watches.values()) {
      if (watchEntry.addresses.length > 0) {
        this.sendSubscribe(watchEntry);
      }
    }
  }

  private sendSubscribe(watchEntry: Watch): void {
    if (watchEntry.addresses.length === 0) return;
    const rpcId = this.nextRpcId;
    this.nextRpcId += 1;
    const request = {
      jsonrpc: '2.0',
      id: rpcId,
      method: 'transactionSubscribe',
      params: [
        { accountInclude: watchEntry.addresses, failed: false, vote: false },
        {
          commitment: 'processed',
          encoding: 'jsonParsed',
          transactionDetails: 'full',
          maxSupportedTransactionVersion: 0,
        },
      ],
    };
    this.sendRpc(rpcId, request, 'subscribe', watchEntry.localId).catch((err) => {
      this.log.warn('transactionSubscribe failed', {
        addresses: watchEntry.addresses,
        error: err instanceof Error ? err.message : String(err),
      });
    });
  }

  private getTouchedAddresses(
    tx: HeliusTransaction,
    addressSet: Set<string>,
  ): string[] {
    const touched = new Set<string>();
    for (const entry of tx.accountData ?? []) {
      if (addressSet.has(entry.account)) touched.add(entry.account);
    }
    for (const transfer of tx.nativeTransfers ?? []) {
      if (transfer.fromUserAccount && addressSet.has(transfer.fromUserAccount)) {
        touched.add(transfer.fromUserAccount);
      }
      if (transfer.toUserAccount && addressSet.has(transfer.toUserAccount)) {
        touched.add(transfer.toUserAccount);
      }
    }
    for (const transfer of tx.tokenTransfers ?? []) {
      if (transfer.fromUserAccount && addressSet.has(transfer.fromUserAccount)) {
        touched.add(transfer.fromUserAccount);
      }
      if (transfer.toUserAccount && addressSet.has(transfer.toUserAccount)) {
        touched.add(transfer.toUserAccount);
      }
    }
    if (tx.feePayer && addressSet.has(tx.feePayer)) touched.add(tx.feePayer);
    return [...touched];
  }

  private async sendUnsubscribe(serverSubId: number): Promise<void> {
    const rpcId = this.nextRpcId;
    this.nextRpcId += 1;
    const request = {
      jsonrpc: '2.0',
      id: rpcId,
      method: 'transactionUnsubscribe',
      params: [serverSubId],
    };
    await this.sendRpc(rpcId, request, 'unsubscribe', -1);
  }

  private sendRpc(
    rpcId: number,
    request: unknown,
    kind: 'subscribe' | 'unsubscribe',
    localId: number,
  ): Promise<unknown> {
    return new Promise((resolve, reject) => {
      if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
        reject(new Error('WebSocket not open'));
        return;
      }
      const timer = setTimeout(() => {
        this.pendingRpc.delete(rpcId);
        reject(new Error(`${request && (request as any).method} timed out`));
      }, RPC_ACK_TIMEOUT_MS);
      this.pendingRpc.set(rpcId, { kind, localId, resolve, reject, timer });
      this.ws.send(JSON.stringify(request), (err) => {
        if (err) {
          clearTimeout(timer);
          this.pendingRpc.delete(rpcId);
          reject(err);
        }
      });
    });
  }

  private failAllPendingRpc(err: Error): void {
    for (const [rpcId, pending] of this.pendingRpc) {
      clearTimeout(pending.timer);
      pending.reject(err);
      this.pendingRpc.delete(rpcId);
    }
  }

  private handleMessage(data: WebSocket.Data): void {
    let parsed: any;
    try {
      parsed = JSON.parse(data.toString());
    } catch (err) {
      this.log.warn('Failed to parse Helius Enhanced WS message as JSON', {
        error: err instanceof Error ? err.message : String(err),
      });
      return;
    }

    if (typeof parsed.id === 'number' && this.pendingRpc.has(parsed.id)) {
      const pending = this.pendingRpc.get(parsed.id)!;
      this.pendingRpc.delete(parsed.id);
      clearTimeout(pending.timer);
      if (parsed.error) {
        pending.reject(new Error(parsed.error.message ?? JSON.stringify(parsed.error)));
        return;
      }
      if (pending.kind === 'subscribe') {
        const watchEntry = this.watches.get(pending.localId);
        if (watchEntry && typeof parsed.result === 'number') {
          watchEntry.serverSubId = parsed.result;
          this.watchesByServerSubId.set(parsed.result, watchEntry);
        }
      }
      pending.resolve(parsed.result);
      return;
    }

    if (parsed.method === 'transactionNotification') {
      const subscription = parsed.params?.subscription;
      const result: RawEnhancedWsTransactionResult | undefined = parsed.params?.result;
      if (typeof subscription !== 'number' || !result) return;
      const watchEntry = this.watchesByServerSubId.get(subscription);
      if (!watchEntry) return;
      const tx = normalizeEnhancedWsTransaction(result);
      if (!tx) return;
      try {
        if (watchEntry.isMulti) {
          const multiCb = watchEntry.callback as MultiTxCallback;
          for (const matched of this.getTouchedAddresses(tx, watchEntry.addressSet)) {
            multiCb(matched, tx);
          }
        } else {
          (watchEntry.callback as TxCallback)(tx);
        }
      } catch (err) {
        this.log.error('transactionSubscribe callback threw', {
          addresses: watchEntry.addresses,
          error: err instanceof Error ? err.message : String(err),
        });
      }
    }
  }
}
