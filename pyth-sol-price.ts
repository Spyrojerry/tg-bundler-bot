import WebSocket from "ws";
import { createLogger } from "./logger";

const log = createLogger("PYTH-SOL");
const PYTH_SOL_USD_ACCOUNT = "7UVimffxr9ow1uXYxsr4LHAcV58mLzhmwaeKvJ1pjLiE";
const RECONNECT_DELAY_MS = 3_000;

export class PythSolPrice {
  private readonly url: string;
  private ws: WebSocket | null = null;
  private reconnectTimer: NodeJS.Timeout | null = null;
  private closed = false;
  private priceUsd: number | null = null;

  constructor(apiKey: string) {
    this.url = `wss://mainnet.helius-rpc.com/?api-key=${encodeURIComponent(apiKey)}`;
    this.connect();
  }

  getPriceUsd(): number | null {
    return this.priceUsd;
  }

  stop(): void {
    this.closed = true;
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    this.reconnectTimer = null;
    this.ws?.close();
    this.ws = null;
  }

  private connect(): void {
    if (this.closed) return;
    const ws = new WebSocket(this.url);
    this.ws = ws;
    ws.on("open", () => {
      ws.send(JSON.stringify({
        jsonrpc: "2.0",
        id: 1,
        method: "accountSubscribe",
        params: [
          PYTH_SOL_USD_ACCOUNT,
          { commitment: "confirmed", encoding: "base64" },
        ],
      }));
      log.info("Subscribed to Pyth SOL/USD account");
    });
    ws.on("message", (data) => this.handleMessage(data.toString()));
    ws.on("error", (err) => log.warn("Pyth SOL/USD WebSocket error", { error: err.message }));
    ws.on("close", () => {
      if (this.closed || this.reconnectTimer) return;
      this.reconnectTimer = setTimeout(() => {
        this.reconnectTimer = null;
        this.connect();
      }, RECONNECT_DELAY_MS);
    });
  }

  private handleMessage(raw: string): void {
    try {
      const message = JSON.parse(raw) as {
        params?: { result?: { value?: { data?: [string, string] } } };
      };
      const encoded = message.params?.result?.value?.data?.[0];
      if (!encoded) return;
      const data = Buffer.from(encoded, "base64");
      // Legacy Pyth price account: exponent at byte 20, aggregate price at 208.
      if (data.length < 216) return;
      const price = Number(data.readBigInt64LE(208));
      const exponent = data.readInt32LE(20);
      const parsed = price * 10 ** exponent;
      if (Number.isFinite(parsed) && parsed > 0) this.priceUsd = parsed;
    } catch (err) {
      log.warn("Failed to decode Pyth SOL/USD account update", {
        error: err instanceof Error ? err.message : String(err),
      });
    }
  }
}
