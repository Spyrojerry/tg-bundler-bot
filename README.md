# GMGN Bundler Monitor

Production-ready Node.js + TypeScript service that monitors a Solana wallet
for newly purchased tokens and continuously tracks GMGN bundler metrics.

---

## Architecture

```
src/
├── index.ts           Entry point — wires everything together, graceful shutdown
├── config.ts          Env-var loader with strict validation
├── types.ts           All shared TypeScript interfaces
├── logger.ts          Structured, levelled console logger
├── database.ts        SQLite persistence (better-sqlite3, WAL mode)
├── rate-limiter.ts    Global Bottleneck queue + adaptive backoff on 429
├── gmgn-client.ts     GMGN OpenAPI REST client with retry logic
├── scheduler.ts       Single-loop scheduler — no per-token setIntervals
└── wallet-monitor.ts  Solana RPC poller — detects new token purchases
```

### Data flow

```
Solana RPC ──poll──► WalletMonitor
                          │ newToken event (only mints bought AFTER startup)
                          ▼
                      Scheduler.addToken()
                          │ single tick loop (1 s)
                          ▼
                   due tokens, sorted newest-first
                          │
                          ▼
                     RateLimiter (Bottleneck)
                      minTime = 500 ms, maxConcurrent = 1
                      adaptive backoff on 429
                          │
                          ▼
                     GmgnClient.fetchBundlerMetrics()
                      GET /token/security?chain=sol&address=<mint>
                      fallback: GET /token/info?chain=sol&address=<mint>
                          │
                          ▼
                   Database.insertMetrics() + console log
```

---

## GMGN API — Engineering Notes

| Field in API response         | Maps to              | Description                       |
|-------------------------------|----------------------|-----------------------------------|
| `bundler_trader_amount_rate`  | `bundlersPercent`    | Fraction (0–1) of volume from bundler wallets × 100 → % |
| `bundle_num`                  | `bundlersCount`      | Number of distinct bundler wallets |
| `bundler_count`               | `bundlersCount`      | Alternative field name (same value) |
| `bundled_amount_rate`         | `bundledAmountRate`  | Raw rate (fallback field name)    |

**Rate limits:** GMGN does not publish a hard limit. Community observation: ~2 req/s
on free-tier keys. The service defaults to `RATE_LIMIT_MIN_TIME=500` (1 request
every 500 ms) with adaptive doubling on 429.

**Safe token capacity formula:**
```
safeCapacity = floor(MONITOR_INTERVAL / RATE_LIMIT_MIN_TIME)
             = floor(3000 / 500)
             = 6 tokens at nominal 3 s interval
```
If you add a 7th token, the scheduler automatically stretches the effective
interval rather than blowing the rate limit.

---

## Quick Start

### 1 — Prerequisites

```bash
node --version   # 18+
npm  --version   # 8+
```

Get a GMGN API key: https://gmgn.ai/ai  
(Generate an Ed25519 key pair → upload public key → receive API Key)

### 2 — Install

```bash
git clone <this-repo>
cd gmgn-monitor
npm install
```

### 3 — Configure

```bash
cp .env.example .env
```

Edit `.env`:

```dotenv
WALLET_ADDRESS=<your Solana wallet>
GMGN_API_KEY=<your GMGN API key>

# Optional — tune as needed
SOLANA_RPC_URL=https://api.mainnet-beta.solana.com
WALLET_POLL_INTERVAL=5000    # ms between wallet checks
MONITOR_INTERVAL=3000        # target ms between GMGN polls per token
RATE_LIMIT_MIN_TIME=500      # minimum ms between any two GMGN requests
RATE_LIMIT_MAX_CONCURRENT=1  # concurrent GMGN requests
DB_PATH=./data/monitor.db
LOG_LEVEL=info               # debug | info | warn | error
```

### 4 — Build & Run

```bash
npm run build    # compile TypeScript → dist/
npm start        # run compiled service
```

Development (no build step):
```bash
npm run dev      # ts-node src/index.ts
```

---

## Example Output

```
[2026-05-22T12:00:00Z] [INFO ] [MAIN  ] Config { wallet: "ABC...", ... }
[2026-05-22T12:00:01Z] [INFO ] [WALLET] Snapshot taken: 3 existing token(s) — these will NOT be monitored
[2026-05-22T12:00:01Z] [INFO ] [MAIN  ] Service fully started — monitoring wallet for new tokens
[2026-05-22T12:00:01Z] [INFO ] [MAIN  ] Safe token capacity at current rate limit: 6 tokens

# ... user buys a new token ...

[2026-05-22T12:01:05Z] [INFO ] [WALLET] [NEW TOKEN] Mint: TokenMintABC123...  Amount: 1000000
[2026-05-22T12:01:05Z] [INFO ] [MAIN  ] [NEW TOKEN] Mint: TokenMintABC123...
[2026-05-22T12:01:05Z] [INFO ] [SCHED ] Scheduler: tracking TokenMintABC123... (1 active)

[2026-05-22T12:01:06Z] [INFO ] [SCHED ] [MONITOR]  Mint: TokenMin…  Bundlers%: 8.5  Count: 14
{"mint":"TokenMintABC123...","time":"2026-05-22T12:01:06Z","bundlersPercent":8.5,"bundlersCount":14}

[2026-05-22T12:01:09Z] [INFO ] [SCHED ] [MONITOR]  Mint: TokenMin…  Bundlers%: 8.7  Count: 14
{"mint":"TokenMintABC123...","time":"2026-05-22T12:01:09Z","bundlersPercent":8.7,"bundlersCount":14}
```

---

## Database Schema

```sql
-- Tokens detected after startup
CREATE TABLE tokens (
  mint               TEXT    PRIMARY KEY,
  first_seen         TEXT,       -- ISO-8601 UTC
  monitoring_status  TEXT,       -- 'active' | 'paused' | 'stopped'
  detected_at_ms     INTEGER
);

-- One row per GMGN poll
CREATE TABLE bundler_metrics (
  id                   INTEGER PRIMARY KEY AUTOINCREMENT,
  mint                 TEXT,
  timestamp            TEXT,       -- ISO-8601 UTC
  bundlers_percent     REAL,       -- 0–100 (%)
  bundlers_count       INTEGER,
  bundled_amount_rate  REAL,       -- 0–1 raw rate
  raw_data             TEXT        -- full API payload (JSON)
);
```

---

## Rate-Limit Safety — How It Works

| Scenario                        | Behaviour                                      |
|---------------------------------|------------------------------------------------|
| Normal operation                | 1 request / 500 ms (Bottleneck enforced)       |
| 429 received                    | minTime doubles (500 → 1000 → 2000 → …)        |
| `Retry-After` header present    | minTime set to that value                       |
| 5 consecutive successes         | minTime steps back down × 0.75                 |
| minTime fully recovered         | Back to baseline (500 ms)                      |
| Token count > safeCapacity      | Effective interval stretched, warning logged   |
| Two requests for same token     | Blocked by `pendingRequest` flag               |

---

## Scaling Beyond 6 Tokens

To monitor more tokens while keeping a 3 s interval, you have two levers:

1. **Lower `RATE_LIMIT_MIN_TIME`** — only safe if your GMGN plan allows it.  
   e.g. minTime=200 → safeCapacity = floor(3000/200) = **15 tokens**

2. **Raise `MONITOR_INTERVAL`** — accepts a slower refresh.  
   e.g. MONITOR_INTERVAL=10000 → safeCapacity = floor(10000/500) = **20 tokens**

The scheduler logs a warning when you exceed safe capacity and automatically
stretches intervals — it will never knowingly exceed the rate limit.

---

## Stopping

Press `Ctrl+C` or send `SIGTERM`. The service:
- Stops the wallet poller
- Stops the scheduler
- Drains in-flight GMGN requests
- Closes the SQLite database cleanly
