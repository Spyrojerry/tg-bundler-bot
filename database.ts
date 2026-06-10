// ─────────────────────────────────────────────────────────────────────────────
//  database.ts  —  SQLite persistence via sql.js (pure JavaScript, no native
//                  compilation required — works on Windows without VS Build Tools)
//
//  sql.js keeps the database in memory and flushes it to disk as a binary file
//  on every write.  For a monitor process that runs for minutes at a time and
//  writes ~30 rows per token this is perfectly fast.
// ─────────────────────────────────────────────────────────────────────────────

import initSqlJs, { Database as SqlJsDatabase } from "sql.js";
import * as fs from "fs";
import * as path from "path";
import {
  BundlerMetrics,
  MonitoringStatus,
  TrackedToken,
  WalletFilterProfileSettings,
  WalletFilterSettings,
} from "./types";
import { createLogger } from "./logger";

const log = createLogger("DB");

export const DEFAULT_WALLET_FILTER_PROFILE_SETTINGS: WalletFilterProfileSettings =
  {
    applyAtSample: 20,
    minBundlersPercent: null,
    maxBundlersPercent: null,
    minBundlersCount: null,
    maxBundlersCount: null,
    maxPctAboveValue: null,
    maxPctAboveOccurrences: null,
    maxPctBelowValue: null,
    maxPctBelowOccurrences: null,
    sellIfFirstThreePctZero: false,
    sellIfNoTeenOrTwentyPct: false,
    sellIfNoPctAbove50: false,
  };

export const DEFAULT_WALLET_FILTER_SETTINGS: WalletFilterSettings = {
  ...DEFAULT_WALLET_FILTER_PROFILE_SETTINGS,
  minBundlersCountChange: 20,
  reverseBuySellTriggerEnabled: false,
  minSolBuy: null,
};

// ── Schema ────────────────────────────────────────────────────────────────────

const SCHEMA = `
CREATE TABLE IF NOT EXISTS tokens (
  wallet_address     TEXT    NOT NULL DEFAULT '',
  mint               TEXT    NOT NULL,
  first_seen         TEXT    NOT NULL,
  monitoring_status  TEXT    NOT NULL DEFAULT 'active',
  detected_at_ms     INTEGER NOT NULL,
  buy_sol            REAL,
  PRIMARY KEY (wallet_address, mint)
);

CREATE TABLE IF NOT EXISTS bought_mints (
  trading_wallet  TEXT NOT NULL,
  mint            TEXT NOT NULL,
  bought_at       TEXT NOT NULL,
  PRIMARY KEY (trading_wallet, mint)
);

CREATE TABLE IF NOT EXISTS monitored_wallets (
  address     TEXT PRIMARY KEY NOT NULL,
  added_at    TEXT NOT NULL,
  status      TEXT NOT NULL DEFAULT 'active'
);
CREATE TABLE IF NOT EXISTS reverse_buy_wallets (
  address     TEXT PRIMARY KEY NOT NULL,
  added_at    TEXT NOT NULL,
  status      TEXT NOT NULL DEFAULT 'active'
);

CREATE TABLE IF NOT EXISTS wallet_settings (
  wallet_address   TEXT PRIMARY KEY NOT NULL,
  filter_settings  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS bundler_metrics (
  id                   INTEGER PRIMARY KEY AUTOINCREMENT,
  wallet_address       TEXT    NOT NULL DEFAULT '',
  mint                 TEXT    NOT NULL,
  timestamp            TEXT    NOT NULL,
  bundlers_percent     REAL,
  bundlers_count       INTEGER,
  initial_base_reserve REAL,
  top_wallets          INTEGER,
  top_10_holder_rate   REAL,
  bundled_amount_rate  REAL,
  raw_data             TEXT
);

CREATE INDEX IF NOT EXISTS idx_bm_mint      ON bundler_metrics(mint);
CREATE INDEX IF NOT EXISTS idx_bm_timestamp ON bundler_metrics(timestamp);

CREATE TABLE IF NOT EXISTS early_bundler_positions (
  id                   INTEGER PRIMARY KEY AUTOINCREMENT,
  trading_wallet       TEXT    NOT NULL,
  mint                 TEXT    NOT NULL,
  token_amount         REAL    NOT NULL,
  buy_sol              REAL,
  status               TEXT    NOT NULL DEFAULT 'active',
  created_at           TEXT    NOT NULL,
  exited_at            TEXT,
  exit_reason          TEXT,
  UNIQUE(trading_wallet, mint)
);

CREATE TABLE IF NOT EXISTS early_bundler_wallets (
  id                   INTEGER PRIMARY KEY AUTOINCREMENT,
  position_id          INTEGER NOT NULL,
  wallet_address       TEXT    NOT NULL,
  initial_token_amount REAL    NOT NULL,
  signature            TEXT    NOT NULL,
  slot                 INTEGER NOT NULL,
  timestamp            INTEGER NOT NULL,
  status               TEXT    NOT NULL DEFAULT 'monitoring',
  total_sold_amount    REAL    NOT NULL DEFAULT 0,
  FOREIGN KEY (position_id) REFERENCES early_bundler_positions(id) ON DELETE CASCADE,
  UNIQUE(position_id, wallet_address)
);

CREATE TABLE IF NOT EXISTS bundler_wallet_sells (
  id                   INTEGER PRIMARY KEY AUTOINCREMENT,
  bundler_wallet_id    INTEGER NOT NULL,
  signature            TEXT    NOT NULL,
  token_amount_sold    REAL    NOT NULL,
  slot                 INTEGER NOT NULL,
  timestamp            INTEGER NOT NULL,
  FOREIGN KEY (bundler_wallet_id) REFERENCES early_bundler_wallets(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_ebp_status ON early_bundler_positions(status);
CREATE INDEX IF NOT EXISTS idx_ebw_position ON early_bundler_wallets(position_id);
CREATE INDEX IF NOT EXISTS idx_bws_bundler ON bundler_wallet_sells(bundler_wallet_id);
`;

// ── Row shapes returned by sql.js ─────────────────────────────────────────────

interface TokenRow {
  mint: string;
  wallet_address: string;
  first_seen: string;
  monitoring_status: string;
  detected_at_ms: number;
  buy_sol: number | null;
}

interface MetricRow {
  id: number;
  wallet_address: string;
  mint: string;
  timestamp: string;
  bundlers_percent: number | null;
  bundlers_count: number | null;
  initial_base_reserve: number | null;
  top_wallets: number | null;
  top_10_holder_rate: number | null;
  bundled_amount_rate: number | null;
  raw_data: string | null;
}

// ── MonitorDatabase ───────────────────────────────────────────────────────────

export class MonitorDatabase {
  private db!: SqlJsDatabase;
  private readonly dbPath: string;

  // sql.js init is async; use the static factory instead of `new`
  private constructor(dbPath: string) {
    this.dbPath = dbPath;
  }

  static async create(dbPath: string): Promise<MonitorDatabase> {
    const instance = new MonitorDatabase(dbPath);
    await instance.init();
    return instance;
  }

  private async init(): Promise<void> {
    const dir = path.dirname(this.dbPath);
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });

    const SQL = await initSqlJs();

    // Load existing DB file if present, otherwise start fresh
    if (fs.existsSync(this.dbPath)) {
      const fileBuffer = fs.readFileSync(this.dbPath);
      this.db = new SQL.Database(fileBuffer);
      log.info(`Database loaded from ${path.resolve(this.dbPath)}`);
    } else {
      this.db = new SQL.Database();
      log.info(`New database created at ${path.resolve(this.dbPath)}`);
    }

    this.db.run(SCHEMA);
    this.migrate();
    this.persist(); // write initial schema to disk
  }

  private migrate(): void {
    this.migrateTokensTable();

    const tokenColumns = this.query<{ name: string }>(
      `PRAGMA table_info(tokens)`,
    );
    if (!tokenColumns.some((c) => c.name === "wallet_address")) {
      this.db.run(
        `ALTER TABLE tokens ADD COLUMN wallet_address TEXT NOT NULL DEFAULT ''`,
      );
    }
    if (!tokenColumns.some((c) => c.name === "buy_sol")) {
      this.db.run(`ALTER TABLE tokens ADD COLUMN buy_sol REAL`);
    }
    const metricColumns = this.query<{ name: string }>(
      `PRAGMA table_info(bundler_metrics)`,
    );
    if (!metricColumns.some((c) => c.name === "wallet_address")) {
      this.db.run(
        `ALTER TABLE bundler_metrics ADD COLUMN wallet_address TEXT NOT NULL DEFAULT ''`,
      );
    }
    if (!metricColumns.some((c) => c.name === "initial_base_reserve")) {
      this.db.run(
        `ALTER TABLE bundler_metrics ADD COLUMN initial_base_reserve REAL`,
      );
    }
    if (!metricColumns.some((c) => c.name === "top_wallets")) {
      this.db.run(`ALTER TABLE bundler_metrics ADD COLUMN top_wallets INTEGER`);
    }
    if (!metricColumns.some((c) => c.name === "top_10_holder_rate")) {
      this.db.run(
        `ALTER TABLE bundler_metrics ADD COLUMN top_10_holder_rate REAL`,
      );
    }
  }

  private migrateTokensTable(): void {
    const columns = this.query<{ name: string; pk: number }>(
      `PRAGMA table_info(tokens)`,
    );
    const walletPk = columns.find((c) => c.name === "wallet_address")?.pk ?? 0;
    const mintPk = columns.find((c) => c.name === "mint")?.pk ?? 0;

    if (walletPk === 1 && mintPk === 2) return;

    this.db.run("BEGIN TRANSACTION");
    try {
      this.db.run(`
        CREATE TABLE IF NOT EXISTS tokens_new (
          wallet_address     TEXT    NOT NULL DEFAULT '',
          mint               TEXT    NOT NULL,
          first_seen         TEXT    NOT NULL,
          monitoring_status  TEXT    NOT NULL DEFAULT 'active',
          detected_at_ms     INTEGER NOT NULL,
          buy_sol            REAL,
          PRIMARY KEY (wallet_address, mint)
        )
      `);

      const hasWalletAddress = columns.some((c) => c.name === "wallet_address");
      const selectWalletAddress = hasWalletAddress ? "wallet_address" : "''";
      const hasBuySol = columns.some((c) => c.name === "buy_sol");
      const selectBuySol = hasBuySol ? "buy_sol" : "NULL";

      this.db.run(`
        INSERT OR IGNORE INTO tokens_new
          (wallet_address, mint, first_seen, monitoring_status, detected_at_ms, buy_sol)
        SELECT ${selectWalletAddress}, mint, first_seen, monitoring_status, detected_at_ms, ${selectBuySol}
        FROM tokens
      `);

      this.db.run("DROP TABLE tokens");
      this.db.run("ALTER TABLE tokens_new RENAME TO tokens");
      this.db.run("COMMIT");
      log.info("Migrated tokens table to wallet+mint primary key");
    } catch (err) {
      this.db.run("ROLLBACK");
      throw err;
    }
  }

  // ── Persistence ────────────────────────────────────────────────────────────

  /** Write the in-memory DB to the file on disk. Called after every mutation. */
  private persist(): void {
    const data = this.db.export();
    fs.writeFileSync(this.dbPath, Buffer.from(data));
  }

  // ── Helper: query → typed rows ─────────────────────────────────────────────

  private query<T>(sql: string, params: (string | number | null)[] = []): T[] {
    const stmt = this.db.prepare(sql);
    stmt.bind(params);
    const rows: T[] = [];
    while (stmt.step()) {
      rows.push(stmt.getAsObject() as T);
    }
    stmt.free();
    return rows;
  }

  private run(sql: string, params: (string | number | null)[] = []): void {
    this.db.run(sql, params);
    this.persist();
  }

  // ── tokens table ───────────────────────────────────────────────────────────

  insertToken(token: TrackedToken): void {
    this.run(
      `INSERT OR IGNORE INTO tokens
         (mint, wallet_address, first_seen, monitoring_status, detected_at_ms, buy_sol)
       VALUES (?, ?, ?, ?, ?, ?)`,
      [
        token.mint,
        token.walletAddress,
        token.firstSeen,
        token.monitoringStatus,
        token.detectedAt,
        token.buySol,
      ],
    );
  }

  updateTokenStatus(
    walletAddress: string,
    mint: string,
    status: MonitoringStatus,
  ): void {
    this.run(
      `UPDATE tokens SET monitoring_status = ? WHERE wallet_address = ? AND mint = ?`,
      [status, walletAddress, mint],
    );
  }

  getAllActiveTokens(): TrackedToken[] {
    const rows = this.query<TokenRow>(
      `SELECT * FROM tokens WHERE monitoring_status = 'active'`,
    );
    return rows.map((r) => ({
      mint: r.mint,
      walletAddress: r.wallet_address,
      firstSeen: r.first_seen,
      monitoringStatus: r.monitoring_status as MonitoringStatus,
      detectedAt: r.detected_at_ms,
      buySol: r.buy_sol,
    }));
  }

  getToken(walletAddress: string, mint: string): TrackedToken | null {
    const rows = this.query<TokenRow>(
      `SELECT * FROM tokens WHERE wallet_address = ? AND mint = ?`,
      [walletAddress, mint],
    );
    const row = rows[0];
    if (!row) return null;
    return {
      mint: row.mint,
      walletAddress: row.wallet_address,
      firstSeen: row.first_seen,
      monitoringStatus: row.monitoring_status as MonitoringStatus,
      detectedAt: row.detected_at_ms,
      buySol: row.buy_sol,
    };
  }

  tokenExists(walletAddress: string, mint: string): boolean {
    const rows = this.query<{ found: number }>(
      `SELECT 1 AS found FROM tokens WHERE wallet_address = ? AND mint = ?`,
      [walletAddress, mint],
    );
    return rows.length > 0;
  }

  addBoughtMint(tradingWallet: string, mint: string): void {
    this.run(
      `INSERT OR IGNORE INTO bought_mints (trading_wallet, mint, bought_at)
     VALUES (?, ?, ?)`,
      [tradingWallet, mint, new Date().toISOString()],
    );
  }

  getBoughtMints(tradingWallet: string): Set<string> {
    const rows = this.query<{ mint: string }>(
      `SELECT mint FROM bought_mints WHERE trading_wallet = ?`,
      [tradingWallet],
    );
    return new Set(rows.map((r) => r.mint));
  }

  addWallet(address: string): void {
    this.run(
      `INSERT INTO monitored_wallets (address, added_at, status)
       VALUES (?, ?, 'active')
       ON CONFLICT(address) DO UPDATE SET status = 'active'`,
      [address, new Date().toISOString()],
    );
  }

  removeWallet(address: string): void {
    this.run(
      `UPDATE monitored_wallets SET status = 'stopped' WHERE address = ?`,
      [address],
    );
  }

  getActiveWallets(): string[] {
    const rows = this.query<{ address: string }>(
      `SELECT address FROM monitored_wallets WHERE status = 'active' ORDER BY added_at ASC`,
    );
    return rows.map((r) => r.address);
  }

  addReverseBuyWallet(address: string): void {
    this.run(
      `INSERT INTO reverse_buy_wallets (address, added_at, status)
       VALUES (?, ?, 'active')
       ON CONFLICT(address) DO UPDATE SET status = 'active'`,
      [address, new Date().toISOString()],
    );
  }

  removeReverseBuyWallet(address: string): void {
    this.run(
      `UPDATE reverse_buy_wallets SET status = 'stopped' WHERE address = ?`,
      [address],
    );
  }

  getActiveReverseBuyWallets(): string[] {
    const rows = this.query<{ address: string }>(
      `SELECT address FROM reverse_buy_wallets WHERE status = 'active' ORDER BY added_at ASC`,
    );
    return rows.map((r) => r.address);
  }

  isReverseBuyWallet(address: string): boolean {
    const rows = this.query<{ found: number }>(
      `SELECT 1 AS found FROM reverse_buy_wallets WHERE address = ? AND status = 'active'`,
      [address],
    );
    return rows.length > 0;
  }

  // ── bundler_metrics table ──────────────────────────────────────────────────

  insertMetrics(m: BundlerMetrics): void {
    this.run(
      `INSERT INTO bundler_metrics
         (wallet_address, mint, timestamp, bundlers_percent, bundlers_count, initial_base_reserve, top_wallets, top_10_holder_rate, bundled_amount_rate, raw_data)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        m.walletAddress ?? "",
        m.mint,
        m.timestamp,
        m.bundlersPercent ?? null,
        m.bundlersCount ?? null,
        m.initialBaseReserve ?? null,
        m.topWallets ?? null,
        m.top10HolderRate ?? null,
        m.bundledAmountRate ?? null,
        m.rawData ?? null,
      ],
    );
  }

  /**
   * Returns up to `limit` samples for a mint, newest-first.
   * Pass a large limit (e.g. 10_000) to get all samples for summary computation.
   */
  getLatestMetrics(mint: string, limit = 10): BundlerMetrics[] {
    const rows = this.query<MetricRow>(
      `SELECT * FROM bundler_metrics WHERE mint = ?
       ORDER BY timestamp DESC LIMIT ?`,
      [mint, limit],
    );
    return rows.map((r) => ({
      id: r.id,
      mint: r.mint,
      timestamp: r.timestamp,
      bundlersPercent: r.bundlers_percent,
      bundlersCount: r.bundlers_count,
      initialBaseReserve: r.initial_base_reserve,
      topWallets: r.top_wallets,
      top10HolderRate: r.top_10_holder_rate,
      bundledAmountRate: r.bundled_amount_rate,
      rawData: r.raw_data ?? undefined,
    }));
  }

  getLatestMetricsForWallet(
    walletAddress: string,
    mint: string,
    limit = 10,
  ): BundlerMetrics[] {
    const rows = this.query<MetricRow>(
      `SELECT * FROM bundler_metrics WHERE wallet_address = ? AND mint = ?
       ORDER BY timestamp DESC LIMIT ?`,
      [walletAddress, mint, limit],
    );
    return rows.map((r) => ({
      id: r.id,
      walletAddress: r.wallet_address,
      mint: r.mint,
      timestamp: r.timestamp,
      bundlersPercent: r.bundlers_percent,
      bundlersCount: r.bundlers_count,
      initialBaseReserve: r.initial_base_reserve,
      topWallets: r.top_wallets,
      top10HolderRate: r.top_10_holder_rate,
      bundledAmountRate: r.bundled_amount_rate,
      rawData: r.raw_data ?? undefined,
    }));
  }

  metricsCount(mint: string): number {
    const rows = this.query<{ cnt: number }>(
      `SELECT COUNT(*) AS cnt FROM bundler_metrics WHERE mint = ?`,
      [mint],
    );
    return rows[0]?.cnt ?? 0;
  }

  metricsCountForWallet(walletAddress: string, mint: string): number {
    const rows = this.query<{ cnt: number }>(
      `SELECT COUNT(*) AS cnt FROM bundler_metrics WHERE wallet_address = ? AND mint = ?`,
      [walletAddress, mint],
    );
    return rows[0]?.cnt ?? 0;
  }

  tokenCountForWallet(walletAddress: string): number {
    const rows = this.query<{ cnt: number }>(
      `SELECT COUNT(*) AS cnt FROM tokens WHERE wallet_address = ?`,
      [walletAddress],
    );
    return rows[0]?.cnt ?? 0;
  }

  sampleCountForWallet(walletAddress: string): number {
    const rows = this.query<{ cnt: number }>(
      `SELECT COUNT(*) AS cnt FROM bundler_metrics WHERE wallet_address = ?`,
      [walletAddress],
    );
    return rows[0]?.cnt ?? 0;
  }

  getWalletSettings(walletAddress: string): WalletFilterSettings {
    const rows = this.query<{ filter_settings: string }>(
      `SELECT filter_settings FROM wallet_settings WHERE wallet_address = ?`,
      [walletAddress],
    );
    if (rows.length === 0) return this.normalizeWalletSettings({});

    try {
      const parsed = JSON.parse(
        rows[0].filter_settings,
      ) as Partial<WalletFilterSettings>;
      return this.normalizeWalletSettings(parsed);
    } catch {
      return this.normalizeWalletSettings({});
    }
  }

  private normalizeWalletSettings(
    parsed: Partial<WalletFilterSettings>,
  ): WalletFilterSettings {
    const legacyProfile: WalletFilterProfileSettings = {
      ...DEFAULT_WALLET_FILTER_PROFILE_SETTINGS,
      applyAtSample:
        parsed.applyAtSample ??
        DEFAULT_WALLET_FILTER_PROFILE_SETTINGS.applyAtSample,
      minBundlersPercent:
        parsed.minBundlersPercent ??
        DEFAULT_WALLET_FILTER_PROFILE_SETTINGS.minBundlersPercent,
      maxBundlersPercent:
        parsed.maxBundlersPercent ??
        DEFAULT_WALLET_FILTER_PROFILE_SETTINGS.maxBundlersPercent,
      minBundlersCount:
        parsed.minBundlersCount ??
        DEFAULT_WALLET_FILTER_PROFILE_SETTINGS.minBundlersCount,
      maxBundlersCount:
        parsed.maxBundlersCount ??
        DEFAULT_WALLET_FILTER_PROFILE_SETTINGS.maxBundlersCount,
      maxPctAboveValue:
        parsed.maxPctAboveValue ??
        DEFAULT_WALLET_FILTER_PROFILE_SETTINGS.maxPctAboveValue,
      maxPctAboveOccurrences:
        parsed.maxPctAboveOccurrences ??
        DEFAULT_WALLET_FILTER_PROFILE_SETTINGS.maxPctAboveOccurrences,
      maxPctBelowValue:
        parsed.maxPctBelowValue ??
        DEFAULT_WALLET_FILTER_PROFILE_SETTINGS.maxPctBelowValue,
      maxPctBelowOccurrences:
        parsed.maxPctBelowOccurrences ??
        DEFAULT_WALLET_FILTER_PROFILE_SETTINGS.maxPctBelowOccurrences,
      sellIfFirstThreePctZero:
        parsed.sellIfFirstThreePctZero ??
        DEFAULT_WALLET_FILTER_PROFILE_SETTINGS.sellIfFirstThreePctZero,
      sellIfNoTeenOrTwentyPct:
        parsed.sellIfNoTeenOrTwentyPct ??
        DEFAULT_WALLET_FILTER_PROFILE_SETTINGS.sellIfNoTeenOrTwentyPct,
      sellIfNoPctAbove50:
        parsed.sellIfNoPctAbove50 ??
        DEFAULT_WALLET_FILTER_PROFILE_SETTINGS.sellIfNoPctAbove50,
    };
    const normalizeProfile = (
      profile?: Partial<WalletFilterProfileSettings>,
    ): WalletFilterProfileSettings => ({
      ...legacyProfile,
      ...(profile ?? {}),
    });
    return {
      ...legacyProfile,
      minBundlersCountChange:
        parsed.minBundlersCountChange ??
        (parsed as { minBundlersPercentIncrease?: number | null })
          .minBundlersPercentIncrease ??
        DEFAULT_WALLET_FILTER_SETTINGS.minBundlersCountChange,
      reverseBuySellTriggerEnabled:
        parsed.reverseBuySellTriggerEnabled ??
        DEFAULT_WALLET_FILTER_SETTINGS.reverseBuySellTriggerEnabled,
      minSolBuy: parsed.minSolBuy ?? DEFAULT_WALLET_FILTER_SETTINGS.minSolBuy,
    };
  }

  updateWalletSettings(
    walletAddress: string,
    settings: WalletFilterSettings,
  ): void {
    this.run(
      `INSERT INTO wallet_settings (wallet_address, filter_settings)
       VALUES (?, ?)
       ON CONFLICT(wallet_address) DO UPDATE SET filter_settings = excluded.filter_settings`,
      [walletAddress, JSON.stringify(settings)],
    );
  }

  // ── Early Bundler Positions ───────────────────────────────────────────────

  insertEarlyBundlerPosition(
    tradingWallet: string,
    mint: string,
    tokenAmount: number,
    buySol: number | null,
  ): number {
    const existing = this.getActiveEarlyBundlerPosition(tradingWallet, mint);
    if (existing) {
      return existing.id;
    }

    this.run(
      `INSERT INTO early_bundler_positions (trading_wallet, mint, token_amount, buy_sol, status, created_at)
       VALUES (?, ?, ?, ?, 'active', ?)`,
      [tradingWallet, mint, tokenAmount, buySol, new Date().toISOString()],
    );
    const rows = this.query<{ id: number }>(`SELECT last_insert_rowid() as id`);
    return rows[0].id;
  }

  getActiveEarlyBundlerPosition(
    tradingWallet: string,
    mint: string,
  ): { id: number; tokenAmount: number; buySol: number | null } | null {
    const rows = this.query<{
      id: number;
      token_amount: number;
      buy_sol: number | null;
    }>(
      `SELECT id, token_amount, buy_sol FROM early_bundler_positions 
       WHERE trading_wallet = ? AND mint = ? AND status = 'active'`,
      [tradingWallet, mint],
    );
    if (rows.length === 0) return null;
    return {
      id: rows[0].id,
      tokenAmount: rows[0].token_amount,
      buySol: rows[0].buy_sol,
    };
  }

  closeEarlyBundlerPosition(positionId: number, exitReason: string): void {
    this.run(
      `UPDATE early_bundler_positions 
       SET status = 'exited', exited_at = ?, exit_reason = ?
       WHERE id = ?`,
      [new Date().toISOString(), exitReason, positionId],
    );
  }

  // ── Early Bundler Wallets ─────────────────────────────────────────────────

  insertEarlyBundlerWallet(
    positionId: number,
    walletAddress: string,
    initialTokenAmount: number,
    signature: string,
    slot: number,
    timestamp: number,
  ): number {
    const rows_existing = this.query<{ id: number }>(
      `SELECT id FROM early_bundler_wallets WHERE position_id = ? AND wallet_address = ?`,
      [positionId, walletAddress],
    );
    if (rows_existing.length > 0) {
      return rows_existing[0].id;
    }

    this.run(
      `INSERT INTO early_bundler_wallets 
       (position_id, wallet_address, initial_token_amount, signature, slot, timestamp, status, total_sold_amount)
       VALUES (?, ?, ?, ?, ?, ?, 'monitoring', 0)`,
      [
        positionId,
        walletAddress,
        initialTokenAmount,
        signature,
        slot,
        timestamp,
      ],
    );
    const rows = this.query<{ id: number }>(`SELECT last_insert_rowid() as id`);
    return rows[0].id;
  }

  getActiveBundlerWallets(positionId: number): Array<{
    id: number;
    walletAddress: string;
    initialTokenAmount: number;
    totalSoldAmount: number;
  }> {
    const rows = this.query<{
      id: number;
      wallet_address: string;
      initial_token_amount: number;
      total_sold_amount: number;
    }>(
      `SELECT id, wallet_address, initial_token_amount, total_sold_amount 
       FROM early_bundler_wallets 
       WHERE position_id = ? AND status = 'monitoring'`,
      [positionId],
    );
    return rows.map((r) => ({
      id: r.id,
      walletAddress: r.wallet_address,
      initialTokenAmount: r.initial_token_amount,
      totalSoldAmount: r.total_sold_amount,
    }));
  }

  updateBundlerWalletSoldAmount(
    bundlerWalletId: number,
    soldAmount: number,
  ): void {
    this.run(
      `UPDATE early_bundler_wallets 
       SET total_sold_amount = total_sold_amount + ? 
       WHERE id = ?`,
      [soldAmount, bundlerWalletId],
    );
  }

  stopMonitoringBundlerWallet(bundlerWalletId: number): void {
    this.run(
      `UPDATE early_bundler_wallets SET status = 'stopped' WHERE id = ?`,
      [bundlerWalletId],
    );
  }

  // ── Bundler Wallet Sells ──────────────────────────────────────────────────

  recordBundlerWalletSell(
    bundlerWalletId: number,
    signature: string,
    tokenAmountSold: number,
    slot: number,
    timestamp: number,
  ): void {
    this.run(
      `INSERT INTO bundler_wallet_sells (bundler_wallet_id, signature, token_amount_sold, slot, timestamp)
       VALUES (?, ?, ?, ?, ?)`,
      [bundlerWalletId, signature, tokenAmountSold, slot, timestamp],
    );
  }

  close(): void {
    this.persist();
    this.db.close();
    log.info("Database connection closed");
  }
}
