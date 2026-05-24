// ─────────────────────────────────────────────────────────────────────────────
//  database.ts  —  SQLite persistence via sql.js (pure JavaScript, no native
//                  compilation required — works on Windows without VS Build Tools)
//
//  sql.js keeps the database in memory and flushes it to disk as a binary file
//  on every write.  For a monitor process that runs for minutes at a time and
//  writes ~30 rows per token this is perfectly fast.
// ─────────────────────────────────────────────────────────────────────────────

import initSqlJs, { Database as SqlJsDatabase } from 'sql.js';
import * as fs from 'fs';
import * as path from 'path';
import {
  BundlerMetrics,
  MonitoringStatus,
  TrackedToken,
  WalletFilterSettings,
} from './types';
import { createLogger } from './logger';

const log = createLogger('DB');

export const DEFAULT_WALLET_FILTER_SETTINGS: WalletFilterSettings = {
  applyAtSample: 20,
  minBundlersPercent: null,
  maxBundlersPercent: null,
  minBundlersCount: null,
  maxBundlersCount: null,
  maxBundlersPercentIncrease: null,
  maxPctAboveValue: 60,
  maxPctAboveOccurrences: 5,
  maxPctBelowValue: null,
  maxPctBelowOccurrences: null,
  sellIfFirstThreePctZero: false,
  sellIfNoTeenOrTwentyPct: false,
};

// ── Schema ────────────────────────────────────────────────────────────────────

const SCHEMA = `
CREATE TABLE IF NOT EXISTS tokens (
  wallet_address     TEXT    NOT NULL DEFAULT '',
  mint               TEXT    NOT NULL,
  first_seen         TEXT    NOT NULL,
  monitoring_status  TEXT    NOT NULL DEFAULT 'active',
  detected_at_ms     INTEGER NOT NULL,
  PRIMARY KEY (wallet_address, mint)
);

CREATE TABLE IF NOT EXISTS monitored_wallets (
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
  bundled_amount_rate  REAL,
  raw_data             TEXT
);

CREATE INDEX IF NOT EXISTS idx_bm_mint      ON bundler_metrics(mint);
CREATE INDEX IF NOT EXISTS idx_bm_timestamp ON bundler_metrics(timestamp);
`;

// ── Row shapes returned by sql.js ─────────────────────────────────────────────

interface TokenRow {
  mint: string;
  wallet_address: string;
  first_seen: string;
  monitoring_status: string;
  detected_at_ms: number;
}

interface MetricRow {
  id: number;
  wallet_address: string;
  mint: string;
  timestamp: string;
  bundlers_percent: number | null;
  bundlers_count: number | null;
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

    const tokenColumns = this.query<{ name: string }>(`PRAGMA table_info(tokens)`);
    if (!tokenColumns.some((c) => c.name === 'wallet_address')) {
      this.db.run(`ALTER TABLE tokens ADD COLUMN wallet_address TEXT NOT NULL DEFAULT ''`);
    }
    const metricColumns = this.query<{ name: string }>(`PRAGMA table_info(bundler_metrics)`);
    if (!metricColumns.some((c) => c.name === 'wallet_address')) {
      this.db.run(`ALTER TABLE bundler_metrics ADD COLUMN wallet_address TEXT NOT NULL DEFAULT ''`);
    }
  }

  private migrateTokensTable(): void {
    const columns = this.query<{ name: string; pk: number }>(`PRAGMA table_info(tokens)`);
    const walletPk = columns.find((c) => c.name === 'wallet_address')?.pk ?? 0;
    const mintPk = columns.find((c) => c.name === 'mint')?.pk ?? 0;

    if (walletPk === 1 && mintPk === 2) return;

    this.db.run('BEGIN TRANSACTION');
    try {
      this.db.run(`
        CREATE TABLE IF NOT EXISTS tokens_new (
          wallet_address     TEXT    NOT NULL DEFAULT '',
          mint               TEXT    NOT NULL,
          first_seen         TEXT    NOT NULL,
          monitoring_status  TEXT    NOT NULL DEFAULT 'active',
          detected_at_ms     INTEGER NOT NULL,
          PRIMARY KEY (wallet_address, mint)
        )
      `);

      const hasWalletAddress = columns.some((c) => c.name === 'wallet_address');
      const selectWalletAddress = hasWalletAddress ? 'wallet_address' : "''";

      this.db.run(`
        INSERT OR IGNORE INTO tokens_new
          (wallet_address, mint, first_seen, monitoring_status, detected_at_ms)
        SELECT ${selectWalletAddress}, mint, first_seen, monitoring_status, detected_at_ms
        FROM tokens
      `);

      this.db.run('DROP TABLE tokens');
      this.db.run('ALTER TABLE tokens_new RENAME TO tokens');
      this.db.run('COMMIT');
      log.info('Migrated tokens table to wallet+mint primary key');
    } catch (err) {
      this.db.run('ROLLBACK');
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
         (mint, wallet_address, first_seen, monitoring_status, detected_at_ms)
       VALUES (?, ?, ?, ?, ?)`,
      [
        token.mint,
        token.walletAddress,
        token.firstSeen,
        token.monitoringStatus,
        token.detectedAt,
      ]
    );
  }

  updateTokenStatus(walletAddress: string, mint: string, status: MonitoringStatus): void {
    this.run(
      `UPDATE tokens SET monitoring_status = ? WHERE wallet_address = ? AND mint = ?`,
      [status, walletAddress, mint]
    );
  }

  getAllActiveTokens(): TrackedToken[] {
    const rows = this.query<TokenRow>(
      `SELECT * FROM tokens WHERE monitoring_status = 'active'`
    );
    return rows.map((r) => ({
      mint: r.mint,
      walletAddress: r.wallet_address,
      firstSeen: r.first_seen,
      monitoringStatus: r.monitoring_status as MonitoringStatus,
      detectedAt: r.detected_at_ms,
    }));
  }

  tokenExists(walletAddress: string, mint: string): boolean {
    const rows = this.query<{ found: number }>(
      `SELECT 1 AS found FROM tokens WHERE wallet_address = ? AND mint = ?`,
      [walletAddress, mint]
    );
    return rows.length > 0;
  }

  addWallet(address: string): void {
    this.run(
      `INSERT INTO monitored_wallets (address, added_at, status)
       VALUES (?, ?, 'active')
       ON CONFLICT(address) DO UPDATE SET status = 'active'`,
      [address, new Date().toISOString()]
    );
  }

  removeWallet(address: string): void {
    this.run(
      `UPDATE monitored_wallets SET status = 'stopped' WHERE address = ?`,
      [address]
    );
  }

  getActiveWallets(): string[] {
    const rows = this.query<{ address: string }>(
      `SELECT address FROM monitored_wallets WHERE status = 'active' ORDER BY added_at ASC`
    );
    return rows.map((r) => r.address);
  }

  // ── bundler_metrics table ──────────────────────────────────────────────────

  insertMetrics(m: BundlerMetrics): void {
    this.run(
      `INSERT INTO bundler_metrics
         (wallet_address, mint, timestamp, bundlers_percent, bundlers_count, bundled_amount_rate, raw_data)
       VALUES (?, ?, ?, ?, ?, ?, ?)`,
      [
        m.walletAddress ?? '',
        m.mint,
        m.timestamp,
        m.bundlersPercent  ?? null,
        m.bundlersCount    ?? null,
        m.bundledAmountRate ?? null,
        m.rawData          ?? null,
      ]
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
      [mint, limit]
    );
    return rows.map((r) => ({
      id:               r.id,
      mint:             r.mint,
      timestamp:        r.timestamp,
      bundlersPercent:  r.bundlers_percent,
      bundlersCount:    r.bundlers_count,
      bundledAmountRate: r.bundled_amount_rate,
      rawData:          r.raw_data ?? undefined,
    }));
  }

  getLatestMetricsForWallet(walletAddress: string, mint: string, limit = 10): BundlerMetrics[] {
    const rows = this.query<MetricRow>(
      `SELECT * FROM bundler_metrics WHERE wallet_address = ? AND mint = ?
       ORDER BY timestamp DESC LIMIT ?`,
      [walletAddress, mint, limit]
    );
    return rows.map((r) => ({
      id:               r.id,
      walletAddress:    r.wallet_address,
      mint:             r.mint,
      timestamp:        r.timestamp,
      bundlersPercent:  r.bundlers_percent,
      bundlersCount:    r.bundlers_count,
      bundledAmountRate: r.bundled_amount_rate,
      rawData:          r.raw_data ?? undefined,
    }));
  }

  metricsCount(mint: string): number {
    const rows = this.query<{ cnt: number }>(
      `SELECT COUNT(*) AS cnt FROM bundler_metrics WHERE mint = ?`,
      [mint]
    );
    return rows[0]?.cnt ?? 0;
  }

  metricsCountForWallet(walletAddress: string, mint: string): number {
    const rows = this.query<{ cnt: number }>(
      `SELECT COUNT(*) AS cnt FROM bundler_metrics WHERE wallet_address = ? AND mint = ?`,
      [walletAddress, mint]
    );
    return rows[0]?.cnt ?? 0;
  }

  tokenCountForWallet(walletAddress: string): number {
    const rows = this.query<{ cnt: number }>(
      `SELECT COUNT(*) AS cnt FROM tokens WHERE wallet_address = ?`,
      [walletAddress]
    );
    return rows[0]?.cnt ?? 0;
  }

  sampleCountForWallet(walletAddress: string): number {
    const rows = this.query<{ cnt: number }>(
      `SELECT COUNT(*) AS cnt FROM bundler_metrics WHERE wallet_address = ?`,
      [walletAddress]
    );
    return rows[0]?.cnt ?? 0;
  }

  getWalletSettings(walletAddress: string): WalletFilterSettings {
    const rows = this.query<{ filter_settings: string }>(
      `SELECT filter_settings FROM wallet_settings WHERE wallet_address = ?`,
      [walletAddress]
    );
    if (rows.length === 0) return { ...DEFAULT_WALLET_FILTER_SETTINGS };

    try {
      const parsed = JSON.parse(rows[0].filter_settings) as Partial<WalletFilterSettings>;
      return { ...DEFAULT_WALLET_FILTER_SETTINGS, ...parsed };
    } catch {
      return { ...DEFAULT_WALLET_FILTER_SETTINGS };
    }
  }

  updateWalletSettings(walletAddress: string, settings: WalletFilterSettings): void {
    this.run(
      `INSERT INTO wallet_settings (wallet_address, filter_settings)
       VALUES (?, ?)
       ON CONFLICT(wallet_address) DO UPDATE SET filter_settings = excluded.filter_settings`,
      [walletAddress, JSON.stringify(settings)]
    );
  }

  close(): void {
    this.persist();
    this.db.close();
    log.info('Database connection closed');
  }
}
