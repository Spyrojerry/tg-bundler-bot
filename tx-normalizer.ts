// ─────────────────────────────────────────────────────────────────────────────
//  tx-normalizer.ts  —  Converts a raw Solana `jsonParsed` transaction (as
//  delivered by Helius Enhanced WebSockets' `transactionSubscribe`) into the
//  same `HeliusTransaction` shape the rest of the codebase already reads
//  (produced today by the paid Enhanced Transactions REST API). This is what
//  lets every existing consumer (classifyTx, inspectBundlerFunderTransaction,
//  isDevFullExitCloseAccountTx, findTokenTransferOut, ...) keep working
//  unmodified while the underlying data source moves from a 100-credit REST
//  fetch to a free/near-free push notification.
//
//  `transactionSubscribe` gives Solana's *native* parsed shape
//  (message.instructions, meta.preBalances/postBalances,
//  meta.preTokenBalances/postTokenBalances) — not Helius's enriched
//  type/tokenTransfers/nativeTransfers/source fields. This file rebuilds
//  those enriched fields locally:
//
//   - nativeTransfers / tokenTransfers are reconstructed from balance deltas
//     (preBalances/postBalances, preTokenBalances/postTokenBalances), the
//     same technique Helius's own enrichment pipeline uses under the hood.
//     Each nonzero delta becomes exactly one transfer record with the real
//     wallet on the correct side and a "__pool__" placeholder counterparty
//     on the other side. Every consumer in this codebase only ever checks
//     `fromUserAccount === X` / `toUserAccount === X` for a *specific known*
//     wallet — never the identity of the other side — so this is a safe,
//     robust simplification that also sidesteps having to correctly pair up
//     multi-hop swap instructions (Jupiter routes, PumpSwap pool legs, etc).
//   - `type` (SWAP / TRANSFER / CLOSE_ACCOUNT / UNKNOWN) is inferred from
//     which program IDs appear anywhere in the instruction tree (top-level +
//     inner instructions).
//   - `source` is only populated for CLOSE_ACCOUNT (SOLANA_PROGRAM_LIBRARY),
//     which is currently the only place the rest of the code reads it.
//
//  NOTE: the exact wire shape of a `transactionSubscribe` notification's
//  `result` payload is not runtime-verified in this environment (no live
//  network path to Helius's WS endpoint from the sandbox this was authored
//  in). Every accessor below is defensive (optional chaining + fallbacks)
//  and `parseRawEnhancedWsResult` logs a warning with a small sample of the
//  unrecognized shape instead of throwing, so a shape mismatch degrades to
//  "this one push notification is skipped, REST backstop catches it later"
//  rather than crashing anything. Watch the logs after deploying — if
//  `[TX-NORMALIZER] Unrecognized transactionSubscribe result shape` shows up,
//  paste a sample here and this file's extraction logic can be adjusted.
// ─────────────────────────────────────────────────────────────────────────────

import { createLogger } from './logger';
import { HeliusTransaction } from './helius-client';

const log = createLogger('TX-NORMALIZER');

const SOL_MINT = 'So11111111111111111111111111111111111111112';
const TOKEN_PROGRAM_ID = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA';
const TOKEN_2022_PROGRAM_ID = 'TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb';

/** Program IDs whose presence anywhere in the instruction tree marks a tx as a SWAP (pump.fun bonding curve, PumpSwap AMM, plus the major Solana DEX/router programs a migrated/graduated token could route through). */
const SWAP_PROGRAM_IDS = new Set([
  '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P', // Pump.fun bonding curve
  'pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA', // PumpSwap AMM
  '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8', // Raydium AMM v4
  'CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK', // Raydium CLMM
  'CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C', // Raydium CPMM
  'JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4', // Jupiter Aggregator v6
]);

const UNKNOWN_COUNTERPARTY = '__pool__';

interface ParsedInstructionLike {
  programId?: string;
  program?: string;
  parsed?: { type?: string; info?: Record<string, unknown> };
}

interface RawParsedTokenBalance {
  accountIndex: number;
  mint: string;
  owner?: string;
  uiTokenAmount?: { uiAmount?: number | null; amount?: string; decimals?: number };
}

/** Shape of the `result` field of a Helius/Solana `transactionNotification` push, defensively typed since it is not runtime-verified here. */
export interface RawEnhancedWsTransactionResult {
  slot?: number;
  transaction?: {
    transaction?: {
      signatures?: string[];
      message?: {
        accountKeys?: Array<string | { pubkey: string }>;
        instructions?: ParsedInstructionLike[];
      };
    };
    meta?: {
      err?: unknown;
      fee?: number;
      preBalances?: number[];
      postBalances?: number[];
      preTokenBalances?: RawParsedTokenBalance[];
      postTokenBalances?: RawParsedTokenBalance[];
      innerInstructions?: Array<{ index: number; instructions?: ParsedInstructionLike[] }>;
      loadedAddresses?: { writable?: string[]; readonly?: string[] };
    };
  };
  signature?: string;
  blockTime?: number | null;
}

function accountKeyToString(key: string | { pubkey: string } | undefined): string | null {
  if (!key) return null;
  return typeof key === 'string' ? key : key.pubkey ?? null;
}

/**
 * Builds the full, index-aligned list of account addresses for a (possibly
 * versioned) transaction, appending resolved address-table-lookup entries so
 * the list lines up 1:1 with preBalances/postBalances (which always include
 * every loaded account, static + looked-up).
 */
function buildFullAccountKeys(
  raw: RawEnhancedWsTransactionResult,
): string[] {
  const staticKeys = (raw.transaction?.transaction?.message?.accountKeys ?? [])
    .map(accountKeyToString)
    .filter((k): k is string => Boolean(k));
  const preBalancesLen = raw.transaction?.meta?.preBalances?.length ?? 0;
  if (staticKeys.length >= preBalancesLen) return staticKeys;
  const loaded = raw.transaction?.meta?.loadedAddresses;
  return [...staticKeys, ...(loaded?.writable ?? []), ...(loaded?.readonly ?? [])];
}

function collectAllInstructions(
  raw: RawEnhancedWsTransactionResult,
): ParsedInstructionLike[] {
  const top = raw.transaction?.transaction?.message?.instructions ?? [];
  const inner = (raw.transaction?.meta?.innerInstructions ?? []).flatMap(
    (entry) => entry.instructions ?? [],
  );
  return [...top, ...inner];
}

function classifyType(
  instructions: ParsedInstructionLike[],
): { type: string; source?: string } {
  for (const ix of instructions) {
    if (ix.programId && SWAP_PROGRAM_IDS.has(ix.programId)) {
      return { type: 'SWAP' };
    }
  }
  for (const ix of instructions) {
    const isTokenProgram =
      ix.programId === TOKEN_PROGRAM_ID || ix.programId === TOKEN_2022_PROGRAM_ID;
    if (isTokenProgram && ix.parsed?.type === 'closeAccount') {
      return { type: 'CLOSE_ACCOUNT', source: 'SOLANA_PROGRAM_LIBRARY' };
    }
  }
  return { type: 'TRANSFER' };
}

/** Reconstructs nativeTransfers[] from preBalances/postBalances deltas. The fee payer's delta has the tx fee added back so the emitted amount reflects only the actual transfer, not the fee. */
function reconstructNativeTransfers(
  raw: RawEnhancedWsTransactionResult,
  accountKeys: string[],
): HeliusTransaction['nativeTransfers'] {
  const meta = raw.transaction?.meta;
  const pre = meta?.preBalances;
  const post = meta?.postBalances;
  if (!pre || !post) return [];
  const fee = meta?.fee ?? 0;
  const transfers: NonNullable<HeliusTransaction['nativeTransfers']> = [];
  const len = Math.min(pre.length, post.length, accountKeys.length);
  for (let i = 0; i < len; i += 1) {
    let delta = post[i] - pre[i];
    if (i === 0) delta += fee;
    if (delta === 0) continue;
    const account = accountKeys[i];
    if (!account) continue;
    if (delta > 0) {
      transfers.push({ fromUserAccount: UNKNOWN_COUNTERPARTY, toUserAccount: account, amount: delta });
    } else {
      transfers.push({ fromUserAccount: account, toUserAccount: UNKNOWN_COUNTERPARTY, amount: -delta });
    }
  }
  return transfers;
}

function tokenBalanceAmount(balance: RawParsedTokenBalance | undefined): number {
  if (!balance?.uiTokenAmount) return 0;
  if (typeof balance.uiTokenAmount.uiAmount === 'number') return balance.uiTokenAmount.uiAmount;
  const raw = Number(balance.uiTokenAmount.amount ?? '0');
  const decimals = balance.uiTokenAmount.decimals ?? 0;
  if (!Number.isFinite(raw)) return 0;
  return raw / 10 ** decimals;
}

/** Reconstructs tokenTransfers[] from preTokenBalances/postTokenBalances deltas, keyed per (mint, owner) — same delta-based approach as nativeTransfers, and for the same reason: it sidesteps having to correctly pair up multi-hop swap legs. */
function reconstructTokenTransfers(
  raw: RawEnhancedWsTransactionResult,
): HeliusTransaction['tokenTransfers'] {
  const meta = raw.transaction?.meta;
  const pre = meta?.preTokenBalances ?? [];
  const post = meta?.postTokenBalances ?? [];
  const deltas = new Map<string, { mint: string; owner: string; delta: number }>();

  const keyFor = (mint: string, owner: string) => `${mint}|${owner}`;

  for (const bal of pre) {
    if (!bal.owner) continue;
    const key = keyFor(bal.mint, bal.owner);
    const amount = tokenBalanceAmount(bal);
    const existing = deltas.get(key);
    deltas.set(key, { mint: bal.mint, owner: bal.owner, delta: (existing?.delta ?? 0) - amount });
  }
  for (const bal of post) {
    if (!bal.owner) continue;
    const key = keyFor(bal.mint, bal.owner);
    const amount = tokenBalanceAmount(bal);
    const existing = deltas.get(key);
    deltas.set(key, { mint: bal.mint, owner: bal.owner, delta: (existing?.delta ?? 0) + amount });
  }

  const transfers: NonNullable<HeliusTransaction['tokenTransfers']> = [];
  for (const { mint, owner, delta } of deltas.values()) {
    if (Math.abs(delta) < 1e-9) continue;
    if (delta > 0) {
      transfers.push({ fromUserAccount: UNKNOWN_COUNTERPARTY, toUserAccount: owner, tokenAmount: delta, mint });
    } else {
      transfers.push({ fromUserAccount: owner, toUserAccount: UNKNOWN_COUNTERPARTY, tokenAmount: -delta, mint });
    }
  }
  return transfers;
}

function reconstructAccountData(
  raw: RawEnhancedWsTransactionResult,
  accountKeys: string[],
): HeliusTransaction['accountData'] {
  const meta = raw.transaction?.meta;
  const pre = meta?.preBalances;
  const post = meta?.postBalances;
  if (!pre || !post) return [];
  const len = Math.min(pre.length, post.length, accountKeys.length);
  const out: NonNullable<HeliusTransaction['accountData']> = [];
  for (let i = 0; i < len; i += 1) {
    const account = accountKeys[i];
    if (!account) continue;
    out.push({
      account,
      nativePostBalance: post[i],
      nativeBalanceChange: post[i] - pre[i],
    });
  }
  return out;
}

/**
 * Normalizes one raw `transactionSubscribe` notification `result` into the
 * `HeliusTransaction` shape. Returns null (and logs once) if the payload is
 * missing the minimum fields needed (message/meta) to normalize at all —
 * callers should treat null as "skip this notification, the REST backstop
 * will catch up eventually" rather than as a hard failure.
 */
export function normalizeEnhancedWsTransaction(
  raw: RawEnhancedWsTransactionResult,
): HeliusTransaction | null {
  try {
    const meta = raw.transaction?.meta;
    const message = raw.transaction?.transaction?.message;
    if (!meta || !message) {
      log.warn('Unrecognized transactionSubscribe result shape (missing transaction/meta)', {
        sample: JSON.stringify(raw).slice(0, 500),
      });
      return null;
    }
    const signature =
      raw.signature ?? raw.transaction?.transaction?.signatures?.[0] ?? '';
    if (!signature) {
      log.warn('Unrecognized transactionSubscribe result shape (missing signature)', {
        sample: JSON.stringify(raw).slice(0, 500),
      });
      return null;
    }
    const accountKeys = buildFullAccountKeys(raw);
    const instructions = collectAllInstructions(raw);
    const { type, source } = classifyType(instructions);
    const nativeTransfers = reconstructNativeTransfers(raw, accountKeys);
    const tokenTransfers = reconstructTokenTransfers(raw);
    const accountData = reconstructAccountData(raw, accountKeys);

    const hasAnyTransfer =
      (nativeTransfers?.length ?? 0) > 0 || (tokenTransfers?.length ?? 0) > 0;
    const finalType = type === 'TRANSFER' && !hasAnyTransfer ? 'UNKNOWN' : type;

    const tx: HeliusTransaction = {
      signature,
      slot: raw.slot ?? 0,
      timestamp: raw.blockTime ?? Math.floor(Date.now() / 1000),
      type: finalType,
      source,
      feePayer: accountKeys[0],
      tokenTransfers,
      nativeTransfers,
      accountData,
      instructions: instructions
        .filter((ix): ix is ParsedInstructionLike & { programId: string } => Boolean(ix.programId))
        .map((ix) => ({ programId: ix.programId })),
    };
    return tx;
  } catch (err) {
    log.warn('Failed to normalize transactionSubscribe result', {
      error: err instanceof Error ? err.message : String(err),
      sample: JSON.stringify(raw).slice(0, 500),
    });
    return null;
  }
}

export { SOL_MINT as TX_NORMALIZER_SOL_MINT, UNKNOWN_COUNTERPARTY };
