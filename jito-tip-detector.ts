// ─────────────────────────────────────────────────────────────────────────────
//  jito-tip-detector.ts — Detect whether a transaction pays a Jito tip.
//
//  Logic:
//    transaction → all instructions (top-level + inner)
//      → find System Program transfers
//      → destination account
//      → compare against the Jito tip accounts
//      → MATCH? yes → jitoTip = true (record account + amount)
//               no  → jitoTip = false
//
//  Works on the native parsed transaction shape (message.instructions with
//  programId + parsed.info), the same shape `tx-normalizer.ts` reads. A
//  balance-delta fallback on nativeTransfers is provided for callers that only
//  have the enriched HeliusTransaction.
// ─────────────────────────────────────────────────────────────────────────────

import { HeliusTransaction } from "./helius-client";

const SYSTEM_PROGRAM_ID = "11111111111111111111111111111111";

/**
 * Canonical Jito tip accounts. Jito randomizes across these; a valid tip
 * transfer goes to exactly one of them. Keep in sync with Jito's published
 * list (https://docs.jito.wtf/lowlatencytxnsend/#tip-accounts).
 */
export const JITO_TIP_ACCOUNTS: ReadonlySet<string> = new Set([
  "96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5",
  "HFqU5x63VTqvQss8hp11i4wVV8bD44PvwucfZ2bU7gRe",
  "Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY",
  "ADaUMid9yfUytqMBgopwjb2DTLSokTSzL1zt6iGPaS49",
  "DfXygSm4jCyNCybVYYK6DwvWqjKee8pbDmJGcLWNDXjh",
  "ADuUkR4vqLUMWXxW9gh6D6L8pMSawimctcNZ5pGwDcEt",
  "DttWaMuVvTiduZRnguLF7jNxTgiMBZ1hyAumKUiL2KRL",
  "3AVi9Tg9Uo68tJfuvoKvqKNWKkC5wPdSSdeBnizKZ6jT",
]);

export interface JitoTipInfo {
  /** The Jito tip account the transfer paid. */
  tipAccount: string;
  /** Tip amount in lamports. */
  lamports: number;
}

interface ParsedInstructionLike {
  programId?: string;
  programIdIndex?: number;
  program?: string;
  parsed?: { type?: string; info?: Record<string, unknown> };
}

function asString(value: unknown): string | null {
  if (typeof value === "string") return value;
  if (value && typeof value === "object" && "pubkey" in value) {
    const pubkey = (value as { pubkey?: unknown }).pubkey;
    return typeof pubkey === "string" ? pubkey : null;
  }
  return null;
}

function asLamports(value: unknown): number {
  const n = typeof value === "string" ? Number(value) : value;
  return typeof n === "number" && Number.isFinite(n) && n > 0 ? n : 0;
}

/**
 * Scans parsed instructions (top-level + inner) for a System Program transfer
 * (or a plain transfer already tagged via `parsed.type`) whose destination is a
 * Jito tip account. Returns the first match, or null.
 */
export function detectJitoTipInInstructions(
  instructions: ParsedInstructionLike[],
): JitoTipInfo | null {
  for (const ix of instructions) {
    const isSystemProgram =
      ix.programId === SYSTEM_PROGRAM_ID || ix.program === "system";
    if (!isSystemProgram) continue;

    const type = ix.parsed?.type;
    // "transfer" (System Program) and "transferWithSeed"/"advanceNonce" style
    // variants all carry a destination + lamports; only treat transfer types.
    if (type && !/^transfer/i.test(type)) continue;

    const info = ix.parsed?.info ?? {};
    const destination = asString(info.destination) ?? asString(info.to);
    if (!destination || !JITO_TIP_ACCOUNTS.has(destination)) continue;

    const lamports = asLamports(info.lamports) || asLamports(info.amount);
    return { tipAccount: destination, lamports };
  }
  return null;
}

/** Collects all instructions (top-level + inner) from a raw parsed tx. */
export function detectJitoTipInRawTx(raw: unknown): JitoTipInfo | null {
  const message = (raw as any)?.transaction?.transaction?.message ?? (raw as any)?.message;
  const meta = (raw as any)?.transaction?.meta ?? (raw as any)?.meta;
  const top: ParsedInstructionLike[] = message?.instructions ?? [];
  const inner: ParsedInstructionLike[] = (meta?.innerInstructions ?? []).flatMap(
    (entry: { instructions?: ParsedInstructionLike[] }) => entry.instructions ?? [],
  );
  return detectJitoTipInInstructions([...top, ...inner]);
}

/**
 * Balance-delta fallback: an enriched HeliusTransaction whose nativeTransfers
 * show SOL flowing to a Jito tip account. The normalized WS path uses a
 * "__pool__" placeholder counterparty, so this only resolves real destinations
 * when the source still carries them (REST-enriched txs). Returns null when the
 * destination is a placeholder or no tip account is present.
 */
export function detectJitoTipInTransaction(
  tx: HeliusTransaction,
): JitoTipInfo | null {
  for (const transfer of tx.nativeTransfers ?? []) {
    const destination = transfer.toUserAccount;
    if (destination && JITO_TIP_ACCOUNTS.has(destination)) {
      return { tipAccount: destination, lamports: transfer.amount ?? 0 };
    }
  }
  return null;
}

/** True when the transaction pays a Jito tip. */
export function usesJitoTip(tx: HeliusTransaction): boolean {
  return tx.jitoTip != null || detectJitoTipInTransaction(tx) !== null;
}

/** Returns the Jito tip info for a tx from either the normalized field or nativeTransfers. */
export function resolveJitoTip(tx: HeliusTransaction): JitoTipInfo | null {
  return tx.jitoTip ?? detectJitoTipInTransaction(tx);
}
