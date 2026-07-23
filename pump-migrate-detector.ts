// ─────────────────────────────────────────────────────────────────────────────
//  pump-migrate-detector.ts — Detect Pump.fun migrate / migrate_v2 txs and
//  extract the migrated mint from parsed Enhanced WS payloads.
// ─────────────────────────────────────────────────────────────────────────────

import type { RawEnhancedWsTransactionResult } from './wallet-swap-detector';

export const PUMP_FUN_PROGRAM = '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P';
export const PUMP_MIGRATE_INSTRUCTIONS = new Set(['migrate', 'migrate_v2']);

interface ParsedInstructionLike {
  programId?: string;
  programIdIndex?: number;
  program?: string;
  parsed?: { type?: string; info?: Record<string, unknown> };
}

function normalizeAccountKey(account: string | { pubkey: string }): string {
  return typeof account === 'string' ? account : account.pubkey;
}

function buildFullAccountKeys(raw: RawEnhancedWsTransactionResult): string[] {
  const message = raw.transaction?.transaction?.message;
  const meta = raw.transaction?.meta;
  const staticKeys = (message?.accountKeys ?? [])
    .map(normalizeAccountKey)
    .filter(Boolean);
  const preLen = meta?.preBalances?.length ?? 0;
  if (staticKeys.length >= preLen) return staticKeys;
  return [
    ...staticKeys,
    ...(meta?.loadedAddresses?.writable ?? []),
    ...(meta?.loadedAddresses?.readonly ?? []),
  ];
}

function resolveProgramId(
  ix: ParsedInstructionLike,
  accountKeys: string[],
): string | null {
  if (ix.programId) return ix.programId;
  if (typeof ix.programIdIndex === 'number') {
    return accountKeys[ix.programIdIndex] ?? null;
  }
  return ix.program ?? null;
}

function collectParsedInstructions(
  raw: RawEnhancedWsTransactionResult,
): ParsedInstructionLike[] {
  const message = raw.transaction?.transaction?.message;
  const meta = raw.transaction?.meta;
  const accountKeys = buildFullAccountKeys(raw);
  const out: ParsedInstructionLike[] = [];
  const addIx = (ix: ParsedInstructionLike) => {
    const programId = resolveProgramId(ix, accountKeys);
    out.push({ ...ix, programId: programId ?? ix.programId });
  };
  for (const ix of (message?.instructions ?? []) as ParsedInstructionLike[]) {
    addIx(ix);
  }
  for (const group of meta?.innerInstructions ?? []) {
    for (const ix of (group.instructions ?? []) as ParsedInstructionLike[]) {
      addIx(ix);
    }
  }
  return out;
}

function mintFromInstructionInfo(info: Record<string, unknown> | undefined): string | null {
  if (!info) return null;
  for (const key of ['mint', 'tokenMint', 'baseMint']) {
    const value = info[key];
    if (typeof value === 'string' && value.length > 0) return value;
  }
  return null;
}

export function isPumpMigrateRawTransaction(raw: RawEnhancedWsTransactionResult): boolean {
  for (const ix of collectParsedInstructions(raw)) {
    if (ix.programId !== PUMP_FUN_PROGRAM) continue;
    const type = ix.parsed?.type?.trim().toLowerCase();
    if (type && PUMP_MIGRATE_INSTRUCTIONS.has(type)) return true;
  }
  return false;
}

export function extractPumpMigrateMintFromRaw(
  raw: RawEnhancedWsTransactionResult,
): string | null {
  for (const ix of collectParsedInstructions(raw)) {
    if (ix.programId !== PUMP_FUN_PROGRAM) continue;
    const type = ix.parsed?.type?.trim().toLowerCase();
    if (!type || !PUMP_MIGRATE_INSTRUCTIONS.has(type)) continue;
    const mint = mintFromInstructionInfo(ix.parsed?.info);
    if (mint) return mint;
  }

  const postMints = new Set<string>();
  for (const balance of raw.transaction?.meta?.postTokenBalances ?? []) {
    if (balance.mint?.endsWith('pump')) postMints.add(balance.mint);
  }
  if (postMints.size === 1) return [...postMints][0] ?? null;
  return null;
}

export function isExchangeFundedBy(funding: {
  funderType?: string | null;
  funderName?: string | null;
} | null): boolean {
  if (!funding) return false;
  const type = funding.funderType?.trim().toLowerCase() ?? '';
  return type === 'centralized exchange';
}
