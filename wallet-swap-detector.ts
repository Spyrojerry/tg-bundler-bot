// ─────────────────────────────────────────────────────────────────────────────
//  wallet-swap-detector.ts — Per-wallet swap classification from asset deltas
//  plus known DEX program participation (not program IDs alone).
// ─────────────────────────────────────────────────────────────────────────────

import { HeliusTransaction } from './helius-client';

export const WSOL_MINT = 'So11111111111111111111111111111111111111112';

interface RawParsedTokenBalance {
  accountIndex: number;
  mint: string;
  owner?: string;
  uiTokenAmount?: { amount?: string; decimals?: number; uiAmount?: number | null };
}

/** Shape of the `result` field of a `transactionSubscribe` notification. */
export interface RawEnhancedWsTransactionResult {
  slot?: number;
  transaction?: {
    transaction?: {
      signatures?: string[];
      message?: {
        accountKeys?: Array<string | { pubkey: string }>;
        instructions?: InstructionLike[];
      };
    };
    meta?: {
      err?: unknown;
      fee?: number;
      preBalances?: number[];
      postBalances?: number[];
      preTokenBalances?: RawParsedTokenBalance[];
      postTokenBalances?: RawParsedTokenBalance[];
      innerInstructions?: Array<{ index: number; instructions?: InstructionLike[] }>;
      loadedAddresses?: { writable?: string[]; readonly?: string[] };
    };
  };
  signature?: string;
  blockTime?: number | null;
}

/** Known swap/DEX program IDs (bonding curves, AMMs, routers). */
export const SWAP_PROGRAM_IDS = new Set([
  '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P', // Pump.fun bonding curve
  'pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA', // PumpSwap AMM
  '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8', // Raydium AMM v4
  'CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK', // Raydium CLMM
  'CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C', // Raydium CPMM
  'JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4', // Jupiter Aggregator v6
  'LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo', // Meteora DLMM
  'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc', // Orca Whirlpool
]);

export type SwapDirection = 'BUY' | 'SELL' | 'TOKEN_TO_TOKEN';

export interface WalletSwapDetection {
  isSwap: boolean;
  direction?: SwapDirection;
  inputMint?: string;
  outputMint?: string;
  inputAmountRaw?: bigint;
  outputAmountRaw?: bigint;
  solChangeLamports?: bigint;
}

interface TokenChange {
  mint: string;
  beforeRaw: bigint;
  afterRaw: bigint;
  changeRaw: bigint;
  decimals: number;
}

interface InstructionLike {
  programId?: string;
  programIdIndex?: number;
}

function normalizeAccountKey(account: string | { pubkey: string }): string {
  return typeof account === 'string' ? account : account.pubkey;
}

function getRawTokenAmount(balance: {
  uiTokenAmount?: { amount?: string; decimals?: number };
}): bigint {
  try {
    return BigInt(balance?.uiTokenAmount?.amount ?? '0');
  } catch {
    return 0n;
  }
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

function collectInvolvedPrograms(
  accountKeys: string[],
  topInstructions: InstructionLike[],
  innerGroups: Array<{ instructions?: InstructionLike[] }>,
): Set<string> {
  const involved = new Set<string>();
  const addIx = (ix: InstructionLike) => {
    if (ix.programId) involved.add(ix.programId);
    if (
      typeof ix.programIdIndex === 'number' &&
      accountKeys[ix.programIdIndex]
    ) {
      involved.add(accountKeys[ix.programIdIndex]!);
    }
  };
  for (const ix of topInstructions) addIx(ix);
  for (const group of innerGroups) {
    for (const ix of group.instructions ?? []) addIx(ix);
  }
  return involved;
}

function tokenChangesForWallet(
  wallet: string,
  preTokenBalances: Array<{
    owner?: string;
    mint: string;
    uiTokenAmount?: { amount?: string; decimals?: number };
  }>,
  postTokenBalances: Array<{
    owner?: string;
    mint: string;
    uiTokenAmount?: { amount?: string; decimals?: number };
  }>,
): TokenChange[] {
  const changesByMint = new Map<
    string,
    { beforeRaw: bigint; afterRaw: bigint; decimals: number }
  >();

  for (const balance of preTokenBalances) {
    if (balance.owner !== wallet) continue;
    changesByMint.set(balance.mint, {
      beforeRaw: getRawTokenAmount(balance),
      afterRaw: 0n,
      decimals: balance.uiTokenAmount?.decimals ?? 0,
    });
  }

  for (const balance of postTokenBalances) {
    if (balance.owner !== wallet) continue;
    const existing = changesByMint.get(balance.mint);
    changesByMint.set(balance.mint, {
      beforeRaw: existing?.beforeRaw ?? 0n,
      afterRaw: getRawTokenAmount(balance),
      decimals:
        balance.uiTokenAmount?.decimals ?? existing?.decimals ?? 0,
    });
  }

  return [...changesByMint.entries()]
    .map(([mint, value]) => ({
      mint,
      beforeRaw: value.beforeRaw,
      afterRaw: value.afterRaw,
      changeRaw: value.afterRaw - value.beforeRaw,
      decimals: value.decimals,
    }))
    .filter((change) => change.changeRaw !== 0n);
}

function classifySwapFromChanges(
  hasKnownSwapProgram: boolean,
  solChangeLamports: bigint,
  tokenChanges: TokenChange[],
): WalletSwapDetection {
  if (!hasKnownSwapProgram) return { isSwap: false };

  const tokensReceived = tokenChanges.filter((c) => c.changeRaw > 0n);
  const tokensSpent = tokenChanges.filter((c) => c.changeRaw < 0n);

  const receivedNonWsol = tokensReceived.find((c) => c.mint !== WSOL_MINT);
  const spentNonWsol = tokensSpent.find((c) => c.mint !== WSOL_MINT);
  const receivedWsol = tokensReceived.find((c) => c.mint === WSOL_MINT);
  const spentWsol = tokensSpent.find((c) => c.mint === WSOL_MINT);

  // SOL/WSOL → token buy
  if (
    receivedNonWsol &&
    (solChangeLamports < 0n || spentWsol !== undefined)
  ) {
    return {
      isSwap: true,
      direction: 'BUY',
      inputMint: WSOL_MINT,
      outputMint: receivedNonWsol.mint,
      inputAmountRaw:
        spentWsol !== undefined ? -spentWsol.changeRaw : -solChangeLamports,
      outputAmountRaw: receivedNonWsol.changeRaw,
      solChangeLamports,
    };
  }

  // Token → SOL/WSOL sell
  if (
    spentNonWsol &&
    (solChangeLamports > 0n || receivedWsol !== undefined)
  ) {
    return {
      isSwap: true,
      direction: 'SELL',
      inputMint: spentNonWsol.mint,
      outputMint: WSOL_MINT,
      inputAmountRaw: -spentNonWsol.changeRaw,
      outputAmountRaw:
        receivedWsol !== undefined ? receivedWsol.changeRaw : solChangeLamports,
      solChangeLamports,
    };
  }

  // Token A → Token B
  if (tokensSpent.length > 0 && tokensReceived.length > 0) {
    return {
      isSwap: true,
      direction: 'TOKEN_TO_TOKEN',
      inputMint: tokensSpent[0]!.mint,
      outputMint: tokensReceived[0]!.mint,
      inputAmountRaw: -tokensSpent[0]!.changeRaw,
      outputAmountRaw: tokensReceived[0]!.changeRaw,
      solChangeLamports,
    };
  }

  return { isSwap: false };
}

/** Classify a swap for one wallet from a raw `transactionSubscribe` payload. */
export function detectWalletSwapFromRaw(
  raw: RawEnhancedWsTransactionResult,
  wallet: string,
  knownSwapPrograms: Set<string> = SWAP_PROGRAM_IDS,
): WalletSwapDetection {
  const wrapper = raw.transaction;
  const transaction = wrapper?.transaction;
  const meta = wrapper?.meta;
  const message = transaction?.message;

  if (!message || !meta || meta.err != null) {
    return { isSwap: false };
  }

  const accountKeys = buildFullAccountKeys(raw);
  const walletIndex = accountKeys.indexOf(wallet);
  if (walletIndex === -1) return { isSwap: false };

  const involvedPrograms = collectInvolvedPrograms(
    accountKeys,
    message.instructions ?? [],
    meta.innerInstructions ?? [],
  );
  const hasKnownSwapProgram = [...involvedPrograms].some((id) =>
    knownSwapPrograms.has(id),
  );

  const tokenChanges = tokenChangesForWallet(
    wallet,
    meta.preTokenBalances ?? [],
    meta.postTokenBalances ?? [],
  );

  const solChangeLamports =
    BigInt(meta.postBalances?.[walletIndex] ?? 0) -
    BigInt(meta.preBalances?.[walletIndex] ?? 0);

  return classifySwapFromChanges(
    hasKnownSwapProgram,
    solChangeLamports,
    tokenChanges,
  );
}

/** Classify a swap for one wallet from a normalized `HeliusTransaction`. */
export function detectWalletSwapFromHeliusTx(
  tx: HeliusTransaction,
  wallet: string,
  knownSwapPrograms: Set<string> = SWAP_PROGRAM_IDS,
): WalletSwapDetection {
  const involvedPrograms = new Set(
    (tx.instructions ?? [])
      .map((ix) => ix.programId)
      .filter(Boolean),
  );
  const hasKnownSwapProgram =
    [...involvedPrograms].some((id) => knownSwapPrograms.has(id)) ||
    tx.type === 'SWAP';

  const accountEntry = (tx.accountData ?? []).find((a) => a.account === wallet);
  const solChangeLamports = BigInt(accountEntry?.nativeBalanceChange ?? 0);

  const byMint = new Map<string, number>();
  for (const transfer of tx.tokenTransfers ?? []) {
    const mint = transfer.mint;
    if (!mint) continue;
    const amount = transfer.tokenAmount ?? 0;
    if (amount === 0) continue;
    if (transfer.toUserAccount === wallet) {
      byMint.set(mint, (byMint.get(mint) ?? 0) + amount);
    }
    if (transfer.fromUserAccount === wallet) {
      byMint.set(mint, (byMint.get(mint) ?? 0) - amount);
    }
  }

  const tokenChanges: TokenChange[] = [...byMint.entries()]
    .filter(([, delta]) => Math.abs(delta) > 1e-12)
    .map(([mint, delta]) => ({
      mint,
      beforeRaw: 0n,
      afterRaw: delta > 0 ? 1n : 0n,
      changeRaw: delta > 0 ? 1n : -1n,
      decimals: 0,
    }));

  return classifySwapFromChanges(
    hasKnownSwapProgram,
    solChangeLamports,
    tokenChanges,
  );
}

/** Returns the output mint when the wallet executed a token buy swap, else null. */
export function findWalletSwapBuyMint(
  tx: HeliusTransaction,
  wallet: string,
): string | null {
  const detection = detectWalletSwapFromHeliusTx(tx, wallet);
  if (detection.isSwap && detection.direction === 'BUY') {
    const mint = detection.outputMint;
    if (mint && mint !== WSOL_MINT) return mint;
  }

  // Helius REST fallback: enriched type SWAP plus token-in / SOL-out pattern.
  if (tx.type !== 'SWAP') return null;

  let receivedMint: string | null = null;
  for (const transfer of tx.tokenTransfers ?? []) {
    if (transfer.mint === WSOL_MINT) continue;
    if (transfer.toUserAccount !== wallet) continue;
    if ((transfer.tokenAmount ?? 0) <= 0) continue;
    receivedMint = transfer.mint;
    break;
  }
  if (!receivedMint) return null;

  const accountEntry = (tx.accountData ?? []).find((a) => a.account === wallet);
  const solDown =
    accountEntry?.nativeBalanceChange !== undefined &&
    accountEntry.nativeBalanceChange < 0;
  const spentNative = (tx.nativeTransfers ?? []).some(
    (t) => t.fromUserAccount === wallet && t.amount > 0,
  );
  if (solDown || spentNative) return receivedMint;

  return null;
}

/** True when any wallet in the set shows a qualifying swap on this raw notification. */
export function transactionInvolvesWalletSwap(
  raw: RawEnhancedWsTransactionResult,
  walletsToCheck: Iterable<string>,
): boolean {
  for (const wallet of walletsToCheck) {
    if (detectWalletSwapFromRaw(raw, wallet).isSwap) return true;
  }
  return false;
}

/** Collect wallets with native or token balance changes in a raw notification. */
export function collectWalletsWithBalanceChanges(
  raw: RawEnhancedWsTransactionResult,
  accountKeys: string[],
): Set<string> {
  const wallets = new Set<string>();
  const meta = raw.transaction?.meta;
  if (!meta) return wallets;

  const pre = meta.preBalances ?? [];
  const post = meta.postBalances ?? [];
  const len = Math.min(pre.length, post.length, accountKeys.length);
  for (let i = 0; i < len; i += 1) {
    if (pre[i] !== post[i] && accountKeys[i]) wallets.add(accountKeys[i]!);
  }

  for (const bal of meta.preTokenBalances ?? []) {
    if (bal.owner) wallets.add(bal.owner);
  }
  for (const bal of meta.postTokenBalances ?? []) {
    if (bal.owner) wallets.add(bal.owner);
  }

  return wallets;
}

export interface EarlyBundlerBuyRecord {
  wallet: string;
  tokenAmount: number;
  signature: string;
  buySol: number | null;
  feePayer: string | null;
  timestamp: number;
}

/** First N unique token recipients from chronological SWAP txs on a mint. */
export function extractFirstUniqueEarlyBundlerBuys(
  swaps: HeliusTransaction[],
  mint: string,
  requiredCount = 4,
  estimateBuySol?: (tx: HeliusTransaction, wallet: string) => number | null,
): EarlyBundlerBuyRecord[] {
  const firstBuys: EarlyBundlerBuyRecord[] = [];
  const seenWallets = new Set<string>();
  const sortedSwaps = [...swaps].sort(
    (a, b) => a.slot - b.slot || a.timestamp - b.timestamp,
  );

  for (const tx of sortedSwaps) {
    if (tx.type !== 'SWAP') continue;
    for (const transfer of tx.tokenTransfers ?? []) {
      if (transfer.mint !== mint) continue;
      const wallet = transfer.toUserAccount;
      if (!wallet || seenWallets.has(wallet)) continue;
      seenWallets.add(wallet);
      firstBuys.push({
        wallet,
        tokenAmount: transfer.tokenAmount ?? 0,
        signature: tx.signature,
        buySol: estimateBuySol?.(tx, wallet) ?? null,
        feePayer: tx.feePayer ?? null,
        timestamp: tx.timestamp,
      });
      if (firstBuys.length >= requiredCount) {
        return firstBuys;
      }
    }
  }

  return firstBuys;
}

function walletSetsEqual(a: ReadonlySet<string>, b: ReadonlySet<string>): boolean {
  if (a.size !== b.size) return false;
  for (const wallet of a) {
    if (!b.has(wallet)) return false;
  }
  return true;
}

/** True when every wallet in `groupRecipients` is in `firstFourWallets` and sizes match. */
export function watchedGroupMatchesFirstFourBundlers(
  groupRecipients: ReadonlySet<string>,
  firstFourWallets: readonly string[],
): boolean {
  if (firstFourWallets.length < 4 || groupRecipients.size !== 4) {
    return false;
  }
  return walletSetsEqual(groupRecipients, new Set(firstFourWallets));
}
