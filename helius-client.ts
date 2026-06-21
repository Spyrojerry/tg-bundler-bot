// ─────────────────────────────────────────────────────────────────────────────
//  helius-client.ts  —  Helius API client for fetching early bundler wallets
// ─────────────────────────────────────────────────────────────────────────────

import { createLogger } from './logger';

const log = createLogger('HELIUS');

export interface HeliusTransaction {
  signature: string;
  slot: number;
  timestamp: number;
  type?: string;
  source?: string;
  feePayer?: string;
  description?: string;
  tokenTransfers?: Array<{
    fromUserAccount: string;
    toUserAccount: string;
    tokenAmount: number;
    mint: string;
  }>;
  nativeTransfers?: Array<{
    fromUserAccount: string;
    toUserAccount: string;
    amount: number;
  }>;
  instructions?: Array<{
    programId: string;
    accounts?: string[];
    innerInstructions?: Array<{
      programId: string;
      accounts?: string[];
    }>;
  }>;
}

export interface EarlyBundlerInfo {
  walletAddress: string;
  tokenAmount: number;
  signature: string;
  slot: number;
  timestamp: number;
  isMint: boolean;
  creatorVaultAddress?: string;
}

export interface HeliusProjectUsage {
  creditsUsed: number;
  creditsRemaining: number;
  prepaidCreditsUsed: number;
  prepaidCreditsRemaining: number;
  subscriptionDetails?: {
    plan?: string;
    creditsLimit?: number;
    billingCycle?: {
      start?: string;
      end?: string;
    };
  };
  usage?: Record<string, number>;
}

export interface HeliusCreditExhaustionInfo {
  projectId: string;
  label: string;
  usage: HeliusProjectUsage;
}

interface HeliusClientOptions {
  projectId?: string;
  label?: string;
  onCreditsExhausted?: (
    info: HeliusCreditExhaustionInfo,
  ) => void | Promise<void>;
}

export class HeliusClient {
  private readonly apiKey: string;
  private readonly baseUrl: string;
  private readonly projectId: string | null;
  private readonly label: string;
  private readonly onCreditsExhausted:
    | HeliusClientOptions['onCreditsExhausted']
    | null;
  private creditCheckPromise: Promise<void> | null = null;
  private lastCreditCheckAt = 0;
  private creditsExhaustedNotified = false;

  constructor(apiKey: string, options: HeliusClientOptions = {}) {
    this.apiKey = apiKey;
    this.baseUrl = 'https://api-mainnet.helius-rpc.com';
    this.projectId = options.projectId?.trim() || null;
    this.label = options.label?.trim() || 'Helius';
    this.onCreditsExhausted = options.onCreditsExhausted ?? null;
  }

  private async fetchWithCreditCheck(
    input: string,
    init?: RequestInit,
  ): Promise<Response> {
    const response = await fetch(input, init);
    if (response.status === 429) {
      await this.checkProjectCreditsAfter429();
    }
    return response;
  }

  private async checkProjectCreditsAfter429(): Promise<void> {
    if (this.creditsExhaustedNotified) return;
    if (!this.projectId) {
      log.warn(`${this.label} received Helius 429 but has no project ID configured`, {
        requiredSetting: 'INSIDER_HELIUS_PROJECT_ID[_2|_3|_4]',
      });
      return;
    }
    if (this.creditCheckPromise) {
      await this.creditCheckPromise;
      return;
    }
    if (Date.now() - this.lastCreditCheckAt < 30_000) return;

    this.lastCreditCheckAt = Date.now();
    this.creditCheckPromise = this.fetchProjectUsage()
      .then(async (usage) => {
        const creditsRemaining = Number(usage.creditsRemaining);
        const prepaidCreditsRemaining = Number(
          usage.prepaidCreditsRemaining,
        );
        const hasConfirmedBalances =
          Number.isFinite(creditsRemaining) &&
          Number.isFinite(prepaidCreditsRemaining);
        const exhausted =
          hasConfirmedBalances &&
          creditsRemaining <= 0 &&
          prepaidCreditsRemaining <= 0;

        log.warn(`${this.label} Helius project usage checked after 429`, {
          projectId: this.projectId,
          creditsUsed: usage.creditsUsed,
          creditsRemaining,
          prepaidCreditsUsed: usage.prepaidCreditsUsed,
          prepaidCreditsRemaining,
          exhausted,
        });
        if (!exhausted || this.creditsExhaustedNotified) return;

        this.creditsExhaustedNotified = true;
        await this.onCreditsExhausted?.({
          projectId: this.projectId!,
          label: this.label,
          usage,
        });
      })
      .catch((err) => {
        log.warn(`${this.label} Helius Admin credit check failed after 429`, {
          projectId: this.projectId,
          error: err instanceof Error ? err.message : String(err),
        });
      })
      .finally(() => {
        this.creditCheckPromise = null;
      });
    await this.creditCheckPromise;
  }

  async handlePossibleRateLimitError(error: unknown): Promise<void> {
    const message = error instanceof Error ? error.message : String(error);
    if (/\b429\b|too many requests/i.test(message)) {
      await this.checkProjectCreditsAfter429();
    }
  }

  private async fetchProjectUsage(): Promise<HeliusProjectUsage> {
    const projectId = encodeURIComponent(this.projectId!);
    const params = new URLSearchParams({ 'api-key': this.apiKey });
    const url =
      `https://admin-api.helius.xyz/v0/projects/${projectId}/usage?${params.toString()}`;
    const response = await globalThis.fetch(url, {
      headers: {
        Authorization: `Bearer ${this.apiKey}`,
        'X-API-Key': this.apiKey,
      },
    });
    if (!response.ok) {
      const text = await response.text();
      throw new Error(
        `Helius Admin API error: ${response.status} ${response.statusText} - ${text}`,
      );
    }
    return await response.json() as HeliusProjectUsage;
  }

  /**
   * Fetches the first 5 transactions for a token mint.
   * The first tx is the mint, the remaining 4 are early bundler wallets.
   */
  async getEarlyBundlers(mintAddress: string): Promise<EarlyBundlerInfo[]> {
    const url = `${this.baseUrl}/v0/addresses/${mintAddress}/transactions?token-accounts=none&sort-order=asc&limit=5&api-key=${this.apiKey}`;
    
    log.info(`Fetching early bundlers for mint ${mintAddress}`);
    
    let lastError: Error | null = null;
    
    // Retry up to 3 times with a delay, as tokens might be very new
    for (let attempt = 1; attempt <= 3; attempt++) {
      try {
        const response = await this.fetchWithCreditCheck(url);
        if (!response.ok) {
          const text = await response.text();
          throw new Error(`Helius API error: ${response.status} ${response.statusText} - ${text}`);
        }
        
        const data = await response.json() as HeliusTransaction[];
        log.info(`Received ${data.length} transactions from Helius (attempt ${attempt})`);
        
        if (data.length === 0) {
          throw new Error('No transactions found for mint yet');
        }
        
        return this.parseEarlyBundlers(data, mintAddress);
      } catch (err) {
        lastError = err instanceof Error ? err : new Error(String(err));
        log.warn(`Attempt ${attempt} failed to fetch early bundlers`, { error: lastError.message });
        
        if (attempt < 3) {
          await new Promise(resolve => setTimeout(resolve, attempt * 1000));
        }
      }
    }
    
    log.error('Failed to fetch early bundlers from Helius after all attempts', lastError);
    throw lastError;
  }

  async getTokenSystemTransfers(
    mintAddress: string,
    limit: number = 10,
    timeoutMs?: number
  ): Promise<HeliusTransaction[]> {
    const params = new URLSearchParams({
      'token-accounts': 'none',
      'sort-order': 'asc',
      'api-key': this.apiKey,
      limit: String(limit),
      type: 'TRANSFER',
      source: 'SYSTEM_PROGRAM',
    });
    const url = `${this.baseUrl}/v0/addresses/${mintAddress}/transactions?${params.toString()}`;

    const controller = timeoutMs ? new AbortController() : null;
    const timeout = controller
      ? setTimeout(() => controller.abort(), timeoutMs)
      : null;

    try {
      const response = await this.fetchWithCreditCheck(
        url,
        controller ? { signal: controller.signal } : undefined,
      );
      if (!response.ok) {
        const text = await response.text();
        throw new Error(`Helius API error: ${response.status} ${response.statusText} - ${text}`);
      }

      return await response.json() as HeliusTransaction[];
    } catch (err) {
      log.error(`Failed to fetch token system transfers for ${mintAddress}`, err);
      throw err;
    } finally {
      if (timeout) clearTimeout(timeout);
    }
  }

  async getTokenCreationTimestamp(mintAddress: string): Promise<number | null> {
    const params = new URLSearchParams({
      'token-accounts': 'none',
      'sort-order': 'asc',
      'api-key': this.apiKey,
      limit: '1',
    });
    const url = `${this.baseUrl}/v0/addresses/${mintAddress}/transactions?${params.toString()}`;

    try {
      const response = await this.fetchWithCreditCheck(url);
      if (!response.ok) {
        const text = await response.text();
        throw new Error(`Helius API error: ${response.status} ${response.statusText} - ${text}`);
      }

      const [first] = await response.json() as HeliusTransaction[];
      return first?.timestamp ?? null;
    } catch (err) {
      log.error(`Failed to fetch token creation timestamp for ${mintAddress}`, err);
      throw err;
    }
  }

  /**
   * Fetches early SWAP transactions for a token mint (skips CREATE).
   * Returns the first `swapLimit` SWAP txs after the mint CREATE tx.
   */
  async getMintCreateTransaction(mintAddress: string): Promise<HeliusTransaction | null> {
    const params = new URLSearchParams({
      'token-accounts': 'none',
      'sort-order': 'asc',
      'api-key': this.apiKey,
      limit: '5',
    });
    const url = `${this.baseUrl}/v0/addresses/${mintAddress}/transactions?${params.toString()}`;

    const response = await this.fetchWithCreditCheck(url);
    if (!response.ok) {
      const text = await response.text();
      throw new Error(`Helius API error: ${response.status} ${response.statusText} - ${text}`);
    }

    const data = await response.json() as HeliusTransaction[];
    return data.find((tx) => tx.type === 'CREATE') ?? data[0] ?? null;
  }

  async getEarlyInsiderSwaps(
    mintAddress: string,
    swapLimit: number = 4,
  ): Promise<HeliusTransaction[]> {
    const params = new URLSearchParams({
      'token-accounts': 'none',
      'sort-order': 'asc',
      'api-key': this.apiKey,
      limit: String(swapLimit + 1),
    });
    const url = `${this.baseUrl}/v0/addresses/${mintAddress}/transactions?${params.toString()}`;

    log.info(`Fetching early insider swaps for mint ${mintAddress}`);

    let lastError: Error | null = null;
    for (let attempt = 1; attempt <= 3; attempt++) {
      try {
        const response = await this.fetchWithCreditCheck(url);
        if (!response.ok) {
          const text = await response.text();
          throw new Error(`Helius API error: ${response.status} ${response.statusText} - ${text}`);
        }

        const data = await response.json() as HeliusTransaction[];
        const swaps = data.filter((tx) => tx.type === 'SWAP').slice(0, swapLimit);
        if (swaps.length === 0) {
          throw new Error('No SWAP transactions found for mint yet');
        }

        log.info(`Found ${swaps.length} early insider SWAP txs for ${mintAddress}`);
        return swaps;
      } catch (err) {
        lastError = err instanceof Error ? err : new Error(String(err));
        log.warn(`Attempt ${attempt} failed to fetch early insider swaps`, { error: lastError.message });
        if (attempt < 3) {
          await new Promise((resolve) => setTimeout(resolve, attempt * 1000));
        }
      }
    }

    throw lastError ?? new Error('Failed to fetch early insider swaps');
  }

  /**
   * Fetches recent transactions for a wallet address (newest first).
   */
  async getWalletTransactionsDesc(
    address: string,
    limit: number = 21,
  ): Promise<HeliusTransaction[]> {
    const params = new URLSearchParams({
      'token-accounts': 'none',
      'sort-order': 'desc',
      'api-key': this.apiKey,
      limit: String(limit),
    });
    const url = `${this.baseUrl}/v0/addresses/${address}/transactions?${params.toString()}`;

    let lastError: Error | null = null;
    for (let attempt = 1; attempt <= 3; attempt++) {
      try {
        const response = await this.fetchWithCreditCheck(url);
        if (!response.ok) {
          const text = await response.text();
          throw new Error(`Helius API error: ${response.status} ${response.statusText} - ${text}`);
        }

        const data = await response.json() as HeliusTransaction[];
        log.info(`Fetched ${data.length} transactions for ${address} (attempt ${attempt})`);
        return data;
      } catch (err) {
        lastError = err instanceof Error ? err : new Error(String(err));
        log.warn(`Attempt ${attempt} failed to fetch desc transactions for ${address}`, { error: lastError.message });
        if (attempt < 3) {
          await new Promise((resolve) => setTimeout(resolve, attempt * 1000));
        }
      }
    }

    throw lastError ?? new Error('Failed to fetch wallet transactions');
  }

  /**
   * Fetches recent transactions for a wallet address.
   */
  async getTransactionsForAddress(address: string, limit: number = 10): Promise<HeliusTransaction[]> {
    const url = `${this.baseUrl}/v0/addresses/${address}/transactions?api-key=${this.apiKey}&limit=${limit}`;
    
    try {
      const response = await this.fetchWithCreditCheck(url);
      if (!response.ok) {
        const text = await response.text();
        throw new Error(`Helius API error: ${response.status} ${response.statusText} - ${text}`);
      }
      
      return await response.json() as HeliusTransaction[];
    } catch (err) {
      log.error(`Failed to fetch transactions for ${address} from Helius`, err);
      throw err;
    }
  }

  async getAddressTransactionsAsc(
    address: string,
    afterSignature?: string,
    limit: number = 50,
  ): Promise<HeliusTransaction[]> {
    const params = new URLSearchParams({
      'token-accounts': 'none',
      'sort-order': 'asc',
      'api-key': this.apiKey,
      limit: String(limit),
    });
    if (afterSignature) {
      params.set('after-signature', afterSignature);
    }
    const url = `${this.baseUrl}/v0/addresses/${address}/transactions?${params.toString()}`;

    const response = await this.fetchWithCreditCheck(url);
    if (!response.ok) {
      const text = await response.text();
      throw new Error(`Helius API error: ${response.status} ${response.statusText} - ${text}`);
    }
    return await response.json() as HeliusTransaction[];
  }

  async getTransactionsBySignatures(signatures: string[]): Promise<HeliusTransaction[]> {
    if (signatures.length === 0) return [];
    const url = `${this.baseUrl}/v0/transactions?api-key=${this.apiKey}`;

    try {
      const response = await this.fetchWithCreditCheck(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ transactions: signatures }),
      });
      if (!response.ok) {
        const text = await response.text();
        throw new Error(`Helius API error: ${response.status} ${response.statusText} - ${text}`);
      }

      return await response.json() as HeliusTransaction[];
    } catch (err) {
      log.error(`Failed to fetch transactions by signature from Helius`, err);
      throw err;
    }
  }

  private parseEarlyBundlers(transactions: HeliusTransaction[], mintAddress: string): EarlyBundlerInfo[] {
    const bundlers: EarlyBundlerInfo[] = [];
    
    for (let i = 0; i < transactions.length; i++) {
      const tx = transactions[i];
      const isMint = i === 0; // First transaction is always the mint
      
      // Find the token transfer for this mint
      const tokenTransfer = tx.tokenTransfers?.find(
        transfer => transfer.mint === mintAddress
      );
      
      if (tokenTransfer) {
        bundlers.push({
          walletAddress: tokenTransfer.toUserAccount,
          tokenAmount: tokenTransfer.tokenAmount,
          signature: tx.signature,
          slot: tx.slot,
          timestamp: tx.timestamp,
          isMint,
          creatorVaultAddress: isMint ? this.extractCreatorVaultAddress(tx) : undefined,
        });
        
        log.info(`Found ${isMint ? 'mint' : 'bundler'}: ${tokenTransfer.toUserAccount} - ${tokenTransfer.tokenAmount} tokens`);
      }
    }
    
    return bundlers;
  }

  private extractCreatorVaultAddress(tx: HeliusTransaction): string | undefined {
    if (tx.type !== 'CREATE') return undefined;
    const transfers = tx.nativeTransfers ?? [];

    for (let i = 0; i < transfers.length - 1; i++) {
      const transfer = transfers[i];
      const nextTransfer = transfers[i + 1];
      if (
        transfer.amount === 890_880
        && transfer.toUserAccount
        && nextTransfer?.toUserAccount === transfer.toUserAccount
      ) {
        log.info(`Found creator vault address: ${transfer.toUserAccount}`);
        return transfer.toUserAccount;
      }
    }

    return undefined;
  }
}
