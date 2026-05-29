// ─────────────────────────────────────────────────────────────────────────────
//  helius-client.ts  —  Helius API client for fetching early bundler wallets
// ─────────────────────────────────────────────────────────────────────────────

import { createLogger } from './logger';

const log = createLogger('HELIUS');

export interface HeliusTransaction {
  signature: string;
  slot: number;
  timestamp: number;
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
  type?: string;
}

export interface EarlyBundlerInfo {
  walletAddress: string;
  tokenAmount: number;
  signature: string;
  slot: number;
  timestamp: number;
  isMint: boolean;
}

export class HeliusClient {
  private readonly apiKey: string;
  private readonly baseUrl: string;

  constructor(apiKey: string) {
    this.apiKey = apiKey;
    this.baseUrl = 'https://api-mainnet.helius-rpc.com';
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
        const response = await fetch(url);
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

  /**
   * Fetches recent transactions for a wallet address.
   */
  async getTransactionsForAddress(address: string, limit: number = 10): Promise<HeliusTransaction[]> {
    const url = `${this.baseUrl}/v0/addresses/${address}/transactions?api-key=${this.apiKey}&limit=${limit}`;
    
    try {
      const response = await fetch(url);
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
        });
        
        log.info(`Found ${isMint ? 'mint' : 'bundler'}: ${tokenTransfer.toUserAccount} - ${tokenTransfer.tokenAmount} tokens`);
      }
    }
    
    return bundlers;
  }
}
