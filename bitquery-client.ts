// ─────────────────────────────────────────────────────────────────────────────
//  bitquery-client.ts — Bitquery GraphQL (Pump.fun creator aggregates)
// ─────────────────────────────────────────────────────────────────────────────

import { createLogger } from './logger';

const log = createLogger('BITQUERY');

const BITQUERY_GRAPHQL_URL = 'https://streaming.bitquery.io/graphql';

const PUMP_CREATOR_COUNT_QUERY = `
query CreatorCount($wallet: String!) {
  Solana(network: solana) {
    Instructions(
      where: {
        Instruction: {
          Program: {
            Name: { is: "pump" }
            Method: { in: ["create", "create_v2"] }
          }
        }
        Transaction: {
          Signer: { is: $wallet }
          Result: { Success: true }
        }
      }
    ) {
      tokensCreated: count
    }
  }
}
`;

interface CreatorCountResponse {
  data?: {
    Solana?: {
      Instructions?: Array<{ tokensCreated?: number | null } | null> | null;
    } | null;
  } | null;
  errors?: Array<{ message?: string }>;
}

export class BitqueryClient {
  constructor(private readonly accessToken: string) {}

  /** Count successful Pump.fun create / create_v2 instructions signed by `wallet`. */
  async countPumpFunTokensCreatedByWallet(wallet: string): Promise<number> {
    const response = await fetch(BITQUERY_GRAPHQL_URL, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${this.accessToken}`,
      },
      body: JSON.stringify({
        query: PUMP_CREATOR_COUNT_QUERY,
        variables: { wallet },
      }),
    });

    if (!response.ok) {
      const text = await response.text();
      throw new Error(
        `Bitquery API error: ${response.status} ${response.statusText} - ${text}`,
      );
    }

    const payload = (await response.json()) as CreatorCountResponse;
    if (payload.errors?.length) {
      throw new Error(
        `Bitquery GraphQL error: ${payload.errors.map((e) => e.message).join('; ')}`,
      );
    }

    const count = payload.data?.Solana?.Instructions?.[0]?.tokensCreated;
    if (typeof count !== 'number' || !Number.isFinite(count)) {
      log.warn('Bitquery creator count missing in response; treating as 0', {
        wallet,
      });
      return 0;
    }

    return count;
  }
}
