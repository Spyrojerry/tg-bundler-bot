// ─────────────────────────────────────────────────────────────────────────────
//  bitquery-client.ts — Bitquery GraphQL (Pump.fun creator aggregates)
// ─────────────────────────────────────────────────────────────────────────────

import { createLogger } from './logger';

const log = createLogger('BITQUERY');

/** Bitquery V2 streaming GraphQL — OAuth Bearer token, not v1 API key. */
const DEFAULT_BITQUERY_GRAPHQL_URL = 'https://streaming.bitquery.io/graphql';

/** Pump.fun program — create / create_v2 instructions. */
const PUMP_FUN_PROGRAM_ADDRESS = '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P';

const PUMP_CREATOR_COUNT_QUERY = `
query CreatorCount($wallet: String!) {
  Solana(dataset: combined, network: solana) {
    Instructions(
      where: {
        Instruction: {
          Program: {
            Address: { is: "${PUMP_FUN_PROGRAM_ADDRESS}" }
            Method: { in: ["create", "create_v2"] }
          }
        }
        Transaction: {
          Signer: { is: $wallet }
          Result: { Success: true }
        }
      }
    ) {
      count
    }
  }
}
`;

interface CreatorCountResponse {
  data?: {
    Solana?: {
      Instructions?: Array<{ count?: number | null } | null> | null;
    } | null;
  } | null;
  errors?: Array<{ message?: string }>;
}

function normalizeBitqueryAccessToken(raw: string): string {
  const trimmed = raw.trim();
  return trimmed.replace(/^Bearer\s+/i, '');
}

export class BitqueryClient {
  private readonly accessToken: string;
  private readonly graphqlUrl: string;

  constructor(accessToken: string, graphqlUrl = DEFAULT_BITQUERY_GRAPHQL_URL) {
    this.accessToken = normalizeBitqueryAccessToken(accessToken);
    this.graphqlUrl = graphqlUrl.trim() || DEFAULT_BITQUERY_GRAPHQL_URL;
    if (!this.accessToken) {
      throw new Error('BITQUERY_ACCESS_TOKEN is empty after trim');
    }
  }

  /** Count successful Pump.fun create / create_v2 instructions signed by `wallet`. */
  async countPumpFunTokensCreatedByWallet(wallet: string): Promise<number> {
    const response = await fetch(this.graphqlUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Accept: 'application/json',
        Authorization: `Bearer ${this.accessToken}`,
      },
      body: JSON.stringify({
        query: PUMP_CREATOR_COUNT_QUERY,
        variables: { wallet },
      }),
    });

    if (!response.ok) {
      const text = await response.text();
      const authHint =
        response.status === 403
          ? ' — verify BITQUERY_ACCESS_TOKEN is a V2 OAuth token (ory_at_...) from https://account.bitquery.io/, sent to https://streaming.bitquery.io/graphql with Authorization: Bearer'
          : '';
      throw new Error(
        `Bitquery API error: ${response.status} ${response.statusText} - ${text}${authHint}`,
      );
    }

    const payload = (await response.json()) as CreatorCountResponse;
    if (payload.errors?.length) {
      throw new Error(
        `Bitquery GraphQL error: ${payload.errors.map((e) => e.message).join('; ')}`,
      );
    }

    const count = payload.data?.Solana?.Instructions?.[0]?.count;
    if (typeof count !== 'number' || !Number.isFinite(count)) {
      log.warn('Bitquery creator count missing in response; treating as 0', {
        wallet,
      });
      return 0;
    }

    return count;
  }
}
