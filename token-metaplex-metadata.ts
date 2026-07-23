// ─────────────────────────────────────────────────────────────────────────────
//  token-metaplex-metadata.ts — Token metadata URI via Helius DAS getAsset
// ─────────────────────────────────────────────────────────────────────────────

import { PublicKey } from '@solana/web3.js';
import { createLogger } from './logger';

const log = createLogger('TOKEN-METADATA');
const REQUEST_TIMEOUT_MS = 2_000;

const METADATA_PROGRAM_ID = new PublicKey(
  'metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s',
);

/** Required normalized metadata gateway prefix (ipfs.io + CIDv1 `baf…`). */
export const REQUIRED_IPFS_IO_BAF_URI_PREFIX = 'https://ipfs.io/ipfs/baf';

export interface TokenMetadataUriResult {
  uri: string;
  metadataUrl: string;
  metadataPda: string;
}

export class TokenMetaplexMetadataClient {
  private readonly endpoint: string | null;

  constructor(apiKey: string | null | undefined) {
    const key = apiKey?.trim();
    this.endpoint = key
      ? `https://mainnet.helius-rpc.com/?api-key=${encodeURIComponent(key)}`
      : null;
  }

  isConfigured(): boolean {
    return this.endpoint !== null;
  }

  async fetchTokenMetadataUri(mintAddress: string): Promise<TokenMetadataUriResult | null> {
    if (!this.endpoint) {
      log.warn('Helius DAS getAsset not configured for metadata lookup');
      return null;
    }

    let mint: PublicKey;
    try {
      mint = new PublicKey(mintAddress);
    } catch {
      log.warn('Invalid mint for metadata lookup', { mintAddress });
      return null;
    }

    const metadataPda = findMetadataPda(mint);
    const uri = await this.fetchJsonUriFromDas(mintAddress);
    if (!uri) return null;

    const metadataUrl = toIpfsIoUrl(uri);
    return {
      uri,
      metadataUrl,
      metadataPda: metadataPda.toBase58(),
    };
  }

  private async fetchJsonUriFromDas(mint: string): Promise<string | null> {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
    try {
      const resp = await fetch(this.endpoint!, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          jsonrpc: '2.0',
          id: 'follow-token-metadata',
          method: 'getAsset',
          params: { id: mint },
        }),
        signal: controller.signal,
      });

      const text = await resp.text();
      if (!resp.ok) {
        log.debug('Helius DAS getAsset HTTP error', {
          mint,
          status: resp.status,
          body: text.slice(0, 200),
        });
        return null;
      }

      let json: unknown;
      try {
        json = JSON.parse(text);
      } catch {
        log.debug('Helius DAS getAsset returned malformed JSON', { mint });
        return null;
      }

      const record = getRecord(json);
      const error = getRecord(record.error);
      if ('code' in error && typeof error.message === 'string' && error.message.length > 0) {
        log.debug('Helius DAS getAsset RPC error', {
          mint,
          code: error.code,
          message: error.message,
        });
        return null;
      }

      const uri = extractJsonUriFromAsset(getRecord(record.result));
      if (!uri) {
        log.debug('Helius DAS getAsset missing content.json_uri', { mint });
        return null;
      }

      return uri;
    } catch (err) {
      if (err instanceof Error && err.name === 'AbortError') {
        log.debug('Helius DAS getAsset timed out', {
          mint,
          timeoutMs: REQUEST_TIMEOUT_MS,
        });
        return null;
      }
      log.debug('Helius DAS getAsset failed', {
        mint,
        error: err instanceof Error ? err.message : String(err),
      });
      return null;
    } finally {
      clearTimeout(timer);
    }
  }
}

export function findMetadataPda(mint: PublicKey): PublicKey {
  const [pda] = PublicKey.findProgramAddressSync(
    [Buffer.from('metadata'), METADATA_PROGRAM_ID.toBuffer(), mint.toBuffer()],
    METADATA_PROGRAM_ID,
  );
  return pda;
}

function extractJsonUriFromAsset(asset: Record<string, unknown>): string | null {
  const content = getRecord(asset.content);
  const jsonUri =
    pickNonEmptyString(content.json_uri) ??
    pickNonEmptyString(content.jsonUri);
  if (jsonUri) return jsonUri;

  const files = content.files;
  if (Array.isArray(files)) {
    for (const file of files) {
      const uri = pickNonEmptyString(getRecord(file).uri);
      if (uri) return uri;
    }
  }

  return null;
}

function pickNonEmptyString(value: unknown): string | null {
  if (typeof value !== 'string') return null;
  const trimmed = value.replace(/\0/g, '').trim();
  return trimmed.length > 0 ? trimmed : null;
}

function getRecord(value: unknown): Record<string, unknown> {
  return value && typeof value === 'object' ? (value as Record<string, unknown>) : {};
}

export function toIpfsIoUrl(uri: string): string {
  const cleanUri = uri.replace(/\0/g, '').trim();

  if (cleanUri.startsWith('ipfs://')) {
    return `https://ipfs.io/ipfs/${cleanUri.slice('ipfs://'.length)}`;
  }

  const ipfsPath = cleanUri.match(/\/ipfs\/(.+)$/i);
  if (ipfsPath) {
    return `https://ipfs.io/ipfs/${ipfsPath[1]}`;
  }

  return cleanUri;
}

export function hasRequiredIpfsIoBafUri(metadataUrl: string): boolean {
  return metadataUrl
    .trim()
    .toLowerCase()
    .startsWith(REQUIRED_IPFS_IO_BAF_URI_PREFIX.toLowerCase());
}
