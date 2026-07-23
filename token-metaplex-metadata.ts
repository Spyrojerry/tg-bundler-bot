// ─────────────────────────────────────────────────────────────────────────────
//  token-metaplex-metadata.ts — Metaplex metadata URI via Helius RPC
//  (PDA derive + getAccountInfo decode — same result as mpl-token-metadata)
// ─────────────────────────────────────────────────────────────────────────────

import { Connection, PublicKey } from '@solana/web3.js';
import { createLogger } from './logger';

const log = createLogger('TOKEN-METADATA');

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
  private readonly connection: Connection;

  constructor(rpcUrl: string) {
    this.connection = new Connection(rpcUrl, 'confirmed');
  }

  async fetchTokenMetadataUri(mintAddress: string): Promise<TokenMetadataUriResult | null> {
    let mint: PublicKey;
    try {
      mint = new PublicKey(mintAddress);
    } catch {
      log.warn('Invalid mint for metadata lookup', { mintAddress });
      return null;
    }

    const metadataPda = findMetadataPda(mint);
    const account = await this.connection.getAccountInfo(metadataPda, 'confirmed');
    if (!account?.data?.length) {
      return null;
    }

    const uri = decodeMetadataUri(Buffer.from(account.data));
    if (!uri) return null;

    const metadataUrl = toIpfsIoUrl(uri);
    return {
      uri,
      metadataUrl,
      metadataPda: metadataPda.toBase58(),
    };
  }
}

export function findMetadataPda(mint: PublicKey): PublicKey {
  const [pda] = PublicKey.findProgramAddressSync(
    [Buffer.from('metadata'), METADATA_PROGRAM_ID.toBuffer(), mint.toBuffer()],
    METADATA_PROGRAM_ID,
  );
  return pda;
}

/** Borsh-style string: u32 LE length + utf8 bytes (Metaplex Data.name/symbol/uri). */
function readRustString(
  data: Buffer,
  offset: number,
): { value: string; offset: number } | null {
  if (offset + 4 > data.length) return null;
  const len = data.readUInt32LE(offset);
  offset += 4;
  if (len < 0 || offset + len > data.length) return null;
  const value = data.subarray(offset, offset + len).toString('utf8');
  return { value, offset: offset + len };
}

export function decodeMetadataUri(accountData: Buffer): string | null {
  if (accountData.length < 65) return null;
  let offset = 1 + 32 + 32;

  const name = readRustString(accountData, offset);
  if (!name) return null;
  offset = name.offset;

  const symbol = readRustString(accountData, offset);
  if (!symbol) return null;
  offset = symbol.offset;

  const uri = readRustString(accountData, offset);
  if (!uri?.value) return null;

  return uri.value.replace(/\0/g, '').trim();
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
