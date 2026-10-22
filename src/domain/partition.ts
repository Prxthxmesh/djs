import { createHash } from 'node:crypto';

export const PARTITION_COUNT = 64;

/** Stable across processes and languages: sha256(tenant + NUL + job), first 8 bytes mod 64. */
export function partitionFor(tenantId: string, jobId: string): number {
  const digest = createHash('sha256').update(tenantId).update('\u0000').update(jobId).digest();
  return Number(digest.readBigUInt64BE(0) % BigInt(PARTITION_COUNT));
}
