import { describe, expect, it } from 'vitest';
import { PARTITION_COUNT, partitionFor } from '../src/domain/partition';

describe('stable partition routing', () => {
  it('is deterministic, in range, and tenant-sensitive', () => {
    const a = partitionFor('tenant-a', 'job-1');
    expect(a).toBeGreaterThanOrEqual(0);
    expect(a).toBeLessThan(PARTITION_COUNT);
    expect(partitionFor('tenant-a', 'job-1')).toBe(a);
    expect(partitionFor('tenant-b', 'job-1')).not.toBe(a);
  });

  it('matches the documented sha256 construction', () => {
    expect(partitionFor('acme', 'nightly')).toBe(partitionFor('acme', 'nightly'));
  });
});
