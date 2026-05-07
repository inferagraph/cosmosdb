import { describe, it, expect, vi, beforeEach } from 'vitest';

// ---------------------------------------------------------------------------
// Contract pin-down for the cache provider's `set()`. The cache document is
// owned end-to-end by the provider — there are no host-controlled fields to
// preserve, so a full upsert is the documented contract. This test pins that
// behavior so any future shift to a patch-based shape stays intentional.
//
// (Audit conclusion for the 0.3.2 data-safety pass: CosmosCacheProvider is
// safe as-is.)
// ---------------------------------------------------------------------------
const { mockState, makeContainer } = vi.hoisted(() => {
  function makeContainer() {
    const docs = new Map<string, Record<string, unknown>>();
    const upsertCalls: Record<string, unknown>[] = [];
    return {
      docs,
      upsertCalls,
      items: {
        upsert: vi.fn().mockImplementation(async (item: Record<string, unknown>) => {
          upsertCalls.push({ ...item });
          docs.set(String(item.id), { ...item });
          return { resource: item };
        }),
        query: vi.fn().mockReturnValue({
          fetchAll: vi.fn().mockResolvedValue({ resources: [] }),
        }),
      },
      item: vi.fn(),
    };
  }
  const mockState = { cache: makeContainer() };
  return { mockState, makeContainer };
});

vi.mock('@azure/cosmos', () => {
  const databaseObj = { container: vi.fn(() => mockState.cache) };
  return {
    CosmosClient: vi.fn().mockImplementation(() => ({
      database: vi.fn().mockReturnValue(databaseObj),
    })),
  };
});

import { CosmosClient } from '@azure/cosmos';
import { CosmosCacheProvider } from '../src/CosmosCacheProvider.js';

describe('CosmosCacheProvider.set — contract: full upsert is the documented behavior', () => {
  let provider: CosmosCacheProvider;

  beforeEach(() => {
    mockState.cache = makeContainer();
    const client = new CosmosClient({ endpoint: 'x', key: 'y' });
    provider = new CosmosCacheProvider({ client, database: 'db' });
  });

  it('replaces the entire cache doc on each set (cache provider owns the document end-to-end)', async () => {
    // Even if some "extra" field were present, cache entries are
    // provider-owned — the contract is "the cache stores {id, pk, value, ttl?}"
    // and replaces in full. Document this so future maintainers know it is
    // intentional, not the same wipe pattern as the embedding store.
    await provider.set('k1', 'v1', { ttlSeconds: 60 });
    expect(mockState.cache.upsertCalls).toHaveLength(1);
    const doc = mockState.cache.upsertCalls[0];
    expect(doc).toEqual({ id: 'k1', pk: 'k1', value: 'v1', ttl: 60 });
  });
});
