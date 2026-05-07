import { describe, it, expect, vi, beforeEach } from 'vitest';

// ---------------------------------------------------------------------------
// The 0.3.0/0.3.1 wipe bug fired most easily when the read step inside set()
// failed transiently — the catch path treated it as "no existing doc" and the
// follow-up upsert wiped the real body. With patch-based set(), the same
// failure surface is replaced by patch's atomic semantics: a 404 on the doc
// no longer means "I'll write a stub for you", it means "skip silently".
//
// This test pins both halves of that contract:
//   1. A 404 from patch -> set() resolves without throwing, NO upsert fires.
//   2. A non-404 error from patch -> set() rethrows, NO upsert fires.
// ---------------------------------------------------------------------------
const { mockState, makeContainer } = vi.hoisted(() => {
  function makeContainer(name: string) {
    const upsertedItems: unknown[] = [];
    const patchCalls: { id: string; pk: unknown }[] = [];
    let nextPatchError: { code: number | string } | undefined;

    const container = {
      _name: name,
      upsertedItems,
      patchCalls,
      get nextPatchError() { return nextPatchError; },
      set nextPatchError(v: { code: number | string } | undefined) {
        nextPatchError = v;
      },
      items: {
        query: vi.fn().mockReturnValue({
          fetchAll: vi.fn().mockResolvedValue({ resources: [] }),
        }),
        upsert: vi.fn().mockImplementation(async (item: unknown) => {
          upsertedItems.push(item);
          return { resource: item };
        }),
      },
      item: vi.fn().mockImplementation((id: string, pk: unknown) => ({
        read: vi.fn().mockImplementation(async () => {
          // Should never be called by the new patch-based set().
          throw new Error('read() should not be invoked by set()');
        }),
        patch: vi.fn().mockImplementation(async () => {
          patchCalls.push({ id, pk });
          if (nextPatchError) {
            const err = new Error('forced') as Error & { code: number | string };
            err.code = nextPatchError.code;
            throw err;
          }
          return { resource: {} };
        }),
      })),
    };
    return container;
  }

  const mockState = { units: makeContainer('units') };
  return { mockState, makeContainer };
});

vi.mock('@azure/cosmos', () => {
  const databaseObj = {
    container: vi.fn(() => mockState.units),
  };
  return {
    CosmosClient: vi.fn().mockImplementation(() => ({
      database: vi.fn().mockReturnValue(databaseObj),
    })),
  };
});

import { CosmosClient } from '@azure/cosmos';
import { CosmosVectorEmbeddingStore } from '../src/CosmosVectorEmbeddingStore.js';

const baseRecord = {
  nodeId: 'cain',
  vector: [0.1, 0.2],
  meta: {
    model: 'm',
    modelVersion: 'v',
    contentHash: 'h',
    generatedAt: '2026-05-06T06:00:00.000Z',
  },
};

describe('CosmosVectorEmbeddingStore.set — patch error handling', () => {
  let client: CosmosClient;
  let store: CosmosVectorEmbeddingStore;

  beforeEach(() => {
    mockState.units = makeContainer('units');
    client = new CosmosClient({ endpoint: 'x', key: 'y' });
    store = new CosmosVectorEmbeddingStore({
      client,
      database: 'db',
      container: 'units',
    });
  });

  it('swallows 404 from patch and does NOT upsert (no stub-doc fallback)', async () => {
    mockState.units.nextPatchError = { code: 404 };
    await expect(store.set(baseRecord)).resolves.toBeUndefined();
    expect(mockState.units.patchCalls).toHaveLength(1);
    expect(mockState.units.upsertedItems).toEqual([]);
  });

  it('rethrows non-404 patch failures (propagates real outages)', async () => {
    mockState.units.nextPatchError = { code: 503 };
    await expect(store.set(baseRecord)).rejects.toThrow('forced');
    // The classic wipe path was: catch read error, then upsert. Pin that
    // upsert NEVER fires regardless of the failure code.
    expect(mockState.units.upsertedItems).toEqual([]);
  });
});
