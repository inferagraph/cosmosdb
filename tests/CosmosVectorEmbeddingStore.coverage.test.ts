import { describe, it, expect, vi, beforeEach } from 'vitest';

// ---------------------------------------------------------------------------
// Coverage for paths in CosmosVectorEmbeddingStore that the main suite does
// not exercise:
//   - `similar(queryVector, k)` delegating to `searchVector` (kept on the
//     EmbeddingStore contract for parity even though prod calls searchVector
//     directly).
//   - `is400OrPathNotFound` recognizing a 400-coded error during `clear()`
//     (the Cosmos NoSQL "path not found" surface for missing JSON Patch
//     remove targets).
//   - constructor's path-normalization branch when the host passes
//     `embeddingPath` without a leading slash.
//   - `clear()` rethrowing when patch fails with a non-400/non-404 code.
// ---------------------------------------------------------------------------
const { mockState, makeContainer } = vi.hoisted(() => {
  type PatchOp =
    | { op: 'add'; path: string; value: unknown }
    | { op: 'remove'; path: string };

  function makeContainer() {
    const docs = new Map<string, Record<string, unknown>>();
    let queryResources: unknown[] = [];
    const queryCalls: { spec: { query: string } }[] = [];
    const patchCalls: { id: string; pk: unknown; ops: PatchOp[] }[] = [];
    let nextPatchError: { code: number | string; message?: string } | undefined;

    const container = {
      docs,
      queryCalls,
      patchCalls,
      get queryResources() { return queryResources; },
      set queryResources(v: unknown[]) { queryResources = v; },
      get nextPatchError() { return nextPatchError; },
      set nextPatchError(v: { code: number | string; message?: string } | undefined) {
        nextPatchError = v;
      },
      items: {
        upsert: vi.fn().mockResolvedValue({ resource: {} }),
        query: vi.fn().mockImplementation((spec: { query: string }) => {
          queryCalls.push({ spec });
          return { fetchAll: vi.fn().mockResolvedValue({ resources: queryResources }) };
        }),
      },
      item: vi.fn().mockImplementation((id: string, pk: unknown) => ({
        read: vi.fn().mockResolvedValue({ resource: undefined }),
        delete: vi.fn().mockResolvedValue({}),
        patch: vi.fn().mockImplementation(async (ops: PatchOp[]) => {
          patchCalls.push({ id, pk, ops });
          if (nextPatchError) {
            const err = new Error(nextPatchError.message ?? 'forced-patch') as Error & {
              code: number | string;
              message: string;
            };
            err.code = nextPatchError.code;
            throw err;
          }
          return { resource: docs.get(id) ?? {} };
        }),
      })),
    };
    return container;
  }
  const mockState = { units: makeContainer() };
  return { mockState, makeContainer };
});

vi.mock('@azure/cosmos', () => {
  const databaseObj = { container: vi.fn(() => mockState.units) };
  return {
    CosmosClient: vi.fn().mockImplementation(() => ({
      database: vi.fn().mockReturnValue(databaseObj),
    })),
  };
});

import { CosmosClient } from '@azure/cosmos';
import { CosmosVectorEmbeddingStore } from '../src/CosmosVectorEmbeddingStore.js';

describe('CosmosVectorEmbeddingStore — coverage gaps', () => {
  let client: CosmosClient;
  let store: CosmosVectorEmbeddingStore;

  beforeEach(() => {
    mockState.units = makeContainer();
    client = new CosmosClient({ endpoint: 'x', key: 'y' });
    store = new CosmosVectorEmbeddingStore({
      client,
      database: 'db',
      container: 'units',
    });
  });

  describe('similar()', () => {
    it('delegates to searchVector and maps hits to {nodeId, score}', async () => {
      // similar() lives on the EmbeddingStore contract for parity with
      // non-vector stores. It must call searchVector under the hood and
      // ignore the optional model/version filter args (persisted entries
      // already carry that scope via embeddingModel + embeddingVersion).
      mockState.units.queryResources = [
        { id: 'a', score: 0.10 },
        { id: 'b', score: 0.91 },
      ];
      const hits = await store.similar([0.1, 0.2], 5, 'some-model', 'some-version');
      expect(hits.map(h => h.nodeId)).toEqual(['b', 'a']);
      // The underlying SQL is the same shape the searchVector tests pin.
      expect(mockState.units.queryCalls[0].spec.query).toMatch(/SELECT\s+TOP\s+@k/i);
    });
  });

  describe('clear() error tolerance', () => {
    it('tolerates a 400-coded "path not found" patch failure (idempotent cleanup)', async () => {
      // Cosmos returns HTTP 400 when a JSON Patch `remove` op targets a path
      // the doc does not have. The store treats that as benign — the field
      // is already absent so the cleanup goal is met. A bare 400 with no
      // message keyword should still be caught (the helper handles 400 OR
      // path-not-found message).
      mockState.units.queryResources = [{ id: 'doc-1' }];
      mockState.units.nextPatchError = { code: 400 };
      await expect(store.clear()).resolves.toBeUndefined();
    });

    it('rethrows a non-404, non-400 patch failure during clear()', async () => {
      // 503 (or any other code) is a real outage and must propagate so the
      // host knows the cleanup did not complete.
      mockState.units.queryResources = [{ id: 'doc-1' }];
      mockState.units.nextPatchError = { code: 503 };
      await expect(store.clear()).rejects.toThrow();
    });
  });

  describe('embeddingPath normalization', () => {
    it('handles a path without a leading slash by stripping nothing (uses literal field name)', async () => {
      // Pins the constructor branch where path.startsWith('/') is false.
      const slashlessStore = new CosmosVectorEmbeddingStore({
        client,
        database: 'db',
        container: 'units',
        embeddingPath: 'embedding',
      });
      mockState.units.queryResources = [];
      await slashlessStore.searchVector([0.1], { top: 1 });
      const sql = mockState.units.queryCalls[0].spec.query;
      expect(sql).toMatch(/c\.embedding/);
      // No accidental double-dot or stray slash.
      expect(sql).not.toMatch(/c\.\./);
      expect(sql).not.toMatch(/c\./.source + '/');
    });
  });
});
