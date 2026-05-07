import { describe, it, expect, vi, beforeEach } from 'vitest';

// ---------------------------------------------------------------------------
// Shared mock state for the @azure/cosmos client. Tests reset these in
// beforeEach so every test starts from a clean slate. Each container exposes
// `items` and `item(id, partitionKey)` plumbing similar to real Cosmos.
// ---------------------------------------------------------------------------
const { mockState, makeContainer } = vi.hoisted(() => {
  type QueryCall = { spec: unknown };
  type PatchOp =
    | { op: 'add'; path: string; value: unknown }
    | { op: 'remove'; path: string }
    | { op: 'replace'; path: string; value: unknown };

  function makeContainer(name: string) {
    const queryCalls: QueryCall[] = [];
    const queryResources: unknown[][] = [];
    const upsertedItems: unknown[] = [];
    const docs = new Map<string, Record<string, unknown>>();
    const itemDeleted: string[] = [];
    const patchCalls: { id: string; pk: unknown; ops: PatchOp[] }[] = [];
    let patchShouldThrow404 = false;
    let itemReadShouldThrow404 = false;

    const container = {
      _name: name,
      queryCalls,
      queryResources,
      upsertedItems,
      docs,
      itemDeleted,
      patchCalls,
      get patchShouldThrow404() { return patchShouldThrow404; },
      set patchShouldThrow404(v: boolean) { patchShouldThrow404 = v; },
      get itemReadShouldThrow404() { return itemReadShouldThrow404; },
      set itemReadShouldThrow404(v: boolean) { itemReadShouldThrow404 = v; },
      items: {
        query: vi.fn().mockImplementation((spec: unknown) => {
          queryCalls.push({ spec });
          const idx = queryCalls.length - 1;
          const resources = queryResources[idx] ?? [];
          return { fetchAll: vi.fn().mockResolvedValue({ resources }) };
        }),
        upsert: vi.fn().mockImplementation(async (item: unknown) => {
          upsertedItems.push(item);
          return { resource: item };
        }),
      },
      item: vi.fn().mockImplementation((id: string, pk?: unknown) => ({
        read: vi.fn().mockImplementation(async () => {
          if (container.itemReadShouldThrow404) {
            const err = new Error('NotFound') as Error & { code: number };
            err.code = 404;
            throw err;
          }
          return { resource: docs.get(id) };
        }),
        delete: vi.fn().mockImplementation(async () => {
          itemDeleted.push(id);
          if (container.itemReadShouldThrow404) {
            const err = new Error('NotFound') as Error & { code: number };
            err.code = 404;
            throw err;
          }
          return {};
        }),
        // Cosmos NoSQL JSON Patch — applies ops atomically to the doc.
        patch: vi.fn().mockImplementation(async (ops: PatchOp[]) => {
          patchCalls.push({ id, pk, ops });
          if (container.patchShouldThrow404) {
            const err = new Error('NotFound') as Error & { code: number };
            err.code = 404;
            throw err;
          }
          const doc = docs.get(id);
          if (!doc) {
            const err = new Error('NotFound') as Error & { code: number };
            err.code = 404;
            throw err;
          }
          for (const op of ops) {
            const field = op.path.startsWith('/') ? op.path.slice(1) : op.path;
            if (op.op === 'add' || op.op === 'replace') {
              doc[field] = op.value;
            } else if (op.op === 'remove') {
              if (!(field in doc)) {
                // Real Cosmos returns 400 with "path not found" — we throw a
                // 404-coded error to keep the mock simple; the store's
                // remove-path-not-found tolerance treats 400 and 404 alike.
                const err = new Error('PathNotFound') as Error & { code: number };
                err.code = 404;
                throw err;
              }
              delete doc[field];
            }
          }
          return { resource: doc };
        }),
      })),
    };

    return container;
  }

  const mockState = {
    units: makeContainer('units'),
    inferredEdges: makeContainer('inferred_edges'),
  };

  return { mockState, makeContainer };
});

vi.mock('@azure/cosmos', () => {
  const databaseObj = {
    container: vi.fn((name: string) => {
      if (name === 'inferred_edges') return mockState.inferredEdges;
      return mockState.units;
    }),
  };
  return {
    CosmosClient: vi.fn().mockImplementation(() => ({
      database: vi.fn().mockReturnValue(databaseObj),
    })),
  };
});

import { CosmosClient } from '@azure/cosmos';
import { CosmosVectorEmbeddingStore } from '../src/CosmosVectorEmbeddingStore.js';

describe('CosmosVectorEmbeddingStore', () => {
  let client: CosmosClient;
  let store: CosmosVectorEmbeddingStore;

  beforeEach(() => {
    mockState.units = makeContainer('units');
    mockState.inferredEdges = makeContainer('inferred_edges');
    client = new CosmosClient({ endpoint: 'x', key: 'y' });
    store = new CosmosVectorEmbeddingStore({
      client,
      database: 'db',
      container: 'units',
    });
  });

  // ---- get() ----
  describe('get', () => {
    it('returns undefined when document is missing', async () => {
      mockState.units.queryResources[0] = [];
      const record = await store.get('n1', 'text-embedding-3-large', '', 'hash-abc');
      expect(record).toBeUndefined();
    });

    it('returns undefined when document exists but contentHash differs', async () => {
      // The query asks for a doc with matching id AND matching hash;
      // a stale doc returns no row from the query.
      mockState.units.queryResources[0] = [];
      const record = await store.get('n1', 'text-embedding-3-large', '', 'hash-new');
      expect(record).toBeUndefined();
    });

    it('returns the embedding record when contentHash matches', async () => {
      mockState.units.queryResources[0] = [
        {
          id: 'n1',
          embedding: [0.1, 0.2, 0.3],
          embeddingModel: 'text-embedding-3-large',
          embeddingVersion: '2024-10-21',
          embeddingHash: 'hash-abc',
          embeddingGeneratedAt: '2026-05-06T00:00:00.000Z',
        },
      ];
      const record = await store.get('n1', 'text-embedding-3-large', '2024-10-21', 'hash-abc');
      expect(record).toBeDefined();
      expect(record!.nodeId).toBe('n1');
      expect(record!.vector).toEqual([0.1, 0.2, 0.3]);
      expect(record!.meta.model).toBe('text-embedding-3-large');
      expect(record!.meta.modelVersion).toBe('2024-10-21');
      expect(record!.meta.contentHash).toBe('hash-abc');
      expect(record!.meta.generatedAt).toBe('2026-05-06T00:00:00.000Z');
    });

    it('issues a SQL query that filters by id, model, version, and embeddingHash', async () => {
      mockState.units.queryResources[0] = [];
      await store.get('n1', 'text-embedding-3-large', '2024-10-21', 'hash-abc');
      const call = mockState.units.queryCalls[0].spec as {
        query: string;
        parameters: { name: string; value: unknown }[];
      };
      expect(call.query).toMatch(/WHERE\s+c\.id\s*=\s*@id/i);
      expect(call.query).toMatch(/c\.embeddingHash\s*=\s*@hash/i);
      expect(call.parameters).toEqual(
        expect.arrayContaining([
          { name: '@id', value: 'n1' },
          { name: '@model', value: 'text-embedding-3-large' },
          { name: '@modelVersion', value: '2024-10-21' },
          { name: '@hash', value: 'hash-abc' },
        ]),
      );
    });
  });

  // ---- set() ----
  describe('set', () => {
    it('patches the embedding fields onto the document, leaving other fields intact', async () => {
      mockState.units.docs.set('n1', { id: 'n1', name: 'Cain' });
      await store.set({
        nodeId: 'n1',
        vector: [0.11, 0.22, 0.33],
        meta: {
          model: 'text-embedding-3-large',
          modelVersion: '2024-10-21',
          contentHash: 'hash-abc',
          generatedAt: '2026-05-06T01:00:00.000Z',
        },
      });
      // No upsert ever (the wipe path).
      expect(mockState.units.upsertedItems).toHaveLength(0);
      expect(mockState.units.patchCalls).toHaveLength(1);
      const doc = mockState.units.docs.get('n1');
      expect(doc!.id).toBe('n1');
      expect(doc!.name).toBe('Cain');
      expect(doc!.embedding).toEqual([0.11, 0.22, 0.33]);
      expect(doc!.embeddingModel).toBe('text-embedding-3-large');
      expect(doc!.embeddingVersion).toBe('2024-10-21');
      expect(doc!.embeddingHash).toBe('hash-abc');
      expect(doc!.embeddingGeneratedAt).toBe('2026-05-06T01:00:00.000Z');
    });

    it('skips silently when the document does not exist (no stub-doc creation)', async () => {
      // Host has not seeded this node's body. The 0.3.1 behavior was to upsert
      // a stub `{id}` doc, which masked real wipe scenarios in production. The
      // fix: skip — host owns body lifecycle, embedding writes are additive.
      mockState.units.patchShouldThrow404 = true;
      await store.set({
        nodeId: 'orphan',
        vector: [1, 2, 3],
        meta: {
          model: 'm',
          modelVersion: 'v',
          contentHash: 'h',
          generatedAt: '2026-05-06T02:00:00.000Z',
        },
      });
      expect(mockState.units.upsertedItems).toHaveLength(0);
      expect(mockState.units.docs.get('orphan')).toBeUndefined();
    });

    it('passes the doc id as the partition key (default /id partitioning)', async () => {
      mockState.units.docs.set('n1', { id: 'n1' });
      await store.set({
        nodeId: 'n1',
        vector: [0.1],
        meta: { model: 'm', modelVersion: 'v', contentHash: 'h', generatedAt: 't' },
      });
      const call = mockState.units.patchCalls[0];
      expect(call.id).toBe('n1');
      expect(call.pk).toBe('n1');
    });
  });

  // ---- searchVector() ----
  describe('searchVector', () => {
    it('issues SQL using VectorDistance and TOP @k against the units container', async () => {
      mockState.units.queryResources[0] = [];
      await store.searchVector([0.1, 0.2], { top: 5 });
      const call = mockState.units.queryCalls[0].spec as {
        query: string;
        parameters: { name: string; value: unknown }[];
      };
      expect(call.query).toMatch(/SELECT\s+TOP\s+@k/i);
      expect(call.query).toMatch(/VectorDistance\s*\(\s*c\.embedding\s*,\s*@q\s*\)/i);
      expect(call.query).toMatch(/ORDER\s+BY\s+VectorDistance\s*\(\s*c\.embedding\s*,\s*@q\s*\)/i);
      expect(call.parameters).toEqual(
        expect.arrayContaining([
          { name: '@k', value: 5 },
          { name: '@q', value: [0.1, 0.2] },
        ]),
      );
    });

    it('returns hits sorted by descending similarity', async () => {
      // Cosmos VectorDistance returns the configured distance/similarity score
      // as a column literally aliased "score". The store exposes it directly
      // and sorts descending.
      mockState.units.queryResources[0] = [
        { id: 'a', score: 0.10 },
        { id: 'b', score: 0.91 },
        { id: 'c', score: 0.55 },
      ];
      const hits = await store.searchVector([0.1, 0.2], { top: 3 });
      expect(hits.map(h => h.nodeId)).toEqual(['b', 'c', 'a']);
      expect(hits[0].score).toBeCloseTo(0.91);
    });

    it('queries the inferred_edges container when container option is "inferred_edges"', async () => {
      mockState.inferredEdges.queryResources[0] = [{ id: 'e1', score: 0.7 }];
      const hits = await store.searchVector([0.1, 0.2], { top: 5, container: 'inferred_edges' });
      expect(mockState.inferredEdges.queryCalls).toHaveLength(1);
      expect(mockState.units.queryCalls).toHaveLength(0);
      expect(hits[0].nodeId).toBe('e1');
    });
  });
});
