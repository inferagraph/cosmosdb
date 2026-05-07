import { describe, it, expect, vi, beforeEach } from 'vitest';

// ---------------------------------------------------------------------------
// Regression test for the 0.3.0/0.3.1 wipe bug. The earlier `set()` did
// `read -> merge -> upsert` which atomically REPLACED the entire document.
// When the read failed (or returned partial data) the upsert wiped real body
// fields like `content` / `title` / `type` that the host had previously
// written. The fix uses Cosmos NoSQL `patch` so other fields are untouched.
//
// This file simulates real Cosmos `patch` semantics with a tiny in-memory
// document store and asserts that pre-existing fields survive `set()`.
// ---------------------------------------------------------------------------
const { mockState, makeContainer } = vi.hoisted(() => {
  type PatchOp =
    | { op: 'add'; path: string; value: unknown }
    | { op: 'remove'; path: string }
    | { op: 'replace'; path: string; value: unknown };

  function makeContainer(name: string) {
    const docs = new Map<string, Record<string, unknown>>();
    const upsertedItems: unknown[] = [];
    const patchCalls: { id: string; pk: unknown; ops: PatchOp[] }[] = [];
    let patchShouldThrow404 = false;

    const container = {
      _name: name,
      docs,
      upsertedItems,
      patchCalls,
      get patchShouldThrow404() { return patchShouldThrow404; },
      set patchShouldThrow404(v: boolean) { patchShouldThrow404 = v; },
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
          const doc = docs.get(id);
          if (!doc) {
            const err = new Error('NotFound') as Error & { code: number };
            err.code = 404;
            throw err;
          }
          return { resource: doc };
        }),
        // Real Cosmos patch: applies ops atomically to the existing doc.
        // 404 on missing doc; we throw so the store's catch path runs.
        patch: vi.fn().mockImplementation(async (ops: PatchOp[]) => {
          patchCalls.push({ id, pk, ops });
          if (patchShouldThrow404) {
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
  };
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

describe('CosmosVectorEmbeddingStore.set — preserves existing fields (regression)', () => {
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

  it('keeps content/title/type intact when writing an embedding to an existing doc', async () => {
    // Host previously seeded the body. This is what the wipe bug destroyed.
    mockState.units.docs.set('cain', {
      id: 'cain',
      content: 'CAIN MARKDOWN BODY',
      title: 'Cain',
      type: 'person',
    });

    await store.set({
      nodeId: 'cain',
      vector: [0.1, 0.2],
      meta: {
        model: 'text-embedding-3-large',
        modelVersion: '2024-10-21',
        contentHash: 'hash-cain',
        generatedAt: '2026-05-06T03:00:00.000Z',
      },
    });

    const doc = mockState.units.docs.get('cain');
    expect(doc).toBeDefined();
    // Pre-existing fields MUST survive.
    expect(doc!.content).toBe('CAIN MARKDOWN BODY');
    expect(doc!.title).toBe('Cain');
    expect(doc!.type).toBe('person');
    // New embedding fields MUST be present.
    expect(doc!.embedding).toEqual([0.1, 0.2]);
    expect(doc!.embeddingModel).toBe('text-embedding-3-large');
    expect(doc!.embeddingVersion).toBe('2024-10-21');
    expect(doc!.embeddingHash).toBe('hash-cain');
    expect(doc!.embeddingGeneratedAt).toBe('2026-05-06T03:00:00.000Z');
    // No upsert should ever fire — that's the wipe path.
    expect(mockState.units.upsertedItems).toHaveLength(0);
    // One atomic patch call should have been issued.
    expect(mockState.units.patchCalls).toHaveLength(1);
  });

  it('uses Cosmos patch (add ops) so OTHER fields on the document are untouched', async () => {
    mockState.units.docs.set('cain', {
      id: 'cain',
      content: 'BODY',
      title: 'Cain',
    });

    await store.set({
      nodeId: 'cain',
      vector: [0.5, 0.6],
      meta: {
        model: 'm',
        modelVersion: 'v',
        contentHash: 'h',
        generatedAt: '2026-05-06T04:00:00.000Z',
      },
    });

    expect(mockState.units.patchCalls).toHaveLength(1);
    const ops = mockState.units.patchCalls[0].ops;
    // Every op must be `add` (creates-or-replaces single field, never the doc).
    for (const op of ops) expect(op.op).toBe('add');
    const paths = ops.map(o => o.path).sort();
    expect(paths).toEqual(
      [
        '/embedding',
        '/embeddingGeneratedAt',
        '/embeddingHash',
        '/embeddingModel',
        '/embeddingVersion',
      ].sort(),
    );
    // No upsert ever.
    expect(mockState.units.upsertedItems).toHaveLength(0);
  });

  it('skips silently when the document is missing (no stub doc creation)', async () => {
    // Host has not seeded this node's body yet. The 0.3.1 behavior was to
    // upsert a stub `{id}` doc; that masked real wipe scenarios in production.
    // The fix: no stub creation — host owns body lifecycle, embedding writes
    // are additive only.
    mockState.units.patchShouldThrow404 = true;

    await expect(
      store.set({
        nodeId: 'orphan',
        vector: [1, 2, 3],
        meta: {
          model: 'm',
          modelVersion: 'v',
          contentHash: 'h',
          generatedAt: '2026-05-06T05:00:00.000Z',
        },
      }),
    ).resolves.toBeUndefined();

    // No upsert, no document created.
    expect(mockState.units.upsertedItems).toHaveLength(0);
    expect(mockState.units.docs.get('orphan')).toBeUndefined();
    // Patch was attempted exactly once (then 404'd).
    expect(mockState.units.patchCalls).toHaveLength(1);
  });
});
