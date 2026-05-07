import { describe, it, expect, vi, beforeEach } from 'vitest';

// ---------------------------------------------------------------------------
// Sibling regression for `clear()`. Pre-fix, `clear()` did
//   for each doc: read -> strip embedding fields -> upsert
// which meant a transient read failure (or a malformed read result) wiped
// everything else on the doc. The fix uses Cosmos patch `remove` ops so the
// clean operation only touches the embedding fields it owns.
// ---------------------------------------------------------------------------
const { mockState, makeContainer } = vi.hoisted(() => {
  type PatchOp =
    | { op: 'add'; path: string; value: unknown }
    | { op: 'remove'; path: string };

  function makeContainer(name: string) {
    const docs = new Map<string, Record<string, unknown>>();
    const upsertedItems: unknown[] = [];
    const patchCalls: { id: string; pk: unknown; ops: PatchOp[] }[] = [];
    let queryResources: unknown[] = [];

    const container = {
      _name: name,
      docs,
      upsertedItems,
      patchCalls,
      get queryResources() { return queryResources; },
      set queryResources(v: unknown[]) { queryResources = v; },
      items: {
        query: vi.fn().mockImplementation(() => ({
          fetchAll: vi.fn().mockResolvedValue({ resources: queryResources }),
        })),
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
        patch: vi.fn().mockImplementation(async (ops: PatchOp[]) => {
          patchCalls.push({ id, pk, ops });
          const doc = docs.get(id);
          if (!doc) {
            const err = new Error('NotFound') as Error & { code: number };
            err.code = 404;
            throw err;
          }
          for (const op of ops) {
            const field = op.path.startsWith('/') ? op.path.slice(1) : op.path;
            if (op.op === 'add') {
              doc[field] = (op as { value: unknown }).value;
            } else if (op.op === 'remove') {
              if (!(field in doc)) {
                // Cosmos returns 400 with "path not found"; mock as 400.
                const err = new Error('PathNotFound') as Error & {
                  code: number;
                  message: string;
                };
                err.code = 400;
                err.message = 'path not found';
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

describe('CosmosVectorEmbeddingStore.clear — preserves existing fields (regression)', () => {
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

  it('removes embedding fields without touching content/title/type', async () => {
    mockState.units.docs.set('cain', {
      id: 'cain',
      content: 'CAIN MARKDOWN BODY',
      title: 'Cain',
      type: 'person',
      embedding: [0.1, 0.2],
      embeddingModel: 'text-embedding-3-large',
      embeddingVersion: '2024-10-21',
      embeddingHash: 'hash',
      embeddingGeneratedAt: '2026-05-06T07:00:00.000Z',
    });
    mockState.units.queryResources = [{ id: 'cain' }];

    await store.clear();

    const doc = mockState.units.docs.get('cain');
    expect(doc).toBeDefined();
    // Body fields untouched.
    expect(doc!.content).toBe('CAIN MARKDOWN BODY');
    expect(doc!.title).toBe('Cain');
    expect(doc!.type).toBe('person');
    // Embedding fields gone.
    expect(doc!.embedding).toBeUndefined();
    expect(doc!.embeddingModel).toBeUndefined();
    expect(doc!.embeddingVersion).toBeUndefined();
    expect(doc!.embeddingHash).toBeUndefined();
    expect(doc!.embeddingGeneratedAt).toBeUndefined();
    // Path: zero upserts. Patch only.
    expect(mockState.units.upsertedItems).toHaveLength(0);
    expect(mockState.units.patchCalls.length).toBeGreaterThanOrEqual(1);
  });

  it('tolerates docs that are missing some embedding fields (path-not-found is harmless)', async () => {
    // Only `embedding` and `embeddingModel` exist; the others were never
    // written. clear() must still leave the doc intact (just with the present
    // fields removed) and not throw.
    mockState.units.docs.set('partial', {
      id: 'partial',
      content: 'BODY',
      embedding: [0.5],
      embeddingModel: 'm',
    });
    mockState.units.queryResources = [{ id: 'partial' }];

    await expect(store.clear()).resolves.toBeUndefined();

    const doc = mockState.units.docs.get('partial');
    expect(doc!.content).toBe('BODY');
    expect(doc!.embedding).toBeUndefined();
    expect(doc!.embeddingModel).toBeUndefined();
  });
});
