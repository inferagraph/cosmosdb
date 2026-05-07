import { describe, it, expect, vi, beforeEach } from 'vitest';

// ---------------------------------------------------------------------------
// Coverage for the non-404 delete failure paths in CosmosInferredEdgeStore.
// Both `set()` (drop-and-replace) and `clear()` iterate existing rows and
// delete each, tolerating 404 (already-gone) but rethrowing every other code
// so a partial wipe in the face of a real outage is visible to the caller.
// ---------------------------------------------------------------------------
const { mockState, makeContainer } = vi.hoisted(() => {
  function makeContainer() {
    let queryResources: unknown[] = [];
    let nextDeleteError: { code: number | string } | undefined;
    const itemDeleted: { id: string }[] = [];

    const container = {
      itemDeleted,
      get queryResources() { return queryResources; },
      set queryResources(v: unknown[]) { queryResources = v; },
      get nextDeleteError() { return nextDeleteError; },
      set nextDeleteError(v: { code: number | string } | undefined) { nextDeleteError = v; },
      items: {
        upsert: vi.fn().mockResolvedValue({ resource: {} }),
        query: vi.fn().mockImplementation(() => ({
          fetchAll: vi.fn().mockResolvedValue({ resources: queryResources }),
        })),
      },
      item: vi.fn().mockImplementation((id: string) => ({
        read: vi.fn().mockResolvedValue({ resource: undefined }),
        delete: vi.fn().mockImplementation(async () => {
          itemDeleted.push({ id });
          if (nextDeleteError) {
            const err = new Error('forced-delete') as Error & { code: number | string };
            err.code = nextDeleteError.code;
            throw err;
          }
          return {};
        }),
      })),
    };
    return container;
  }
  const mockState = { inferredEdges: makeContainer() };
  return { mockState, makeContainer };
});

vi.mock('@azure/cosmos', () => {
  const databaseObj = { container: vi.fn(() => mockState.inferredEdges) };
  return {
    CosmosClient: vi.fn().mockImplementation(() => ({
      database: vi.fn().mockReturnValue(databaseObj),
    })),
  };
});

import { CosmosClient } from '@azure/cosmos';
import { CosmosInferredEdgeStore } from '../src/CosmosInferredEdgeStore.js';

describe('CosmosInferredEdgeStore — non-404 delete failure paths', () => {
  let store: CosmosInferredEdgeStore;

  beforeEach(() => {
    mockState.inferredEdges = makeContainer();
    const client = new CosmosClient({ endpoint: 'x', key: 'y' });
    store = new CosmosInferredEdgeStore({ client, database: 'db' });
  });

  it('set() rethrows a non-404 delete failure during the drop-and-replace step', async () => {
    mockState.inferredEdges.queryResources = [
      { id: 'old-1', sourceId: 'x' },
    ];
    mockState.inferredEdges.nextDeleteError = { code: 500 };
    await expect(
      store.set([
        { sourceId: 'a', targetId: 'b', type: 'r', score: 0.5, sources: ['embedding'] },
      ]),
    ).rejects.toThrow('forced-delete');
  });

  it('clear() rethrows a non-404 delete failure (does not silently continue)', async () => {
    mockState.inferredEdges.queryResources = [{ id: 'a-b-r', sourceId: 'a' }];
    mockState.inferredEdges.nextDeleteError = { code: 503 };
    await expect(store.clear()).rejects.toThrow('forced-delete');
  });

  it('respects an embedding path without leading slash (config.embeddingPath = "embedding")', async () => {
    // Pins the constructor's path-normalization branch: when the host omits
    // the leading slash, the SQL accessor must still read `c.embedding` and
    // not `c.` (a stray dot). Verified via the `searchInferredEdges` query
    // shape since that's where fieldName is interpolated.
    const client = new CosmosClient({ endpoint: 'x', key: 'y' });
    const slashlessStore = new CosmosInferredEdgeStore({
      client,
      database: 'db',
      embeddingPath: 'embedding',
    });
    mockState.inferredEdges.queryResources = [];
    await slashlessStore.searchInferredEdges([0.1, 0.2], 3);
    const queryArg = (mockState.inferredEdges.items.query as unknown as {
      mock: { calls: [{ query: string }][] };
    }).mock.calls[0][0];
    expect(queryArg.query).toMatch(/c\.embedding/);
    expect(queryArg.query).not.toMatch(/c\.\./);
  });
});
