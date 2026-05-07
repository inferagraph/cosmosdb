import { describe, it, expect, vi, beforeEach } from 'vitest';

// ---------------------------------------------------------------------------
// Contract pin-down for inferred-edge `set(edges)`. Inferred edges are
// computed entirely by the indexer; there are no host-owned fields on these
// documents to preserve. The documented contract is "delete-then-upsert
// bulk replace", and that's what this test locks in.
//
// (Audit conclusion for the 0.3.2 data-safety pass: CosmosInferredEdgeStore
// is safe as-is.)
// ---------------------------------------------------------------------------
const { mockState, makeContainer } = vi.hoisted(() => {
  function makeContainer() {
    const queryResources: unknown[][] = [];
    const upsertedItems: unknown[] = [];
    const itemDeleted: { id: string }[] = [];
    let queryCount = 0;
    return {
      queryResources,
      upsertedItems,
      itemDeleted,
      items: {
        query: vi.fn().mockImplementation(() => {
          const idx = queryCount++;
          return { fetchAll: vi.fn().mockResolvedValue({ resources: queryResources[idx] ?? [] }) };
        }),
        upsert: vi.fn().mockImplementation(async (item: unknown) => {
          upsertedItems.push(item);
          return { resource: item };
        }),
      },
      item: vi.fn().mockImplementation((id: string) => ({
        delete: vi.fn().mockImplementation(async () => {
          itemDeleted.push({ id });
          return {};
        }),
      })),
    };
  }
  const mockState = { edges: makeContainer() };
  return { mockState, makeContainer };
});

vi.mock('@azure/cosmos', () => {
  const databaseObj = { container: vi.fn(() => mockState.edges) };
  return {
    CosmosClient: vi.fn().mockImplementation(() => ({
      database: vi.fn().mockReturnValue(databaseObj),
    })),
  };
});

import { CosmosClient } from '@azure/cosmos';
import { CosmosInferredEdgeStore } from '../src/CosmosInferredEdgeStore.js';

describe('CosmosInferredEdgeStore.set — contract: bulk replace is the documented behavior', () => {
  let store: CosmosInferredEdgeStore;

  beforeEach(() => {
    mockState.edges = makeContainer();
    const client = new CosmosClient({ endpoint: 'x', key: 'y' });
    store = new CosmosInferredEdgeStore({ client, database: 'db' });
  });

  it('drops the prior set entirely (inferred-edge documents are end-to-end owned by the indexer)', async () => {
    // Any old row must be dropped; new rows are upserted. Pin the contract so
    // a future "preserve fields" change has to be explicit and intentional.
    mockState.edges.queryResources[0] = [
      { id: 'old-1', sourceId: 'x', targetId: 'y', type: 't' },
    ];
    await store.set([
      {
        sourceId: 'a',
        targetId: 'b',
        type: 'related_to',
        score: 0.5,
        sources: ['embedding'],
      },
    ]);
    expect(mockState.edges.itemDeleted.map(d => d.id)).toEqual(['old-1']);
    expect(mockState.edges.upsertedItems).toHaveLength(1);
  });
});
