import { describe, it, expect, vi, beforeEach } from 'vitest';

const { mockState, makeContainer } = vi.hoisted(() => {
  type QueryCall = { spec: unknown };

  function makeContainer(name: string) {
    const queryCalls: QueryCall[] = [];
    const queryResources: unknown[][] = [];
    const upsertedItems: unknown[] = [];
    const itemReads = new Map<string, unknown>();
    const itemDeleted: { id: string; pk?: unknown }[] = [];
    let deleteShouldThrow404 = false;
    let readShouldThrow404 = false;

    const container = {
      _name: name,
      queryCalls,
      queryResources,
      upsertedItems,
      itemReads,
      itemDeleted,
      get readShouldThrow404() { return readShouldThrow404; },
      set readShouldThrow404(v: boolean) { readShouldThrow404 = v; },
      get deleteShouldThrow404() { return deleteShouldThrow404; },
      set deleteShouldThrow404(v: boolean) { deleteShouldThrow404 = v; },
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
          if (container.readShouldThrow404) {
            const err = new Error('NotFound') as Error & { code: number };
            err.code = 404;
            throw err;
          }
          return { resource: itemReads.get(id) };
        }),
        delete: vi.fn().mockImplementation(async () => {
          itemDeleted.push({ id, pk });
          if (container.deleteShouldThrow404) {
            const err = new Error('NotFound') as Error & { code: number };
            err.code = 404;
            throw err;
          }
          return {};
        }),
      })),
    };
    return container;
  }

  const mockState = {
    inferredEdges: makeContainer('inferred_edges'),
  };
  return { mockState, makeContainer };
});

vi.mock('@azure/cosmos', () => {
  const databaseObj = {
    container: vi.fn(() => mockState.inferredEdges),
  };
  return {
    CosmosClient: vi.fn().mockImplementation(() => ({
      database: vi.fn().mockReturnValue(databaseObj),
    })),
  };
});

import { CosmosClient } from '@azure/cosmos';
import { CosmosInferredEdgeStore } from '../src/CosmosInferredEdgeStore.js';

describe('CosmosInferredEdgeStore', () => {
  let client: CosmosClient;
  let store: CosmosInferredEdgeStore;

  beforeEach(() => {
    mockState.inferredEdges = makeContainer('inferred_edges');
    client = new CosmosClient({ endpoint: 'x', key: 'y' });
    store = new CosmosInferredEdgeStore({
      client,
      database: 'db',
    });
  });

  describe('get(sourceId, targetId)', () => {
    it('returns undefined when nothing matches', async () => {
      mockState.inferredEdges.queryResources[0] = [];
      const edge = await store.get('a', 'b');
      expect(edge).toBeUndefined();
    });

    it('returns the edge when present', async () => {
      mockState.inferredEdges.queryResources[0] = [
        {
          id: 'a-b-related_to',
          sourceId: 'a',
          targetId: 'b',
          type: 'related_to',
          score: 0.82,
          sources: ['embedding'],
          description: 'A is related to B',
        },
      ];
      const edge = await store.get('a', 'b');
      expect(edge).toBeDefined();
      expect(edge!.sourceId).toBe('a');
      expect(edge!.targetId).toBe('b');
      expect(edge!.type).toBe('related_to');
      expect(edge!.score).toBeCloseTo(0.82);
      expect(edge!.sources).toEqual(['embedding']);
    });
  });

  describe('getAllForNode', () => {
    it('returns every edge incident to a node in either direction', async () => {
      mockState.inferredEdges.queryResources[0] = [
        { id: 'a-b-r', sourceId: 'a', targetId: 'b', type: 'r', score: 0.5, sources: ['embedding'] },
        { id: 'c-a-r', sourceId: 'c', targetId: 'a', type: 'r', score: 0.4, sources: ['graph'] },
      ];
      const edges = await store.getAllForNode('a');
      expect(edges).toHaveLength(2);
      const call = mockState.inferredEdges.queryCalls[0].spec as {
        query: string;
        parameters: { name: string; value: unknown }[];
      };
      expect(call.query).toMatch(/sourceId\s*=\s*@id\s+OR\s+c\.targetId\s*=\s*@id/i);
      expect(call.parameters).toEqual(
        expect.arrayContaining([{ name: '@id', value: 'a' }]),
      );
    });
  });

  describe('getAll', () => {
    it('returns every stored edge', async () => {
      mockState.inferredEdges.queryResources[0] = [
        { id: 'a-b-r', sourceId: 'a', targetId: 'b', type: 'r', score: 0.5, sources: ['embedding'] },
      ];
      const edges = await store.getAll();
      expect(edges).toHaveLength(1);
    });
  });

  describe('set(edges)', () => {
    it('replaces the entire stored set with the given edges', async () => {
      // Existing rows that need to be cleared first
      mockState.inferredEdges.queryResources[0] = [
        { id: 'old-1', sourceId: 'x', targetId: 'y', type: 't', score: 0.1, sources: ['graph'] },
      ];
      await store.set([
        {
          sourceId: 'a',
          targetId: 'b',
          type: 'related_to',
          score: 0.7,
          sources: ['embedding'],
          reasoning: 'A and B share setting',
        },
      ]);
      // One delete for 'old-1', one upsert for new edge
      expect(mockState.inferredEdges.itemDeleted.map(d => d.id)).toEqual(['old-1']);
      expect(mockState.inferredEdges.upsertedItems).toHaveLength(1);
      const item = mockState.inferredEdges.upsertedItems[0] as Record<string, unknown>;
      expect(item.id).toBe('a-b-related_to');
      expect(item.sourceId).toBe('a');
      expect(item.targetId).toBe('b');
      expect(item.type).toBe('related_to');
      expect(item.score).toBeCloseTo(0.7);
      expect(item.sources).toEqual(['embedding']);
      expect(item.reasoning).toBe('A and B share setting');
    });
  });

  describe('clear', () => {
    it('deletes every stored edge and is idempotent on already-empty containers', async () => {
      mockState.inferredEdges.queryResources[0] = [];
      await store.clear();
      expect(mockState.inferredEdges.itemDeleted).toHaveLength(0);
    });

    it('deletes each existing row by id', async () => {
      mockState.inferredEdges.queryResources[0] = [
        { id: 'a-b-r', sourceId: 'a' },
        { id: 'b-c-r', sourceId: 'b' },
      ];
      await store.clear();
      expect(mockState.inferredEdges.itemDeleted.map(d => d.id).sort()).toEqual(['a-b-r', 'b-c-r']);
    });
  });

  describe('searchInferredEdges', () => {
    it('issues vector SQL against the inferred_edges container', async () => {
      mockState.inferredEdges.queryResources[0] = [
        { id: 'e1', score: 0.91 },
        { id: 'e2', score: 0.42 },
      ];
      const hits = await store.searchInferredEdges([0.1, 0.2, 0.3], 5);
      const call = mockState.inferredEdges.queryCalls[0].spec as {
        query: string;
        parameters: { name: string; value: unknown }[];
      };
      expect(call.query).toMatch(/SELECT\s+TOP\s+@k/i);
      expect(call.query).toMatch(/VectorDistance\s*\(\s*c\.embedding\s*,\s*@q\s*\)/i);
      expect(call.parameters).toEqual(
        expect.arrayContaining([
          { name: '@k', value: 5 },
          { name: '@q', value: [0.1, 0.2, 0.3] },
        ]),
      );
      // Sorted descending by score
      expect(hits.map(h => h.nodeId)).toEqual(['e1', 'e2']);
      expect(hits[0].score).toBeCloseTo(0.91);
    });
  });
});
