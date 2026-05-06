import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

// Mock data
const mockNodeDoc1 = {
  id: 'n1',
  _docType: 'node',
  _rid: 'rid1',
  _self: 'self1',
  _etag: 'etag1',
  _attachments: 'att1',
  _ts: 1000,
  name: 'Node One',
  type: 'person',
};

const mockNodeDoc2 = {
  id: 'n2',
  _docType: 'node',
  _rid: 'rid2',
  _self: 'self2',
  _etag: 'etag2',
  _attachments: 'att2',
  _ts: 2000,
  name: 'Node Two',
  type: 'place',
};

const mockNodeDoc3 = {
  id: 'n3',
  _docType: 'node',
  _rid: 'rid3',
  _self: 'self3',
  _etag: 'etag3',
  _attachments: 'att3',
  _ts: 3000,
  name: 'Node Three',
  type: 'person',
};

const mockEdgeDoc1 = {
  id: 'e1',
  _docType: 'edge',
  _rid: 'erid1',
  _self: 'eself1',
  _etag: 'eetag1',
  _attachments: 'eatt1',
  _ts: 1000,
  sourceId: 'n1',
  targetId: 'n2',
  type: 'related_to',
};

const mockEdgeDoc2 = {
  id: 'e2',
  _docType: 'edge',
  _rid: 'erid2',
  _self: 'eself2',
  _etag: 'eetag2',
  _attachments: 'eatt2',
  _ts: 2000,
  sourceId: 'n2',
  targetId: 'n3',
  type: 'connected_to',
};

const mockContentDoc = {
  id: 'n1',
  content: 'Some content about Node One.',
  contentType: 'markdown',
};

// Use vi.hoisted to create shared state accessible inside vi.mock factory
const { mockState, createMockItems } = vi.hoisted(() => {
  function createMockItems(resources: unknown[]) {
    return {
      query: vi.fn().mockReturnValue({
        fetchAll: vi.fn().mockResolvedValue({ resources }),
      }),
    };
  }

  const mockState = {
    containerItems: createMockItems([]),
    edgesContainerItems: createMockItems([]),
  };

  return { mockState, createMockItems };
});

vi.mock('@azure/cosmos', () => {
  const mockContainerObj = {
    get items() { return mockState.containerItems; },
  };
  const mockEdgesContainerObj = {
    get items() { return mockState.edgesContainerItems; },
  };
  const mockDatabaseObj = {
    container: vi.fn((name: string) => {
      if (name === 'edges') return mockEdgesContainerObj;
      return mockContainerObj;
    }),
  };

  return {
    CosmosClient: vi.fn().mockImplementation(() => ({
      database: vi.fn().mockReturnValue(mockDatabaseObj),
    })),
    Container: vi.fn(),
    Database: vi.fn(),
  };
});

import { CosmosDataSource } from '../src/CosmosDataSource.js';

describe('CosmosDataSource', () => {
  let datasource: CosmosDataSource;
  const config = {
    endpoint: 'https://test.documents.azure.com:443/',
    key: 'test-key',
    database: 'test-db',
    container: 'test-container',
  };

  beforeEach(() => {
    mockState.containerItems = createMockItems([]);
    mockState.edgesContainerItems = createMockItems([]);
    datasource = new CosmosDataSource(config);
  });

  // --- name property ---
  it('should have name "cosmosdb"', () => {
    expect(datasource.name).toBe('cosmosdb');
  });

  // --- Lifecycle ---
  describe('connect / disconnect / isConnected', () => {
    it('should not be connected initially', () => {
      expect(datasource.isConnected()).toBe(false);
    });

    it('should be connected after connect()', async () => {
      await datasource.connect();
      expect(datasource.isConnected()).toBe(true);
    });

    it('should not be connected after disconnect()', async () => {
      await datasource.connect();
      await datasource.disconnect();
      expect(datasource.isConnected()).toBe(false);
    });

    it('should set up edgesContainer when configured', async () => {
      const dsWithEdges = new CosmosDataSource({
        ...config,
        edgesContainer: 'edges',
      });
      await dsWithEdges.connect();
      expect(dsWithEdges.isConnected()).toBe(true);
    });
  });

  // --- ensureConnected ---
  describe('ensureConnected', () => {
    it('should throw when not connected', async () => {
      await expect(datasource.getNode('n1')).rejects.toThrow(
        'CosmosDataSource is not connected. Call connect() first.',
      );
    });

    it('should throw for getInitialView when not connected', async () => {
      await expect(datasource.getInitialView()).rejects.toThrow(
        'CosmosDataSource is not connected.',
      );
    });

    it('should throw for getNeighbors when not connected', async () => {
      await expect(datasource.getNeighbors('n1')).rejects.toThrow(
        'CosmosDataSource is not connected.',
      );
    });

    it('should throw for findPath when not connected', async () => {
      await expect(datasource.findPath('n1', 'n2')).rejects.toThrow(
        'CosmosDataSource is not connected.',
      );
    });

    it('should throw for search when not connected', async () => {
      await expect(datasource.search('test')).rejects.toThrow(
        'CosmosDataSource is not connected.',
      );
    });

    it('should throw for filter when not connected', async () => {
      await expect(datasource.filter({})).rejects.toThrow(
        'CosmosDataSource is not connected.',
      );
    });

    it('should throw for getContent when not connected', async () => {
      await expect(datasource.getContent('n1')).rejects.toThrow(
        'CosmosDataSource is not connected.',
      );
    });
  });

  // --- getInitialView ---
  describe('getInitialView', () => {
    it('should query nodes with default limit 100', async () => {
      await datasource.connect();
      mockState.containerItems = createMockItems([mockNodeDoc1]);

      const result = await datasource.getInitialView();

      expect(result.nodes).toHaveLength(1);
      expect(result.nodes[0].id).toBe('n1');
      expect(result.nodes[0].attributes).toEqual({ name: 'Node One', type: 'person' });
      expect(mockState.containerItems.query).toHaveBeenCalledWith(
        expect.stringContaining('SELECT TOP 100'),
      );
    });

    it('should use custom limit from config', async () => {
      await datasource.connect();
      mockState.containerItems = createMockItems([]);

      await datasource.getInitialView({ limit: 50 });

      expect(mockState.containerItems.query).toHaveBeenCalledWith(
        expect.stringContaining('SELECT TOP 50'),
      );
    });

    it('should return edges connecting returned nodes', async () => {
      await datasource.connect();

      let callCount = 0;
      mockState.containerItems = {
        query: vi.fn().mockImplementation(() => {
          callCount++;
          if (callCount === 1) {
            return { fetchAll: vi.fn().mockResolvedValue({ resources: [mockNodeDoc1, mockNodeDoc2] }) };
          }
          return { fetchAll: vi.fn().mockResolvedValue({ resources: [mockEdgeDoc1] }) };
        }),
      };

      const result = await datasource.getInitialView();
      expect(result.nodes).toHaveLength(2);
      expect(result.edges).toHaveLength(1);
      expect(result.edges[0].sourceId).toBe('n1');
      expect(result.edges[0].targetId).toBe('n2');
    });

    it('should return empty edges when no nodes', async () => {
      await datasource.connect();
      mockState.containerItems = createMockItems([]);

      const result = await datasource.getInitialView();
      expect(result.nodes).toHaveLength(0);
      expect(result.edges).toHaveLength(0);
    });
  });

  // --- getNode ---
  describe('getNode', () => {
    it('should return a node when found', async () => {
      await datasource.connect();
      mockState.containerItems = createMockItems([mockNodeDoc1]);

      const node = await datasource.getNode('n1');
      expect(node).toBeDefined();
      expect(node!.id).toBe('n1');
      expect(node!.attributes.name).toBe('Node One');
      // Cosmos metadata should be stripped
      expect(node!.attributes).not.toHaveProperty('_rid');
      expect(node!.attributes).not.toHaveProperty('_self');
      expect(node!.attributes).not.toHaveProperty('_etag');
      expect(node!.attributes).not.toHaveProperty('_ts');
      expect(node!.attributes).not.toHaveProperty('_docType');
    });

    it('should return undefined when not found', async () => {
      await datasource.connect();
      mockState.containerItems = createMockItems([]);

      const node = await datasource.getNode('nonexistent');
      expect(node).toBeUndefined();
    });
  });

  // --- getNeighbors ---
  describe('getNeighbors', () => {
    it('should find neighbors of a node', async () => {
      await datasource.connect();

      let callCount = 0;
      mockState.containerItems = {
        query: vi.fn().mockImplementation(() => {
          callCount++;
          if (callCount === 1) {
            return { fetchAll: vi.fn().mockResolvedValue({ resources: [mockEdgeDoc1] }) };
          }
          return { fetchAll: vi.fn().mockResolvedValue({ resources: [mockNodeDoc1, mockNodeDoc2] }) };
        }),
      };

      const result = await datasource.getNeighbors('n1');
      expect(result.nodes).toHaveLength(2);
      expect(result.edges).toHaveLength(1);
    });

    it('should include the requested node in results', async () => {
      await datasource.connect();

      let callCount = 0;
      mockState.containerItems = {
        query: vi.fn().mockImplementation(() => {
          callCount++;
          if (callCount === 1) {
            return { fetchAll: vi.fn().mockResolvedValue({ resources: [mockEdgeDoc1] }) };
          }
          return { fetchAll: vi.fn().mockResolvedValue({ resources: [mockNodeDoc1, mockNodeDoc2] }) };
        }),
      };

      const result = await datasource.getNeighbors('n1');
      const nodeIds = result.nodes.map(n => n.id);
      expect(nodeIds).toContain('n1');
    });

    it('should use separate edgesContainer when configured', async () => {
      const dsWithEdges = new CosmosDataSource({
        ...config,
        edgesContainer: 'edges',
      });

      await dsWithEdges.connect();

      mockState.edgesContainerItems = {
        query: vi.fn().mockReturnValue({
          fetchAll: vi.fn().mockResolvedValue({ resources: [mockEdgeDoc1] }),
        }),
      };
      mockState.containerItems = {
        query: vi.fn().mockReturnValue({
          fetchAll: vi.fn().mockResolvedValue({ resources: [mockNodeDoc1, mockNodeDoc2] }),
        }),
      };

      const result = await dsWithEdges.getNeighbors('n1');
      expect(result.nodes).toHaveLength(2);
      expect(mockState.edgesContainerItems.query).toHaveBeenCalled();
    });

    it('should expand to depth 2 via BFS, returning 2-hop neighbors', async () => {
      await datasource.connect();

      // Order of calls when getNeighbors('n1', 2) runs (single container):
      //   1. edges for n1 -> [e1]            (n1 <-> n2)
      //   2. edges for n2 -> [e1, e2]         (n2 <-> n3)  -> e1 deduped
      //   3. final node fetch -> [n1, n2, n3]
      let callCount = 0;
      mockState.containerItems = {
        query: vi.fn().mockImplementation(() => {
          callCount++;
          if (callCount === 1) {
            return { fetchAll: vi.fn().mockResolvedValue({ resources: [mockEdgeDoc1] }) };
          }
          if (callCount === 2) {
            return { fetchAll: vi.fn().mockResolvedValue({ resources: [mockEdgeDoc1, mockEdgeDoc2] }) };
          }
          return { fetchAll: vi.fn().mockResolvedValue({ resources: [mockNodeDoc1, mockNodeDoc2, mockNodeDoc3] }) };
        }),
      };

      const result = await datasource.getNeighbors('n1', 2);

      // Two BFS edge queries (depth=2) + 1 final node query = 3 total
      expect(mockState.containerItems.query).toHaveBeenCalledTimes(3);
      expect(result.nodes.map(n => n.id).sort()).toEqual(['n1', 'n2', 'n3']);
      // Edges deduped by id: e1 fetched twice, must appear once
      expect(result.edges.map(e => e.id).sort()).toEqual(['e1', 'e2']);
    });

    it('should dedupe nodes and edges across BFS levels', async () => {
      await datasource.connect();

      // Both n1 and n2 return the same edge e1 in their 1-hop expansions.
      // depth=2 should not emit duplicate e1 nor revisit n1/n2.
      let callCount = 0;
      mockState.containerItems = {
        query: vi.fn().mockImplementation(() => {
          callCount++;
          if (callCount === 1) {
            // edges for n1
            return { fetchAll: vi.fn().mockResolvedValue({ resources: [mockEdgeDoc1] }) };
          }
          if (callCount === 2) {
            // edges for n2 -- includes e1 again (already collected)
            return { fetchAll: vi.fn().mockResolvedValue({ resources: [mockEdgeDoc1] }) };
          }
          // final node fetch
          return { fetchAll: vi.fn().mockResolvedValue({ resources: [mockNodeDoc1, mockNodeDoc2] }) };
        }),
      };

      const result = await datasource.getNeighbors('n1', 2);
      expect(result.nodes.map(n => n.id).sort()).toEqual(['n1', 'n2']);
      expect(result.edges).toHaveLength(1);
      expect(result.edges[0].id).toBe('e1');
    });

    it('should terminate BFS early when no new neighbors are found', async () => {
      await datasource.connect();

      // depth=5 but the graph is just n1 with no edges. Loop must exit
      // after the first level because frontier becomes empty.
      let callCount = 0;
      mockState.containerItems = {
        query: vi.fn().mockImplementation(() => {
          callCount++;
          if (callCount === 1) {
            return { fetchAll: vi.fn().mockResolvedValue({ resources: [] }) };
          }
          // final node fetch
          return { fetchAll: vi.fn().mockResolvedValue({ resources: [mockNodeDoc1] }) };
        }),
      };

      const result = await datasource.getNeighbors('n1', 5);

      // 1 edge query (level 0) + 1 final node query = 2 total
      expect(mockState.containerItems.query).toHaveBeenCalledTimes(2);
      expect(result.nodes.map(n => n.id)).toEqual(['n1']);
      expect(result.edges).toHaveLength(0);
    });
  });

  // --- findPath ---
  describe('findPath', () => {
    it('should find a direct path between two nodes', async () => {
      await datasource.connect();

      mockState.containerItems = {
        query: vi.fn().mockImplementation((q: unknown) => {
          const queryStr = typeof q === 'string' ? q : (q as { query: string }).query;

          if (queryStr.includes('_docType') && queryStr.includes('edge')) {
            return { fetchAll: vi.fn().mockResolvedValue({ resources: [mockEdgeDoc1] }) };
          }
          return { fetchAll: vi.fn().mockResolvedValue({ resources: [mockNodeDoc1, mockNodeDoc2] }) };
        }),
      };

      const result = await datasource.findPath('n1', 'n2');

      expect(result.nodes.length).toBeGreaterThanOrEqual(2);
      expect(result.edges.length).toBeGreaterThanOrEqual(1);
    });

    it('should return empty graph when no path exists', async () => {
      await datasource.connect();

      mockState.containerItems = {
        query: vi.fn().mockReturnValue({
          fetchAll: vi.fn().mockResolvedValue({ resources: [] }),
        }),
      };

      const result = await datasource.findPath('n1', 'n99');
      expect(result.nodes).toHaveLength(0);
      expect(result.edges).toHaveLength(0);
    });

    it('should find a multi-hop path via BFS', async () => {
      await datasource.connect();

      let bfsCallCount = 0;
      mockState.containerItems = {
        query: vi.fn().mockImplementation((q: unknown) => {
          const queryStr = typeof q === 'string' ? q : (q as { query: string }).query;

          if (queryStr.includes('_docType') && queryStr.includes('edge')) {
            bfsCallCount++;
            if (bfsCallCount === 1) {
              return { fetchAll: vi.fn().mockResolvedValue({ resources: [mockEdgeDoc1] }) };
            }
            if (bfsCallCount === 2) {
              return { fetchAll: vi.fn().mockResolvedValue({ resources: [mockEdgeDoc1, mockEdgeDoc2] }) };
            }
            return { fetchAll: vi.fn().mockResolvedValue({ resources: [] }) };
          }
          return { fetchAll: vi.fn().mockResolvedValue({ resources: [mockNodeDoc1, mockNodeDoc2, mockNodeDoc3] }) };
        }),
      };

      const result = await datasource.findPath('n1', 'n3');

      expect(result.nodes.length).toBeGreaterThanOrEqual(2);
      expect(result.edges.length).toBeGreaterThanOrEqual(1);
    });
  });

  // --- search ---
  describe('search', () => {
    it('should search nodes by text query', async () => {
      await datasource.connect();
      mockState.containerItems = createMockItems([mockNodeDoc1]);

      const result = await datasource.search('node');
      expect(result.items).toHaveLength(1);
      expect(result.items[0].id).toBe('n1');
      expect(result.total).toBe(1);
      expect(result.hasMore).toBe(false);
    });

    it('should return empty results for no matches', async () => {
      await datasource.connect();
      mockState.containerItems = createMockItems([]);

      const result = await datasource.search('nonexistent');
      expect(result.items).toHaveLength(0);
      expect(result.total).toBe(0);
    });

    it('should apply pagination', async () => {
      await datasource.connect();
      mockState.containerItems = createMockItems([mockNodeDoc1, mockNodeDoc2, mockNodeDoc3]);

      const result = await datasource.search('node', { offset: 0, limit: 2 });
      expect(result.items).toHaveLength(2);
      expect(result.total).toBe(3);
      expect(result.hasMore).toBe(true);
    });

    it('should paginate with offset', async () => {
      await datasource.connect();
      mockState.containerItems = createMockItems([mockNodeDoc1, mockNodeDoc2, mockNodeDoc3]);

      const result = await datasource.search('node', { offset: 2, limit: 2 });
      expect(result.items).toHaveLength(1);
      expect(result.total).toBe(3);
      expect(result.hasMore).toBe(false);
    });
  });

  // --- filter ---
  describe('filter', () => {
    it('should filter with no criteria', async () => {
      await datasource.connect();
      mockState.containerItems = createMockItems([mockNodeDoc1, mockNodeDoc2]);

      const result = await datasource.filter({});
      expect(result.items).toHaveLength(2);
      expect(result.total).toBe(2);
    });

    it('should filter by types', async () => {
      await datasource.connect();
      mockState.containerItems = createMockItems([mockNodeDoc1]);

      const result = await datasource.filter({ types: ['person'] });
      expect(mockState.containerItems.query).toHaveBeenCalledWith(
        expect.objectContaining({
          query: expect.stringContaining('c.type IN (@types)'),
        }),
      );
      expect(result.items).toHaveLength(1);
    });

    it('should filter by search text', async () => {
      await datasource.connect();
      mockState.containerItems = createMockItems([mockNodeDoc1]);

      const result = await datasource.filter({ search: 'node' });
      expect(mockState.containerItems.query).toHaveBeenCalledWith(
        expect.objectContaining({
          query: expect.stringContaining('CONTAINS(LOWER(c.name), LOWER(@search))'),
        }),
      );
      expect(result.items).toHaveLength(1);
    });

    it('should filter by attributes', async () => {
      await datasource.connect();
      mockState.containerItems = createMockItems([mockNodeDoc1]);

      const result = await datasource.filter({ attributes: { type: 'person' } });
      expect(mockState.containerItems.query).toHaveBeenCalledWith(
        expect.objectContaining({
          query: expect.stringContaining('c.type = @attr0'),
          parameters: expect.arrayContaining([
            expect.objectContaining({ name: '@attr0', value: 'person' }),
          ]),
        }),
      );
      expect(result.items).toHaveLength(1);
    });

    it('should combine multiple filter criteria', async () => {
      await datasource.connect();
      mockState.containerItems = createMockItems([mockNodeDoc1]);

      await datasource.filter({
        types: ['person'],
        search: 'node',
        attributes: { era: 'ancient' },
      });

      const queryCall = mockState.containerItems.query.mock.calls[0][0] as { query: string };
      expect(queryCall.query).toContain('c.type IN (@types)');
      expect(queryCall.query).toContain('CONTAINS(LOWER(c.name), LOWER(@search))');
      expect(queryCall.query).toContain('c.era = @attr0');
    });

    it('should apply pagination to filter results', async () => {
      await datasource.connect();
      mockState.containerItems = createMockItems([mockNodeDoc1, mockNodeDoc2, mockNodeDoc3]);

      const result = await datasource.filter({}, { offset: 0, limit: 1 });
      expect(result.items).toHaveLength(1);
      expect(result.total).toBe(3);
      expect(result.hasMore).toBe(true);
    });
  });

  // --- getContent ---
  describe('getContent', () => {
    it('should return content when found', async () => {
      await datasource.connect();
      mockState.containerItems = createMockItems([mockContentDoc]);

      const content = await datasource.getContent('n1');
      expect(content).toBeDefined();
      expect(content!.nodeId).toBe('n1');
      expect(content!.content).toBe('Some content about Node One.');
      expect(content!.contentType).toBe('markdown');
    });

    it('should return undefined when no content exists', async () => {
      await datasource.connect();
      mockState.containerItems = createMockItems([]);

      const content = await datasource.getContent('n99');
      expect(content).toBeUndefined();
    });

    it('should default contentType to "text" when missing', async () => {
      await datasource.connect();
      mockState.containerItems = createMockItems([{ id: 'n1', content: 'Raw text' }]);

      const content = await datasource.getContent('n1');
      expect(content).toBeDefined();
      expect(content!.contentType).toBe('text');
    });
  });

  // --- Document transformation ---
  describe('document transformation', () => {
    it('should strip Cosmos DB metadata from node documents', async () => {
      await datasource.connect();
      mockState.containerItems = createMockItems([mockNodeDoc1]);

      const node = await datasource.getNode('n1');
      expect(node).toBeDefined();
      expect(node!.attributes).not.toHaveProperty('_rid');
      expect(node!.attributes).not.toHaveProperty('_self');
      expect(node!.attributes).not.toHaveProperty('_etag');
      expect(node!.attributes).not.toHaveProperty('_attachments');
      expect(node!.attributes).not.toHaveProperty('_ts');
      expect(node!.attributes).not.toHaveProperty('_docType');
      expect(node!.attributes.name).toBe('Node One');
      expect(node!.attributes.type).toBe('person');
    });

    it('should strip Cosmos DB metadata from edge documents', async () => {
      await datasource.connect();

      let callCount = 0;
      mockState.containerItems = {
        query: vi.fn().mockImplementation(() => {
          callCount++;
          if (callCount === 1) {
            return { fetchAll: vi.fn().mockResolvedValue({ resources: [mockEdgeDoc1] }) };
          }
          return { fetchAll: vi.fn().mockResolvedValue({ resources: [mockNodeDoc1, mockNodeDoc2] }) };
        }),
      };

      const result = await datasource.getNeighbors('n1');
      const edge = result.edges[0];
      expect(edge.attributes).not.toHaveProperty('_rid');
      expect(edge.attributes).not.toHaveProperty('_self');
      expect(edge.attributes).not.toHaveProperty('_etag');
      expect(edge.attributes).not.toHaveProperty('_attachments');
      expect(edge.attributes).not.toHaveProperty('_ts');
      expect(edge.attributes).not.toHaveProperty('_docType');
      expect(edge.attributes.type).toBe('related_to');
    });
  });

  // --- Pagination helper ---
  describe('pagination', () => {
    it('should return all items when no pagination provided', async () => {
      await datasource.connect();
      mockState.containerItems = createMockItems([mockNodeDoc1, mockNodeDoc2]);

      const result = await datasource.search('node');
      expect(result.items).toHaveLength(2);
      expect(result.total).toBe(2);
      expect(result.hasMore).toBe(false);
    });

    it('should handle offset at exact end of list', async () => {
      await datasource.connect();
      mockState.containerItems = createMockItems([mockNodeDoc1, mockNodeDoc2]);

      const result = await datasource.search('node', { offset: 2, limit: 10 });
      expect(result.items).toHaveLength(0);
      expect(result.total).toBe(2);
      expect(result.hasMore).toBe(false);
    });

    it('should handle offset beyond list length', async () => {
      await datasource.connect();
      mockState.containerItems = createMockItems([mockNodeDoc1]);

      const result = await datasource.search('node', { offset: 10, limit: 5 });
      expect(result.items).toHaveLength(0);
      expect(result.total).toBe(1);
      expect(result.hasMore).toBe(false);
    });
  });
});
