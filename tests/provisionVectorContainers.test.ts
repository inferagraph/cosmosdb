import { describe, it, expect, vi, beforeEach } from 'vitest';

const { mockState, makeContainer } = vi.hoisted(() => {
  function makeContainer(name: string, opts: { exists?: boolean; vectorPath?: string } = {}) {
    return {
      _name: name,
      exists: opts.exists ?? false,
      read: vi.fn().mockImplementation(async () => {
        if (opts.vectorPath) {
          return {
            resource: {
              id: name,
              vectorEmbeddingPolicy: {
                vectorEmbeddings: [
                  { path: opts.vectorPath, dimensions: 3072, dataType: 'float32', distanceFunction: 'cosine' },
                ],
              },
              indexingPolicy: {
                vectorIndexes: [{ path: opts.vectorPath, type: 'quantizedFlat' }],
              },
            },
          };
        }
        return {
          resource: { id: name, vectorEmbeddingPolicy: undefined, indexingPolicy: { indexingMode: 'consistent' } },
        };
      }),
      replace: vi.fn().mockImplementation(async (def: unknown) => ({ resource: def })),
    };
  }

  const mockState: {
    units: ReturnType<typeof makeContainer>;
    inferredEdges: ReturnType<typeof makeContainer>;
    createCalls: unknown[];
  } = {
    units: makeContainer('units'),
    inferredEdges: makeContainer('inferred_edges'),
    createCalls: [],
  };
  return { mockState, makeContainer };
});

vi.mock('@azure/cosmos', () => {
  const databaseObj = {
    container: vi.fn((name: string) => {
      // Any non-units container name routes to inferredEdges so tests can
      // assert against custom container names without rewiring the mock.
      if (name === mockState.units._name) return mockState.units;
      return mockState.inferredEdges;
    }),
    containers: {
      createIfNotExists: vi.fn().mockImplementation(async (def: unknown) => {
        mockState.createCalls.push(def);
        const name = (def as { id?: string }).id;
        // After creation, refresh the mock so subsequent reads see the new policy
        if (name === 'inferred_edges') {
          mockState.inferredEdges = makeContainer('inferred_edges', {
            exists: true,
            vectorPath: '/embedding',
          });
        }
        return { resource: def };
      }),
    },
  };
  const databaseResponse = { database: databaseObj };
  return {
    CosmosClient: vi.fn().mockImplementation(() => ({
      databases: {
        createIfNotExists: vi.fn().mockResolvedValue(databaseResponse),
      },
      database: vi.fn().mockReturnValue(databaseObj),
    })),
    VectorEmbeddingDataType: { Float16: 'float16', Float32: 'float32', UInt8: 'uint8', Int8: 'int8' },
    VectorEmbeddingDistanceFunction: { Euclidean: 'euclidean', Cosine: 'cosine', DotProduct: 'dotproduct' },
    VectorIndexType: { Flat: 'flat', DiskANN: 'diskANN', QuantizedFlat: 'quantizedFlat' },
  };
});

import { provisionVectorContainers } from '../src/provisionVectorContainers.js';

describe('provisionVectorContainers', () => {
  beforeEach(() => {
    mockState.units = makeContainer('units');
    mockState.inferredEdges = makeContainer('inferred_edges');
    mockState.createCalls.length = 0;
  });

  it('no-ops on the units container when it already has the vector policy on /embedding', async () => {
    mockState.units = makeContainer('units', { exists: true, vectorPath: '/embedding' });
    // inferred_edges already provisioned too
    mockState.inferredEdges = makeContainer('inferred_edges', { exists: true, vectorPath: '/embedding' });
    await provisionVectorContainers({
      endpoint: 'x',
      key: 'y',
      database: 'db',
      unitsContainer: 'units',
    });
    expect(mockState.units.replace).not.toHaveBeenCalled();
  });

  it('alters the units container to add vector policy when missing', async () => {
    // units has no vector policy
    mockState.units = makeContainer('units', { exists: true });
    await provisionVectorContainers({
      endpoint: 'x',
      key: 'y',
      database: 'db',
      unitsContainer: 'units',
    });
    expect(mockState.units.replace).toHaveBeenCalledTimes(1);
    const call = mockState.units.replace.mock.calls[0][0] as {
      vectorEmbeddingPolicy: { vectorEmbeddings: { path: string; dimensions: number; dataType: string; distanceFunction: string }[] };
      indexingPolicy: { vectorIndexes: { path: string; type: string }[] };
    };
    expect(call.vectorEmbeddingPolicy.vectorEmbeddings[0].path).toBe('/embedding');
    expect(call.vectorEmbeddingPolicy.vectorEmbeddings[0].dimensions).toBe(3072);
    expect(call.vectorEmbeddingPolicy.vectorEmbeddings[0].distanceFunction).toBe('cosine');
    expect(call.indexingPolicy.vectorIndexes[0].type).toBe('quantizedFlat');
  });

  it('creates inferred_edges with the vector policy when missing', async () => {
    mockState.units = makeContainer('units', { exists: true, vectorPath: '/embedding' });
    // inferred_edges starts missing
    mockState.inferredEdges = makeContainer('inferred_edges', { exists: false });
    await provisionVectorContainers({
      endpoint: 'x',
      key: 'y',
      database: 'db',
      unitsContainer: 'units',
    });
    expect(mockState.createCalls).toHaveLength(1);
    const def = mockState.createCalls[0] as {
      id: string;
      partitionKey: { paths: string[] };
      vectorEmbeddingPolicy: { vectorEmbeddings: { path: string; dimensions: number; distanceFunction: string }[] };
      indexingPolicy: { vectorIndexes: { path: string; type: string }[] };
    };
    expect(def.id).toBe('inferred_edges');
    expect(def.partitionKey.paths).toEqual(['/sourceId']);
    expect(def.vectorEmbeddingPolicy.vectorEmbeddings[0].path).toBe('/embedding');
    expect(def.vectorEmbeddingPolicy.vectorEmbeddings[0].dimensions).toBe(3072);
    expect(def.indexingPolicy.vectorIndexes[0].type).toBe('quantizedFlat');
  });

  it('honors custom embeddingDimensions, vectorIndexType, and distanceFunction', async () => {
    mockState.units = makeContainer('units', { exists: true });
    mockState.inferredEdges = makeContainer('inferred_edges', { exists: false });
    await provisionVectorContainers({
      endpoint: 'x',
      key: 'y',
      database: 'db',
      unitsContainer: 'units',
      embeddingDimensions: 1536,
      vectorIndexType: 'diskANN',
      distanceFunction: 'dotproduct',
    });
    const unitsCall = mockState.units.replace.mock.calls[0][0] as {
      vectorEmbeddingPolicy: { vectorEmbeddings: { dimensions: number; distanceFunction: string }[] };
      indexingPolicy: { vectorIndexes: { type: string }[] };
    };
    expect(unitsCall.vectorEmbeddingPolicy.vectorEmbeddings[0].dimensions).toBe(1536);
    expect(unitsCall.vectorEmbeddingPolicy.vectorEmbeddings[0].distanceFunction).toBe('dotproduct');
    expect(unitsCall.indexingPolicy.vectorIndexes[0].type).toBe('diskANN');

    const edgesDef = mockState.createCalls[0] as {
      vectorEmbeddingPolicy: { vectorEmbeddings: { dimensions: number; distanceFunction: string }[] };
      indexingPolicy: { vectorIndexes: { type: string }[] };
    };
    expect(edgesDef.vectorEmbeddingPolicy.vectorEmbeddings[0].dimensions).toBe(1536);
    expect(edgesDef.indexingPolicy.vectorIndexes[0].type).toBe('diskANN');
  });

  it('honors a custom inferredEdgesContainer name', async () => {
    mockState.units = makeContainer('units', { exists: true, vectorPath: '/embedding' });
    mockState.inferredEdges = makeContainer('custom_edges', { exists: false });
    await provisionVectorContainers({
      endpoint: 'x',
      key: 'y',
      database: 'db',
      unitsContainer: 'units',
      inferredEdgesContainer: 'custom_edges',
    });
    // The custom name reaches the create call
    expect(mockState.createCalls).toHaveLength(1);
    expect((mockState.createCalls[0] as { id: string }).id).toBe('custom_edges');
  });

  // ---- Fix 5: alter-rejection handling ----
  describe('alter-rejection (Fix 5)', () => {
    it('throws a clear, actionable message when replace() is rejected with the known Cosmos alter error', async () => {
      mockState.units = makeContainer('units', { exists: true });
      const cosmosErr = Object.assign(
        new Error("Operation 'replace' on resource 'colls' is not allowed"),
        { code: 400, substatus: 1001 },
      );
      mockState.units.replace.mockRejectedValueOnce(cosmosErr);
      await expect(
        provisionVectorContainers({
          endpoint: 'x',
          key: 'y',
          database: 'db',
          unitsContainer: 'units',
        }),
      ).rejects.toThrow(
        /Container 'units' exists but cannot be altered to add the vector policy\. Drop the container manually \(DATA LOSS\) and re-run provisionVectorContainers, or recreate it with the desired policy from the start\./,
      );
    });

    it('propagates unknown errors raw (does not swallow them)', async () => {
      mockState.units = makeContainer('units', { exists: true });
      const otherErr = Object.assign(new Error('Unexpected boom'), { code: 500 });
      mockState.units.replace.mockRejectedValueOnce(otherErr);
      await expect(
        provisionVectorContainers({
          endpoint: 'x',
          key: 'y',
          database: 'db',
          unitsContainer: 'units',
        }),
      ).rejects.toThrow('Unexpected boom');
    });
  });

  // ---- Fix 6: dataType option ----
  describe('dataType option (Fix 6)', () => {
    it('defaults dataType to Float32 in the units vectorEmbeddingPolicy', async () => {
      mockState.units = makeContainer('units', { exists: true });
      await provisionVectorContainers({
        endpoint: 'x',
        key: 'y',
        database: 'db',
        unitsContainer: 'units',
      });
      const call = mockState.units.replace.mock.calls[0][0] as {
        vectorEmbeddingPolicy: { vectorEmbeddings: { dataType: string }[] };
      };
      expect(call.vectorEmbeddingPolicy.vectorEmbeddings[0].dataType).toBe('float32');
    });

    it('honors dataType: Float16 in the vectorEmbeddingPolicy on both containers', async () => {
      mockState.units = makeContainer('units', { exists: true });
      mockState.inferredEdges = makeContainer('inferred_edges', { exists: false });
      await provisionVectorContainers({
        endpoint: 'x',
        key: 'y',
        database: 'db',
        unitsContainer: 'units',
        dataType: 'Float16',
      });
      const unitsCall = mockState.units.replace.mock.calls[0][0] as {
        vectorEmbeddingPolicy: { vectorEmbeddings: { dataType: string }[] };
      };
      expect(unitsCall.vectorEmbeddingPolicy.vectorEmbeddings[0].dataType).toBe('float16');

      const edgesDef = mockState.createCalls[0] as {
        vectorEmbeddingPolicy: { vectorEmbeddings: { dataType: string }[] };
      };
      expect(edgesDef.vectorEmbeddingPolicy.vectorEmbeddings[0].dataType).toBe('float16');
    });

    it('honors dataType: Int8', async () => {
      mockState.units = makeContainer('units', { exists: true });
      mockState.inferredEdges = makeContainer('inferred_edges', { exists: false });
      await provisionVectorContainers({
        endpoint: 'x',
        key: 'y',
        database: 'db',
        unitsContainer: 'units',
        dataType: 'Int8',
      });
      const unitsCall = mockState.units.replace.mock.calls[0][0] as {
        vectorEmbeddingPolicy: { vectorEmbeddings: { dataType: string }[] };
      };
      expect(unitsCall.vectorEmbeddingPolicy.vectorEmbeddings[0].dataType).toBe('int8');
    });
  });
});
