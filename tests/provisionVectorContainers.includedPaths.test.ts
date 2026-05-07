import { describe, it, expect, vi, beforeEach } from 'vitest';

// ---------------------------------------------------------------------------
// Bug fix (0.3.4):
// Cosmos requires the indexing policy to declare what to do with paths that
// are NOT explicitly excluded — the "special mandatory indexing path '/'"
// rule. When `includedPaths` is omitted, Cosmos rejects container creation
// with:
//   "The special mandatory indexing path '/' is not provided in any of the
//    path type sets. Please provide this path in one of the sets."
// provisionVectorContainers must default `includedPaths` to the catch-all
// `[{ path: '/*' }]` on the create path (buildEdgesDefinition) and on the
// alter-in-place path (mergeVectorPolicy) when the source container lacks
// it; existing includedPaths must be preserved untouched.
// ---------------------------------------------------------------------------

const { mockState, makeContainer } = vi.hoisted(() => {
  function makeContainer(
    name: string,
    opts: {
      exists?: boolean;
      vectorPath?: string;
      existingExcludedPaths?: { path: string }[];
      existingIncludedPaths?: { path: string }[];
      omitIncludedPaths?: boolean;
    } = {},
  ) {
    return {
      _name: name,
      exists: opts.exists ?? false,
      read: vi.fn().mockImplementation(async () => {
        const indexingPolicy: Record<string, unknown> = {
          indexingMode: 'consistent',
          automatic: true,
        };
        if (!opts.omitIncludedPaths) {
          indexingPolicy.includedPaths =
            opts.existingIncludedPaths ?? [{ path: '/*' }];
        }
        if (opts.vectorPath) {
          indexingPolicy.vectorIndexes = [
            { path: opts.vectorPath, type: 'quantizedFlat' },
          ];
        }
        if (opts.existingExcludedPaths) {
          indexingPolicy.excludedPaths = opts.existingExcludedPaths;
        }
        const resource: Record<string, unknown> = {
          id: name,
          indexingPolicy,
        };
        if (opts.vectorPath) {
          resource.vectorEmbeddingPolicy = {
            vectorEmbeddings: [
              {
                path: opts.vectorPath,
                dimensions: 3072,
                dataType: 'float32',
                distanceFunction: 'cosine',
              },
            ],
          };
        } else {
          resource.vectorEmbeddingPolicy = undefined;
        }
        return { resource };
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
      if (name === mockState.units._name) return mockState.units;
      return mockState.inferredEdges;
    }),
    containers: {
      createIfNotExists: vi.fn().mockImplementation(async (def: unknown) => {
        mockState.createCalls.push(def);
        return { resource: def };
      }),
    },
  };
  return {
    CosmosClient: vi.fn().mockImplementation(() => ({
      database: vi.fn().mockReturnValue(databaseObj),
    })),
    VectorEmbeddingDataType: { Float16: 'float16', Float32: 'float32', UInt8: 'uint8', Int8: 'int8' },
    VectorEmbeddingDistanceFunction: { Euclidean: 'euclidean', Cosine: 'cosine', DotProduct: 'dotproduct' },
    VectorIndexType: { Flat: 'flat', DiskANN: 'diskANN', QuantizedFlat: 'quantizedFlat' },
  };
});

import { provisionVectorContainers } from '../src/provisionVectorContainers.js';

interface IndexingPolicyShape {
  vectorIndexes?: { path: string; type: string }[];
  excludedPaths?: { path: string }[];
  includedPaths?: { path: string }[];
}

describe('provisionVectorContainers — includedPaths catch-all (0.3.4)', () => {
  beforeEach(() => {
    mockState.units = makeContainer('units');
    mockState.inferredEdges = makeContainer('inferred_edges');
    mockState.createCalls.length = 0;
  });

  it('buildEdgesDefinition includes the catch-all includedPaths [{ path: "/*" }]', async () => {
    mockState.units = makeContainer('units', { exists: true, vectorPath: '/embedding' });
    mockState.inferredEdges = makeContainer('inferred_edges', { exists: false });

    await provisionVectorContainers({
      endpoint: 'x',
      key: 'y',
      database: 'db',
      unitsContainer: 'units',
    });

    expect(mockState.createCalls).toHaveLength(1);
    const def = mockState.createCalls[0] as { indexingPolicy: IndexingPolicyShape };
    expect(def.indexingPolicy.includedPaths).toEqual([{ path: '/*' }]);
  });

  it('mergeVectorPolicy preserves existing includedPaths from the units container', async () => {
    mockState.units = makeContainer('units', {
      exists: true,
      existingIncludedPaths: [{ path: '/myPath/*' }],
    });
    mockState.inferredEdges = makeContainer('inferred_edges', { exists: false });

    await provisionVectorContainers({
      endpoint: 'x',
      key: 'y',
      database: 'db',
      unitsContainer: 'units',
    });

    expect(mockState.units.replace).toHaveBeenCalledTimes(1);
    const call = mockState.units.replace.mock.calls[0][0] as {
      indexingPolicy: IndexingPolicyShape;
    };
    expect(call.indexingPolicy.includedPaths).toEqual([{ path: '/myPath/*' }]);
  });

  it('mergeVectorPolicy defaults includedPaths to /* when units container has none', async () => {
    mockState.units = makeContainer('units', {
      exists: true,
      omitIncludedPaths: true,
    });
    mockState.inferredEdges = makeContainer('inferred_edges', { exists: false });

    await provisionVectorContainers({
      endpoint: 'x',
      key: 'y',
      database: 'db',
      unitsContainer: 'units',
    });

    expect(mockState.units.replace).toHaveBeenCalledTimes(1);
    const call = mockState.units.replace.mock.calls[0][0] as {
      indexingPolicy: IndexingPolicyShape;
    };
    expect(call.indexingPolicy.includedPaths).toEqual([{ path: '/*' }]);
  });

  it('regression: excludedPaths still works alongside includedPaths in the new edges definition', async () => {
    mockState.units = makeContainer('units', { exists: true, vectorPath: '/embedding' });
    mockState.inferredEdges = makeContainer('inferred_edges', { exists: false });

    await provisionVectorContainers({
      endpoint: 'x',
      key: 'y',
      database: 'db',
      unitsContainer: 'units',
    });

    expect(mockState.createCalls).toHaveLength(1);
    const def = mockState.createCalls[0] as { indexingPolicy: IndexingPolicyShape };
    expect(def.indexingPolicy.includedPaths).toEqual([{ path: '/*' }]);
    expect(def.indexingPolicy.excludedPaths).toEqual(
      expect.arrayContaining([{ path: '/embedding/*' }]),
    );
  });
});
