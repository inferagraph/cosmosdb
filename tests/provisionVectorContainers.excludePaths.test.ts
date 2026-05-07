import { describe, it, expect, vi, beforeEach } from 'vitest';

// ---------------------------------------------------------------------------
// Bug fix (0.3.3):
// Cosmos requires every path listed in `indexingPolicy.vectorIndexes` to be
// ALSO listed in `indexingPolicy.excludedPaths` (as `/<path>/*`). When the
// exclusion is missing, Cosmos rejects the policy with the misleading error:
//   "The Vector Indexing Policy's Index Type::quantizedFlat has been provided
//    but the capability has not been enabled on your account."
// even when vector search IS enabled. provisionVectorContainers must add the
// matching excludedPaths entry on both the alter-in-place path
// (mergeVectorPolicy) and the create path (buildEdgesDefinition), preserving
// any existing excludedPaths and avoiding duplicates.
// ---------------------------------------------------------------------------

const { mockState, makeContainer } = vi.hoisted(() => {
  function makeContainer(
    name: string,
    opts: {
      exists?: boolean;
      vectorPath?: string;
      existingExcludedPaths?: { path: string }[];
    } = {},
  ) {
    return {
      _name: name,
      exists: opts.exists ?? false,
      read: vi.fn().mockImplementation(async () => {
        const indexingPolicy: Record<string, unknown> = {
          indexingMode: 'consistent',
        };
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
}

describe('provisionVectorContainers — excludedPaths for vector path (0.3.3)', () => {
  beforeEach(() => {
    mockState.units = makeContainer('units');
    mockState.inferredEdges = makeContainer('inferred_edges');
    mockState.createCalls.length = 0;
  });

  it('mergeVectorPolicy adds excludedPaths entry for /<embeddingPath>/* on the units container', async () => {
    mockState.units = makeContainer('units', { exists: true });
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
    expect(call.indexingPolicy.excludedPaths).toEqual(
      expect.arrayContaining([{ path: '/embedding/*' }]),
    );
  });

  it('buildEdgesDefinition includes excludedPaths for /<embeddingPath>/* on the inferred_edges container', async () => {
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
    expect(def.indexingPolicy.excludedPaths).toEqual(
      expect.arrayContaining([{ path: '/embedding/*' }]),
    );
  });

  it('idempotency — when units already has the wildcard exclusion, the resulting definition contains it exactly once', async () => {
    // Vector policy is missing (so replace runs), but excludedPaths already
    // carries the wildcard. The fix must not duplicate it.
    mockState.units = makeContainer('units', {
      exists: true,
      existingExcludedPaths: [{ path: '/embedding/*' }],
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
    const matches = (call.indexingPolicy.excludedPaths ?? []).filter(
      (p) => p.path === '/embedding/*',
    );
    expect(matches).toHaveLength(1);
  });

  it('idempotency — when units already has the full vector policy on /embedding, replace() is not called at all', async () => {
    mockState.units = makeContainer('units', { exists: true, vectorPath: '/embedding' });
    mockState.inferredEdges = makeContainer('inferred_edges', { exists: true, vectorPath: '/embedding' });

    await provisionVectorContainers({
      endpoint: 'x',
      key: 'y',
      database: 'db',
      unitsContainer: 'units',
    });

    expect(mockState.units.replace).not.toHaveBeenCalled();
  });

  it('preserves existing non-vector excludedPaths entries when adding the vector exclusion', async () => {
    mockState.units = makeContainer('units', {
      exists: true,
      existingExcludedPaths: [{ path: '/_etag/?' }, { path: '/secrets/*' }],
    });
    mockState.inferredEdges = makeContainer('inferred_edges', { exists: false });

    await provisionVectorContainers({
      endpoint: 'x',
      key: 'y',
      database: 'db',
      unitsContainer: 'units',
    });

    const call = mockState.units.replace.mock.calls[0][0] as {
      indexingPolicy: IndexingPolicyShape;
    };
    expect(call.indexingPolicy.excludedPaths).toEqual(
      expect.arrayContaining([
        { path: '/_etag/?' },
        { path: '/secrets/*' },
        { path: '/embedding/*' },
      ]),
    );
  });

  it('custom embeddingPath threads through to excludedPaths on both containers', async () => {
    mockState.units = makeContainer('units', { exists: true });
    mockState.inferredEdges = makeContainer('inferred_edges', { exists: false });

    await provisionVectorContainers({
      endpoint: 'x',
      key: 'y',
      database: 'db',
      unitsContainer: 'units',
      embeddingPath: '/myCustomVector',
    });

    const unitsCall = mockState.units.replace.mock.calls[0][0] as {
      indexingPolicy: IndexingPolicyShape;
    };
    expect(unitsCall.indexingPolicy.excludedPaths).toEqual(
      expect.arrayContaining([{ path: '/myCustomVector/*' }]),
    );

    const edgesDef = mockState.createCalls[0] as { indexingPolicy: IndexingPolicyShape };
    expect(edgesDef.indexingPolicy.excludedPaths).toEqual(
      expect.arrayContaining([{ path: '/myCustomVector/*' }]),
    );
  });
});
