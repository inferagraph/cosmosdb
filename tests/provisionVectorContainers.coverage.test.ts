import { describe, it, expect, vi, beforeEach } from 'vitest';

// ---------------------------------------------------------------------------
// Coverage gaps in provisionVectorContainers:
//   - The catch path when reading the inferred_edges container fails (the
//     "missing container" surface — read() rejects, edgesResource stays
//     undefined, and the create-if-not-exists path runs).
//   - vectorIndexType: 'flat' branch in toVectorIndexType.
//   - distanceFunction: 'euclidean' branch in toDistanceFunction.
//   - isAlterRejection matching the alternate error phrases ("cannot be
//     modified", "cannot be altered", body.message variant).
// ---------------------------------------------------------------------------
const { mockState, makeContainer } = vi.hoisted(() => {
  function makeContainer(
    name: string,
    opts: {
      exists?: boolean;
      vectorPath?: string;
      readThrows?: boolean;
      replaceError?: { code: number | string; message?: string; body?: unknown };
    } = {},
  ) {
    return {
      _name: name,
      exists: opts.exists ?? false,
      read: vi.fn().mockImplementation(async () => {
        if (opts.readThrows) {
          throw new Error('container does not exist');
        }
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
          resource: {
            id: name,
            vectorEmbeddingPolicy: undefined,
            indexingPolicy: { indexingMode: 'consistent' },
          },
        };
      }),
      replace: vi.fn().mockImplementation(async (def: unknown) => {
        if (opts.replaceError) {
          const err = new Error(opts.replaceError.message ?? 'forced') as Error & {
            code: number | string;
            body?: unknown;
          };
          err.code = opts.replaceError.code;
          if (opts.replaceError.body) err.body = opts.replaceError.body;
          throw err;
        }
        return { resource: def };
      }),
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

describe('provisionVectorContainers — coverage gaps', () => {
  beforeEach(() => {
    mockState.units = makeContainer('units');
    mockState.inferredEdges = makeContainer('inferred_edges');
    mockState.createCalls.length = 0;
  });

  it('treats a read failure on inferred_edges as "missing" and creates it', async () => {
    // Pins the catch path: when the inferred_edges container does not yet
    // exist, the SDK throws on read; the function must swallow that and
    // proceed to createIfNotExists with the desired policy.
    mockState.units = makeContainer('units', { exists: true, vectorPath: '/embedding' });
    mockState.inferredEdges = makeContainer('inferred_edges', { readThrows: true });

    await provisionVectorContainers({
      endpoint: 'x',
      key: 'y',
      database: 'db',
      unitsContainer: 'units',
    });

    expect(mockState.createCalls).toHaveLength(1);
    expect((mockState.createCalls[0] as { id: string }).id).toBe('inferred_edges');
  });

  it('honors vectorIndexType: "flat"', async () => {
    mockState.units = makeContainer('units', { exists: true });
    mockState.inferredEdges = makeContainer('inferred_edges', { exists: false });

    await provisionVectorContainers({
      endpoint: 'x',
      key: 'y',
      database: 'db',
      unitsContainer: 'units',
      vectorIndexType: 'flat',
    });

    const unitsCall = mockState.units.replace.mock.calls[0][0] as {
      indexingPolicy: { vectorIndexes: { type: string }[] };
    };
    expect(unitsCall.indexingPolicy.vectorIndexes[0].type).toBe('flat');
  });

  it('honors distanceFunction: "euclidean"', async () => {
    mockState.units = makeContainer('units', { exists: true });
    mockState.inferredEdges = makeContainer('inferred_edges', { exists: false });

    await provisionVectorContainers({
      endpoint: 'x',
      key: 'y',
      database: 'db',
      unitsContainer: 'units',
      distanceFunction: 'euclidean',
    });

    const unitsCall = mockState.units.replace.mock.calls[0][0] as {
      vectorEmbeddingPolicy: { vectorEmbeddings: { distanceFunction: string }[] };
    };
    expect(unitsCall.vectorEmbeddingPolicy.vectorEmbeddings[0].distanceFunction).toBe('euclidean');
  });

  describe('isAlterRejection — alternate phrasings', () => {
    it('treats a 412-coded "cannot be modified" replace failure as the alter-rejection surface', async () => {
      mockState.units = makeContainer('units', {
        exists: true,
        replaceError: { code: 412, message: 'Container cannot be modified after creation' },
      });
      await expect(
        provisionVectorContainers({
          endpoint: 'x',
          key: 'y',
          database: 'db',
          unitsContainer: 'units',
        }),
      ).rejects.toThrow(
        /Container 'units' exists but cannot be altered to add the vector policy/,
      );
    });

    it('treats a "cannot be altered" replace failure (string-coded 400) as the alter-rejection surface', async () => {
      mockState.units = makeContainer('units', {
        exists: true,
        replaceError: { code: '400', message: 'Vector policy cannot be altered in place' },
      });
      await expect(
        provisionVectorContainers({
          endpoint: 'x',
          key: 'y',
          database: 'db',
          unitsContainer: 'units',
        }),
      ).rejects.toThrow(
        /Container 'units' exists but cannot be altered to add the vector policy/,
      );
    });

    it('reads the alter-rejection phrase out of err.body.message when err.message is empty', async () => {
      // Some SDK paths surface the rich message on `body.message` instead of
      // the top-level `message`. The helper must concatenate both before
      // matching.
      mockState.units = makeContainer('units', {
        exists: true,
        replaceError: {
          code: 400,
          message: '',
          body: { message: "Operation 'replace' on resource 'colls' is not allowed" },
        },
      });
      await expect(
        provisionVectorContainers({
          endpoint: 'x',
          key: 'y',
          database: 'db',
          unitsContainer: 'units',
        }),
      ).rejects.toThrow(
        /Container 'units' exists but cannot be altered to add the vector policy/,
      );
    });
  });
});
