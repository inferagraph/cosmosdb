import { describe, it, expect, vi, beforeEach } from 'vitest';

const { mockState } = vi.hoisted(() => {
  const mockState = {
    queryCalls: [] as { spec: unknown }[],
    queryResources: [] as unknown[][],
  };
  return { mockState };
});

vi.mock('@azure/cosmos', () => {
  const containerObj = {
    items: {
      query: vi.fn().mockImplementation((spec: unknown) => {
        mockState.queryCalls.push({ spec });
        const idx = mockState.queryCalls.length - 1;
        const resources = mockState.queryResources[idx] ?? [];
        return { fetchAll: vi.fn().mockResolvedValue({ resources }) };
      }),
    },
  };
  const databaseObj = {
    container: vi.fn().mockReturnValue(containerObj),
  };
  return {
    CosmosClient: vi.fn().mockImplementation(() => ({
      database: vi.fn().mockReturnValue(databaseObj),
    })),
    Container: vi.fn(),
    Database: vi.fn(),
  };
});

import { CosmosDbDatasource } from '../src/CosmosDbDatasource.js';

describe('CosmosDbDatasource.searchVector', () => {
  let datasource: CosmosDbDatasource;

  beforeEach(() => {
    mockState.queryCalls.length = 0;
    mockState.queryResources.length = 0;
    datasource = new CosmosDbDatasource({
      endpoint: 'x',
      key: 'y',
      database: 'db',
      container: 'units',
    });
  });

  it('throws when not connected', async () => {
    await expect(datasource.searchVector([0.1, 0.2], { top: 5 })).rejects.toThrow(
      'CosmosDbDatasource is not connected.',
    );
  });

  it('uses the same SQL shape as VectorEmbeddingStore.searchVector', async () => {
    await datasource.connect();
    mockState.queryResources[0] = [];
    await datasource.searchVector([0.1, 0.2, 0.3], { top: 4 });
    const call = mockState.queryCalls[0].spec as {
      query: string;
      parameters: { name: string; value: unknown }[];
    };
    expect(call.query).toMatch(/SELECT\s+TOP\s+@k/i);
    expect(call.query).toMatch(/VectorDistance\s*\(\s*c\.embedding\s*,\s*@q\s*\)/i);
    expect(call.query).toMatch(/ORDER\s+BY\s+VectorDistance\s*\(\s*c\.embedding\s*,\s*@q\s*\)/i);
    expect(call.parameters).toEqual(
      expect.arrayContaining([
        { name: '@k', value: 4 },
        { name: '@q', value: [0.1, 0.2, 0.3] },
      ]),
    );
  });

  it('returns hits sorted descending by score', async () => {
    await datasource.connect();
    mockState.queryResources[0] = [
      { id: 'a', score: 0.2 },
      { id: 'b', score: 0.8 },
      { id: 'c', score: 0.5 },
    ];
    const hits = await datasource.searchVector([0.1, 0.2], { top: 3 });
    expect(hits.map(h => h.nodeId)).toEqual(['b', 'c', 'a']);
  });
});
