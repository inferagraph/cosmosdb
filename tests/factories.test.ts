import { describe, it, expect, vi, beforeEach } from 'vitest';

// Mock the Cosmos SDK so factories don't actually open network connections.
// We track CosmosClient construction calls to verify factories build the
// SDK internally rather than asking callers for a pre-built client.
const { mockState } = vi.hoisted(() => {
  const mockState = {
    constructorCalls: [] as Array<{ endpoint: string; key: string }>,
  };
  return { mockState };
});

vi.mock('@azure/cosmos', () => {
  const containerObj = {
    items: {
      query: vi.fn().mockReturnValue({
        fetchAll: vi.fn().mockResolvedValue({ resources: [] }),
      }),
      upsert: vi.fn().mockResolvedValue({}),
    },
    item: vi.fn().mockReturnValue({
      read: vi.fn().mockResolvedValue({ resource: undefined }),
      delete: vi.fn().mockResolvedValue({}),
    }),
  };
  const databaseObj = {
    container: vi.fn().mockReturnValue(containerObj),
    containers: {
      createIfNotExists: vi.fn().mockResolvedValue({ resource: { id: 'c' } }),
    },
  };
  return {
    CosmosClient: vi.fn().mockImplementation((cfg: { endpoint: string; key: string }) => {
      mockState.constructorCalls.push({ endpoint: cfg.endpoint, key: cfg.key });
      return {
        database: vi.fn().mockReturnValue(databaseObj),
        databases: {
          createIfNotExists: vi.fn().mockResolvedValue({ database: databaseObj }),
        },
      };
    }),
    Container: vi.fn(),
    Database: vi.fn(),
  };
});

import { CosmosDataSource, cosmosDataSource } from '../src/CosmosDataSource.js';
import {
  CosmosVectorEmbeddingStore,
  cosmosVectorEmbeddingStore,
} from '../src/CosmosVectorEmbeddingStore.js';
import {
  CosmosInferredEdgeStore,
  cosmosInferredEdgeStore,
} from '../src/CosmosInferredEdgeStore.js';
import {
  CosmosConversationStore,
  cosmosConversationStore,
} from '../src/CosmosConversationStore.js';
import {
  CosmosCacheProvider,
  cosmosCacheProvider,
} from '../src/CosmosCacheProvider.js';

describe('factory functions', () => {
  beforeEach(() => {
    mockState.constructorCalls.length = 0;
  });

  it('cosmosDataSource returns an instance of CosmosDataSource', () => {
    const ds = cosmosDataSource({
      endpoint: 'https://x.documents.azure.com:443/',
      key: 'k',
      database: 'db',
      container: 'units',
    });
    expect(ds).toBeInstanceOf(CosmosDataSource);
    expect(ds.constructor.name).toBe('CosmosDataSource');
  });

  it('cosmosVectorEmbeddingStore returns an instance of CosmosVectorEmbeddingStore', () => {
    const store = cosmosVectorEmbeddingStore({
      endpoint: 'https://x.documents.azure.com:443/',
      key: 'k',
      database: 'db',
      container: 'units',
    });
    expect(store).toBeInstanceOf(CosmosVectorEmbeddingStore);
    expect(store.constructor.name).toBe('CosmosVectorEmbeddingStore');
  });

  it('cosmosInferredEdgeStore returns an instance of CosmosInferredEdgeStore', () => {
    const store = cosmosInferredEdgeStore({
      endpoint: 'https://x.documents.azure.com:443/',
      key: 'k',
      database: 'db',
    });
    expect(store).toBeInstanceOf(CosmosInferredEdgeStore);
  });

  it('cosmosConversationStore returns an instance of CosmosConversationStore', () => {
    const store = cosmosConversationStore({
      endpoint: 'https://x.documents.azure.com:443/',
      key: 'k',
      database: 'db',
    });
    expect(store).toBeInstanceOf(CosmosConversationStore);
  });

  it('cosmosCacheProvider returns an instance of CosmosCacheProvider', () => {
    const provider = cosmosCacheProvider({
      endpoint: 'https://x.documents.azure.com:443/',
      key: 'k',
      database: 'db',
    });
    expect(provider).toBeInstanceOf(CosmosCacheProvider);
  });

  it('cosmosDataSource constructs CosmosClient internally on connect (host never imports @azure/cosmos)', async () => {
    const ds = cosmosDataSource({
      endpoint: 'https://internal.documents.azure.com:443/',
      key: 'secret-key',
      database: 'db',
      container: 'units',
    });
    await ds.connect();
    expect(mockState.constructorCalls.length).toBeGreaterThanOrEqual(1);
    const last = mockState.constructorCalls.at(-1)!;
    expect(last.endpoint).toBe('https://internal.documents.azure.com:443/');
    expect(last.key).toBe('secret-key');
  });

  it('cosmosVectorEmbeddingStore constructs CosmosClient internally with endpoint+key', () => {
    cosmosVectorEmbeddingStore({
      endpoint: 'https://factory.documents.azure.com:443/',
      key: 'fk',
      database: 'db',
      container: 'units',
    });
    expect(mockState.constructorCalls.length).toBeGreaterThanOrEqual(1);
    const last = mockState.constructorCalls.at(-1)!;
    expect(last.endpoint).toBe('https://factory.documents.azure.com:443/');
    expect(last.key).toBe('fk');
  });

  it('class constructors still accept a pre-built CosmosClient (escape hatch)', async () => {
    const { CosmosClient } = await import('@azure/cosmos');
    const client = new CosmosClient({ endpoint: 'x', key: 'y' });
    const store = new CosmosVectorEmbeddingStore({
      client,
      database: 'db',
      container: 'units',
    });
    expect(store).toBeInstanceOf(CosmosVectorEmbeddingStore);
    const edges = new CosmosInferredEdgeStore({
      client,
      database: 'db',
    });
    expect(edges).toBeInstanceOf(CosmosInferredEdgeStore);
  });
});
