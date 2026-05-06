import { describe, it, expect, vi, beforeEach } from 'vitest';

const { mockState, makeContainer } = vi.hoisted(() => {
  function makeContainer(name: string) {
    const docs = new Map<string, Record<string, unknown>>();
    const upsertCalls: Record<string, unknown>[] = [];
    const deletedIds: string[] = [];
    const queryCalls: { spec: unknown }[] = [];
    let queryResources: unknown[] = [];
    const container = {
      _name: name,
      docs,
      upsertCalls,
      deletedIds,
      queryCalls,
      get queryResources() { return queryResources; },
      set queryResources(v: unknown[]) { queryResources = v; },
      items: {
        upsert: vi.fn().mockImplementation(async (item: Record<string, unknown>) => {
          upsertCalls.push({ ...item });
          docs.set(String(item.id), { ...item });
          return { resource: item };
        }),
        query: vi.fn().mockImplementation((spec: unknown) => {
          queryCalls.push({ spec });
          return { fetchAll: vi.fn().mockResolvedValue({ resources: queryResources }) };
        }),
      },
      item: vi.fn().mockImplementation((id: string) => ({
        read: vi.fn().mockImplementation(async () => {
          const doc = docs.get(id);
          if (!doc) {
            const err = new Error('NotFound') as Error & { code: number };
            err.code = 404;
            throw err;
          }
          return { resource: doc };
        }),
        delete: vi.fn().mockImplementation(async () => {
          if (!docs.has(id)) {
            const err = new Error('NotFound') as Error & { code: number };
            err.code = 404;
            throw err;
          }
          deletedIds.push(id);
          docs.delete(id);
          return {};
        }),
      })),
    };
    return container;
  }
  const mockState = {
    cache: makeContainer('cache'),
  };
  return { mockState, makeContainer };
});

vi.mock('@azure/cosmos', () => {
  const databaseObj = {
    container: vi.fn(() => mockState.cache),
  };
  return {
    CosmosClient: vi.fn().mockImplementation(() => ({
      database: vi.fn().mockReturnValue(databaseObj),
    })),
  };
});

import { CosmosClient } from '@azure/cosmos';
import { CosmosCacheProvider } from '../src/CosmosCacheProvider.js';

describe('CosmosCacheProvider', () => {
  let client: CosmosClient;
  let provider: CosmosCacheProvider;

  beforeEach(() => {
    mockState.cache = makeContainer('cache');
    client = new CosmosClient({ endpoint: 'x', key: 'y' });
    provider = new CosmosCacheProvider({ client, database: 'db' });
  });

  describe('set', () => {
    it('upserts the value with id, pk, and value fields', async () => {
      await provider.set('k1', 'v1');
      expect(mockState.cache.upsertCalls).toHaveLength(1);
      const doc = mockState.cache.upsertCalls[0];
      expect(doc.id).toBe('k1');
      expect(doc.pk).toBe('k1');
      expect(doc.value).toBe('v1');
    });

    it('writes ttl when opts.ttlSeconds is provided', async () => {
      await provider.set('k1', 'v1', { ttlSeconds: 60 });
      const doc = mockState.cache.upsertCalls[0];
      expect(doc.ttl).toBe(60);
    });

    it('omits ttl when opts.ttlSeconds is not provided (container default applies)', async () => {
      await provider.set('k1', 'v1');
      const doc = mockState.cache.upsertCalls[0];
      expect(doc).not.toHaveProperty('ttl');
    });

    it('falls back to constructor ttlSeconds when opts.ttlSeconds is omitted', async () => {
      const ttlProvider = new CosmosCacheProvider({
        client,
        database: 'db',
        ttlSeconds: 3600,
      });
      await ttlProvider.set('k1', 'v1');
      const doc = mockState.cache.upsertCalls[0];
      expect(doc.ttl).toBe(3600);
    });

    it('per-call opts.ttlSeconds wins over constructor default', async () => {
      const ttlProvider = new CosmosCacheProvider({
        client,
        database: 'db',
        ttlSeconds: 3600,
      });
      await ttlProvider.set('k1', 'v1', { ttlSeconds: 60 });
      const doc = mockState.cache.upsertCalls[0];
      expect(doc.ttl).toBe(60);
    });
  });

  describe('get', () => {
    it('returns the value when present', async () => {
      mockState.cache.docs.set('k1', { id: 'k1', pk: 'k1', value: 'cached' });
      const got = await provider.get('k1');
      expect(got).toBe('cached');
    });

    it('returns undefined on a 404 (missing key)', async () => {
      const got = await provider.get('missing');
      expect(got).toBeUndefined();
    });
  });

  describe('delete', () => {
    it('removes the item', async () => {
      mockState.cache.docs.set('k1', { id: 'k1', pk: 'k1', value: 'v' });
      await provider.delete('k1');
      expect(mockState.cache.deletedIds).toContain('k1');
    });

    it('is idempotent on missing keys (does not throw)', async () => {
      await expect(provider.delete('never-existed')).resolves.toBeUndefined();
    });
  });

  describe('clear', () => {
    it('queries all ids and deletes each', async () => {
      mockState.cache.docs.set('a', { id: 'a', pk: 'a', value: '1' });
      mockState.cache.docs.set('b', { id: 'b', pk: 'b', value: '2' });
      mockState.cache.docs.set('c', { id: 'c', pk: 'c', value: '3' });
      mockState.cache.queryResources = [{ id: 'a' }, { id: 'b' }, { id: 'c' }];
      await provider.clear();
      // The query was issued; every returned id was deleted.
      expect(mockState.cache.queryCalls.length).toBeGreaterThanOrEqual(1);
      expect(mockState.cache.deletedIds.sort()).toEqual(['a', 'b', 'c']);
    });
  });
});
