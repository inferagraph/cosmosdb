import { describe, it, expect, vi, beforeEach } from 'vitest';

// ---------------------------------------------------------------------------
// Coverage for the non-404 error paths in CosmosCacheProvider:
//   - `get(key)` rethrows when read fails with a non-404 code (e.g. 503).
//   - `clear()` rethrows when an individual delete fails with a non-404 code.
// The 404 paths are already covered by the main suite; these tests pin the
// rethrow branches that the data-safety audit added explicit handling for.
// ---------------------------------------------------------------------------
const { mockState, makeContainer } = vi.hoisted(() => {
  function makeContainer() {
    const docs = new Map<string, Record<string, unknown>>();
    let queryResources: unknown[] = [];
    let nextReadError: { code: number | string } | undefined;
    let nextDeleteError: { code: number | string } | undefined;

    const container = {
      docs,
      get queryResources() { return queryResources; },
      set queryResources(v: unknown[]) { queryResources = v; },
      get nextReadError() { return nextReadError; },
      set nextReadError(v: { code: number | string } | undefined) { nextReadError = v; },
      get nextDeleteError() { return nextDeleteError; },
      set nextDeleteError(v: { code: number | string } | undefined) { nextDeleteError = v; },
      items: {
        upsert: vi.fn().mockResolvedValue({ resource: {} }),
        query: vi.fn().mockImplementation(() => ({
          fetchAll: vi.fn().mockResolvedValue({ resources: queryResources }),
        })),
      },
      item: vi.fn().mockImplementation((id: string) => ({
        read: vi.fn().mockImplementation(async () => {
          if (nextReadError) {
            const err = new Error('forced-read') as Error & { code: number | string };
            err.code = nextReadError.code;
            throw err;
          }
          return { resource: docs.get(id) };
        }),
        delete: vi.fn().mockImplementation(async () => {
          if (nextDeleteError) {
            const err = new Error('forced-delete') as Error & { code: number | string };
            err.code = nextDeleteError.code;
            throw err;
          }
          docs.delete(id);
          return {};
        }),
      })),
    };
    return container;
  }
  const mockState = { cache: makeContainer() };
  return { mockState, makeContainer };
});

vi.mock('@azure/cosmos', () => {
  const databaseObj = { container: vi.fn(() => mockState.cache) };
  return {
    CosmosClient: vi.fn().mockImplementation(() => ({
      database: vi.fn().mockReturnValue(databaseObj),
    })),
  };
});

import { CosmosClient } from '@azure/cosmos';
import { CosmosCacheProvider } from '../src/CosmosCacheProvider.js';

describe('CosmosCacheProvider — non-404 error paths', () => {
  let provider: CosmosCacheProvider;

  beforeEach(() => {
    mockState.cache = makeContainer();
    const client = new CosmosClient({ endpoint: 'x', key: 'y' });
    provider = new CosmosCacheProvider({ client, database: 'db' });
  });

  it('get() rethrows a non-404 read failure (does not silently return undefined)', async () => {
    // 404 is the documented "missing entry" signal that maps to undefined.
    // Anything else (503, network error, etc.) must propagate so the host
    // knows the cache layer is unhealthy.
    mockState.cache.nextReadError = { code: 503 };
    await expect(provider.get('k1')).rejects.toThrow('forced-read');
  });

  it('clear() rethrows a non-404 delete failure (does not silently swallow real errors)', async () => {
    // The clear() loop tolerates 404 (the row is already gone) but must
    // surface any other code so a partial wipe in the face of a real outage
    // is visible to the caller.
    mockState.cache.queryResources = [{ id: 'a' }];
    mockState.cache.nextDeleteError = { code: 500 };
    await expect(provider.clear()).rejects.toThrow('forced-delete');
  });
});
