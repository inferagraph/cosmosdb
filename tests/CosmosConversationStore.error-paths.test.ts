import { describe, it, expect, vi, beforeEach } from 'vitest';

// ---------------------------------------------------------------------------
// Coverage for the non-404 error path in CosmosConversationStore.readDoc():
// `getTurns()` reaches readDoc() which swallows 404 (returning undefined →
// empty turns) but must rethrow on any other code so the host sees real
// outages instead of an empty conversation that silently masks them.
//
// Also covers `clear()` rethrowing a non-404 delete failure — the same
// "swallow 404, surface everything else" contract the cache provider uses.
// ---------------------------------------------------------------------------
const { mockState, makeContainer } = vi.hoisted(() => {
  function makeContainer() {
    const docs = new Map<string, Record<string, unknown>>();
    let nextReadError: { code: number | string } | undefined;
    let nextDeleteError: { code: number | string } | undefined;

    const container = {
      docs,
      get nextReadError() { return nextReadError; },
      set nextReadError(v: { code: number | string } | undefined) { nextReadError = v; },
      get nextDeleteError() { return nextDeleteError; },
      set nextDeleteError(v: { code: number | string } | undefined) { nextDeleteError = v; },
      items: {
        upsert: vi.fn().mockResolvedValue({ resource: {} }),
        query: vi.fn().mockReturnValue({
          fetchAll: vi.fn().mockResolvedValue({ resources: [] }),
        }),
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
        patch: vi.fn().mockResolvedValue({ resource: {} }),
      })),
    };
    return container;
  }
  const mockState = { conversations: makeContainer() };
  return { mockState, makeContainer };
});

vi.mock('@azure/cosmos', () => {
  const databaseObj = { container: vi.fn(() => mockState.conversations) };
  return {
    CosmosClient: vi.fn().mockImplementation(() => ({
      database: vi.fn().mockReturnValue(databaseObj),
    })),
  };
});

import { CosmosClient } from '@azure/cosmos';
import { CosmosConversationStore } from '../src/CosmosConversationStore.js';

describe('CosmosConversationStore — non-404 error paths', () => {
  let store: CosmosConversationStore;

  beforeEach(() => {
    mockState.conversations = makeContainer();
    const client = new CosmosClient({ endpoint: 'x', key: 'y' });
    store = new CosmosConversationStore({ client, database: 'db' });
  });

  it('getTurns() rethrows a non-404 read failure (caller sees the outage)', async () => {
    mockState.conversations.nextReadError = { code: 503 };
    await expect(store.getTurns('c1', 10)).rejects.toThrow('forced-read');
  });

  it('clear() rethrows a non-404 delete failure (does not silently swallow)', async () => {
    mockState.conversations.nextDeleteError = { code: 500 };
    await expect(store.clear('c1')).rejects.toThrow('forced-delete');
  });
});
