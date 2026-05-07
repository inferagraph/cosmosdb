import { describe, it, expect, vi, beforeEach } from 'vitest';

// Single-document-per-conversation backing. Each test starts with a fresh
// in-memory store so reads return undefined unless a prior write seeded the
// document. The mock mimics:
//   - container.items.upsert(item) -> stores { id, ... }
//   - container.item(id, pk).read() -> resource | 404
//   - container.item(id, pk).delete() -> removes the entry
const { mockState, makeContainer } = vi.hoisted(() => {
  type PatchOp =
    | { op: 'add'; path: string; value: unknown }
    | { op: 'remove'; path: string };

  function makeContainer(name: string) {
    const docs = new Map<string, Record<string, unknown>>();
    const upsertCalls: Record<string, unknown>[] = [];
    const deletedIds: string[] = [];
    const patchCalls: { id: string; pk: unknown; ops: PatchOp[] }[] = [];
    const container = {
      _name: name,
      docs,
      upsertCalls,
      deletedIds,
      patchCalls,
      items: {
        upsert: vi.fn().mockImplementation(async (item: Record<string, unknown>) => {
          upsertCalls.push({ ...item });
          docs.set(String(item.id), { ...item });
          return { resource: item };
        }),
        query: vi.fn().mockReturnValue({
          fetchAll: vi.fn().mockResolvedValue({ resources: [] }),
        }),
      },
      item: vi.fn().mockImplementation((id: string, pk?: unknown) => ({
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
        patch: vi.fn().mockImplementation(async (ops: PatchOp[]) => {
          patchCalls.push({ id, pk, ops });
          let doc = docs.get(id);
          if (!doc) {
            const err = new Error('NotFound') as Error & { code: number };
            err.code = 404;
            throw err;
          }
          doc = { ...doc };
          for (const op of ops) {
            if (op.op === 'add' && op.path === '/turns/-') {
              const turns = Array.isArray(doc.turns)
                ? [...(doc.turns as unknown[])]
                : [];
              turns.push((op as { value: unknown }).value);
              doc.turns = turns;
              continue;
            }
            const field = op.path.startsWith('/') ? op.path.slice(1) : op.path;
            if (op.op === 'add') {
              doc[field] = (op as { value: unknown }).value;
            } else if (op.op === 'remove') {
              if (!(field in doc)) {
                const err = new Error('PathNotFound') as Error & { code: number };
                err.code = 400;
                throw err;
              }
              delete doc[field];
            }
          }
          docs.set(id, doc);
          return { resource: doc };
        }),
      })),
    };
    return container;
  }
  const mockState = {
    conversations: makeContainer('conversations'),
  };
  return { mockState, makeContainer };
});

vi.mock('@azure/cosmos', () => {
  const databaseObj = {
    container: vi.fn(() => mockState.conversations),
  };
  return {
    CosmosClient: vi.fn().mockImplementation(() => ({
      database: vi.fn().mockReturnValue(databaseObj),
    })),
  };
});

import { CosmosClient } from '@azure/cosmos';
import { CosmosConversationStore } from '../src/CosmosConversationStore.js';

describe('CosmosConversationStore', () => {
  let client: CosmosClient;
  let store: CosmosConversationStore;

  beforeEach(() => {
    mockState.conversations = makeContainer('conversations');
    client = new CosmosClient({ endpoint: 'x', key: 'y' });
    store = new CosmosConversationStore({
      client,
      database: 'db',
    });
  });

  describe('appendTurn', () => {
    it('creates a new conversation document on first append', async () => {
      await store.appendTurn('c1', {
        role: 'user',
        content: 'hello',
        timestamp: 1_000,
      });
      expect(mockState.conversations.upsertCalls).toHaveLength(1);
      const doc = mockState.conversations.upsertCalls[0];
      expect(doc.id).toBe('c1');
      expect(doc.pk).toBe('c1');
      expect(doc.turns).toEqual([
        { role: 'user', content: 'hello', timestamp: 1_000 },
      ]);
    });

    it('appends to an existing conversation, preserving prior turns', async () => {
      await store.appendTurn('c1', { role: 'user', content: 'hi', timestamp: 1 });
      await store.appendTurn('c1', { role: 'assistant', content: 'hi back', timestamp: 2 });
      // Second append goes through patch; final doc state is the source of truth.
      const doc = mockState.conversations.docs.get('c1')!;
      expect(doc.turns).toEqual([
        { role: 'user', content: 'hi', timestamp: 1 },
        { role: 'assistant', content: 'hi back', timestamp: 2 },
      ]);
    });

    it('refreshes ttl on every append when ttlSeconds is configured (sliding window)', async () => {
      const ttlStore = new CosmosConversationStore({
        client,
        database: 'db',
        ttlSeconds: 3600,
      });
      await ttlStore.appendTurn('c1', { role: 'user', content: 'a', timestamp: 1 });
      await ttlStore.appendTurn('c1', { role: 'assistant', content: 'b', timestamp: 2 });
      // First append seeds the doc via upsert; subsequent appends slide ttl
      // forward via a patch `add /ttl`. Both writes carry the configured TTL.
      const firstUpsert = mockState.conversations.upsertCalls[0];
      expect(firstUpsert.ttl).toBe(3600);
      const ttlPatch = mockState.conversations.patchCalls.find(c =>
        c.ops.some(o => o.path === '/ttl'),
      );
      expect(ttlPatch).toBeDefined();
      const ttlOp = ttlPatch!.ops.find(o => o.path === '/ttl') as
        | { value: number }
        | undefined;
      expect(ttlOp?.value).toBe(3600);
    });

    it('omits ttl when ttlSeconds is not configured', async () => {
      await store.appendTurn('c1', { role: 'user', content: 'hi', timestamp: 1 });
      const doc = mockState.conversations.upsertCalls[0];
      expect(doc.ttl).toBeUndefined();
    });
  });

  describe('getTurns', () => {
    it('returns empty array for unknown conversations', async () => {
      const turns = await store.getTurns('unknown', 10);
      expect(turns).toEqual([]);
    });

    it('returns turns in oldest-to-newest order, respecting the limit (tail window)', async () => {
      await store.appendTurn('c1', { role: 'user', content: 'a', timestamp: 1 });
      await store.appendTurn('c1', { role: 'assistant', content: 'b', timestamp: 2 });
      await store.appendTurn('c1', { role: 'user', content: 'c', timestamp: 3 });
      await store.appendTurn('c1', { role: 'assistant', content: 'd', timestamp: 4 });

      // limit=2 -> oldest-first within the most-recent 2 turns
      const turns = await store.getTurns('c1', 2);
      expect(turns).toHaveLength(2);
      expect(turns[0].content).toBe('c');
      expect(turns[1].content).toBe('d');
    });

    it('returns all turns when limit exceeds count', async () => {
      await store.appendTurn('c1', { role: 'user', content: 'a', timestamp: 1 });
      const turns = await store.getTurns('c1', 100);
      expect(turns).toHaveLength(1);
      expect(turns[0].content).toBe('a');
    });
  });

  describe('clear', () => {
    it('deletes the conversation document', async () => {
      await store.appendTurn('c1', { role: 'user', content: 'a', timestamp: 1 });
      await store.clear('c1');
      expect(mockState.conversations.deletedIds).toContain('c1');
      const turns = await store.getTurns('c1', 10);
      expect(turns).toEqual([]);
    });

    it('is idempotent on missing conversations (does not throw)', async () => {
      await expect(store.clear('never-existed')).resolves.toBeUndefined();
    });
  });
});
