import { describe, it, expect, vi, beforeEach } from 'vitest';

// ---------------------------------------------------------------------------
// `appendTurn` previously did `read -> push turn -> upsert`. Hosts that wrote
// metadata (user labels, custom flags) onto the conversation doc had those
// fields wiped on the next append. The fix uses Cosmos JSON Patch
// `add /turns/-` to append atomically.
// ---------------------------------------------------------------------------
const { mockState, makeContainer } = vi.hoisted(() => {
  type PatchOp =
    | { op: 'add'; path: string; value: unknown }
    | { op: 'remove'; path: string };

  function makeContainer(name: string) {
    const docs = new Map<string, Record<string, unknown>>();
    const upsertCalls: Record<string, unknown>[] = [];
    const patchCalls: { id: string; pk: unknown; ops: PatchOp[] }[] = [];

    const container = {
      _name: name,
      docs,
      upsertCalls,
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
      item: vi.fn().mockImplementation((id: string, pk: unknown) => ({
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
            // Handle the `/turns/-` array-append idiom.
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

  const mockState = { conversations: makeContainer('conversations') };
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

describe('CosmosConversationStore.appendTurn — preserves existing fields (regression)', () => {
  let client: CosmosClient;
  let store: CosmosConversationStore;

  beforeEach(() => {
    mockState.conversations = makeContainer('conversations');
    client = new CosmosClient({ endpoint: 'x', key: 'y' });
    store = new CosmosConversationStore({ client, database: 'db' });
  });

  it('keeps host-owned metadata fields intact when appending a turn', async () => {
    // Host previously wrote a custom flag onto the conversation doc.
    mockState.conversations.docs.set('c1', {
      id: 'c1',
      pk: 'c1',
      turns: [{ role: 'user', content: 'hello', timestamp: 1 }],
      userLabel: 'session-from-mobile',
      customFlag: true,
    });

    await store.appendTurn('c1', {
      role: 'assistant',
      content: 'hi back',
      timestamp: 2,
    });

    const doc = mockState.conversations.docs.get('c1');
    expect(doc).toBeDefined();
    expect(doc!.turns).toEqual([
      { role: 'user', content: 'hello', timestamp: 1 },
      { role: 'assistant', content: 'hi back', timestamp: 2 },
    ]);
    // Host metadata MUST survive.
    expect(doc!.userLabel).toBe('session-from-mobile');
    expect(doc!.customFlag).toBe(true);
    // Patch path, not upsert.
    expect(mockState.conversations.upsertCalls).toHaveLength(0);
    expect(mockState.conversations.patchCalls.length).toBeGreaterThanOrEqual(1);
  });
});
