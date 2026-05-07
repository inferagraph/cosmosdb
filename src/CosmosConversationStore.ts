import { CosmosClient } from '@azure/cosmos';
import type { Container, Database } from '@azure/cosmos';
import type { ConversationStore, ConversationTurn } from '@inferagraph/core/data';

/**
 * Configuration for {@link CosmosConversationStore}. Backing storage is one
 * Cosmos NoSQL container (default `'conversations'`) where each conversation
 * is a single document keyed by id with the turn array inlined. Single-doc
 * is the simpler choice and works for conversations up to ~few hundred
 * turns; high-volume hosts should swap in a per-turn store implementation.
 */
export interface CosmosConversationStoreConfig {
  /** Pre-built Cosmos client (escape hatch for shared-client/custom-auth). */
  client: CosmosClient;
  database: string;
  /** Defaults to `'conversations'`. */
  container?: string;
  /**
   * Sliding TTL (seconds) refreshed on every {@link appendTurn}. When omitted,
   * the document is written without a `ttl` field and Cosmos applies the
   * container's default policy (no expiry unless `defaultTtl` is set on the
   * container). Refreshing on append gives "expire N seconds after last
   * activity" behavior, which is the standard chat-session pattern.
   */
  ttlSeconds?: number;
}

interface ConversationDoc {
  id: string;
  pk: string;
  turns: ConversationTurn[];
  ttl?: number;
}

/**
 * Persistent {@link ConversationStore} backed by a Cosmos NoSQL container.
 *
 * Document shape (one per conversation):
 *   `{ id, pk: id, turns: ConversationTurn[], ttl?: number }`
 *
 * `appendTurn` reads the existing doc, pushes the turn, writes back. When
 * {@link CosmosConversationStoreConfig.ttlSeconds} is set, every write
 * carries `ttl` so the expiry slides forward on each turn — Cosmos system
 * TTL handles eviction.
 *
 * `getTurns(id, limit)` returns the tail `limit` turns in oldest-to-newest
 * order. Missing conversations yield `[]` (no throw).
 */
export class CosmosConversationStore implements ConversationStore {
  private readonly database: Database;
  private readonly containerName: string;
  private readonly ttlSeconds?: number;

  constructor(config: CosmosConversationStoreConfig) {
    this.database = config.client.database(config.database);
    this.containerName = config.container ?? 'conversations';
    this.ttlSeconds = config.ttlSeconds;
  }

  async getTurns(conversationId: string, limit: number): Promise<ConversationTurn[]> {
    const doc = await this.readDoc(conversationId);
    if (!doc) return [];
    const turns = Array.isArray(doc.turns) ? doc.turns : [];
    if (limit <= 0) return [];
    return turns.slice(Math.max(0, turns.length - limit));
  }

  async appendTurn(conversationId: string, turn: ConversationTurn): Promise<void> {
    const container = this.container();
    const partitionKeyValue = conversationId;
    // First-touch path: doc does not exist yet, so seed it via upsert. Once
    // it exists, every subsequent append uses patch so host-owned metadata
    // fields on the doc survive (the previous read-merge-upsert path wiped
    // them on every turn).
    try {
      const ops: Array<{ op: 'add'; path: string; value: unknown }> = [
        { op: 'add', path: '/turns/-', value: turn },
      ];
      if (this.ttlSeconds !== undefined) {
        ops.push({ op: 'add', path: '/ttl', value: this.ttlSeconds });
      }
      await container.item(conversationId, partitionKeyValue).patch(ops);
      return;
    } catch (err) {
      if (!is404(err)) throw err;
    }
    // Doc not present — create it. This is the only place we ever upsert.
    const doc: ConversationDoc = {
      id: conversationId,
      pk: conversationId,
      turns: [turn],
    };
    if (this.ttlSeconds !== undefined) doc.ttl = this.ttlSeconds;
    await container.items.upsert(doc);
  }

  async clear(conversationId: string): Promise<void> {
    try {
      await this.container().item(conversationId).delete();
    } catch (err) {
      if (!is404(err)) throw err;
    }
  }

  private async readDoc(conversationId: string): Promise<ConversationDoc | undefined> {
    try {
      const result = await this.container().item(conversationId).read();
      return (result as { resource?: ConversationDoc }).resource;
    } catch (err) {
      if (is404(err)) return undefined;
      throw err;
    }
  }

  private container(): Container {
    return this.database.container(this.containerName);
  }
}

/**
 * Configuration for {@link cosmosConversationStore}. Mirrors
 * {@link CosmosConversationStoreConfig} but trades the pre-built `client`
 * for `endpoint` + `key`.
 */
export interface CosmosConversationStoreFactoryConfig {
  endpoint: string;
  key: string;
  database: string;
  container?: string;
  ttlSeconds?: number;
}

/**
 * Construct a {@link CosmosConversationStore}. Factory owns SDK construction;
 * use the class constructor for shared-client scenarios.
 */
export function cosmosConversationStore(
  config: CosmosConversationStoreFactoryConfig,
): CosmosConversationStore {
  const client = new CosmosClient({ endpoint: config.endpoint, key: config.key });
  return new CosmosConversationStore({
    client,
    database: config.database,
    container: config.container,
    ttlSeconds: config.ttlSeconds,
  });
}

function is404(err: unknown): boolean {
  if (!err || typeof err !== 'object') return false;
  const code = (err as { code?: number | string }).code;
  return code === 404 || code === '404';
}
