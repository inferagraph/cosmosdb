import { CosmosClient } from '@azure/cosmos';
import type { Container, Database } from '@azure/cosmos';
import type { CacheProvider } from '@inferagraph/core/data';

/**
 * Configuration for {@link CosmosCacheProvider}. Backing storage is one
 * Cosmos NoSQL container (default `'cache'`) where every cache entry is one
 * document keyed by the cache key.
 *
 * Document shape:
 *   `{ id, pk: id, value, ttl? }`
 *
 * Cosmos system TTL handles expiry. The container's `defaultTtl` should be
 * set when the host wants a global expiry policy; per-entry overrides via
 * `set(..., { ttlSeconds })` win over the container default.
 */
export interface CosmosCacheProviderConfig {
  /** Pre-built Cosmos client (escape hatch). */
  client: CosmosClient;
  database: string;
  /** Defaults to `'cache'`. */
  container?: string;
  /**
   * Default per-entry TTL in seconds when {@link CacheProvider.set} is called
   * without an explicit `opts.ttlSeconds`. When omitted, writes carry no
   * `ttl` field and the container's `defaultTtl` (or "no expiry") applies.
   */
  ttlSeconds?: number;
}

/**
 * Persistent {@link CacheProvider} backed by a Cosmos NoSQL container.
 *
 * - `set(key, value, opts?)` upserts a `{ id, pk, value, ttl? }` doc. When
 *   `opts.ttlSeconds` is given, that wins; otherwise falls back to the
 *   constructor default; otherwise omits `ttl`.
 * - `get(key)` point-reads the document; Cosmos' TTL evicts expired items
 *   server-side so a 404 maps to `undefined`.
 * - `delete(key)` removes the item; missing keys are a no-op (404 swallowed).
 * - `clear()` queries every id in the container and deletes each. Note: in
 *   high-volume deployments this scales with cache size — hosts that
 *   periodically cycle the cache should drop and recreate the container
 *   instead.
 */
export class CosmosCacheProvider implements CacheProvider {
  private readonly database: Database;
  private readonly containerName: string;
  private readonly defaultTtlSeconds?: number;

  constructor(config: CosmosCacheProviderConfig) {
    this.database = config.client.database(config.database);
    this.containerName = config.container ?? 'cache';
    this.defaultTtlSeconds = config.ttlSeconds;
  }

  async get(key: string): Promise<string | undefined> {
    try {
      const result = await this.container().item(key).read();
      const resource = (result as { resource?: { value?: string } }).resource;
      if (!resource) return undefined;
      return resource.value;
    } catch (err) {
      if (is404(err)) return undefined;
      throw err;
    }
  }

  async set(key: string, value: string, opts?: { ttlSeconds?: number }): Promise<void> {
    const doc: Record<string, unknown> = {
      id: key,
      pk: key,
      value,
    };
    const ttl = opts?.ttlSeconds ?? this.defaultTtlSeconds;
    if (ttl !== undefined) doc.ttl = ttl;
    await this.container().items.upsert(doc);
  }

  async delete(key: string): Promise<void> {
    try {
      await this.container().item(key).delete();
    } catch (err) {
      if (!is404(err)) throw err;
    }
  }

  async clear(): Promise<void> {
    const container = this.container();
    const { resources } = await container.items
      .query({ query: 'SELECT c.id FROM c', parameters: [] })
      .fetchAll();
    for (const row of resources as { id: string }[]) {
      try {
        await container.item(row.id).delete();
      } catch (err) {
        if (!is404(err)) throw err;
      }
    }
  }

  private container(): Container {
    return this.database.container(this.containerName);
  }
}

/**
 * Configuration for {@link cosmosCacheProvider}. Mirrors
 * {@link CosmosCacheProviderConfig} but trades the pre-built `client` for
 * `endpoint` + `key`.
 */
export interface CosmosCacheProviderFactoryConfig {
  endpoint: string;
  key: string;
  database: string;
  container?: string;
  ttlSeconds?: number;
}

/**
 * Construct a {@link CosmosCacheProvider}. Factory owns SDK construction; use
 * the class constructor for shared-client scenarios.
 */
export function cosmosCacheProvider(
  config: CosmosCacheProviderFactoryConfig,
): CosmosCacheProvider {
  const client = new CosmosClient({ endpoint: config.endpoint, key: config.key });
  return new CosmosCacheProvider({
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
