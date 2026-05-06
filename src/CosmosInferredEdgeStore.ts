import { CosmosClient } from '@azure/cosmos';
import type { Container, Database } from '@azure/cosmos';
import type {
  InferredEdge,
  InferredEdgeSource,
  InferredEdgeStore,
  NodeId,
  SearchVectorHit,
  Vector,
} from '@inferagraph/core';

/**
 * Configuration for {@link CosmosInferredEdgeStore}. Provider-agnostic:
 * `embeddingPath`, `embeddingDimensions` are constructor options so the
 * store does not assume a specific embedding model or provider.
 */
export interface CosmosInferredEdgeStoreConfig {
  client: CosmosClient;
  database: string;
  /** Defaults to `'inferred_edges'`. */
  container?: string;
  /** Defaults to `'/embedding'`. */
  embeddingPath?: string;
  /** Defaults to `3072`. Stored on the config for symmetry with provisioning. */
  embeddingDimensions?: number;
}

/**
 * Persistent {@link InferredEdgeStore} backed by a separate Cosmos NoSQL
 * container (default `inferred_edges`) with its own vector index on the
 * embedding field.
 *
 * Beyond the {@link InferredEdgeStore} contract this class also exposes
 * {@link searchInferredEdges} — a vector-native top-K query the AIEngine's
 * hybrid retrieval calls when surfacing inferred relationships in chat
 * prompts.
 */
export class CosmosInferredEdgeStore implements InferredEdgeStore {
  private readonly client: CosmosClient;
  private readonly database: Database;
  private readonly containerName: string;
  private readonly fieldName: string;

  constructor(config: CosmosInferredEdgeStoreConfig) {
    this.client = config.client;
    this.database = this.client.database(config.database);
    this.containerName = config.container ?? 'inferred_edges';
    const path = config.embeddingPath ?? '/embedding';
    this.fieldName = path.startsWith('/') ? path.slice(1) : path;
  }

  async get(sourceId: NodeId, targetId: NodeId): Promise<InferredEdge | undefined> {
    const container = this.container();
    const { resources } = await container.items
      .query({
        query: `SELECT * FROM c WHERE c.sourceId = @s AND c.targetId = @t`,
        parameters: [
          { name: '@s', value: sourceId },
          { name: '@t', value: targetId },
        ],
      })
      .fetchAll();
    if (resources.length === 0) return undefined;
    return this.toEdge(resources[0] as Record<string, unknown>);
  }

  async getAllForNode(nodeId: NodeId): Promise<InferredEdge[]> {
    const container = this.container();
    const { resources } = await container.items
      .query({
        query: `SELECT * FROM c WHERE c.sourceId = @id OR c.targetId = @id`,
        parameters: [{ name: '@id', value: nodeId }],
      })
      .fetchAll();
    return (resources as Record<string, unknown>[]).map(d => this.toEdge(d));
  }

  async getAll(): Promise<InferredEdge[]> {
    const container = this.container();
    const { resources } = await container.items
      .query({ query: `SELECT * FROM c`, parameters: [] })
      .fetchAll();
    return (resources as Record<string, unknown>[]).map(d => this.toEdge(d));
  }

  /**
   * Replace the entire stored set with `edges`. Implementation: list all
   * existing rows, delete each, then upsert every entry in `edges`. Cosmos
   * has no atomic "drop and replace" so this two-step approach is the
   * pragmatic equivalent. When the same `(sourceId, targetId)` appears
   * multiple times in `edges`, the LAST occurrence wins per the
   * {@link InferredEdgeStore} contract — `upsert` with the same id naturally
   * gives that behavior.
   */
  async set(edges: ReadonlyArray<InferredEdge>): Promise<void> {
    const container = this.container();
    // Snapshot existing ids and delete them.
    const { resources: existing } = await container.items
      .query({ query: `SELECT c.id, c.sourceId FROM c`, parameters: [] })
      .fetchAll();
    for (const row of existing as { id: string; sourceId?: string }[]) {
      try {
        await container.item(row.id, row.sourceId).delete();
      } catch (err) {
        // Idempotent: already-gone rows are fine.
        if (!is404(err)) throw err;
      }
    }
    for (const edge of edges) {
      await container.items.upsert(this.toDoc(edge));
    }
  }

  async clear(): Promise<void> {
    const container = this.container();
    const { resources } = await container.items
      .query({ query: `SELECT c.id, c.sourceId FROM c`, parameters: [] })
      .fetchAll();
    for (const row of resources as { id: string; sourceId?: string }[]) {
      try {
        await container.item(row.id, row.sourceId).delete();
      } catch (err) {
        if (!is404(err)) throw err;
      }
    }
  }

  /**
   * Vector-native top-K against the inferred_edges container. Same SQL
   * shape as {@link CosmosVectorEmbeddingStore.searchVector}; sorted descending
   * by score so the highest-similarity hits surface first.
   */
  async searchInferredEdges(
    queryEmbedding: Vector,
    top: number,
  ): Promise<SearchVectorHit[]> {
    const container = this.container();
    const { resources } = await container.items
      .query({
        query: `SELECT TOP @k c.id, VectorDistance(c.${this.fieldName}, @q) AS score FROM c ORDER BY VectorDistance(c.${this.fieldName}, @q)`,
        parameters: [
          { name: '@k', value: top },
          { name: '@q', value: queryEmbedding },
        ],
      })
      .fetchAll();
    const hits: SearchVectorHit[] = (resources as { id: string; score: number }[]).map(row => ({
      nodeId: row.id,
      score: row.score,
    }));
    hits.sort((a, b) => b.score - a.score);
    return hits;
  }

  private container(): Container {
    return this.database.container(this.containerName);
  }

  private toEdge(doc: Record<string, unknown>): InferredEdge {
    return {
      sourceId: String(doc.sourceId),
      targetId: String(doc.targetId),
      type: String(doc.type ?? ''),
      score: Number(doc.score ?? 0),
      sources: (doc.sources as InferredEdgeSource[] | undefined) ?? [],
      reasoning: doc.reasoning as string | undefined,
      perSource: doc.perSource as InferredEdge['perSource'],
    };
  }

  private toDoc(edge: InferredEdge): Record<string, unknown> {
    return {
      id: `${edge.sourceId}-${edge.targetId}-${edge.type}`,
      sourceId: edge.sourceId,
      targetId: edge.targetId,
      type: edge.type,
      score: edge.score,
      sources: edge.sources,
      reasoning: edge.reasoning,
      perSource: edge.perSource,
    };
  }
}

/**
 * Configuration for {@link cosmosInferredEdgeStore}. Mirrors
 * {@link CosmosInferredEdgeStoreConfig} but trades the pre-built `client`
 * for `endpoint` + `key`.
 */
export interface CosmosInferredEdgeStoreFactoryConfig {
  endpoint: string;
  key: string;
  database: string;
  container?: string;
  embeddingPath?: string;
  embeddingDimensions?: number;
  /** Documentation-only mirror of provisioning's vector data type. */
  dataType?: 'Float32' | 'Float16' | 'Int8';
}

/**
 * Construct a {@link CosmosInferredEdgeStore}. Factory owns SDK construction;
 * use the class constructor for shared-client scenarios.
 */
export function cosmosInferredEdgeStore(
  config: CosmosInferredEdgeStoreFactoryConfig,
): CosmosInferredEdgeStore {
  const client = new CosmosClient({ endpoint: config.endpoint, key: config.key });
  return new CosmosInferredEdgeStore({
    client,
    database: config.database,
    container: config.container,
    embeddingPath: config.embeddingPath,
    embeddingDimensions: config.embeddingDimensions,
  });
}

function is404(err: unknown): boolean {
  if (!err || typeof err !== 'object') return false;
  const code = (err as { code?: number | string }).code;
  return code === 404 || code === '404';
}
