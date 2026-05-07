import { CosmosClient } from '@azure/cosmos';
import type { Container, Database } from '@azure/cosmos';
import type {
  EmbeddingRecord,
  EmbeddingStore,
  NodeId,
  SearchVectorHit,
  SimilarHit,
  Vector,
} from '@inferagraph/core/data';

/**
 * Configuration for {@link CosmosVectorEmbeddingStore}. Targets the Cosmos NoSQL
 * SQL/document protocol — provider-specific knobs (`embeddingPath`,
 * `embeddingDimensions`) are constructor options with neutral defaults so the
 * datasource stays provider-agnostic (no hard-coded OpenAI assumptions).
 */
export interface CosmosVectorEmbeddingStoreConfig {
  /** Live, already-constructed Cosmos client; tests inject a stub. */
  client: CosmosClient;
  /** Logical database name. */
  database: string;
  /** Default container the store reads + writes embeddings against (typically `units`). */
  container: string;
  /**
   * JSON path to the embedding array on each document. Defaults to
   * `'/embedding'` (the convention used by `provisionVectorContainers`); the
   * SQL `c.embedding` accessor is derived from this path's last segment so
   * hosts may rename the field if their schema requires it.
   */
  embeddingPath?: string;
  /** Optional embedding model used when {@link EmbeddingStore.set} is called without explicit meta — currently unused at runtime, kept for future provisioning helpers. */
  embeddingModel?: string;
  /**
   * Vector dimensionality — kept on the config for symmetry with
   * {@link provisionVectorContainers}; the store itself does not need to know
   * the dimension to read or write, but holding it here lets a host derive
   * provisioning args from a single source of truth.
   */
  embeddingDimensions?: number;
}

/**
 * Persistent {@link EmbeddingStore} backed by a Cosmos NoSQL container with a
 * vector index on the embedding field.
 *
 * The store doubles as the engine's `searchVector` source — the optional
 * method on {@link EmbeddingStore} that the hybrid retrieval path prefers
 * over the linear `similar()` scan. Hosts wire one instance per process and
 * point it at the units container; the same instance can also query the
 * `inferred_edges` container when callers pass `{container: 'inferred_edges'}`,
 * letting the engine read both vector indexes through one store.
 *
 * Provider-agnostic by design: the embedding model, dimensions, and JSON
 * field path are all constructor options. The store never assumes OpenAI or
 * any specific provider — it only knows that Cosmos exposes
 * `VectorDistance(c.embedding, @q)` over a vector-indexed column.
 */
export class CosmosVectorEmbeddingStore implements EmbeddingStore {
  private readonly client: CosmosClient;
  private readonly database: Database;
  private readonly defaultContainerName: string;
  private readonly fieldName: string;

  constructor(config: CosmosVectorEmbeddingStoreConfig) {
    this.client = config.client;
    this.database = this.client.database(config.database);
    this.defaultContainerName = config.container;
    // Strip the leading `/` so the SQL accessor reads `c.<field>`.
    const path = config.embeddingPath ?? '/embedding';
    this.fieldName = path.startsWith('/') ? path.slice(1) : path;
  }

  /**
   * Direct lookup keyed by `(nodeId, model, modelVersion, contentHash)`.
   * Returns `undefined` when the document is missing OR when its embedding
   * metadata does not match — the store treats stale entries as cache misses
   * so the caller will re-embed.
   */
  async get(
    nodeId: NodeId,
    model: string,
    modelVersion: string,
    contentHash: string,
  ): Promise<EmbeddingRecord | undefined> {
    const container = this.containerFor(this.defaultContainerName);
    const { resources } = await container.items
      .query({
        query: `SELECT c.id, c.${this.fieldName} AS embedding, c.embeddingModel, c.embeddingVersion, c.embeddingHash, c.embeddingGeneratedAt FROM c WHERE c.id = @id AND c.embeddingModel = @model AND c.embeddingVersion = @modelVersion AND c.embeddingHash = @hash`,
        parameters: [
          { name: '@id', value: nodeId },
          { name: '@model', value: model },
          { name: '@modelVersion', value: modelVersion },
          { name: '@hash', value: contentHash },
        ],
      })
      .fetchAll();
    if (resources.length === 0) return undefined;
    const doc = resources[0] as {
      id: string;
      embedding: Vector;
      embeddingModel: string;
      embeddingVersion: string;
      embeddingHash: string;
      embeddingGeneratedAt?: string;
    };
    return {
      nodeId: doc.id,
      vector: doc.embedding,
      meta: {
        model: doc.embeddingModel,
        modelVersion: doc.embeddingVersion,
        contentHash: doc.embeddingHash,
        generatedAt: doc.embeddingGeneratedAt ?? '',
      },
    };
  }

  /**
   * Persist an embedding by upserting the existing document with new
   * embedding fields. When the document does not yet exist (cold seed, the
   * upsert preserves the embedding fields under a stub `{id}` document) so
   * the next `tools/`-side run can patch the rest of the body.
   */
  async set(record: EmbeddingRecord): Promise<void> {
    const container = this.containerFor(this.defaultContainerName);
    let existing: Record<string, unknown> | undefined;
    try {
      const result = await container.item(record.nodeId).read();
      existing = result.resource as Record<string, unknown> | undefined;
    } catch (err) {
      // Cosmos throws a 404-coded error when the doc is missing; treat as
      // "no existing body" rather than a hard failure.
      if (!is404(err)) throw err;
    }
    const merged: Record<string, unknown> = {
      ...(existing ?? { id: record.nodeId }),
      id: record.nodeId,
      [this.fieldName]: record.vector,
      embeddingModel: record.meta.model,
      embeddingVersion: record.meta.modelVersion,
      embeddingHash: record.meta.contentHash,
      embeddingGeneratedAt: record.meta.generatedAt,
    };
    await container.items.upsert(merged);
  }

  /**
   * Linear-scan similarity over the entire container — kept on the contract
   * for parity with {@link EmbeddingStore.similar}. Production hosts always
   * call {@link searchVector} instead, which uses the vector index. This
   * method delegates to {@link searchVector} and ignores the model/version
   * scope filters since persisted entries are scoped on write (the document
   * carries `embeddingModel` + `embeddingVersion`).
   */
  async similar(
    queryVector: Vector,
    k: number,
    _model?: string,
    _modelVersion?: string,
  ): Promise<SimilarHit[]> {
    void _model;
    void _modelVersion;
    const hits = await this.searchVector(queryVector, { top: k });
    return hits.map(h => ({ nodeId: h.nodeId, score: h.score }));
  }

  /**
   * Drop every embedding field from every document in the default container.
   * Implementation note: a true `clear` would delete the documents
   * themselves, but the units container also holds the source-of-truth body —
   * we only zero out the embedding fields. Hosts that want to fully reset
   * vectors should drop and re-provision the container instead.
   */
  async clear(): Promise<void> {
    const container = this.containerFor(this.defaultContainerName);
    const { resources } = await container.items
      .query({
        query: `SELECT c.id FROM c WHERE IS_DEFINED(c.${this.fieldName})`,
        parameters: [],
      })
      .fetchAll();
    for (const doc of resources as { id: string }[]) {
      const result = await container.item(doc.id).read();
      const existing = result.resource as Record<string, unknown> | undefined;
      if (!existing) continue;
      const { [this.fieldName]: _embedding, embeddingModel: _model, embeddingVersion: _v, embeddingHash: _h, embeddingGeneratedAt: _g, ...rest } = existing;
      void _embedding; void _model; void _v; void _h; void _g;
      await container.items.upsert(rest);
    }
  }

  /**
   * Vector-native top-K against the configured container, or against
   * `inferred_edges` when `opts.container === 'inferred_edges'`. Uses Cosmos
   * `VectorDistance(c.embedding, @q)` — the function returns the configured
   * distance metric (cosine similarity by convention here, where higher
   * means more similar). Result rows are sorted descending by score; ties
   * are broken by document id implicitly via the underlying ORDER BY.
   */
  async searchVector(
    queryEmbedding: Vector,
    opts: { top: number; container?: 'units' | 'inferred_edges' },
  ): Promise<SearchVectorHit[]> {
    const containerName =
      opts.container === 'inferred_edges' ? 'inferred_edges' : this.defaultContainerName;
    const container = this.containerFor(containerName);
    const { resources } = await container.items
      .query({
        query: `SELECT TOP @k c.id, VectorDistance(c.${this.fieldName}, @q) AS score FROM c ORDER BY VectorDistance(c.${this.fieldName}, @q)`,
        parameters: [
          { name: '@k', value: opts.top },
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

  private containerFor(name: string): Container {
    return this.database.container(name);
  }
}

/**
 * Configuration for {@link cosmosVectorEmbeddingStore}. Mirrors
 * {@link CosmosVectorEmbeddingStoreConfig} but trades the pre-built `client`
 * for `endpoint` + `key` so the factory owns SDK construction.
 */
export interface CosmosVectorEmbeddingStoreFactoryConfig {
  endpoint: string;
  key: string;
  database: string;
  container: string;
  embeddingPath?: string;
  embeddingModel?: string;
  embeddingDimensions?: number;
  /**
   * Documentation-only assertion of the vector data type the host wrote into
   * the container's policy via {@link provisionVectorContainers}. The store
   * never enforces this — Cosmos' container policy does — but holding it on
   * the config gives a single source of truth for hosts that compose
   * provisioning + storage.
   */
  dataType?: VectorDataTypeOption;
}

/** Mirror of {@link provisionVectorContainers} `dataType` for type symmetry. */
export type VectorDataTypeOption = 'Float32' | 'Float16' | 'Int8';

/**
 * Construct a {@link CosmosVectorEmbeddingStore}. Recommended on-ramp: hosts
 * pass domain config and the factory builds the underlying `CosmosClient`
 * internally. For shared-client scenarios, use the class constructor.
 */
export function cosmosVectorEmbeddingStore(
  config: CosmosVectorEmbeddingStoreFactoryConfig,
): CosmosVectorEmbeddingStore {
  const client = new CosmosClient({ endpoint: config.endpoint, key: config.key });
  return new CosmosVectorEmbeddingStore({
    client,
    database: config.database,
    container: config.container,
    embeddingPath: config.embeddingPath,
    embeddingModel: config.embeddingModel,
    embeddingDimensions: config.embeddingDimensions,
  });
}

function is404(err: unknown): boolean {
  if (!err || typeof err !== 'object') return false;
  const code = (err as { code?: number | string }).code;
  return code === 404 || code === '404';
}
