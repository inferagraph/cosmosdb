import {
  CosmosClient,
  VectorEmbeddingDataType,
  VectorEmbeddingDistanceFunction,
  VectorIndexType,
} from '@azure/cosmos';
import type { ContainerDefinition, IndexingPolicy, VectorEmbeddingPolicy, VectorIndex } from '@azure/cosmos';

/** Vector index strategy. Cosmos NoSQL exposes three engines today. */
export type VectorIndexTypeOption = 'quantizedFlat' | 'diskANN' | 'flat';

/** Distance function used by the vector index. Cosmos supports three. */
export type DistanceFunctionOption = 'cosine' | 'dotproduct' | 'euclidean';

/**
 * Vector embedding scalar type. Float32 is the default and matches OpenAI /
 * Voyage / most general-purpose embeddings; Float16 + Int8 are bandwidth /
 * storage optimizations Cosmos supports natively when the host wrote the
 * embeddings already quantized.
 */
export type VectorDataTypeOption = 'Float32' | 'Float16' | 'Int8';

/**
 * Configuration for {@link provisionVectorContainers}. Provider-agnostic
 * defaults: 3072-dim cosine vectors at `/embedding` indexed by
 * `quantizedFlat`. Hosts targeting other embedding providers override the
 * dimensions, function, and index type.
 */
export interface ProvisionVectorContainersConfig {
  endpoint: string;
  key: string;
  database: string;
  /** The units container that holds source documents (it gets a vector index added in place). */
  unitsContainer: string;
  /** Defaults to `'inferred_edges'`. */
  inferredEdgesContainer?: string;
  /** Defaults to `3072` (matches `text-embedding-3-large`; configurable for other models). */
  embeddingDimensions?: number;
  /** Defaults to `'/embedding'`. */
  embeddingPath?: string;
  /** Defaults to `'quantizedFlat'`. */
  vectorIndexType?: VectorIndexTypeOption;
  /** Defaults to `'cosine'`. */
  distanceFunction?: DistanceFunctionOption;
  /** Defaults to `'Float32'`. Float16 / Int8 trade precision for storage + index size. */
  dataType?: VectorDataTypeOption;
}

/**
 * One-time, idempotent setup for the Cosmos vector containers.
 *
 * - Reads the units container's current definition. If it already carries a
 *   vector embedding policy on the configured path, no-ops; otherwise issues
 *   a `replace()` to add the policy + index entry.
 * - Creates the inferred-edges container with the same vector policy if it
 *   does not exist; if it exists with a matching policy, no-ops; if it
 *   exists with a stale policy, leaves it (Cosmos cannot alter an existing
 *   container's vector policy via this path).
 *
 * Hosts call this once during deploy or as part of a setup script before
 * wiring {@link CosmosVectorEmbeddingStore} or {@link CosmosInferredEdgeStore}.
 */
export async function provisionVectorContainers(
  config: ProvisionVectorContainersConfig,
): Promise<void> {
  const dimensions = config.embeddingDimensions ?? 3072;
  const path = config.embeddingPath ?? '/embedding';
  const indexType: VectorIndexTypeOption = config.vectorIndexType ?? 'quantizedFlat';
  const distance: DistanceFunctionOption = config.distanceFunction ?? 'cosine';
  const dataType: VectorDataTypeOption = config.dataType ?? 'Float32';
  const inferredEdgesName = config.inferredEdgesContainer ?? 'inferred_edges';

  const client = new CosmosClient({ endpoint: config.endpoint, key: config.key });
  const db = client.database(config.database);

  // -------- units container: alter in place if missing the vector policy --------
  const unitsContainer = db.container(config.unitsContainer);
  const unitsDef = await unitsContainer.read();
  const unitsResource = (unitsDef as { resource?: ContainerDefinition }).resource;
  if (!hasVectorPolicyOn(unitsResource, path)) {
    const desired = mergeVectorPolicy(unitsResource, config.unitsContainer, {
      path,
      dimensions,
      distance,
      indexType,
      dataType,
    });
    try {
      await unitsContainer.replace(desired);
    } catch (err) {
      if (isAlterRejection(err)) {
        throw new Error(
          `Container '${config.unitsContainer}' exists but cannot be altered to add the vector policy. ` +
            `Drop the container manually (DATA LOSS) and re-run provisionVectorContainers, or recreate it ` +
            `with the desired policy from the start.`,
        );
      }
      throw err;
    }
  }

  // -------- inferred_edges container: create if missing --------
  const edgesContainer = db.container(inferredEdgesName);
  let edgesResource: ContainerDefinition | undefined;
  try {
    const def = await edgesContainer.read();
    edgesResource = (def as { resource?: ContainerDefinition }).resource;
  } catch {
    edgesResource = undefined;
  }
  if (!edgesResource || !hasVectorPolicyOn(edgesResource, path)) {
    await db.containers.createIfNotExists(
      buildEdgesDefinition(inferredEdgesName, {
        path,
        dimensions,
        distance,
        indexType,
        dataType,
      }),
    );
  }
}

interface VectorOpts {
  path: string;
  dimensions: number;
  distance: DistanceFunctionOption;
  indexType: VectorIndexTypeOption;
  dataType: VectorDataTypeOption;
}

function hasVectorPolicyOn(
  def: ContainerDefinition | undefined,
  path: string,
): boolean {
  if (!def) return false;
  const policy = (def as ContainerDefinition & { vectorEmbeddingPolicy?: VectorEmbeddingPolicy })
    .vectorEmbeddingPolicy;
  if (!policy?.vectorEmbeddings?.length) return false;
  return policy.vectorEmbeddings.some(v => v.path === path);
}

function mergeVectorPolicy(
  existing: ContainerDefinition | undefined,
  id: string,
  opts: VectorOpts,
): ContainerDefinition {
  const baseIndexing: IndexingPolicy =
    (existing as { indexingPolicy?: IndexingPolicy } | undefined)?.indexingPolicy ?? {
      indexingMode: 'consistent',
      automatic: true,
    };
  const vectorIndexes: VectorIndex[] = baseIndexing.vectorIndexes
    ? baseIndexing.vectorIndexes.filter(v => v.path !== opts.path)
    : [];
  vectorIndexes.push({ path: opts.path, type: toVectorIndexType(opts.indexType) });
  const vectorPolicy: VectorEmbeddingPolicy = {
    vectorEmbeddings: [
      {
        path: opts.path,
        dimensions: opts.dimensions,
        dataType: toDataType(opts.dataType),
        distanceFunction: toDistanceFunction(opts.distance),
      },
    ],
  };
  return {
    ...(existing ?? { id }),
    id: existing?.id ?? id,
    indexingPolicy: { ...baseIndexing, vectorIndexes },
    vectorEmbeddingPolicy: vectorPolicy,
  } as ContainerDefinition;
}

function buildEdgesDefinition(id: string, opts: VectorOpts): ContainerDefinition {
  return {
    id,
    partitionKey: { paths: ['/sourceId'] },
    indexingPolicy: {
      indexingMode: 'consistent',
      automatic: true,
      vectorIndexes: [{ path: opts.path, type: toVectorIndexType(opts.indexType) }],
    },
    vectorEmbeddingPolicy: {
      vectorEmbeddings: [
        {
          path: opts.path,
          dimensions: opts.dimensions,
          dataType: toDataType(opts.dataType),
          distanceFunction: toDistanceFunction(opts.distance),
        },
      ],
    },
  } as ContainerDefinition;
}

function toVectorIndexType(value: VectorIndexTypeOption): VectorIndexType {
  switch (value) {
    case 'flat':
      return VectorIndexType.Flat;
    case 'diskANN':
      return VectorIndexType.DiskANN;
    case 'quantizedFlat':
    default:
      return VectorIndexType.QuantizedFlat;
  }
}

function toDistanceFunction(value: DistanceFunctionOption): VectorEmbeddingDistanceFunction {
  switch (value) {
    case 'euclidean':
      return VectorEmbeddingDistanceFunction.Euclidean;
    case 'dotproduct':
      return VectorEmbeddingDistanceFunction.DotProduct;
    case 'cosine':
    default:
      return VectorEmbeddingDistanceFunction.Cosine;
  }
}

function toDataType(value: VectorDataTypeOption): VectorEmbeddingDataType {
  switch (value) {
    case 'Float16':
      return VectorEmbeddingDataType.Float16;
    case 'Int8':
      return VectorEmbeddingDataType.Int8;
    case 'Float32':
    default:
      return VectorEmbeddingDataType.Float32;
  }
}

/**
 * Detect the Cosmos error pattern that means "this container exists but its
 * indexing or embedding policy cannot be altered in place". The SDK surfaces
 * these as `code: 400` with a body whose message mentions the disallowed
 * operation; some legacy / regional configurations also return `412`. We
 * match liberally on either status + a phrase that appears in the message
 * so the wrapper still fires when the SDK shape drifts.
 */
function isAlterRejection(err: unknown): boolean {
  if (!err || typeof err !== 'object') return false;
  const e = err as { code?: number | string; message?: string; body?: unknown };
  const code = typeof e.code === 'string' ? Number(e.code) : e.code;
  if (code !== 400 && code !== 412) return false;
  const message = typeof e.message === 'string' ? e.message : '';
  const bodyMessage =
    e.body && typeof e.body === 'object' && 'message' in (e.body as Record<string, unknown>)
      ? String((e.body as { message: unknown }).message ?? '')
      : '';
  const haystack = `${message} ${bodyMessage}`.toLowerCase();
  return (
    haystack.includes('not allowed') ||
    haystack.includes('cannot be modified') ||
    haystack.includes('cannot be altered') ||
    haystack.includes("operation 'replace'")
  );
}
