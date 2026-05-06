# @inferagraph/cosmosdb-datasource

Azure Cosmos DB NoSQL datasource plugin for [@inferagraph/core](https://github.com/inferagraph/core).

## Installation

```bash
pnpm add @inferagraph/cosmosdb-datasource @inferagraph/core @azure/cosmos
```

## Usage

```typescript
import { CosmosDbDatasource } from '@inferagraph/cosmosdb-datasource';

const datasource = new CosmosDbDatasource({
  endpoint: 'https://your-account.documents.azure.com:443/',
  key: 'your-key',
  database: 'my-database',
  container: 'my-container',
});

await datasource.connect();

const view = await datasource.getInitialView();
console.log(view.nodes, view.edges);

await datasource.disconnect();
```

### Multi-hop neighbors

`getNeighbors(nodeId, depth)` supports `depth > 1`. Cosmos DB NoSQL has no native graph traversal, so the datasource does an application-level BFS — one 1-hop fan-out per level, deduping nodes and edges by id. Single-hop callers see no change.

### Configuration

| Option | Required | Description |
|---|---|---|
| `endpoint` | Yes | Cosmos DB account endpoint URL |
| `key` | Yes | Account key for authentication |
| `database` | Yes | Database name |
| `container` | Yes | Container name for nodes (and edges if not separated) |
| `edgesContainer` | No | Separate container for edge documents |
| `inferredEdgesContainer` | No | Separate container for inferred-edge embeddings (typically `inferred_edges`) |
| `embeddingPath` | No | JSON path of the embedding field on documents (default `/embedding`) |
| `partitionKeyPath` | No | Partition key path (default: `/type`) |

### Document Format

Nodes and edges are stored as JSON documents differentiated by a `_docType` field:

**Node document:**
```json
{
  "id": "node-1",
  "_docType": "node",
  "name": "Example Node",
  "type": "person"
}
```

**Edge document:**
```json
{
  "id": "edge-1",
  "_docType": "edge",
  "sourceId": "node-1",
  "targetId": "node-2",
  "type": "related_to"
}
```

## Vector + RAG setup

This package ships three building blocks that turn a Cosmos NoSQL account into the persistence layer for `@inferagraph/core`'s RAG pipeline:

1. `provisionVectorContainers` — one-time, idempotent setup of the units container's vector index policy plus the inferred-edges container.
2. `VectorEmbeddingStore` — implements `EmbeddingStore` from `@inferagraph/core@^0.8.0`. Backed by the units container with a vector index on `/embedding`.
3. `CosmosInferredEdgeStore` — implements `InferredEdgeStore` from `@inferagraph/core@^0.8.0`. Backed by a separate `inferred_edges` container with its own vector index.

All three are provider-agnostic. Embedding model, dimensions, distance function, and the JSON path of the embedding field are constructor options — the datasource never assumes a specific LLM provider.

### 1. Provision the containers

Call `provisionVectorContainers` once during setup (CI deploy, or a manual setup script):

```typescript
import { provisionVectorContainers } from '@inferagraph/cosmosdb-datasource';

await provisionVectorContainers({
  endpoint: process.env.COSMOS_ENDPOINT!,
  key: process.env.COSMOS_KEY!,
  database: 'biblegraph',
  unitsContainer: 'units',
  // Optional — sensible defaults shown:
  inferredEdgesContainer: 'inferred_edges', // default
  embeddingDimensions: 3072,                // default; matches text-embedding-3-large
  embeddingPath: '/embedding',              // default
  vectorIndexType: 'quantizedFlat',         // default; alternatives: 'diskANN', 'flat'
  distanceFunction: 'cosine',               // default; alternatives: 'dotproduct', 'euclidean'
});
```

The function is idempotent: it no-ops on the units container when it already carries the policy, and only creates `inferred_edges` when missing.

### 2. Wire the stores into the `GraphIndexer`

`@inferagraph/core@^0.8.0` exposes `GraphIndexer`, the engine that walks the in-memory graph, calls the LLM provider's `embed()`, and persists vectors via the `EmbeddingStore` you give it. Pass the Cosmos-backed implementations from this package:

```typescript
import { CosmosClient } from '@azure/cosmos';
import { GraphIndexer } from '@inferagraph/core';
import {
  VectorEmbeddingStore,
  CosmosInferredEdgeStore,
} from '@inferagraph/cosmosdb-datasource';

const client = new CosmosClient({
  endpoint: process.env.COSMOS_ENDPOINT!,
  key: process.env.COSMOS_KEY!,
});

const embeddingStore = new VectorEmbeddingStore({
  client,
  database: 'biblegraph',
  container: 'units',
});

const inferredEdgeStore = new CosmosInferredEdgeStore({
  client,
  database: 'biblegraph',
  // container defaults to 'inferred_edges'
});

const indexer = new GraphIndexer({
  store: graphStore,                      // GraphStore loaded from your DataAdapter
  provider: llmProvider,                  // any @inferagraph LLMProvider with embed()
  embeddingStore,
  inferredEdgeStore,
  contentKeys: ['content'],
  embeddingModel: 'text-embedding-3-large',
  embeddingDimensions: 3072,
});

await indexer.embedAll({ onProgress: (stage, done, total) => console.log(stage, done, total) });
await indexer.computeInferredEdges();
```

### 3. Wire the same stores into the `AIEngine` for retrieval

The same instances power chat-time retrieval. The engine calls `embeddingStore.searchVector(...)` (and `inferredEdgeStore.searchInferredEdges(...)` via the same vector index, container `'inferred_edges'`) instead of the in-memory linear scan:

```typescript
import { AIEngine } from '@inferagraph/core';

const engine = new AIEngine({
  store: graphStore,
  provider: llmProvider,
  embeddingStore,
  inferredEdgeStore,
  embeddingContentKeys: ['content'],
  chatRerankEnabled: true,
});

const stream = engine.chat('Tell me about Cain', { conversationId: 'session-1' });
for await (const event of stream) {
  // ...
}
```

### Low-level alternative: `CosmosDbDatasource.searchVector`

Hosts that want to bypass `VectorEmbeddingStore` can call straight into the datasource:

```typescript
const hits = await datasource.searchVector(queryEmbedding, { top: 8 });
// or against the inferred_edges container:
const inferredHits = await datasource.searchVector(queryEmbedding, {
  top: 8,
  container: 'inferred_edges',
});
```

The SQL shape and sort guarantee are identical to `VectorEmbeddingStore.searchVector`.

### Index strategy notes

- `quantizedFlat` (default) is fast and cheap up to roughly 10K vectors per container.
- Switch `vectorIndexType` to `'diskANN'` for larger corpora.
- `flat` is exact but slow; useful for diagnostics only.
- Distance function defaults to `cosine` — change with `distanceFunction: 'dotproduct'` or `'euclidean'` when your embedding model expects it.

## License

MIT
