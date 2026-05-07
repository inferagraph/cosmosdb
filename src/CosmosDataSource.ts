import { DataSource } from '@inferagraph/core/data';
import type {
  DataAdapterConfig, GraphData, NodeId, NodeData, EdgeData,
  ContentData, PaginationOptions, PaginatedResult, DataFilter,
  SearchVectorHit, Vector,
} from '@inferagraph/core/data';
import { CosmosClient, Container, Database } from '@azure/cosmos';
import type { SqlParameter } from '@azure/cosmos';
import type { CosmosDataSourceConfig } from './types.js';

export class CosmosDataSource extends DataSource {
  readonly name = 'cosmosdb';
  private client: CosmosClient | null = null;
  private database: Database | null = null;
  private container: Container | null = null;
  private edgesContainer: Container | null = null;
  private config: CosmosDataSourceConfig;

  constructor(config: CosmosDataSourceConfig) {
    super();
    this.config = config;
  }

  async connect(): Promise<void> {
    this.client = new CosmosClient({
      endpoint: this.config.endpoint,
      key: this.config.key,
    });
    this.database = this.client.database(this.config.database);
    this.container = this.database.container(this.config.container);
    if (this.config.edgesContainer) {
      this.edgesContainer = this.database.container(this.config.edgesContainer);
    }
  }

  async disconnect(): Promise<void> {
    this.client = null;
    this.database = null;
    this.container = null;
    this.edgesContainer = null;
  }

  isConnected(): boolean {
    return this.client !== null && this.container !== null;
  }

  async getInitialView(config?: DataAdapterConfig): Promise<GraphData> {
    this.ensureConnected();
    const limit = (config?.limit as number) ?? 100;

    const { resources: docs } = await this.container!.items
      .query(`SELECT TOP ${limit} * FROM c WHERE c._docType = 'node'`)
      .fetchAll();

    const nodes = docs.map(doc => this.transformDocument(doc));

    // Get edges
    const edgeCont = this.edgesContainer ?? this.container!;
    const nodeIds = nodes.map(n => n.id);

    let edges: EdgeData[] = [];
    if (nodeIds.length > 0) {
      const { resources: edgeDocs } = await edgeCont.items
        .query({
          query: `SELECT * FROM c WHERE c._docType = 'edge' AND c.sourceId IN (@ids) AND c.targetId IN (@ids)`,
          parameters: [{ name: '@ids', value: nodeIds }],
        })
        .fetchAll();
      edges = edgeDocs.map(doc => this.transformEdgeDocument(doc));
    }

    return { nodes, edges };
  }

  async getNode(id: NodeId): Promise<NodeData | undefined> {
    this.ensureConnected();

    const { resources } = await this.container!.items
      .query({
        query: 'SELECT * FROM c WHERE c.id = @id',
        parameters: [{ name: '@id', value: id }],
      })
      .fetchAll();

    if (resources.length === 0) return undefined;
    return this.transformDocument(resources[0]);
  }

  async getNeighbors(nodeId: NodeId, depth: number = 1): Promise<GraphData> {
    this.ensureConnected();

    // Cosmos DB NoSQL has no native graph traversal. depth>1 is implemented as
    // application-level BFS: iterate 1-hop fan-out from each newly discovered
    // frontier node up to `depth` levels. Dedupe edges and nodes by id.
    const effectiveDepth = Math.max(1, Math.floor(depth));

    const visitedNodeIds = new Set<NodeId>([nodeId]);
    const collectedEdgeDocs = new Map<string, Record<string, unknown>>();
    let frontier: NodeId[] = [nodeId];

    for (let level = 0; level < effectiveDepth && frontier.length > 0; level++) {
      const nextFrontier: NodeId[] = [];

      for (const currentId of frontier) {
        const edgeDocs = await this.fetchEdgesForNode(currentId);
        for (const edge of edgeDocs) {
          const edgeId = String(edge.id);
          if (!collectedEdgeDocs.has(edgeId)) {
            collectedEdgeDocs.set(edgeId, edge);
          }
          const sourceId = String(edge.sourceId);
          const targetId = String(edge.targetId);
          const otherId = sourceId === currentId ? targetId : sourceId;
          if (!visitedNodeIds.has(otherId)) {
            visitedNodeIds.add(otherId);
            nextFrontier.push(otherId);
          }
        }
      }

      frontier = nextFrontier;
    }

    // Fetch all visited node documents in a single query
    const allIds = [...visitedNodeIds];
    const { resources: nodeDocs } = await this.container!.items
      .query({
        query: `SELECT * FROM c WHERE c.id IN (@ids)`,
        parameters: [{ name: '@ids', value: allIds }],
      })
      .fetchAll();

    const nodes = nodeDocs.map(doc => this.transformDocument(doc));
    const edges = [...collectedEdgeDocs.values()].map(doc => this.transformEdgeDocument(doc));

    return { nodes, edges };
  }

  private async fetchEdgesForNode(nodeId: NodeId): Promise<Record<string, unknown>[]> {
    const edgeCont = this.edgesContainer ?? this.container!;
    const { resources } = await edgeCont.items
      .query({
        query: `SELECT * FROM c WHERE c._docType = 'edge' AND (c.sourceId = @nodeId OR c.targetId = @nodeId)`,
        parameters: [{ name: '@nodeId', value: nodeId }],
      })
      .fetchAll();
    return resources as Record<string, unknown>[];
  }

  async findPath(fromId: NodeId, toId: NodeId): Promise<GraphData> {
    this.ensureConnected();

    // CosmosDB NoSQL doesn't support graph traversal natively
    // Implement BFS at application level
    const edgeCont = this.edgesContainer ?? this.container!;

    const visited = new Set<string>([fromId]);
    const parent = new Map<string, { nodeId: string; edgeDoc: Record<string, unknown> }>();
    let frontier = [fromId];
    let found = false;

    while (frontier.length > 0 && !found) {
      const nextFrontier: string[] = [];

      for (const currentId of frontier) {
        const { resources: edgeDocs } = await edgeCont.items
          .query({
            query: `SELECT * FROM c WHERE c._docType = 'edge' AND (c.sourceId = @id OR c.targetId = @id)`,
            parameters: [{ name: '@id', value: currentId }],
          })
          .fetchAll();

        for (const edge of edgeDocs) {
          const neighborId = edge.sourceId === currentId ? edge.targetId : edge.sourceId;
          if (!visited.has(neighborId)) {
            visited.add(neighborId);
            parent.set(neighborId, { nodeId: currentId, edgeDoc: edge });
            nextFrontier.push(neighborId);
            if (neighborId === toId) {
              found = true;
              break;
            }
          }
        }
        if (found) break;
      }

      frontier = nextFrontier;
    }

    if (!found) return { nodes: [], edges: [] };

    // Reconstruct path
    const pathIds: string[] = [toId];
    const pathEdgeDocs: Record<string, unknown>[] = [];
    let current = toId;
    while (parent.has(current)) {
      const p = parent.get(current)!;
      pathIds.push(p.nodeId);
      pathEdgeDocs.push(p.edgeDoc);
      current = p.nodeId;
    }

    // Fetch all path nodes
    const { resources: nodeDocs } = await this.container!.items
      .query({
        query: `SELECT * FROM c WHERE c.id IN (@ids)`,
        parameters: [{ name: '@ids', value: pathIds }],
      })
      .fetchAll();

    return {
      nodes: nodeDocs.map(doc => this.transformDocument(doc)),
      edges: pathEdgeDocs.map(doc => this.transformEdgeDocument(doc)),
    };
  }

  async search(query: string, pagination?: PaginationOptions): Promise<PaginatedResult<NodeData>> {
    this.ensureConnected();

    const { resources } = await this.container!.items
      .query({
        query: `SELECT * FROM c WHERE c._docType = 'node' AND CONTAINS(LOWER(c.name), LOWER(@q))`,
        parameters: [{ name: '@q', value: query }],
      })
      .fetchAll();

    const allItems = resources.map(doc => this.transformDocument(doc));
    return this.paginate(allItems, pagination);
  }

  async filter(filter: DataFilter, pagination?: PaginationOptions): Promise<PaginatedResult<NodeData>> {
    this.ensureConnected();

    let query = `SELECT * FROM c WHERE c._docType = 'node'`;
    const parameters: SqlParameter[] = [];

    if (filter.types?.length) {
      query += ` AND c.type IN (@types)`;
      parameters.push({ name: '@types', value: filter.types });
    }
    if (filter.search) {
      query += ` AND CONTAINS(LOWER(c.name), LOWER(@search))`;
      parameters.push({ name: '@search', value: filter.search });
    }
    if (filter.attributes) {
      let i = 0;
      for (const [key, value] of Object.entries(filter.attributes)) {
        query += ` AND c.${key} = @attr${i}`;
        parameters.push({ name: `@attr${i}`, value: value as SqlParameter['value'] });
        i++;
      }
    }

    const { resources } = await this.container!.items
      .query({ query, parameters })
      .fetchAll();

    const allItems = resources.map(doc => this.transformDocument(doc));
    return this.paginate(allItems, pagination);
  }

  /**
   * Low-level vector-native top-K against the units container. Hosts that
   * want to bypass {@link CosmosVectorEmbeddingStore} and call straight into the
   * datasource can use this method — same SQL shape, same sort guarantee.
   *
   * Provider-agnostic: the embedding-field name is taken from
   * {@link CosmosDataSourceConfig.embeddingPath} (default `/embedding`),
   * so hosts using a different vector field can override it without forking
   * the datasource.
   */
  async searchVector(
    queryEmbedding: Vector,
    opts: { top: number; container?: 'units' | 'inferred_edges' },
  ): Promise<SearchVectorHit[]> {
    this.ensureConnected();
    const fieldName = this.embeddingFieldName();
    const container =
      opts.container === 'inferred_edges' && this.config.inferredEdgesContainer
        ? this.database!.container(this.config.inferredEdgesContainer)
        : this.container!;
    const { resources } = await container.items
      .query({
        query: `SELECT TOP @k c.id, VectorDistance(c.${fieldName}, @q) AS score FROM c ORDER BY VectorDistance(c.${fieldName}, @q)`,
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

  async getContent(nodeId: NodeId): Promise<ContentData | undefined> {
    this.ensureConnected();

    const { resources } = await this.container!.items
      .query({
        query: `SELECT c.id, c.content, c.contentType FROM c WHERE c.id = @id AND IS_DEFINED(c.content)`,
        parameters: [{ name: '@id', value: nodeId }],
      })
      .fetchAll();

    if (resources.length === 0) return undefined;

    const doc = resources[0];
    return {
      nodeId,
      content: String(doc.content),
      contentType: doc.contentType ?? 'text',
    };
  }

  // --- Private Helpers ---

  private embeddingFieldName(): string {
    const path = this.config.embeddingPath ?? '/embedding';
    return path.startsWith('/') ? path.slice(1) : path;
  }

  private ensureConnected(): void {
    if (!this.container) {
      throw new Error('CosmosDataSource is not connected. Call connect() first.');
    }
  }

  private transformDocument(doc: Record<string, unknown>): NodeData {
    const { id, _rid, _self, _etag, _attachments, _ts, _docType, ...attributes } = doc;
    return { id: String(id), attributes };
  }

  private transformEdgeDocument(doc: Record<string, unknown>): EdgeData {
    const { id, sourceId, targetId, _rid, _self, _etag, _attachments, _ts, _docType, type, ...rest } = doc;
    return {
      id: String(id),
      sourceId: String(sourceId),
      targetId: String(targetId),
      attributes: { type: String(type ?? ''), ...rest },
    };
  }

  private paginate(items: NodeData[], pagination?: PaginationOptions): PaginatedResult<NodeData> {
    const total = items.length;
    if (!pagination) return { items, total, hasMore: false };
    const { offset, limit } = pagination;
    const sliced = items.slice(offset, offset + limit);
    return { items: sliced, total, hasMore: offset + limit < total };
  }
}

/**
 * Construct a {@link CosmosDataSource}. Recommended on-ramp: hosts pass
 * domain config (endpoint, key, database, container) and the package owns
 * `@azure/cosmos` SDK construction internally — the SDK becomes an
 * implementation detail rather than a host dependency.
 *
 * For shared-client or custom-auth scenarios, use the {@link CosmosDataSource}
 * class constructor directly (the escape hatch).
 */
export function cosmosDataSource(config: CosmosDataSourceConfig): CosmosDataSource {
  return new CosmosDataSource(config);
}
