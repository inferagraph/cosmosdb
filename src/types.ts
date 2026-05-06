export interface CosmosDataSourceConfig {
  endpoint: string;
  key: string;
  database: string;
  container: string;
  edgesContainer?: string;
  partitionKeyPath?: string;
  /**
   * Optional separate container for inferred-edge embeddings (default
   * `'inferred_edges'` is created by `provisionVectorContainers`). When set,
   * `searchVector` with `{container: 'inferred_edges'}` queries this
   * container; otherwise it falls back to the units container.
   */
  inferredEdgesContainer?: string;
  /**
   * JSON path of the embedding field on documents (default `'/embedding'`).
   * Provider-agnostic — set this if your schema names the field differently.
   */
  embeddingPath?: string;
}
