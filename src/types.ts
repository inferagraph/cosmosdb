export interface CosmosDbDatasourceConfig {
  endpoint: string;
  key: string;
  database: string;
  container: string;
  edgesContainer?: string;
  partitionKeyPath?: string;
}
