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

### Configuration

| Option | Required | Description |
|---|---|---|
| `endpoint` | Yes | Cosmos DB account endpoint URL |
| `key` | Yes | Account key for authentication |
| `database` | Yes | Database name |
| `container` | Yes | Container name for nodes (and edges if not separated) |
| `edgesContainer` | No | Separate container for edge documents |
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

## License

MIT
