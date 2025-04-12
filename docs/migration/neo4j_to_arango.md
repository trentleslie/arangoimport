# Migrating from Neo4j to ArangoDB

This guide describes the process of migrating data from a Neo4j graph database to ArangoDB using the ArangoImport tool.

## Migration Overview

### Key Differences Between Neo4j and ArangoDB

| Feature | Neo4j | ArangoDB |
|---------|-------|----------|
| Data Model | Property Graph | Multi-model (Documents, Graphs, Key/Value) |
| Node/Edge Identification | Internal IDs | Document keys (_key attribute) |
| Relationship Structure | Direct connections | Edge documents with _from and _to fields |
| Query Language | Cypher | AQL (ArangoDB Query Language) |
| Schema | Optional schema/constraints | Schema-free |

### Migration Process

1. **Export from Neo4j**: Export data from Neo4j to a JSON or JSONL format
2. **Transform Data**: Prepare data for ArangoDB format
3. **Import to ArangoDB**: Import the transformed data using ArangoImport
4. **Verify Data**: Ensure data integrity in ArangoDB

## Step 1: Export from Neo4j

### Using Neo4j's APOC Export

The preferred method is to use Neo4j's APOC library to export data:

```cypher
CALL apoc.export.json.all("neo4j_export.json", {useTypes: true, writeNodeProperties: true})
```

Alternatively, you can use custom Cypher queries to export specific parts of your graph:

```cypher
MATCH (n)
WITH collect(n) as nodes
CALL apoc.export.json.data(nodes, [], "nodes.json", {useTypes: true, writeNodeProperties: true})
YIELD file, source, format, nodes, relationships, properties, time, rows, batchSize, batches, done, data
RETURN file, source, format, nodes, relationships, properties, time, rows, batchSize, batches, done, data
```

```cypher
MATCH ()-[r]->()
WITH collect(r) as rels
CALL apoc.export.json.data([], rels, "relationships.json", {useTypes: true, writeNodeProperties: true})
YIELD file, source, format, nodes, relationships, properties, time, rows, batchSize, batches, done, data
RETURN file, source, format, nodes, relationships, properties, time, rows, batchSize, batches, done, data
```

### Convert to JSONL Format

Convert the exported JSON to JSONL format, where each line contains a valid JSON object. The expected format is:

```jsonl
{"type":"node","id":"123","labels":["Person"],"properties":{"name":"Alice","age":30}}
{"type":"node","id":"456","labels":["Person"],"properties":{"name":"Bob","age":28}}
{"type":"relationship","id":"789","start":{"id":"123"},"end":{"id":"456"},"label":"KNOWS","properties":{"since":2010}}
```

## Step 2: Import to ArangoDB

Use the ArangoImport tool to import the data:

```bash
python -m arangoimport.cli import-data /path/to/data.jsonl --username root --password mypassword --db-name my_database --host localhost --port 8529 --processes 8
```

### Import Process Details

The import process follows these steps:

1. **Node Import Phase**: All nodes are imported first, with unique keys based on their original IDs
2. **Edge Import Phase**: After all nodes are imported, edges are created between them
3. **Key Generation**: For edges, keys are generated based on source, target, and relationship type
4. **Deduplication**: The import process automatically detects and handles duplicate edges

## Step 3: Verify Data

After importing, verify your data integrity:

```bash
python -m arangoimport.cli query-db --host localhost --port 8529 --username root --password mypassword --db-name my_database --query "RETURN { nodes: LENGTH(Nodes), edges: LENGTH(Edges) }"
```

### Common Verification Queries

Check for nodes with a specific label:

```aql
FOR doc IN Nodes
  FILTER "Person" IN doc.labels
  RETURN doc
```

Check for specific relationships:

```aql
FOR edge IN Edges
  FILTER edge.label == "KNOWS"
  RETURN edge
```

## Handling Special Cases

### Large Datasets

For large datasets:
- Increase the number of worker processes (`--processes`)
- Use a machine with sufficient memory and CPU cores
- Consider splitting the export into smaller chunks

### Node References in Edges

Ensure that edge references use the correct format:
- `_from`: "Nodes/source_id"
- `_to`: "Nodes/target_id"

### Property Types

ArangoDB and Neo4j have different type systems. Pay attention to:
- Date/time values
- Spatial types
- Arrays and nested objects

## Case Study: SPOKE Migration

See the [SPOKE Migration Case Study](/docs/case_studies/spoke/migration_analysis.md) for a detailed example of migrating a large biomedical knowledge graph from Neo4j to ArangoDB.
