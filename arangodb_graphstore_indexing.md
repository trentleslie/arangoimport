# ArangoDB GraphStore Indexing Strategy for SPOKE Database

## Overview

This document outlines the comprehensive indexing strategy for the SPOKE knowledge graph in ArangoDB. Proper indexing is critical for query performance, especially for large-scale knowledge graphs like SPOKE with 43+ million nodes and 184+ million edges.

Indexing specific fields like UniProt IDs, gene symbols, and pathway identifiers significantly improves query performance for common biomedical entity lookups. This strategy focuses on optimizing the most frequent query patterns while minimizing index maintenance overhead.

## Base ArangoGraphStore Implementation

```python
import arango

class ArangoGraphStore:
    def __init__(self, url: str, username: str, password: str, request_timeout: int = 300):
        self.url = url
        self.username = username
        self.password = password
        self.request_timeout = request_timeout
        self._client = None

    @property
    def client(self):
        if self._client is None:
            self._client = arango.ArangoClient(self.url)
        return self._client
    
    def spoke(self):
        return self.get_db("spokev6")
    
    def get_db(self, db_name: str):
        return self.client.db(
            db_name, 
            username=self.username, 
            password=self.password,
            timeout=self.request_timeout
        )

    def aql(self, query: str, db_name: str = None, bind_vars: dict = None):
        if db_name is None:
            db = self.spoke()
        else:
            db = self.get_db(db_name)
        return db.aql.execute(query, bind_vars=bind_vars)
    
    def create_index(self, collection_name: str, index_type: str, fields: list, 
                     unique: bool = False, sparse: bool = False, name: str = None,
                     db_name: str = None):
        """Create an index on a collection
        
        Args:
            collection_name: Name of the collection
            index_type: Type of index (persistent, hash, skiplist, etc)
            fields: List of fields to index
            unique: Whether the index enforces uniqueness
            sparse: Whether the index should be sparse (exclude null values)
            name: Optional name for the index
            db_name: Optional database name, defaults to spoke()
        
        Returns:
            dict: The index creation result
        """
        if db_name is None:
            db = self.spoke()
        else:
            db = self.get_db(db_name)
            
        collection = db.collection(collection_name)
        
        index_params = {
            "type": index_type,
            "fields": fields,
            "unique": unique,
            "sparse": sparse
        }
        
        if name:
            index_params["name"] = name
            
        return collection.add_index(**index_params)
```

## Basic Index Implementation

```python
from plrag.datastores.arango import ArangoGraphStore
import os
from dotenv import load_dotenv

load_dotenv("../.env")

# Initialize graph store with extended timeout for large operations
graph_store = ArangoGraphStore(
    url=os.getenv('SPOKE_DATABASE_URL'), 
    username=os.getenv("SPOKE_DATABASE_USERNAME"), 
    password=os.getenv("SPOKE_DATABASE_PASSWORD"),
    request_timeout=300
)

# Create a persistent index on properties.identifier and labels
graph_store.create_index(
    collection_name="Nodes",
    index_type="persistent",
    fields=["properties.identifier", "labels"],
    unique=False,
    name="idx_nodes_identifier_labels"
)
```

## Comprehensive Indexing Strategy

Below is a complete implementation of all recommended indexes for the SPOKE database:

```python
db = graph_store.spoke()

# 1. Primary Node Indexes - Essential for all queries
graph_store.create_index(
    collection_name="Nodes",
    index_type="persistent",
    fields=["properties.identifier"],
    unique=False,
    name="idx_nodes_identifier"
)

graph_store.create_index(
    collection_name="Nodes",
    index_type="persistent",
    fields=["labels"],
    unique=False,
    name="idx_nodes_labels"
)

# 2. Node Type-Specific Indexes

# Protein indexes
graph_store.create_index(
    collection_name="Nodes",
    index_type="persistent",
    fields=["properties.name"],
    unique=False,
    name="idx_nodes_name"
)

graph_store.create_index(
    collection_name="Nodes",
    index_type="persistent",
    fields=["properties.uniprot"],
    unique=False,
    name="idx_nodes_uniprot"
)

# Gene indexes
graph_store.create_index(
    collection_name="Nodes",
    index_type="persistent",
    fields=["properties.gene"],
    unique=False,
    name="idx_nodes_gene"
)

graph_store.create_index(
    collection_name="Nodes",
    index_type="persistent",
    fields=["properties.entrez"],
    unique=False,
    name="idx_nodes_entrez"
)

# Pathway indexes
graph_store.create_index(
    collection_name="Nodes",
    index_type="persistent",
    fields=["properties.reactome"],
    unique=False,
    name="idx_nodes_reactome"
)

# Compound indexes
graph_store.create_index(
    collection_name="Nodes",
    index_type="persistent",
    fields=["properties.pubchem"],
    unique=False,
    name="idx_nodes_pubchem"
)

# Disease indexes
graph_store.create_index(
    collection_name="Nodes",
    index_type="persistent",
    fields=["properties.mesh"],
    unique=False,
    name="idx_nodes_mesh"
)

# 3. Edge Indexes
graph_store.create_index(
    collection_name="Edges",
    index_type="persistent",
    fields=["label"],
    unique=False,
    name="idx_edges_label"
)

# 4. Fulltext index for text searches (if needed)
db.collection("Nodes").add_index(
    type="fulltext",
    fields=["properties.name"],
    minLength=3,
    name="idx_fulltext_name"
)
```

## Indexing Strategy by Node Type

### Protein Nodes
- **Primary identifiers**: UniProt IDs (`properties.uniprot`)
- **Names**: Protein names (`properties.name`)
- **Related fields**: Gene symbols, sequence features

### Gene Nodes
- **Primary identifiers**: Gene symbols (`properties.gene`), Entrez IDs (`properties.entrez`)
- **Related fields**: Chromosomal location, OMIM IDs

### Pathway Nodes
- **Primary identifiers**: Reactome IDs (`properties.reactome`), KEGG IDs
- **Name fields**: Pathway names for text searches

### Compound Nodes
- **Primary identifiers**: PubChem IDs (`properties.pubchem`), ChEMBL IDs, DrugBank IDs
- **Name fields**: Drug/compound names

### Disease Nodes
- **Primary identifiers**: MeSH IDs (`properties.mesh`), OMIM IDs, DOID
- **Name fields**: Disease names and synonyms

## Performance Considerations

### Index Selection Strategy

1. **Prioritize high-frequency queries**: Index fields that appear in WHERE clauses of your most common queries
2. **Consider cardinality**: Fields with high cardinality (many distinct values) benefit most from indexing
3. **Balance maintenance costs**: Each index increases write time and storage requirements
4. **Monitor index usage**: Use ArangoDB's query profiling to identify which indexes are being used

### Indexing Best Practices

1. **Name your indexes** for easier management and monitoring
2. **Index compound fields** that are frequently queried together
3. **Use sparse indexes** for fields that are often null/undefined
4. **Prefer persistent indexes** for most use cases (best balance of performance vs. overhead)
5. **Limit fulltext indexes** to only essential text search fields
6. **Consider edge index** for frequently traversed relationship types

### Edge Collection Optimization

For the Edges collection with 184+ million edges, these optimizations are crucial:

1. **Index edge labels**: Most graph traversals filter by edge type
2. **Avoid over-indexing**: The `_from` and `_to` fields are automatically indexed
3. **Consider sparse indexes** for edge properties that exist only on specific edge types
4. **Monitor memory usage**: Edge indexes can consume significant memory with high edge counts

## Example Queries That Benefit From Indexes

### 1. Find a protein by UniProt ID (uses `idx_nodes_uniprot`)

```aql
FOR n IN Nodes
  FILTER n.properties.uniprot == "P05019"  // IGF1_HUMAN
  RETURN n
```

### 2. Find all genes with a specific symbol (uses `idx_nodes_gene`)

```aql
FOR n IN Nodes
  FILTER n.labels[0] == "Gene" AND n.properties.gene == "IGF1"
  RETURN n
```

### 3. Find connections between a protein and pathways (uses multiple indexes)

```aql
LET protein = (FOR n IN Nodes FILTER n.properties.uniprot == "P05019" RETURN n)[0]
FOR v, e, p IN 1..2 OUTBOUND protein Edges
  FILTER e.label == "PARTICIPATES_IN" 
  FILTER v.labels[0] == "Pathway"
  RETURN { pathway: v, connection: e }
```

### 4. Find diseases associated with a gene (uses label and gene indexes)

```aql
LET gene = (FOR n IN Nodes FILTER n.labels[0] == "Gene" AND n.properties.gene == "IGF1" RETURN n)[0]
FOR v, e, p IN 1..2 ANY gene Edges
  FILTER v.labels[0] == "Disease"
  RETURN { disease: v, connection: e }
```

### 5. Text search for proteins (uses fulltext index)

```aql
FOR n IN Nodes
  FILTER n.labels[0] == "Protein" AND PHRASE(n.properties.name, "insulin", "text_en")
  RETURN n
```

## Monitoring Index Performance

### Check index usage with query explain

```python
def explain_query(query, bind_vars=None):
    db = graph_store.spoke()
    explanation = db.aql.explain(query, bind_vars=bind_vars, opt_rules=['+all'])
    
    # Check if any indexes are being used
    indexes_used = []
    for plan in explanation['plans']:
        for node in plan.get('nodes', []):
            if node.get('indexes'):
                indexes_used.extend(node['indexes'])
    
    return {
        'indexes_used': indexes_used,
        'estimated_cost': explanation.get('estimatedCost', 'Unknown'),
        'execution_time': explanation.get('estimatedNrItems', 'Unknown'),
        'full_explanation': explanation
    }

# Example usage
query = "FOR n IN Nodes FILTER n.properties.uniprot == 'P05019' RETURN n"
result = explain_query(query)
print(f"Indexes used: {result['indexes_used']}")
print(f"Estimated cost: {result['estimated_cost']}")
```

### Analyze index sizes

```python
def get_index_stats(collection_name):
    db = graph_store.spoke()
    collection = db.collection(collection_name)
    indexes = collection.indexes()
    
    stats = []
    for idx in indexes:
        stats.append({
            'name': idx.get('name', 'unnamed'),
            'type': idx.get('type'),
            'fields': idx.get('fields', []),
            'size': idx.get('size', 'unknown'),
            'unique': idx.get('unique', False),
            'sparse': idx.get('sparse', False)
        })
    
    return stats

# Example usage
node_indexes = get_index_stats("Nodes")
for idx in node_indexes:
    print(f"Index {idx['name']}: {idx['type']} on {', '.join(idx['fields'])}, Size: {idx['size']} bytes")
```

## Index Maintenance

### Periodic review

Regularly review index usage and performance to identify opportunities for optimization:

1. Remove unused indexes
2. Add indexes for new query patterns
3. Monitor database size growth
4. Check for index fragmentation

### Implementation Considerations

For the SPOKE database with 43+ million nodes and 184+ million edges:

1. **Create indexes before bulk imports** when possible
2. **Add indexes incrementally** if adding to an existing database
3. **Monitor system resources** during index creation (CPU, memory, disk)
4. **Consider background indexing** for production systems

## Practical Implementation for the Current Import

After your current import of 184+ million edges completes, you should add these critical indexes in this order:

1. First add the basic label index to speed up common queries:
   ```python
   graph_store.create_index(
       collection_name="Nodes",
       index_type="persistent",
       fields=["labels"],
       unique=False,
       name="idx_nodes_labels"
   )
   ```

2. Add the edge label index (crucial for traversals):
   ```python
   graph_store.create_index(
       collection_name="Edges",
       index_type="persistent",
       fields=["label"],
       unique=False,
       name="idx_edges_label"
   )
   ```

3. Then add protein identifiers index (used in many queries):
   ```python
   graph_store.create_index(
       collection_name="Nodes",
       index_type="persistent",
       fields=["properties.uniprot"],
       unique=False,
       name="idx_nodes_uniprot"
   )
   ```

## Working with ArangoDB's Web Interface

The ArangoDB web interface provides a convenient way to manage indexes:

1. Navigate to http://localhost:8529 (or your ArangoDB server address)
2. Log in with your credentials (root/ph)
3. Select the `spokeV6` database
4. Go to "Collections" and select the "Nodes" or "Edges" collection
5. Click on the "Indexes" tab to view, create, or delete indexes

## Conclusion

Properly indexing the SPOKE database significantly improves query performance for common biomedical entity lookups. The indexing strategy outlined in this document focuses on the most frequently accessed fields while balancing performance gains against maintenance costs.

As query patterns evolve, periodically review and update the indexing strategy to ensure optimal performance.
