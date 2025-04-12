# SPOKE Database Analysis: Neo4j vs ArangoDB

## Executive Summary

This document presents an analysis of the SPOKE biomedical knowledge graph stored in two different database systems: Neo4j and ArangoDB. We discovered significant discrepancies in how protein connections are represented between these systems, particularly for the human insulin-like growth factor (IGF1) protein. While the protein nodes themselves contain identical information, the Neo4j instance reveals over 1,300 connections for IGF1, whereas the ArangoDB instance only shows 2 connections.

## Database Overview

**SPOKE (Scalable Precision Medicine Open Knowledge Engine)** is a large biomedical knowledge graph that integrates data from numerous sources to represent relationships between genes, proteins, diseases, drugs, and other biomedical entities.

| Database Details | Value |
| --------------- | ----- |
| Database Name   | spokev6 |
| Total Nodes     | 43,623,150 |
| Total Edges     | 184,206,157 |
| Edge to Node Ratio | 4.22 |

## Case Study: IGF1 Protein (P05019)

To understand differences between the database implementations, we conducted a detailed investigation of a specific protein: Insulin-like growth factor I (IGF1), with the UniProt identifier P05019.

### Protein Details (Identical in Both Systems)

```
Identifier: P05019
Name: IGF1_HUMAN
Description: Insulin-like growth factor I (IGF-I) (Mechano growth factor) (MGF) (Somatomedin-C)
Gene: IGF1
Organism: Homo sapiens (Human)
```

### Connection Comparison

| Database | Total Direct Connections | Connection Types |
| -------- | ----------------------- | ---------------- |
| Neo4j    | 1,325                   | 10 different relationship types |
| ArangoDB | 2                       | 2 different relationship types |

#### Neo4j Connections for IGF1
| Relationship Type    | Connected Node Type | Count |
| -------------------- | ------------------ | ----- |
| INTERACTS_PiP        | Protein            | 829   |
| INTERACTS_PiC        | Compound           | 391   |
| EXPRESSEDIN_PeCT     | CellType           | 69    |
| MEASURES_CLmP        | ClinicalLab        | 28    |
| PARTOF_PDpP          | ProteinDomain      | 2     |
| BINDS_CbP            | Compound           | 2     |
| ENCODES_GeP          | Gene               | 1     |
| INCREASEDIN_PiD      | Disease            | 1     |
| INTERACTS_as_LR      | Protein            | 1     |
| ENCODES_OeP          | Organism           | 1     |

#### ArangoDB Connections for IGF1
| Relationship Type    | Connected Node Type | Connected Node Id | Connected Node Name |
| -------------------- | ------------------ | ---------------- | ------------------ |
| ENCODES_OeP          | Protein            | A0A7W7EE23       | A0A7W7EE23_RHIET (bacterial protein) |
| PARTOF_PDpP          | Protein            | A0A7G7P8W9       | A0A7G7P8W9_STEMA (bacterial protein) |

## Relationship Types in ArangoDB

Interestingly, all the relationship types that connect to IGF1 in Neo4j also exist in ArangoDB, with substantial numbers of connections:

| Relationship Type | Total Count in ArangoDB |
| ----------------- | --------------------- |
| PARTOF_PDpP       | 50,838,715            |
| BINDS_CbP         | 840,080               |
| MEASURES_CLmP     | 4,839                 |
| ENCODES_GeP       | 157,319               |
| INTERACTS_PiP     | 2,298,272             |
| INTERACTS_PiC     | 69,200,000            |
| EXPRESSEDIN_PeCT  | 908,296               |
| INCREASEDIN_PiD   | 11,850                |
| INTERACTS_as_LR   | 939                   |
| ENCODES_OeP       | 38,921,901            |

## Analysis of Discrepancies

Our investigation revealed several possible explanations for the dramatic difference in connections between the two database systems:

1. **Incomplete Data Migration**: The most likely explanation is that while the node data was completely migrated from Neo4j to ArangoDB, the connections (edges) for human proteins like IGF1 were not fully transferred.

2. **Different Connection Patterns**: The graph structure might be fundamentally different between the two systems. In Neo4j, connections might be directly established between nodes, while in ArangoDB they might require intermediate nodes or different traversal patterns.

3. **Data Filtering**: There may be filtering mechanisms in the ArangoDB implementation that exclude certain types of connections, either intentionally or unintentionally.

4. **Selective Replication**: The ArangoDB instance may have been populated with a subset of the data focused on bacterial proteins rather than human proteins and their interactions.

## Evidence and Methodology

We conducted our analysis using both Python queries against ArangoDB and Cypher queries against Neo4j:

### ArangoDB Queries
- Searched for protein P05019 by identifier and by examining the properties field
- Traversed all edges (regardless of type) connecting to the protein node
- Verified the existence of all edge types in the database
- Confirmed the node structure was identical to Neo4j

### Neo4j Queries
```cypher
// Finding the protein
MATCH (protein:Protein)
WHERE protein.identifier = "P05019"
RETURN protein
LIMIT 10

// Finding all connections
MATCH (p:Protein {identifier: "P05019"})-[r]-(connected)
RETURN type(r), labels(connected), count(*) as count
```

## Implications

This discovery has several important implications for researchers working with the SPOKE database:

1. **Database Selection**: For comprehensive analyses of human protein interactions, the Neo4j instance currently appears to be more complete.

2. **Validation Requirements**: Results from either database system should be cross-validated against the other system or external sources.

3. **Data Synchronization**: A process to synchronize the connections between the two systems would be valuable for consistent analyses.

4. **Query Strategies**: Different query strategies may be needed for the two database systems, even for seemingly identical questions.

## Recommendations

Based on our findings, we recommend:

1. **Use Neo4j for Human Protein Analyses**: Given the more complete connection data, Neo4j appears to be the preferred system for studying human protein interactions.

2. **Investigate Synchronization Issues**: Determine why the ArangoDB instance doesn't contain the full set of connections and address any data migration issues.

3. **Create Data Validation Routines**: Develop scripts that can verify data consistency between the two systems.

4. **Document Database Differences**: Maintain clear documentation about the differences between the two database instances to guide researchers.

5. **Consider Database Purpose**: Evaluate whether the two database instances are intended to serve different purposes, which might explain the different connection patterns.

## Conclusion

The SPOKE biomedical knowledge graph offers tremendous value for researchers, but users should be aware of significant differences between the Neo4j and ArangoDB implementations. The case study of IGF1 protein connections demonstrates that these differences can dramatically impact analysis results and should be carefully considered when designing research queries and interpreting results.

## Technical Appendix

### ArangoDB Connection Code
```python
def get_all_connections(db, protein_id: str):
    """Get ALL connections for a specific protein without filtering by edge collection."""
    # First get the protein info to get its internal ID
    protein_info = get_protein_info(db, protein_id)
    if not protein_info:
        return None
    
    # Get ALL connections using the ANY operator over ALL edges
    aql = """
    LET protein_id = @node_id
    
    // Count all connections by edge type and connected node type
    LET connection_summary = (
      FOR v, e IN 1..1 ANY @node_id Edges
        COLLECT edge_type = e.label, node_type = v.labels[0] WITH COUNT INTO count
        RETURN {
          edge_type: edge_type, 
          node_type: node_type, 
          count: count
        }
    )
    
    // Get some sample connections for each type
    LET samples = (
      FOR edge_group IN connection_summary
        LET edge_type = edge_group.edge_type
        LET node_type = edge_group.node_type
        
        LET sample_connections = (
          FOR v, e IN 1..1 ANY @node_id Edges
            FILTER e.label == edge_type
            FILTER v.labels[0] == node_type
            LIMIT 5
            RETURN {
              node: {
                id: v.properties.identifier,
                name: v.properties.name || v.properties.identifier,
                description: v.properties.description,
                key: v._key
              },
              edge: {
                label: e.label,
                direction: e._from == @node_id ? "outbound" : "inbound",
                key: e._key
              }
            }
        )
        
        RETURN {
          edge_type: edge_type,
          node_type: node_type,
          count: edge_group.count,
          samples: sample_connections
        }
    )
    
    RETURN {
      protein: protein_id,
      total_connections: SUM(connection_summary[*].count),
      connection_types: connection_summary,
      connection_samples: samples
    }
    """
    
    cursor = db.aql.execute(aql, bind_vars={"node_id": protein_info["_id"]})
    return list(cursor)
```
