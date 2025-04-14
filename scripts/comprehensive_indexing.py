#!/usr/bin/env python3
"""
Comprehensive indexing script for SPOKE database in ArangoDB.

This script implements a complete indexing strategy based on the SPOKE knowledge graph
query patterns and metapaths. It adds indexes that significantly improve performance for
common biomedical queries including:
- Compound-protein binding
- Pathway analysis
- Gene expression
- Disease associations
- Metabolic reactions
- Pharmacological class relationships

The script includes robust retry logic and timeout handling for working with large databases.
"""

import time
import sys
import argparse
from arango import ArangoClient
from arango.exceptions import ArangoServerError, ArangoClientError


def add_index_with_retry(collection, index_data, max_retries=5, retry_delay=5):
    """Add an index with retry logic for timeout handling.
    
    Args:
        collection: ArangoDB collection object
        index_data: Index definition dictionary
        max_retries: Maximum number of retry attempts
        retry_delay: Delay between retries in seconds
    
    Returns:
        dict: Index creation result or None if failed
    """
    for attempt in range(max_retries):
        try:
            print(f"Adding index {index_data.get('name')} to {collection.name}, attempt {attempt+1}/{max_retries}")
            result = collection.add_index(index_data)
            print(f"Successfully added index: {result}")
            return result
        except (ArangoServerError, ArangoClientError) as e:
            if "duplicate" in str(e).lower():
                print(f"Index {index_data.get('name')} already exists.")
                return None
            
            print(f"Error adding index (attempt {attempt+1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                sleep_time = retry_delay * (attempt + 1)  # Exponential backoff
                print(f"Retrying in {sleep_time} seconds...")
                time.sleep(sleep_time)
            else:
                print(f"Failed to add index after {max_retries} attempts.")
                return None


def get_essential_node_indexes():
    """Get the most critical node indexes based on SPOKE metapaths."""
    return [
        # Primary indexing for efficient node filtering
        {
            "type": "persistent",
            "fields": ["labels"],
            "name": "idx_nodes_labels",
            "sparse": False,
            "unique": False
        },
        
        # Identifier index for common lookups
        {
            "type": "persistent",
            "fields": ["properties.identifier"],
            "name": "idx_nodes_identifier",
            "sparse": False,
            "unique": False
        },
        
        # Protein-specific indexes for binding queries
        {
            "type": "persistent",
            "fields": ["properties.uniprot"],
            "name": "idx_nodes_uniprot",
            "sparse": True,  # Many nodes won't have this property
            "unique": False
        },
        
        # Gene indexes for expression metapaths
        {
            "type": "persistent",
            "fields": ["properties.gene"],
            "name": "idx_nodes_gene",
            "sparse": True,
            "unique": False
        },
        
        # Enzyme Commission (EC) index for metabolic/reaction paths
        {
            "type": "persistent",
            "fields": ["properties.ec"],
            "name": "idx_nodes_ec",
            "sparse": True,
            "unique": False
        },
        
        # Disease index for disease associations
        {
            "type": "persistent",
            "fields": ["properties.mesh"],
            "name": "idx_nodes_mesh",
            "sparse": True,
            "unique": False
        },
        
        # Compound index for pharmacological queries
        {
            "type": "persistent",
            "fields": ["properties.pubchem"],
            "name": "idx_nodes_pubchem",
            "sparse": True,
            "unique": False
        },
        
        # Combined node type + property indexes for efficient filtering
        {
            "type": "persistent",
            "fields": ["labels", "properties.identifier"],
            "name": "idx_compound_identifier",
            "sparse": False,
            "unique": False
        }
    ]


def get_essential_edge_indexes():
    """Get the most critical edge indexes based on SPOKE metapaths."""
    return [
        # Primary edge label index for traversal filtering
        {
            "type": "persistent",
            "fields": ["label"],
            "name": "idx_edges_label",
            "sparse": False,
            "unique": False
        },
        
        # Edge label + direction for fast traversals in specific directions
        {
            "type": "persistent",
            "fields": ["label", "_from"],
            "name": "idx_edges_label_direction",
            "sparse": False,
            "unique": False
        },
        
        # Treatment evidence index for evidence-based filtering
        {
            "type": "persistent",
            "fields": ["label", "properties.sources"],
            "name": "idx_edges_evidence",
            "sparse": True,
            "unique": False
        }
    ]


def main():
    """Add comprehensive indexing to SPOKE ArangoDB database."""
    parser = argparse.ArgumentParser(description="Add comprehensive indexing to SPOKE ArangoDB database")
    parser.add_argument("--host", default="localhost", help="ArangoDB host")
    parser.add_argument("--port", type=int, default=8529, help="ArangoDB port")
    parser.add_argument("--username", default="root", help="ArangoDB username")
    parser.add_argument("--password", default="ph", help="ArangoDB password")
    parser.add_argument("--db-name", default="spokeV6", help="Database name")
    parser.add_argument("--timeout", type=int, default=900, help="Request timeout in seconds")
    parser.add_argument("--phase", choices=["all", "nodes", "edges"], default="all", 
                        help="Indexing phase to run (nodes, edges, or all)")
    
    args = parser.parse_args()
    
    print(f"Connecting to ArangoDB at {args.host}:{args.port}, database: {args.db_name}")
    
    # Create a client connection with a longer timeout for large operations
    client = ArangoClient(hosts=f"http://{args.host}:{args.port}", request_timeout=args.timeout)
    
    try:
        # Connect to the database
        db = client.db(args.db_name, username=args.username, password=args.password)
        print("Successfully connected to database.")
        
        # Add node indexes if specified
        if args.phase in ["all", "nodes"]:
            print("\n=== ADDING NODE INDEXES ===")
            node_indexes = get_essential_node_indexes()
            for idx_data in node_indexes:
                add_index_with_retry(db.collection("Nodes"), idx_data)
        
        # Add edge indexes if specified
        if args.phase in ["all", "edges"]:
            print("\n=== ADDING EDGE INDEXES ===")
            edge_indexes = get_essential_edge_indexes()
            for idx_data in edge_indexes:
                add_index_with_retry(db.collection("Edges"), idx_data)
        
        print("\nIndex creation complete!")
        
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
