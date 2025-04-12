#!/usr/bin/env python3
"""
Diagnostic script to identify nodes missing from ArangoDB import.
Compares the nodes in the JSONL file with the nodes in ArangoDB.
"""

import json
import sys
from typing import Dict, List, Set, Any, Tuple
from pathlib import Path
from arango import ArangoClient


def load_jsonl_nodes(file_path: str) -> Tuple[Set[str], Dict[str, Dict[str, Any]]]:
    """Load node IDs and data from a JSONL file.
    
    Args:
        file_path: Path to the JSONL file
        
    Returns:
        Tuple containing:
        - Set of node IDs
        - Dictionary mapping node IDs to their data
    """
    node_ids = set()
    node_data = {}
    
    with open(file_path, 'r') as f:
        for line_num, line in enumerate(f, 1):
            try:
                doc = json.loads(line)
                if doc.get('type') == 'node':
                    node_id = str(doc.get('id', ''))
                    if node_id:
                        node_ids.add(node_id)
                        node_data[node_id] = doc
                    else:
                        print(f"Line {line_num}: Found node without id: {doc}")
            except json.JSONDecodeError:
                print(f"Line {line_num}: Invalid JSON: {line[:100]}...")
    
    return node_ids, node_data


def get_arango_node_ids(
    host: str = 'localhost', 
    port: int = 8529, 
    db_name: str = 'igf1_test', 
    username: str = 'root', 
    password: str = 'ph'
) -> Set[str]:
    """Retrieve Neo4j IDs from an ArangoDB collection.
    
    Args:
        host: ArangoDB host
        port: ArangoDB port
        db_name: Database name
        username: Database username
        password: Database password
        
    Returns:
        Set of Neo4j IDs stored in ArangoDB
    """
    client = ArangoClient(hosts=f'http://{host}:{port}')
    db = client.db(db_name, username=username, password=password)
    
    arango_ids = set()
    
    # Query all nodes and get their neo4j_id
    try:
        collection = db.collection('Nodes')
        documents = collection.all()
        
        for doc in documents:
            if doc.get('neo4j_id'):
                arango_ids.add(str(doc['neo4j_id']))
            # Fallback to id if neo4j_id is not available
            elif doc.get('id'):
                arango_ids.add(str(doc['id']))
    except Exception as e:
        print(f"Error querying ArangoDB: {e}")
    

    
    return arango_ids


def check_key_collisions(
    missing_ids: List[str], 
    node_data: Dict[str, Dict[str, Any]]
) -> Dict[str, List[str]]:
    """Check for potential key collisions among missing nodes.
    
    Args:
        missing_ids: List of missing node IDs
        node_data: Dictionary mapping node IDs to their data
        
    Returns:
        Dictionary mapping ArangoDB keys to lists of Neo4j IDs that would share that key
    """
    key_to_ids = {}
    
    for node_id in missing_ids:
        # This mimics the key generation in the importer
        key = str(node_id).replace(':', '_').replace('/', '_')
        
        if key not in key_to_ids:
            key_to_ids[key] = []
        key_to_ids[key].append(node_id)
    
    # Filter to only keys with multiple IDs (collisions)
    return {k: v for k, v in key_to_ids.items() if len(v) > 1}


def analyze_missing_nodes(
    jsonl_path: str,
    host: str = 'localhost', 
    port: int = 8529, 
    db_name: str = 'igf1_test', 
    username: str = 'root', 
    password: str = 'ph'
) -> None:
    """Analyze which nodes are missing from ArangoDB import.
    
    Args:
        jsonl_path: Path to the JSONL file
        host: ArangoDB host
        port: ArangoDB port
        db_name: Database name
        username: Database username
        password: Database password
    """
    print(f"Loading nodes from {jsonl_path}...")
    jsonl_node_ids, node_data = load_jsonl_nodes(jsonl_path)
    print(f"Found {len(jsonl_node_ids)} nodes in JSONL file")
    
    print(f"Connecting to ArangoDB at {host}:{port}...")
    arango_node_ids = get_arango_node_ids(host, port, db_name, username, password)
    print(f"Found {len(arango_node_ids)} nodes in ArangoDB")
    
    missing_nodes = jsonl_node_ids - arango_node_ids
    print(f"\nMissing nodes: {len(missing_nodes)}")
    
    if missing_nodes:
        # Check for key collisions
        sample_missing = list(missing_nodes)[:10]
        print("\nSample of missing node IDs:")
        for node_id in sample_missing:
            node = node_data.get(node_id, {})
            print(f"  - {node_id} (labels: {node.get('labels', [])})")
        
        # Look for potential key collisions
        collisions = check_key_collisions(list(missing_nodes), node_data)
        if collisions:
            print(f"\nFound {len(collisions)} potential key collisions:")
            for key, ids in list(collisions.items())[:5]:
                print(f"  - Key '{key}' would be shared by {len(ids)} nodes: {ids}")
        else:
            print("\nNo key collisions detected among missing nodes")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python diagnose_missing_nodes.py <jsonl_file> [host] [port] [db] [user] [password]")
        sys.exit(1)
    
    jsonl_path = sys.argv[1]
    
    host = sys.argv[2] if len(sys.argv) > 2 else 'localhost'
    port = int(sys.argv[3]) if len(sys.argv) > 3 else 8529
    db_name = sys.argv[4] if len(sys.argv) > 4 else 'igf1_test'
    username = sys.argv[5] if len(sys.argv) > 5 else 'root'
    password = sys.argv[6] if len(sys.argv) > 6 else 'ph'
    
    analyze_missing_nodes(jsonl_path, host, port, db_name, username, password)
