#!/usr/bin/env python3
"""
Diagnostic script to examine the structure of nodes that weren't imported into ArangoDB.
"""

import json
import sys
from typing import Dict, List, Set, Any, Tuple
from pathlib import Path
from arango import ArangoClient


def load_jsonl_nodes(file_path: str) -> Dict[str, Dict[str, Any]]:
    """Load node data from a JSONL file.
    
    Args:
        file_path: Path to the JSONL file
        
    Returns:
        Dictionary mapping node IDs to their data
    """
    node_data = {}
    
    with open(file_path, 'r') as f:
        for line_num, line in enumerate(f, 1):
            try:
                doc = json.loads(line)
                if doc.get('type') == 'node':
                    node_id = str(doc.get('id', ''))
                    if node_id:
                        node_data[node_id] = doc
            except json.JSONDecodeError:
                print(f"Line {line_num}: Invalid JSON: {line[:100]}...")
    
    return node_data


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


def check_node_validation(node: Dict[str, Any]) -> Tuple[bool, str]:
    """Check if a node would pass validation using the same logic as the importer.
    
    Args:
        node: Node document to validate
        
    Returns:
        Tuple containing (is_valid, reason)
    """
    if not isinstance(node, dict):
        return False, "Document must be a dictionary"

    if "type" not in node:
        return False, "Document must have a 'type' field"

    doc_type = node.get("type", "").lower()
    if doc_type != "node":
        return False, f"Invalid document type: {doc_type}"

    if not (node.get("id") or node.get("_key")):
        return False, "Node document must have either 'id' or '_key' field"

    # Validate ID format if present
    if "id" in node and not isinstance(node["id"], (str, int)):
        return False, f"Invalid node id type: {type(node['id'])}"

    # Check properties
    if "properties" in node and not isinstance(node["properties"], dict):
        return False, "Node properties must be a dictionary"

    # Validate label if present
    if "label" in node and not isinstance(node["label"], str):
        return False, "Node label must be a string"

    return True, "Valid"


def examine_missing_nodes(
    jsonl_path: str,
    host: str = 'localhost', 
    port: int = 8529, 
    db_name: str = 'igf1_test', 
    username: str = 'root', 
    password: str = 'ph'
) -> None:
    """Examine characteristics of nodes missing from ArangoDB import.
    
    Args:
        jsonl_path: Path to the JSONL file
        host: ArangoDB host
        port: ArangoDB port
        db_name: Database name
        username: Database username
        password: Database password
    """
    print(f"Loading nodes from {jsonl_path}...")
    all_nodes = load_jsonl_nodes(jsonl_path)
    print(f"Found {len(all_nodes)} nodes in JSONL file")
    
    print(f"Connecting to ArangoDB at {host}:{port}...")
    arango_node_ids = get_arango_node_ids(host, port, db_name, username, password)
    print(f"Found {len(arango_node_ids)} nodes in ArangoDB")
    
    missing_ids = set(all_nodes.keys()) - arango_node_ids
    print(f"\nMissing nodes: {len(missing_ids)}")
    
    if missing_ids:
        missing_nodes = {id: all_nodes[id] for id in missing_ids}
        
        # Analyze label distribution
        label_counts = {}
        for node_id, node in missing_nodes.items():
            labels = node.get('labels', [])
            for label in labels:
                label_counts[label] = label_counts.get(label, 0) + 1
        
        print("\nLabel distribution of missing nodes:")
        for label, count in sorted(label_counts.items(), key=lambda x: x[1], reverse=True):
            print(f"  - {label}: {count}")
        
        # Check validation status
        invalid_nodes = []
        validation_failures = {}
        
        for node_id, node in missing_nodes.items():
            is_valid, reason = check_node_validation(node)
            if not is_valid:
                invalid_nodes.append(node_id)
                validation_failures[reason] = validation_failures.get(reason, 0) + 1
        
        if invalid_nodes:
            print(f"\nFound {len(invalid_nodes)} nodes that would fail validation:")
            for reason, count in validation_failures.items():
                print(f"  - {reason}: {count}")
            
            # Sample of invalid nodes
            print("\nSample of invalid nodes:")
            for node_id in invalid_nodes[:5]:
                node = missing_nodes[node_id]
                print(f"  - Node {node_id}:")
                print(f"    Labels: {node.get('labels', [])}")
                print(f"    Properties keys: {list(node.get('properties', {}).keys())}")
        else:
            print("\nAll missing nodes would pass validation.")
            
        # Examine structure patterns
        property_patterns = {}
        for node_id, node in missing_nodes.items():
            props = node.get('properties', {})
            has_properties = len(props) > 0
            
            pattern = f"has_properties={has_properties}"
            property_patterns[pattern] = property_patterns.get(pattern, 0) + 1
        
        print("\nProperty patterns in missing nodes:")
        for pattern, count in property_patterns.items():
            print(f"  - {pattern}: {count}")
        
        # Sample of missing nodes
        print("\nDetailed sample of missing nodes:")
        for node_id in list(missing_ids)[:3]:
            node = all_nodes[node_id]
            print(f"\n  Node {node_id}:")
            print(f"    Type: {node.get('type')}")
            print(f"    Labels: {node.get('labels', [])}")
            print(f"    Properties: {json.dumps(node.get('properties', {}), indent=2)}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python examine_missing_nodes.py <jsonl_file> [host] [port] [db] [user] [password]")
        sys.exit(1)
    
    jsonl_path = sys.argv[1]
    
    host = sys.argv[2] if len(sys.argv) > 2 else 'localhost'
    port = int(sys.argv[3]) if len(sys.argv) > 3 else 8529
    db_name = sys.argv[4] if len(sys.argv) > 4 else 'igf1_test'
    username = sys.argv[5] if len(sys.argv) > 5 else 'root'
    password = sys.argv[6] if len(sys.argv) > 6 else 'ph'
    
    examine_missing_nodes(jsonl_path, host, port, db_name, username, password)
