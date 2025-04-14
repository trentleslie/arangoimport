#!/usr/bin/env python3
"""
Script to add essential indexes to SPOKE database in ArangoDB.
This version uses even longer timeouts for the large edge collection.
"""

import time
import sys
from arango import ArangoClient
from arango.exceptions import ArangoServerError, ArangoClientError


def add_index_with_retry(collection, index_data, max_retries=8, retry_delay=10):
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


def main():
    """Add essential indexes to SPOKE ArangoDB database."""
    # Connection settings
    host = "localhost"
    port = 8529
    username = "root"
    password = "ph"
    db_name = "spokeV6"
    
    print(f"Connecting to ArangoDB at {host}:{port}, database: {db_name}")
    
    # Create a client connection with a much longer timeout
    client = ArangoClient(hosts=f"http://{host}:{port}", request_timeout=600)
    
    try:
        # Connect to the database
        db = client.db(db_name, username=username, password=password)
        print("Successfully connected to database.")
        
        # Add edge label index (critical for traversals)
        edge_label_index = {
            "type": "persistent", 
            "fields": ["label"],
            "name": "idx_edges_label",
            "sparse": False,
            "unique": False
        }
        
        add_index_with_retry(db.collection("Edges"), edge_label_index)
        
        # Add node identifier index (helpful for lookups)
        node_identifier_index = {
            "type": "persistent",
            "fields": ["properties.identifier"],
            "name": "idx_nodes_identifier",
            "sparse": False,
            "unique": False
        }
        
        add_index_with_retry(db.collection("Nodes"), node_identifier_index)
        
        print("Index creation script completed.")
        
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
