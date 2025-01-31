"""Tutorial demonstrating how to use arangoimport as a Python package.

This script shows how to programmatically import Neo4j graph data exports
into ArangoDB. The tool expects a line-delimited JSON (JSONL) file that was
exported from Neo4j using 'apoc.export.json.all' or similar APOC procedures.

Installation and Usage:
1. Install the package:
   pip install arangoimport

2. Export your Neo4j database to JSONL:
   CALL apoc.export.json.all("path/to/export.jsonl", {useTypes: true})

3. You can use the tool in two ways:

   A. Command Line Interface (CLI):
      After installation, the 'arangoimport' command will be available in your terminal:
      
      # Show help and available options
      arangoimport --help
      
      # Import data with default settings (will prompt for password)
      arangoimport import-data /path/to/neo4j_export.jsonl
      
      # Import with custom settings
      arangoimport import-data /path/to/neo4j_export.jsonl \\
          --db-name my_graph \\
          --host arangodb.example.com \\
          --port 8530 \\
          --username graph_user
      
      Note: You can set ARANGO_PASSWORD environment variable to avoid password prompt
   
   B. Python API (this script):
      Update the configuration below and run:
      python tutorial.py

The script includes two examples of using the Python API:
- Example 1: Using default settings (gets password from ARANGO_PASSWORD env var)
- Example 2: Using custom settings (uses CUSTOM_CONFIG values below)
"""

import os
from pathlib import Path
from typing import Any, Dict

from arangoimport.connection import ArangoConfig
from arangoimport.importer import parallel_load_data
from arangoimport.logging import setup_logging

# ArangoDB Connection Settings
DEFAULT_HOST = "localhost"
DEFAULT_PORT = 8529
DEFAULT_USERNAME = "root"
DEFAULT_DB_NAME = "db_name"  # Change this to your target database name

# Import Configuration
DEFAULT_NUM_PROCESSES = None  # None means use (CPU count - 1) processes
DATA_FILE = "path/to/neo4j_export.jsonl"  # Replace with your Neo4j export file path

# Custom Settings Example
CUSTOM_CONFIG = {
    "host": "arangodb.example.com",
    "port": 8530,
    "username": "graph_user",
    "password": "your_password",  # For demonstration only - don't hardcode passwords
    "db_name": "my_graph",
    "num_processes": 4,  # Explicitly set number of processes (otherwise uses CPU count - 1)
}

def import_graph_data(
    file_path: str | Path,
    db_name: str = DEFAULT_DB_NAME,
    host: str = DEFAULT_HOST,
    port: int = DEFAULT_PORT,
    username: str = DEFAULT_USERNAME,
    password: str | None = None,
    num_processes: int | None = DEFAULT_NUM_PROCESSES,
) -> tuple[int, int]:
    """Import Neo4j graph data export into ArangoDB.
    
    This function imports a Neo4j database export in JSONL format into ArangoDB.
    The input file should be created using Neo4j's APOC export procedures, such as:
    CALL apoc.export.json.all("path/to/export.jsonl", {useTypes: true})
    
    Args:
        file_path: Path to JSONL file exported from Neo4j
        db_name: Name of the target ArangoDB database
        host: ArangoDB host
        port: ArangoDB port
        username: Database username
        password: Database password (if None, will try to get from env var ARANGO_PASSWORD)
        num_processes: Number of processes to use for parallel import.
                      If None, uses (CPU count - 1) for optimal performance.
    
    Returns:
        tuple[int, int]: Number of nodes and edges added
    
    Raises:
        ValueError: If password is not provided and ARANGO_PASSWORD env var is not set
        ConnectionError: If unable to connect to ArangoDB
    """
    # Get password from environment if not provided
    if password is None:
        password = os.getenv("ARANGO_PASSWORD")
        if not password:
            raise ValueError(
                "Database password must be provided either as an argument "
                "or through ARANGO_PASSWORD environment variable"
            )

    # Configure database connection
    db_config = ArangoConfig(
        host=host,
        port=port,
        username=username,
        password=password,
        db_name=db_name,
    )

    # Convert to dict for parallel processing
    config_dict: Dict[str, Any] = dict(db_config)

    # Import the data
    return parallel_load_data(
        str(file_path),
        config_dict,
        num_processes=num_processes
    )

def main() -> None:
    """Example usage of the import_graph_data function."""
    # Set up logging
    setup_logging()
    
    try:
        # Example 1: Import data using default settings (password from env var)
        print("\nExample 1: Using default settings")
        print("---------------------------------")
        nodes, edges = import_graph_data(
            DATA_FILE,
            host=DEFAULT_HOST,
            port=DEFAULT_PORT,
            username=DEFAULT_USERNAME,
            db_name=DEFAULT_DB_NAME,
        )
        print(f"Successfully imported {nodes:,} nodes and {edges:,} edges!")
        
        # Example 2: Import with custom settings
        print("\nExample 2: Using custom settings")
        print("-------------------------------")
        nodes, edges = import_graph_data(
            DATA_FILE,
            host=CUSTOM_CONFIG["host"],
            port=CUSTOM_CONFIG["port"],
            username=CUSTOM_CONFIG["username"],
            password=CUSTOM_CONFIG["password"],
            db_name=CUSTOM_CONFIG["db_name"],
            num_processes=CUSTOM_CONFIG["num_processes"],
        )
        print(f"Successfully imported {nodes:,} nodes and {edges:,} edges!")
        
    except Exception as e:
        print(f"Error importing data: {e}")

if __name__ == "__main__":
    main()
