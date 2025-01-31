# ArangoImport

A high-performance tool for importing Neo4j JSONL graph data exports into ArangoDB.

## Features

- Import Neo4j database exports into ArangoDB
- Efficient parallel processing of large JSONL files
- Support for both local and Docker ArangoDB instances
- Dynamic memory management and batch sizing
- Connection pooling for optimal performance
- Progress tracking and detailed logging
- Available as both CLI tool and Python package

## Installation

```bash
pip install arangoimport
```

## Quick Start

1. Export your Neo4j database to JSONL:
   ```cypher
   CALL apoc.export.json.all("path/to/export.jsonl", {useTypes: true})
   ```

2. Import into ArangoDB using either method:

   ### A. Command Line Interface (CLI)
   After installation, the `arangoimport` command is available in your terminal:
   ```bash
   # Show help and available options
   arangoimport --help
   
   # Import data with default settings (will prompt for password)
   arangoimport import-data /path/to/neo4j_export.jsonl
   
   # Import with custom settings
   arangoimport import-data /path/to/neo4j_export.jsonl \
       --db-name my_graph \
       --host arangodb.example.com \
       --port 8530 \
       --username graph_user
   ```

   ### B. Python API
   ```python
   from arangoimport.connection import ArangoConfig
   from arangoimport.importer import parallel_load_data
   
   # Configure database connection
   db_config = ArangoConfig(
       host="localhost",
       port=8529,
       username="root",
       password="your_password",  # Or use ARANGO_PASSWORD env var
       db_name="db_name"
   )
   
   # Import the data
   nodes, edges = parallel_load_data(
       "path/to/neo4j_export.jsonl",
       dict(db_config),
       num_processes=None  # None means use (CPU count - 1)
   )
   
   print(f"Successfully imported {nodes:,} nodes and {edges:,} edges!")
   ```

## Environment Variables

- `ARANGO_PASSWORD`: Database password (avoid hardcoding in scripts)
- `ARANGO_USER`: Username (default: root)

## CLI Options

### General Options
- `--file <string>`: The file to import ("-" for stdin)
- `--type <string>`: Input format (auto/csv/json/jsonl/tsv, default: auto)
- `--collection <string>`: Target collection name
- `--create-collection <boolean>`: Create collection if missing (default: false)
- `--create-collection-type <string>`: Collection type if created (document/edge, default: document)
- `--create-database <boolean>`: Create database if missing (default: false)
- `--threads <uint32>`: Number of parallel import threads (default: 32)
- `--batch-size <uint64>`: Data batch size in bytes (default: 8MB)
- `--progress <boolean>`: Show progress (default: true)

### Server Connection
- `--server.database <string>`: Target database (default: "_system")
- `--server.endpoint <string>`: Server endpoint (default: "http+tcp://127.0.0.1:8529")
- `--server.username <string>`: Username (default: "root")
- `--server.password <string>`: Password (prompted if not provided)
- `--server.authentication <boolean>`: Require authentication (default: true)

### Performance Options
- `--auto-rate-limit <boolean>`: Auto-adjust loading rate (default: false)
- `--compress-transfer <boolean>`: Compress data transfer (default: false)
- `--max-errors <uint64>`: Maximum errors before stopping (default: 20)
- `--skip-validation <boolean>`: Skip schema validation (default: false)

For a complete list of options, run:
```bash
arangoimport --help
```

## Docker Support

When using Docker, ensure your ArangoDB container is running:
```bash
docker run -p 8529:8529 -e ARANGO_ROOT_PASSWORD=yourpassword arangodb:latest
```

Then import using either the CLI or Python API, pointing to the exposed port.

## Performance Tuning

The importer automatically optimizes for:
- Available system memory
- CPU cores (uses CPU count - 1 by default)
- Network conditions

You can fine-tune performance with:
- `--threads`: Control parallel threads
- `--batch-size`: Adjust batch size
- `--auto-rate-limit`: Enable automatic rate limiting
- `--compress-transfer`: Enable data compression

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.