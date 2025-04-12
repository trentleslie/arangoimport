# Configuration Reference

This document provides a reference for configuring the ArangoImport tool.

## Configuration Options

ArangoImport supports configuration through command-line arguments and environment variables.

### Database Connection

| Option | Environment Variable | Default | Description |
|--------|---------------------|---------|-------------|
| `--host` | - | `localhost` | ArangoDB host |
| `--port` | - | `8529` | ArangoDB port |
| `--username` | `ARANGO_USER` | `root` | Database username |
| `--password` | `ARANGO_PASSWORD` | - | Database password |
| `--db-name` | - | `arango_import_db` | Database name |

### Import Process Configuration

| Option | Default | Description |
|--------|---------|-------------|
| `--processes` | CPU count | Number of worker processes |
| `--create-db` | `True` | Create database if it doesn't exist |
| `--overwrite-db` | `False` | Drop database if it exists before import |
| `--collection-nodes` | `Nodes` | Node collection name |
| `--collection-edges` | `Edges` | Edge collection name |
| `--batch-size` | `1000` | Batch size for imports |
| `--stop-on-error` | `False` | Stop import on first error |
| `--log-level` | `WARNING` | Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL) |

## Environment Variables

These environment variables can be used instead of command-line arguments:

```
ARANGO_USER=root
ARANGO_PASSWORD=your_password
```

## Configuration for Different Provider Types

When using different data providers, additional configuration options may be available.

### SPOKE Provider Configuration

The SPOKE provider includes the following default settings:

```python
{
    "database": {
        "db_name": "spokeV6",
        "nodes_collection": "Nodes",
        "edges_collection": "Edges",
        "create_db_if_missing": True,
        "overwrite_db": False,
    },
    "import": {
        "batch_size": 1000,
        "stop_on_error": False,
        "skip_missing_refs": True,
        "preserve_id_fields": True,
    },
    "schema": {
        "node_id_field": "id",
        "edge_from_field": "start.id",
        "edge_to_field": "end.id",
        "edge_label_field": "label",
    }
}
```

## Advanced Configuration

### Connection Pooling

ArangoImport uses a connection pool to manage database connections efficiently. The pool size is determined by the number of worker processes.

### Logging Configuration

Logging can be configured in more detail by modifying the `log_config.py` file. By default, logs are written to:

- Console (stderr)
- Log file with timestamp (e.g., `arangodb_import_20250409_220330.log`)

### Import Configuration

The `ImportConfig` class can be used to configure the import process in more detail:

```python
ImportConfig(
    batch_size=1000,
    stop_on_error=False,
    skip_missing_refs=True,
    preserve_id_fields=True
)
```

## Configuration Files (Future Feature)

In future versions, ArangoImport will support YAML configuration files for more complex import scenarios.

Example:

```yaml
arangodb:
  endpoint: "http://localhost:8529"
  database: "my_database"
  username: "root"
  password: "${ARANGO_PASSWORD}"

import_job:
  nodes:
    collection: "MyNodes"
    sources: [{path: "/data/nodes/*.jsonl", format: "jsonl"}]
    key_field: "id"
    on_duplicate: "update"
  edges:
    - collection: "REL_TYPE_A"
      sources: [{path: "/data/edges_a*.jsonl", format: "jsonl"}]
      from_field: "source_id"
      to_field: "target_id"
      on_duplicate: "ignore"

settings:
  threads: 8
  deduplicate_edges: true
  log_level: "WARNING"
```
