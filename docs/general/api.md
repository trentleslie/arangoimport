# API Reference

This document provides a reference for the ArangoImport API, including classes, functions, and their parameters.

## Core Components

### ImportConfig

The `ImportConfig` class defines configuration settings for the import process.

```python
ImportConfig(
    batch_size: int = 1000,
    stop_on_error: bool = False,
    skip_missing_refs: bool = True,
    preserve_id_fields: bool = True
)
```

#### Parameters:

- `batch_size`: Number of documents to import in a single batch
- `stop_on_error`: Whether to stop the import process on the first error
- `skip_missing_refs`: Whether to skip edges with missing source or target nodes
- `preserve_id_fields`: Whether to preserve original ID fields in imported documents

### DataProvider

The `DataProvider` abstract base class defines the interface for data providers.

```python
DataProvider
```

#### Methods:

- `get_nodes()`: Returns a generator yielding node data
- `get_edges()`: Returns a generator yielding edge data
- `validate_node(node)`: Validates a node dictionary
- `validate_edge(edge)`: Validates an edge dictionary
- `transform_node(node)`: Transforms a node dictionary to match the ArangoDB format
- `transform_edge(edge)`: Transforms an edge dictionary to match the ArangoDB format

### ProviderFactory

The `ProviderFactory` class handles provider registration and instantiation.

```python
ProviderFactory
```

#### Methods:

- `register_provider(name, provider_class)`: Registers a provider class
- `create_provider(name, config)`: Creates a provider instance

## Utility Functions

### parallel_load_data

Loads data from a file into ArangoDB using multiple parallel processes.

```python
parallel_load_data(
    file_path: str,
    db_config: Dict[str, Any],
    processes: int = multiprocessing.cpu_count(),
    import_config: Optional[ImportConfig] = None,
    log_level_str: str = 'WARNING'
) -> Tuple[int, int]
```

#### Parameters:

- `file_path`: Path to the input file
- `db_config`: Database configuration dictionary
- `processes`: Number of worker processes to use
- `import_config`: Import configuration
- `log_level_str`: Logging level

#### Returns:

- Tuple of (nodes_added, edges_added)

### setup_logging

Sets up logging for the import process.

```python
setup_logging(level_str: str = 'WARNING') -> None
```

#### Parameters:

- `level_str`: Logging level ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL')

## Command Line Interface

### Main Commands

- `import-data`: Import data from a file into ArangoDB
- `query-db`: Execute an AQL query against a database
- `drop-db`: Drop a database

### import-data Options

- `--host`: ArangoDB host
- `--port`: ArangoDB port
- `--username`: Database username
- `--password`: Database password
- `--db-name`: Database name
- `--processes`: Number of worker processes
- `--create-db`: Create database if it doesn't exist
- `--overwrite-db`: Drop database if it exists before import
- `--collection-nodes`: Node collection name
- `--collection-edges`: Edge collection name
- `--batch-size`: Batch size for imports
- `--stop-on-error`: Stop import on first error
