# Usage Guide

This guide explains how to use the ArangoImport tool for importing data into ArangoDB.

## Basic Usage

The basic command structure for ArangoImport is:

```bash
python -m arangoimport.cli import-data [OPTIONS] FILE_PATH
```

### Required Arguments

- `FILE_PATH`: Path to the input file (typically a JSONL file)

### Common Options

- `--host`: ArangoDB host (default: "localhost")
- `--port`: ArangoDB port (default: 8529)
- `--username`: Database username (default: "root")
- `--password`: Database password
- `--db-name`: Database name
- `--processes`: Number of worker processes (default: CPU count)
- `--log-level`: Logging level (default: "WARNING")

## Example Commands

### Import Data

```bash
python -m arangoimport.cli import-data /path/to/data.jsonl --username root --password mypassword --db-name my_database
```

### Query Database

```bash
python -m arangoimport.cli query-db --host localhost --port 8529 --username root --password mypassword --db-name my_database --query "RETURN LENGTH(Nodes)"
```

### Drop Database

```bash
python -m arangoimport.cli drop-db my_database --host localhost --port 8529 --username root --password mypassword --yes
```

## Using Different Providers

When using different providers, you can specify the provider with the `--provider` option:

```bash
python -m arangoimport.cli import-data /path/to/data.jsonl --provider spoke --username root --password mypassword --db-name my_database
```

## Configuration File

For complex imports, you can use a configuration file:

```bash
python -m arangoimport.cli import-data --config config.yaml
```

See the [Configuration Reference](./configuration.md) for details on the configuration file format.
