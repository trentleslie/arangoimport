# Migration Overview

This document provides an overview of migrating data from various sources to ArangoDB using the ArangoImport tool.

## Migration Process

The general migration process involves these steps:

1. **Extract**: Extract data from the source database
2. **Transform**: Transform the data into a format compatible with ArangoDB
3. **Import**: Import the transformed data into ArangoDB

## Supported Data Sources

ArangoImport currently supports the following data sources:

- **JSONL files**: Each line contains a valid JSON object
- **Neo4j exports**: Data exported from Neo4j databases (via the SPOKE provider)

## Migration Considerations

When migrating data, consider the following:

### Node Keys

ArangoDB requires each node to have a unique `_key` attribute. You can either:
- Use existing IDs from your source data
- Let ArangoDB generate keys automatically

### Edge Requirements

In ArangoDB, edges require:
- A `_from` attribute referencing the source node
- A `_to` attribute referencing the target node
- Both references must use the format `collection/key`

### Data Validation

ArangoImport performs validation during the import process to ensure data integrity. Validation includes:
- Checking required fields
- Validating edge references
- Type checking

## Performance Optimization

To optimize migration performance:
- Use parallel processing with multiple worker processes
- Configure appropriate batch sizes
- Consider using a local ArangoDB instance during import

## Provider-Specific Details

Each data source provider may have specific requirements and optimizations. See the provider-specific documentation for details:

- [SPOKE Migration](../case_studies/spoke/migration_analysis.md)
