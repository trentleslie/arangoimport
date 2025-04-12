# Roadmap for Generalizing the ArangoDB Import Process

This document outlines the planned phases to generalize the ArangoDB import process, making it configurable and reusable beyond the initial SPOKE use case. The generalization will build upon the solid foundation of the existing codebase while preserving SPOKE-specific functionality for backward compatibility.

## Phase 0: Architecture Assessment & Preservation Strategy

### Current Architecture

The existing architecture follows a well-structured modular design:

```
/src/arangoimport/
├── __init__.py
├── cli.py              # Command-line interface 
├── config.py           # Configuration settings
├── connection.py       # ArangoDB connection management
├── id_mapping.py       # ID mapping for preserving relationships
├── importer.py         # Core import functionality
├── log_config.py       # Logging configuration
├── monitoring.py       # Import monitoring and statistics
├── quality.py          # Data quality assessment
├── utils.py            # Utility functions
└── validation.py       # Data validation
```

### SPOKE Preservation Strategy

1. **Create Provider Pattern**:
   - Develop a provider interface for data source abstraction
   - Move SPOKE-specific logic to a dedicated provider implementation
   - Structure as follows:

```
/src/arangoimport/
└── providers/
    └── spoke/
        ├── __init__.py
        ├── adapters.py      # SPOKE-specific data adapters
        ├── config.py        # SPOKE-specific configurations
        └── transformers.py  # SPOKE-specific data transformations
```

2. **Documentation Consolidation**:
   - Organize existing documentation into a structured hierarchy
   - Preserve all migration reports and troubleshooting information
   - Create the following structure:

```
/docs/
├── general/
│   ├── installation.md
│   ├── usage.md
│   └── api.md
├── migration/
│   ├── overview.md
│   ├── neo4j_to_arango.md
│   └── best_practices.md
└── case_studies/
    └── spoke/
        ├── migration_analysis.md     # Current MIGRATION_ANALYSIS.md
        ├── troubleshooting.md        # Current arangoimport_troublehshooting.md
        └── database_analysis.md      # Current spoke_database_analysis.md
```

## Phase 1: Design & Configuration

1.  **Identify Core Configurable Elements:** List everything that might change between different import tasks:
    *   **Source Data:**
        *   List of input files or directories for nodes.
        *   List of input files or directories for edges.
        *   Data format (`jsonl`, `csv`, `tsv`).
        *   CSV/TSV specific options (separator, quote char, header row).
    *   **Target ArangoDB:**
        *   Endpoint (e.g., `http://localhost:8529`).
        *   Database name (`_system` or a custom one).
        *   Username & Password (consider secrets management).
        *   Target node collection name(s).
        *   Target edge collection name(s).
    *   **Mapping & Schema:**
        *   Field in node data to use as `_key` (if any).
        *   Fields in edge data to use as `_from` and `_to`.
        *   Collection name prefix required for `_from`/`to` values (e.g., `"Nodes/"`).
        *   Field in edge data representing the edge type/label (optional).
    *   **Import Process Control:**
        *   `arangoimport` parameters: `--threads`, `--batch-size`.
        *   `--on-duplicate` policy for nodes (e.g., `update`, `replace`, `ignore`).
        *   `--on-duplicate` policy for edges (likely `ignore` initially).
        *   Whether to create collections if they don't exist.
        *   Whether to create the database if it doesn't exist.
    *   **Post-Processing:**
        *   Enable/disable edge deduplication step.
        *   Custom AQL queries or scripts to run post-import (for indexing, validation, etc.).
    *   **Logging/Output:**
        *   Log file path.
        *   Verbosity level.

2.  **Choose Configuration Format:** Select YAML for its readability and support for structured data (e.g., `import_config.yaml`).

3.  **Design Configuration Structure:** Create a clear YAML structure, grouping related settings. Example snippet:
    ```yaml
    arangodb:
      endpoint: "http://localhost:8529"
      database: "my_database"
      # ... credentials ...

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
        # ... more edge types ...

    settings:
      threads: 8
      deduplicate_edges: true
      # ... other settings ...
    ```

## Phase 2: Core Architecture Implementation

### Provider Architecture

1. **Define Provider Interface**:
   - Create a base `DataProvider` abstract class with required methods
   - Implement provider discovery and registration mechanism
   - Define standardized interfaces for data transformation and validation

2. **Refactor Core Components**:
   - Update importer to use provider interfaces
   - Create factory class for provider instantiation
   - Implement dependency injection to decouple components

3. **SPOKE Provider Implementation**:
   - Migrate existing SPOKE-specific code to provider implementation
   - Add comprehensive tests to ensure compatibility
   - Document SPOKE provider features and configuration

## Phase 3: Implementation

4.  **Develop the Importer Script (Python):**
    *   Use `PyYAML` to load and parse the `import_config.yaml`.
    *   Use a library like `Pydantic` or `jsonschema` to validate the loaded config against a defined schema, providing clear error messages for invalid configurations.
    *   Use `pyArango` (or basic HTTP requests) to connect to ArangoDB for pre-checks (database/collection existence, `allowUserKeys`) and optionally create resources if configured.
    *   Implement the **two-phase import logic**:
        *   **Node Phase:** Iterate through configured `nodes.sources`. Construct the exact `arangoimport` command for each source, dynamically setting arguments like `--type`, `--collection`, `--on-duplicate`, `--file`, server connection details, `--threads`, `--batch-size`, and potentially mapping the `_key` using `--translate` if the `key_field` is specified.
        *   Execute node import commands using `subprocess.run` or `Popen`. Capture stdout/stderr for logging. Wait for all node commands to complete successfully before proceeding.
        *   **Edge Phase:** Iterate through configured `edges`. For each edge definition, construct the `arangoimport` command. **Critically:** Add logic to format `--from-collection-prefix` and `--to-collection-prefix` based on the node collection name(s) *unless* the source data already contains the full `Collection/Key` strings. Map `--create-collection-type edge` if creating edge collections.
        *   Execute edge import commands using `subprocess`. Wait for completion.
    *   If `deduplicate_edges` is enabled, use `pyArango` to connect and execute the deduplication AQL query dynamically for each edge collection listed in the config.
    *   Implement robust logging using Python's `logging` module, writing to the configured file and controlling verbosity.

## Phase 4: Extension & Plugin System

1. **Plugin Architecture**:
   - Design a plugin system for custom transformations
   - Create plugin discovery and loading mechanism
   - Implement validation for plugins

2. **Extension Points**:
   - Define clear extension points for:
     - Custom data sources (beyond file-based imports)
     - Data transformations
     - Validation rules
     - Post-processing operations

3. **Example Extensions**:
   - Develop reference implementations:
     - CSV/TSV data source plugin
     - Graph database connector (Neo4j, TigerGraph)
     - JSON Schema validator

## Phase 5: Refinement & Testing

1. **Secrets Management:** 
   - Refactor to avoid storing passwords directly in YAML 
   - Support environment variables, a `.env` file (with `python-dotenv`), or integration with a secrets management system
   - Implement credential providers for different authentication methods

2. **Error Handling:** 
   - Create a comprehensive error taxonomy
   - Enhance error reporting with clear, actionable messages
   - Implement robust exception handling for all operations:
     - File operations
     - Configuration parsing
     - Subprocess execution (`arangoimport` non-zero exit codes)
     - AQL execution
   - Add detailed logging with context information

3. **Testing:**
   - Create a comprehensive test suite:
     - Unit tests for core components
     - Integration tests for providers
     - End-to-end tests for complete workflows
   - Test with the validated SPOKE import configuration
   - Develop synthetic test cases:
     - Various data formats (JSONL, CSV, TSV)
     - Different key strategies (provided vs. auto-generated)
     - Imports with/without deduplication
     - Targeting existing vs. new databases/collections
   - Implement benchmarking tools for performance testing

4. **Documentation:** 
   - Develop comprehensive documentation:
     - Purpose and key features
     - Architecture overview
     - Installation guide
     - Configuration reference
     - Provider development guide
     - Plugin development guide
     - Troubleshooting guide
   - Create practical examples:
     - Basic import workflows
     - Advanced configurations
     - Custom provider implementations

## Phase 6: Distribution & Maintenance

1. **Package Distribution:**
   - Prepare for PyPI publication
   - Create Docker container for easy deployment
   - Develop CI/CD pipeline for automated testing and releases

2. **Monitoring & Telemetry:**
   - Add optional anonymized usage statistics
   - Implement performance monitoring
   - Create detailed import reports

3. **Community Building:**
   - Establish contribution guidelines
   - Create provider registry for community-contributed providers
   - Develop showcase examples of real-world applications
