# Installation Guide

This guide explains how to install and set up the ArangoImport tool.

## Requirements

- Python 3.9 or higher
- ArangoDB 3.7 or higher
- pip or poetry for dependency management

## Installation Steps

### Using pip

```bash
pip install arangoimport
```

### Using poetry

```bash
poetry add arangoimport
```

### From source

```bash
git clone https://github.com/user/arangoimport.git
cd arangoimport
poetry install
```

## Configuration

After installation, you'll need to configure ArangoImport for your specific use case. See the [Usage Guide](./usage.md) for details on configuration options.

## Verifying Installation

To verify that ArangoImport is correctly installed, run:

```bash
python -m arangoimport.cli --version
```

This should display the version of ArangoImport installed.
