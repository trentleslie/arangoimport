#!/bin/bash

# Fresh Start Import Script
# This script cleans up logs, drops the database, and starts a fresh import
# with the optimized edge-only enhanced logging

echo "==================================================================="
echo "SPOKE Fresh Import with Optimized Edge-Only Logging"
echo "Starting at $(date)"
echo "==================================================================="

# 1. Clean up log directory
echo "Cleaning up log directory..."
mkdir -p /home/ubuntu/spoke/arangoimport/logs/archive
find /home/ubuntu/spoke/arangoimport/logs/ -type f -not -path "*/archive/*" -exec mv {} /home/ubuntu/spoke/arangoimport/logs/archive/ \;
echo "Log directory cleaned."

# 2. Drop the existing database
echo "Dropping existing 'spokeV6' database..."
~/.local/bin/poetry run python -m src.arangoimport.cli --log-level INFO drop-db --host localhost --port 8529 --username root --password ph --db-name spokeV6
echo "Database dropped."

# 3. Set environment variables
export ARANGO_USERNAME=root
export ARANGO_PASSWORD=ph

# 4. Create a new database
echo "Creating fresh 'spokeV6' database..."
~/.local/bin/poetry run python -m src.arangoimport.cli --log-level INFO create-db --host localhost --port 8529 --username root --password ph --db-name spokeV6
echo "Fresh database created."

# 5. System resource check
echo "System resource check before import:"
echo "Memory usage:"
free -h
echo "Disk space:"
df -h /

# 6. Start SPOKE import with optimized parameters
echo "Starting SPOKE import with optimized parameters..."
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
echo "Import started at $(date)" > /home/ubuntu/spoke/arangoimport/logs/import_${TIMESTAMP}.log

# Running with 12 workers (instead of 16) for better stability
~/.local/bin/poetry run python -m src.arangoimport.cli --log-level INFO import \
  --host localhost \
  --port 8529 \
  --username root \
  --password ph \
  --db-name spokeV6 \
  --edges-file /home/ubuntu/spoke/arangoimport/data/edges.jsonl \
  --nodes-file /home/ubuntu/spoke/arangoimport/data/nodes.jsonl \
  --chunk-size 17500 \
  --processes 12 \
  --log-file /home/ubuntu/spoke/arangoimport/logs/import_${TIMESTAMP}.log \
  --errors-file /home/ubuntu/spoke/arangoimport/logs/import_errors_${TIMESTAMP}.log

# 7. Run post-import analysis
echo "Import completed at $(date)"
echo "Running post-import analysis..."
~/.local/bin/poetry run python -m src.arangoimport.enhanced_logging analyze-import

echo "==================================================================="
echo "SPOKE Import Process Complete"
echo "Finished at $(date)"
echo "==================================================================="
