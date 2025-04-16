#!/bin/bash

# Edge-Focused Import Script with 25 Processors
# This script runs a fresh import with 25 processors and
# focuses only on edge batches divisible by 100 for enhanced logging

echo "==================================================================="
echo "SPOKE Edge-Focused Import with 25 PROCESSORS"
echo "Starting at $(date)"
echo "==================================================================="

# Create a new database
echo "Creating fresh 'spokeV6' database..."
~/.local/bin/poetry run python -m src.arangoimport.cli --log-level INFO create-db --host localhost --port 8529 --username root --password ph spokeV6
echo "Fresh database created."

# Set environment variables
export ARANGO_USERNAME=root
export ARANGO_PASSWORD=ph

# System resource check
echo "System resource check before import:"
echo "Memory usage:"
free -h
echo "Disk space:"
df -h /

# Start SPOKE import with optimized parameters and 25 processors
echo "Starting SPOKE import with 25 processors and edge-focused logging..."
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
echo "Import started at $(date)" > /home/ubuntu/spoke/arangoimport/logs/import_${TIMESTAMP}.log

# Set up a periodic stats check (every 5 minutes)
(
  while true; do
    echo "======== DATABASE STATS at $(date) ========" >> /home/ubuntu/spoke/arangoimport/logs/db_stats_${TIMESTAMP}.log
    ~/.local/bin/poetry run python -m src.arangoimport.cli --log-level INFO query-db --host localhost --port 8529 --username root --password ph spokeV6 --query "RETURN { nodes: LENGTH(Nodes), edges: LENGTH(Edges) }" >> /home/ubuntu/spoke/arangoimport/logs/db_stats_${TIMESTAMP}.log 2>&1
    echo "" >> /home/ubuntu/spoke/arangoimport/logs/db_stats_${TIMESTAMP}.log
    sleep 300  # Check every 5 minutes
  done
) &
STATS_PID=$!

# Running with 25 workers for maximum speed with edge-focused logging
~/.local/bin/poetry run python -m src.arangoimport.cli --log-level INFO import \
  --host localhost \
  --port 8529 \
  --username root \
  --password ph \
  --db-name spokeV6 \
  --edges-file /home/ubuntu/spoke/arangoimport/data/edges.jsonl \
  --nodes-file /home/ubuntu/spoke/arangoimport/data/nodes.jsonl \
  --chunk-size 18000 \
  --processes 25 \
  --log-file /home/ubuntu/spoke/arangoimport/logs/import_${TIMESTAMP}.log \
  --errors-file /home/ubuntu/spoke/arangoimport/logs/import_errors_${TIMESTAMP}.log

# Stop the stats checking process
kill $STATS_PID 2>/dev/null || true

# Run post-import analysis to examine the "divisible by 100" patterns
echo "Import completed at $(date)"
echo "Running edge batch analysis..."
~/.local/bin/poetry run python -m src.arangoimport.cli --log-level INFO query-db --host localhost --port 8529 --username root --password ph spokeV6 --query "RETURN { nodes: LENGTH(Nodes), edges: LENGTH(Edges) }" > /home/ubuntu/spoke/arangoimport/logs/final_stats_${TIMESTAMP}.log

echo "==================================================================="
echo "SPOKE Import Process Complete"
echo "Final statistics:"
cat /home/ubuntu/spoke/arangoimport/logs/final_stats_${TIMESTAMP}.log
echo "==================================================================="
