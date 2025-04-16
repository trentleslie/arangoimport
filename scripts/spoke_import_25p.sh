#!/bin/bash

# SPOKE Import Script with 25 Processors
# This script runs a fresh import with 25 processors and
# focuses only on edge batches divisible by 100 for enhanced logging

echo "==================================================================="
echo "SPOKE Edge-Focused Import with 25 PROCESSORS"
echo "Starting at $(date)"
echo "==================================================================="

# Set environment variables for convenience
export ARANGO_USERNAME=root
export ARANGO_PASSWORD=ph

# System resource check
echo "System resource check before import:"
echo "Memory usage:"
free -h
echo "Disk space:"
df -h /

# Create timestamp for log files
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
mkdir -p /home/ubuntu/spoke/arangoimport/logs

# Start import with 25 processors and edge-focused logging
echo "Starting SPOKE import with 25 processors and edge-focused logging..."
echo "Import started at $(date)" > /home/ubuntu/spoke/arangoimport/logs/import_${TIMESTAMP}.log

# Create a background process to monitor database stats every 5 minutes
(
  while true; do
    echo "======== DATABASE STATS at $(date) ========" >> /home/ubuntu/spoke/arangoimport/logs/db_stats_${TIMESTAMP}.log
    ~/.local/bin/poetry run python -m src.arangoimport.cli --log-level INFO query-db --host localhost --port 8529 --username root --password ph --db-name spokeV6 --query "RETURN { nodes: LENGTH(Nodes), edges: LENGTH(Edges) }" >> /home/ubuntu/spoke/arangoimport/logs/db_stats_${TIMESTAMP}.log 2>&1
    echo "" >> /home/ubuntu/spoke/arangoimport/logs/db_stats_${TIMESTAMP}.log
    sleep 300  # Check every 5 minutes
  done
) &
STATS_PID=$!

# Run the import command with the correct syntax and 25 processors
~/.local/bin/poetry run python -m src.arangoimport.cli --log-level INFO import-data \
  --host localhost \
  --port 8529 \
  --username root \
  --password ph \
  --db-name spokeV6 \
  --overwrite-db \
  --processes 25 \
  --batch-size 1800 \
  /home/ubuntu/spoke/arangoimport/data/edges.jsonl \
  > /home/ubuntu/spoke/arangoimport/logs/import_${TIMESTAMP}.log \
  2> /home/ubuntu/spoke/arangoimport/logs/import_errors_${TIMESTAMP}.log

IMPORT_EXIT_CODE=$?

# Stop the stats checking process
kill $STATS_PID 2>/dev/null || true

# Run final database query to get stats
echo "Import completed at $(date) with exit code: $IMPORT_EXIT_CODE"
echo "Running final database query..."
~/.local/bin/poetry run python -m src.arangoimport.cli --log-level INFO query-db \
  --host localhost \
  --port 8529 \
  --username root \
  --password ph \
  --db-name spokeV6 \
  --query "RETURN { nodes: LENGTH(Nodes), edges: LENGTH(Edges) }" \
  > /home/ubuntu/spoke/arangoimport/logs/final_stats_${TIMESTAMP}.log

echo "==================================================================="
echo "SPOKE Import Process Complete"
echo "Final statistics:"
cat /home/ubuntu/spoke/arangoimport/logs/final_stats_${TIMESTAMP}.log
echo "==================================================================="
