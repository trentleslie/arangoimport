#!/bin/bash
# Script to re-run SPOKE import with enhanced logging for edge batch analysis
# This script drops the existing database and runs a fresh import with detailed logging

# Set up error handling
set -e
echo "=== SPOKE Import with Enhanced Logging ==="
echo "Starting at $(date)"

# Database credentials
DB_USER="root"
DB_PASS="ph"
DB_NAME="spokeV6"
DB_HOST="localhost"
DB_PORT="8529"

# Paths
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
LOGS_DIR="$PROJECT_DIR/logs"
JSONL_FILE="/home/ubuntu/spoke/data/spokeV6.jsonl"

# Create logs directory if it doesn't exist
mkdir -p "$LOGS_DIR"

# Ensure arango is running
echo "Checking ArangoDB status..."
CONTAINER_ID=$(docker ps -q -f name=arangoimport_arangodb)
if [ -z "$CONTAINER_ID" ]; then
    CONTAINER_ID=$(docker ps -q -f name=arangodb)
fi

if [ -z "$CONTAINER_ID" ]; then
    echo "Error: ArangoDB container not found!"
    exit 1
fi

echo "ArangoDB container found: $CONTAINER_ID"

# Drop the existing database
echo "Dropping existing database $DB_NAME..."
curl -u $DB_USER:$DB_PASS -X DELETE "http://$DB_HOST:$DB_PORT/_api/database/$DB_NAME"
echo "Database dropped."

# Create the database again
echo "Creating fresh database $DB_NAME..."
curl -u $DB_USER:$DB_PASS -X POST "http://$DB_HOST:$DB_PORT/_api/database" -d "{\"name\":\"$DB_NAME\"}"
echo "Database created."

# Set up Python path
export PYTHONPATH="$PROJECT_DIR"

# Run the import with enhanced logging
echo "Starting SPOKE import with enhanced logging at $(date)..."
cd "$PROJECT_DIR"

# Get timestamp for log file name
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="$LOGS_DIR/import_${TIMESTAMP}.log"

# Run the import using poetry with the correct 'import-data' subcommand
echo "Detailed log will be saved to $LOG_FILE"
~/.local/bin/poetry run python -m src.arangoimport.cli --log-level "INFO" import-data "$JSONL_FILE" \
    --host "$DB_HOST" \
    --port "$DB_PORT" \
    --db-name "$DB_NAME" \
    --username "$DB_USER" \
    --password "$DB_PASS" \
    --processes 16 \
    | tee -a "$LOG_FILE"

# Run the analysis script
echo "Import completed. Running analysis..."
~/.local/bin/poetry run python "$SCRIPT_DIR/analyze_missing_edges.py"

echo "Process completed at $(date)"
