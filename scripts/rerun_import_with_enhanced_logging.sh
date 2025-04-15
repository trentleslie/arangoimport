#!/bin/bash
# Script to re-run SPOKE import with enhanced logging for edge batch analysis
# This script drops the existing database and runs a fresh import with detailed logging

# Set up error handling and enhanced logging
set -e

# Log with timestamp for easier debugging
function log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

log "=== SPOKE Import with Enhanced Logging ==="
log "Starting import process with enhanced batch tracking"

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
log "Created logs directory at $LOGS_DIR"

# Add cleanup function to ensure proper handling of errors
function cleanup() {
    EXIT_CODE=$?
    if [ $EXIT_CODE -ne 0 ]; then
        log "ERROR: Script failed with exit code $EXIT_CODE"
        log "See logs for details in $LOG_FILE"
    fi
    log "Script completed at $(date)"
}

# Register the cleanup function to run on exit
trap cleanup EXIT

# Ensure arango is running
log "Checking ArangoDB status..."
CONTAINER_ID=$(docker ps -q -f name=arangoimport_arangodb)
if [ -z "$CONTAINER_ID" ]; then
    CONTAINER_ID=$(docker ps -q -f name=arangodb)
fi

if [ -z "$CONTAINER_ID" ]; then
    log "Error: ArangoDB container not found!"
    exit 1
fi

log "ArangoDB container found: $CONTAINER_ID"

# Check system resources to ensure we have enough capacity
MEM_AVAIL=$(free -g | grep Mem | awk '{print $7}')
DISK_AVAIL=$(df -h | grep /$ | awk '{print $4}' | sed 's/G//')

log "Available memory: ${MEM_AVAIL}GB"
log "Available disk space: ${DISK_AVAIL}GB"

if [[ $MEM_AVAIL -lt 4 ]]; then
    log "WARNING: Low memory available (${MEM_AVAIL}GB). This may affect import performance."
fi

if [[ ${DISK_AVAIL%.*} -lt 30 ]]; then
    log "WARNING: Low disk space available (${DISK_AVAIL}GB). This may affect import performance."
fi

# Drop the existing database
log "Dropping existing database $DB_NAME..."
DB_DROP_RESULT=$(curl -s -u $DB_USER:$DB_PASS -X DELETE "http://$DB_HOST:$DB_PORT/_api/database/$DB_NAME")
log "Database drop result: $DB_DROP_RESULT"

# Create the database again
log "Creating fresh database $DB_NAME..."
DB_CREATE_RESULT=$(curl -s -u $DB_USER:$DB_PASS -X POST "http://$DB_HOST:$DB_PORT/_api/database" -d "{\"name\":\"$DB_NAME\"}")
log "Database create result: $DB_CREATE_RESULT"

# Verify the database exists before proceeding
log "Verifying database was created successfully..."
DB_CHECK=$(curl -s -u $DB_USER:$DB_PASS "http://$DB_HOST:$DB_PORT/_api/database")
if [[ ! $DB_CHECK =~ $DB_NAME ]]; then
    log "ERROR: Failed to create database $DB_NAME!"
    log "Database list: $DB_CHECK"
    exit 1
fi
log "Database $DB_NAME verified successfully."

# Set up Python path
export PYTHONPATH="$PROJECT_DIR"

# Increase max file descriptors for better performance
ulimit -n 65535 2>/dev/null || log "Could not increase file descriptor limit - continuing anyway"

# Run the import with enhanced logging
log "Starting SPOKE import with enhanced logging..."
cd "$PROJECT_DIR"

# Get timestamp for log file name
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="$LOGS_DIR/import_${TIMESTAMP}.log"
ERROR_LOG_FILE="$LOGS_DIR/import_errors_${TIMESTAMP}.log"

# Run the import using poetry - reduce the number of processes to 12 for more stability
log "Detailed log will be saved to $LOG_FILE"
log "Starting import process with 12 workers..."

# Write to both terminal and logs
(~/.local/bin/poetry run python -m src.arangoimport.cli --log-level "INFO" import-data "$JSONL_FILE" \
    --host "$DB_HOST" \
    --port "$DB_PORT" \
    --db-name "$DB_NAME" \
    --username "$DB_USER" \
    --password "$DB_PASS" \
    --processes 12 2>&1 | tee -a "$LOG_FILE" "$ERROR_LOG_FILE") || {
    log "ERROR: Import process failed! See logs for details."
    exit 1
}

# Check if we have any enhanced logging data
log "Import process completed. Checking for enhanced logging data..."

# Setup watchdog to monitor progress - runs every 5 minutes to check database stats
log "Starting a background watchdog process to monitor database stats every 5 minutes..."
cmd="while true; do date >> $LOGS_DIR/watchdog_${TIMESTAMP}.log; \
    echo 'DB Stats:' >> $LOGS_DIR/watchdog_${TIMESTAMP}.log; \
    ~/.local/bin/poetry run python -m src.arangoimport.cli --log-level INFO query-db \
    --host $DB_HOST --port $DB_PORT --username $DB_USER --password $DB_PASS \
    --db-name $DB_NAME --query 'RETURN { nodes: LENGTH(Nodes), edges: LENGTH(Edges) }' \
    >> $LOGS_DIR/watchdog_${TIMESTAMP}.log 2>&1; \
    sleep 300; done"
nohup bash -c "$cmd" > /dev/null 2>&1 &
WATCHDOG_PID=$!
log "Watchdog process started (PID: $WATCHDOG_PID)"

# Run the analysis script after completion
log "Running edge batch analysis..."
~/.local/bin/poetry run python "$SCRIPT_DIR/analyze_missing_edges.py" | tee -a "$LOG_FILE"

# Kill watchdog process
kill $WATCHDOG_PID 2>/dev/null || true
log "Process completed successfully at $(date)"
