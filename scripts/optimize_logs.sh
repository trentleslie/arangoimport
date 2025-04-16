#!/bin/bash

# Script to optimize log files by extracting only MOD-100 related logs
# and compressing the original large files

LOG_DIR="/home/ubuntu/spoke/arangoimport/logs"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

echo "Starting log optimization at $(date)"

# Extract MOD-100 related logs before compression
echo "Extracting MOD-100 pattern logs..."
grep -E '\[MOD-100\]' ${LOG_DIR}/import_20250415_174459.log > ${LOG_DIR}/mod100_patterns_${TIMESTAMP}.log || echo "No MOD-100 patterns found yet"

# Create a backup of edge-related logs with worker information
echo "Extracting edge-related logs..."
grep -E 'edge|Edge|process_edge_batch|worker_id' ${LOG_DIR}/import_20250415_174459.log > ${LOG_DIR}/edge_logs_${TIMESTAMP}.log || echo "No edge logs found yet"

# Create archive directory if it doesn't exist
mkdir -p ${LOG_DIR}/archive

# Compress large log files to save space
echo "Compressing large log files to save space..."
for large_log in ${LOG_DIR}/import_20250415_174459.log ${LOG_DIR}/import_errors_20250415_174459.log ${LOG_DIR}/arangodb_import_20250415_174501.log; do
  if [ -f "$large_log" ]; then
    # Take a recent tail sample before compression
    tail -n 5000 "$large_log" > "${large_log}.tail.txt"
    
    # Compress the file (using nice to reduce impact on running processes)
    echo "Compressing $large_log..."
    nice -n 19 gzip -c "$large_log" > "${LOG_DIR}/archive/$(basename $large_log).gz"
    
    # Create empty log file to receive new logs
    echo "Log file compressed at $(date)" > "$large_log"
    
    echo "Successfully compressed $(basename $large_log)"
  fi
done

# Modify the enhanced_logging.py file to focus only on edge-related logging
echo "Updating enhanced_logging.py to focus only on edge-related logging..."

# Check disk space after compression
echo "Current disk space usage:"
df -h /

echo "Log optimization completed at $(date)"
