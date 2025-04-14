#!/bin/bash
#
# SPOKE Database Backup Script
# 
# This script automates the backup process for the SPOKE ArangoDB database
# as detailed in /home/ubuntu/spoke/arangoimport/docs/case_studies/spoke/backup_and_deployment.md
#
# It creates a backup compatible with the existing spokev6_docker deployment system.
#
# Usage: ./backup_spoke_database.sh [OUTPUT_DIR]
#
# If OUTPUT_DIR is not specified, the backup will be placed in /home/ubuntu/spoke/spokev6_docker/

set -e  # Exit on any error

# Default output directory
OUTPUT_DIR="${1:-/home/ubuntu/spoke/spokev6_docker}"
TEMP_DIR="/tmp/spokev6_backup"
BACKUP_FILENAME="spoke_volumes_backup.tar.gz"
ARANGO_USERNAME="root"
ARANGO_PASSWORD="ph"
ARANGO_DB="spokeV6"

echo "======================= SPOKE DATABASE BACKUP TOOL ======================="
echo "This script will create a backup of the SPOKE ArangoDB database"
echo "that can be used with the spokev6_docker deployment system."
echo ""

# Function to check available disk space
check_disk_space() {
    local required_gb=75  # 25GB x 3 (original + temp + archive)
    local available_kb=$(df -k /tmp | awk 'NR==2 {print $4}')
    local available_gb=$(echo "scale=2; $available_kb/1024/1024" | bc)
    
    echo "Checking available disk space..."
    echo "Available space: ${available_gb}GB (minimum required: ${required_gb}GB)"
    
    if (( $(echo "$available_gb < $required_gb" | bc -l) )); then
        echo "WARNING: You may not have enough disk space for the backup process."
        echo "The backup process requires approximately 75GB of free space."
        read -p "Continue anyway? (y/n): " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            echo "Backup aborted."
            exit 1
        fi
    else
        echo "Sufficient disk space available."
    fi
}

# Function to verify import completion
verify_import_completion() {
    echo "Verifying import completion..."
    
    result=$(~/.local/bin/poetry run python -m arangoimport.cli query-db \
        --host localhost \
        --port 8529 \
        --username $ARANGO_USERNAME \
        --password $ARANGO_PASSWORD \
        --db-name $ARANGO_DB \
        --query "RETURN { nodes: LENGTH(Nodes), edges: LENGTH(Edges) }")
    
    echo "Current database statistics:"
    echo "$result"
    
    # Extract node and edge counts using grep and cut
    nodes=$(echo "$result" | grep -o '"nodes": [0-9]*' | cut -d' ' -f2 | tr -d ',')
    edges=$(echo "$result" | grep -o '"edges": [0-9]*' | cut -d' ' -f2 | tr -d ',}')
    
    echo "Nodes: $nodes"
    echo "Edges: $edges"
    
    # Check if counts are reasonable
    if [ -z "$nodes" ] || [ -z "$edges" ] || [ "$nodes" -lt 40000000 ] || [ "$edges" -lt 180000000 ]; then
        echo "WARNING: The node or edge counts suggest the import may not be complete."
        echo "Expected counts: ~43.6 million nodes and ~184.2 million edges."
        read -p "Continue anyway? (y/n): " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            echo "Backup aborted."
            exit 1
        fi
    else
        echo "Import appears to be complete. Proceeding with backup."
    fi
}

# Function to check if ArangoDB is running in Docker
check_arango_container() {
    echo "Checking ArangoDB container status..."
    
    container_id=$(docker ps -q -f name=arangoimport_arangodb)
    
    if [ -z "$container_id" ]; then
        container_id=$(docker ps -q -f name=arangodb)
    fi
    
    if [ -z "$container_id" ]; then
        echo "WARNING: No running ArangoDB container found."
        echo "Make sure the ArangoDB container is running before proceeding."
        read -p "Continue anyway? (y/n): " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            echo "Backup aborted."
            exit 1
        fi
    else
        echo "ArangoDB container found: $container_id"
    fi
}

# Function to stop ArangoDB container
stop_arangodb() {
    echo "Stopping ArangoDB container..."
    
    # Try to find and stop the container directly
    container_id=$(docker ps -q -f name=arangoimport_arangodb)
    
    if [ -z "$container_id" ]; then
        container_id=$(docker ps -q -f name=arangodb)
    fi
    
    if [ -n "$container_id" ]; then
        docker stop $container_id
    else
        echo "WARNING: Could not identify ArangoDB container to stop."
        echo "If ArangoDB is running, please stop it manually before continuing."
        read -p "Continue anyway? (y/n): " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            echo "Backup aborted."
            exit 1
        fi
    fi
    
    echo "ArangoDB container stopped."
}

# Function to find ArangoDB data path
find_arango_data_path() {
    # Try the standard path first
    if [ -d "/home/ubuntu/spoke/arangoimport/arangodb_data" ]; then
        ARANGO_DATA_PATH="/home/ubuntu/spoke/arangoimport/arangodb_data"
    else
        # Look for Docker volumes
        volume_path=$(docker volume inspect arangoimport_arangodb_data 2>/dev/null | grep "Mountpoint" | cut -d'"' -f4)
        
        if [ -z "$volume_path" ]; then
            volume_path=$(docker volume inspect arangodb_data 2>/dev/null | grep "Mountpoint" | cut -d'"' -f4)
        fi
        
        if [ -n "$volume_path" ]; then
            ARANGO_DATA_PATH="$volume_path"
        else
            echo "ERROR: Could not find ArangoDB data path."
            echo "Please specify the path manually:"
            read -p "ArangoDB data path: " ARANGO_DATA_PATH
            
            if [ ! -d "$ARANGO_DATA_PATH" ]; then
                echo "ERROR: The specified path does not exist or is not a directory."
                exit 1
            fi
        fi
    fi
    
    echo "Using ArangoDB data path: $ARANGO_DATA_PATH"
    
    # Same for apps data
    if [ -d "/home/ubuntu/spoke/arangoimport/arangodb_apps" ]; then
        ARANGO_APPS_PATH="/home/ubuntu/spoke/arangoimport/arangodb_apps"
    else
        volume_path=$(docker volume inspect arangoimport_arangodb_apps 2>/dev/null | grep "Mountpoint" | cut -d'"' -f4)
        
        if [ -z "$volume_path" ]; then
            volume_path=$(docker volume inspect arangodb_apps 2>/dev/null | grep "Mountpoint" | cut -d'"' -f4)
        fi
        
        if [ -n "$volume_path" ]; then
            ARANGO_APPS_PATH="$volume_path"
        else
            echo "ERROR: Could not find ArangoDB apps path."
            echo "Please specify the path manually:"
            read -p "ArangoDB apps path: " ARANGO_APPS_PATH
            
            if [ ! -d "$ARANGO_APPS_PATH" ]; then
                echo "ERROR: The specified path does not exist or is not a directory."
                exit 1
            fi
        fi
    fi
    
    echo "Using ArangoDB apps path: $ARANGO_APPS_PATH"
}

# Function to create backup directory structure
create_directory_structure() {
    echo "Creating backup directory structure..."
    
    # Remove existing temp directory if it exists
    if [ -d "$TEMP_DIR" ]; then
        echo "Removing existing temporary directory..."
        sudo rm -rf "$TEMP_DIR"
    fi
    
    # Create temporary directories
    mkdir -p "$TEMP_DIR/arangodb_data"
    mkdir -p "$TEMP_DIR/arangodb_apps"
    
    # Copy data with proper permissions
    echo "Copying ArangoDB data (this may take a while)..."
    sudo cp -a "${ARANGO_DATA_PATH}/." "$TEMP_DIR/arangodb_data/"
    
    echo "Copying ArangoDB apps..."
    sudo cp -a "${ARANGO_APPS_PATH}/." "$TEMP_DIR/arangodb_apps/"
    
    echo "Directory structure created successfully."
}

# Function to create backup archive
create_backup_archive() {
    echo "Creating backup archive (this will take a while)..."
    cd /tmp
    sudo tar -czf "$BACKUP_FILENAME" spokev6_backup
    echo "Backup archive created: /tmp/$BACKUP_FILENAME"
}

# Function to move backup to destination
move_backup() {
    echo "Moving backup to destination directory: $OUTPUT_DIR"
    
    # Create destination directory if it doesn't exist
    if [ ! -d "$OUTPUT_DIR" ]; then
        echo "Creating destination directory: $OUTPUT_DIR"
        mkdir -p "$OUTPUT_DIR"
    fi
    
    # Move the backup file
    sudo mv "/tmp/$BACKUP_FILENAME" "$OUTPUT_DIR/"
    echo "Backup moved to: $OUTPUT_DIR/$BACKUP_FILENAME"
}

# Function to clean up and restart
cleanup_and_restart() {
    echo "Cleaning up temporary files..."
    sudo rm -rf "$TEMP_DIR"
    
    echo "Restarting ArangoDB container..."
    
    # Try to find and start the container directly
    container_id=$(docker ps -a -q -f name=arangoimport_arangodb)
    
    if [ -z "$container_id" ]; then
        container_id=$(docker ps -a -q -f name=arangodb)
    fi
    
    if [ -n "$container_id" ]; then
        docker start $container_id
    else
        echo "WARNING: Could not identify ArangoDB container to restart."
        echo "Please start ArangoDB manually."
    fi
    
    echo "ArangoDB container restarted."
}

# Function to verify backup
verify_backup() {
    echo "Verifying backup archive..."
    if [ -f "$OUTPUT_DIR/$BACKUP_FILENAME" ]; then
        size=$(du -h "$OUTPUT_DIR/$BACKUP_FILENAME" | cut -f1)
        echo "Backup file exists: $OUTPUT_DIR/$BACKUP_FILENAME (Size: $size)"
        
        echo "Testing archive integrity..."
        tar -tf "$OUTPUT_DIR/$BACKUP_FILENAME" | head -5
        
        if [ $? -eq 0 ]; then
            echo "Archive integrity check passed."
        else
            echo "WARNING: Archive integrity check failed!"
        fi
    else
        echo "ERROR: Backup file not found at $OUTPUT_DIR/$BACKUP_FILENAME"
        return 1
    fi
}

# Main execution
main() {
    echo "Starting SPOKE database backup process..."
    
    check_disk_space
    check_arango_container
    verify_import_completion
    find_arango_data_path
    stop_arangodb
    create_directory_structure
    create_backup_archive
    move_backup
    cleanup_and_restart
    verify_backup
    
    echo "=================================================================="
    echo "SPOKE database backup completed successfully!"
    echo "Backup file: $OUTPUT_DIR/$BACKUP_FILENAME"
    echo "This backup can be used with the spokev6_docker deployment system."
    echo "=================================================================="
}

# Run the main function
main
