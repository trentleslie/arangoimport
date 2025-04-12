# SPOKE Backup and Deployment Guide

This document provides instructions for creating a backup of the SPOKE ArangoDB database and deploying it on another system using the `spokev6_docker` deployment system.

## Overview

The SPOKE database can be backed up into a portable archive file (`spoke_volumes_backup.tar.gz`) that can be used with the deployment scripts in the `spokev6_docker` directory. This enables easy migration and deployment of the SPOKE database to other systems.

## Prerequisites

- A fully imported SPOKE database in ArangoDB
- Docker and Docker Compose installed
- Sufficient disk space (at least 50GB)
- Root/sudo access on the host system

## Creating the Backup Archive

Follow these steps to create a backup of your SPOKE ArangoDB database that's compatible with the deployment system:

### 1. Wait for Import Completion

First, confirm that the import process has fully completed by checking the node and edge counts:

```bash
~/.local/bin/poetry run python -m arangoimport.cli query-db \
  --host localhost \
  --port 8529 \
  --username root \
  --password ph \
  --db-name spokeV6 \
  --query "RETURN { nodes: LENGTH(Nodes), edges: LENGTH(Edges) }"
```

The import is complete when:
- Nodes reach approximately 43.6 million
- Edges reach approximately 184.2 million

### 2. Stop the ArangoDB Container

To ensure data consistency during backup, stop the ArangoDB container:

```bash
cd /home/ubuntu/spoke/arangoimport
docker-compose stop arangodb
```

### 3. Create the Proper Directory Structure

Create a temporary directory structure to match what the deployment script expects:

```bash
# Create temporary directories to match Docker volume structure
mkdir -p /tmp/spokev6_backup/arangodb_data
mkdir -p /tmp/spokev6_backup/arangodb_apps

# Copy data with proper permissions
sudo cp -a /home/ubuntu/spoke/arangoimport/arangodb_data/. /tmp/spokev6_backup/arangodb_data/
sudo cp -a /home/ubuntu/spoke/arangoimport/arangodb_apps/. /tmp/spokev6_backup/arangodb_apps/
```

### 4. Create the Backup Archive

Create the tar.gz archive with the correct structure:

```bash
cd /tmp
sudo tar -czvf spoke_volumes_backup.tar.gz spokev6_backup
```

### 5. Move the Backup to Deployment Directory

Move the archive to the deployment directory:

```bash
sudo mv /tmp/spoke_volumes_backup.tar.gz /home/ubuntu/spoke/spokev6_docker/
```

### 6. Clean Up and Restart

Clean up temporary files and restart the ArangoDB container:

```bash
# Remove temporary files
sudo rm -rf /tmp/spokev6_backup

# Restart the ArangoDB container
cd /home/ubuntu/spoke/arangoimport
docker-compose start arangodb
```

## Backup Size and Storage Considerations

The complete SPOKE database backup file will be approximately 25GB. Ensure you have:
- Sufficient disk space for the original database (~25GB)
- Enough space for the temporary files during backup (~25GB)
- Enough space for the final archive file (~25GB)

## Deploying from the Backup

To deploy the SPOKE database on a new system using the backup:

1. Copy the `spokev6_docker` directory to the target system
2. Ensure the `spoke_volumes_backup.tar.gz` file is in the `spokev6_docker` directory
3. Run the setup script:

```bash
cd /path/to/spokev6_docker
./setup.sh
```

The setup script will:
1. Check for system requirements
2. Install Docker if needed
3. Create Docker volumes
4. Extract the backup data to the volumes
5. Start the ArangoDB container

## Troubleshooting

### Insufficient Disk Space

If you encounter disk space issues during backup:
- Use the `df -h` command to check available space
- Consider using a different partition with more free space
- Remove unnecessary files or old backups

### Permission Issues

If you encounter permission issues:
- Ensure you have sudo/root access
- Check the file ownership of the ArangoDB data directory
- Use `sudo` when needed for operations requiring elevated privileges

### Container Won't Stop

If the ArangoDB container won't stop gracefully:
- Try `docker-compose down` instead
- As a last resort, use `docker kill <container-id>`

## Verification

To verify your backup is properly created:

```bash
# Check the archive exists and has the expected size
ls -lh /home/ubuntu/spoke/spokev6_docker/spoke_volumes_backup.tar.gz

# Test the archive integrity
tar -tf /home/ubuntu/spoke/spokev6_docker/spoke_volumes_backup.tar.gz | head
```

## Customization

If your ArangoDB data is stored in a non-standard location, adjust the paths in the backup commands accordingly:

```bash
# Example for custom data location
sudo cp -a /custom/path/to/arangodb_data/. /tmp/spokev6_backup/arangodb_data/
sudo cp -a /custom/path/to/arangodb_apps/. /tmp/spokev6_backup/arangodb_apps/
```
