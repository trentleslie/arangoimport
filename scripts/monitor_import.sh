#!/bin/bash

# Monitor import progress by querying node and edge counts
# Uses direct curl to ArangoDB API for best performance

echo "SPOKE Import Progress Monitor"
echo "=============================="
echo "Checking node and edge counts every 30 seconds..."
echo ""

while true; do
  # Get current timestamp
  TIMESTAMP=$(date +"%Y-%m-%d %H:%M:%S")
  
  # Query ArangoDB for node and edge counts
  RESULT=$(curl -s -X POST \
    --data-binary '{"query":"RETURN { nodes: LENGTH(Nodes), edges: LENGTH(Edges) }"}' \
    -H "Content-Type: application/json" \
    -u root:ph \
    http://localhost:8529/_db/spokeV6/_api/cursor)
  
  # Extract counts using grep and sed (basic parsing)
  NODES=$(echo $RESULT | grep -o '"nodes":[0-9]*' | sed 's/"nodes"://')
  EDGES=$(echo $RESULT | grep -o '"edges":[0-9]*' | sed 's/"edges"://')
  
  # Print with timestamp
  echo "$TIMESTAMP - Nodes: $NODES, Edges: $EDGES"
  
  # Sleep for 30 seconds
  sleep 30
done
