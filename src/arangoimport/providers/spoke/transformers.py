"""
SPOKE-specific data transformations.

This module contains transformation functions specific to SPOKE data.
These functions convert SPOKE data into the format expected by ArangoDB.
"""
from typing import Dict, Any, Optional
import json


def transform_node(node: Dict[str, Any]) -> Dict[str, Any]:
    """
    Transform a SPOKE node to ArangoDB format.
    
    Args:
        node: Original node data
        
    Returns:
        Transformed node data
    """
    # Placeholder for node transformation logic to be migrated from original code
    # This will handle SPOKE-specific node structure conversion
    result = node.copy()
    
    # Ensure the node has a _key field
    if "id" in node and "_key" not in node:
        result["_key"] = str(node["id"])
    
    return result


def transform_edge(edge: Dict[str, Any]) -> Dict[str, Any]:
    """
    Transform a SPOKE edge to ArangoDB format.
    
    Args:
        edge: Original edge data
        
    Returns:
        Transformed edge data
    """
    # Placeholder for edge transformation logic to be migrated from original code
    # This will handle SPOKE-specific edge structure and convert to _from and _to fields
    result = edge.copy()
    
    # Handle from/to references based on SPOKE format
    if "start" in edge and isinstance(edge["start"], dict) and "id" in edge["start"]:
        result["_from"] = f"Nodes/{edge['start']['id']}"
        
    if "end" in edge and isinstance(edge["end"], dict) and "id" in edge["end"]:
        result["_to"] = f"Nodes/{edge['end']['id']}"
    
    # Add edge label/type if available
    if "label" in edge:
        result["type"] = edge["label"]
    
    return result


def get_node_key(node: Dict[str, Any]) -> Optional[str]:
    """
    Extract the key from a SPOKE node.
    
    Args:
        node: Node data
        
    Returns:
        Node key as string, or None if not found
    """
    # Handle various ways of storing IDs in SPOKE data
    if "id" in node:
        return str(node["id"])
    
    if "_id" in node:
        return str(node["_id"])
    
    return None


def get_edge_key(edge: Dict[str, Any]) -> Optional[str]:
    """
    Generate a unique key for a SPOKE edge.
    
    Args:
        edge: Edge data
        
    Returns:
        Generated edge key, or None if required fields are missing
    """
    # Create a composite key from source, target, and label
    src_id = None
    tgt_id = None
    
    if "start" in edge and isinstance(edge["start"], dict) and "id" in edge["start"]:
        src_id = str(edge["start"]["id"])
    
    if "end" in edge and isinstance(edge["end"], dict) and "id" in edge["end"]:
        tgt_id = str(edge["end"]["id"])
    
    label = edge.get("label", "")
    
    if src_id and tgt_id:
        return f"{src_id}_{label}_{tgt_id}"
    
    return None
