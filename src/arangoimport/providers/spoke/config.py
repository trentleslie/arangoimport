"""
SPOKE-specific configuration defaults and settings.

This module contains the default configuration and settings specific to
SPOKE data imports.
"""
from typing import Dict, Any


DEFAULT_SPOKE_CONFIG = {
    # Database configuration
    "database": {
        "db_name": "spokeV6",
        "nodes_collection": "Nodes",
        "edges_collection": "Edges",
        "create_db_if_missing": True,
        "overwrite_db": False,
    },
    
    # Import settings
    "import": {
        "batch_size": 1000,
        "stop_on_error": False,
        "skip_missing_refs": True,
        "preserve_id_fields": True,
    },
    
    # Schema settings
    "schema": {
        "node_id_field": "id",
        "edge_from_field": "start.id",
        "edge_to_field": "end.id",
        "edge_label_field": "label",
    }
}


def get_spoke_config() -> Dict[str, Any]:
    """
    Get the default SPOKE configuration.
    
    Returns:
        Default configuration dictionary for SPOKE imports
    """
    return DEFAULT_SPOKE_CONFIG.copy()


def merge_with_defaults(user_config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Merge user configuration with default SPOKE configuration.
    
    Args:
        user_config: User-provided configuration
        
    Returns:
        Merged configuration
    """
    config = get_spoke_config()
    
    # Merge database settings
    if "database" in user_config:
        config["database"].update(user_config["database"])
    
    # Merge import settings
    if "import" in user_config:
        config["import"].update(user_config["import"])
    
    # Merge schema settings
    if "schema" in user_config:
        config["schema"].update(user_config["schema"])
    
    # Add any other user settings
    for key, value in user_config.items():
        if key not in config:
            config[key] = value
    
    return config
