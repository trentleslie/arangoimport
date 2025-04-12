"""Document validation functionality."""

from typing import Dict, Any, Optional, Tuple
import logging
from .config import ImportConfig, ValidationLevel
from .quality import QualityMonitor

logger = logging.getLogger(__name__)

def validate_node_document(
    doc: Dict[str, Any], config: ImportConfig
) -> Tuple[bool, Optional[str]]:
    """Validate a node document.
    
    Args:
        doc: Document to validate
        config: Import configuration
        
    Returns:
        Tuple[bool, Optional[str]]: (is_valid, error_message)
    """
    # Apply pre-validation hook if configured
    if config.pre_validate_hook:
        try:
            doc = config.pre_validate_hook(doc)
        except Exception as e:
            return False, f"Pre-validation hook failed: {str(e)}"
    
    # Basic validation
    if not isinstance(doc, dict):
        return False, "Document must be a dictionary"
        
    if "type" not in doc:
        return False, "Document must have a 'type' field"
        
    # Get node type configuration
    node_type = doc.get("type")
    type_config = config.node_type_configs.get(node_type)
    
    # Skip detailed validation if no type config and not in strict mode
    if not type_config and config.validation_level != ValidationLevel.STRICT:
        return True, None
        
    # Validate required fields
    if type_config:
        for field in type_config.required_fields:
            parts = field.split(".")
            value = doc
            for part in parts:
                value = value.get(part, {}) if isinstance(value, dict) else None
                if value is None:
                    return False, f"Missing required field: {field}"
                    
        # Validate property types
        properties = doc.get("properties", {})
        for prop, expected_type in type_config.property_types.items():
            if prop in properties:
                value = properties[prop]
                if not isinstance(value, expected_type):
                    return False, f"Invalid type for property {prop}: expected {expected_type.__name__}, got {type(value).__name__}"
                    
    # Apply post-validation hook if configured
    if config.post_validate_hook:
        try:
            doc = config.post_validate_hook(doc)
        except Exception as e:
            return False, f"Post-validation hook failed: {str(e)}"
            
    return True, None

def validate_edge_document(
    doc: Dict[str, Any], config: ImportConfig, quality_monitor: Optional[QualityMonitor] = None
) -> Tuple[bool, Optional[str]]:
    """Validate an edge document.
    
    Args:
        doc: Document to validate
        config: Import configuration
        quality_monitor: Optional quality monitor for reference tracking
        
    Returns:
        Tuple[bool, Optional[str]]: (is_valid, error_message)
    """
    # Apply pre-validation hook if configured
    if config.pre_validate_hook:
        try:
            doc = config.pre_validate_hook(doc)
        except Exception as e:
            return False, f"Pre-validation hook failed: {str(e)}"
    
    # Basic validation
    if not isinstance(doc, dict):
        return False, "Document must be a dictionary"
        
    if "type" not in doc:
        return False, "Document must have a 'type' field"
        
    if doc["type"] not in ["edge", "relationship"]:
        return False, f"Invalid document type for edge: {doc['type']}"
        
    # Validate edge structure
    if "_from" not in doc or "_to" not in doc:
        return False, "Edge document must have _from and _to fields"
        
    if not isinstance(doc["_from"], str) or not isinstance(doc["_to"], str):
        return False, "_from and _to must be strings"
        
    if "/" not in doc["_from"] or "/" not in doc["_to"]:
        return False, "_from and _to must contain collection prefix"
        
    # Validate references if quality monitor is provided
    if quality_monitor and config.validation_level == ValidationLevel.STRICT:
        if not quality_monitor.validate_references(doc):
            if not config.skip_missing_refs:
                return False, "Invalid edge references"
                
    # Validate properties if present
    if "properties" in doc and not isinstance(doc["properties"], dict):
        return False, "Edge properties must be a dictionary"
        
    # Apply post-validation hook if configured
    if config.post_validate_hook:
        try:
            doc = config.post_validate_hook(doc)
        except Exception as e:
            return False, f"Post-validation hook failed: {str(e)}"
            
    return True, None

def validate_document(
    doc: Dict[str, Any], config: ImportConfig, quality_monitor: Optional[QualityMonitor] = None
) -> Tuple[bool, Optional[str]]:
    """Validate a document based on its type.
    
    Args:
        doc: Document to validate
        config: Import configuration
        quality_monitor: Optional quality monitor for reference tracking
        
    Returns:
        Tuple[bool, Optional[str]]: (is_valid, error_message)
    """
    if config.validation_level == ValidationLevel.NONE:
        return True, None
        
    try:
        # Basic type validation
        if not isinstance(doc, dict):
            return False, "Document must be a dictionary"
            
        doc_type = doc.get("type")
        if not doc_type:
            return False, "Document must have a type"
            
        # Validate based on document type
        if doc_type == "node":
            is_valid, error = validate_node_document(doc, config)
        elif doc_type in ["edge", "relationship"]:
            is_valid, error = validate_edge_document(doc, config, quality_monitor)
        else:
            return False, f"Unknown document type: {doc_type}"
            
        # Track validation result
        config.track_document(doc, is_valid)
        
        # Track error if validation failed
        if not is_valid and error:
            config.track_error(error, doc)
            
        return is_valid, error
        
    except Exception as e:
        error_msg = f"Validation error: {str(e)}"
        config.track_error(error_msg, doc)
        return False, error_msg
