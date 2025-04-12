"""Configuration for import process."""

from dataclasses import dataclass, field
from typing import Dict, Any, List, Literal, Optional, Callable, Set
from enum import Enum, auto

class ValidationLevel(Enum):
    """Level of validation to perform during import."""
    NONE = auto()    # No validation
    BASIC = auto()   # Basic structure validation
    STRICT = auto()  # Strict validation including references

@dataclass
class QualityMetrics:
    """Metrics tracking import quality.
    
    Attributes:
        total_documents: Total number of documents processed
        valid_documents: Number of valid documents
        invalid_documents: Number of invalid documents
        duplicates_found: Number of duplicates found
        missing_references: Number of missing references
        validation_errors: List of validation error messages
        type_stats: Statistics per document type
    """
    total_documents: int = 0
    valid_documents: int = 0
    invalid_documents: int = 0
    duplicates_found: int = 0
    missing_references: int = 0
    validation_errors: List[str] = field(default_factory=list)
    type_stats: Dict[str, Dict[str, int]] = field(default_factory=dict)
    
    def track_document(self, doc_type: str, is_valid: bool) -> None:
        """Track a document in type-specific stats."""
        if doc_type not in self.type_stats:
            self.type_stats[doc_type] = {
                "total": 0,
                "valid": 0,
                "invalid": 0,
                "duplicates": 0
            }
        
        stats = self.type_stats[doc_type]
        stats["total"] += 1
        stats["valid" if is_valid else "invalid"] += 1
    
    @property
    def validity_ratio(self) -> float:
        """Calculate ratio of valid to total documents."""
        return self.valid_documents / self.total_documents if self.total_documents > 0 else 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert metrics to dictionary format."""
        return {
            "total_documents": self.total_documents,
            "valid_documents": self.valid_documents,
            "invalid_documents": self.invalid_documents,
            "duplicates_found": self.duplicates_found,
            "missing_references": self.missing_references,
            "validity_ratio": self.validity_ratio,
            "validation_errors": self.validation_errors[:100],  # Limit error list
            "type_stats": self.type_stats
        }

@dataclass
class ImportConfig:
    """Configuration for import process.
    
    Attributes:
        on_duplicate: Strategy for handling duplicate documents ('replace', 'update', or 'ignore')
        dedup_enabled: Whether to enable deduplication
        validate_nodes: Whether to validate node documents
        transform_enabled: Whether to enable data transformations
        batch_size: Size of batches for bulk operations
        error_threshold: Maximum error rate before failing
        node_type_configs: Configuration for specific node types
        skip_missing_refs: Skip edges with missing node references
        log_level: Logging level for import operations
        validation_level: Level of validation to perform
        pre_validate_hook: Optional function to run before validation
        post_validate_hook: Optional function to run after validation
        error_handler: Optional function to handle validation errors
        reference_collections: Collections to check for references
    """
    on_duplicate: Literal["replace", "update", "ignore", "error"] = "replace"
    dedup_enabled: bool = True
    validate_nodes: bool = True
    transform_enabled: bool = True
    batch_size: int = 1000
    error_threshold: float = 0.01  # Max error rate before failing
    node_type_configs: Dict[str, "NodeTypeConfig"] = field(default_factory=dict)
    skip_missing_refs: bool = True  # Skip edges with missing node references
    log_level: str = "INFO"
    validation_level: ValidationLevel = ValidationLevel.STRICT
    pre_validate_hook: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = None
    post_validate_hook: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = None
    error_handler: Optional[Callable[[str, Dict[str, Any]], None]] = None
    reference_collections: Dict[str, str] = field(default_factory=lambda: {
        "nodes": "nodes",
        "edges": "edges"
    })
    _metrics: QualityMetrics = field(default_factory=QualityMetrics)
    _seen_keys: Set[str] = field(default_factory=set)
    
    def track_error(self, error: str, document: Optional[Dict[str, Any]] = None) -> None:
        """Track a validation or import error.
        
        Args:
            error: Error message
            document: Optional document that caused the error
        """
        self._metrics.validation_errors.append(error)
        self._metrics.invalid_documents += 1
        
        if document and "type" in document:
            self._metrics.track_document(document["type"], False)
        
        if self.error_handler:
            self.error_handler(error, document or {})
    
    def track_document(self, document: Dict[str, Any], is_valid: bool) -> None:
        """Track a processed document.
        
        Args:
            document: The document being processed
            is_valid: Whether the document is valid
        """
        self._metrics.total_documents += 1
        if is_valid:
            self._metrics.valid_documents += 1
        else:
            self._metrics.invalid_documents += 1
            
        if "type" in document:
            self._metrics.track_document(document["type"], is_valid)
            
    def track_duplicate(self, key: str, doc_type: Optional[str] = None) -> None:
        """Track a duplicate document.
        
        Args:
            key: Document key that was duplicated
            doc_type: Optional document type for type-specific stats
        """
        self._metrics.duplicates_found += 1
        
        if doc_type and doc_type in self._metrics.type_stats:
            self._metrics.type_stats[doc_type]["duplicates"] += 1
        
    def track_missing_reference(self, ref_type: str, ref_key: str) -> None:
        """Track a missing reference.
        
        Args:
            ref_type: Type of reference (node, edge)
            ref_key: Key that was not found
        """
        self._metrics.missing_references += 1
        self.track_error(f"Missing {ref_type} reference: {ref_key}")
        
    def get_metrics(self) -> QualityMetrics:
        """Get current quality metrics.
        
        Returns:
            QualityMetrics: Current import metrics
        """
        return self._metrics
    
    def is_duplicate(self, key: str, doc_type: Optional[str] = None) -> bool:
        """Check if a key has been seen before.
        
        Args:
            key: Document key to check
            doc_type: Optional document type for type-specific tracking
            
        Returns:
            bool: True if key is duplicate
        """
        if not self.dedup_enabled:
            return False
            
        if key in self._seen_keys:
            self.track_duplicate(key, doc_type)
            return True
            
        self._seen_keys.add(key)
        return False

@dataclass
class NodeTypeConfig:
    """Configuration for specific node types."""
    required_fields: List[str]
    unique_fields: List[str]
    property_types: Dict[str, type]
    transform_rules: Dict[str, Any]
    dedup_fields: List[str]

# Default configurations for different node types
DEFAULT_CONFIGS = {
    "Gene": NodeTypeConfig(
        required_fields=["id", "properties.name", "properties.organism"],
        unique_fields=["properties.name", "properties.organism"],
        property_types={
            "name": str,
            "organism": str,
            "ensembl": str,
        },
        transform_rules={
            "ensembl": lambda x: str(x) if x else "",
            "name": str.lower,
        },
        dedup_fields=["name", "organism"]
    ),
    "Protein": NodeTypeConfig(
        required_fields=["id", "properties.name", "properties.organism"],
        unique_fields=["properties.name", "properties.organism"],
        property_types={
            "name": str,
            "organism": str,
            "uniprot": str,
        },
        transform_rules={
            "uniprot": lambda x: str(x) if x else "",
            "name": str.lower,
        },
        dedup_fields=["name", "organism"]
    ),
    "Compound": NodeTypeConfig(
        required_fields=["id", "properties.name", "properties.inchikey"],
        unique_fields=["properties.inchikey"],
        property_types={
            "name": str,
            "inchikey": str,
            "smiles": str,
        },
        transform_rules={
            "inchikey": str.upper,
            "smiles": str,
        },
        dedup_fields=["inchikey"]
    ),
}
