"""Quality monitoring and validation for import process."""

from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional, Set
import logging
from .config import ImportConfig, QualityMetrics

logger = logging.getLogger(__name__)

@dataclass
class QualityReport:
    """Report on import quality and validation results.
    
    Attributes:
        metrics: Quality metrics from the import
        node_validation: Validation results for nodes
        edge_validation: Validation results for edges
        reference_validation: Results of reference validation
        recommendations: List of recommendations for improvement
    """
    metrics: QualityMetrics
    node_validation: Dict[str, Dict[str, int]] = field(default_factory=dict)
    edge_validation: Dict[str, Dict[str, int]] = field(default_factory=dict)
    reference_validation: Dict[str, Dict[str, int]] = field(default_factory=dict)
    recommendations: List[str] = field(default_factory=list)
    
    def add_recommendation(self, recommendation: str) -> None:
        """Add a recommendation to the report."""
        self.recommendations.append(recommendation)
        
    def analyze_metrics(self) -> None:
        """Analyze metrics and generate recommendations."""
        # Check validity ratio
        if self.metrics.validity_ratio < 0.95:
            self.add_recommendation(
                f"Low document validity ratio ({self.metrics.validity_ratio:.2%}). "
                "Consider reviewing validation rules and document structure."
            )
            
        # Check duplicate rate
        dup_rate = self.metrics.duplicates_found / self.metrics.total_documents if self.metrics.total_documents > 0 else 0
        if dup_rate > 0.05:
            self.add_recommendation(
                f"High duplicate rate ({dup_rate:.2%}). "
                "Consider reviewing deduplication strategy and key generation."
            )
            
        # Check missing references
        if self.metrics.missing_references > 0:
            self.add_recommendation(
                f"Found {self.metrics.missing_references} missing references. "
                "Ensure all referenced nodes exist before creating edges."
            )
            
        # Analyze type-specific issues
        for doc_type, stats in self.metrics.type_stats.items():
            invalid_rate = stats["invalid"] / stats["total"] if stats["total"] > 0 else 0
            if invalid_rate > 0.05:
                self.add_recommendation(
                    f"High invalid rate for {doc_type} ({invalid_rate:.2%}). "
                    "Review validation rules for this document type."
                )
                
    def to_dict(self) -> Dict[str, Any]:
        """Convert report to dictionary format."""
        self.analyze_metrics()
        return {
            "metrics": self.metrics.to_dict(),
            "node_validation": self.node_validation,
            "edge_validation": self.edge_validation,
            "reference_validation": self.reference_validation,
            "recommendations": self.recommendations
        }

class QualityMonitor:
    """Monitor import quality and generate reports."""
    
    def __init__(self, config: ImportConfig):
        """Initialize quality monitor.
        
        Args:
            config: Import configuration
        """
        self.config = config
        self._seen_refs: Set[str] = set()
        
    def validate_references(self, doc: Dict[str, Any]) -> bool:
        """Validate document references.
        
        Args:
            doc: Document to validate
            
        Returns:
            bool: True if all references are valid
        """
        if doc.get("type") != "edge":
            return True
            
        # Check _from and _to references
        from_ref = doc.get("_from")
        to_ref = doc.get("_to")
        
        if not from_ref or not to_ref:
            self.config.track_error("Edge missing _from or _to reference", doc)
            return False
            
        # Track missing references
        if from_ref not in self._seen_refs:
            self.config.track_missing_reference("node", from_ref)
            
        if to_ref not in self._seen_refs:
            self.config.track_missing_reference("node", to_ref)
            
        return from_ref in self._seen_refs and to_ref in self._seen_refs
        
    def track_reference(self, ref: str) -> None:
        """Track a valid reference.
        
        Args:
            ref: Reference to track
        """
        self._seen_refs.add(ref)
        
    def generate_report(self) -> QualityReport:
        """Generate quality report.
        
        Returns:
            QualityReport: Report of import quality
        """
        return QualityReport(metrics=self.config.get_metrics())
