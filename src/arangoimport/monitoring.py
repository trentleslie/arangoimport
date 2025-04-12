"""Monitoring and quality verification for import process."""

import logging
from typing import Dict, Any, List, Optional
from dataclasses import dataclass
from arango.database import Database as ArangoDatabase

logger = logging.getLogger(__name__)

@dataclass
class ImportStats:
    """Statistics for import process."""
    processed: int = 0
    skipped: int = 0
    errors: List[Dict[str, Any]] = None
    
    def __post_init__(self):
        if self.errors is None:
            self.errors = []

    @property
    def error_rate(self) -> float:
        """Calculate error rate."""
        if self.processed + self.skipped == 0:
            return 0
        return len(self.errors) / (self.processed + self.skipped)

class ImportMonitor:
    """Monitor import progress and quality."""
    
    def __init__(self, db: ArangoDatabase):
        self.db = db
        self.stats = ImportStats()
        self.queries = {
            "node_counts": """
                FOR doc IN @@collection
                COLLECT label = doc.labels[0] WITH COUNT INTO count 
                RETURN {label, count}
            """,
            "edge_counts": """
                FOR doc IN @@collection
                COLLECT type = doc.type WITH COUNT INTO count 
                RETURN {type, count}
            """,
            "duplicate_check": """
                FOR doc IN @@collection
                COLLECT identifier = doc.properties.identifier 
                WITH COUNT INTO count
                FILTER count > 1
                RETURN {identifier, count}
            """
        }
        
    def get_node_counts(self) -> Dict[str, int]:
        """Get current node counts by label."""
        try:
            cursor = self.db.aql.execute(
                self.queries["node_counts"],
                bind_vars={"@collection": "Nodes"}
            )
            return {doc["label"]: doc["count"] for doc in cursor}
        except Exception as e:
            logger.error(f"Error getting node counts: {e}")
            return {}
            
    def get_edge_counts(self) -> Dict[str, int]:
        """Get current edge counts by type."""
        try:
            cursor = self.db.aql.execute(
                self.queries["edge_counts"],
                bind_vars={"@collection": "Edges"}
            )
            return {doc["type"]: doc["count"] for doc in cursor}
        except Exception as e:
            logger.error(f"Error getting edge counts: {e}")
            return {}
            
    def check_duplicates(self) -> List[Dict[str, Any]]:
        """Check for duplicate nodes."""
        try:
            cursor = self.db.aql.execute(
                self.queries["duplicate_check"],
                bind_vars={"@collection": "Nodes"}
            )
            return [doc for doc in cursor]
        except Exception as e:
            logger.error(f"Error checking duplicates: {e}")
            return []
            
    def verify_import_quality(
        self, 
        original_counts: Dict[str, int],
        threshold: float = 0.1
    ) -> bool:
        """Verify data quality after import.
        
        Args:
            original_counts: Original node counts by label
            threshold: Maximum acceptable loss rate (0.1 = 10% loss)
            
        Returns:
            bool: True if quality checks pass
        """
        current_counts = self.get_node_counts()
        issues = []
        
        # Check for significant data loss
        for label, count in original_counts.items():
            current = current_counts.get(label, 0)
            if current < count * (1 - threshold):
                issues.append(
                    f"Significant data loss for {label}: {count} -> {current} "
                    f"({((count - current) / count) * 100:.1f}% loss)"
                )
                
        # Check for duplicates
        duplicates = self.check_duplicates()
        if duplicates:
            issues.append(
                f"Found {len(duplicates)} duplicate identifiers"
            )
            
        if issues:
            for issue in issues:
                logger.warning(issue)
            return False
            
        return True
        
    def update_stats(self, stats: ImportStats) -> None:
        """Update import statistics.

        Args:
            stats: Import statistics to update
        """
        try:
            # Update stats
            self.stats.processed += stats.processed
            self.stats.skipped += stats.skipped
            self.stats.errors.extend(stats.errors)

            # Log progress
            logger.info(
                f"Progress: {self.stats.processed} processed, "
                f"{self.stats.skipped} skipped, "
                f"{len(self.stats.errors)} errors"
            )

            # Check error rate
            error_rate = self.stats.error_rate
            if error_rate > 0.01:  # 1% error threshold
                logger.warning(
                    f"High error rate detected: {error_rate:.2%}"
                )
        except Exception as e:
            logger.error(f"Error updating stats: {e}")

    def log_progress(self, stats: ImportStats) -> None:
        """Log current import progress."""
        logger.info(
            f"Progress: processed={stats.processed}, "
            f"skipped={stats.skipped}, "
            f"errors={len(stats.errors)}, "
            f"error_rate={stats.error_rate:.2%}"
        )
