"""Enhanced logging module for tracking batch processing in edge imports."""

import logging
from typing import Dict, Set, Any, List, Tuple, Optional
import json
import os
import time
from collections import Counter

# Configure enhanced logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("/home/ubuntu/spoke/arangoimport/edge_import_analysis.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger("edge_import_analysis")


class BatchTracker:
    """Tracks edge batch processing statistics and errors."""
    
    def __init__(self, log_dir: str = "/home/ubuntu/spoke/arangoimport/logs"):
        self.log_dir = log_dir
        self.total_edges_seen = 0
        self.total_edges_saved = 0
        self.batch_counts: Dict[int, int] = {}  # Batch number -> edge count
        self.error_batches: Set[int] = set()    # Batch numbers with errors
        self.skipped_batches: Set[int] = set()  # Completely skipped batches
        self.rejected_edges: Set[str] = set()   # Edge keys that failed to import
        self.edge_type_counts = Counter()       # Edge type -> count
        self.start_time = time.time()
        
        # Create log directory if it doesn't exist
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
    
    def record_batch_start(self, batch_num: int, batch_size: int) -> None:
        """Record the start of batch processing."""
        self.batch_counts[batch_num] = batch_size
        self.total_edges_seen += batch_size
        logger.info(f"Starting batch {batch_num} with {batch_size} edges")
    
    def record_batch_error(self, batch_num: int, error: Exception) -> None:
        """Record a batch processing error."""
        self.error_batches.add(batch_num)
        logger.error(f"Error processing batch {batch_num}: {error}")
    
    def record_batch_skip(self, batch_num: int, reason: str) -> None:
        """Record a completely skipped batch."""
        self.skipped_batches.add(batch_num)
        logger.warning(f"Skipped batch {batch_num}: {reason}")
    
    def record_batch_completion(self, batch_num: int, edges_saved: int) -> None:
        """Record successful batch completion."""
        self.total_edges_saved += edges_saved
        if batch_num in self.batch_counts:
            expected = self.batch_counts[batch_num]
            if edges_saved != expected:
                logger.warning(
                    f"Batch {batch_num} saved {edges_saved} edges but expected {expected}"
                )
        logger.info(f"Completed batch {batch_num}, saved {edges_saved} edges")
    
    def record_edge_rejection(self, edge_key: str, reason: str) -> None:
        """Record a rejected edge."""
        self.rejected_edges.add(edge_key)
        logger.warning(f"Rejected edge {edge_key}: {reason}")
    
    def record_edge_type(self, edge_type: str) -> None:
        """Record an edge type for aggregation."""
        self.edge_type_counts[edge_type] += 1
    
    def save_statistics(self) -> None:
        """Save all collected statistics to disk."""
        duration = time.time() - self.start_time
        
        stats = {
            "total_edges_seen": self.total_edges_seen,
            "total_edges_saved": self.total_edges_saved,
            "missing_edges": self.total_edges_seen - self.total_edges_saved,
            "error_batch_count": len(self.error_batches),
            "skipped_batch_count": len(self.skipped_batches),
            "rejected_edge_count": len(self.rejected_edges),
            "edge_type_counts": dict(self.edge_type_counts),
            "error_batches": sorted(list(self.error_batches)),
            "skipped_batches": sorted(list(self.skipped_batches)),
            "duration_seconds": duration,
            "timestamp": time.time()
        }
        
        # Save rejected edges to a separate file if there are any
        if self.rejected_edges:
            with open(f"{self.log_dir}/rejected_edges.json", "w") as f:
                json.dump(sorted(list(self.rejected_edges)), f, indent=2)
        
        # Save main statistics
        with open(f"{self.log_dir}/edge_import_stats.json", "w") as f:
            json.dump(stats, f, indent=2)
        
        logger.info(
            f"Import statistics saved. Saw {self.total_edges_seen} edges, "
            f"saved {self.total_edges_saved}, "
            f"missing {self.total_edges_seen - self.total_edges_saved} edges."
        )


# Global tracker instance for use throughout the application
batch_tracker = BatchTracker()


# Enhanced batch saving function to track exactly what's happening
def track_batch_save(
    collection_name: str,
    batch_num: int,
    docs: List[Dict[str, Any]],
    on_duplicate: str = "replace"
) -> Tuple[int, List[str]]:
    """
    Track details of a batch save operation.
    
    Args:
        collection_name: Name of the collection
        batch_num: Batch number for tracking
        docs: Documents to save
        on_duplicate: Duplicate handling strategy
    
    Returns:
        Tuple of (saved_count, error_keys)
    """
    batch_tracker.record_batch_start(batch_num, len(docs))
    
    # Record edge types for analysis
    for doc in docs:
        if "label" in doc:
            batch_tracker.record_edge_type(doc["label"])
    
    # This would be replaced with the actual save operation
    # Here we're just simulating tracking
    saved_count = len(docs)
    error_keys = []
    
    # Simulate completion
    batch_tracker.record_batch_completion(batch_num, saved_count)
    
    return saved_count, error_keys


def summarize_batch_processing():
    """Generate a summary of batch processing and identify patterns in missing edges."""
    batch_tracker.save_statistics()
    
    # Additional analysis could be performed here
    if batch_tracker.total_edges_seen != batch_tracker.total_edges_saved:
        missing = batch_tracker.total_edges_seen - batch_tracker.total_edges_saved
        logger.info(f"Analysis of {missing} missing edges:")
        
        if missing % 100 == 0:
            logger.info(f"Missing edge count ({missing}) is divisible by 100!")
            
        if batch_tracker.error_batches:
            logger.info(f"Found {len(batch_tracker.error_batches)} batches with errors")
            
        if batch_tracker.skipped_batches:
            logger.info(f"Found {len(batch_tracker.skipped_batches)} completely skipped batches")
            
    return {
        "total_seen": batch_tracker.total_edges_seen,
        "total_saved": batch_tracker.total_edges_saved,
        "missing": batch_tracker.total_edges_seen - batch_tracker.total_edges_saved
    }
