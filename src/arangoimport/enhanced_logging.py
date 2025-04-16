"""Enhanced logging module for tracking batch processing in edge imports.
   OPTIMIZED VERSION: Focuses only on edge batches divisible by 100."""

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
        
        # Special tracking for the "divisible by 100" pattern
        self.batch_size_distribution = Counter()  # Track frequency of each batch size
        self.mod_100_batches = []  # Track batches with sizes divisible by 100
        self.mod_100_outcomes = {}  # Track outcomes of mod 100 batches
        self.worker_process_stats = {}  # Track statistics by worker process
        
        self.start_time = time.time()
        
        # Create log directory if it doesn't exist
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
    
    def record_batch_start(self, batch_num: int, batch_size: int, worker_id: str = "unknown") -> None:
        """Record the start of batch processing.
        OPTIMIZED: Only log edge batches divisible by 100"""
        
        # Skip node batches completely - only track edges
        if not batch_num or 'edge' not in str(batch_num).lower():
            return
            
        self.batch_counts[batch_num] = batch_size
        self.total_edges_seen += batch_size
        
        # Track batch size distribution, but only for batches divisible by 100
        if batch_size % 100 == 0:
            self.batch_size_distribution[batch_size] += 1
            
            # Special tracking for the "divisible by 100" pattern
            entry = {
                "batch_num": batch_num,
                "size": batch_size,
                "worker_id": worker_id,
                "status": "started",
                "timestamp": time.time()
            }
            self.mod_100_batches.append(entry)
            self.mod_100_outcomes[batch_num] = entry.copy()
            logger.info(f"[MOD-100] Starting batch {batch_num} with {batch_size} edges (divisible by 100) on worker {worker_id}")
        
            # Track by worker process, but only for mod-100 batches
            if worker_id not in self.worker_process_stats:
                self.worker_process_stats[worker_id] = {
                    "batches_started": 0,
                    "edges_seen": 0,
                    "edges_saved": 0,
                    "errors": 0,
                    "mod_100_batches": 0
                }
            
            self.worker_process_stats[worker_id]["batches_started"] += 1
            self.worker_process_stats[worker_id]["edges_seen"] += batch_size
            self.worker_process_stats[worker_id]["mod_100_batches"] += 1
    
    def record_batch_error(self, batch_num: int, error: Exception, worker_id: str = "unknown") -> None:
        """Record a batch processing error.
        OPTIMIZED: Only log edge batches divisible by 100"""
        # Skip node batches completely - only track edges
        if not batch_num or 'edge' not in str(batch_num).lower():
            return
            
        # Only track errors for mod-100 batches
        if batch_num in self.mod_100_outcomes:
            self.error_batches.add(batch_num)
            
            # Track by worker process
            if worker_id in self.worker_process_stats:
                self.worker_process_stats[worker_id]["errors"] += 1
            
            # Special tracking for the "divisible by 100" pattern
            self.mod_100_outcomes[batch_num].update({
                "status": "error",
                "error": str(error),
                "completion_timestamp": time.time()
            })
            logger.error(f"[MOD-100] Error in batch {batch_num} on worker {worker_id}: {str(error)}")
            logger.error(f"[MOD-100] Error processing batch {batch_num} on worker {worker_id}: {error}")
        else:
            logger.error(f"Error processing batch {batch_num} on worker {worker_id}: {error}")
    
    def record_batch_skip(self, batch_num: int, reason: str) -> None:
        """Record a completely skipped batch.
        OPTIMIZED: Only log edge batches divisible by 100"""
        # Skip node batches completely - only track edges
        if not batch_num or 'edge' not in str(batch_num).lower():
            return
            
        # Only track skips for batches divisible by 100
        if batch_num in self.mod_100_outcomes:
            self.skipped_batches.add(batch_num)
            logger.warning(f"[MOD-100] Skipped batch {batch_num}: {reason}")
        logger.warning(f"Skipped batch {batch_num}: {reason}")
    
    def record_batch_completion(self, batch_num: int, edges_saved: int, worker_id: str = "unknown") -> None:
        """Record successful batch completion.
        OPTIMIZED: Only log edge batches"""
        # Skip node batches completely - only track edges
        if not batch_num or 'edge' not in str(batch_num).lower():
            return
            
        # Add to the total count of saved edges
        self.total_edges_saved += edges_saved
        
        # Special handling for batches divisible by 100
        if batch_num in self.mod_100_outcomes:
            # Calculate any discrepancy
            expected = self.batch_counts.get(batch_num, 0)
            missing = expected - edges_saved
            
            # Update worker stats
            if worker_id in self.worker_process_stats:
                self.worker_process_stats[worker_id]["edges_saved"] += edges_saved
            
            # Update the tracking record
            self.mod_100_outcomes[batch_num].update({
                "status": "completed",
                "saved": edges_saved,
                "expected": expected,
                "missing": missing,
                "completion_timestamp": time.time()
            })
            
            # Log completion with more details for batches divisible by 100
            if missing > 0:
                logger.warning(
                    f"[MOD-100] Batch {batch_num} completed but missing edges: "
                    f"expected {expected}, saved {edges_saved}, missing {missing} on worker {worker_id}"
                )
            else:
                logger.info(
                    f"[MOD-100] Batch {batch_num} completed successfully: "
                    f"saved all {edges_saved} edges on worker {worker_id}"
                )
            logger.info(f"[MOD-100] Completed batch {batch_num}, saved {edges_saved} edges on worker {worker_id}")
        else:
            logger.info(f"Completed batch {batch_num}, saved {edges_saved} edges on worker {worker_id}")
    
    def record_edge_rejection(self, edge_key: str, reason: str) -> None:
        """Record a rejected edge.
        OPTIMIZED: Only track rejects for mod-100 batches"""
        # Only track if we're investigating a mod-100 batch
        if any(self.mod_100_outcomes):
            self.rejected_edges.add(edge_key)
            logger.warning(f"[MOD-100] Edge rejected: {edge_key}, reason: {reason}")
        logger.warning(f"Rejected edge {edge_key}: {reason}")
    
    def record_edge_type(self, edge_type: str) -> None:
        """Record an edge type for aggregation.
        OPTIMIZED: Only track for mod-100 batches"""
        # Only track edge types when we're actively processing mod-100 batches
        if any(self.mod_100_outcomes):
            self.edge_type_counts[edge_type] += 1
    
    def save_statistics(self) -> None:
        """Save all collected statistics to disk."""
        duration = time.time() - self.start_time
        
        # Basic stats
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
        
        # Enhanced mod-100 pattern statistics
        mod_100_stats = {
            "batch_size_distribution": dict(self.batch_size_distribution),
            "mod_100_batches": self.mod_100_batches,
            "mod_100_outcomes": self.mod_100_outcomes,
            "worker_process_stats": self.worker_process_stats
        }
        
        # Save detailed mod-100 statistics
        with open(f"{self.log_dir}/mod_100_analysis.json", "w") as f:
            json.dump(mod_100_stats, f, indent=2)
        
        # Save rejected edges to a separate file if there are any
        if self.rejected_edges:
            with open(f"{self.log_dir}/rejected_edges.json", "w") as f:
                json.dump(sorted(list(self.rejected_edges)), f, indent=2)
        
        # Save main statistics
        with open(f"{self.log_dir}/edge_import_stats.json", "w") as f:
            json.dump(stats, f, indent=2)
        
        # Special analysis for missing edges
        missing = self.total_edges_seen - self.total_edges_saved
        if missing > 0:
            if missing % 100 == 0:
                logger.info(f"[MOD-100] Found pattern: Missing {missing} edges is exactly divisible by 100")
            elif missing % 10 == 0:
                logger.info(f"[MOD-100] Found pattern: Missing {missing} edges is divisible by 10 (but not 100)")
        
        logger.info(
            f"Import statistics saved. Saw {self.total_edges_seen} edges, "
            f"saved {self.total_edges_saved}, "
            f"missing {self.total_edges_seen - self.total_edges_saved} edges."
        )


# Global tracker instance for use throughout the application
batch_tracker = BatchTracker()

# Create the log directory
if not os.path.exists(batch_tracker.log_dir):
    os.makedirs(batch_tracker.log_dir)
    logger.info(f"Created log directory: {batch_tracker.log_dir}")


# Special function to detect and analyze the "divisible by 100" issue
def detect_divisible_by_100_issue(worker_id: str = None) -> dict:
    """Analyze the current batch statistics to identify if we're experiencing the 'divisible by 100' issue.
    
    Args:
        worker_id: Optional worker ID to filter the analysis by
        
    Returns:
        Dictionary with analysis results
    """
    # Get all batches either overall or for a specific worker
    if worker_id:
        relevant_batches = {k: v for k, v in batch_tracker.mod_100_outcomes.items() 
                           if v.get('worker_id') == worker_id}
    else:
        relevant_batches = batch_tracker.mod_100_outcomes
    
    # Count total and problematic batches
    total_batches = len(relevant_batches)
    problem_batches = []
    
    for batch_num, outcome in relevant_batches.items():
        expected = outcome.get('expected', 0)
        saved = outcome.get('saved', 0)
        
        if expected > saved and expected % 100 == 0:
            # This is a problematic batch - it lost edges and had a mod-100 size
            problem_batches.append({
                'batch_num': batch_num,
                'expected': expected,
                'saved': saved,
                'missing': expected - saved,
                'worker_id': outcome.get('worker_id', 'unknown')
            })
    
    # Calculate overall missing edges
    total_missing = batch_tracker.total_edges_seen - batch_tracker.total_edges_saved
    
    # Check if missing count is divisible by 100
    is_divisible_by_100 = total_missing % 100 == 0
    
    # Get most problematic workers
    worker_issues = {}
    for batch in problem_batches:
        w_id = batch['worker_id']
        if w_id not in worker_issues:
            worker_issues[w_id] = {'batches': 0, 'missing': 0}
        worker_issues[w_id]['batches'] += 1
        worker_issues[w_id]['missing'] += batch['missing']
    
    # Sort workers by missing edges
    problematic_workers = [{'worker_id': w, 'batches': stats['batches'], 'missing': stats['missing']} 
                          for w, stats in worker_issues.items()]
    problematic_workers.sort(key=lambda x: x['missing'], reverse=True)
    
    return {
        'total_missing': total_missing,
        'is_divisible_by_100': is_divisible_by_100,
        'total_mod_100_batches': total_batches,
        'problem_batches_count': len(problem_batches),
        'problem_batches': problem_batches[:10],  # Top 10 most problematic
        'problematic_workers': problematic_workers[:5]  # Top 5 most problematic
    }


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


def analyze_mod_100_pattern(batch_tracker):
    """Analyze batches with sizes divisible by 100 for patterns."""
    mod_100_stats = {
        "total_mod_100_batches": len(batch_tracker.mod_100_batches),
        "completed": 0,
        "errors": 0,
        "incomplete": 0,
        "by_worker": {}
    }
    
    # Analyze outcomes by worker
    for batch_num, outcome in batch_tracker.mod_100_outcomes.items():
        status = outcome.get("status")
        worker_id = outcome.get("worker_id", "unknown")
        
        if worker_id not in mod_100_stats["by_worker"]:
            mod_100_stats["by_worker"][worker_id] = {
                "total": 0,
                "completed": 0,
                "errors": 0,
                "incomplete": 0
            }
        
        mod_100_stats["by_worker"][worker_id]["total"] += 1
        
        if status == "completed":
            mod_100_stats["completed"] += 1
            mod_100_stats["by_worker"][worker_id]["completed"] += 1
        elif status == "error":
            mod_100_stats["errors"] += 1
            mod_100_stats["by_worker"][worker_id]["errors"] += 1
        elif status == "incomplete":
            mod_100_stats["incomplete"] += 1
            mod_100_stats["by_worker"][worker_id]["incomplete"] += 1
    
    # Look for patterns in specific workers
    problematic_workers = []
    for worker_id, stats in mod_100_stats["by_worker"].items():
        if stats["errors"] > 0 or stats["incomplete"] > 0:
            problematic_workers.append({
                "worker_id": worker_id,
                "total_batches": stats["total"],
                "error_rate": stats["errors"] / stats["total"] if stats["total"] > 0 else 0,
                "incomplete_rate": stats["incomplete"] / stats["total"] if stats["total"] > 0 else 0
            })
    
    # Sort problematic workers by error+incomplete rate
    problematic_workers.sort(key=lambda w: w["error_rate"] + w["incomplete_rate"], reverse=True)
    
    return {
        "mod_100_stats": mod_100_stats,
        "problematic_workers": problematic_workers[:5] if problematic_workers else []  # Top 5 most problematic
    }

def analyze_missing_edges(batch_tracker):
    """Analyze patterns in missing edges."""
    total_missing = batch_tracker.total_edges_seen - batch_tracker.total_edges_saved
    
    # Find batches with missing edges
    batches_with_missing = []
    for batch_num, expected in batch_tracker.batch_counts.items():
        if batch_num in batch_tracker.mod_100_outcomes:
            saved = batch_tracker.mod_100_outcomes[batch_num].get("saved", 0)
            if saved < expected:
                batches_with_missing.append({
                    "batch_num": batch_num,
                    "expected": expected,
                    "saved": saved,
                    "missing": expected - saved,
                    "worker_id": batch_tracker.mod_100_outcomes[batch_num].get("worker_id", "unknown")
                })
    
    # Sort by number of missing edges
    batches_with_missing.sort(key=lambda b: b["missing"], reverse=True)
    
    return {
        "total_missing": total_missing,
        "divisible_by_100": (total_missing % 100 == 0),
        "divisible_by_50": (total_missing % 50 == 0),
        "divisible_by_10": (total_missing % 10 == 0),
        "top_missing_batches": batches_with_missing[:10] if batches_with_missing else []  # Top 10 with most missing
    }

def summarize_batch_processing():
    """Generate a summary of batch processing and identify patterns in missing edges."""
    batch_tracker.save_statistics()
    
    # Comprehensive analysis of the missing edges patterns
    analysis = {
        "total_stats": {
            "total_seen": batch_tracker.total_edges_seen,
            "total_saved": batch_tracker.total_edges_saved,
            "missing": batch_tracker.total_edges_seen - batch_tracker.total_edges_saved,
            "error_batches": len(batch_tracker.error_batches),
            "skipped_batches": len(batch_tracker.skipped_batches)
        }
    }
    
    # Analyze missing edges
    if batch_tracker.total_edges_seen != batch_tracker.total_edges_saved:
        missing = batch_tracker.total_edges_seen - batch_tracker.total_edges_saved
        logger.info(f"Analysis of {missing} missing edges:")
        
        if missing % 100 == 0:
            logger.info(f"[MOD-100] Missing edge count ({missing}) is divisible by 100!")
        
        # Add specific analysis for the "divisible by 100" pattern
        analysis["mod_100_analysis"] = analyze_mod_100_pattern(batch_tracker)
        analysis["missing_edges_analysis"] = analyze_missing_edges(batch_tracker)
        
        # Log insights about worker processes
        if analysis["mod_100_analysis"]["problematic_workers"]:
            logger.info(f"[MOD-100] Found {len(analysis['mod_100_analysis']['problematic_workers'])} potentially problematic worker processes")
            for worker in analysis["mod_100_analysis"]["problematic_workers"]:
                logger.info(f"[MOD-100] Worker {worker['worker_id']} has {worker['error_rate']*100:.1f}% error rate on mod-100 batches")
    
    # Save detailed analysis to file
    with open(f"{batch_tracker.log_dir}/edge_import_analysis.json", "w") as f:
        json.dump(analysis, f, indent=2)
    
    return analysis
