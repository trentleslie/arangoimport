#!/usr/bin/env python
"""
Analyze missing edges in the SPOKE database import with a focus on the "divisible by 100" pattern.

This script analyzes log data from the edge import process to identify patterns
in missing edges, particularly focusing on batches with sizes divisible by 100.
"""

import os
import sys
import json
import logging
from collections import Counter
import argparse
from pathlib import Path

# Add the project directory to the path to allow importing project modules
sys.path.append(str(Path(__file__).parent.parent))

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("edge_analysis")

try:
    from src.arangoimport.enhanced_logging import detect_divisible_by_100_issue, batch_tracker
    has_batch_tracker = True
except ImportError:
    logger.warning("Could not import enhanced_logging. Analysis will be limited to log files.")
    has_batch_tracker = False


def analyze_log_files(log_dir: str = "/home/ubuntu/spoke/arangoimport/logs"):
    """Analyze log files in the specified directory to look for patterns in missing edges."""
    logger.info(f"Analyzing log files in {log_dir}")
    
    # Ensure log directory exists
    if not os.path.exists(log_dir):
        logger.error(f"Log directory {log_dir} does not exist.")
        return
    
    # Edge stats file
    edge_stats_file = os.path.join(log_dir, "edge_import_stats.json")
    if not os.path.exists(edge_stats_file):
        logger.error(f"Edge stats file {edge_stats_file} does not exist.")
        return
    
    with open(edge_stats_file, "r") as f:
        edge_stats = json.load(f)
    
    # Basic stats
    total_seen = edge_stats.get("total_edges_seen", 0)
    total_saved = edge_stats.get("total_edges_saved", 0)
    missing = edge_stats.get("missing_edges", 0)
    
    logger.info(f"Total edges seen: {total_seen:,}")
    logger.info(f"Total edges saved: {total_saved:,}")
    logger.info(f"Missing edges: {missing:,}")
    
    # Check if missing is divisible by 100
    if missing % 100 == 0:
        logger.info(f"PATTERN DETECTED: Missing edge count ({missing}) is EXACTLY divisible by 100!")
        logger.info(f"This strongly suggests a systematic issue with batches of size 100.")
    elif missing % 10 == 0:
        logger.info(f"PATTERN DETECTED: Missing edge count ({missing}) is divisible by 10.")
    
    # Analyze mod-100 patterns if available
    mod_100_file = os.path.join(log_dir, "mod_100_analysis.json")
    if os.path.exists(mod_100_file):
        with open(mod_100_file, "r") as f:
            mod_100_data = json.load(f)
            
        # Analyze batch size distribution
        batch_sizes = mod_100_data.get("batch_size_distribution", {})
        if batch_sizes:
            logger.info("Batch size distribution:")
            batch_size_counter = Counter({int(k): v for k, v in batch_sizes.items()})
            for size, count in batch_size_counter.most_common(10):
                logger.info(f"  Size {size}: {count} batches")
            
            # Check for patterns in problematic sizes
            mod_100_sizes = [size for size in batch_size_counter.keys() if int(size) % 100 == 0]
            if mod_100_sizes:
                logger.info(f"Found {len(mod_100_sizes)} different batch sizes divisible by 100:")
                for size in sorted(mod_100_sizes):
                    logger.info(f"  Size {size}: {batch_size_counter[size]} batches")
        
        # Analyze problematic workers
        worker_stats = mod_100_data.get("worker_process_stats", {})
        if worker_stats:
            logger.info("Worker process statistics:")
            problem_workers = []
            
            for worker_id, stats in worker_stats.items():
                edges_seen = stats.get("edges_seen", 0)
                edges_saved = stats.get("edges_saved", 0)
                worker_missing = edges_seen - edges_saved
                
                if worker_missing > 0:
                    problem_workers.append({
                        "worker_id": worker_id,
                        "edges_seen": edges_seen,
                        "edges_saved": edges_saved,
                        "missing": worker_missing,
                        "missing_pct": (worker_missing / edges_seen) * 100 if edges_seen > 0 else 0
                    })
            
            # Sort by missing percentage
            problem_workers.sort(key=lambda w: w["missing_pct"], reverse=True)
            
            if problem_workers:
                logger.info(f"Found {len(problem_workers)} workers with missing edges:")
                for worker in problem_workers[:10]:  # Show top 10
                    logger.info(f"  Worker {worker['worker_id']}: "
                              f"Missing {worker['missing']:,} edges "
                              f"({worker['missing_pct']:.2f}% of {worker['edges_seen']:,} processed)")
            else:
                logger.info("No workers with missing edges found in the logs.")


def run_live_analysis():
    """Run analysis using the live batch_tracker instance."""
    if not has_batch_tracker:
        logger.error("Live analysis requires the enhanced_logging module.")
        return
    
    logger.info("Running live analysis with batch_tracker...")
    
    # Get the current missing edges count
    total_seen = batch_tracker.total_edges_seen
    total_saved = batch_tracker.total_edges_saved
    missing = total_seen - total_saved
    
    logger.info(f"Current stats - Seen: {total_seen:,}, Saved: {total_saved:,}, Missing: {missing:,}")
    
    # Run the specialized divisible-by-100 analysis
    analysis = detect_divisible_by_100_issue()
    
    logger.info("\n=== DIVISIBLE-BY-100 ANALYSIS ===")
    logger.info(f"Missing edges: {analysis['total_missing']:,}")
    logger.info(f"Is divisible by 100: {analysis['is_divisible_by_100']}")
    logger.info(f"Total mod-100 batches: {analysis['total_mod_100_batches']}")
    logger.info(f"Problem batches count: {analysis['problem_batches_count']}")
    
    if analysis['problem_batches']:
        logger.info("\nTop problematic batches:")
        for batch in analysis['problem_batches']:
            logger.info(f"  Batch {batch['batch_num']}: Expected {batch['expected']}, "
                      f"Saved {batch['saved']}, Missing {batch['missing']}")
    
    if analysis['problematic_workers']:
        logger.info("\nMost problematic workers:")
        for worker in analysis['problematic_workers']:
            logger.info(f"  Worker {worker['worker_id']}: "
                      f"{worker['missing']} missing edges across {worker['batches']} batches")


def main():
    parser = argparse.ArgumentParser(description="Analyze missing edges in SPOKE database import")
    parser.add_argument("--log-dir", default="/home/ubuntu/spoke/arangoimport/logs",
                        help="Directory containing log files")
    parser.add_argument("--live", action="store_true",
                        help="Run live analysis using batch_tracker")
    
    args = parser.parse_args()
    
    if args.live and has_batch_tracker:
        run_live_analysis()
    else:
        analyze_log_files(args.log_dir)
    
    logger.info("Analysis complete.")


if __name__ == "__main__":
    main()
