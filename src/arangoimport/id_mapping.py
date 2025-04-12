"""Manages mapping between Neo4j IDs and ArangoDB keys."""

from typing import Dict, Optional, List, Tuple
from threading import Lock
import multiprocessing
import time
from dataclasses import dataclass
from multiprocessing.managers import DictProxy
from .log_config import get_logger

@dataclass
class IDMappingStats:
    """Statistics for ID mapping operations."""
    total_mappings: int = 0
    missing_mappings: int = 0
    duplicate_mappings: int = 0
    invalid_keys: int = 0
    sync_timeouts: int = 0

logger = get_logger(__name__)

class IDMapper:
    """Process-safe mapper between Neo4j IDs and ArangoDB keys."""
    
    def __init__(self) -> None:
        """Initialize an empty ID mapper using multiprocessing Manager."""
        # Create a multiprocessing manager for cross-process sharing
        self._manager = multiprocessing.Manager()
        # Create a shared dictionary that can be accessed across processes
        self._mapping: DictProxy = self._manager.dict()
        self._key_to_id: DictProxy = self._manager.dict()  # Reverse mapping
        self._lock = self._manager.Lock()
        self._sync_event = self._manager.Event()
        self._node_count = self._manager.Value('i', 0)
        self._batch_size = 1000  # Size for batch operations
        self._sync_timeout = 10.0  # Increased timeout for sync operations
        self._max_retries = 5  # Increased retry count
        self._stats = IDMappingStats()
        self._mapping_dir = None  # Will be set by the importer
        
    def _is_valid_key(self, key: str) -> bool:
        """Check if the given key follows basic ArangoDB _key rules.
        
        Args:
            key: Key to validate
            
        Returns:
            bool: True if key is valid, False otherwise
        """
        if not key or len(key) > 254:
            return False
            
        # ArangoDB key rules:
        # 1. Must be a string
        # 2. Length between 1 and 254 chars
        # 3. Cannot contain '/', space, or other special chars
        invalid_chars = {'/', ' ', ':', '"', '\'', '\\', ',', ';', '{', '}', '[', ']', '=', '(', ')'}
        return not any(ch in invalid_chars for ch in key)
        
    def add_mapping(self, neo4j_id: str, arango_key: str) -> Tuple[bool, Optional[str]]:
        """Add a mapping from Neo4j ID to ArangoDB key.
        With our direct Neo4j ID approach, we simply store the Neo4j ID as the ArangoDB key.
        
        Args:
            neo4j_id: Original Neo4j node ID
            arango_key: ArangoDB document key (now same as neo4j_id with direct approach)
            
        Returns:
            Tuple[bool, Optional[str]]: (success, error_message)
        """
        try:
            neo4j_id = str(neo4j_id)
            # With direct Neo4j ID approach, Neo4j ID is used directly as ArangoDB key
            arango_key = str(neo4j_id)
            
            # Validate key format
            if not self._is_valid_key(arango_key):
                self._stats.invalid_keys += 1
                return False, f"Invalid ArangoDB key format: {arango_key}"
            
            with self._lock:
                # Check for existing mappings
                if neo4j_id in self._mapping:
                    self._stats.duplicate_mappings += 1
                    return False, f"Neo4j ID already mapped: {neo4j_id}"
                
                if arango_key in self._key_to_id:
                    self._stats.duplicate_mappings += 1
                    return False, f"ArangoDB key already mapped: {arango_key}"
                
                # Add mappings - Neo4j ID is directly used as ArangoDB key
                self._mapping[neo4j_id] = neo4j_id
                self._key_to_id[arango_key] = neo4j_id
                self._node_count.value += 1
                self._stats.total_mappings += 1
                
                return True, None
                
        except Exception as e:
            logger.error(f"Error adding mapping: {str(e)}")
            return False, str(e)
            
    def get_arango_key(self, neo4j_id: str, max_retries: Optional[int] = None, retry_delay: Optional[float] = None) -> Optional[str]:
        """Get ArangoDB key for a Neo4j ID with retries.
        With direct Neo4j ID approach, we just return the Neo4j ID itself.
        
        Args:
            neo4j_id: Neo4j node ID to look up
            max_retries: Maximum number of retry attempts (unused with direct approach)
            retry_delay: Delay between retries in seconds (unused with direct approach)
            
        Returns:
            Optional[str]: The Neo4j ID itself (as it's now used directly as ArangoDB key)
        """
        # With direct Neo4j ID approach, no need for retries or lookups
        # We simply return the Neo4j ID as it is now used directly as the ArangoDB key
        return str(neo4j_id)
            
    def get_arango_keys_batch(self, neo4j_ids: List[str], max_retries: Optional[int] = None, retry_delay: Optional[float] = None) -> Dict[str, Optional[str]]:
        """Get ArangoDB keys for multiple Neo4j IDs with retries.
        With direct Neo4j ID approach, we simply return a mapping of each ID to itself.
        
        Args:
            neo4j_ids: List of Neo4j node IDs to look up
            max_retries: Maximum number of retry attempts (unused with direct approach)
            retry_delay: Delay between retries in seconds (unused with direct approach)
            
        Returns:
            Dict[str, Optional[str]]: Mapping of Neo4j IDs to themselves (as they're now used directly as ArangoDB keys)
        """
        # With direct Neo4j ID approach, no need for retries or lookups
        # We simply return each Neo4j ID mapped to itself
        return {str(id_): str(id_) for id_ in neo4j_ids}

    def mark_sync_complete(self) -> None:
        """Mark that all node mappings have been synchronized."""
        logger.info(f"Node mapping complete with {self._node_count.value} mappings")
        self._sync_event.set()
            
    def __len__(self) -> int:
        """Get number of mappings."""
        with self._lock:
            return len(self._mapping)
            
    def close(self) -> None:
        """Clean up manager resources."""
        if hasattr(self, '_manager'):
            self._manager.shutdown()
