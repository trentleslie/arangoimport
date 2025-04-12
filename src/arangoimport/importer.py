"""Core import functionality for ArangoDB."""

import json
import multiprocessing
import os
import queue
import tempfile
import time
from collections.abc import Callable, Generator
from pathlib import Path
from typing import Any, BinaryIO, Dict, List, Optional, Tuple
from .config import ImportConfig
from .monitoring import ImportMonitor, ImportStats
from .id_mapping import IDMapper

import ijson
from arango.collection import Collection
from arango.database import Database as ArangoDatabase
from arango.response import Response

from .connection import ArangoConnection
from .log_config import get_logger, setup_logging
from .utils import retry_with_backoff

logger = get_logger(__name__)


from dataclasses import dataclass

@dataclass
class ImportResult:
    """Detailed results of an import operation.
    
    Attributes:
        total_saved: Total number of documents saved
        created: Number of new documents created
        replaced: Number of documents replaced
        updated: Number of documents updated
        errors: Number of errors encountered
        details: Optional list of error details
    """
    total_saved: int
    created: int
    replaced: int
    updated: int
    errors: int
    details: Optional[List[str]]

def _handle_import_bulk_result(result: Any) -> ImportResult:
    """Handle the result from import_bulk with detailed statistics.
    
    Args:
        result: The raw result from ArangoDB import_bulk operation
        
    Returns:
        ImportResult containing detailed statistics about the operation
        
    Raises:
        ValueError: If the import completely failed with no successful documents
    """
    if not isinstance(result, dict):
        return ImportResult(1, 1, 0, 0, 0, None)  # Fallback for Response objects
        
    created = result.get("created", 0)
    replaced = result.get("replaced", 0)
    updated = result.get("updated", 0)
    imported = result.get("imported", 0)
    errors = result.get("errors", 0)
    details = result.get("details", [])
    
    total_saved = created + replaced + updated + imported
    
    if errors > 0:
        if total_saved == 0:
            error_details = details[:5] if details else "No details available"
            logger.error(f"Import failed. Sample errors: {error_details}")
            raise ValueError(f"Failed to import documents: {result}")
        else:
            logger.warning(f"Encountered {errors} errors during import")
            if details:
                logger.debug(f"Sample error details: {details[:5]}")
    
    logger.debug(
        f"Import stats: Created={created}, Replaced={replaced}, "
        f"Updated={updated}, Imported={imported}, Errors={errors}"
    )
    
    return ImportResult(
        total_saved=total_saved,
        created=created,
        replaced=replaced,
        updated=updated,
        errors=errors,
        details=details if details else None
    )


def batch_save_documents(
    collection: Collection, docs: list[dict[str, Any]], batch_size: int,
    import_config: Optional[ImportConfig] = None
) -> ImportResult:
    """Save documents in batches.

    Args:
        collection: ArangoDB collection
        docs: List of documents to save
        batch_size: Size of batches for saving

    Returns:
        int: Number of documents saved

    Raises:
        AttributeError: If collection is None or has no bulk insertion method
        Exception: If batch save operation fails after retries
    """
    if collection is None:
        raise AttributeError("Collection cannot be None")

    if not docs:
        return ImportResult(0, 0, 0, 0, 0, None)

    if not (hasattr(collection, "import_bulk") or hasattr(collection, "bulkSave")):
        raise AttributeError("Collection must have import_bulk or bulkSave method")

    config = import_config or ImportConfig()
    logger.setLevel(config.log_level)
    
    @retry_with_backoff(max_retries=3)
    def _save_batch(batch: list[dict[str, Any]]) -> ImportResult:
        # Add debug logging for document keys before import
        sample_size = min(3, len(batch))
        for i in range(sample_size):
            sample_doc = batch[i]
            logger.debug(f"Pre-import document: id={sample_doc.get('id')}, _key={sample_doc.get('_key')}, _id={sample_doc.get('_id')}")
        
        # Log debug information that will be visible across process boundaries
        # if batch and len(batch) > 0:
        #     debug_msg = f"\n=== BATCH_SAVE_DOCUMENTS DEBUG ===\nBATCH SIZE: {len(batch)}"
            
        #     # Log details for first 3 documents
        #     for i in range(min(3, len(batch))):
        #         doc = batch[i]
        #         debug_msg += f"\nDOC {i}: _key={doc.get('_key')}, id={doc.get('id')}, _id={doc.get('_id')}"
        #         debug_msg += f"\nDOC {i} KEYS: {sorted(list(doc.keys()))}"
            
        #     # Add document type information
        #     debug_msg += f"\nDOCUMENT TYPE: {type(batch[0]).__name__}"
        #     debug_msg += "\n================================\n"
            
        #     # Use logger.critical for maximum visibility
        #     logger.critical(debug_msg)
            
        #     # Also write to a dedicated debug file for reliable capture
        #     with open(f"arangoimport_debug_{os.getpid()}.log", "a") as debug_file:
        #         debug_file.write(debug_msg)
        #         debug_file.flush()
            
        result = collection.import_bulk(
            batch,
            on_duplicate="replace",  # Explicitly set to replace to ensure our keys are respected
            overwrite=False  # Set to False to preserve our _key values and not generate new ones
        )
        return _handle_import_bulk_result(result)

    total_result = ImportResult(0, 0, 0, 0, 0, [])
    
    for i in range(0, len(docs), batch_size):
        batch = docs[i : i + batch_size]
        batch_result = _save_batch(batch)
        
        # Aggregate results
        total_result = ImportResult(
            total_saved=total_result.total_saved + batch_result.total_saved,
            created=total_result.created + batch_result.created,
            replaced=total_result.replaced + batch_result.replaced,
            updated=total_result.updated + batch_result.updated,
            errors=total_result.errors + batch_result.errors,
            details=(total_result.details or []) + (batch_result.details or [])
        )

    return total_result


@retry_with_backoff(max_retries=3)
def process_nodes_batch(
    nodes_col: Collection, nodes: list[dict[str, Any]], batch_size: int
) -> int:
    """Process a batch of nodes and save to database.

    Args:
        nodes_col: Collection to save nodes to
        nodes: List of nodes to save
        batch_size: Size of batches for saving

    Returns:
        int: Number of nodes saved

    Raises:
        ValueError: If nodes_col is None or nodes is None
    """
    if nodes_col is None:
        raise ValueError("nodes_col cannot be None")
    if nodes is None:
        raise ValueError("nodes cannot be None")

    nodes_added = 0
    node_docs: list[dict[str, Any]] = []

    for node in nodes:
        try:
            # Skip non-dict nodes
            if not isinstance(node, dict):
                continue

            # For nodes, get Neo4j ID from 'id' field
            neo4j_id = str(node.get("id", "") or node.get("_key", ""))
            if not neo4j_id:
                continue
            
            # Use Neo4j ID directly as ArangoDB key without any modifications
            # This ensures direct compatibility with edge connections
            key = str(neo4j_id)  # Explicitly convert to string to ensure compatibility with ArangoDB

            # Create node document with only _key, letting ArangoDB generate _id
            node_doc: dict[str, Any] = {
                "_key": str(key)  # Only provide _key, not _id
            }

            # Copy all fields from the original document
            for field, value in node.items():
                if field not in ["_key", "id"]:  # Don't duplicate key fields
                    node_doc[field] = value

            node_docs.append(node_doc)

            # NOTE: Removed in-loop import_bulk call to avoid multiple imports
            # This ensures a single consistent import operation

        except Exception as e:
            logger.error(f"Error processing node: {e}")
            continue

    # Process nodes using individual insert calls instead of bulk import
    if node_docs:
        try:
            # Log sample of documents being imported
            sample_size = min(3, len(node_docs))
            for i in range(sample_size):
                sample_doc = node_docs[i]
                logger.info(f"Pre-insert document {i}: _key={sample_doc.get('_key')}, id={sample_doc.get('id')}")
            
            # Insert documents individually
            nodes_added = 0
            for doc in node_docs:
                try:
                    # Individual insert to ensure key preservation
                    nodes_col.insert(doc, overwrite=True)
                    nodes_added += 1
                    
                    # Log progress periodically
                    if nodes_added % 100 == 0:
                        logger.info(f"Inserted {nodes_added}/{len(node_docs)} nodes")
                except Exception as doc_error:
                    logger.error(f"Error inserting node: {doc_error}")
            
            logger.info(f"Successfully inserted {nodes_added} nodes")
        except Exception as e:
            logger.error(f"Error in node insertion process: {e}")

    return nodes_added


def process_edge_batch(edge_batch: list[tuple[dict, str, str]], id_mapper: IDMapper, edges_col: Collection) -> None:
    """Process a batch of edges using batch ID lookup.
    
    Args:
        edge_batch: List of tuples containing (doc, start_id, end_id)
        id_mapper: ID mapper instance
        edges_col: Edges collection
    """
    # Keep track of edges we've seen to avoid duplicates
    seen_edges = set()
    
    if not edge_batch:
        return
    
    # Get all start and end IDs - with Neo4j IDs as keys approach, we use these directly
    start_ids = [start_id for _, start_id, _ in edge_batch]
    end_ids = [end_id for _, _, end_id in edge_batch]
    
    # Process edges with direct Neo4j IDs as ArangoDB keys
    valid_edges = []
    skipped = 0
    for doc, start_id, end_id in edge_batch:
        # With direct Neo4j ID approach, we use the IDs directly as keys
        # These are the exact Neo4j IDs from the nodes
        start_key = start_id
        end_key = end_id
        
        # Ensure IDs are valid strings
        if not isinstance(start_key, str) or not isinstance(end_key, str):
            start_key = str(start_key) if start_key is not None else ""
            end_key = str(end_key) if end_key is not None else ""
            
        # Skip if either key is empty
        if not start_key.strip() or not end_key.strip():
            logger.warning(f"Empty keys detected - start: '{start_key}', end: '{end_key}'")
            skipped += 1
            continue
            
        # Generate edge key using Neo4j ID and label
        edge_id = doc.get('id', '')
        edge_label = doc.get('label', '')
        edge_key = str(edge_id).strip('_')
        
        if not edge_key:
            logger.warning(f"[EDGE] doc has empty edge_key. doc={doc}")
        
        # Keep track of edges we've seen to avoid duplicates
        edge_sig = f"{start_key}_{end_key}_{edge_label}_{json.dumps(doc, sort_keys=True)}"
        if edge_sig in seen_edges:
            skipped += 1
            continue
            
        seen_edges.add(edge_sig)

        # Log the connection to aid debugging
        logger.debug(f"Creating edge from {start_key} to {end_key}")
        
        # Create edge document with ArangoDB collection/key format for _from and _to
        edge_doc = {
            "_key": str(edge_key),  # Ensure key is explicitly a string
            "_from": f"Nodes/{str(start_key)}",  # Ensure Neo4j ID is a string when used as ArangoDB key
            "_to": f"Nodes/{str(end_key)}",  # Ensure Neo4j ID is a string when used as ArangoDB key
            "label": edge_label,  # Ensure label is always present
            "neo4j_id": edge_id   # Store original Neo4j edge ID
            # Not including _id, letting ArangoDB generate it
        }
        
        # Copy all other properties except special fields
        for key, value in doc.items():
            if key not in ["_key", "_from", "_to", "type", "id", "start", "end", "label"]:
                edge_doc[key] = value
        
        # Enforce correct ArangoDB format for edge connections
        # This ensures _from and _to are never overwritten by properties
        # Using Neo4j IDs directly as ArangoDB keys
        edge_doc["_from"] = f"Nodes/{start_key}"
        edge_doc["_to"] = f"Nodes/{end_key}"
                
        # Validate edge document structure
        if not all(k in edge_doc for k in ["_key", "_from", "_to"]):
            logger.warning(f"Invalid edge document structure: {edge_doc}")
            skipped += 1
            continue
            
        valid_edges.append(edge_doc)
    
    if skipped:
        logger.warning(f"Skipped {skipped} edges due to invalid mappings or structure")

    
    if valid_edges:
        try:
            result = batch_save_documents(edges_col, valid_edges, batch_size=1000)
        except Exception as e:
            logger.error(f"Failed to save edge batch: {e}")
            if len(valid_edges) > 1:
                # Try saving edges individually to identify problematic ones
                for edge in valid_edges:
                    try:
                        edges_col.insert(edge)
                    except Exception as e2:
                        logger.error(
                            f"Failed to save edge {edge.get('_key', 'unknown')}: {e2}. "
                            f"From: {edge.get('_from')}, To: {edge.get('_to')}"
                        )
            raise

@retry_with_backoff(max_retries=3)
def process_edges_batch(
    edges_col: Collection, edges: list[dict[str, Any]], batch_size: int
) -> int:
    """Process a batch of edges and save to database.

    Args:
        edges_col: Collection to save edges to
        edges: List of edges to save
        batch_size: Size of batches for saving

    Returns:
        int: Number of edges saved

    Raises:
        ValueError: If edges_col is None or edges is None
    """
    if edges_col is None:
        raise ValueError("edges_col cannot be None")
    if edges is None:
        raise ValueError("edges cannot be None")

    edges_added = 0
    edge_docs: list[dict[str, Any]] = []

    for edge in edges:
        try:
            # Skip non-dict edges
            if not isinstance(edge, dict):
                continue

            # Get Neo4j IDs based on document format
            start_id = ""
            end_id = ""
            label = ""
            
            if "_from" in edge and "_to" in edge:
                # Pre-formatted edge
                start_id = edge['_from'].split('/')[-1]
                end_id = edge['_to'].split('/')[-1]
                label = edge.get('label', '')
            elif "neo4j_start_id" in edge and "neo4j_end_id" in edge:
                # Processed relationship
                start_id = edge["neo4j_start_id"]
                end_id = edge["neo4j_end_id"]
                label = edge.get('label', '')
            else:
                # Skip invalid edges
                logger.warning(f"Skipping edge with invalid format: {edge.get('id', 'unknown')}")
                continue

            # Use Neo4j IDs directly as ArangoDB keys
            start_key = start_id
            end_key = end_id
            
            if not start_key or not end_key:
                msg = f"Missing node mapping for edge: start={start_id}, end={end_id}"
                if not config or config.skip_missing_refs:
                    logger.warning(msg)
                    continue
                else:
                    raise ValueError(msg)
            
            # Create edge document
            edge_key = f"{start_id}_{end_id}_{label}".strip('_')
            edge_doc = {
                "_key": edge_key,
                "_from": f"Nodes/{start_key}",  # Use Neo4j ID directly
                "_to": f"Nodes/{end_key}",  # Use Neo4j ID directly
                "label": label,
                "neo4j_start_id": start_id,  # Store Neo4j IDs
                "neo4j_end_id": end_id
            }
            
            # Copy all properties
            for key, value in edge.items():
                if key not in ["_key", "_from", "_to", "type", "id", "start", "end", 
                              "neo4j_start_id", "neo4j_end_id", "_start_node", "_end_node"]:
                    edge_doc[key] = value

            edge_docs.append(edge_doc)

            # Process in batches to improve performance
            if len(edge_docs) >= batch_size:
                try:
                    result = edges_col.import_bulk(edge_docs, on_duplicate="update")
                    edges_added += _handle_import_bulk_result(result)
                    edge_docs = []
                except Exception as e:
                    if "unique constraint violated" in str(e).lower():
                        # Skip duplicate edges silently
                        edge_docs = []
                        continue
                    logger.error(f"Error processing edge batch: {e}")
                    raise

        except Exception as e:
            logger.error(f"Error processing edge: {e}")
            continue

    # Process any remaining edges
    if edge_docs:
        try:
            result = edges_col.import_bulk(edge_docs, on_duplicate="update")
            edges_added += _handle_import_bulk_result(result)
        except Exception as e:
            if "unique constraint violated" in str(e).lower():
                # Skip duplicate nodes silently
                pass
            else:
                logger.error(f"Error processing final edge batch: {e}")

    return edges_added


def stream_json_objects(
    f: BinaryIO, path_prefix: str
) -> Generator[dict[str, Any], None, None]:
    """Stream objects from a JSON dictionary at the given prefix using minimal memory.

    Args:
        f: File object to read from
        path_prefix: JSON path prefix to stream from

    Returns:
        Generator yielding objects from the JSON dictionary
    """
    parser = ijson.parse(f)
    current_object: dict[str, Any] = {}
    current_prefix: str | None = None

    for prefix, event, value in parser:
        if prefix == path_prefix and event == "start_map":
            current_object = {}
            current_prefix = prefix
        elif current_prefix is not None and prefix.startswith(f"{current_prefix}."):
            key = prefix[len(current_prefix) + 1 :]
            if event in ("string", "number", "boolean", "null"):
                current_object[key] = value
        elif prefix == current_prefix and event == "end_map":
            yield current_object
            current_object = {}
            current_prefix = None


def _process_full_json(
    full_data: dict[str, Any],
    chunk_size: int,
    write_chunk: Callable[[dict[str, list[dict[str, Any]]]], str],
) -> Generator[str, None, None]:
    """Process a full JSON object, splitting it into chunks.

    Args:
        full_data: The full JSON data to process
        chunk_size: Size of chunks in bytes
        write_chunk: Function to write a chunk to disk

    Returns:
        Generator yielding paths to chunk files
    """
    current_chunk: dict[str, list[dict[str, Any]]] = {"nodes": [], "edges": []}
    current_size = 0

    # Process nodes
    for node in full_data.get("nodes", []):
        node_size = len(json.dumps(node).encode("utf-8"))
        if current_size + node_size > chunk_size and current_chunk["nodes"]:
            yield write_chunk(current_chunk)
            current_chunk = {"nodes": [], "edges": []}
            current_size = 0
        current_chunk["nodes"].append(node)
        current_size += node_size

    # Process edges
    for edge in full_data.get("edges", []):
        edge_size = len(json.dumps(edge).encode("utf-8"))
        if current_size + edge_size > chunk_size and (
            current_chunk["nodes"] or current_chunk["edges"]
        ):
            yield write_chunk(current_chunk)
            current_chunk = {"nodes": [], "edges": []}
            current_size = 0
        current_chunk["edges"].append(edge)
        current_size += edge_size

    # Write final chunk if there's data
    if current_chunk["nodes"] or current_chunk["edges"]:
        yield write_chunk(current_chunk)


def _process_jsonl(
    f: Any,
    chunk_size: int,
    write_chunk: Callable[[dict[str, list[dict[str, Any]]]], str],
) -> Generator[str, None, None]:
    """Process a JSONL file, splitting it into chunks.

    Args:
        f: File object to read from
        chunk_size: Size of chunks in bytes
        write_chunk: Function to write a chunk to disk

    Returns:
        Generator yielding paths to chunk files
    """
    current_chunk: dict[str, list[dict[str, Any]]] = {"nodes": [], "edges": []}
    current_size = 0

    for line in f:
        try:
            item = json.loads(line.strip())
            if not isinstance(item, dict):
                continue

            item_size = len(line.encode("utf-8"))
            if current_size + item_size > chunk_size and (
                current_chunk["nodes"] or current_chunk["edges"]
            ):
                yield write_chunk(current_chunk)
                current_chunk = {"nodes": [], "edges": []}
                current_size = 0

            item_type = item.get("type", "").lower()
            if item_type == "node":
                current_chunk["nodes"].append(item)
            elif item_type in ["edge", "relationship"]:
                if item_type == "relationship":
                    # Extract start and end nodes from the nested structure
                    start_node = item.get("start", {})
                    end_node = item.get("end", {})
                    
                    # Get the Neo4j IDs directly from the nested objects
                    start_id = str(start_node.get("id", ""))
                    end_id = str(end_node.get("id", ""))
                    
                    if not start_id or not end_id:
                        logger.warning(f"Skipping relationship missing start/end ID: {item.get('id', 'unknown')}")
                        continue
                    
                    logger.debug(f"Processing relationship: {item.get('id')} from {start_id} to {end_id}")
                        
                    # Create edge document that properly connects Neo4j nodes
                    edge_doc = {
                        "id": item.get('id', ''),
                        "neo4j_start_id": start_id,
                        "neo4j_end_id": end_id,
                        "label": item.get('label', ''),
                        "_from": f"Nodes/{start_id}",  # Use Neo4j ID directly 
                        "_to": f"Nodes/{end_id}",      # Use Neo4j ID directly
                        "type": "relationship"
                    }
                    
                    # Generate a unique edge key
                    edge_doc["_key"] = f"{item.get('id', '')}".strip('_')
                    
                    # Add all properties
                    if "properties" in item:
                        for prop_key, prop_value in item["properties"].items():
                            if prop_key not in ["_key", "_from", "_to"]:
                                edge_doc[prop_key] = prop_value
                    
                    current_chunk["edges"].append(edge_doc)
                else:
                    # For pre-formatted edge documents
                    current_chunk["edges"].append(item)
            current_size += item_size

        except json.JSONDecodeError as e:
            logger.warning(f"Skipping invalid JSON line: {e!s}")
            continue

    # Write final chunk if there's data
    if current_chunk["nodes"] or current_chunk["edges"]:
        yield write_chunk(current_chunk)


def split_json_file(
    filename: str | Path,
    chunk_size_mb: int = 100,
) -> Generator[str, None, None]:
    """Split a large JSON file into smaller valid JSON files.

    Args:
        filename: Path to input JSON file
        chunk_size_mb: Target size of each chunk in MB

    Returns:
        Generator yielding paths to chunk files
    """
    chunk_size = chunk_size_mb * 1024 * 1024  # Convert to bytes
    temp_dir = tempfile.mkdtemp(prefix="json_chunks_")
    chunk_number = 0

    def write_chunk(chunk_data: dict[str, list[dict[str, Any]]]) -> str:
        nonlocal chunk_number
        chunk_file = os.path.join(temp_dir, f"chunk_{chunk_number}.json")
        with open(chunk_file, "w") as f:
            json.dump(chunk_data, f)
        chunk_number += 1
        return chunk_file

    try:
        with open(filename) as f:
            first_char = f.read(1)
            f.seek(0)

            # Handle full JSON file format
            if first_char == "{":
                try:
                    full_data = json.load(f)
                    if isinstance(full_data, dict) and (
                        "nodes" in full_data or "edges" in full_data
                    ):
                        yield from _process_full_json(
                            full_data, chunk_size, write_chunk
                        )
                except json.JSONDecodeError:
                    f.seek(0)  # Reset file pointer to try JSONL format

            # Handle JSONL format
            yield from _process_jsonl(f, chunk_size, write_chunk)

    except Exception as e:
        logger.error(f"Error splitting file: {e!s}", exc_info=True)
        raise


def ensure_collections(db: ArangoDatabase) -> None:
    """Ensure required collections exist in the database.

    Args:
        db: ArangoDB database connection
    """
    # Verify database exists and is accessible
    try:
        db_info = db.properties()
        logger.info(f"Connected to database: {db_info.get('name')} (ID: {db_info.get('id')})")
    except Exception as e:
        logger.error(f"Failed to access database: {e}")
        raise
    
    # Get current collections
    try:
        collections = db.collections()
        if hasattr(collections, "result") and callable(collections.result):
            collections = collections.result()

        if isinstance(collections, (list, dict)):
            collection_names = {c["name"] if isinstance(c, dict) else c for c in collections}
        else:
            collection_names = set(collections)
            
        logger.info(f"Current collections: {collection_names}")
    except Exception as e:
        logger.error(f"Failed to get collections: {e}")
        raise

    # Create collections if they don't exist
    try:
        # Setup Nodes collection
        if "Nodes" not in collection_names:
            logger.info("Creating Nodes collection...")
            nodes = db.create_collection("Nodes")
            logger.info("Created new Nodes collection")
        else:
            nodes = db.collection("Nodes")
            logger.info("Using existing Nodes collection")
            
        # Verify nodes collection
        props = nodes.properties()
        if not props:
            raise ValueError("Failed to verify Nodes collection - no properties returned")
        logger.info(f"Nodes collection verified - ID: {props.get('id')}, Status: {props.get('status')}")
        
        # Add/verify node indexes
        logger.info("Adding/verifying indexes on Nodes collection...")
        try:
            # Add unique constraint on neo4j_id
            nodes.add_hash_index(["neo4j_id"], unique=True)
            logger.info("Unique index on neo4j_id created successfully")
        except Exception as e:
            if "duplicate" not in str(e).lower():
                raise
            logger.info("Unique index on neo4j_id already exists")
            
        # Setup Edges collection
        if "Edges" not in collection_names:
            logger.info("Creating Edges collection...")
            edges = db.create_collection("Edges", edge=True)
            logger.info("Created new Edges collection")
        else:
            edges = db.collection("Edges")
            logger.info("Using existing Edges collection")
            
        # Verify edges collection
        props = edges.properties()
        if not props:
            raise ValueError("Failed to verify Edges collection - no properties returned")
        logger.info(f"Edges collection verified - ID: {props.get('id')}, Status: {props.get('status')}")
        
        # Add/verify edge indexes
        logger.info("Adding/verifying indexes on edge properties...")
        for field in ["neo4j_start_id", "neo4j_end_id", "label"]:
            try:
                edges.add_persistent_index([field])
                logger.info(f"Edge index on {field} created successfully")
            except Exception as e:
                if "duplicate" not in str(e).lower():
                    raise
                logger.info(f"Edge index on {field} already exists")
                
        # Log collection stats
        for col in [nodes, edges]:
            try:
                stats = col.statistics()
                logger.info(
                    f"{col.name} stats - "
                    f"Count: {stats.get('count', 0)}, "
                    f"Size: {stats.get('size', 0)} bytes"
                )
            except Exception as e:
                logger.warning(f"Could not get stats for {col.name}: {e}")
    except Exception as e:
        if "duplicate" not in str(e).lower():
            raise


def process_chunk_data(
    db: ArangoDatabase,
    chunk_data: dict[str, Any],
    batch_size: int,
    id_mapper: IDMapper,
    config: Optional[ImportConfig] = None,
    monitor: Optional[ImportMonitor] = None,
    nodes_only: bool = False,
    edges_only: bool = False
) -> tuple[int, int]:
    start_time = time.time()
    """Process chunk data and insert into database.

    Args:
        db: ArangoDB database connection
        chunk_data: Data to process
        batch_size: Size of batches for processing
        id_mapper: Mapper for Neo4j ID to ArangoDB key
        config: Optional import configuration
        monitor: Optional monitor for tracking progress

    Returns:
        Tuple[int, int]: Number of nodes and edges added
    """
    nodes_added = 0
    edges_added = 0
    stats = ImportStats()

    nodes_col = db["Nodes"]
    edges_col = db["Edges"]

    # Process nodes in batches
    nodes = chunk_data.get("nodes", [])
    if nodes and not edges_only:
        logger.info(f"Processing {len(nodes)} nodes...")
        try:
            # Process and validate nodes
            valid_nodes = []
            for node in nodes:
                if not isinstance(node, dict):
                    continue
                    
                # Validate node
                if validate_document(node):
                    valid_nodes.append(node)
            if valid_nodes:
                try:
                    # Process nodes in batches
                    for i in range(0, len(valid_nodes), batch_size):
                        batch = valid_nodes[i:i + batch_size]
                        try: # Try processing this specific batch
                            batch_added = process_nodes_batch(nodes_col, batch, batch_size)
                            nodes_added += batch_added
                            stats.processed += batch_added
                        except Exception as e: # Handle errors for this specific batch
                            if "unique constraint violated" in str(e).lower():
                                # Skip duplicate nodes silently
                                stats.skipped += len(batch)
                            else:
                                logger.warning(f"Error processing node batch: {e}")
                                # Optionally log sample of failed batch here
                                # logger.warning(f"Failed nodes sample: {batch[:3]}")
                                stats.errors.append({"error": str(e), "count": len(batch)})
                                stats.skipped += len(batch)
                        # Progress tracking (runs after each batch try/except)
                        if stats.processed > 0 and stats.processed % 10000 == 0:
                            elapsed = time.time() - start_time
                            if elapsed > 0:
                                rate = stats.processed / elapsed
                                logger.debug(f"Processed {stats.processed:,} nodes. Rate: {rate:.0f} items/sec")
                            else:
                                logger.debug(f"Processed {stats.processed:,} nodes.")
                    # Log final count after loop
                    logger.info(f"Finished node processing loop. Added {nodes_added} of {len(valid_nodes)} valid nodes.")

                except Exception as e:
                    logger.error(f"Error during overall node processing phase: {e}")
                    # Update stats with potentially incomplete counts
                    stats.errors.append({"error": f"Overall node processing error: {e}", "count": len(valid_nodes) - nodes_added})
                    stats.skipped += len(valid_nodes) - nodes_added # Assume remaining nodes were skipped due to error
                    if monitor:
                        monitor.update_stats(stats)
            else:
                logger.warning("No valid nodes found")
        except Exception as e:
            # This outer except catches errors *before* the batch loop starts (e.g., in valid_nodes filtering)
            logger.error(f"Error preparing nodes for processing: {e}")
            stats.errors.append({"error": f"Node preparation error: {e}", "count": len(nodes)})
            stats.skipped += len(nodes)
            if monitor:
                monitor.update_stats(stats)

    # Process edges in batches
    edges = chunk_data.get("edges", [])
    if edges and not nodes_only:
        # Wait for node mappings if needed
        if edges_only and not id_mapper._sync_event.is_set():
            logger.debug("Waiting for node mappings to synchronize...")
            id_mapper._sync_event.wait(timeout=5.0)
            if not id_mapper._sync_event.is_set():
                logger.warning("Node mapping synchronization timed out")

        logger.debug(f"Processing {len(edges)} edges...")
        try:
            # Filter out invalid edges
            valid_edges = []
            for edge in edges:
                if not isinstance(edge, dict):
                    continue
                    
                # Check for required fields
                if "_key" in edge and "_from" in edge and "_to" in edge:
                    valid_edges.append(edge)
                elif "start" in edge and "end" in edge:
                    valid_edges.append(edge)
                else:
                    continue

            if valid_edges:
                try:
                    # Process edges in batches
                    for i in range(0, len(valid_edges), batch_size):
                        batch = valid_edges[i:i + batch_size]
                        try:
                            batch_added = process_edges_batch(edges_col, batch, batch_size)
                            edges_added += batch_added
                            stats.processed += batch_added
                        except Exception as e:
                            if "unique constraint violated" in str(e).lower():
                                # Skip duplicate edges silently
                                continue
                            logger.error(f"Error processing edge batch: {e}")
                            stats.errors.append({"error": str(e), "count": len(batch)})
                            stats.skipped += len(batch)
                except Exception as e:
                    logger.error(f"Error processing edge batch: {e}")
                    stats.errors.append({"error": str(e), "count": len(valid_edges)})
                    stats.skipped += len(valid_edges)
                    if monitor:
                        monitor.update_stats(stats)
            else:
                logger.warning("No valid edges found")
        except Exception as e:
            logger.error(f"Error processing edges: {e}")
            stats.errors.append({"error": str(e), "count": len(edges)})
            stats.skipped += len(edges)
            if monitor:
                monitor.update_stats(stats)

    # Check error threshold if configured
    if config and config.error_threshold is not None:
        error_rate = stats.error_rate
        if error_rate > config.error_threshold:
            raise ValueError(
                f"Error rate {error_rate:.2%} exceeds threshold "
                f"{config.error_threshold:.2%}"
            )

    # Progress tracking
    if stats.processed > 0 and stats.processed % 10000 == 0:
        elapsed = time.time() - start_time
        rate = stats.processed / elapsed
        logger.debug(f"Processed {stats.processed:,} items. Rate: {rate:.0f} items/sec")

    return nodes_added, edges_added


def validate_document(doc: dict[str, Any]) -> tuple[bool, Optional[str]]:
    """Validate document structure and content.

    Args:
        doc: Document to validate

    Returns:
        tuple[bool, Optional[str]]: (is_valid, error_message)
        - is_valid: True if document is valid, False otherwise
        - error_message: None if valid, error description if invalid
    """
    try:
        if not isinstance(doc, dict):
            return False, "Document must be a dictionary"

        if "type" not in doc:
            return False, "Document must have a 'type' field"

        doc_type = doc.get("type", "").lower()
        if doc_type not in ["node", "relationship"]:
            return False, f"Invalid document type: {doc_type}"

        # Node validation
        if doc_type == "node":
            if not (doc.get("id") or doc.get("_key")):
                return False, "Node document must have either 'id' or '_key' field"

            # Validate ID format if present
            if "id" in doc and not isinstance(doc["id"], (str, int)):
                return False, f"Invalid node id type: {type(doc['id'])}"

            # Check properties
            if "properties" in doc and not isinstance(doc["properties"], dict):
                return False, "Node properties must be a dictionary"

            # Validate label if present
            if "label" in doc and not isinstance(doc["label"], str):
                return False, "Node label must be a string"

            return True, None

        # Relationship validation
        if doc_type == "relationship":
            if "start" in doc and "end" in doc:
                start = doc["start"]
                end = doc["end"]

                if not isinstance(start, dict):
                    return False, "Start node must be a dictionary"
                if not isinstance(end, dict):
                    return False, "End node must be a dictionary"
                if not start.get("id"):
                    return False, "Start node must have an 'id' field"
                if not end.get("id"):
                    return False, "End node must have an 'id' field"

                # Check properties if they exist
                if start.get("properties") is not None and not isinstance(start["properties"], dict):
                    return False, "Start node properties must be a dictionary"
                if end.get("properties") is not None and not isinstance(end["properties"], dict):
                    return False, "End node properties must be a dictionary"

            elif "_from" in doc and "_to" in doc:
                # Check that _from and _to are valid strings with a '/'
                if not isinstance(doc["_from"], str):
                    return False, "_from must be a string"
                if not isinstance(doc["_to"], str):
                    return False, "_to must be a string"
                if "/" not in doc["_from"]:
                    return False, "_from must contain a '/' separator"
                if "/" not in doc["_to"]:
                    return False, "_to must contain a '/' separator"

            else:
                return False, "Relationship must have either start/end or _from/_to fields"

            # Validate label if present
            if "label" in doc and not isinstance(doc["label"], str):
                return False, "Relationship label must be a string"

            # Validate properties if present
            if "properties" in doc and not isinstance(doc["properties"], dict):
                return False, "Relationship properties must be a dictionary"

            return True, None

        return False, "Unknown document type"

    except Exception as e:
        logger.warning("Error validating document: %s", e)
        return False, str(e)


def _process_node_document(
    doc: dict[str, Any],
    nodes_col: Collection,
    id_mapper: IDMapper,
    progress_queue: Optional[queue.Queue[tuple[int, int]]] = None
) -> int:
    """Process a node document.

    Args:
        doc: Node document to process
        db: ArangoDatabase connection
        progress_queue: Queue to report progress

    Returns:
        int: Number of nodes added
    """
    try:
        # Validate document first
        if not validate_document(doc):
            return 0

        # For nodes, get Neo4j ID from 'id' field
        neo4j_id = str(doc.get("id", ""))
        if not neo4j_id:
            logger.warning("Node document missing 'id' field")
            return 0

        # Use Neo4j ID directly as ArangoDB key for consistent relationship preservation
        key = neo4j_id

        # Create node document with _key and neo4j_id
        node_doc: dict[str, Any] = {
            "_key": key,
            "neo4j_id": neo4j_id,
            "type": doc.get("type", ""),
            "labels": doc.get("labels", [])
        }

        # Add properties from the original document
        properties = doc.get("properties", {})
        if properties:
            node_doc.update(properties)

        # Copy all fields from the original document
        for field, value in doc.items():
            if field not in ["_key", "id"]:  # Don't duplicate key fields
                node_doc[field] = value

        # Insert document with overwrite option
        try:
            result = nodes_col.insert(node_doc, overwrite=True)
            # Add mapping regardless of whether it was an insert or update
            id_mapper.add_mapping(neo4j_id, key)
            if progress_queue is not None:
                progress_queue.put((1, 0))
            return 1
        except Exception as e:
            logger.error(f"Error inserting node {key}: {e}")
            return 0
    except Exception as e:
        logger.error(f"Error processing node document: {e}")
        return 0


def _process_relationship_document(
    doc: dict[str, Any],
    edges_col: Collection,
    id_mapper: IDMapper,
    config: Optional[ImportConfig] = None,
    progress_queue: Optional[queue.Queue[tuple[int, int]]] = None
) -> int:
    """Process a relationship document.

    Args:
        doc: Relationship document to process
        edges_col: Collection to save edges to
        id_mapper: Mapper for Neo4j ID to ArangoDB key
        config: Import configuration
        progress_queue: Queue to report progress

    Returns:
        int: Number of edges added (0 or 1)

    Raises:
        ValueError: If skip_missing_refs is False and a node reference is missing
    """
    try:
        # Validate document first
        if not validate_document(doc):
            return 0

        # Extract start and end IDs from the document
        if "start" in doc and "end" in doc:
            start = doc.get("start", {})
            end = doc.get("end", {})
            if not isinstance(start, dict) or not isinstance(end, dict):
                logger.warning(f"Invalid start/end format in edge: {doc}")
                return 0
            start_id = str(start.get("id", ""))
            end_id = str(end.get("id", ""))
        else:
            # Assume the document has _from and _to in the form "Nodes/<id>"
            try:
                start_id = doc["_from"].split("/", 1)[1] if "_from" in doc else ""
                end_id = doc["_to"].split("/", 1)[1] if "_to" in doc else ""
            except (KeyError, IndexError):
                logger.warning(f"Invalid _from/_to format in edge: {doc}")
                return 0

        if not start_id or not end_id:
            logger.warning(f"Missing start/end IDs in edge: {doc}")
            return 0

        # Use Neo4j IDs directly as ArangoDB keys
        def get_node_keys():
            # With direct Neo4j ID approach, we use the IDs themselves as keys
            start_key = start_id
            end_key = end_id
            
            # Validate the keys are not empty
            if not start_key or not end_key:
                msg = (
                    f"Missing node IDs for edge {doc.get('id', 'unknown')}: "
                    f"start={start_id}, end={end_id}"
                )
                if not config or config.skip_missing_refs:
                    logger.warning(msg)
                    return None, None
                else:
                    raise ValueError(msg)
            return start_key, end_key

        try:
            start_key, end_key = get_node_keys()
            if not start_key or not end_key:
                return 0
        except Exception as e:
            logger.error(f"Failed to get node keys for edge {doc.get('id', 'unknown')}: {e}")
            return 0

        # Create edge document using Neo4j IDs directly for connections
        edge_doc = {
            "_key": f"{doc.get('id', '')}_{start_id}_{end_id}",  # Ensure unique key
            "_from": f"Nodes/{start_key}",  # Use Neo4j ID directly for edge connection
            "_to": f"Nodes/{end_key}",      # Use Neo4j ID directly for edge connection
            "properties": doc.get("properties", {}),
            "neo4j_start_id": start_id,  # Store Neo4j IDs as properties
            "neo4j_end_id": end_id,
            "label": doc.get("label", "")  # Use 'label' field, not 'type'
        }

        # Copy any additional properties
        for key, value in doc.items():
            if key not in ["_from", "_to", "type", "id", "start", "end", "properties"]:
                edge_doc[key] = value

        # Save document with retry
        @retry_with_backoff(max_retries=3)
        def save_edge():
            result = edges_col.import_bulk([edge_doc], on_duplicate="update")
            return 1 if result.get("created", 0) > 0 else 0

        try:
            edges_added = save_edge()
            if progress_queue is not None:
                progress_queue.put((0, edges_added))
            return edges_added
        except Exception as e:
            logger.error(f"Error processing edge document {doc.get('id', 'unknown')}: {e}")
            return 0
    except Exception as e:
        if "unique constraint violated" in str(e):
            logger.warning(
                "Edge document already exists between %s and %s, skipping...",
                start_id,
                end_id,
            )
        else:
            logger.error(f"Error processing edge document: {e}")
        return 0


def process_document(
    doc: dict[str, Any], nodes_col: Collection, edges_col: Collection, progress_queue: Optional[queue.Queue[tuple[int, int]]] = None
) -> tuple[int, int]:
    """Process a single document and save to database.

    Args:
        doc: Document to process
        nodes_col: Collection to save nodes to
        edges_col: Collection to save edges to
        progress_queue: Queue to report progress

    Returns:
        Tuple[int, int]: Number of nodes and edges added
    """
    nodes_added: int = 0
    edges_added: int = 0

    try:
        # Verify collections are not None
        if nodes_col is None:
            logger.error("Nodes collection is None")
            return 0, 0
        if edges_col is None:
            logger.error("Edges collection is None")
            return 0, 0

        doc_type: str = doc.get("type", "").lower()
        if doc_type == "node":
            nodes_added = _process_node_document(doc, nodes_col, progress_queue)
        elif doc_type == "relationship":
            edges_added = _process_relationship_document(doc, edges_col, progress_queue)
        else:
            logger.warning("Invalid document type: %s", doc_type)
    except KeyError as e:
        logger.warning("Invalid document format - missing required field: %s", e)
    except Exception as e:
        logger.warning("Error processing document: %s", e)

    return nodes_added, edges_added


def _get_collection_with_retry(
    db: ArangoDatabase,
    collection_name: str,
    max_retries: int = 3,
    retry_delay: float = 1.0,
    initial_delay: float = 1.0
) -> Collection:
    """Get a collection with retry logic.

    Args:
        db: ArangoDB database connection
        collection_name: Name of collection to get
        max_retries: Maximum number of retry attempts
        retry_delay: Delay between retries in seconds

    Returns:
        Collection: The requested ArangoDB collection

    Raises:
        ValueError: If collection cannot be obtained after retries
    """
    collection: Optional[Collection] = None
    retry_count = 0
    last_error = None

    # Add initial delay to ensure collection is ready
    time.sleep(initial_delay)

    while retry_count < max_retries:
        try:
            # First check if collection exists
            collections = {c["name"]: c for c in db.collections()}
            
            if collection_name not in collections:
                # Create collection if it doesn't exist
                logger.info(f"Creating collection {collection_name}...")
                if collection_name == "Edges":
                    db.create_collection(collection_name, edge=True)
                else:
                    db.create_collection(collection_name)
                time.sleep(retry_delay)  # Wait for collection to be ready
            collection = db.collection(collection_name)

            if collection is None:
                raise ValueError(f"Collection {collection_name} not found or could not be created")

            # Verify collection is fully accessible by attempting to get properties
            _ = collection.properties()
            logger.info(f"Successfully verified collection {collection_name}")
            return collection

        except Exception as e:
            retry_count += 1
            last_error = e
            if retry_count < max_retries:
                logger.warning(
                    f"Failed to get/create {collection_name} collection "
                    f"(attempt {retry_count}/{max_retries}): {e}"
                )
                time.sleep(retry_delay * (2 ** retry_count))  # Exponential backoff
            
    error_msg = f"Failed to get/create {collection_name} collection after {max_retries} retries"
    if last_error:
        error_msg += f": {last_error}"
    raise ValueError(error_msg)


def validate_node_existence(node_ids: List[str], id_mapper: IDMapper) -> Dict[str, bool]:
    """Validate that nodes exist in the database.
    
    Args:
        node_ids: List of node IDs to validate
        id_mapper: ID mapper instance
        
    Returns:
        Dict[str, bool]: Mapping of node IDs to existence status
    """
    # Get ArangoDB keys for all nodes
    keys = id_mapper.get_arango_keys_batch(node_ids)
    return {id_: key is not None for id_, key in keys.items()}

def process_chunk(
    file_path: str,
    db_config: dict[str, Any],
    start_pos: int,
    end_pos: int,
    progress_queue: queue.Queue[tuple[int, int]] | None = None,
    config: Optional[ImportConfig] = None,
    monitor: Optional[ImportMonitor] = None,
    id_mapper: Optional[IDMapper] = None,
    retry_attempts: int = 5,  # Increase retry attempts
    retry_delay: float = 2.0,  # Increase retry delay
    nodes_only: bool = False,
    edges_only: bool = False,
    log_level_str: str = 'WARNING',  # Add log_level_str parameter
) -> tuple[int, int]:
    """Process a chunk of data from a file.
    
    Args:
        file_path: Path to file to process
        db_config: Database configuration
        start_pos: Start position in file
        end_pos: End position in file
        progress_queue: Queue to report progress
        config: Optional configuration for import settings and validation
        monitor: Optional monitor for tracking progress and quality
        id_mapper: Optional ID mapper instance
        retry_attempts: Number of retry attempts
        retry_delay: Delay between retries
        nodes_only: Only process nodes
        edges_only: Only process edges
        log_level_str: The logging level string for this worker process
        
    Returns:
        tuple[int, int]: Number of nodes and edges added
    """
    """Process a chunk of data from a file.

    Args:
        file_path: Path to file to process
        db_config: Database configuration
        start_pos: Start position in file
        end_pos: End position in file
        progress_queue: Queue to report progress
        import_config: Optional configuration for import settings and validation
        monitor: Optional monitor for tracking progress and quality

    Returns:
        tuple[int, int]: Number of nodes and edges added
    """
    nodes_added = 0
    edges_added = 0
    max_retries = 3
    retry_count = 0
    retry_delay = 1.0

    while retry_count < retry_attempts:
        try:
            logger.info(f"Worker initializing with database: {db_config['db_name']} (attempt {retry_count + 1}/{retry_attempts})")
            
            # Use the provided ID mapper or create a new one
            connection = ArangoConnection(
                host=db_config["host"],
                port=db_config["port"],
                username=db_config["username"],
                password=db_config["password"],
                db_name=db_config["db_name"],
                pool_size=2,
                max_retries=5,
                retry_delay=retry_delay
            )
            
            if id_mapper is None:
                id_mapper = IDMapper()
                logger.warning("No ID mapper provided, creating new one")
                
                # Create temp directory for ID mapper
                mapping_dir = os.path.join(os.path.dirname(file_path), '.id_mapping')
                os.makedirs(mapping_dir, exist_ok=True)
                id_mapper._mapping_dir = mapping_dir
            
            # Get a connection to the database
            with connection.get_connection() as db:
                # Verify we can access the database by listing collections
                try:
                    _ = db.collections()
                except Exception as e:
                    raise ValueError(f"Cannot access database {db_config['db_name']}: {e}")
                    
                # Get collections with increased retry parameters
                nodes_col = _get_collection_with_retry(db, "Nodes", max_retries=5, retry_delay=retry_delay, initial_delay=retry_delay)
                if nodes_col is None:
                    raise ValueError("Failed to get Nodes collection")
                    
                edges_col = _get_collection_with_retry(db, "Edges", max_retries=5, retry_delay=retry_delay, initial_delay=retry_delay)
                if edges_col is None:
                    raise ValueError("Failed to get Edges collection")
                
                # Verify collections are still accessible
                nodes_col.properties()
                edges_col.properties()
                
                logger.info(f"Worker: Collections verified in database {db_config['db_name']}")

                # Process documents
                with open(file_path, encoding="utf-8") as f:
                    f.seek(start_pos)
                    # Read to the next newline if not at the start
                    if start_pos > 0:
                        f.readline()

                    # Initialize batches with memory-aware sizing
                    node_batch: list[dict[str, Any]] = []
                    edge_batch: list[dict[str, Any]] = []
                    
                    # Calculate optimal batch size based on chunk size
                    chunk_size_mb = (end_pos - start_pos) / (1024 * 1024)  # Convert to MB
                    batch_size = min(
                        max(100, int(chunk_size_mb / 100)),  # At least 100, scales with chunk size
                        5000  # Cap at 5000 to avoid memory issues
                    )
                    logger.info(f"Using batch size {batch_size} for {chunk_size_mb:.2f}MB chunk")
                    
                    # Clear existing edges if we're processing edges
                    if edges_only:
                        clear_existing_edges(edges_col)
                        
                    # Wait for node mappings if needed for edge processing
                    if edges_only and id_mapper and not id_mapper._sync_event.is_set():
                        logger.debug("Waiting for node mappings to synchronize...")
                        id_mapper._sync_event.wait(timeout=5.0)
                        if not id_mapper._sync_event.is_set():
                            logger.warning("Node mapping synchronization timed out")

                    def flush_node_batch():
                        nonlocal nodes_added, node_batch
                        if not node_batch:
                            return
                            
                        # Validate Neo4j IDs are being set as keys before saving
                        sample_size = min(5, len(node_batch))
                        for i in range(sample_size):
                            node = node_batch[i]
                            if node.get("_key") != str(node.get("id")):
                                logger.warning(f"Node _key mismatch: _key={node.get('_key')}, id={node.get('id')}")
                            if node.get("_id") != f"Nodes/{str(node.get('id'))}":
                                logger.warning(f"Node _id mismatch: _id={node.get('_id')}, expected=Nodes/{node.get('id')}")
                                
                        # Debug logging to track actual keys being used
                        logger.debug(f"Sample node keys: {[doc.get('_key', 'MISSING') for doc in node_batch[:3]]}")
                        logger.debug(f"Sample node Neo4j IDs: {[doc.get('id', 'MISSING') for doc in node_batch[:3]]}")
                        logger.debug(f"Sample node _id values: {[doc.get('_id', 'MISSING') for doc in node_batch[:3]]}")
                            
                        retry_count = 0
                        while retry_count < retry_attempts and node_batch:
                            try:
                                # Process in smaller sub-batches if needed
                                sub_batch = node_batch[:batch_size]
                                batch_save_documents(nodes_col, sub_batch, batch_size)
                                nodes_added += len(sub_batch)
                                
                                if progress_queue is not None:
                                    progress_queue.put((len(sub_batch), 0))
                                    
                                # Remove processed items
                                node_batch = node_batch[batch_size:]
                                retry_count = 0  # Reset counter on success
                                
                            except Exception as e:
                                if "unique constraint violated" in str(e).lower():
                                    # Skip duplicate nodes
                                    node_batch = node_batch[batch_size:]
                                    continue
                                    
                                retry_count += 1
                                if retry_count < retry_attempts:
                                    logger.warning(f"Retrying node batch after error: {e}")
                                    time.sleep(retry_delay * retry_count)
                                else:
                                    logger.error(f"Failed to process node batch after {retry_attempts} attempts: {e}")
                                    if node_batch:
                                        sample_size = min(3, len(node_batch))
                                        logger.error(f"Failed nodes sample: {node_batch[:sample_size]}")
                                    node_batch = []  # Clear batch after max retries
                                    
                        node_batch = []  # Clear any remaining items

                    def flush_edge_batch():
                        nonlocal edges_added, edge_batch
                        if not edge_batch:
                            return
                        
                        # Validate edge connections are using Neo4j IDs
                        sample_size = min(5, len(edge_batch))
                        for i in range(sample_size):
                            edge_info = edge_batch[i]
                            if isinstance(edge_info, tuple):
                                doc, start_id, end_id = edge_info
                                logger.debug(f"Edge connection: from={start_id} to={end_id}")
                            
                        # Debug logging for edge connections
                        logger.debug(f"Sample edge batch connections: {[(e[1], e[2]) for e in edge_batch[:3] if isinstance(e, tuple)]}")
                            
                        retry_count = 0
                        while retry_count < retry_attempts and edge_batch:
                            try:
                                # Process in smaller sub-batches if needed
                                sub_batch = edge_batch[:batch_size]

                                process_edge_batch(sub_batch, id_mapper, edges_col)
                                edges_added += len(sub_batch)
                                
                                if progress_queue is not None:
                                    progress_queue.put((0, len(sub_batch)))
                                    
                                # Remove processed items
                                edge_batch = edge_batch[batch_size:]
                                retry_count = 0  # Reset counter on success
                                
                            except Exception as e:
                                if "unique constraint violated" in str(e).lower():
                                    # Skip duplicate edges
                                    edge_batch = edge_batch[batch_size:]
                                    continue
                                    
                                retry_count += 1
                                if retry_count < retry_attempts:
                                    logger.warning(f"Retrying edge batch after error: {e}")
                                    time.sleep(retry_delay * retry_count)
                                else:
                                    logger.error(f"Failed to process edge batch after {retry_attempts} attempts: {e}")
                                    if edge_batch:
                                        sample_size = min(3, len(edge_batch))
                                        logger.error(f"Failed edges sample: {edge_batch[:sample_size]}")
                                    edge_batch = []  # Clear batch after max retries
                                    
                        edge_batch = []  # Clear any remaining items

                    line_count = 0
                    while True:
                        current_pos = f.tell()
                        # Read the line before checking boundary
                        line = f.readline()
                        
                        if not line:
                            break  # End of file

                        line = line.strip()
                        if not line:
                            continue
                            
                        # If we're at or past the end position, only continue if this is
                        # the first worker or we've just started (to handle the edge case
                        # where a single line spans an entire chunk)
                        if current_pos >= end_pos and current_pos > start_pos:
                            break  # Stop at chunk boundary
                            
                        line_count += 1

                        try:
                            doc = json.loads(line)
                            # Validate document before processing
                            if not validate_document(doc):
                                continue

                            # Add to appropriate batch based on mode
                            doc_type = doc.get("type", "").lower()
                            
                            # Process nodes first - only process if it's explicitly a node
                            if not edges_only and doc_type == "node":
                                # Ensure we have required node properties
                                if "id" not in doc:
                                    logger.warning(f"Skipping node without id: {doc}")
                                    
                                # Always set the _key field to the Neo4j ID to ensure consistent key usage
                                doc["_key"] = str(doc["id"])
                                # Also set _id with the proper collection prefix to ensure proper key extraction
                                doc["_id"] = f"Nodes/{str(doc['id'])}"
                                
                                # CRITICAL DEBUG: Add high-visibility logging of document key setup
                                logger.debug(f"NODE KEY ASSIGNMENT: pid={os.getpid()} _key={doc['_key']} id={doc['id']} _id={doc['_id']}")
                                # Log the first few documents in each batch with all their keys
                                if len(node_batch) < 3:
                                    logger.debug(f"DOCUMENT KEYS: {sorted(list(doc.keys()))}")
                                
                                # Add mapping to shared ID mapper
                                if id_mapper is not None:
                                    id_mapper.add_mapping(str(doc["id"]), doc["_key"])
                                    
                                node_batch.append(doc)
                                if len(node_batch) >= batch_size:
                                    flush_node_batch()
                                    
                            # Then process edges
                            elif not nodes_only and doc_type == "relationship":
                                # Convert relationship format to edge format
                                start_node = doc.get("start", {})
                                end_node = doc.get("end", {})
                                start_id = str(start_node.get("id", ""))
                                end_id = str(end_node.get("id", ""))

                                if not start_id or not end_id:
                                    logger.warning(f"Skipping edge with missing start/end: {doc}")
                                    continue

                                # Add to edge batch for batch processing
                                edge_batch.append((doc, start_id, end_id))
                                
                                if len(edge_batch) >= batch_size:
                                    flush_edge_batch()

                        except json.JSONDecodeError:
                            logger.warning("Invalid JSON in line: %s...", line[:100])
                        except Exception as e:
                            logger.warning(f"Error processing document: {e}")
                            continue

                    # Flush any remaining batches
                    if node_batch:
                        # Just flush the batch without verbose logging
                        flush_node_batch()
                    if edge_batch:
                        # Just flush the batch without verbose logging
                        try:
                            process_edge_batch(edge_batch, id_mapper, edges_col)
                            edges_added += len(edge_batch)
                            if progress_queue is not None:
                                progress_queue.put((0, len(edge_batch)))
                        except Exception as e:
                            logger.warning(f"Error processing final edge batch: {e}")
                            if edge_batch:
                                sample_size = min(3, len(edge_batch))
                                logger.warning(f"Failed edges sample: {edge_batch[:sample_size]}")

                return nodes_added, edges_added

        except Exception as e:
            retry_count += 1
            if retry_count < max_retries:
                logger.warning(
                    f"Worker: Failed to access collections (attempt {retry_count}): {e}"
                )
                time.sleep(retry_delay)
            else:
                logger.error(f"Worker: Failed to access collections after {max_retries} retries: {e}")
                raise
    raise ValueError("Failed to process chunk after maximum retries")


def get_db_max_connections(db: ArangoDatabase) -> int:
    """Get the maximum number of connections allowed by the database.

    Args:
        db: ArangoDB database connection

    Returns:
        int: Maximum number of connections allowed
    """
    try:
        # Get server version and settings
        # Note: This is a workaround as the type stubs don't include all methods
        conn = getattr(db, "connection", None)
        if conn and hasattr(conn, "get"):
            server_info = conn.get(
                "/_api/endpoint"
            )  # We know this exists but mypy doesn't
            if server_info and isinstance(server_info, dict):
                max_connections = server_info.get("maxConnections", 128)
                return int(max_connections)
    except Exception as e:
        logger.warning(f"Could not determine database max connections: {e!s}")
        return 128  # Default to ArangoDB's default

    return 128  # Fallback to default


def clear_existing_edges(edges_col: Collection) -> None:
    """Clear existing edges before import.

    Args:
        edges_col: The edge collection to clear
    """
    logger.info("Clearing existing edges...")
    edges_col.truncate()


def parallel_load_data(
    filename: str | Path,
    db_config: dict[str, Any],
    processes: int = 16,
    progress_queue: queue.Queue[tuple[int, int]] | None = None,
    import_config: Optional[ImportConfig] = None,
    log_level_str: str = 'WARNING',  # Add log_level_str parameter
) -> tuple[int, int]:
    """Load data in parallel using multiple processes.

    Args:
        filename: Path to input file
        db_config: Database configuration
        processes: Number of processes to use
        progress_queue: Queue to report progress
        import_config: Optional configuration for import settings and validation
        log_level_str: The logging level string for this worker process

    Returns:
        tuple[int, int]: Number of nodes and edges added
    """
    logger.info(f"Main ({os.getpid()}): Starting parallel_load_data with {processes} processes for {filename}")

    # Initialize monitoring and create collections
    from .monitoring import ImportMonitor
    from .connection import ArangoConnection
    
    # Create a connection for monitoring
    conn = ArangoConnection(
        host=db_config["host"],
        port=db_config["port"],
        username=db_config["username"],
        password=db_config["password"],
        db_name=db_config["db_name"]
    )
    
    # First connect to _system database to create our target database
    system_conn = ArangoConnection(
        host=db_config["host"],
        port=db_config["port"],
        username=db_config["username"],
        password=db_config["password"],
        db_name="_system"  # Connect to system database first
    )
    
    # Create the target database if it doesn't exist
    with system_conn.get_connection() as sys_db:
        databases = sys_db.databases()
        if db_config['db_name'] not in databases:
            logger.info(f"Creating database: {db_config['db_name']}")
            sys_db.create_database(db_config['db_name'])
            time.sleep(2)  # Wait for database creation
            
            # Verify database was created
            if db_config['db_name'] not in sys_db.databases():
                raise ValueError(f"Failed to create database {db_config['db_name']}")
    
    # Now connect to our target database and set up collections
    with conn.get_connection() as db:
        logger.info(f"Main: Setting up collections in database: {db_config['db_name']}")
            
        # Get existing collections
        collections = {c["name"]: c for c in db.collections()}
        
        # Create collections if they don't exist
        try:
            # Create Nodes collection if needed
            if "Nodes" not in collections:
                logger.info("Creating Nodes collection...")
                db.create_collection("Nodes")
                time.sleep(2)  # Wait for collection creation to complete
                
                # Verify Nodes collection
                nodes_col = db.collection("Nodes")
                if nodes_col is None:
                    raise ValueError("Failed to create Nodes collection")
                nodes_col.properties()  # Verify it's accessible
                logger.info("Nodes collection created and verified")
            
            # Create Edges collection if needed
            if "Edges" not in collections:
                logger.info("Creating Edges collection...")
                db.create_collection("Edges", edge=True)
                time.sleep(2)  # Wait for collection creation to complete
                
                # Verify Edges collection
                edges_col = db.collection("Edges")
                if edges_col is None:
                    raise ValueError("Failed to create Edges collection")
                edges_col.properties()  # Verify it's accessible
                logger.info("Edges collection created and verified")
            
            # Final verification of both collections
            nodes_col = db.collection("Nodes")
            edges_col = db.collection("Edges")
            
            if nodes_col is None or edges_col is None:
                raise ValueError("Collections not properly initialized")
                
            # Verify both collections are accessible
            nodes_col.properties()
            edges_col.properties()
            
            logger.info("All collections successfully verified")
            
            # Final verification
            try:
                final_collections = db.collections()
                logger.info(f"Database collections: {[c['name'] for c in final_collections]}")
                
                # Verify collections are accessible
                nodes_col = db.collection("Nodes")
                edges_col = db.collection("Edges")
                _ = nodes_col.count()
                _ = edges_col.count()
                logger.info("Collections verified and ready")
                
                # Initialize monitor after collections exist
                monitor = ImportMonitor(db)
                
                # Get initial counts for quality verification
                original_counts = monitor.get_node_counts()
            except Exception as e:
                logger.error(f"Failed to verify collections: {e}")
                raise
        except Exception as e:
            logger.error(f"Failed to setup collections: {e}")
            raise

    file_size = os.path.getsize(filename)
    chunk_size = file_size // processes

    # Process nodes first
    logger.info("Processing nodes...")
    node_processes = []
    node_result_queue = multiprocessing.Queue()

    # Create a shared ID mapper for all processes
    id_mapper = IDMapper()

    for i in range(processes):
        start = i * chunk_size
        end = start + chunk_size if i < processes - 1 else file_size
        logger.info(f"Main: Starting node worker {i + 1}/{processes} for chunk [{start}-{end}]")
        p = multiprocessing.Process(
            target=lambda q, *args: q.put(process_chunk(*args)),
            args=(
                node_result_queue,
                filename,
                db_config,
                start,
                end,
                progress_queue,
                import_config,
                monitor,
                id_mapper,  # Pass the shared ID mapper
                5,  # retry_attempts
                2.0,  # retry_delay
                True,  # nodes_only
                False,  # edges_only
                log_level_str,  # Pass log_level_str
            ),
        )
        p.start()
        logger.info(f"Main: Started node worker {p.pid} for chunk {i + 1}/{processes} [{start}-{end}]")
        node_processes.append(p)

    # Wait for all node processes to complete
    logger.info(f"Main: Waiting for {len(node_processes)} node workers to join...")
    for p in node_processes:
        logger.debug(f"Main: Joining node worker {p.pid}...")
        p.join()
        logger.info(f"Main: Node worker {p.pid} joined. Exit code: {p.exitcode}")

    # Get node results and ensure all nodes are mapped
    logger.info("Main: All node workers finished. Collecting results...")
    node_results = []
    while not node_result_queue.empty():
        node_results.append(node_result_queue.get())

    # Validate that all nodes are properly mapped
    total_nodes = id_mapper.__len__()
    logger.info(f"Node processing complete with {total_nodes} mappings")
    if total_nodes == 0:
        logger.error("No nodes were mapped! This will cause edge processing to fail.")
        raise ValueError("No nodes were mapped during import")
    
    # Mark node mapping as complete to unblock edge processing
    logger.info(f"Node mapping complete and validated with {total_nodes} mappings")
    id_mapper.mark_sync_complete()

    # Now process edges with the complete node mappings
    logger.info("Processing edges...")
    edge_processes = []
    edge_result_queue = multiprocessing.Queue()

    for i in range(processes):
        start = i * chunk_size
        end = start + chunk_size if i < processes - 1 else file_size
        logger.info(f"Main: Starting edge worker {i + 1}/{processes} for chunk [{start}-{end}]")
        p = multiprocessing.Process(
            target=lambda q, *args: q.put(process_chunk(*args)),
            args=(
                edge_result_queue,
                filename,
                db_config,
                start,
                end,
                progress_queue,
                None,
                monitor,
                id_mapper,  # Pass the synchronized id_mapper
                5,  # retry_attempts
                2.0,  # retry_delay
                False,  # nodes_only
                True,  # edges_only
                log_level_str,  # Pass log_level_str
            ),
        )
        p.start()
        logger.info(f"Main: Started edge worker {p.pid} for chunk {i + 1}/{processes} [{start}-{end}]")
        edge_processes.append(p)

    # Wait for all edge processes to complete
    logger.info(f"Main: Waiting for {len(edge_processes)} edge workers to join...")
    for p in edge_processes:
        logger.debug(f"Main: Joining edge worker {p.pid}...")
        p.join()
        logger.info(f"Main: Edge worker {p.pid} joined. Exit code: {p.exitcode}")

    # Get edge results
    logger.info("Main: All edge workers finished. Collecting results...")
    edge_results = []
    while not edge_result_queue.empty():
        edge_results.append(edge_result_queue.get())

    # Combine results
    results = node_results + edge_results

    # Sum up results
    total_nodes = sum(r[0] for r in results)
    total_edges = sum(r[1] for r in results)

    # Verify import quality
    if import_config:
        if not monitor.verify_import_quality(
            original_counts,
            threshold=import_config.error_threshold
        ):
            logger.warning("Import quality verification failed")

    logger.info(f"Main: parallel_load_data finished. Total added: Nodes={total_nodes}, Edges={total_edges}")
    return total_nodes, total_edges
