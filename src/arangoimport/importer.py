"""Core import functionality for ArangoDB."""

import json
import multiprocessing
import os
import queue
import tempfile
from collections.abc import Generator
from pathlib import Path
from typing import Any, BinaryIO

import ijson
from arango.collection import Collection
from arango.database import Database as ArangoDatabase
from arango.response import Response

from arangoimport.connection import ArangoConnection
from arangoimport.logging import get_logger
from arangoimport.utils import (
    retry_with_backoff,
)

logger = get_logger(__name__)


def _handle_import_bulk_result(result: Any) -> int:
    """Handle the result from import_bulk.

    Args:
        result: Result from import_bulk

    Returns:
        int: Number of documents saved

    Raises:
        ValueError: If no documents were imported and there were errors
    """
    if isinstance(result, dict):
        saved = result.get("created", 0) or result.get("imported", 0)
        errors = result.get("errors", 0)

        # If we got errors but no documents were created/imported,
        # raise an exception to trigger retry
        if errors > 0 and saved == 0:
            raise ValueError(f"Failed to import documents: {result}")

        if errors > 0:
            logger.debug(f"Skipped {errors} existing documents")

        return int(saved)
    elif isinstance(result, Response):
        return 1  # Response doesn't have created count
    else:
        return 1  # Fallback assuming success


@retry_with_backoff(max_retries=3)
def batch_save_documents(
    collection: Any, docs: list[dict[str, Any]], batch_size: int
) -> int:
    """Save documents in batches.

    Args:
        collection: ArangoDB collection
        docs: List of documents to save
        batch_size: Size of batches for saving

    Returns:
        int: Number of documents saved
    """
    logger.debug(f"batch_save_documents called with {len(docs)} documents")
    saved = 0
    for i in range(0, len(docs), batch_size):
        batch = docs[i : i + batch_size]
        try:
            # Try bulkSave first if available
            if hasattr(collection, "bulkSave") and collection.bulkSave is not None:
                logger.debug("Using bulkSave method")
                collection.bulkSave(batch, onError="ignore")
                saved += len(batch)
            # Fall back to import_bulk if available
            elif hasattr(collection, "import_bulk"):
                logger.debug("Using import_bulk method")
                result = collection.import_bulk(
                    batch, overwrite=False, on_duplicate="ignore"
                )
                saved += _handle_import_bulk_result(result)
            else:
                raise AttributeError("Collection object has no bulk insertion method")
        except Exception as e:
            logger.warning(f"Some documents may have been skipped: {e!s}")
            raise  # Re-raise for retry

    logger.debug(f"Saved {saved} documents")
    return saved


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
    """
    return batch_save_documents(nodes_col, nodes, batch_size)


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
    """
    return batch_save_documents(edges_col, edges, batch_size)


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


def split_json_file(
    filename: str, chunk_size_mb: int = 100
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
    current_chunk: dict[str, list[dict[str, Any]]] = {"nodes": [], "edges": []}
    current_size = 0
    chunk_number = 0

    def write_chunk() -> str:
        nonlocal chunk_number
        chunk_file = os.path.join(temp_dir, f"chunk_{chunk_number}.json")
        with open(chunk_file, "w") as f:
            json.dump(current_chunk, f)
        chunk_number += 1
        return chunk_file

    try:
        with open(filename) as f:
            for line in f:
                try:
                    item = json.loads(line.strip())
                    if not isinstance(item, dict):
                        continue

                    # Determine if it's a node or edge based on the item type
                    item_type = item.get("type", "").lower()
                    if item_type == "node":
                        current_chunk["nodes"].append(item)
                    elif item_type in ["edge", "relationship"]:
                        # Convert Neo4j relationship format to ArangoDB edge format
                        if item_type == "relationship":
                            # Extract start and end nodes for the edge
                            start_node = item.get("start", {})
                            end_node = item.get("end", {})

                            # Create edge document with required _from and _to fields
                            edge_doc = {
                                "_from": f"nodes/{start_node.get('id', '')}",
                                "_to": f"nodes/{end_node.get('id', '')}",
                                "type": item.get(
                                    "label", ""
                                ),  # Use relationship label as edge type
                                "properties": item.get("properties", {}),
                            }
                            current_chunk["edges"].append(edge_doc)
                        else:
                            current_chunk["edges"].append(item)
                    current_size += len(line)

                    if current_size >= chunk_size:
                        yield write_chunk()
                        current_chunk = {"nodes": [], "edges": []}
                        current_size = 0
                except json.JSONDecodeError as e:
                    logger.warning(f"Skipping invalid JSON line: {e!s}")
                    continue

        # Write final chunk if there's data
        if current_chunk["nodes"] or current_chunk["edges"]:
            yield write_chunk()

    except Exception as e:
        logger.error(f"Error splitting file: {e!s}", exc_info=True)
        raise
    finally:
        pass  # Cleanup will be handled by the caller


def ensure_collections(db: ArangoDatabase) -> None:
    """Ensure required collections exist in the database.

    Args:
        db: ArangoDB database connection
    """
    collections = db.collections()
    if isinstance(collections, list | dict):
        collection_names = [c["name"] for c in collections]
    else:
        collection_names = []

    if "nodes" not in collection_names:
        db.create_collection("nodes")
    if "edges" not in collection_names:
        db.create_collection("edges", edge=True)


def process_chunk_data(
    db: ArangoDatabase, chunk_data: dict[str, Any], batch_size: int
) -> tuple[int, int]:
    """Process chunk data and insert into database.

    Args:
        db: ArangoDB database connection
        chunk_data: Data to process
        batch_size: Size of batches for processing

    Returns:
        Tuple[int, int]: Number of nodes and edges added
    """
    nodes_added = 0
    edges_added = 0

    nodes_col = db["nodes"]
    edges_col = db["edges"]

    # Process nodes in batches
    nodes = chunk_data.get("nodes", [])
    if nodes:
        logger.info(f"Processing {len(nodes)} nodes...")
    for i in range(0, len(nodes), batch_size):
        batch = nodes[i : i + batch_size]
        if batch:
            added = process_nodes_batch(nodes_col, batch, batch_size)
            nodes_added += added
            logger.info(
                f"Nodes {i + 1}-{min(i + batch_size, len(nodes))}: "
                f"Added {added} of {len(nodes)}"
            )

    # Process edges in batches
    edges = chunk_data.get("edges", [])
    if edges:
        logger.info(f"Processing {len(edges)} edges...")
    for i in range(0, len(edges), batch_size):
        batch = edges[i : i + batch_size]
        if batch:
            added = process_edges_batch(edges_col, batch, batch_size)
            edges_added += added
            logger.info(
                f"Edges {i + 1}-{min(i + batch_size, len(edges))}: "
                f"Added {added} of {len(edges)}"
            )

    return nodes_added, edges_added


def process_chunk(
    filename: str,
    db_config: dict[str, Any],
    chunk_number: int,
    total_chunks: int,
    progress_queue: Any | None = None,
) -> tuple[int, int]:
    """Process a chunk of the file and insert into database.

    Args:
        filename: Path to the input file
        db_config: Database configuration
        chunk_number: Current chunk number
        total_chunks: Total number of chunks
        progress_queue: Queue to report progress

    Returns:
        Tuple[int, int]: Number of nodes and edges added
    """
    logger.info(f"Processing chunk {chunk_number + 1}/{total_chunks}")
    nodes_added = 0
    edges_added = 0

    try:
        with ArangoConnection(**db_config).get_connection() as db:
            ensure_collections(db)

            # Calculate chunk bounds
            file_size = os.path.getsize(filename)
            chunk_size = file_size // total_chunks
            start = chunk_number * chunk_size
            end = start + chunk_size if chunk_number < total_chunks - 1 else file_size

            # Process the chunk
            with open(filename, "rb") as f:
                f.seek(start)
                # Read to the next newline if not at the start
                if start > 0:
                    f.readline()

                while f.tell() < end:
                    line = f.readline().decode("utf-8").strip()
                    if not line:
                        continue

                    try:
                        doc = json.loads(line)
                        if doc.get("type") == "node":
                            nodes_added += batch_save_documents(db["nodes"], [doc], 1)
                            if progress_queue is not None:
                                progress_queue.put(("node", 1))
                        elif doc.get("type") in ["edge", "relationship"]:
                            edges_added += batch_save_documents(db["edges"], [doc], 1)
                            if progress_queue is not None:
                                progress_queue.put(("edge", 1))
                    except json.JSONDecodeError:
                        logger.warning(f"Invalid JSON in line: {line[:100]}...")
                        continue
                    except Exception as e:
                        logger.error(f"Error processing document: {e}")
                        continue

    except Exception as e:
        logger.error(f"Error processing chunk {chunk_number}: {e}")

    return nodes_added, edges_added


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


def parallel_load_data(
    file_path: str | Path,
    db_config: dict[str, Any],
    num_processes: int | None = None,
    max_processes: int | None = None,
) -> tuple[int, int]:
    """Load data from a JSON file in parallel.

    Args:
        file_path: Path to JSON file
        db_config: Database configuration
        num_processes: Number of processes to use. Defaults to CPU count - 1
            up to max_processes limit.
        max_processes: Maximum number of processes to use. Defaults to CPU count.
            Higher values speed up processing but use more resources.
            Lower values are more stable but slower.

    Returns:
        Tuple[int, int]: Number of nodes and edges added
    """
    file_path = str(file_path)
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    # Determine number of processes
    if max_processes is None:
        max_processes = multiprocessing.cpu_count()

    if num_processes is None:
        num_processes = min(multiprocessing.cpu_count() - 1, max_processes)
    else:
        num_processes = min(num_processes, max_processes)

    # Create a manager for shared resources
    with multiprocessing.Manager() as manager:
        # Create a queue for progress tracking
        progress_queue = manager.Queue()

        # Create process pool and distribute chunks
        logger.info(f"Starting import with {num_processes} processes")
        total_nodes_added = 0
        total_edges_added = 0
        nodes_processed = 0
        edges_processed = 0

        with multiprocessing.Pool(num_processes) as pool:
            # Create chunk arguments
            chunk_args = [
                (file_path, db_config, i, num_processes, progress_queue)
                for i in range(num_processes)
            ]

            # Start processing chunks
            results = pool.starmap_async(process_chunk, chunk_args)

            # Monitor progress while chunks are being processed
            while not results.ready():
                try:
                    doc_type, count = progress_queue.get(timeout=1)
                    if doc_type == "node":
                        nodes_processed += count
                        logger.info(f"Nodes processed: {nodes_processed:,}")
                    else:
                        edges_processed += count
                        logger.info(f"Edges processed: {edges_processed:,}")
                except queue.Empty:
                    continue

            # Get final results
            chunk_results = results.get()
            for nodes, edges in chunk_results:
                total_nodes_added += nodes
                total_edges_added += edges

    msg = "Import complete. Added {:,} nodes and {:,} edges"
    logger.info(msg.format(total_nodes_added, total_edges_added))
    return total_nodes_added, total_edges_added
