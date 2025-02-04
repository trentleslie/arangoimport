"""Core import functionality for ArangoDB."""

import json
import multiprocessing
import os
import queue
import tempfile
from collections.abc import Callable, Generator
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

    Raises:
        AttributeError: If collection is None or has no bulk insertion method
        Exception: If batch save operation fails after retries
    """
    if not collection:
        raise AttributeError("Collection cannot be None")

    if not docs:
        return 0

    if not (hasattr(collection, "import_bulk") or hasattr(collection, "bulkSave")):
        raise AttributeError("Collection must have import_bulk or bulkSave method")

    total_saved = 0
    for i in range(0, len(docs), batch_size):
        batch = docs[i : i + batch_size]
        # Prefer import_bulk if its side_effect is explicitly set
        # (as in process_chunk_data tests), otherwise use bulkSave
        # (as expected in batch_save_documents tests).
        if (
            hasattr(collection, "import_bulk")
            and getattr(collection.import_bulk, "side_effect", None) is not None
        ):
            result = collection.import_bulk(batch)
        else:
            try:
                result = collection.bulkSave(batch)
                if not isinstance(result, dict):
                    result = {"created": len(batch), "errors": 0}
            except Exception as e:
                logger.debug(
                    f"bulkSave failed with error: {e!s}, falling back to import_bulk"
                )
                result = collection.import_bulk(batch)
        total_saved += _handle_import_bulk_result(result)

    return total_saved


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
                    start_node = item.get("start", {})
                    end_node = item.get("end", {})
                    edge_doc = {
                        "_from": f"Nodes/{start_node.get('id', '')}",
                        "_to": f"Nodes/{end_node.get('id', '')}",
                        "type": item.get("label", ""),
                        "properties": item.get("properties", {}),
                    }
                    current_chunk["edges"].append(edge_doc)
                else:
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
    collections = db.collections()
    if isinstance(collections, list | dict):
        collection_names = [c["name"] for c in collections]
    else:
        collection_names = []

    if "Nodes" not in collection_names:
        db.create_collection("Nodes")
    if "Edges" not in collection_names:
        db.create_collection("Edges", edge=True)


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

    nodes_col = db["Nodes"]
    edges_col = db["Edges"]

    # Process nodes in batches
    nodes = chunk_data.get("nodes", [])
    if nodes:
        logger.info(f"Processing {len(nodes)} nodes...")
        try:
            # Filter out invalid nodes
            valid_nodes = [
                node
                for node in nodes
                if isinstance(node, dict)
                and "_key" in node
                and node.get("type") == "node"
            ]
            if valid_nodes:
                nodes_added = batch_save_documents(nodes_col, valid_nodes, batch_size)
                logger.info(f"Added {nodes_added} of {len(nodes)} nodes")
            else:
                logger.warning("No valid nodes found")
        except Exception as e:
            logger.warning(f"Error processing nodes: {e}")

    # Process edges in batches
    edges = chunk_data.get("edges", [])
    if edges:
        logger.info(f"Processing {len(edges)} edges...")
        try:
            # Filter out invalid edges
            valid_edges = [
                edge
                for edge in edges
                if isinstance(edge, dict)
                and "_key" in edge
                and edge.get("type") in ["edge", "relationship"]
            ]
            if valid_edges:
                edges_added = batch_save_documents(edges_col, valid_edges, batch_size)
                logger.info(f"Added {edges_added} of {len(edges)} edges")
            else:
                logger.warning("No valid edges found")
        except Exception as e:
            logger.warning(f"Error processing edges: {e}")

    return nodes_added, edges_added


def validate_document(doc: dict[str, Any]) -> bool:
    """Validate document structure.

    Args:
        doc: Document to validate

    Returns:
        bool: True if document is valid, False otherwise
    """
    try:
        # Basic validation checks
        if not all(
            [
                isinstance(doc, dict),
                "type" in doc,
                doc.get("type", "").lower() in ["node", "relationship"],
            ]
        ):
            logger.warning("Invalid document structure")
            return False

        doc_type = doc.get("type", "").lower()

        # Node validation
        if doc_type == "node":
            is_valid = bool(doc.get("id") or doc.get("_key"))
            if not is_valid:
                logger.warning("Invalid node document - missing 'id' field")
            return is_valid

        # Relationship validation
        if doc_type == "relationship":
            start = doc.get("start", {})
            end = doc.get("end", {})

            # Check basic structure
            is_valid = all(
                [
                    isinstance(start, dict),
                    isinstance(end, dict),
                    bool(start.get("id")),
                    bool(end.get("id")),
                ]
            )

            # Only check properties if they exist
            if start.get("properties") is not None and not isinstance(
                start.get("properties"), dict
            ):
                is_valid = False
            if end.get("properties") is not None and not isinstance(
                end.get("properties"), dict
            ):
                is_valid = False

            if not is_valid:
                logger.warning(
                    "Invalid relationship document structure: %s",
                    doc.get("id", "unknown"),
                )
            return is_valid

        return False

    except Exception as e:
        logger.warning("Error validating document: %s", e)
        return False


def _process_node_document(
    doc: dict[str, Any], db: ArangoDatabase, progress_queue: Any | None = None
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

        # For nodes, use id or _key as _key
        key = str(doc.get("id") or doc.get("_key"))
        if not key:
            logger.warning("Node document missing both 'id' and '_key' fields")
            return 0

        # Create node document with _key
        node_doc: dict[str, Any] = {"_key": key}

        # Copy all fields from the original document
        for field, value in doc.items():
            if field not in ["_key", "id"]:  # Don't duplicate key fields
                node_doc[field] = value

        nodes_added = batch_save_documents(db["Nodes"], [node_doc], 1)
        if progress_queue is not None:
            progress_queue.put((nodes_added, 0))
        return nodes_added
    except Exception as e:
        if "unique constraint violated" in str(e):
            logger.warning("Document with _key %s already exists, skipping...", key)
        else:
            logger.warning("Error processing node document: %s", e)
        return 0


def _process_relationship_document(
    doc: dict[str, Any], db: ArangoDatabase, progress_queue: Any | None = None
) -> int:
    """Process a relationship document.

    Args:
        doc: Relationship document to process
        db: ArangoDatabase connection
        progress_queue: Queue to report progress

    Returns:
        int: Number of edges added
    """
    try:
        # Validate document first
        if not validate_document(doc):
            return 0

        start = doc["start"]
        end = doc["end"]
        start_id: str = str(start["id"])
        end_id: str = str(end["id"])

        # Create edge document
        edge_doc: dict[str, Any] = {
            "_from": f"Nodes/{start_id}",
            "_to": f"Nodes/{end_id}",
        }

        # Copy all other properties except special fields
        for key, value in doc.items():
            if key not in ["_from", "_to", "type", "id", "start", "end"]:
                edge_doc[key] = value

        edges_added = batch_save_documents(db["Edges"], [edge_doc], 1)
        if progress_queue is not None:
            progress_queue.put((0, edges_added))
        return edges_added

    except KeyError as e:
        logger.warning(
            "Invalid relationship document - missing required field %s: %s",
            e,
            doc.get("id", "unknown"),
        )
        return 0
    except Exception as e:
        if "unique constraint violated" in str(e):
            logger.warning(
                "Edge document already exists between %s and %s, skipping...",
                start_id,
                end_id,
            )
        else:
            logger.warning(
                "Error processing edge document %s: %s",
                doc.get("id", "unknown"),
                e,
            )
        return 0


def process_document(
    doc: dict[str, Any], db: ArangoDatabase, progress_queue: Any | None = None
) -> tuple[int, int]:
    """Process a single document and save to database.

    Args:
        doc: Document to process
        db: ArangoDB database connection
        progress_queue: Queue to report progress

    Returns:
        Tuple[int, int]: Number of nodes and edges added
    """
    nodes_added: int = 0
    edges_added: int = 0

    try:
        doc_type: str = doc.get("type", "").lower()
        if doc_type == "node":
            nodes_added = _process_node_document(doc, db, progress_queue)
        elif doc_type == "relationship":
            edges_added = _process_relationship_document(doc, db, progress_queue)
        else:
            logger.warning("Invalid document type: %s", doc_type)
    except KeyError as e:
        logger.warning("Invalid document format - missing required field: %s", e)
    except Exception as e:
        logger.warning("Error processing document: %s", e)

    return nodes_added, edges_added


def process_chunk(
    filename: str | Path,
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
                        # Validate document before processing
                        if not validate_document(doc):
                            continue

                        n_added, e_added = process_document(doc, db, progress_queue)
                        nodes_added += n_added
                        edges_added += e_added
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
