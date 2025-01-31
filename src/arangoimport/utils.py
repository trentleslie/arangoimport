"""Utility functions for arangoimport."""

import time
from collections.abc import Callable
from pathlib import Path
from typing import Any, NotRequired, TypedDict, TypeVar

import psutil

from .logging import get_logger

logger = get_logger(__name__)

# Constants
BYTES_PER_UNIT = 1024
MEMORY_USAGE_FRACTION = 0.75  # Use 75% of available memory
MEMORY_PER_DOC = 10 * 1024  # Estimate 10KB per document

T = TypeVar("T")


class ArangoDocument(TypedDict):
    """Type for ArangoDB documents."""

    _id: NotRequired[str]
    _key: NotRequired[str]


def retry_with_backoff(
    max_retries: int = 5,
    initial_wait: float = 1.0,
    max_wait: float = 60.0,
    backoff_factor: float = 2.0,
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """Retry a function with exponential backoff."""

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        def wrapper(*args: Any, **kwargs: Any) -> T:
            wait = initial_wait
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_retries - 1:
                        raise
                    logger.warning(f"Attempt {attempt + 1} failed: {e!s}. Retrying...")
                    time.sleep(min(wait, max_wait))
                    wait *= backoff_factor
            raise RuntimeError("Maximum retries exceeded")  # This will never be reached

        return wrapper

    return decorator


def get_available_memory() -> int:
    """Get available system memory in bytes using MEMORY_USAGE_FRACTION.

    Returns:
        int: Available memory in bytes for our process to use
    """
    mem = psutil.virtual_memory()
    return int(mem.available * MEMORY_USAGE_FRACTION)


def adjust_batch_size(available_memory: int) -> int:
    """Dynamically adjust batch size based on available memory.

    Args:
        available_memory: Available memory in bytes

    Returns:
        int: Adjusted batch size
    """
    # Use at most 10% of available memory for batching to be conservative
    base_size = (available_memory // 10) // MEMORY_PER_DOC

    # Scale batch size with memory, but keep within reasonable limits
    if available_memory < 1024 * 1024 * 1024:  # < 1GB
        return min(500, max(100, base_size))
    elif available_memory < 10 * 1024 * 1024 * 1024:  # < 10GB
        return min(5000, max(500, base_size))
    else:  # >= 10GB
        return min(50000, max(5000, base_size))


def chunk_file(filename: str, num_chunks: int) -> list[tuple[int, int]]:
    """Split file into chunks and return list of (start, end) byte positions.
    Ensures each chunk fits within memory limits.

    Args:
        filename: Path to the file to chunk
        num_chunks: Number of chunks to split the file into

    Returns:
        List[Tuple[int, int]]: List of (start, end) byte positions
    """
    file_size = Path(filename).stat().st_size
    available_memory = get_available_memory()

    # Calculate maximum chunk size based on memory limits
    # Use at most 20% of available memory per chunk
    max_chunk_size = available_memory // 5

    # Calculate number of chunks needed to stay within memory limits
    min_chunks_needed = max(1, file_size // max_chunk_size)
    num_chunks = max(num_chunks, min_chunks_needed)

    chunk_size = file_size // num_chunks
    chunks = []

    for i in range(num_chunks):
        start = i * chunk_size
        # Handle the last chunk
        end = file_size if i == num_chunks - 1 else (i + 1) * chunk_size
        chunks.append((start, end))

    total_mem = psutil.virtual_memory()
    logger.info(f"Split {format_size(file_size)} file into {len(chunks)} chunks")

    chunk_msg = f"Chunk size: ~{format_size(chunk_size)}"
    chunk_msg += f" (max allowed: {format_size(max_chunk_size)})"
    logger.info(chunk_msg)

    mem_msg = f"Memory limit: {format_size(available_memory)}"
    mem_msg += f" ({int(MEMORY_USAGE_FRACTION * 100)}% of available: "
    mem_msg += f"{format_size(total_mem.available)})"
    logger.info(mem_msg)

    return chunks


def detect_file_type(file_path: str | Path) -> str:
    """Detect the type of input file.

    Args:
        file_path: Path to input file

    Returns:
        str: Detected file type ('json' or 'jsonl')

    Raises:
        ValueError: If file type is not supported
    """
    file_path = Path(file_path)
    suffix = file_path.suffix.lower()

    if suffix == ".json":
        return "json"
    elif suffix in (".jsonl", ".ndjson"):
        return "jsonl"
    else:
        raise ValueError(
            f"Unsupported file type: {suffix}. "
            "Supported types are: .json, .jsonl, .ndjson"
        )


def detect_json_type(data: Any) -> str:
    """Detect the type of JSON data.

    If it is a dictionary, treat it as JSON.
    If it is a list of dictionaries, treat it as JSON Lines.
    Raises ValueError if neither.

    Args:
        data: The data to check, either a dict or list of dicts

    Returns:
        str: 'json' for dict, 'jsonl' for list

    Raises:
        ValueError: If data is neither dict nor list
    """
    if isinstance(data, dict):
        return "json"
    elif isinstance(data, list) and all(isinstance(item, dict) for item in data):
        return "jsonl"
    else:
        raise ValueError(
            "Data must be either a dictionary (JSON) or "
            "a list of dictionaries (JSON Lines)"
        )


def validate_document(doc: ArangoDocument) -> bool:
    """Validate a document before import.

    This function performs runtime validation of document fields required by ArangoDB.
    The document must have either '_id' or '_key' field.

    Args:
        doc: Document to validate

    Returns:
        bool: Always returns True if validation passes

    Raises:
        ValueError: If document is missing required fields
    """
    if "_id" not in doc and "_key" not in doc:
        raise ValueError("Document must have either '_id' or '_key'")
    return True


def format_size(size: float) -> str:
    """Format size in bytes to human readable string."""
    if size == 0:
        return "0 B"
    elif size < BYTES_PER_UNIT:
        return f"{size:.0f} B"

    units = ["B", "KB", "MB", "GB", "TB"]
    unit_index = 0

    while size >= BYTES_PER_UNIT and unit_index < len(units) - 1:
        size /= BYTES_PER_UNIT
        unit_index += 1

    return f"{size:.1f} {units[unit_index]}"
