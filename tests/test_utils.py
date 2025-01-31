"""Test utility functions."""

import json
import os
import tempfile
from pathlib import Path

import pytest

from arangoimport.utils import (
    adjust_batch_size,
    chunk_file,
    detect_file_type,
    detect_json_type,
    format_size,
    get_available_memory,
    retry_with_backoff,
    validate_document,
)

# Constants for test values
MAX_RETRIES = 3
TEST_CHUNKS = 4
RETRY_ATTEMPTS = 2
MEMORY_SIZES = [
    (1024 * 1024, 100),  # 1 MB
    (1024 * 1024 * 1024, 1000),  # 1 GB
    (10 * 1024 * 1024 * 1024, 10000),  # 10 GB
]
TEST_LINES = 100


def test_retry_with_backoff():
    """Test retry with backoff decorator."""
    counter = 0

    @retry_with_backoff(max_retries=MAX_RETRIES)
    def failing_function():
        nonlocal counter
        counter += 1
        if counter < MAX_RETRIES:
            raise ValueError("Test error")
        return "success"

    # Test successful retry
    counter = 0
    result = failing_function()
    assert result == "success"
    assert counter == MAX_RETRIES

    # Test maximum retries exceeded
    counter = 0

    @retry_with_backoff(max_retries=RETRY_ATTEMPTS)
    def always_fails():
        nonlocal counter
        counter += 1
        raise ValueError("Always fails")

    with pytest.raises(ValueError):
        always_fails()
    assert counter == RETRY_ATTEMPTS


def test_get_available_memory():
    """Test getting available memory."""
    memory = get_available_memory()
    assert isinstance(memory, int)
    assert memory > 0


def test_adjust_batch_size():
    """Test batch size adjustment."""
    # Test with different memory sizes
    for memory, expected_min in MEMORY_SIZES:
        batch_size = adjust_batch_size(memory)
        assert isinstance(batch_size, int)
        assert batch_size >= expected_min


def test_chunk_file():
    """Test file chunking."""
    # Create test file
    with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
        f.write("test" * 100)  # Write some test data
        f.flush()

        # Test chunking
        chunks = chunk_file(f.name, TEST_CHUNKS)
        assert len(chunks) == TEST_CHUNKS

        # Verify chunks cover entire file
        file_size = os.path.getsize(f.name)
        assert chunks[0][0] == 0  # First chunk starts at 0
        assert chunks[-1][1] == file_size  # Last chunk ends at file size

        # Clean up
        os.unlink(f.name)


def test_detect_file_type():
    """Test file type detection."""
    # Test JSON file
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump({"test": "data"}, f)
        f.flush()
        assert detect_file_type(f.name) == "json"
        assert detect_file_type(Path(f.name)) == "json"
    os.unlink(f.name)

    # Test JSONL file
    with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
        f.write('{"test": "line1"}\n{"test": "line2"}\n')
        f.flush()
        assert detect_file_type(f.name) == "jsonl"
    os.unlink(f.name)

    # Test invalid file
    with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
        f.write("not json")
        f.flush()
        with pytest.raises(ValueError):
            detect_file_type(f.name)
    os.unlink(f.name)


def test_detect_json_type():
    """Test JSON type detection."""
    # Test single document
    assert detect_json_type({"test": "data"}) == "json"

    # Test multiple documents
    assert detect_json_type([{"test": "data1"}, {"test": "data2"}]) == "jsonl"

    # Test invalid types
    with pytest.raises(ValueError):
        detect_json_type("not json")
    with pytest.raises(ValueError):
        detect_json_type([1, 2, 3])


def test_validate_document():
    """Test document validation."""
    # Valid documents
    assert validate_document({"_id": "test"})
    assert validate_document({"_key": "test"})
    assert validate_document({"_id": "test", "_key": "test"})

    # Invalid documents
    with pytest.raises(ValueError):
        validate_document({})
    with pytest.raises(ValueError):
        validate_document({"other": "field"})


def test_format_size():
    """Test size formatting."""
    sizes = [
        (0, "0 B"),
        (1023, "1023 B"),
        (1024, "1.0 KB"),
        (1024 * 1024, "1.0 MB"),
        (1024 * 1024 * 1024, "1.0 GB"),
        (1024 * 1024 * 1024 * 1024, "1.0 TB"),
    ]
    for size, expected in sizes:
        assert format_size(size) == expected
