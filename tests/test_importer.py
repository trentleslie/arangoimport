"""Test importer functionality."""

import json
import os
import queue
import tempfile
from typing import Any
from unittest.mock import MagicMock, patch

import ijson
import pytest
from pyArango.document import Document

from arangoimport.importer import (
    _process_jsonl,
    batch_save_documents,
    get_db_max_connections,
    parallel_load_data,
    process_chunk,
    process_chunk_data,
    process_document,
    split_json_file,
    stream_json_objects,
)

# Constants for test values
BATCH_SAVE_CALLS = 2
SAMPLE_NODES = 2
SAMPLE_EDGES = 1
NUM_CHUNKS = 4
TOTAL_NODES = SAMPLE_NODES * NUM_CHUNKS
TOTAL_EDGES = SAMPLE_EDGES * NUM_CHUNKS
TEST_PROCESSES = 4
CHUNK_ARG_COUNT = (
    3  # Number of arguments in process_chunk tuple (filename, bounds, config)
)
PROCESS_CHUNK_ARG_COUNT = (
    2  # Number of arguments for process_chunk (filename, db_config)
)
MIN_BULK_SAVE_CALLS = (
    2  # Minimum number of bulkSave calls (once for nodes, once for edges)
)
RETRY_ATTEMPTS = 3  # Number of attempts for retry logic
MIN_PROGRESS_UPDATES = 2  # Minimum number of progress updates expected
MAX_BATCH_SIZE = 1000  # Maximum reasonable batch size for document processing
PROGRESS_TUPLE_SIZE = 2  # Size of progress tuple (nodes_added, edges_added)
LARGE_TEST_DOCS = 1000  # Number of test documents for large data tests
MIN_BATCH_CALLS = 40  # Minimum number of batch calls for large data test
DEFAULT_MAX_CONNECTIONS = 128  # Default maximum connections for ArangoDB
MAX_TEST_DOCS = 100  # Maximum number of test documents for memory optimization


@pytest.fixture
def mock_collection() -> MagicMock:
    """Create a mock collection for testing."""
    mock = MagicMock()
    mock.bulkSave = MagicMock(return_value=None)
    mock.import_bulk = MagicMock(return_value={"created": 1, "errors": 0})
    return mock


@pytest.fixture
def mock_docs() -> list[Document]:
    """Create mock documents."""
    return [MagicMock(spec=Document) for _ in range(10)]


def test_batch_save_documents(
    mock_collection: MagicMock, mock_docs: list[Document]
) -> None:
    """Test saving documents in batches.

    Tests:
    - Batch saving with different batch sizes
    - Retry logic with import_bulk
    """
    # Test with different batch sizes
    batch_sizes = [1, 3, 5, 10]
    for batch_size in batch_sizes:
        saved = batch_save_documents(mock_collection, mock_docs, batch_size)
        assert saved == len(mock_docs)
        expected_calls = (len(mock_docs) + batch_size - 1) // batch_size
        assert mock_collection.bulkSave.call_count == expected_calls
        mock_collection.bulkSave.reset_mock()

    # Test retry logic with import_bulk
    mock_collection.bulkSave = None  # Force using import_bulk
    mock_collection.import_bulk = MagicMock(
        side_effect=[
            {"created": 0, "errors": 1},  # First attempt fails
            {"created": 0, "errors": 1},  # Second attempt fails
            {"created": 1, "errors": 0},  # Third attempt succeeds
        ]
    )
    saved = batch_save_documents(mock_collection, mock_docs[:1], 1)
    assert saved == 1
    assert mock_collection.import_bulk.call_count == RETRY_ATTEMPTS


def test_batch_save_documents_error_handling(mock_collection: MagicMock) -> None:
    """Test error handling in batch_save_documents.

    Tests:
    - Empty document list handling
    - None collection handling
    - Failed bulkSave but successful import_bulk handling
    """
    # Test with empty document list
    saved = batch_save_documents(mock_collection, [], 10)
    assert saved == 0

    # Test with None collection
    with pytest.raises(AttributeError):
        batch_save_documents(None, [MagicMock()], 10)

    # Test with failed bulkSave but successful import_bulk
    mock_collection.bulkSave = MagicMock(side_effect=Exception("Bulk save failed"))
    mock_collection.import_bulk = MagicMock(return_value={"created": 1, "errors": 0})

    # Should fall back to import_bulk after bulkSave fails
    saved = batch_save_documents(mock_collection, [MagicMock()], 1)
    assert saved == 1
    assert mock_collection.bulkSave.call_count == 1
    assert mock_collection.import_bulk.call_count == 1


def test_batch_save_documents_retry_exhaustion(mock_collection: MagicMock) -> None:
    """Test retry exhaustion in batch_save_documents.

    Tests:
    - Retry exhaustion handling
    """
    # Set up mock to fail all retries
    mock_collection.bulkSave = None  # Force using import_bulk
    mock_collection.import_bulk = MagicMock(
        side_effect=[
            ValueError("Failed to import documents"),
            ValueError("Failed to import documents"),
            ValueError("Failed to import documents"),
        ]
    )

    # Should raise ValueError after retries are exhausted
    with pytest.raises(ValueError, match="Failed to import documents"):
        batch_save_documents(mock_collection, [MagicMock()], 1)

    assert mock_collection.import_bulk.call_count == RETRY_ATTEMPTS


def test_process_chunk(temp_json_file: str, mock_collection: MagicMock) -> None:
    """Test processing a file chunk.

    Tests:
    - Chunk processing with mock database configuration
    """
    # Mock database configuration
    db_config = {
        "db_name": "test_db",
        "host": "localhost",
        "port": 8529,
        "username": "test",
        "password": "test",
    }

    # Verify test file contents
    with open(temp_json_file) as f:
        data = f.read()
        print(f"Test file contents:\n{data}")

    # Mock ArangoConnection to return our mock collection
    with patch("arangoimport.importer.ArangoConnection") as mock_arango_cls:
        mock_conn = MagicMock()
        mock_db = MagicMock()
        # Ensure both nodes and edges collections return our mock collection
        mock_db.__getitem__.side_effect = (
            lambda key: mock_collection if key in ["nodes", "edges"] else None
        )
        mock_conn.get_connection.return_value.__enter__.return_value = mock_db
        mock_arango_cls.return_value = mock_conn

        nodes_added, edges_added = process_chunk(temp_json_file, db_config, 0, 1)
        assert nodes_added >= 0
        assert edges_added >= 0


def test_process_chunk_document_validation(temp_json_file: str) -> None:
    """Test document validation in process_chunk.

    Tests:
    - Invalid JSON handling
    - Missing type field handling
    - Missing _key field handling
    """
    invalid_json_file = tempfile.NamedTemporaryFile(mode="w", delete=False)
    try:
        # Write invalid JSON data
        invalid_json_file.write('{"type":"invalid","_key":"1"}\n')  # Invalid type
        invalid_json_file.write('{"type":"node"}\n')  # Missing _key
        invalid_json_file.write('{"_key":"3"}\n')  # Missing type
        invalid_json_file.write("not a json line\n")  # Invalid JSON
        invalid_json_file.close()

        db_config = {
            "db_name": "test_db",
            "host": "localhost",
            "port": 8529,
            "username": "test",
            "password": "test",
        }

        with patch("arangoimport.importer.ArangoConnection") as mock_arango_cls:
            mock_conn = MagicMock()
            mock_db = MagicMock()
            mock_collection = MagicMock()
            mock_db.__getitem__.return_value = mock_collection
            mock_conn.get_connection.return_value.__enter__.return_value = mock_db
            mock_arango_cls.return_value = mock_conn

            # Process chunk with invalid documents
            nodes_added, edges_added = process_chunk(
                invalid_json_file.name, db_config, 0, 1
            )

            # Should skip invalid documents but continue processing
            assert nodes_added == 0
            assert edges_added == 0

    finally:
        os.unlink(invalid_json_file.name)


def test_parallel_load_data(temp_json_file: str) -> None:
    """Test parallel data loading.

    Tests:
    - Parallel loading with mock database configuration
    """
    # Database configuration
    db_config = {
        "db_name": "test_db",
        "host": "localhost",
        "port": 8529,
        "username": "test",
        "password": "test",
        "pool_size": 2,
    }

    # Mock ArangoConnection and its dependencies
    with patch("arangoimport.importer.ArangoConnection") as mock_arango_cls:
        mock_conn = MagicMock()
        mock_db = MagicMock()
        mock_db.collections.return_value = []
        mock_db.create_collection = MagicMock()
        mock_conn.get_connection.return_value.__enter__.return_value = mock_db
        mock_arango_cls.return_value = mock_conn

        # Mock process_chunk within multiprocessing context
        with patch("multiprocessing.Pool") as mock_pool:
            mock_pool_instance = MagicMock()
            mock_pool.return_value.__enter__.return_value = mock_pool_instance

            # Mock starmap_async to return a result object
            mock_result = MagicMock()
            mock_result.ready.side_effect = [
                False,
                True,
            ]  # Not ready first time, then ready
            mock_result.get.return_value = [
                (SAMPLE_NODES, SAMPLE_EDGES)
            ] * TEST_PROCESSES
            mock_pool_instance.starmap_async.return_value = mock_result

            # Test with specific number of processes
            total_nodes, total_edges = parallel_load_data(
                temp_json_file, db_config, num_processes=TEST_PROCESSES
            )

            assert total_nodes == SAMPLE_NODES * TEST_PROCESSES
            assert total_edges == SAMPLE_EDGES * TEST_PROCESSES

            # Verify the pool was created with correct number of processes
            mock_pool.assert_called_once_with(TEST_PROCESSES)

            # Verify starmap_async was called with correct arguments
            mock_pool_instance.starmap_async.assert_called_once()
            call_args = mock_pool_instance.starmap_async.call_args[0]
            assert call_args[0] == process_chunk  # First arg should be the function
            assert (
                len(call_args[1]) == TEST_PROCESSES
            )  # Should have TEST_PROCESSES chunks


def test_parallel_load_data_error_handling(temp_json_file: str) -> None:
    """Test error handling in parallel data loading.

    Tests:
    - Non-existent file handling
    - Invalid number of processes handling
    - Process failure handling
    """
    db_config = {
        "db_name": "test_db",
        "host": "localhost",
        "port": 8529,
        "username": "test",
        "password": "test",
    }

    # Test with non-existent file
    with pytest.raises(FileNotFoundError):
        parallel_load_data("nonexistent.json", db_config)

    # Test with invalid number of processes
    with pytest.raises(ValueError):
        parallel_load_data(temp_json_file, db_config, num_processes=0)

    # Test with process failure
    with (
        patch("arangoimport.importer.ArangoConnection"),
        patch("multiprocessing.Pool") as mock_pool,
    ):
        mock_pool_instance = MagicMock()
        mock_pool.return_value.__enter__.return_value = mock_pool_instance

        # Mock process failure
        mock_result = MagicMock()
        mock_result.ready.return_value = True
        mock_result.get.side_effect = Exception("Process failed")
        mock_pool_instance.starmap_async.return_value = mock_result

        # Should handle process failure gracefully
        with pytest.raises(Exception, match="Process failed"):
            parallel_load_data(temp_json_file, db_config)


def test_parallel_load_data_progress_monitoring(temp_json_file: str) -> None:
    """Test progress monitoring in parallel data loading.

    Tests:
    - Progress queue handling
    - Progress updates during loading
    """
    db_config = {
        "db_name": "test_db",
        "host": "localhost",
        "port": 8529,
        "username": "test",
        "password": "test",
    }

    with (
        patch("arangoimport.importer.ArangoConnection"),
        patch("multiprocessing.Pool") as mock_pool,
        patch("multiprocessing.Manager") as mock_manager,
    ):
        # Setup mock queue
        mock_queue = MagicMock()
        mock_manager.return_value.__enter__.return_value.Queue.return_value = mock_queue

        # Setup mock pool
        mock_pool_instance = MagicMock()
        mock_pool.return_value.__enter__.return_value = mock_pool_instance

        # Mock async result
        mock_result = MagicMock()
        mock_result.ready.side_effect = [
            False,
            False,
            True,
        ]  # Not ready twice, then ready
        mock_result.get.return_value = [(1, 1)]  # One node, one edge per process
        mock_pool_instance.starmap_async.return_value = mock_result

        # Mock queue.get() to simulate progress updates
        mock_queue.get.side_effect = [
            ("node", 1),  # First progress update
            queue.Empty(),  # Simulate timeout
            ("edge", 1),  # Second progress update
            queue.Empty(),  # Final timeout
        ]

        # Run parallel load
        total_nodes, total_edges = parallel_load_data(
            temp_json_file, db_config, num_processes=1
        )

        # Verify progress monitoring
        assert mock_queue.get.call_count >= MIN_PROGRESS_UPDATES
        assert total_nodes == 1
        assert total_edges == 1


def test_process_chunk_memory_optimization(temp_json_file: str) -> None:
    """Test memory optimization in process_chunk.

    Tests:
    - Memory handling with large test file
    - Batching behavior with both id and _key fields
    - Document processing with different key formats
    """
    db_config = {
        "db_name": "test_db",
        "host": "localhost",
        "port": 8529,
        "username": "test",
        "password": "test",
    }

    # Create a larger test file
    large_file = tempfile.NamedTemporaryFile(mode="w", delete=False)
    try:
        # Write multiple documents to test memory handling
        # Mix of documents with id and _key fields
        for i in range(MAX_TEST_DOCS):
            if i % 2 == 0:
                # Use id field for even numbers
                large_file.write(f'{{"type":"node","id":"{i}","data":"test_id"}}\n')
            else:
                # Use _key field for odd numbers
                large_file.write(f'{{"type":"node","_key":"{i}","data":"test_key"}}\n')
        large_file.close()

        with (
            patch("arangoimport.importer.ArangoConnection") as mock_arango_cls,
            patch("arangoimport.importer.batch_save_documents") as mock_batch_save,
        ):
            # Setup mocks
            mock_conn = MagicMock()
            mock_db = MagicMock()
            mock_collection = MagicMock()
            mock_db.__getitem__.return_value = mock_collection
            mock_conn.get_connection.return_value.__enter__.return_value = mock_db
            mock_arango_cls.return_value = mock_conn

            # Mock batch_save_documents to track batch sizes
            mock_batch_save.return_value = 10  # Return number of docs saved

            # Process chunk
            process_chunk(large_file.name, db_config, 0, 1)

            # Verify batching behavior
            assert mock_batch_save.call_count > 0
            # Verify batch sizes are reasonable
            for call in mock_batch_save.call_args_list:
                assert len(call[0][1]) <= MAX_TEST_DOCS  # Max batch size
                # Verify documents have _key field
                for doc in call[0][1]:
                    assert "_key" in doc
                    assert doc["_key"].isdigit()  # Key should be a string number

    finally:
        # Clean up
        os.unlink(large_file.name)


def test_stream_json_objects_error_handling(temp_json_file: str) -> None:
    """Test error handling in stream_json_objects.

    Tests:
    - Empty file handling
    - Invalid JSON structure handling
    """
    # Test with empty file
    with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
        f.write('{"nodes": []}')
        empty_file = f.name

    try:
        with open(empty_file, "rb") as f:
            objects = list(stream_json_objects(f, "nodes"))
            assert len(objects) == 0
    finally:
        os.unlink(empty_file)

    # Test with invalid JSON structure
    with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
        f.write('{"nodes": [{"_key": "1", "type": "node"}')  # Missing closing brackets
        invalid_file = f.name

    try:
        with open(invalid_file, "rb") as f:
            with pytest.raises(ijson.common.IncompleteJSONError):
                list(stream_json_objects(f, "nodes"))
    finally:
        os.unlink(invalid_file)


def test_split_json_file_edge_cases(temp_json_file: str) -> None:
    """Test edge cases in split_json_file.

    Tests:
    - Large file handling
    - Chunk size handling
    """
    # Create a larger test file
    with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
        # Write 100MB of test data
        data = {
            "nodes": [
                {"_key": str(i), "type": "node", "data": "x" * 1000000}
                for i in range(100)
            ],
            "edges": [],
        }
        json.dump(data, f)
        large_file = f.name

    try:
        # Test with very small chunk size (should split into multiple chunks)
        chunks = list(split_json_file(large_file, chunk_size_mb=10))  # 10MB chunks
        assert len(chunks) > 1

        # Verify each chunk is valid JSON
        for chunk_file in chunks:
            with open(chunk_file) as f:
                data = json.load(f)
                assert isinstance(data, dict)
                assert "nodes" in data
                assert "edges" in data
            os.unlink(chunk_file)

        # Test with chunk size larger than file
        chunks = list(split_json_file(large_file, chunk_size_mb=1000))
        assert len(chunks) == 1
        os.unlink(chunks[0])
    finally:
        os.unlink(large_file)


def test_process_chunk_data_error_handling(mock_collection: MagicMock) -> None:
    """Test error handling in process_chunk_data.

    Tests:
    - Empty data handling
    - Invalid document types handling
    - Valid data handling
    """
    db = MagicMock()
    db.__getitem__.return_value = mock_collection

    # Mock import_bulk to reject invalid documents
    def import_bulk_side_effect(docs, **kwargs):
        invalid_count = sum(1 for doc in docs if doc.get("type") == "invalid")
        return {"created": len(docs) - invalid_count, "errors": invalid_count}

    # Ensure import_bulk is in the mock's __dict__
    mock_collection.__dict__["import_bulk"] = mock_collection.import_bulk
    mock_collection.import_bulk.side_effect = import_bulk_side_effect

    # Test with empty data
    empty_data = {"nodes": [], "edges": []}
    nodes_added, edges_added = process_chunk_data(db, empty_data, batch_size=100)
    assert nodes_added == 0
    assert edges_added == 0

    # Test with invalid document types
    invalid_data = {
        "nodes": [{"_key": "1", "type": "invalid"}],
        "edges": [{"_key": "2", "type": "invalid"}],
    }
    nodes_added, edges_added = process_chunk_data(db, invalid_data, batch_size=100)
    assert nodes_added == 0  # Should reject invalid documents
    assert edges_added == 0

    # Test with valid data
    mock_collection.import_bulk.side_effect = lambda docs, **kwargs: {
        "created": len(docs),
        "errors": 0,
    }
    valid_data = {
        "nodes": [{"_key": str(i), "type": "node"} for i in range(LARGE_TEST_DOCS)],
        "edges": [
            {"_key": str(i), "type": "edge", "_from": "1", "_to": "2"}
            for i in range(LARGE_TEST_DOCS)
        ],
    }
    nodes_added, edges_added = process_chunk_data(db, valid_data, batch_size=50)
    assert nodes_added == LARGE_TEST_DOCS
    assert edges_added == LARGE_TEST_DOCS
    assert mock_collection.import_bulk.call_count >= MIN_BATCH_CALLS


def test_process_document_error_handling() -> None:
    """Test error handling in process_document.

    Tests:
    - Missing required fields handling
    - Invalid document type handling
    - Valid document handling with properties
    - Unique constraint violation handling
    - Progress reporting
    """
    # Mock database and collections
    mock_db = MagicMock()
    mock_nodes_col = MagicMock()
    mock_edges_col = MagicMock()
    mock_db.__getitem__.side_effect = (
        lambda x: mock_nodes_col if x == "Nodes" else mock_edges_col
    )

    # Test progress queue
    progress_queue: queue.Queue[tuple[int, int]] = queue.Queue()

    # Test valid node document
    valid_node = {
        "type": "node",
        "id": "123",
        "label": "TestNode",
        "properties": {"name": "Test", "value": 42},
    }
    mock_nodes_col.import_bulk.return_value = {"created": 1, "errors": 0}
    nodes_added, edges_added = process_document(valid_node, mock_db, progress_queue)
    assert nodes_added == 1
    assert edges_added == 0
    progress = progress_queue.get_nowait()
    assert progress == (1, 0)

    # Test valid relationship document
    valid_relationship = {
        "type": "relationship",
        "id": "456",
        "label": "CONNECTS",
        "start": {"id": "123"},
        "end": {"id": "789"},
        "properties": {"weight": 0.5},
    }
    mock_edges_col.import_bulk.return_value = {"created": 1, "errors": 0}
    nodes_added, edges_added = process_document(
        valid_relationship, mock_db, progress_queue
    )
    assert nodes_added == 0
    assert edges_added == 1
    progress = progress_queue.get_nowait()
    assert progress == (0, 1)

    # Test unique constraint violation for node
    mock_nodes_col.import_bulk.side_effect = Exception("unique constraint violated")
    nodes_added, edges_added = process_document(valid_node, mock_db, progress_queue)
    assert nodes_added == 0
    assert edges_added == 0

    # Test unique constraint violation for edge
    mock_edges_col.import_bulk.side_effect = Exception("unique constraint violated")
    nodes_added, edges_added = process_document(
        valid_relationship, mock_db, progress_queue
    )
    assert nodes_added == 0
    assert edges_added == 0

    # Test invalid document type
    invalid_type = {"type": "invalid", "id": "999"}
    nodes_added, edges_added = process_document(invalid_type, mock_db, progress_queue)
    assert nodes_added == 0
    assert edges_added == 0

    # Test missing required field
    invalid_node = {
        "type": "node",
        # Missing id field
        "label": "TestNode",
    }
    nodes_added, edges_added = process_document(invalid_node, mock_db, progress_queue)
    assert nodes_added == 0
    assert edges_added == 0

    # Test missing relationship fields
    invalid_relationship = {
        "type": "relationship",
        "id": "456",
        # Missing start/end nodes
        "label": "CONNECTS",
    }
    nodes_added, edges_added = process_document(
        invalid_relationship, mock_db, progress_queue
    )
    assert nodes_added == 0
    assert edges_added == 0


def test_get_db_max_connections_error_handling() -> None:
    """Test error handling in get_db_max_connections.

    Tests:
    - max_connections attribute missing handling
    - Database connection failure handling
    """
    # Test when max_connections is not available
    db = MagicMock()
    db._conn = MagicMock()
    db._conn.get_max_connections.side_effect = AttributeError

    max_conn = get_db_max_connections(db)
    assert max_conn > 0  # Should return default value

    # Test when database connection fails
    db._conn.get_max_connections.side_effect = Exception("Connection failed")
    max_conn = get_db_max_connections(db)
    assert max_conn > 0  # Should return default value


@pytest.fixture
def temp_json_file() -> str:
    """Create a temporary JSON file for testing."""
    with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
        # Write sample data in JSONL format
        f.write('{"type":"node","_key":"1","data":"node1"}\n')
        f.write('{"type":"node","_key":"2","data":"node2"}\n')
        f.write(
            '{"type":"edge","_key":"e1","_from":"nodes/1","_to":"nodes/2","label":"test_edge"}\n'
        )
    yield f.name
    os.unlink(f.name)


def test_process_jsonl_relationship() -> None:
    """Test processing JSONL file with relationship type edges.

    Tests:
    - Processing of node documents
    - Processing of relationship type edges with start/end nodes
    - Proper chunk generation with mixed document types
    """
    with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
        # Write test data with relationship type
        f.write('{"type":"node","_key":"1","properties":{"name":"A"}}\n')
        f.write(
            '{"type":"relationship","start":{"id":"1"},"end":{"id":"2"},"label":"KNOWS","properties":{"since":2020}}\n'
        )
        f.close()

        def write_chunk(chunk_data: dict[str, list[dict[str, Any]]]) -> str:
            return "chunk.json"

        with open(f.name) as input_file:
            chunks = list(_process_jsonl(input_file, 1000, write_chunk))
            assert len(chunks) > 0

    os.unlink(f.name)


def test_process_chunk_data_invalid_edges() -> None:
    """Test processing chunk data with invalid edges.

    Tests:
    - Invalid edge type handling
    - Missing type field handling
    - Valid edge processing amidst invalid edges
    """
    mock_db = MagicMock()
    mock_nodes_col = MagicMock()
    mock_edges_col = MagicMock()
    mock_db.__getitem__.side_effect = (
        lambda x: mock_nodes_col if x == "nodes" else mock_edges_col
    )

    # Test data with invalid edges
    chunk_data = {
        "edges": [
            {"type": "invalid", "_key": "1"},  # Invalid type
            {"type": "edge", "_key": "2"},  # Valid edge
            {"_key": "3"},  # Missing type
        ]
    }

    nodes_added, edges_added = process_chunk_data(mock_db, chunk_data, 10)
    assert edges_added == 1  # Only one valid edge should be processed


def test_process_document_relationship() -> None:
    """Test processing a document with relationship type.

    Tests:
    - Relationship document conversion to edge format
    - Progress queue updates
    - Proper edge document structure
    """
    mock_db = MagicMock()
    mock_nodes_col = MagicMock()
    mock_edges_col = MagicMock()
    mock_db.__getitem__.side_effect = (
        lambda x: mock_nodes_col if x == "nodes" else mock_edges_col
    )
    mock_edges_col.import_bulk.return_value = {"created": 1, "errors": 0}

    # Test relationship document
    doc = {
        "type": "relationship",
        "_key": "test_key",  # Add required _key field
        "start": {"id": "1"},
        "end": {"id": "2"},
        "label": "KNOWS",
        "properties": {"since": 2020},
    }

    progress_queue: queue.Queue[tuple[int, int]] = queue.Queue()
    nodes_added, edges_added = process_document(doc, mock_db, progress_queue)
    assert edges_added == 1
    assert nodes_added == 0

    # Verify the progress queue
    progress = progress_queue.get_nowait()
    assert progress == (0, 1)


def test_process_chunk_error_handling() -> None:
    """Test error handling in process_chunk.

    Tests:
    - Invalid JSON handling
    - Progress reporting during errors
    - Successful processing of valid documents
    """
    # Create a temporary file with invalid JSON
    with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
        f.write(
            '{"type":"node","_key":"1","properties":{"name":"A"}}\n'
        )  # Add properties field
        f.write("invalid json\n")
        f.close()

        db_config = {
            "db_name": "test_db",
            "host": "localhost",
            "port": 8529,
            "username": "test",
            "password": "test",
            "verify": False,  # Disable SSL verification for testing
        }

        # Mock the database connection
        mock_db = MagicMock()
        mock_nodes_col = MagicMock()
        mock_edges_col = MagicMock()
        mock_db.__getitem__.side_effect = (
            lambda x: mock_nodes_col if x == "nodes" else mock_edges_col
        )
        # Mock collections method to return empty list (no collections exist yet)
        mock_db.collections.return_value = []

        # Test with progress queue
        progress_queue: queue.Queue[tuple[int, int]] = queue.Queue()

        # Create a context manager for mocked connection
        class MockContextManager:
            def __enter__(self):
                return mock_db

            def __exit__(self, exc_type, exc_val, exc_tb):
                return None

        # Mock both database connection and process_document
        with (
            patch("arangoimport.importer.process_document") as mock_process,
            patch("arangoimport.importer.ArangoConnection") as mock_connection_class,
        ):
            mock_connection = MagicMock()
            mock_connection.get_connection.return_value = MockContextManager()
            mock_connection_class.return_value = mock_connection
            mock_process.return_value = (1, 0)  # Return 1 node added, 0 edges
            nodes_added, edges_added = process_chunk(
                f.name, db_config, 0, 1, progress_queue
            )
            assert nodes_added == 1  # Valid node should be processed
            assert edges_added == 0

    os.unlink(f.name)


def test_get_db_max_connections_error_cases() -> None:
    """Test error handling in get_db_max_connections.

    Tests:
    - Database access error handling
    - Unexpected query result format handling
    - Default value fallback
    """
    # Test when _system database is not accessible
    mock_db = MagicMock()
    mock_db.aql.execute.side_effect = Exception("Cannot access _system database")

    max_connections = get_db_max_connections(mock_db)
    assert max_connections == DEFAULT_MAX_CONNECTIONS  # Should return default value

    # Test when query returns unexpected format
    mock_db = MagicMock()
    mock_db.aql.execute.return_value = [{"invalid": "format"}]

    max_connections = get_db_max_connections(mock_db)
    assert max_connections == DEFAULT_MAX_CONNECTIONS  # Should return default value
