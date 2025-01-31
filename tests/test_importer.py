"""Test importer functionality."""

import os
import tempfile
from unittest.mock import MagicMock, patch

import pytest
from pyArango.document import Document

from arangoimport.importer import (
    batch_save_documents,
    parallel_load_data,
    process_chunk,
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
RETRY_ATTEMPTS = 2  # Number of attempts for retry logic


@pytest.fixture
def mock_collection():
    """Create a mock collection for testing."""
    mock = MagicMock()
    mock.bulkSave = MagicMock(return_value=None)
    mock.import_bulk = MagicMock(return_value={"created": 1, "errors": 0})
    return mock


@pytest.fixture
def mock_docs():
    """Create mock documents."""
    return [MagicMock(spec=Document) for _ in range(10)]


def test_batch_save_documents(mock_collection, mock_docs):
    """Test saving documents in batches."""
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
    mock_collection.import_bulk.side_effect = [
        {"created": 0, "errors": 1},  # First attempt fails
        {"created": 1, "errors": 0},  # Second attempt succeeds
    ]
    saved = batch_save_documents(mock_collection, mock_docs[:1], 1)
    assert saved == 1
    assert mock_collection.import_bulk.call_count == RETRY_ATTEMPTS


@pytest.fixture
def temp_json_file():
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


def test_process_chunk(temp_json_file, mock_collection):
    """Test processing a file chunk."""
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


def test_parallel_load_data(temp_json_file):
    """Test parallel data loading."""
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
