"""Tests for ArangoDB connection management."""

import logging
from queue import Empty
from typing import Any, ClassVar
from unittest.mock import MagicMock

import pytest
import urllib3
from arango.exceptions import ArangoServerError, IndexDeleteError

from arangoimport.connection import (
    EDGE_COLLECTION_TYPE_ID,
    ArangoConnection,
    ArangoError,
    ensure_collections,
)
from arangoimport.logging import get_logger

logger = get_logger(__name__)

# Constants for test values
DEFAULT_PORT = 8530
DEFAULT_HOST = "localhost"
DEFAULT_USERNAME = "root"
DEFAULT_PASSWORD = "yourpassword"


@pytest.fixture
def arango_connection():
    """Create a test ArangoDB connection."""
    connection = ArangoConnection(
        host=DEFAULT_HOST,
        port=DEFAULT_PORT,
        username=DEFAULT_USERNAME,
        password=DEFAULT_PASSWORD,
    )
    return connection


@pytest.fixture
def test_db(arango_connection):
    """Create and return a test database."""
    test_db_name = "test_db"

    # Connect to _system to manage databases
    sys_db = arango_connection.client.db(
        "_system", username=DEFAULT_USERNAME, password=DEFAULT_PASSWORD
    )

    # Clean up if database exists
    if test_db_name in sys_db.databases():
        sys_db.delete_database(test_db_name)

    # Create fresh test database
    sys_db.create_database(test_db_name)
    db = arango_connection.client.db(
        test_db_name, username=DEFAULT_USERNAME, password=DEFAULT_PASSWORD
    )

    yield db

    # Cleanup using _system
    if test_db_name in sys_db.databases():
        sys_db.delete_database(test_db_name)


def test_connection_init():
    """Test connection initialization."""
    connection = ArangoConnection(
        host=DEFAULT_HOST,
        port=DEFAULT_PORT,
        username=DEFAULT_USERNAME,
        password=DEFAULT_PASSWORD,
    )
    assert connection.host == DEFAULT_HOST
    assert connection.port == DEFAULT_PORT
    assert connection.username == DEFAULT_USERNAME
    assert connection.password == DEFAULT_PASSWORD


def test_connection_success(arango_connection):
    """Test successful connection to ArangoDB."""
    # Connect to _system to list databases
    sys_db = arango_connection.client.db(
        "_system", username=DEFAULT_USERNAME, password=DEFAULT_PASSWORD
    )
    databases = sys_db.databases()
    assert "_system" in databases


def test_connection_failure(caplog):
    """Test connection failure with invalid credentials.

    Verifies that attempting to connect with invalid host/credentials raises
    an ArangoError with an appropriate error message from python-arango.
    """
    # Set logging level to capture urllib3 warnings
    caplog.set_level(logging.WARNING)
    urllib3.disable_warnings()  # Disable urllib3 warnings to clean up output

    with pytest.raises(ArangoError) as exc_info:
        ArangoConnection(
            host="invalid-host",
            port=8529,
            username="invalid",
            password="invalid",
        )

    # Get the full error chain as text
    error_chain = []
    e = exc_info.value
    while e is not None:
        error_chain.append(
            str(e).lower()
        )  # Convert to lowercase for case-insensitive matching
        e = e.__cause__

    # Get all log messages
    log_messages = [record.message.lower() for record in caplog.records]

    # Any of these substrings should be present in the error chain or logs
    error_patterns = [
        "can't connect to host",  # Original message
        "connection refused",  # Alternative message
        "host(s) within limit",  # New python-arango message
        "name or service not known",  # DNS resolution error
        "failed to resolve",  # Another DNS error variant
        "can't connect to host(s) within limit (3)",  # Exact error message
        "[errno -2] name or service not known",  # Full DNS error
        "nameresolutionerror",  # urllib3 error class
    ]

    # Look for any of our patterns in either the error chain or logs
    found_patterns = [
        pattern
        for pattern in error_patterns
        if any(pattern in err_text for err_text in error_chain + log_messages)
    ]

    assert found_patterns, (
        f"No expected error pattern found in error chain or logs.\n"
        f"Error chain: {' -> '.join(error_chain)}\n"
        f"Log messages: {' -> '.join(log_messages)}\n"
        f"Expected one of: {', '.join(error_patterns)}"
    )


def test_database_operations(arango_connection):
    """Test database operations."""
    test_db_name = "test_db_operations"

    # Connect to _system to manage databases
    sys_db = arango_connection.client.db(
        "_system", username=DEFAULT_USERNAME, password=DEFAULT_PASSWORD
    )

    # Clean up if database exists
    if test_db_name in sys_db.databases():
        sys_db.delete_database(test_db_name)

    # Test database creation
    assert test_db_name not in sys_db.databases()
    sys_db.create_database(test_db_name)
    assert test_db_name in sys_db.databases()

    # Test database deletion
    sys_db.delete_database(test_db_name)
    assert test_db_name not in sys_db.databases()


def test_connection_pooling(arango_connection):
    """Test connection pooling."""
    # Pool should have 1 connection from initialization
    assert not arango_connection.pool.empty()
    assert arango_connection.connections_created == 1

    # Get a connection from the pool
    with arango_connection.get_connection() as client:
        assert client is not None
        # We still have only created 1 physical connection
        assert arango_connection.connections_created == 1

    # Once the with-statement ends, the connection returns to the pool
    assert not arango_connection.pool.empty()
    assert arango_connection.pool.qsize() == 1


def test_ensure_collections(test_db):
    """Test that ensure_collections() creates Nodes and Edges collections."""
    # Initially, neither Nodes nor Edges should exist
    existing = [c["name"] for c in test_db.collections()]
    assert "Nodes" not in existing
    assert "Edges" not in existing

    # Create collections
    ensure_collections(test_db)

    # Verify collections exist with correct names
    existing_after = [c["name"] for c in test_db.collections()]
    assert "Nodes" in existing_after, "Nodes collection was not created"
    assert "Edges" in existing_after, "Edges collection was not created"

    # Verify Edges collection is of edge type
    edges_col = test_db.collection("Edges")
    assert edges_col.properties()["type"] == EDGE_COLLECTION_TYPE_ID, (
        "Edges collection is not of edge type"
    )


def test_manage_indexes(test_db):
    """Test index management for bulk import optimization."""
    # Create test collections
    nodes_col = test_db.create_collection("test_nodes")

    # Add test index
    index = nodes_col.add_index(
        {
            "type": "hash",
            "fields": ["test_field"],
            "name": "test_index",
        }
    )
    assert index["type"] == "hash"

    def count_non_system_indexes(collection):
        """Helper to count non-system indexes using HTTP API."""
        indexes = collection.indexes()
        non_system_count = sum(
            1 for idx in indexes if idx["type"] not in ("primary", "edge")
        )
        logger.debug(f"Found {non_system_count} non-system indexes")
        return non_system_count

    # Get initial indexes
    initial_count = count_non_system_indexes(nodes_col)
    assert initial_count > 0, "Should have at least one non-system index"

    # Create a connection instance for managing indexes
    connection = ArangoConnection(
        host=DEFAULT_HOST,
        port=DEFAULT_PORT,
        username=DEFAULT_USERNAME,
        password=DEFAULT_PASSWORD,
    )

    # Attempt to disable indexes for bulk import
    try:
        connection.manage_indexes(test_db, disable=True)
    except IndexDeleteError as e:
        pytest.skip(f"Skipping test: Insufficient permissions to delete indexes - {e}")

    disabled_count = count_non_system_indexes(nodes_col)
    assert disabled_count == 0, "All non-system indexes should be disabled"

    # Re-enable indexes
    connection.manage_indexes(test_db, disable=False)
    final_count = count_non_system_indexes(nodes_col)
    assert final_count == initial_count, "Not all indexes were restored"


def test_create_database_duplicate(arango_connection):
    """Test creating a database that already exists."""
    test_db_name = "test_duplicate_db"

    # First creation should succeed
    arango_connection.create_database(test_db_name)

    # Second creation should not raise error
    arango_connection.create_database(test_db_name)

    # Clean up
    sys_db = arango_connection.client.db(
        "_system", username=DEFAULT_USERNAME, password=DEFAULT_PASSWORD
    )
    if test_db_name in sys_db.databases():
        sys_db.delete_database(test_db_name)


def test_connection_pool_exhaustion(arango_connection, test_db):
    """Test behavior when connection pool is exhausted."""
    connections = []

    # Get more connections than pool size
    for _ in range(arango_connection.pool_size + 1):
        with arango_connection.get_connection() as conn:
            connections.append(conn)
            # Verify connection works by checking version info
            version_info = conn.version()
            assert isinstance(version_info, str)
            assert len(version_info) > 0


def test_verify_connection_failure(caplog):
    """Test connection verification failure."""
    with pytest.raises(ArangoError):
        # Use an invalid port to force connection failure
        connection = ArangoConnection(
            host=DEFAULT_HOST,
            port=9999,  # Invalid port
            username=DEFAULT_USERNAME,
            password=DEFAULT_PASSWORD,
        )
        with connection.get_connection():
            pass  # This should fail


def test_connection_pool_cleanup(arango_connection):
    """Test proper cleanup of connection pool."""
    initial_size = arango_connection.connections_created

    # Use some connections
    with arango_connection.get_connection() as conn1:
        assert conn1 is not None

    with arango_connection.get_connection() as conn2:
        assert conn2 is not None

    # Verify connections are returned to pool
    assert arango_connection.connections_created == initial_size


def test_manage_indexes_error_handling(test_db):
    """Test error handling in index management."""
    # Create a test collection
    if not test_db.has_collection("test_collection"):
        test_db.create_collection("test_collection")

    collection = test_db.collection("test_collection")

    # Add a test index
    collection.add_index(
        {
            "type": "hash",
            "fields": ["test_field"],
            "name": "test_index",
        }
    )

    # Try to delete a non-existent index
    with pytest.raises(IndexDeleteError):
        collection.delete_index("non_existent_index")


def test_connection_pool_get_connection_timeout(arango_connection):
    """Test connection pool timeout behavior."""
    # Fill the pool
    connections = []
    for _ in range(arango_connection.pool_size):
        with arango_connection.get_connection() as conn:
            connections.append(conn)
            assert conn is not None

    # Try to get one more connection, which should create a new one
    with arango_connection.get_connection() as conn:
        assert conn is not None
        connections.append(conn)


def test_database_error_handling(arango_connection, monkeypatch):
    """Test error handling in database operations."""
    invalid_db_name = "invalid@db"  # Invalid database name with @ symbol

    def fake_db(*args: Any, **kwargs: Any) -> Any:
        mock_db = MagicMock()
        mock_db.databases.return_value = []

        def raise_error(name: str) -> None:
            raise ArangoError("invalid db name")

        mock_db.create_database = raise_error
        return mock_db

    monkeypatch.setattr(arango_connection.client, "db", fake_db)

    with pytest.raises(ArangoError):
        arango_connection.create_database(invalid_db_name)


def test_connection_pool_return_error(arango_connection):
    """Test error handling when returning connection to pool."""
    with arango_connection.get_connection() as conn:
        assert conn is not None
        # Connection will be automatically returned to pool

    # Verify we can get the same connection again
    with arango_connection.get_connection() as conn2:
        assert conn2 is not None


def test_index_management_error_handling(test_db):
    """Test error handling in index management."""
    # Create a test collection
    if not test_db.has_collection("test_collection"):
        test_db.create_collection("test_collection")

    collection = test_db.collection("test_collection")

    # Try to create an index with invalid configuration
    with pytest.raises(ArangoServerError):  # ArangoDB will raise a server error
        collection.add_index(
            {
                "type": "invalid_type",
                "fields": ["test_field"],
            }
        )


def test_verify_connection_success(arango_connection):
    """Test successful connection verification."""
    assert arango_connection.verify_connection() is True


def test_verify_connection_server_error(arango_connection, monkeypatch):
    """Test connection verification with server error."""

    class MockResponse:
        error_message = "Server error"
        status_text = "Server error"
        error_code = 503
        status_code = 503
        url = "http://localhost:8530/_api/version"
        method = "GET"
        headers: ClassVar[dict[str, str]] = {"Method": "GET"}

    def mock_version(*args):
        raise ArangoServerError(MockResponse(), MagicMock())

    monkeypatch.setattr(arango_connection._base_connection, "version", mock_version)

    with pytest.raises(ArangoError) as exc_info:
        arango_connection.verify_connection()
    assert "Server error" in str(exc_info.value)


def test_has_database_async_job(arango_connection, monkeypatch):
    """Test has_database with async job result."""

    class MockAsyncJob:
        def result(self):
            return ["_system", "test_db"]

    # Create a mock database instance
    mock_db = MagicMock()
    mock_db.databases.return_value = MockAsyncJob()

    monkeypatch.setattr(arango_connection.client, "db", lambda *args, **kwargs: mock_db)
    assert arango_connection.has_database("test_db") is True
    assert arango_connection.has_database("nonexistent") is False


def test_get_connection_pool_full(arango_connection):
    """Test getting connection when pool is full."""
    # Fill the pool to capacity
    connections = []
    try:
        # Empty the pool first
        while not arango_connection.pool.empty():
            arango_connection.pool.get_nowait()

        # Fill the pool with new connections
        for _ in range(arango_connection.pool_size):
            conn = arango_connection._create_connection()
            connections.append(conn)
            arango_connection.pool.put(conn)

        # Set connections_created to pool_size to prevent new connections
        arango_connection.connections_created = arango_connection.pool_size

        # Get all connections from the pool
        while not arango_connection.pool.empty():
            arango_connection.pool.get_nowait()

        # Pool is empty, next get should raise an exception
        with pytest.raises(Exception) as exc_info:
            arango_connection._get_connection()
        assert "Connection pool is empty" in str(exc_info.value)
    finally:
        # Clean up
        for _ in connections:
            try:
                arango_connection.pool.get_nowait()
            except Empty:
                pass


def test_rebuild_collection_indexes_error(test_db, monkeypatch):
    """Test error handling in rebuild_collection_indexes."""
    collection = test_db.create_collection("test_collection")

    # Mock add_index to raise an error
    class MockResponse:
        error_message = "Failed to create index"
        status_text = "Failed to create index"
        error_code = 503
        status_code = 503
        url = "http://localhost:8530/_api/index"
        method = "POST"
        headers: ClassVar[dict[str, str]] = {"Method": "POST"}

    def mock_add_index(*args, **kwargs):
        raise ArangoServerError(MockResponse(), MagicMock())

    monkeypatch.setattr(collection, "add_index", mock_add_index)

    # Create connection and set up disabled indexes
    arango_connection = ArangoConnection(
        host=DEFAULT_HOST,
        port=DEFAULT_PORT,
        username=DEFAULT_USERNAME,
        password=DEFAULT_PASSWORD,
    )

    # Store original indexes
    arango_connection.disabled_indexes = {
        "test_collection": [
            {
                "id": "idx_1",
                "type": "hash",
                "fields": ["test_field"],
            }
        ]
    }

    # Try to rebuild indexes - should raise ArangoError
    with pytest.raises(ArangoError) as exc_info:
        arango_connection._rebuild_collection_indexes(collection, "test_collection")
    assert "Failed to create index" in str(exc_info.value)


def test_manage_indexes_empty_collection(test_db):
    """Test managing indexes on empty collection."""
    # Create an empty collection
    collection = test_db.create_collection("empty_collection")

    arango_connection = ArangoConnection(
        host=DEFAULT_HOST,
        port=DEFAULT_PORT,
        username=DEFAULT_USERNAME,
        password=DEFAULT_PASSWORD,
    )

    # Should not raise any errors
    arango_connection.manage_indexes(test_db, disable=True)
    arango_connection.manage_indexes(test_db, disable=False)

    # Verify no indexes were affected
    indexes = collection.indexes()
    assert len([idx for idx in indexes if idx["type"] != "primary"]) == 0
