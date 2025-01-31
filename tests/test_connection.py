"""Tests for ArangoDB connection management."""

import logging
import time

import pytest
import urllib3
from arango.exceptions import IndexDeleteError

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
    # Create collections first
    ensure_collections(test_db)
    time.sleep(1)  # Allow time for collection creation to complete

    # Verify Nodes collection exists
    assert "Nodes" in [c["name"] for c in test_db.collections()], (
        "Nodes collection missing"
    )

    nodes_col = test_db.collection("Nodes")

    # Create a hash index programmatically
    index = nodes_col.add_hash_index(
        fields=["test_field"], sparse=False, unique=False, name="test_index"
    )
    assert index is not None, "Failed to create test index"

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
