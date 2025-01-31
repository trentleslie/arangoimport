"""Pytest configuration and fixtures."""

import os
from pathlib import Path

import pytest
from arango import ArangoClient
from dotenv import load_dotenv

from arangoimport.connection import ArangoConnection

# Load environment variables from .env file
env_path = Path(__file__).parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

# Get ArangoDB configuration from environment
ARANGO_HOST = os.getenv("ARANGO_HOST", "localhost")
ARANGO_PORT = int(os.getenv("ARANGO_PORT", "8529"))
ARANGO_USER = os.getenv("ARANGO_USER", "root")
ARANGO_PASS = os.getenv("ARANGO_PASS", "")


@pytest.fixture
def arango_client():
    """Create a test ArangoDB client."""
    client = ArangoClient(hosts=f"http://{ARANGO_HOST}:{ARANGO_PORT}")
    return client


@pytest.fixture
def arango_connection():
    """Create a test ArangoDB connection."""
    connection = ArangoConnection(
        host=ARANGO_HOST,
        port=ARANGO_PORT,
        username=ARANGO_USER,
        password=ARANGO_PASS,
    )
    return connection


@pytest.fixture
def test_db(arango_client):
    """Create and return a test database."""
    db_name = "test_arangoimport"
    sys_db = arango_client.db("_system", username=ARANGO_USER, password=ARANGO_PASS)

    # Clean up any existing test database
    if sys_db.has_database(db_name):
        sys_db.delete_database(db_name)

    # Create fresh test database
    sys_db.create_database(db_name)
    db = arango_client.db(db_name, username=ARANGO_USER, password=ARANGO_PASS)

    yield db

    # Cleanup
    sys_db.delete_database(db_name)


@pytest.fixture
def test_collection(test_db):
    """Create and return a test collection."""
    coll_name = "test_collection"

    if test_db.has_collection(coll_name):
        test_db.delete_collection(coll_name)

    collection = test_db.create_collection(coll_name)
    return collection
