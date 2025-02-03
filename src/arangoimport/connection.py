"""ArangoDB connection management."""

import fcntl
import threading
import time
from collections.abc import Generator
from contextlib import contextmanager
from queue import Empty, Queue
from typing import Any, TypedDict

from arango.client import ArangoClient
from arango.collection import StandardCollection
from arango.database import Database
from arango.exceptions import (
    ArangoClientError,
    ArangoServerError,
    CollectionCreateError,
)
from arango.job import AsyncJob, BatchJob

from arangoimport.logging import get_logger

logger = get_logger(__name__)

# Collection types
EDGE_COLLECTION_TYPE = "edge"
EDGE_COLLECTION_TYPE_ID = 3  # ArangoDB internal type ID for edge collections


def ensure_collections(db: Database) -> None:
    """Ensure required collections exist and are of correct type.

    Args:
        db: Database object
    """
    try:
        # Create Nodes collection if it doesn't exist
        if not db.has_collection("Nodes"):
            db.create_collection("Nodes")
            logger.info("Created Nodes collection")

        # Create Edges collection if it doesn't exist
        if not db.has_collection("Edges"):
            db.create_collection("Edges", edge=(EDGE_COLLECTION_TYPE == "edge"))
            logger.info("Created Edges collection")
    except CollectionCreateError as e:
        logger.error(f"Error creating collections: {e}")
        raise


class ArangoConfig(TypedDict, total=False):
    """ArangoDB connection configuration."""

    host: str
    port: int
    username: str
    password: str
    db_name: str
    pool_size: int
    max_retries: int
    retry_delay: float


class ArangoConnection:
    """ArangoDB connection manager."""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 8529,
        username: str = "root",
        password: str = "",
        **kwargs: Any,
    ) -> None:
        """Initialize connection manager.

        Args:
            host: ArangoDB host
            port: ArangoDB port
            username: ArangoDB username
            password: ArangoDB password
            **kwargs: Additional configuration options
        """
        try:
            config: ArangoConfig = {
                "host": host,
                "port": port,
                "username": username,
                "password": password,
                "db_name": kwargs.get("db_name", "spoke_human"),
                "pool_size": kwargs.get("pool_size", 10),
                "max_retries": kwargs.get("max_retries", 3),
                "retry_delay": kwargs.get("retry_delay", 1.0),
            }

            self.host = config["host"]
            self.port = config["port"]
            self.username = config["username"]
            self.password = config["password"]
            self.db_name = config["db_name"]
            self.pool_size = config["pool_size"]
            self.max_retries = config["max_retries"]
            self.retry_delay = config["retry_delay"]

            self.client = ArangoClient(hosts=f"http://{self.host}:{self.port}")
            self.pool: Queue[Database] = Queue(maxsize=self.pool_size)
            self.lock = threading.Lock()
            self.disabled_indexes: dict[str, list[dict[str, Any]]] = {}
            self.connections_created = 0
            self._init_pool()
        except (ArangoClientError, OSError, ConnectionAbortedError) as e:
            # Let the original error propagate through
            raise ArangoError(str(e)) from e

    def create_database(self, db_name: str) -> None:
        """Create a database if it does not exist.

        This method connects to the _system database to create a new database if
        needed. Handles connection, client, and network-related errors during the
        process. In parallel processing scenarios, multiple processes may attempt
        to create the database at once. If the database exists when we try to
        create it, we'll log this at debug level and continue.

        Args:
            db_name: Database name to create

        Raises:
            ArangoError: If database creation fails due to connection,
                authentication, or other client errors
        """
        # Use file lock to synchronize database creation across processes
        lock_file = f"/tmp/arango_db_{db_name}.lock"
        with open(lock_file, "w") as f:
            try:
                # Get exclusive lock
                fcntl.flock(f, fcntl.LOCK_EX)

                sys_db = self.client.db(
                    "_system", username=self.username, password=self.password
                )
                databases = sys_db.databases()

                # Handle async/batch jobs
                if isinstance(databases, AsyncJob | BatchJob):
                    databases = databases.result()

                # Check if database exists
                if isinstance(databases, list) and db_name not in databases:
                    try:
                        sys_db.create_database(db_name)
                        logger.info(f"Created database: {db_name}")
                        # Sleep briefly to allow database creation to complete
                        time.sleep(0.5)
                    except ArangoClientError as e:
                        # If error is duplicate database,
                        # another process created it first
                        if "duplicate" in str(e).lower():
                            msg = "Database {} was already created by another process"
                            logger.debug(msg.format(db_name))
                        else:
                            raise
            except (OSError, ConnectionAbortedError) as e:
                # Let the original error propagate through
                logger.error(f"Error creating database {db_name}: {e}")
                raise ArangoError(str(e)) from e
            finally:
                # Release lock
                fcntl.flock(f, fcntl.LOCK_UN)

    def _init_pool(self) -> None:
        """Initialize connection pool."""
        try:
            # Create database if it doesn't exist
            self.create_database(self.db_name)

            # Initialize base connection
            self._base_connection = self.client.db(
                self.db_name,
                username=self.username,
                password=self.password,
            )

            # Verify connection works
            try:
                self._base_connection.version()
            except (ArangoClientError, OSError) as e:
                # Let the original error propagate through
                logger.error(f"Error verifying connection: {e}")
                raise ArangoError(str(e)) from e

            # Add to pool
            self.pool.put(self._base_connection)
            self.connections_created = 1

            logger.info(
                f"Successfully initialized connection pool for database {self.db_name}"
            )
        except (ArangoClientError, OSError) as e:
            # Let the original error propagate through
            logger.error(f"Error initializing connection pool: {e}")
            raise ArangoError(str(e)) from e

    def verify_connection(self) -> bool:
        """Verify connection to ArangoDB.

        Returns:
            bool: True if connection is successful
        """
        try:
            self._base_connection.version()
            return True
        except (ArangoClientError, ArangoServerError, ConnectionError) as e:
            logger.error(f"Error verifying connection: {e}")
            raise ArangoError(str(e)) from e

    def has_database(self, db_name: str) -> bool:
        """Check if database exists.

        Args:
            db_name: Database name

        Returns:
            bool: True if database exists

        Raises:
            ArangoError: If checking database existence fails
        """
        try:
            sys_db = self.client.db(
                "_system", username=self.username, password=self.password
            )
            databases = sys_db.databases()

            # Handle async/batch jobs
            if isinstance(databases, AsyncJob | BatchJob):
                databases = databases.result()

            # Check if database exists in the list
            return isinstance(databases, list) and db_name in databases
        except (ArangoClientError, ConnectionError) as e:
            logger.error(f"Error checking database existence: {e}")
            raise ArangoError(str(e)) from e

    def _get_connection(self) -> Database:
        """Get a connection from the pool.

        Returns:
            Database: ArangoDB connection
        """
        try:
            return self.pool.get_nowait()
        except Empty:
            with self.lock:
                if self.connections_created < self.pool_size:
                    self.connections_created += 1
                    return self._create_connection()
                else:
                    return self.pool.get()

    def _create_connection(self) -> Database:
        """Create a new connection.

        Returns:
            Database: ArangoDB connection
        """
        try:
            return self.client.db(
                self.db_name,
                username=self.username,
                password=self.password,
            )
        except ArangoClientError as e:
            logger.error(f"Error creating connection: {e}")
            raise ArangoError(str(e)) from e

    @contextmanager
    def get_connection(self) -> Generator[Database, None, None]:
        """Get a connection from the pool.

        Yields:
            Database: ArangoDB connection
        """
        connection = self._get_connection()
        try:
            yield connection
        finally:
            self.pool.put(connection)

    def _return_connection(self, conn: Database) -> None:
        """Return a connection to the pool."""
        try:
            self.pool.put(conn)
        except Exception as e:
            logger.error(f"Error returning connection to pool: {e}")
            raise

    def _get_collection_indexes(
        self, collection: StandardCollection
    ) -> list[dict[str, Any]]:
        """Get collection indexes.

        Args:
            collection: Collection object

        Returns:
            list[dict[str, Any]]: List of index definitions
        """
        try:
            result = collection.indexes()
            if isinstance(result, AsyncJob | BatchJob):
                result = result.result()
            return (
                [
                    idx
                    for idx in result
                    if idx["type"] != "primary"  # Skip primary index
                ]
                if result
                else []
            )
        except ArangoClientError as e:
            logger.error(f"Error getting collection indexes: {e}")
            raise ArangoError(str(e)) from e

    def _disable_collection_indexes(
        self, collection: StandardCollection, collection_name: str
    ) -> None:
        """Disable collection indexes.

        Args:
            collection: Collection object
            collection_name: Collection name
        """
        try:
            # Get and store indexes
            indexes = self._get_collection_indexes(collection)
            if indexes:
                self.disabled_indexes[collection_name] = indexes
                # Delete indexes
                for idx in indexes:
                    collection.delete_index(idx["id"])
                    logger.info(f"Disabled index {idx['id']} in {collection_name}")
        except ArangoClientError as e:
            logger.error(f"Error disabling collection indexes: {e}")
            raise ArangoError(str(e)) from e

    def _rebuild_collection_indexes(
        self, collection: StandardCollection, collection_name: str
    ) -> None:
        """Rebuild collection indexes.

        Args:
            collection: Collection object
            collection_name: Collection name
        """
        try:
            # Rebuild indexes if they were disabled
            if collection_name in self.disabled_indexes:
                for idx in self.disabled_indexes[collection_name]:
                    idx_type = idx["type"]
                    idx_fields = idx["fields"]
                    if idx_type == "hash":
                        collection.add_hash_index(
                            fields=idx_fields,
                            unique=idx.get("unique", False),
                            sparse=idx.get("sparse", False),
                        )
                    elif idx_type == "skiplist":
                        collection.add_skiplist_index(
                            fields=idx_fields,
                            unique=idx.get("unique", False),
                            sparse=idx.get("sparse", False),
                        )
                    elif idx_type == "persistent":
                        collection.add_persistent_index(
                            fields=idx_fields,
                            unique=idx.get("unique", False),
                            sparse=idx.get("sparse", False),
                        )
                    elif idx_type == "ttl":
                        collection.add_ttl_index(
                            fields=idx_fields,
                            expiry_time=idx.get("expiry_time", 0),
                        )
                    elif idx_type == "geo":
                        collection.add_geo_index(
                            fields=idx_fields,
                            geo_json=idx.get("geo_json", False),
                        )
                    elif idx_type == "fulltext":
                        collection.add_fulltext_index(
                            fields=idx_fields,
                            min_length=idx.get("min_length", None),
                        )
                    logger.info(f"Rebuilt index {idx['id']} in {collection_name}")
                # Clear disabled indexes
                del self.disabled_indexes[collection_name]
        except ArangoClientError as e:
            logger.error(f"Error rebuilding collection indexes: {e}")
            raise ArangoError(str(e)) from e

    def manage_indexes(self, db: Database, disable: bool = False) -> None:
        """Manage database indexes.

        Args:
            db: Database object
            disable: If True, disable all non-primary indexes.
                    If False, rebuild indexes.

        Raises:
            ArangoError: If an error occurs while managing indexes
        """
        try:
            # Get collections
            collections_result = db.collections()
            if isinstance(collections_result, AsyncJob | BatchJob):
                collections_result = collections_result.result()
            if not collections_result:
                logger.info("No collections found")
                return

            # Process each collection
            for col_info in collections_result:
                col_name = col_info["name"]
                if col_name.startswith("_"):  # Skip system collections
                    continue

                collection = db[col_name]
                if not isinstance(collection, StandardCollection):
                    continue  # type: ignore[unreachable]

                try:
                    if disable:
                        self._disable_collection_indexes(collection, col_name)
                    else:
                        self._rebuild_collection_indexes(collection, col_name)
                except ArangoClientError as e:
                    logger.error(
                        f"Error managing indexes for collection {col_name}: {e}"
                    )
                    raise ArangoError(str(e)) from e

            logger.info("Indexes %s", "disabled" if disable else "rebuilt")

        except ArangoClientError as e:
            logger.error(f"Error managing indexes: {e}")
            raise ArangoError(str(e)) from e

    def ensure_collections(self, db: Database) -> None:
        """Ensure required collections exist and are of correct type.

        Args:
            db: Database object
        """
        # Use file lock to synchronize collection creation across processes
        lock_file = f"/tmp/arango_collections_{self.db_name}.lock"
        with open(lock_file, "w") as f:
            try:
                # Get exclusive lock
                fcntl.flock(f, fcntl.LOCK_EX)

                try:
                    # Create Nodes collection if it doesn't exist
                    if not db.has_collection("Nodes"):
                        db.create_collection("Nodes")
                        logger.info("Created Nodes collection")
                        # Sleep briefly to allow collection creation to complete
                        time.sleep(0.5)

                    # Create Edges collection if it doesn't exist
                    if not db.has_collection("Edges"):
                        db.create_collection(
                            "Edges", edge=(EDGE_COLLECTION_TYPE == "edge")
                        )
                        logger.info("Created Edges collection")
                        # Sleep briefly to allow collection creation to complete
                        time.sleep(0.5)
                except CollectionCreateError as e:
                    if "duplicate" not in str(e).lower():
                        logger.error(f"Error creating collections: {e}")
                        raise
                    logger.debug("Collections already exist")
            finally:
                # Release lock
                fcntl.flock(f, fcntl.LOCK_UN)


class ArangoError(Exception):
    """Custom wrapper exception for ArangoDB-related errors.

    This exception class serves as a high-level wrapper for various errors that can
    occur when interacting with ArangoDB, including:
    - Network connectivity issues (e.g., DNS resolution, connection timeouts)
    - Authentication failures
    - Database and collection operations
    - Client errors from python-arango

    By wrapping these errors in our custom exception, we provide:
    1. Consistent error handling across the application
    2. Preservation of the original error chain through exception chaining
    3. Unified logging and error reporting
    """

    pass
