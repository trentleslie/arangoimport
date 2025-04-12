"""Test import configuration and result handling."""

from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from arango.collection import Collection

from arangoimport.config import ImportConfig
from arangoimport.importer import ImportResult, batch_save_documents, _handle_import_bulk_result


@pytest.fixture
def mock_collection() -> MagicMock:
    """Create a mock collection for testing."""
    mock = MagicMock(spec=Collection)
    mock.import_bulk = MagicMock()
    return mock


def test_import_config_defaults() -> None:
    """Test ImportConfig default values."""
    config = ImportConfig()
    assert config.on_duplicate == "replace"
    assert config.log_level == "INFO"
    assert config.batch_size == 1000
    assert config.validate_nodes is True
    assert config.dedup_enabled is True


def test_import_config_custom_values() -> None:
    """Test ImportConfig with custom values."""
    config = ImportConfig(
        on_duplicate="update",
        log_level="DEBUG",
        batch_size=500,
        validate_nodes=False,
        dedup_enabled=False
    )
    assert config.on_duplicate == "update"
    assert config.log_level == "DEBUG"
    assert config.batch_size == 500
    assert config.validate_nodes is False
    assert config.dedup_enabled is False


def test_handle_import_bulk_result_success() -> None:
    """Test successful import result handling."""
    result = {
        "created": 5,
        "replaced": 2,
        "updated": 1,
        "imported": 0,
        "errors": 0,
        "details": []
    }
    
    import_result = _handle_import_bulk_result(result)
    assert import_result.total_saved == 8
    assert import_result.created == 5
    assert import_result.replaced == 2
    assert import_result.updated == 1
    assert import_result.errors == 0
    assert import_result.details is None


def test_handle_import_bulk_result_with_errors() -> None:
    """Test import result handling with non-fatal errors."""
    result = {
        "created": 3,
        "replaced": 1,
        "updated": 0,
        "imported": 0,
        "errors": 2,
        "details": ["Error 1", "Error 2"]
    }
    
    import_result = _handle_import_bulk_result(result)
    assert import_result.total_saved == 4
    assert import_result.errors == 2
    assert import_result.details == ["Error 1", "Error 2"]


def test_handle_import_bulk_result_complete_failure() -> None:
    """Test import result handling with complete failure."""
    result = {
        "created": 0,
        "replaced": 0,
        "updated": 0,
        "imported": 0,
        "errors": 3,
        "details": ["Fatal error"]
    }
    
    with pytest.raises(ValueError, match="Failed to import documents"):
        _handle_import_bulk_result(result)


def test_batch_save_documents_with_config(mock_collection: MagicMock) -> None:
    """Test batch_save_documents with different ImportConfig settings."""
    docs = [{"_key": str(i)} for i in range(5)]
    
    # Test with replace strategy
    config = ImportConfig(on_duplicate="replace")
    mock_collection.import_bulk.return_value = {"created": 5, "errors": 0}
    result = batch_save_documents(mock_collection, docs, 2, config)
    assert result.total_saved == 5
    mock_collection.import_bulk.assert_called_with(
        docs[:2],  # First batch
        on_duplicate="replace",
        complete=True,
        details=True
    )

    # Test with update strategy
    config = ImportConfig(on_duplicate="update")
    mock_collection.import_bulk.reset_mock()
    mock_collection.import_bulk.return_value = {"updated": 5, "errors": 0}
    result = batch_save_documents(mock_collection, docs, 2, config)
    assert result.total_saved == 5
    mock_collection.import_bulk.assert_called_with(
        docs[:2],  # First batch
        on_duplicate="update",
        complete=True,
        details=True
    )


def test_batch_save_documents_result_aggregation(mock_collection: MagicMock) -> None:
    """Test that batch_save_documents properly aggregates results."""
    docs = [{"_key": str(i)} for i in range(4)]
    config = ImportConfig(on_duplicate="replace")
    
    # Mock different results for each batch
    mock_collection.import_bulk.side_effect = [
        {"created": 2, "errors": 0},  # First batch
        {"replaced": 2, "errors": 1, "details": ["Error"]}  # Second batch
    ]
    
    result = batch_save_documents(mock_collection, docs, 2, config)
    assert result.total_saved == 4
    assert result.created == 2
    assert result.replaced == 2
    assert result.errors == 1
    assert result.details == ["Error"]


@pytest.mark.parametrize("on_duplicate", ["replace", "update", "ignore"])
def test_batch_save_documents_duplicate_strategies(
    mock_collection: MagicMock, on_duplicate: str
) -> None:
    """Test batch_save_documents with different duplicate handling strategies."""
    docs = [{"_key": "1"}]
    config = ImportConfig(on_duplicate=on_duplicate)
    
    mock_collection.import_bulk.return_value = {
        "created": 1 if on_duplicate == "replace" else 0,
        "updated": 1 if on_duplicate == "update" else 0,
        "errors": 0
    }
    
    result = batch_save_documents(mock_collection, docs, 1, config)
    assert result.total_saved == 1
    mock_collection.import_bulk.assert_called_with(
        docs,
        on_duplicate=on_duplicate,
        complete=True,
        details=True
    )
