"""Tests for ID mapping functionality."""
from typing import Any, Dict
import pytest
from unittest.mock import MagicMock, patch

from arangoimport.id_mapping import IDMapper
from arangoimport.config import ImportConfig
from arangoimport.importer import _process_relationship_document

@pytest.fixture
def id_mapper() -> IDMapper:
    """Create a test ID mapper."""
    return IDMapper()

@pytest.fixture
def mock_edges_col() -> MagicMock:
    """Create a mock edges collection."""
    col = MagicMock()
    col.import_bulk.return_value = {"created": 1, "errors": 0}
    return col

@pytest.fixture
def edge_doc() -> Dict[str, Any]:
    """Create a test edge document."""
    return {
        "id": "test_edge",
        "type": "relationship",
        "start": {"id": "123"},
        "end": {"id": "456"},
        "properties": {
            "type": "TEST_EDGE",
            "weight": 1.0
        }
    }

def test_id_mapper_basic():
    """Test basic ID mapper functionality."""
    mapper = IDMapper()
    
    # Test adding and retrieving mappings
    mapper.add_mapping("123", "node1")
    mapper.add_mapping("456", "node2")
    
    assert mapper.get_arango_key("123") == "node1"
    assert mapper.get_arango_key("456") == "node2"
    assert mapper.get_arango_key("789") is None
    
    # Test length
    assert len(mapper) == 2

def test_id_mapper_thread_safety():
    """Test ID mapper thread safety."""
    mapper = IDMapper()
    
    # Add mappings from multiple threads
    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = []
        for i in range(100):
            futures.append(
                executor.submit(mapper.add_mapping, f"id{i}", f"key{i}")
            )
        for f in futures:
            f.result()
    
    # Verify all mappings were added
    assert len(mapper) == 100
    for i in range(100):
        assert mapper.get_arango_key(f"id{i}") == f"key{i}"

def test_process_relationship_skip_missing(
    mock_edges_col: MagicMock,
    id_mapper: IDMapper,
    edge_doc: Dict[str, Any]
):
    """Test processing relationship with missing refs (skip mode)."""
    config = ImportConfig(skip_missing_refs=True)
    
    # No mappings in id_mapper yet
    result = _process_relationship_document(edge_doc, mock_edges_col, id_mapper, config)
    assert result == 0  # Should skip due to missing refs
    mock_edges_col.import_bulk.assert_not_called()
    
    # Add one mapping but not both
    id_mapper.add_mapping("123", "node1")
    result = _process_relationship_document(edge_doc, mock_edges_col, id_mapper, config)
    assert result == 0  # Should still skip
    mock_edges_col.import_bulk.assert_not_called()
    
    # Add both mappings
    id_mapper.add_mapping("456", "node2")
    result = _process_relationship_document(edge_doc, mock_edges_col, id_mapper, config)
    assert result == 1  # Should succeed
    mock_edges_col.import_bulk.assert_called_once()

def test_process_relationship_fail_missing(
    mock_edges_col: MagicMock,
    id_mapper: IDMapper,
    edge_doc: Dict[str, Any]
):
    """Test processing relationship with missing refs (fail mode)."""
    config = ImportConfig(skip_missing_refs=False)
    
    # No mappings should raise error
    with pytest.raises(ValueError) as exc:
        _process_relationship_document(edge_doc, mock_edges_col, id_mapper, config)
    assert "Missing node mapping" in str(exc.value)
    
    # Partial mappings should still raise
    id_mapper.add_mapping("123", "node1")
    with pytest.raises(ValueError) as exc:
        _process_relationship_document(edge_doc, mock_edges_col, id_mapper, config)
    assert "Missing node mapping" in str(exc.value)
    
    # Complete mappings should succeed
    id_mapper.add_mapping("456", "node2")
    result = _process_relationship_document(edge_doc, mock_edges_col, id_mapper, config)
    assert result == 1

def test_process_relationship_correct_keys(
    mock_edges_col: MagicMock,
    id_mapper: IDMapper,
    edge_doc: Dict[str, Any]
):
    """Test that correct ArangoDB keys are used in edge refs."""
    config = ImportConfig()
    id_mapper.add_mapping("123", "node1")
    id_mapper.add_mapping("456", "node2")
    
    _process_relationship_document(edge_doc, mock_edges_col, id_mapper, config)
    
    # Check that the edge was created with correct ArangoDB keys
    call_args = mock_edges_col.import_bulk.call_args[0][0]
    assert len(call_args) == 1
    created_edge = call_args[0]
    assert created_edge["_from"] == "Nodes/node1"
    assert created_edge["_to"] == "Nodes/node2"
    assert created_edge["properties"] == edge_doc["properties"]
