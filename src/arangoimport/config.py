"""Configuration for import process."""

from dataclasses import dataclass, field
from typing import Dict, Any, List

@dataclass
class ImportConfig:
    """Configuration for import process."""
    dedup_enabled: bool = True
    validate_nodes: bool = True
    transform_enabled: bool = True
    batch_size: int = 1000
    error_threshold: float = 0.01  # Max error rate before failing
    node_type_configs: Dict[str, "NodeTypeConfig"] = field(default_factory=dict)

@dataclass
class NodeTypeConfig:
    """Configuration for specific node types."""
    required_fields: List[str]
    unique_fields: List[str]
    property_types: Dict[str, type]
    transform_rules: Dict[str, Any]
    dedup_fields: List[str]

# Default configurations for different node types
DEFAULT_CONFIGS = {
    "Gene": NodeTypeConfig(
        required_fields=["id", "properties.name", "properties.organism"],
        unique_fields=["properties.name", "properties.organism"],
        property_types={
            "name": str,
            "organism": str,
            "ensembl": str,
        },
        transform_rules={
            "ensembl": lambda x: str(x) if x else "",
            "name": str.lower,
        },
        dedup_fields=["name", "organism"]
    ),
    "Protein": NodeTypeConfig(
        required_fields=["id", "properties.name", "properties.organism"],
        unique_fields=["properties.name", "properties.organism"],
        property_types={
            "name": str,
            "organism": str,
            "uniprot": str,
        },
        transform_rules={
            "uniprot": lambda x: str(x) if x else "",
            "name": str.lower,
        },
        dedup_fields=["name", "organism"]
    ),
    "Compound": NodeTypeConfig(
        required_fields=["id", "properties.name", "properties.inchikey"],
        unique_fields=["properties.inchikey"],
        property_types={
            "name": str,
            "inchikey": str,
            "smiles": str,
        },
        transform_rules={
            "inchikey": str.upper,
            "smiles": str,
        },
        dedup_fields=["inchikey"]
    ),
}
