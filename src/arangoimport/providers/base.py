"""
Base classes and interfaces for data providers.

This module defines the abstract interfaces that all data providers must implement
to be compatible with the ArangoImport system.
"""
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Tuple, Generator

from ..config import ImportConfig


class DataProvider(ABC):
    """
    Abstract base class for data providers.
    
    A data provider is responsible for reading source data and transforming it
    into a format that can be imported into ArangoDB. Different providers can
    handle different source formats and transformations.
    """
    
    @abstractmethod
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the provider with configuration settings.
        
        Args:
            config: Configuration dictionary for this provider
        """
        pass
    
    @abstractmethod
    def get_nodes(self) -> Generator[Dict[str, Any], None, None]:
        """
        Get a generator yielding node data.
        
        Returns:
            Generator yielding dictionaries representing nodes
        """
        pass
    
    @abstractmethod
    def get_edges(self) -> Generator[Dict[str, Any], None, None]:
        """
        Get a generator yielding edge data.
        
        Returns:
            Generator yielding dictionaries representing edges
        """
        pass
    
    @abstractmethod
    def validate_node(self, node: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """
        Validate a node dictionary.
        
        Args:
            node: Node data to validate
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        pass
    
    @abstractmethod
    def validate_edge(self, edge: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """
        Validate an edge dictionary.
        
        Args:
            edge: Edge data to validate
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        pass
    
    @abstractmethod
    def transform_node(self, node: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform a node dictionary to match the ArangoDB format.
        
        Args:
            node: Node data to transform
            
        Returns:
            Transformed node dictionary
        """
        pass
    
    @abstractmethod
    def transform_edge(self, edge: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform an edge dictionary to match the ArangoDB format.
        
        Args:
            edge: Edge data to transform
            
        Returns:
            Transformed edge dictionary
        """
        pass


class ProviderFactory:
    """
    Factory class for creating provider instances.
    
    This class handles provider registration and instantiation.
    """
    
    _providers = {}
    
    @classmethod
    def register_provider(cls, name: str, provider_class):
        """
        Register a provider class.
        
        Args:
            name: Provider name
            provider_class: Provider class to register
        """
        cls._providers[name] = provider_class
    
    @classmethod
    def create_provider(cls, name: str, config: Dict[str, Any]) -> DataProvider:
        """
        Create a provider instance.
        
        Args:
            name: Provider name
            config: Configuration for the provider
            
        Returns:
            DataProvider instance
            
        Raises:
            ValueError: If the provider is not registered
        """
        if name not in cls._providers:
            raise ValueError(f"Provider '{name}' not registered")
        
        return cls._providers[name](config)
