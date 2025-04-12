"""Provider registration and management.

This module handles registration of data providers and provides
access to the provider factory.
"""
from typing import Dict, Any, Type

# Import the provider base class and factory
from .base import DataProvider, ProviderFactory

# Import provider implementations
from .spoke.adapters import SpokeProvider

# Register default providers
ProviderFactory.register_provider("spoke", SpokeProvider)


def get_provider(name: str, config: Dict[str, Any]) -> DataProvider:
    """Get a provider instance by name.
    
    Args:
        name: Provider name
        config: Provider configuration
        
    Returns:
        Provider instance
    """
    return ProviderFactory.create_provider(name, config)


def list_providers() -> list[str]:
    """List available provider names.
    
    Returns:
        List of registered provider names
    """
    return list(ProviderFactory._providers.keys())