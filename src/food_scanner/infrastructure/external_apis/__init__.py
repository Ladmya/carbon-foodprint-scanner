"""
External API clients for data retrieval.
"""

from .openfoodfacts import OpenFoodFactsClient
from .base_client import BaseAPIClient

__all__ = [
    "OpenFoodFactsClient",
    "BaseAPIClient",
]