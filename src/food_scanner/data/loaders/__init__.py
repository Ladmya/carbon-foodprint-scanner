# src/food_scanner/data/loaders/__init__.py
"""
Loading components for Carbon Foodprint Scanner

This package contains components for loading validated products 
to Supabase database with proper batching, error handling,
and progress monitoring.

Main Components:
- batch_loader.py: Batch processing logic
- loading_monitor.py: Progress tracking and metrics

Usage:
    from food_scanner.data.loaders import ProductBatchLoader, LoadingMonitor
    # For load_products_to_supabase.py: Main loading script in data_engineering/scripts/loading/
    # python data_engineering/scripts/loading/load_products_to_supabase.py --auto-find --environment test
"""

from .batch_loader import ProductBatchLoader, BatchValidationError, SupabaseConnectionError
from .loading_monitor import LoadingMonitor, ProgressDisplay

__version__ = "1.0.0"
__author__ = "Carbon Foodprint Scanner by Ladmya"

__all__ = [
    "ProductBatchLoader",
    "LoadingMonitor", 
    "ProgressDisplay",
    "BatchValidationError",
    "SupabaseConnectionError"
]

