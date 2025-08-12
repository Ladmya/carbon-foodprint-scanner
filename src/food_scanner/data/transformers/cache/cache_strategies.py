"""
src/food_scanner/data/transformers/cache/cache_strategies.py
Responsibility: Different configurations for different scenarios
- No business logic
- Just configuration of the cache
- Always optional (never breaks the pipeline)
"""

from cache_config import CacheConfig

# Different configurations for different scenarios
def get_development_config() -> CacheConfig:  # TODO: try this method 
    """Development configuration - reproducible tests"""
    return CacheConfig(
        enable_backup=True,
        enable_deduplication=False,  # Disabled for tests
        deduplication_strategy="disabled",
        enable_monitoring=True
    )

def get_production_config() -> CacheConfig: # TODO: try this method 
    """Configuration pour production - toutes optimisations"""
    return CacheConfig(
        enable_backup=True,
        enable_deduplication=True,
        deduplication_strategy="time_based",
        deduplication_window_hours=24,
        validated_ttl_hours=90 * 24,    # 90 days for validated products
        rejected_ttl_hours=7 * 24,      # 7 days for rejected products  
        partial_ttl_hours=30 * 24,      # 30 days for partial products
        enable_monitoring=True
    )

def get_testing_config() -> CacheConfig:  # TODO: try this method 
    """Configuration for tests - predictable behavior"""
    return CacheConfig(
        enable_backup=False,
        enable_deduplication=False,
        deduplication_strategy="disabled",
        enable_monitoring=True
    )

def get_retry_focused_config() -> CacheConfig:  # TODO: try this method 
    """Configuration optimized for re-testing rejected products"""
    return CacheConfig(
        enable_backup=True,
        enable_deduplication=True,
        deduplication_strategy="time_based",
        deduplication_window_hours=6,           # Short for quick retry
        validated_ttl_hours=180 * 24,   # Long for validated products (6 months)
        rejected_ttl_hours=3 * 24,      # Very short for rejected products (3 days)
        partial_ttl_hours=14 * 24,      # Medium for partial products (2 weeks)
        enable_monitoring=True
    )
