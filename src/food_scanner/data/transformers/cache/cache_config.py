"""
src/food_scanner/data/transformers/cache/cache_config.py
Responsibility: Configuration of the cache
Shared by the whole ETL pipeline
- No business logic
- Just configuration of the cache
- Always optional (never breaks the pipeline)
"""
from pathlib import Path
from dataclasses import dataclass


@dataclass
class CacheConfig:
    """System cache configuration"""
    enable_backup: bool = True          # Local backup
    enable_deduplication: bool = True   # Deduplication
    backup_dir: Path = None             # Backup directory
    deduplication_strategy: str = "time_based"  # time_based, content_based, disabled
    deduplication_window_hours: int = 24        # Deduplication window
    # Differentiated TTL
    validated_ttl_hours: int = 90 * 24  # 90 days for validated products
    rejected_ttl_hours: int = 7 * 24    # 7 days for rejected products
    partial_ttl_hours: int = 30 * 24    # 30 days for partial products    
    # Monitoring
    enable_monitoring: bool = True      # Cache statistics  