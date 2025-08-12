"""
src/food_scanner/data/transformers/cache/smart_deduplicator.py
Responsibility: Intelligent deduplication
- Just deduplication of data
- Differentiated TTL according to product status
- Monitoring of deduplication process
- Graceful fallback if no cache
- Multiple deduplication strategies
- Automatic cache cleaning : expired entries are removed
- Differentiated TTL 
"""

import json
import hashlib
from datetime import datetime
from typing import Dict, Any, List
from dataclasses import dataclass

from .cache_config import CacheConfig
from .cache_strategies import get_production_config, get_development_config, get_testing_config, get_retry_focused_config



class SmartDeduplicator:
    """
    RESPONSIBILITY : Intelligent deduplication
    - Same behavior with empty or full cache
    - Multiple deduplication strategies
    - Graceful fallback if no cache
    - Differentiated TTL according to product status
    - Monitoring of deduplication process
    """
    
    def __init__(self, config: CacheConfig):
        self.config = config
        self.cache_file = config.backup_dir / "deduplication_cache.json" if config.backup_dir else None
        self.deduplication_cache = self._load_deduplication_cache() if self.cache_file else {}
    
        # Monitoring stats
        self.stats = {
            "cache_hits": 0,
            "cache_misses": 0,
            "total_requests": 0,
            "expired_entries_cleaned": 0
        }

    def remove_duplicates(self, extracted_products: Dict[str, Any],timestamp: str) -> Dict[str, Any]:
        """
        Remove duplicates - Same behavior with empty or full cache
        Differentiated TTL 
        Automatic cache cleaning : expired entries are removed
        """
        if not self.config.enable_deduplication:
            print(f"      ⚠️ Deduplication disabled")
            return extracted_products
        
        # Automatic cache cleaning : expired entries are removed
        self._clean_expired_entries(timestamp)
        
        print(f"      🔍 Deduplication: {self.config.deduplication_strategy} strategy")
        
        if self.config.deduplication_strategy == "disabled":
            return extracted_products
        elif self.config.deduplication_strategy == "time_based":
            return self._time_based_deduplication(extracted_products, timestamp)
        elif self.config.deduplication_strategy == "content_based":
            return self._content_based_deduplication(extracted_products, timestamp)
        else:
            print(f"      ⚠️ Unknown deduplication strategy: {self.config.deduplication_strategy}")
            return extracted_products
    
    def _time_based_deduplication(self, extracted_products: Dict[str, Any],timestamp: str) -> Dict[str, Any]:
        """Time-based deduplication with different TTL according to product status"""
        
        non_duplicates = {}
        duplicates_found = 0
        current_time = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
        
        for barcode, product_data in extracted_products.items():
            self.stats["total_requests"] += 1
            # Check the cache (if available)
            cached_entry = self.deduplication_cache.get(barcode)
            
            is_duplicate = False
            
            if cached_entry:
                try:
                    last_seen_str = cached_entry.get("last_seen", "")
                    last_seen = datetime.fromisoformat(last_seen_str.replace('Z', '+00:00'))
                    # Differentiated TTL according to product status
                    product_status = cached_entry.get("status", "unknown")
                    ttl_hours = self._get_ttl_for_status(product_status)

                    hours_diff = (current_time - last_seen).total_seconds() / 3600
                    
                    if hours_diff < ttl_hours:
                        is_duplicate = True
                        duplicates_found += 1
                        self.stats["cache_hits"] += 1
                        print(f"      🎯 Cache HIT: {barcode} (status: {product_status}, age: {hours_diff:.1f}h)")
                    else:
                        self.stats["cache_misses"] += 1
                        print(f"      ⏰ Cache EXPIRED: {barcode} (status: {product_status}, age: {hours_diff:.1f}h)")
                except ValueError:
                    # If date parsing fails, do not consider it as duplicate
                    self.stats["cache_misses"] += 1
            else:
                self.stats["cache_misses"] += 1   
            
            if not is_duplicate:
                non_duplicates[barcode] = product_data
                
                # Update the cache (without status, will be updated after validation)
                self.deduplication_cache[barcode] = {
                    "last_seen": timestamp,
                    "first_seen": cached_entry.get("first_seen", timestamp) if cached_entry else timestamp,
                    "seen_count": cached_entry.get("seen_count", 0) + 1 if cached_entry else 1,
                    "status": cached_entry.get("status", "pending") # Keep the status of the previous entry
                }
        
        # Save the cache (if possible)
        self._save_deduplication_cache()
        
        print(f"      → Duplicates removed: {duplicates_found}")
        print(f"      → Products kept: {len(non_duplicates)}")
        
        return non_duplicates

    def _get_ttl_for_status(self, status: str) -> int:
        """Return the appropriate TTL according to the product status"""
        ttl_mapping = {
            "validated": self.config.validated_ttl_hours,
            "rejected": self.config.rejected_ttl_hours,
            "partial": self.config.partial_ttl_hours,
            "pending": self.config.deduplication_window_hours,
            "unknown": self.config.deduplication_window_hours
        }
        return ttl_mapping.get(status, self.config.deduplication_window_hours)

    def update_product_status(self, barcode: str, status: str, timestamp: str) -> None:
        """Update the status of a product in the cache"""
        if barcode in self.deduplication_cache:
            self.deduplication_cache[barcode]["status"] = status
            self.deduplication_cache[barcode]["status_updated_at"] = timestamp
            self._save_deduplication_cache()
    
    def _clean_expired_entries(self, current_timestamp: str) -> None:
        """Clean automatically expired entries"""
        if not self.deduplication_cache:
            return
        
        current_time = datetime.fromisoformat(current_timestamp.replace('Z', '+00:00'))
        expired_keys = []
        
        for barcode, entry in self.deduplication_cache.items():
            try:
                last_seen_str = entry.get("last_seen", "")
                last_seen = datetime.fromisoformat(last_seen_str.replace('Z', '+00:00'))
                
                status = entry.get("status", "unknown")
                ttl_hours = self._get_ttl_for_status(status)
                
                hours_diff = (current_time - last_seen).total_seconds() / 3600
                
                if hours_diff > ttl_hours:
                    expired_keys.append(barcode)
                    
            except (ValueError, KeyError):
                # If parsing fails, mark as expired
                expired_keys.append(barcode)
        
        # Remove expired entries
        for key in expired_keys:
            del self.deduplication_cache[key]
            self.stats["expired_entries_cleaned"] += 1
        
        if expired_keys:
            print(f"      🧹 Cleaned {len(expired_keys)} expired cache entries")
            self._save_deduplication_cache()
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Detailed cache statistics for monitoring"""
        if not self.config.enable_monitoring:
            return {}
        
        total_requests = self.stats["total_requests"]
        hit_rate = (self.stats["cache_hits"] / total_requests * 100) if total_requests > 0 else 0
        
        # Analyze the composition of the cache
        status_breakdown = {}
        for entry in self.deduplication_cache.values():
            status = entry.get("status", "unknown")
            status_breakdown[status] = status_breakdown.get(status, 0) + 1
        
        return {
            "cache_size": len(self.deduplication_cache),
            "hit_rate_percent": round(hit_rate, 1),
            "cache_hits": self.stats["cache_hits"],
            "cache_misses": self.stats["cache_misses"],
            "total_requests": total_requests,
            "expired_entries_cleaned": self.stats["expired_entries_cleaned"],
            "status_breakdown": status_breakdown,
            "ttl_config": {
                "validated_ttl_days": self.config.validated_ttl_hours // 24,
                "rejected_ttl_days": self.config.rejected_ttl_hours // 24,
                "partial_ttl_days": self.config.partial_ttl_hours // 24
            }
        }
    
    def get_rejected_products_for_retry_deduplication(self) -> List[str]:
        """Get the barcodes of rejected products for retry"""
        rejected_barcodes = []
        
        for barcode, entry in self.deduplication_cache.items():
            if entry.get("status") == "rejected":
                rejected_barcodes.append(barcode)
        
        return rejected_barcodes

    def _content_based_deduplication(self, extracted_products: Dict[str, Any],timestamp: str) -> Dict[str, Any]:
        """Content-based deduplication"""
        
        non_duplicates = {}
        content_hashes_seen = set()
        
        # Load existing hashes from the cache
        for cached_entry in self.deduplication_cache.values():
            existing_hash = cached_entry.get("content_hash")
            if existing_hash:
                content_hashes_seen.add(existing_hash)
        
        duplicates_found = 0
        
        for barcode, product_data in extracted_products.items():
            extracted_fields = product_data.get("extracted_fields", {})
            
            # Calculate the content hash
            content_hash = self._calculate_content_hash(extracted_fields)
            
            if content_hash not in content_hashes_seen:
                non_duplicates[barcode] = product_data
                content_hashes_seen.add(content_hash)
                self.stats["cache_misses"] += 1                
                # Update the cache
                self.deduplication_cache[barcode] = {
                    "content_hash": content_hash,
                    "last_seen": timestamp,
                    "first_seen": timestamp,
                    "status": "pending" 
                }
            else:
                duplicates_found += 1
                self.stats["cache_hits"] += 1
        
        self._save_deduplication_cache()
        
        print(f"      → Content duplicates removed: {duplicates_found}")
        return non_duplicates
    
    def _calculate_content_hash(self, extracted_fields: Dict[str, Any]) -> str:
        """Calculate a content hash to detect duplicates"""
        content_fields = {
            "product_name": extracted_fields.get("product_name", ""),
            "brand_name": extracted_fields.get("brand_name", ""),
            "weight": str(extracted_fields.get("weight", "")),
            "unit": extracted_fields.get("product_quantity_unit", "")
        }
        
        content_str = json.dumps(content_fields, sort_keys=True)
        return hashlib.md5(content_str.encode()).hexdigest()
    
    def _load_deduplication_cache(self) -> Dict[str, Any]:
        """Load the deduplication cache (graceful failure)"""
        if not self.cache_file or not self.cache_file.exists():
            return {}
        
        try:
            with open(self.cache_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"      ⚠️ Could not load deduplication cache: {e}")
            return {}
    
    def _save_deduplication_cache(self):
        """Save the deduplication cache (graceful failure)"""
        if not self.cache_file:
            return
        
        try:
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump(self.deduplication_cache, f, indent=2, default=str, ensure_ascii=False)
        except Exception as e:
            print(f"      ⚠️ Could not save deduplication cache: {e}")
