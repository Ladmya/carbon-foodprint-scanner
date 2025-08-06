"""
src/food_scanner/data/utils/duplicate_handler.py
DUPLICATE MANAGEMENT: Handle product duplicates across collection runs
"""

import json
import hashlib
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Any


class DuplicateHandler:
    """
    RESPONSIBILITY: Manage product duplicates across collection runs
    - Track previously collected products
    - Detect duplicates by barcode and content hash
    - Merge data from multiple collection runs
    - Maintain collection history and freshness
    """
    
    def __init__(self, cache_dir: Path = None):
        if cache_dir is None:
            cache_dir = Path(__file__).resolve().parents[4] / "data" / "cache"
        
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        # Cache files
        self.products_cache_file = self.cache_dir / "collected_products_cache.json"
        self.collection_history_file = self.cache_dir / "collection_history.json"
        
        # In-memory caches
        self.products_cache = self._load_products_cache()
        self.collection_history = self._load_collection_history()
        
        # Deduplication stats
        self.dedup_stats = {
            "total_processed": 0,
            "exact_duplicates": 0,
            "content_duplicates": 0,
            "fresh_products": 0,
            "updated_products": 0,
            "cache_hits": 0
        }

    def process_discovered_products(
            self,
            discovered_products: Dict[str, Any], 
            collection_timestamp: str
        ) -> Dict[str, Any]:
        """
        Process discovered products and remove duplicates
        Returns: {new_products, duplicate_products, processing_stats}
        """
        print(f"\n🔍 DUPLICATE DETECTION: Processing {len(discovered_products)} discovered products")
        
        new_products = {}
        duplicate_products = {}
        
        for barcode, product_data in discovered_products.items():
            self.dedup_stats["total_processed"] += 1
            
            duplicate_info = self._check_duplicate(barcode, product_data, collection_timestamp)
            
            if duplicate_info["is_duplicate"]:
                duplicate_products[barcode] = {
                    **product_data,
                    "duplicate_info": duplicate_info
                }
                
                if duplicate_info["duplicate_type"] == "exact":
                    self.dedup_stats["exact_duplicates"] += 1
                elif duplicate_info["duplicate_type"] == "content":
                    self.dedup_stats["content_duplicates"] += 1
                    
                self.dedup_stats["cache_hits"] += 1
                
            else:
                new_products[barcode] = product_data
                self.dedup_stats["fresh_products"] += 1
                
                # Add to cache
                self._add_to_cache(barcode, product_data, collection_timestamp)
        
        print(f"   → New products: {len(new_products)}")
        print(f"   → Exact duplicates: {self.dedup_stats['exact_duplicates']}")
        print(f"   → Content duplicates: {self.dedup_stats['content_duplicates']}")
        print(f"   → Cache efficiency: {(self.dedup_stats['cache_hits'] / max(1, self.dedup_stats['total_processed']) * 100):.1f}%")
        
        return {
            "new_products": new_products,
            "duplicate_products": duplicate_products,
            "processing_stats": self.dedup_stats.copy()
        }

    def process_validated_products(
            self, 
            validated_products: Dict[str, Any],
            collection_timestamp: str
        ) -> Dict[str, Any]:
        """
        Process validated products and update cache with fresh data
        Returns: {merged_products, update_stats}
        """
        print(f"\n🔄 PRODUCT MERGING: Processing {len(validated_products)} validated products")
        
        merged_products = {}
        update_stats = {
            "products_merged": 0,
            "products_updated": 0,
            "new_products_cached": 0
        }
        
        for barcode, product_data in validated_products.items():
            cached_product = self.products_cache.get(barcode)
            
            if cached_product:
                # Merge with existing data
                merged_product = self._merge_product_data(cached_product, product_data, collection_timestamp)
                merged_products[barcode] = merged_product
                update_stats["products_merged"] += 1
                
                # Update cache
                self._update_cached_product(barcode, merged_product, collection_timestamp)
                
                if merged_product.get("data_updated", False):
                    update_stats["products_updated"] += 1
                    
            else:
                # New product
                merged_products[barcode] = product_data
                update_stats["new_products_cached"] += 1
                
                # Add to cache
                self._add_validated_to_cache(barcode, product_data, collection_timestamp)
        
        print(f"   → Products merged: {update_stats['products_merged']}")
        print(f"   → Products updated: {update_stats['products_updated']}")
        print(f"   → New products cached: {update_stats['new_products_cached']}")
        
        return {
            "merged_products": merged_products,
            "update_stats": update_stats
        }

    def clean_expired_cache(self, max_age_days: int = 30) -> Dict[str, int]:
        """
        Clean expired products from cache
        """
        print(f"\n🧹 CACHE CLEANUP: Removing products older than {max_age_days} days")
        
        cutoff_date = datetime.now() - timedelta(days=max_age_days)
        
        expired_products = []
        for barcode, cached_data in self.products_cache.items():
            last_seen = cached_data.get("last_collection_timestamp")
            if last_seen:
                try:
                    last_seen_date = datetime.fromisoformat(last_seen.replace('Z', '+00:00'))
                    if last_seen_date < cutoff_date:
                        expired_products.append(barcode)
                except ValueError:
                    # Invalid date format, consider expired
                    expired_products.append(barcode)
        
        # Remove expired products
        for barcode in expired_products:
            del self.products_cache[barcode]
        
        # Clean collection history
        expired_collections = []
        for collection_id, collection_data in self.collection_history.items():
            collection_date_str = collection_data.get("timestamp", "")
            try:
                collection_date = datetime.fromisoformat(collection_date_str.replace('Z', '+00:00'))
                if collection_date < cutoff_date:
                    expired_collections.append(collection_id)
            except ValueError:
                expired_collections.append(collection_id)
        
        for collection_id in expired_collections:
            del self.collection_history[collection_id]
        
        # Save cleaned caches
        self._save_products_cache()
        self._save_collection_history()
        
        cleanup_stats = {
            "expired_products_removed": len(expired_products),
            "expired_collections_removed": len(expired_collections),
            "remaining_products": len(self.products_cache),
            "remaining_collections": len(self.collection_history)
        }
        
        print(f"   → Expired products removed: {cleanup_stats['expired_products_removed']}")
        print(f"   → Remaining cached products: {cleanup_stats['remaining_products']}")
        
        return cleanup_stats

    def _check_duplicate(self, barcode: str, product_data: Dict[str, Any], 
                        collection_timestamp: str) -> Dict[str, Any]:
        """Check if product is duplicate of cached data"""
        cached_product = self.products_cache.get(barcode)
        
        if not cached_product:
            return {"is_duplicate": False, "duplicate_type": None}
        
        # Check exact duplicate (same barcode, recent collection)
        last_collection = cached_product.get("last_collection_timestamp", "")
        try:
            last_date = datetime.fromisoformat(last_collection.replace('Z', '+00:00'))
            current_date = datetime.fromisoformat(collection_timestamp.replace('Z', '+00:00'))
            
            # If collected within 24 hours, consider exact duplicate
            if (current_date - last_date).total_seconds() < 86400:  # 24 hours
                return {
                    "is_duplicate": True,
                    "duplicate_type": "exact",
                    "last_collection": last_collection,
                    "hours_ago": (current_date - last_date).total_seconds() / 3600
                }
        except ValueError:
            pass
        
        # Check content duplicate (same content hash)
        current_content_hash = self._generate_content_hash(product_data)
        cached_content_hash = cached_product.get("content_hash")
        
        if current_content_hash == cached_content_hash:
            return {
                "is_duplicate": True,
                "duplicate_type": "content",
                "last_collection": last_collection,
                "content_hash": current_content_hash
            }
        
        # Not a duplicate, but exists in cache (will be updated)
        return {
            "is_duplicate": False,
            "duplicate_type": None,
            "cache_exists": True,
            "content_changed": True
        }

    def _generate_content_hash(self, product_data: Dict[str, Any]) -> str:
        """Generate hash of product content for duplicate detection"""
        # Extract key fields for hashing
        raw_data = product_data.get("raw_discovery_data", {})
        
        hash_fields = {
            "product_name": raw_data.get("product_name", ""),
            "product_name_fr": raw_data.get("product_name_fr", ""),
            "brands": raw_data.get("brands", ""),
            "product_quantity": raw_data.get("product_quantity", ""),
            "quantity": raw_data.get("quantity", "")
        }
        
        # Create deterministic hash
        content_string = json.dumps(hash_fields, sort_keys=True)
        return hashlib.md5(content_string.encode()).hexdigest()

    def _add_to_cache(
            self, 
            barcode: str, 
            product_data: Dict[str, Any], 
            collection_timestamp: str
        ):
        """Add discovered product to cache"""
        self.products_cache[barcode] = {
            "barcode": barcode,
            "first_collection_timestamp": collection_timestamp,
            "last_collection_timestamp": collection_timestamp,
            "collection_count": 1,
            "content_hash": self._generate_content_hash(product_data),
            "discovery_data": product_data,
            "validation_status": "pending"
        }

    def _add_validated_to_cache(
            self, 
            barcode: str, 
            product_data: Dict[str, Any],
            collection_timestamp: str
        ):
        """Add validated product to cache"""
        existing = self.products_cache.get(barcode, {})
        
        self.products_cache[barcode] = {
            **existing,
            "barcode": barcode,
            "last_validation_timestamp": collection_timestamp,
            "validation_status": "validated",
            "validated_data": product_data,
            "validation_count": existing.get("validation_count", 0) + 1
        }

    def _merge_product_data(
            self, 
            cached_product: Dict[str, Any], 
            new_product: Dict[str, Any],
            collection_timestamp: str
        ) -> Dict[str, Any]:
        """Merge new product data with cached data"""
        # Use new data as base, but preserve collection history
        merged = {
            **new_product,
            "collection_history": {
                "first_collected": cached_product.get("first_collection_timestamp"),
                "last_collected": collection_timestamp,
                "collection_count": cached_product.get("collection_count", 0) + 1,
                "validation_count": cached_product.get("validation_count", 0) + 1
            }
        }
        
        # Check if data actually changed
        cached_validated = cached_product.get("validated_data", {})
        if cached_validated:
            # Compare key fields to detect updates
            key_fields = ["product_name", "brand_name", "weight", "co2_total"]
            data_changed = False
            
            for field in key_fields:
                if new_product.get("transformed_data", {}).get(field) != cached_validated.get("transformed_data", {}).get(field):
                    data_changed = True
                    break
            
            merged["data_updated"] = data_changed
        else:
            merged["data_updated"] = True  # First validation
        
        return merged

    def _update_cached_product(
            self, 
            barcode: str, 
            merged_product: Dict[str, Any],
            collection_timestamp: str
        ):
        """Update cached product with merged data"""
        self.products_cache[barcode] = {
            **self.products_cache.get(barcode, {}),
            "last_validation_timestamp": collection_timestamp,
            "validation_status": "validated",
            "validated_data": merged_product,
            "validation_count": self.products_cache.get(barcode, {}).get("validation_count", 0) + 1
        }

    def _load_products_cache(self) -> Dict[str, Any]:
        """Load products cache from file"""
        if self.products_cache_file.exists():
            try:
                with open(self.products_cache_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except (json.JSONDecodeError, IOError):
                return {}
        return {}

    def _save_products_cache(self):
        """Save products cache to file"""
        with open(self.products_cache_file, 'w', encoding='utf-8') as f:
            json.dump(self.products_cache, f, indent=2, ensure_ascii=False, default=str)

    def _load_collection_history(self) -> Dict[str, Any]:
        """Load collection history from file"""
        if self.collection_history_file.exists():
            try:
                with open(self.collection_history_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except (json.JSONDecodeError, IOError):
                return {}
        return {}

    def _save_collection_history(self):
        """Save collection history to file"""
        with open(self.collection_history_file, 'w', encoding='utf-8') as f:
            json.dump(self.collection_history, f, indent=2, ensure_ascii=False, default=str)

    def record_collection_run(self, collection_metadata: Dict[str, Any]) -> str:
        """Record a collection run in history"""
        collection_id = collection_metadata.get("timestamp", datetime.now().strftime("%Y%m%d_%H%M%S"))
        
        self.collection_history[collection_id] = {
            **collection_metadata,
            "recorded_at": datetime.now().isoformat()
        }
        
        self._save_collection_history()
        return collection_id

    def get_cache_statistics(self) -> Dict[str, Any]:
        """Get comprehensive cache statistics"""
        total_products = len(self.products_cache)
        validated_products = len([p for p in self.products_cache.values() 
                                if p.get("validation_status") == "validated"])
        
        return {
            "cache_statistics": {
                "total_cached_products": total_products,
                "validated_products": validated_products,
                "pending_validation": total_products - validated_products,
                "total_collections": len(self.collection_history)
            },
            "deduplication_stats": self.dedup_stats.copy(),
            "cache_files": {
                "products_cache": str(self.products_cache_file),
                "collection_history": str(self.collection_history_file)
            }
        }

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Save caches on exit
        self._save_products_cache()
        self._save_collection_history()


# TESTING
if __name__ == "__main__":
    print("🧪 TESTING DUPLICATE HANDLER")
    print("=" * 50)
    
    with DuplicateHandler() as handler:
        # Test with sample data
        sample_products = {
            "3017620422003": {
                "raw_discovery_data": {
                    "code": "3017620422003",
                    "product_name_fr": "Nutella",
                    "brands": "Nutella",
                    "product_quantity": "400g"
                },
                "discovered_via": {"brand": "Nutella", "category": "Chocolates"}
            },
            "duplicate_test": {
                "raw_discovery_data": {
                    "code": "duplicate_test",
                    "product_name_fr": "Test Product",
                    "brands": "Test Brand",
                    "product_quantity": "100g"
                },
                "discovered_via": {"brand": "Test", "category": "Test"}
            }
        }
        
        timestamp = datetime.now().isoformat()
        
        # First collection run
        print("🔍 First collection run:")
        result1 = handler.process_discovered_products(sample_products, timestamp)
        print(f"   New products: {len(result1['new_products'])}")
        
        # Second collection run (should detect duplicates)
        print("\n🔍 Second collection run (same products):")
        result2 = handler.process_discovered_products(sample_products, timestamp)
        print(f"   New products: {len(result2['new_products'])}")
        print(f"   Duplicates: {len(result2['duplicate_products'])}")
        
        # Show cache statistics
        stats = handler.get_cache_statistics()
        print(f"\n📊 Cache Statistics:")
        print(f"   Cached products: {stats['cache_statistics']['total_cached_products']}")
        print(f"   Cache hits: {stats['deduplication_stats']['cache_hits']}")