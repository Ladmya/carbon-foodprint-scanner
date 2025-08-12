"""
src/food_scanner/data/transformers/processing/metadata_processor.py
    RESPONSIBILITY: Add system metadata to transformed products
    
    BUSINESS LOGIC:
    1. Add system timestamps (created_at, updated_at, cache_expires_at)
    2. Add transformation metadata (version, collection_timestamp)
    3. Add raw data backup as JSONB
    4. Track metadata operations
"""

from typing import Dict, Any
from datetime import datetime, timedelta


class MetadataProcessor:
    """
    RESPONSIBILITY: Add system metadata to transformed products
    
    BUSINESS LOGIC:
    1. Add system timestamps (created_at, updated_at, cache_expires_at)
    2. Add transformation metadata (version, collection_timestamp)
    3. Add raw data backup as JSONB
    4. Track metadata operations
    """

    def __init__(self):
        self.metadata_stats = {
            "products_processed": 0,
            "metadata_fields_added": 0,
            "raw_data_backups_created": 0
        }
    
    def add_system_metadata(self, enhanced_products: Dict[str, Any], collection_timestamp: str) -> Dict[str, Any]:
        """
        Add system metadata to all enhanced products
        
        Args:
            enhanced_products: Products with derived fields calculated
            collection_timestamp: Original collection timestamp
            
        Returns:
            Products with complete system metadata
        """
        current_time = datetime.now()
        cache_expires = current_time + timedelta(days=30)
        
        products_with_metadata = {}
        
        for barcode, product_data in enhanced_products.items():
            transformed_data = product_data["transformed_data"]
            
            # Add system metadata
            transformed_data.update({
                "created_at": current_time.isoformat(),
                "updated_at": current_time.isoformat(), 
                "cache_expires_at": cache_expires.isoformat(),
                "collection_timestamp": collection_timestamp,
                "transformation_version": "2.1-robust"
            })
            
            # Add raw data backup as JSONB
            raw_api_data = product_data.get("raw_data_backup", {}).get("raw_api_data", {})
            if raw_api_data:
                transformed_data["raw_data"] = raw_api_data.get("raw_api_response", {})
                self.metadata_stats["raw_data_backups_created"] += 1
            
            products_with_metadata[barcode] = product_data
            self.metadata_stats["products_processed"] += 1
            self.metadata_stats["metadata_fields_added"] += 5  # 5 metadata fields added
        
        return products_with_metadata
    
    def get_metadata_stats(self) -> Dict[str, Any]:
        """Get metadata processing statistics"""
        return self.metadata_stats.copy()
    
    def reset_stats(self):
        """Reset metadata processing statistics"""
        self.metadata_stats = {
            "products_processed": 0,
            "metadata_fields_added": 0,
            "raw_data_backups_created": 0
        }