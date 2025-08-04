"""
src/food_scanner/bot/database_service.py
Database Service for Telegram Bot
Handles all database queries using existing Supabase client
Optimized for scalability with indexed queries
"""

import logging
from typing import Optional, List, Dict, Any
from ..infrastructure.database.repositories.supabase_client import get_supabase_client

logger = logging.getLogger(__name__)


class DatabaseService:
    """Service for database operations with scalable queries"""
    
    def __init__(self):
        self.client = get_supabase_client()
        
    async def get_product_by_barcode(self, barcode: str) -> Optional[Dict[str, Any]]:
        """
        Get complete product information by barcode
        Uses PRIMARY KEY index for O(1) performance
        
        Args:
            barcode: Product barcode (8-18 digits)
            
        Returns:
            Dict with product data or None if not found
        """
        try:
            # Single query using PRIMARY KEY index - scales to 100k+ products
            response = self.client.table("products").select("*").eq("barcode", barcode).execute()
            
            if response.data and len(response.data) > 0:
                product = response.data[0]
                logger.info(f"Product found for barcode {barcode}: {product.get('product_name', 'Unknown')}")
                return product
            
            logger.info(f"No product found for barcode: {barcode}")
            return None
            
        except Exception as e:
            logger.error(f"Database error for barcode {barcode}: {e}")
            raise
    
    async def get_available_brands(self) -> List[str]:
        """
        Get list of available brands in database
        Uses brand_name index for fast retrieval
        
        Returns:
            List of unique brand names
        """
        try:
            # Query using indexed field - performance scales well
            response = (
                self.client
                .table("products")
                .select("brand_name")
                .execute()
            )
            
            if response.data:
                # Extract unique brands and sort
                brands = list(set(item["brand_name"] for item in response.data))
                brands.sort()
                logger.info(f"Retrieved {len(brands)} unique brands")
                return brands
            
            logger.warning("No brands found in database")
            return []
            
        except Exception as e:
            logger.error(f"Error retrieving brands: {e}")
            raise
    
    async def get_database_stats(self) -> Dict[str, Any]:
        """
        Get basic database statistics for monitoring
        Useful for bot health checks and scaling decisions
        
        Returns:
            Dict with database statistics
        """
        try:
            # Count total products
            count_response = (
                self.client
                .table("products")
                .select("*", count="exact")
                .execute()
            )
            
            total_products = count_response.count if count_response.count else 0
            
            # Get impact level distribution using indexed field
            impact_response = (
                self.client
                .table("products")
                .select("impact_level")
                .execute()
            )
            
            impact_distribution = {}
            if impact_response.data:
                for item in impact_response.data:
                    level = item.get("impact_level", "UNKNOWN")
                    impact_distribution[level] = impact_distribution.get(level, 0) + 1
            
            stats = {
                "total_products": total_products,
                "impact_level_distribution": impact_distribution,
                "status": "healthy" if total_products > 0 else "empty"
            }
            
            logger.info(f"Database stats: {stats}")
            return stats
            
        except Exception as e:
            logger.error(f"Error retrieving database stats: {e}")
            return {
                "total_products": 0,
                "impact_level_distribution": {},
                "status": "error",
                "error": str(e)
            }
    
    async def health_check(self) -> bool:
        """
        Quick health check for database connectivity
        Used for bot monitoring and error handling
        
        Returns:
            True if database is accessible, False otherwise
        """
        try:
            # Simple query to test connectivity
            response = (
                self.client
                .table("products")
                .select("barcode")
                .limit(1)
                .execute()
            )
            
            # If we get here without exception, database is accessible
            return True
            
        except Exception as e:
            logger.error(f"Database health check failed: {e}")
            return False