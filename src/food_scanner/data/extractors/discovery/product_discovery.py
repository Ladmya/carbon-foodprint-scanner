"""
src/food_scanner/data/extractors/discovery/product_discovery.py
PRODUCT DISCOVERY: PHASE 1 - Iterate over brands × categories to find products
Responsibility: Discover products by searching OpenFoodFacts API
"""

import asyncio
import sys
from datetime import datetime
from typing import Dict, List, Any, Set
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parents[5] / "src"))

from food_scanner.core.constants import (
    MAJOR_CHOCOLATE_BRANDS_FRANCE, 
    CHOCOLATE_CATEGORIES,
)
from food_scanner.infrastructure.external_apis.openfoodfacts import OpenFoodFactsClient


class ProductDiscovery:
    """
    RESPONSIBILITY: PHASE 1 - Discover products by iterating over brands × categories
    
    Uses OpenFoodFactsClient to search for products matching:
    - Each brand in MAJOR_CHOCOLATE_BRANDS_FRANCE
    - Each category in CHOCOLATE_CATEGORIES  
    - Returns unique barcodes with discovery metadata
    
    This class handles only the discovery phase, not enrichment or field extraction.
    """
    
    def __init__(self, client: OpenFoodFactsClient):
        self.client = client
        
        # Discovery statistics
        self.stats = {
            "start_time": None,
            "brands_processed": 0,
            "categories_processed": 0,
            "api_calls": 0,
            "products_discovered": 0,
            "duplicate_barcodes": 0,
            "extraction_errors": []
        }
    
    async def discover_products_by_brands(
        self, 
        brands: List[str] = None,
        categories: List[str] = None,
        max_products_per_brand: int = 20
        ) -> Dict[str, Any]:
        """
        PHASE 1: Discover products by iterating over brands × categories
        
        Args:
            brands: List of brands to search (defaults to MAJOR_CHOCOLATE_BRANDS_FRANCE)
            categories: List of categories (defaults to CHOCOLATE_CATEGORIES) 
            max_products_per_brand: Limit per brand to avoid hitting rate limits
            
        Returns:
            Dict containing discovered products and search metadata
        """
        self.stats["start_time"] = datetime.now().isoformat()
        
        # Use default constants if not provided
        if brands is None:
            brands = MAJOR_CHOCOLATE_BRANDS_FRANCE[:5]  # Limit for testing
        if categories is None:
            categories = CHOCOLATE_CATEGORIES[:3]  # Top categories only
        
        print(f"🔍 DISCOVERY PHASE: Products from brands × categories")
        print(f"   → Brands: {len(brands)} ({', '.join(brands[:3])}...)")
        print(f"   → Categories: {len(categories)} ({', '.join(categories[:2])}...)")
        print(f"   → Max per brand: {max_products_per_brand}")
        print(f"   → Environment: {'TEST' if hasattr(self.client, 'use_test_env') and self.client.use_test_env else 'PRODUCTION'}")
        print("=" * 60)
        
        discovered_barcodes: Set[str] = set()  # Avoid duplicates
        discovery_results = {
            "discovered_products": {},
            "brand_results": {},
            "search_metadata": []
        }
        
        # Iterate over brands × categories
        for brand_idx, brand in enumerate(brands, 1):
            print(f"\n📦 [{brand_idx}/{len(brands)}] Brand: {brand}")
            self.stats["brands_processed"] += 1
            
            brand_barcodes: Set[str] = set()
            
            for cat_idx, category in enumerate(categories, 1):
                print(f"   🏷️ [{cat_idx}/{len(categories)}] Category: {category}")
                self.stats["categories_processed"] += 1
                
                search_metadata = {
                    "brand": brand,
                    "category": category,
                    "timestamp": datetime.now().isoformat(),
                    "success": False,
                    "products_found": 0,
                    "new_products": 0,
                    "duplicates": 0,
                    "error": None
                }
                
                try:
                    # Use OpenFoodFactsClient for search
                    search_result = await self.client.search_products(
                        brand=brand,
                        categories=[category],
                        page=1,
                        page_size=min(50, max_products_per_brand)
                    )
                    
                    self.stats["api_calls"] += 1
                    
                    if search_result and search_result.get('products'):
                        products = search_result['products']
                        search_metadata["products_found"] = len(products)
                        search_metadata["success"] = True
                        
                        # Extract barcodes and store discovery info
                        new_count = 0
                        duplicate_count = 0
                        
                        for product in products:
                            barcode = product.get('code')
                            if barcode and isinstance(barcode, str):
                                if barcode not in discovered_barcodes:
                                    # Store discovery data (minimal info from search)
                                    discovery_results["discovered_products"][barcode] = {
                                        'barcode': barcode,
                                        'product_name': product.get('product_name', ''),
                                        'product_name_fr': product.get('product_name_fr', ''),
                                        'brands': product.get('brands', ''),
                                        'discovered_via': {
                                            'brand': brand,
                                            'category': category,
                                            'search_timestamp': datetime.now().isoformat()
                                        }
                                    }
                                    discovered_barcodes.add(barcode)
                                    brand_barcodes.add(barcode)
                                    new_count += 1
                                else:
                                    duplicate_count += 1
                        
                        search_metadata["new_products"] = new_count
                        search_metadata["duplicates"] = duplicate_count
                        self.stats["duplicate_barcodes"] += duplicate_count
                        
                        print(f"      ✅ Found: {len(products)}, New: {new_count}, Duplicates: {duplicate_count}")
                    
                    else:
                        print(f"      ❌ No products found")
                
                except Exception as e:
                    search_metadata["error"] = str(e)
                    self.stats["extraction_errors"].append({
                        "type": "search_error",
                        "brand": brand,
                        "category": category,
                        "error": str(e),
                        "timestamp": datetime.now().isoformat()
                    })
                    print(f"      💥 Error: {str(e)}")
                
                discovery_results["search_metadata"].append(search_metadata)
            
            # Store brand results
            discovery_results["brand_results"][brand] = {
                "products_found": len(brand_barcodes),
                "barcodes": list(brand_barcodes)
            }
            
            print(f"   📊 Total for {brand}: {len(brand_barcodes)} products")
        
        self.stats["products_discovered"] = len(discovered_barcodes)
        
        print(f"\n🎯 DISCOVERY COMPLETE")
        print(f"   → Unique products discovered: {self.stats['products_discovered']}")
        print(f"   → Duplicates avoided: {self.stats['duplicate_barcodes']}")
        print(f"   → API calls made: {self.stats['api_calls']}")
        
        return discovery_results
    
    def get_discovery_stats(self) -> Dict[str, Any]:
        """Get discovery phase statistics"""
        return self.stats.copy()
    
    def reset_stats(self):
        """Reset discovery statistics"""
        self.stats = {
            "start_time": None,
            "brands_processed": 0,
            "categories_processed": 0,
            "api_calls": 0,
            "products_discovered": 0,
            "duplicate_barcodes": 0,
            "extraction_errors": []
        }


# Test function for standalone testing
async def test_product_discovery():
    """Test ProductDiscovery class independently"""
    from food_scanner.infrastructure.external_apis.openfoodfacts import OpenFoodFactsClient
    
    async with OpenFoodFactsClient(use_test_env=True) as client:
        discovery = ProductDiscovery(client)
        
        # Test with limited scope
        results = await discovery.discover_products_by_brands(
            brands=["Lindt"],  # Single brand for testing
            categories=["chocolates"],  # Single category for testing
            max_products_per_brand=5
        )
        
        print(f"\n🧪 TEST RESULTS:")
        print(f"   → Products discovered: {len(results['discovered_products'])}")
        print(f"   → Stats: {discovery.get_discovery_stats()}")
        
        return results


if __name__ == "__main__":
    asyncio.run(test_product_discovery())
