"""
src/food_scanner/data/extractors/enrichment/product_enrichment.py
PRODUCT ENRICHMENT: PHASE 2 - Retrieve full product data from OpenFoodFacts API
Responsibility: Enrich discovered products with complete API data
"""

import sys
from datetime import datetime
from typing import Dict, List, Any
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parents[5] / "src"))

from food_scanner.infrastructure.external_apis.openfoodfacts import OpenFoodFactsClient


class ProductEnrichment:
    """
    RESPONSIBILITY: PHASE 2 - Enrich products with full API data using OpenFoodFactsClient
    
    For each discovered barcode, retrieve complete product information
    using get_product() API call. This provides all the detailed fields
    needed for field extraction.
    
    This class handles only the enrichment phase, not discovery or field extraction.
    """
    
    def __init__(self, client: OpenFoodFactsClient):
        self.client = client
    
    async def enrich_discovered_products(
        self, 
        discovered_barcodes: List[str],
        max_products: int = None
        ) -> Dict[str, Any]:
        """
        PHASE 2: Enrich products with full API data using OpenFoodFactsClient
        
        Args:
            discovered_barcodes: List of barcodes from discovery phase
            max_products: Limit number of products for testing
            
        Returns:
            Dict containing enriched products with full API responses
        """
        print(f"\n🔬 ENRICHMENT PHASE: Full product data")
        
        if max_products and len(discovered_barcodes) > max_products:
            discovered_barcodes = discovered_barcodes[:max_products]
            print(f"   → Limited to {max_products} products for testing")
        
        print(f"   → Enriching {len(discovered_barcodes)} products")
        print("=" * 60)
        
        enriched_results = {
            "enriched_products": {},
            "enrichment_stats": {
                "total_requested": len(discovered_barcodes),
                "successful_enrichments": 0,
                "failed_enrichments": 0,
                "enrichment_errors": [],
                "api_calls": 0
            }
        }
        
        for idx, barcode in enumerate(discovered_barcodes, 1):
            print(f"📄 [{idx}/{len(discovered_barcodes)}] Enriching: {barcode}", end="")
            
            try:
                # Use OpenFoodFactsClient to get full product data
                detailed_response = await self.client.get_product(barcode)
                enriched_results["enrichment_stats"]["api_calls"] += 1
                
                if detailed_response and detailed_response.get('product'):
                    # Store raw API response for field extraction
                    enriched_results["enriched_products"][barcode] = {
                        "barcode": barcode,
                        "raw_api_response": detailed_response['product'],
                        "enrichment_timestamp": datetime.now().isoformat(),
                        "api_status": "success"
                    }
                    
                    enriched_results["enrichment_stats"]["successful_enrichments"] += 1
                    print(f" ✅")
                    
                else:
                    # Product not found in API
                    enriched_results["enriched_products"][barcode] = {
                        "barcode": barcode,
                        "raw_api_response": None,
                        "enrichment_timestamp": datetime.now().isoformat(),
                        "api_status": "not_found"
                    }
                    
                    enriched_results["enrichment_stats"]["failed_enrichments"] += 1
                    print(f" ❌ Not found")
                    
            except Exception as e:
                # API error
                error_info = {
                    "barcode": barcode,
                    "error": str(e),
                    "timestamp": datetime.now().isoformat()
                }
                
                enriched_results["enriched_products"][barcode] = {
                    "barcode": barcode,
                    "raw_api_response": None,
                    "enrichment_timestamp": datetime.now().isoformat(),
                    "api_status": "error",
                    "error": str(e)
                }
                
                enriched_results["enrichment_stats"]["enrichment_errors"].append(error_info)
                enriched_results["enrichment_stats"]["failed_enrichments"] += 1
                print(f" 💥 Error: {str(e)[:20]}")
        
        print(f"\n🎯 ENRICHMENT COMPLETE")
        print(f"   → Successful enrichments: {enriched_results['enrichment_stats']['successful_enrichments']}")
        print(f"   → Failed enrichments: {enriched_results['enrichment_stats']['failed_enrichments']}")
        print(f"   → Success rate: {(enriched_results['enrichment_stats']['successful_enrichments'] / len(discovered_barcodes) * 100):.1f}%")
        print(f"   → API calls made: {enriched_results['enrichment_stats']['api_calls']}")
        
        return enriched_results
    



# Test function for standalone testing
async def test_product_enrichment():
    """Test ProductEnrichment class independently"""
    from food_scanner.infrastructure.external_apis.openfoodfacts import OpenFoodFactsClient
    
    async with OpenFoodFactsClient(use_test_env=True) as client:
        enrichment = ProductEnrichment(client)
        
        # Test with sample barcodes (Lindt products from discovery test)
        test_barcodes = [
            "3017620422003",  # Lindt Excellence 70% Cocoa
            "3017620425004",  # Lindt Excellence 85% Cocoa
            "3017620420008"   # Lindt Excellence 90% Cocoa
        ]
        
        # Test enrichment
        results = await enrichment.enrich_discovered_products(
            discovered_barcodes=test_barcodes,
            max_products=3
        )
        
        print(f"\n🧪 TEST RESULTS:")
        print(f"   → Products enriched: {len(results['enriched_products'])}")
        print(f"   → Success rate: {(results['enrichment_stats']['successful_enrichments'] / len(test_barcodes) * 100):.1f}%")
        print(f"   → Stats: {results['enrichment_stats']}")
        
        # Show sample enriched product
        if results['enriched_products']:
            first_barcode = list(results['enriched_products'].keys())[0]
            first_product = results['enriched_products'][first_barcode]
            print(f"   → Sample product status: {first_product['api_status']}")
            if first_product['raw_api_response']:
                print(f"   → Sample product name: {first_product['raw_api_response'].get('product_name', 'N/A')}")
        
        return results


if __name__ == "__main__":
    import asyncio
    asyncio.run(test_product_enrichment())
