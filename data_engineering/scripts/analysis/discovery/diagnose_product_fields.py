"""
data_engineering/scripts/analysis/diagnose_product_fields.py

Diagnostic script to understand why products are being rejected
"""

import asyncio
import json
import sys
from pathlib import Path
from typing import Dict, List, Any

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "src"))

from food_scanner.infrastructure.external_apis.openfoodfacts import OpenFoodFactsClient
from food_scanner.core.constants import CHOCOLATE_CATEGORIES, MAJOR_CHOCOLATE_BRANDS_FRANCE


async def diagnose_sample_products():
    """Analyze a small sample of products to understand field availability"""
    
    print("🔍 DIAGNOSTIC: Product Field Analysis")
    print("=" * 60)
    
    # Test with just 1 brand and 1 category for faster diagnosis
    test_brands = MAJOR_CHOCOLATE_BRANDS_FRANCE[:6]  # Just first brand
    test_categories = CHOCOLATE_CATEGORIES[:6]       # Just first category
    
    async with OpenFoodFactsClient(use_test_env=True) as client:
        print(f"Testing with brand: {test_brands[2]}")
        print(f"Testing with category: {test_categories[1]}")
        
        # Search for products
        search_result = await client.search_products(
            brand=test_brands[2],
            categories=test_categories,
            page=1,
            page_size=5  # Just 5 products for diagnosis
        )
        
        if not search_result or not search_result.get('products'):
            print("❌ No products found in search")
            return
        
        products = search_result['products']
        print(f"Found {len(products)} products for analysis")
        print("-" * 60)
        
        field_stats = {
            'product_name_fr': 0,
            'product_name': 0, 
            'brands': 0,
            'product_quantity': 0,
            'quantity': 0,
            'nutriscore_grade': 0,
            'nutriscore_score': 0,
            'nutriscore': 0,
            'agribalyse': 0,
            'ecoscore_data': 0,
            'nutriments': 0,
            'carbon-footprint_100g': 0
        }
        
        for i, product in enumerate(products, 1):
            barcode = product.get('code', 'unknown')
            print(f"\n📦 PRODUCT {i}: {barcode}")
            
            # Get detailed product data
            detailed_response = await client.get_product(barcode)
            if not detailed_response or not detailed_response.get('product'):
                print("   ❌ Failed to get detailed data")
                continue
                
            detailed_product = detailed_response['product']
            
            # Check each critical field
            print("   🔍 CRITICAL FIELDS ANALYSIS:")
            
            # Product names
            name_fr = detailed_product.get('product_name_fr', '').strip()
            name_en = detailed_product.get('product_name', '').strip()
            print(f"   • product_name_fr: {'✅' if name_fr else '❌'} ({repr(name_fr[:50])})")
            print(f"   • product_name: {'✅' if name_en else '❌'} ({repr(name_en[:50])})")
            if name_fr: field_stats['product_name_fr'] += 1
            if name_en: field_stats['product_name'] += 1
            
            # Brands
            brands = detailed_product.get('brands', '').strip()
            brands_tags = detailed_product.get('brands_tags', [])
            print(f"   • brands: {'✅' if brands else '❌'} ({repr(brands)})")
            print(f"   • brands_tags: {'✅' if brands_tags else '❌'} ({brands_tags})")
            if brands: field_stats['brands'] += 1
            
            # Weight/Quantity
            product_quantity = detailed_product.get('product_quantity', '')
            quantity = detailed_product.get('quantity', '')
            print(f"   • product_quantity: {'✅' if product_quantity else '❌'} ({repr(product_quantity)})")
            print(f"   • quantity: {'✅' if quantity else '❌'} ({repr(quantity)})")
            if product_quantity: field_stats['product_quantity'] += 1
            if quantity: field_stats['quantity'] += 1
            
            # Nutriscore
            nutriscore_grade = detailed_product.get('nutriscore_grade')
            nutriscore_score = detailed_product.get('nutriscore_score')
            nutriscore_data = detailed_product.get('nutriscore', {})
            print(f"   • nutriscore_grade: {'✅' if nutriscore_grade else '❌'} ({nutriscore_grade})")
            print(f"   • nutriscore_score: {'✅' if nutriscore_score else '❌'} ({nutriscore_score})")
            print(f"   • nutriscore object: {'✅' if nutriscore_data else '❌'} ({type(nutriscore_data)})")
            if nutriscore_grade: field_stats['nutriscore_grade'] += 1
            if nutriscore_score is not None: field_stats['nutriscore_score'] += 1
            if nutriscore_data: field_stats['nutriscore'] += 1
            
            # CO2 Data
            agribalyse = detailed_product.get('agribalyse')
            ecoscore_data = detailed_product.get('ecoscore_data')
            nutriments = detailed_product.get('nutriments', {})
            carbon_footprint = nutriments.get('carbon-footprint_100g') if isinstance(nutriments, dict) else None
            
            print(f"   • agribalyse: {'✅' if agribalyse else '❌'} ({type(agribalyse)})")
            print(f"   • ecoscore_data: {'✅' if ecoscore_data else '❌'} ({type(ecoscore_data)})")
            print(f"   • nutriments: {'✅' if nutriments else '❌'} ({type(nutriments)})")
            print(f"   • carbon-footprint_100g: {'✅' if carbon_footprint else '❌'} ({carbon_footprint})")
            
            if agribalyse: field_stats['agribalyse'] += 1
            if ecoscore_data: field_stats['ecoscore_data'] += 1
            if nutriments: field_stats['nutriments'] += 1
            if carbon_footprint is not None: field_stats['carbon-footprint_100g'] += 1
            
            # Weight parsing test
            quantity_str = product_quantity or quantity
            if quantity_str:
                print(f"   🔍 WEIGHT PARSING TEST:")
                print(f"   • Original: {repr(quantity_str)}")
                print(f"   • Type: {type(quantity_str)}")
                if isinstance(quantity_str, (int, float)):
                    print(f"   • Converted to string: {str(quantity_str)}")
                
        # Summary
        print(f"\n📊 FIELD AVAILABILITY SUMMARY (out of {len(products)} products):")
        print("-" * 60)
        for field, count in field_stats.items():
            percentage = (count / len(products)) * 100
            status = "✅" if percentage >= 80 else "⚠️" if percentage >= 50 else "❌"
            print(f"{status} {field:25} {count}/{len(products)} ({percentage:.1f}%)")
        
        # Critical field combinations
        print(f"\n🎯 CRITICAL COMBINATIONS:")
        has_name = field_stats['product_name_fr'] > 0 or field_stats['product_name'] > 0
        has_brand = field_stats['brands'] > 0
        has_weight = field_stats['product_quantity'] > 0 or field_stats['quantity'] > 0
        has_nutri = field_stats['nutriscore_grade'] > 0 or field_stats['nutriscore_score'] > 0
        has_co2 = field_stats['agribalyse'] > 0 or field_stats['carbon-footprint_100g'] > 0
        
        print(f"• Products with name: {has_name}")
        print(f"• Products with brand: {has_brand}")
        print(f"• Products with weight: {has_weight}")
        print(f"• Products with nutriscore: {has_nutri}")
        print(f"• Products with CO2 data: {has_co2}")
        
        if not (has_name and has_brand and has_weight and has_nutri and has_co2):
            print("\n⚠️  IDENTIFIED ISSUES:")
            if not has_name: print("   • Names are missing - check product_name fields")
            if not has_brand: print("   • Brands are missing - check brands field")
            if not has_weight: print("   • Weights are missing - check quantity fields")
            if not has_nutri: print("   • Nutriscore is missing - may need to relax this requirement")
            if not has_co2: print("   • CO2 data is missing - may need to relax this requirement")
            
            print(f"\n💡 RECOMMENDATION:")
            print(f"   Consider making some fields optional or improving fallback logic")
        else:
            print(f"\n✅ All critical fields are available - issue is likely in parsing logic")


if __name__ == "__main__":
    asyncio.run(diagnose_sample_products())