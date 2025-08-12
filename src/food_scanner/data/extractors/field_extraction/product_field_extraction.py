"""
src/food_scanner/data/extractors/field_extraction/product_field_extraction.py
PRODUCT FIELD EXTRACTION: PHASE 3 - Extract and parse individual fields from enriched API data
Responsibility: Transform raw API responses into structured field data for database storage
"""
import sys
from typing import Dict, List, Any, Optional, Tuple
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[5] / "src"))

from .weight_parser import WeightParser


class ProductFieldExtraction:
    """
    RESPONSIBILITY: PHASE 3 - Extract and parse individual fields from enriched API data
    
    This class handles the transformation of raw OpenFoodFacts API responses into
    structured field data that can be stored in the products database table.
    """
    
    def __init__(self):
        """Initialize field extraction with required dependencies"""
        self.weight_parser = WeightParser()
    
    def extract_barcode(self, raw_product: Dict[str, Any]) -> Optional[str]:
        """Extract product barcode (essential for products table primary key)"""
        barcode = raw_product.get('code')
        if barcode:
            return str(barcode)
        return None
    
    def extract_product_name(self, raw_product: Dict[str, Any]) -> Optional[str]:
        """Extract product name with French priority fallback to English"""
        # Priority 1: French product name
        name_fr = raw_product.get('product_name_fr', '').strip() if raw_product.get('product_name_fr') else ''
        if name_fr:
            return name_fr
        
        # Priority 2: Default product name (usually English)
        name_default = raw_product.get('product_name', '').strip() if raw_product.get('product_name') else ''
        if name_default:
            return name_default
        
        return None
    
    def extract_brand_name(self, raw_product: Dict[str, Any]) -> Optional[str]:
        """Extract brand name with multiple fallback sources"""
        # Priority 1: Direct brands field
        brands = raw_product.get('brands', '').strip() if raw_product.get('brands') else ''
        if brands:
            return brands
        
        # Priority 2: First brand from brands_tags
        brands_tags = raw_product.get('brands_tags', [])
        if brands_tags and len(brands_tags) > 0:
            first_tag = brands_tags[0].replace('-', ' ').title()
            return first_tag
        
        # Priority 3: Imported brands field
        brands_imported = raw_product.get('brands_imported', '').strip() if raw_product.get('brands_imported') else ''
        if brands_imported:
            return brands_imported
        
        return None
    
    def extract_brand_tags(self, raw_product: Dict[str, Any]) -> List[str]:
        """Extract brand tags list"""
        brands_tags = raw_product.get('brands_tags', [])
        if isinstance(brands_tags, list):
            return brands_tags
        return []
    
    def extract_weight_and_unit(self, raw_product: Dict[str, Any]) -> Tuple[Optional[float], Optional[str]]:
        """
        Extract weight and unit - prioritize already normalized fields from OpenFoodFacts
        
        EXTRACTION LOGIC:
        1. Try product_quantity + product_quantity_unit (pre-normalized by OpenFoodFacts)
        2. Fallback to WeightParser for complex parsing
        3. Return (weight, unit) tuple
        """
        # Priority 1: Pre-normalized fields from OpenFoodFacts API
        product_quantity = raw_product.get('product_quantity')
        product_quantity_unit = raw_product.get('product_quantity_unit')
        
        if product_quantity is not None and product_quantity_unit is not None:
            try:
                weight = float(product_quantity)
                unit = str(product_quantity_unit).lower()
                return weight, unit
            except (ValueError, TypeError):
                pass
        
        # Priority 2: Use WeightParser for complex parsing
        if product_quantity is not None:
            return self.weight_parser.parse_weight_and_unit(product_quantity)
        
        # Priority 3: Fallback to general quantity field
        quantity = raw_product.get('quantity')
        if quantity is not None:
            return self.weight_parser.parse_weight_and_unit(quantity)
        
        return None, None
    
    def extract_nutriscore_grade(self, raw_product: Dict[str, Any]) -> Optional[str]:
        """Extract nutriscore grade (A-E)"""
        # Priority 1: Nested nutriscore structure
        nutriscore_data = raw_product.get('nutriscore', {})
        if isinstance(nutriscore_data, dict):
            grade = nutriscore_data.get('grade')
            if grade and isinstance(grade, str):
                return grade.upper()
        
        # Priority 2: Direct nutriscore_grade field
        grade = raw_product.get('nutriscore_grade')
        if grade and isinstance(grade, str):
            return grade.upper()
        
        # Priority 3: Alternative nutrition_grades field
        grade = raw_product.get('nutrition_grades')
        if grade and isinstance(grade, str):
            return grade.upper()
        
        return None
    
    def extract_nutriscore_score(self, raw_product: Dict[str, Any]) -> Optional[int]:
        """Extract nutriscore score (-15 to 40)"""
        # Priority 1: Nested nutriscore structure
        nutriscore_data = raw_product.get('nutriscore', {})
        if isinstance(nutriscore_data, dict):
            score = nutriscore_data.get('score')
            if score is not None:
                try:
                    return int(score)
                except (ValueError, TypeError):
                    pass
        
        # Priority 2: Direct nutriscore_score field
        score = raw_product.get('nutriscore_score')
        if score is not None:
            try:
                return int(score)
            except (ValueError, TypeError):
                pass
        
        return None
    
    def extract_ecoscore_grade(self, raw_product: Dict[str, Any]) -> Optional[str]:
        """Extract ecoscore grade (A-E)"""
        eco_grade = raw_product.get('ecoscore_grade')
        if eco_grade and isinstance(eco_grade, str):
            return eco_grade.upper()
        return None
    
    def extract_all_co2_sources(self, raw_product: Dict[str, Any]) -> Dict[str, Optional[float]]:
        """Extract all possible CO2 sources in priority order"""
        # Initialize all sources to None
        co2_sources = {
            'agribalyse_total': None,
            'ecoscore_agribalyse_total': None,
            'nutriments_carbon_footprint': None,
            'nutriments_known_ingredients': None
        }
        
        # Priority 1: agribalyse.co2_total
        agribalyse = raw_product.get('agribalyse')
        if isinstance(agribalyse, dict):
            co2_total = agribalyse.get('co2_total')
            if co2_total is not None:
                try:
                    co2_sources['agribalyse_total'] = float(co2_total)
                except (ValueError, TypeError):
                    pass  # Keep None if conversion fails
        
        # Priority 2: ecoscore_data.agribalyse.co2_total
        ecoscore_data = raw_product.get('ecoscore_data')
        if isinstance(ecoscore_data, dict):
            eco_agri = ecoscore_data.get('agribalyse')
            if isinstance(eco_agri, dict):
                co2_total = eco_agri.get('co2_total')
                if co2_total is not None:
                    try:
                        co2_sources['ecoscore_agribalyse_total'] = float(co2_total)
                    except (ValueError, TypeError):
                        pass  # Keep None if conversion fails
        
        # Priority 3: nutriments.carbon-footprint_100g
        nutriments = raw_product.get('nutriments', {})
        if isinstance(nutriments, dict):
            co2_val = nutriments.get('carbon-footprint_100g')
            if co2_val is not None:
                try:
                    co2_sources['nutriments_carbon_footprint'] = float(co2_val)
                except (ValueError, TypeError):
                    pass  # Keep None if conversion fails
        
        # Priority 4: nutriments.carbon-footprint-from-known-ingredients_100g
        if isinstance(nutriments, dict):
            co2_val = nutriments.get('carbon-footprint-from-known-ingredients_100g')
            if co2_val is not None:
                try:
                    co2_sources['nutriments_known_ingredients'] = float(co2_val)
                except (ValueError, TypeError):
                    pass  # Keep None if conversion fails
        
        return co2_sources
    
    def extract_all_fields(self, raw_product: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract ALL fields needed for products table from raw API response
        Returns extracted fields + extraction success statistics
        """
        weight, unit = self.extract_weight_and_unit(raw_product)
        co2_sources = self.extract_all_co2_sources(raw_product)
        
        # Extract all fields
        extracted_fields = {
            # Products table required fields
            'barcode': self.extract_barcode(raw_product),
            'product_name': self.extract_product_name(raw_product),
            'brand_name': self.extract_brand_name(raw_product),
            'brand_tags': self.extract_brand_tags(raw_product),
            'weight': weight,
            'product_quantity_unit': unit,
            'nutriscore_grade': self.extract_nutriscore_grade(raw_product),
            'nutriscore_score': self.extract_nutriscore_score(raw_product),
            'eco_score': self.extract_ecoscore_grade(raw_product),
            'co2_sources': co2_sources
        }
        
        # Add extraction success flags for statistics
        extracted_fields['extraction_success'] = {
            'barcode': extracted_fields['barcode'] is not None,
            'product_name': extracted_fields['product_name'] is not None,
            'brand_name': extracted_fields['brand_name'] is not None,
            'weight': extracted_fields['weight'] is not None,
            'product_quantity_unit': extracted_fields['product_quantity_unit'] is not None,
            'nutriscore_grade': extracted_fields['nutriscore_grade'] is not None,
            'nutriscore_score': extracted_fields['nutriscore_score'] is not None,
            'co2_total': any(v is not None for v in co2_sources.values())
        }
        
        return extracted_fields


# ============================================================================
# STANDALONE TESTING
# ============================================================================

async def test_product_field_extraction():
    """Test the ProductFieldExtraction class with sample data"""
    print("🧪 TESTING PRODUCT FIELD EXTRACTION")
    print("=" * 60)
    
    # Initialize extractor
    field_extractor = ProductFieldExtraction()
    
    # Sample raw product data (simulating OpenFoodFacts API response)
    sample_raw_product = {
        'code': '3017620422003',
        'product_name_fr': 'Nutella',
        'product_name': 'Nutella',
        'brands': 'Ferrero',
        'brands_tags': ['ferrero', 'nutella'],
        'product_quantity': 400,
        'product_quantity_unit': 'g',
        'nutriscore': {'grade': 'e', 'score': 26},
        'ecoscore_grade': 'd',
        'agribalyse': {'co2_total': 0.8},
        'nutriments': {
            'carbon-footprint_100g': 0.8,
            'carbon-footprint-from-known-ingredients_100g': 0.75
        }
    }
    
    print(f"📄 Sample product: {sample_raw_product['product_name_fr']}")
    print(f"🏷️  Brand: {sample_raw_product['brands']}")
    print(f"⚖️  Weight: {sample_raw_product['product_quantity']} {sample_raw_product['product_quantity_unit']}")
    print("=" * 60)
    
    # Extract all fields
    extracted_data = field_extractor.extract_all_fields(sample_raw_product)
    
    print("🔧 EXTRACTION RESULTS:")
    print(f"   → Barcode: {extracted_data['barcode']}")
    print(f"   → Product Name: {extracted_data['product_name']}")
    print(f"   → Brand Name: {extracted_data['brand_name']}")
    print(f"   → Brand Tags: {extracted_data['brand_tags']}")
    print(f"   → Weight: {extracted_data['weight']} {extracted_data['product_quantity_unit']}")
    print(f"   → NutriScore Grade: {extracted_data['nutriscore_grade']}")
    print(f"   → NutriScore Score: {extracted_data['nutriscore_score']}")
    print(f"   → EcoScore Grade: {extracted_data['eco_score']}")
    print(f"   → CO2 Sources: {extracted_data['co2_sources']}")
    
    print("\n📊 EXTRACTION SUCCESS RATES:")
    success_rates = extracted_data['extraction_success']
    for field, success in success_rates.items():
        status = "✅" if success else "❌"
        print(f"   → {field}: {status}")
    
    print("\n🎯 TEST COMPLETE")
    return extracted_data


if __name__ == "__main__":
    import asyncio
    asyncio.run(test_product_field_extraction())
