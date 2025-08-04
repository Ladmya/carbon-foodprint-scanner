"""
src/food_scanner/data/extractors/product_extractor.py
COMPLETE PRODUCT EXTRACTION: API calls + field extraction from OpenFoodFacts
Combines discovery, enrichment, and field parsing in one class
"""

import asyncio
import sys
import traceback as tr
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parents[4] / "src"))

from food_scanner.core.constants import (
    MAJOR_CHOCOLATE_BRANDS_FRANCE, 
    CHOCOLATE_CATEGORIES,
)
from food_scanner.infrastructure.external_apis.openfoodfacts import OpenFoodFactsClient
from food_scanner.data.transformers.field_transformers.weight_parser import WeightParser
from food_scanner.data.utils.extraction_reporter import ExtractionReporter
from food_scanner.data.analysis.comprehensive_analyzer import analyze_extraction_comprehensive
from food_scanner.data.analysis.co2_analyzer import analyze_co2_from_extraction_results
from food_scanner.data.analysis.barcode_analyzer import analyze_barcode_from_extraction_results
from food_scanner.data.analysis.text_field_analyzer import (
    analyze_product_name_from_extraction_results,
    analyze_brand_name_from_extraction_results
                    )


class ProductExtractor:
    """
    COMPLETE PRODUCT EXTRACTION from OpenFoodFacts API
    
    RESPONSIBILITIES:
    1. Discovery: Iterate over brands × categories to find products  
    2. Enrichment: Retrieve full product data for each discovered product
    3. Field Extraction: Parse individual fields from API responses
    4. Rate Limiting: Respect OpenFoodFacts API limits
    
    This class handles the complete extraction pipeline from API to structured data.
    The transformer will then handle validation and business logic.
    """
    
    def __init__(self, use_test_env: bool = True):
        self.use_test_env = use_test_env
        self.client = None
        self.weight_parser = WeightParser()
        
        # Extraction statistics
        self.stats = {
            "start_time": None,
            "end_time": None,
            "brands_processed": 0,
            "categories_processed": 0,
            "api_calls": 0,
            "products_discovered": 0,
            "products_enriched": 0,
            "failed_extractions": 0,
            "duplicate_barcodes": 0,
            "extraction_errors": []
        }
    
    async def __aenter__(self):
        """Async context manager entry"""
        self.client = OpenFoodFactsClient(use_test_env=self.use_test_env)
        await self.client.__aenter__()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self.client:
            await self.client.__aexit__(exc_type, exc_val, exc_tb)

    # ============================================================================
    # PHASE 1: DISCOVERY - Iterate over brands × categories
    # ============================================================================
    
    async def discover_products_by_brands(
        self, 
        brands: List[str] = None,
        categories: List[str] = None,
        max_products_per_brand: int = 100
    ) -> Dict[str, Any]:
        """
        PHASE 1: Discover products by iterating over brands × categories
        
        Uses OpenFoodFactsClient to search for products matching:
        - Each brand in MAJOR_CHOCOLATE_BRANDS_FRANCE
        - Each category in CHOCOLATE_CATEGORIES  
        - Returns unique barcodes with discovery metadata
        
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
            brands = MAJOR_CHOCOLATE_BRANDS_FRANCE[:10]  # Limit for testing
        if categories is None:
            categories = CHOCOLATE_CATEGORIES[:3]  # Top categories only
        
        print(f"🔍 DISCOVERY PHASE: Products from brands × categories")
        print(f"   → Brands: {len(brands)} ({', '.join(brands[:3])}...)")
        print(f"   → Categories: {len(categories)} ({', '.join(categories[:2])}...)")
        print(f"   → Max per brand: {max_products_per_brand}")
        print(f"   → Environment: {'TEST' if self.use_test_env else 'PRODUCTION'}")
        print("=" * 60)
        
        discovered_barcodes = set()  # Avoid duplicates
        discovery_results = {
            "discovered_products": {},
            "brand_results": {},
            "search_metadata": []
        }
        
        # Iterate over brands × categories
        for brand_idx, brand in enumerate(brands, 1):
            print(f"\n📦 [{brand_idx}/{len(brands)}] Brand: {brand}")
            self.stats["brands_processed"] += 1
            
            brand_barcodes = set()
            
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

    # ============================================================================
    # PHASE 2: ENRICHMENT - Get full product data for each barcode
    # ============================================================================
    
    async def enrich_discovered_products(
        self, 
        discovered_barcodes: List[str],
        max_products: int = None
    ) -> Dict[str, Any]:
        """
        PHASE 2: Enrich products with full API data using OpenFoodFactsClient
        
        For each discovered barcode, retrieve complete product information
        using get_product() API call. This provides all the detailed fields
        needed for field extraction.
        
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
                "enrichment_errors": []
            }
        }
        
        for idx, barcode in enumerate(discovered_barcodes, 1):
            print(f"📄 [{idx}/{len(discovered_barcodes)}] Enriching: {barcode}", end="")
            
            try:
                # Use OpenFoodFactsClient to get full product data
                detailed_response = await self.client.get_product(barcode)
                self.stats["api_calls"] += 1
                
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
                self.stats["extraction_errors"].append(error_info)
                print(f" 💥 Error: {str(e)[:20]}")
        
        self.stats["products_enriched"] = enriched_results["enrichment_stats"]["successful_enrichments"]
        self.stats["failed_extractions"] = enriched_results["enrichment_stats"]["failed_enrichments"]
        
        print(f"\n🎯 ENRICHMENT COMPLETE")
        print(f"   → Successful enrichments: {self.stats['products_enriched']}")
        print(f"   → Failed enrichments: {self.stats['failed_extractions']}")
        print(f"   → Success rate: {(self.stats['products_enriched'] / len(discovered_barcodes) * 100):.1f}%")
        
        return enriched_results

    # ============================================================================
    # PHASE 3: FIELD EXTRACTION - Parse individual fields from API responses
    # ============================================================================
    
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
        co2_sources = {}
        
        # Priority 1: agribalyse.co2_total
        agribalyse = raw_product.get('agribalyse')
        if isinstance(agribalyse, dict):
            co2_total = agribalyse.get('co2_total')
            if co2_total is not None:
                try:
                    co2_sources['agribalyse_total'] = float(co2_total)
                except (ValueError, TypeError):
                    co2_sources['agribalyse_total'] = None
            else:
                co2_sources['agribalyse_total'] = None
        else:
            co2_sources['agribalyse_total'] = None
        
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
                        co2_sources['ecoscore_agribalyse_total'] = None
                else:
                    co2_sources['ecoscore_agribalyse_total'] = None
            else:
                co2_sources['ecoscore_agribalyse_total'] = None
        else:
            co2_sources['ecoscore_agribalyse_total'] = None
        
        # Priority 3: nutriments.carbon-footprint_100g
        nutriments = raw_product.get('nutriments', {})
        if isinstance(nutriments, dict):
            co2_val = nutriments.get('carbon-footprint_100g')
            if co2_val is not None:
                try:
                    co2_sources['nutriments_carbon_footprint'] = float(co2_val)
                except (ValueError, TypeError):
                    co2_sources['nutriments_carbon_footprint'] = None
            else:
                co2_sources['nutriments_carbon_footprint'] = None
        else:
            co2_sources['nutriments_carbon_footprint'] = None
        
        # Priority 4: nutriments.carbon-footprint-from-known-ingredients_100g
        if isinstance(nutriments, dict):
            co2_val = nutriments.get('carbon-footprint-from-known-ingredients_100g')
            if co2_val is not None:
                try:
                    co2_sources['nutriments_known_ingredients'] = float(co2_val)
                except (ValueError, TypeError):
                    co2_sources['nutriments_known_ingredients'] = None
            else:
                co2_sources['nutriments_known_ingredients'] = None
        else:
            co2_sources['nutriments_known_ingredients'] = None
        
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
    # COMPLETE EXTRACTION PIPELINE
    # ============================================================================
    
    async def run_complete_extraction(
        self,
        brands: List[str] = None,
        categories: List[str] = None,
        max_products: int = 500
    ) -> Dict[str, Any]:
        """
        Run complete extraction pipeline: Discovery → Enrichment → Field Extraction
        
        This is the main method that combines all phases to provide a complete
        extraction from OpenFoodFacts API to structured field data.
        
        Args:
            brands: Brands to search (defaults to MAJOR_CHOCOLATE_BRANDS_FRANCE)
            categories: Categories to search (defaults to CHOCOLATE_CATEGORIES)
            max_products: Maximum products to process
            
        Returns:
            Complete extraction results with discovered, enriched, and parsed data
        """
        print("🚀 COMPLETE PRODUCT EXTRACTION PIPELINE")
        print("=" * 60)
        print(f"🎯 Target: Extract chocolate products from OpenFoodFacts")
        print(f"📊 Maximum: {max_products} products")
        print(f"🔧 Environment: {'TEST' if self.use_test_env else 'PRODUCTION'}")
        print("=" * 60)
        
        self.stats["end_time"] = datetime.now().isoformat()
        
        try:
            # Phase 1: Discovery
            discovery_result = await self.discover_products_by_brands(
                brands=brands,
                categories=categories,
                max_products_per_brand=50
            )
            
            if discovery_result["discovered_products"] == {}:
                print("\n❌ No products discovered")
                return {"success": False, "error": "No products discovered"}
            
            discovered_barcodes = list(discovery_result["discovered_products"].keys())
            
            # Phase 2: Enrichment
            enrichment_result = await self.enrich_discovered_products(
                discovered_barcodes=discovered_barcodes,
                max_products=max_products
            )
            
            if enrichment_result["enrichment_stats"]["successful_enrichments"] == 0:
                print("\n❌ No products successfully enriched")
                return {"success": False, "error": "No products enriched"}
            
            # Phase 3: Field Extraction for all enriched products
            print(f"\n🔧 FIELD EXTRACTION PHASE")
            print("=" * 60)
            
            extracted_products = {}
            extraction_stats = {
                "total_processed": 0,
                "successful_extractions": 0,
                "failed_extractions": 0,
                "field_success_counts": {
                    "barcode": 0,
                    "product_name": 0,
                    "brand_name": 0,
                    "weight": 0,
                    "product_quantity_unit": 0,
                    "nutriscore_grade": 0,
                    "nutriscore_score": 0,
                    "co2_total": 0
                }
            }
            
            for barcode, enriched_data in enrichment_result["enriched_products"].items():
                if enriched_data.get("api_status") == "success":
                    raw_product = enriched_data["raw_api_response"]
                    
                    try:
                        # Extract all fields using field extraction methods
                        extracted_fields = self.extract_all_fields(raw_product)
                        
                        extracted_products[barcode] = {
                            "extracted_fields": extracted_fields,
                            "raw_api_data": enriched_data
                        }
                        
                        extraction_stats["successful_extractions"] += 1
                        
                        # Count successful field extractions
                        success_flags = extracted_fields.get('extraction_success', {})
                        for field, success in success_flags.items():
                            if success:
                                extraction_stats["field_success_counts"][field] += 1
                        
                    except Exception as e:
                        print(f"💥 Field extraction error for {barcode}: {e}")
                        extraction_stats["failed_extractions"] += 1
                
                extraction_stats["total_processed"] += 1
            
            print(f"🎯 FIELD EXTRACTION COMPLETE")
            print(f"   → Products processed: {extraction_stats['total_processed']}")
            print(f"   → Successful extractions: {extraction_stats['successful_extractions']}")
            print(f"   → Failed extractions: {extraction_stats['failed_extractions']}")
            
            # Show detailed field extraction statistics
            field_counts = extraction_stats["field_success_counts"]
            total_products = extraction_stats["successful_extractions"]
            print(f"\n📊 FIELD EXTRACTION STATISTICS:")
            for field, count in field_counts.items():
                percentage = (count / max(1, total_products)) * 100
                print(f"   → {field}: {count}/{total_products} ({percentage:.1f}%)")
            
            # Combine all results
            complete_results = {
                "success": True,
                "discovery_results": discovery_result,
                "enrichment_results": enrichment_result,
                "extracted_products": extracted_products,
                "extraction_stats": extraction_stats,
                "pipeline_stats": self.stats.copy()
            }
            
            print(f"\n🎉 COMPLETE EXTRACTION SUCCESSFUL!")
            print(f"   → Products discovered: {self.stats['products_discovered']}")
            print(f"   → Products enriched: {self.stats['products_enriched']}")
            print(f"   → Fields extracted: {extraction_stats['successful_extractions']}")
            print(f"   → API calls made: {self.stats['api_calls']}")
            print(f"   → CO2 data found: {field_counts['co2_total']}/{total_products} products")
            print(f"   → Weight data found: {field_counts['weight']}/{total_products} products")
            
            # Phase 4: Analysis and Reporting
            if extraction_stats["successful_extractions"] > 0:
                print(f"\n📊 PHASE 4: COMPREHENSIVE ANALYSIS & REPORTING")
                print("=" * 60)
                
                try:                    
                    # Run specialized field analysis
                    print(f"   🔍 Running specialized field analysis...")
                    
                    # Barcode analysis (critical for primary key)
                    print(f"      📋 Analyzing barcode extraction...")
                    barcode_analysis = analyze_barcode_from_extraction_results(extracted_products)
                    
                    # Product name analysis (critical for display)
                    print(f"      📝 Analyzing product name extraction...")
                    product_name_analysis = analyze_product_name_from_extraction_results(extracted_products)
                    
                    # Brand name analysis (critical for display)
                    print(f"      🏷️ Analyzing brand name extraction...")
                    brand_name_analysis = analyze_brand_name_from_extraction_results(extracted_products)
                    
                    # CO2 analysis (critical for functionality)
                    print(f"      🌍 Analyzing CO2 extraction...")
                    co2_analysis = analyze_co2_from_extraction_results(extracted_products)
                    
                    # Comprehensive analysis
                    print(f"   📊 Running comprehensive extraction analysis...")
                    comprehensive_analysis = analyze_extraction_comprehensive(
                        extracted_products=extracted_products,
                        extraction_stats=extraction_stats,
                        pipeline_stats=self.stats.copy()
                    )
                    
                    # Generate extraction reports
                    print(f"   📋 Generating extraction reports...")
                    reporter = ExtractionReporter()
                    extraction_reports = reporter.generate_all_reports(
                        extracted_products=extracted_products,
                        extraction_stats=extraction_stats,
                        pipeline_stats=self.stats.copy()
                    )
                    
                    # Collect all analysis results
                    complete_results["field_analysis"] = {
                        "barcode_analysis": barcode_analysis,
                        "product_name_analysis": product_name_analysis,
                        "brand_name_analysis": brand_name_analysis,
                        "co2_analysis": co2_analysis
                    }
                    complete_results["comprehensive_analysis"] = comprehensive_analysis
                    complete_results["extraction_reports"] = extraction_reports
                    
                    # Calculate and display summary
                    self._display_analysis_summary(
                        extraction_stats, comprehensive_analysis, 
                        barcode_analysis, product_name_analysis, brand_name_analysis, co2_analysis,
                        extraction_reports
                    )
                
                except Exception as e:
                    print(f"   ⚠️ Analysis/reporting error: {e}")         
                    import traceback
                    traceback.print_exc()
                    # Continue without failing the extraction
                    complete_results["analysis_error"] = str(e)
            
            return complete_results
            
        except Exception as e:
            print(f"\n💥 Extraction pipeline error: {e}")
            import traceback
            traceback.print_exc()
            return {"success": False, "error": str(e)}
    
    def get_extraction_stats(self) -> Dict[str, Any]:
        """Get extraction statistics"""
        return {
            "extraction_stats": self.stats.copy(),
            "client_info": self.client.get_client_info() if self.client else {}
        }
    
    def _display_analysis_summary(
        self,
        extraction_stats: Dict[str, Any],
        comprehensive_analysis: Any,
        barcode_analysis: Any,
        product_name_analysis: Any, 
        brand_name_analysis: Any,
        co2_analysis: Any,
        extraction_reports: Dict[str, Any]
    ):
        """Display comprehensive analysis summary"""
        
        print(f"\n🎯 COMPREHENSIVE ANALYSIS SUMMARY")
        print("=" * 60)
        
        # Overall metrics
        total_products = extraction_stats.get("successful_extractions", 0)
        production_ready = comprehensive_analysis.rejection_analysis["production_ready_products"]
        overall_quality = comprehensive_analysis.overall_quality_score
        
        print(f"📊 OVERALL PERFORMANCE:")
        print(f"   → Total products analyzed: {total_products}")
        print(f"   → Overall extraction quality: {overall_quality:.1f}%")
        print(f"   → Production-ready products: {production_ready}")
        print(f"   → Success rate: {(production_ready / total_products * 100):.1f}%" if total_products > 0 else "   → Success rate: 0%")
        
        # Field-by-field analysis
        print(f"\n🔍 FIELD EXTRACTION ANALYSIS:")
        
        # Critical fields status
        critical_fields = [
            ("Barcode", barcode_analysis, "🔑"),
            ("Product Name", product_name_analysis, "📝"),
            ("Brand Name", brand_name_analysis, "🏷️"),
            ("CO2 Data", co2_analysis, "🌍")
        ]
        
        for field_name, analysis, icon in critical_fields:
            success_rate = analysis.validity_rate
            status = "✅" if success_rate >= 90 else "⚠️" if success_rate >= 70 else "❌"
            
            print(f"   {icon} {field_name}: {status} {analysis.valid_count}/{analysis.total_products} "
                f"({success_rate:.1f}%)")
            
            # Show primary recommendation if any
            if analysis.transformation_recommendations:
                primary_rec = analysis.transformation_recommendations[0]
                if len(primary_rec) > 80:
                    primary_rec = primary_rec[:77] + "..."
                print(f"      → {primary_rec}")
        
        # Production readiness assessment
        print(f"\n🚀 PRODUCTION READINESS:")
        
        bot_readiness = comprehensive_analysis.rejection_analysis.get("bot_launch_readiness", {})
        
        if bot_readiness.get("ready_for_launch", False):
            print(f"   ✅ READY FOR PRODUCTION DEPLOYMENT")
            print(f"      → {production_ready} products available for bot")
            print(f"      → All critical metrics above thresholds")
        elif bot_readiness.get("minimum_viable", False):
            print(f"   ⚠️ MINIMUM VIABLE PRODUCT READY")
            print(f"      → {production_ready} products available (limited dataset)")
            print(f"      → Some improvements recommended")
        else:
            print(f"   ❌ NOT READY FOR PRODUCTION")
            print(f"      → Only {production_ready} products ready")
            print(f"      → Major improvements needed")
        
        # Critical issues
        if comprehensive_analysis.critical_issues:
            print(f"\n🚨 CRITICAL ISSUES ({len(comprehensive_analysis.critical_issues)}):")
            for i, issue in enumerate(comprehensive_analysis.critical_issues[:3], 1):
                if len(issue) > 80:
                    issue = issue[:77] + "..."
                print(f"   {i}. {issue}")
            
            if len(comprehensive_analysis.critical_issues) > 3:
                print(f"   ... and {len(comprehensive_analysis.critical_issues) - 3} more issues")
        
        # Improvement priorities
        if comprehensive_analysis.improvement_priorities:
            print(f"\n📈 TOP IMPROVEMENT PRIORITIES:")
            for i, priority in enumerate(comprehensive_analysis.improvement_priorities[:3], 1):
                if len(priority) > 80:
                    priority = priority[:77] + "..."
                print(f"   {i}. {priority}")
        
        # Reports summary
        print(f"\n📁 EXTRACTION REPORTS GENERATED ({len(extraction_reports)}):")
        for report_type, file_path in extraction_reports.items():
            print(f"   → {report_type}: {file_path.name}")
        
        # Next steps
        print(f"\n🎯 RECOMMENDED NEXT STEPS:")
        
        if production_ready >= 100:
            print(f"   1. ✅ Deploy to production - sufficient data quality")
            print(f"   2. 📈 Scale data collection to 1000+ products")
            print(f"   3. 🔄 Set up automated quality monitoring")
        elif production_ready >= 50:
            print(f"   1. 🔧 Address critical issues shown above")
            print(f"   2. ⚡ Focus on {comprehensive_analysis.improvement_priorities[0].split(':')[1].strip() if comprehensive_analysis.improvement_priorities else 'top priority fields'}")
            print(f"   3. 🧪 Test with limited dataset before full deployment")
        else:
            print(f"   1. 🚨 Fix critical extraction issues immediately")
            print(f"   2. 🔍 Review extraction logic for failing fields")
            print(f"   3. 📊 Re-run analysis after improvements")
        
        print(f"\n💡 ANALYSIS COMPLETE - Check reports for detailed insights")
        print("=" * 60)


# ============================================================================
# TESTING AND VALIDATION
# ============================================================================

async def test_complete_extraction():
    """Test the complete extraction pipeline with realistic data"""
    
    print("🧪 TESTING COMPLETE PRODUCT EXTRACTOR")
    print("=" * 60)
    print("Testing: Discovery → Enrichment → Field Extraction")
    print("=" * 60)
    
    # Test with limited scope for fast testing
    test_brands = ["Lindt", "Kinder", "cote-d-or", "ferrero-rocher"]  # Just 4 brands
    test_categories = ["en:chocolates", "en:chocolate-bar", "en:chocolate-bar-with-nuts"]  # Just 3 categories
    max_test_products = 300  # Very limited for quick test
    
    async with ProductExtractor(use_test_env=True) as extractor:
        try:
            # Run complete pipeline
            results = await extractor.run_complete_extraction(
                brands=test_brands,
                categories=test_categories,
                max_products=max_test_products
            )
            
            if results.get("success"):
                print(f"\n✅ EXTRACTION TEST SUCCESSFUL!")
                
                # Show extracted fields for 5 products per brand
                extracted_products = results["extracted_products"]
                if extracted_products:
                    print(f"\n📦 EXTRACTED PRODUCTS (5 per brand):")
                    print("=" * 60)
                    
                    # Group products by brand
                    products_by_brand = {}
                    for barcode, product in extracted_products.items():
                        fields = product["extracted_fields"]
                        brand_name = fields.get('brand_name', 'Unknown Brand')
                        if brand_name not in products_by_brand:
                            products_by_brand[brand_name] = []
                        products_by_brand[brand_name].append((barcode, product))
                    
                    # Display 5 products per brand
                    total_displayed = 0
                    for brand_name, brand_products in products_by_brand.items():
                        print(f"\n🏷️ BRAND: {brand_name}")
                        print("=" * 50)
                        
                        # Show up to 5 products for this brand
                        products_to_show = brand_products[:5]
                        for idx, (barcode, product) in enumerate(products_to_show, 1):
                            fields = product["extracted_fields"]
                            
                            print(f"\n[{idx}] PRODUCT: {barcode}")
                            print("-" * 40)
                            print(f"  barcode: {fields['barcode']}")
                            print(f"  product_name: {fields['product_name']}")
                            print(f"  brand_name: {fields['brand_name']}")
                            print(f"  weight: {fields['weight']}")
                            print(f"  product_quantity_unit: {fields['product_quantity_unit']}")
                            print(f"  nutriscore_grade: {fields['nutriscore_grade']}")
                            print(f"  nutriscore_score: {fields['nutriscore_score']}")
                            print(f"  eco_score: {fields['eco_score']}")
                            
                            # Show CO2 total from sources
                            co2_sources = fields['co2_sources']
                            co2_total = co2_sources.get('agribalyse_total') or co2_sources.get('ecoscore_agribalyse_total')
                            print(f"  co2_total: {co2_total}")
                            
                            # Show all CO2 sources
                            print(f"  CO2 sources:")
                            for source, value in co2_sources.items():
                                status = "✅" if value is not None else "❌"
                                print(f"    {status} {source}: {value}")
                            
                            total_displayed += 1
                        
                        # Show if there are more products for this brand
                        if len(brand_products) > 5:
                            remaining = len(brand_products) - 5
                            print(f"\n... and {remaining} more products for {brand_name}")
                    
                    print(f"\n📊 Total products displayed: {total_displayed}")
                    print(f"📊 Total products extracted: {len(extracted_products)}")
                    
                
                # Show stats
                stats = extractor.get_extraction_stats()
                extraction_stats = results.get("extraction_stats", {})
                field_counts = extraction_stats.get("field_success_counts", {})
                total_extracted = extraction_stats.get("successful_extractions", 0)
                
                print(f"\n📊 EXTRACTION STATISTICS:")
                print(f"   → API calls: {stats['extraction_stats']['api_calls']}")
                print(f"   → Products discovered: {stats['extraction_stats']['products_discovered']}")
                print(f"   → Products enriched: {stats['extraction_stats']['products_enriched']}")
                print(f"   → Successful field extractions: {total_extracted}")
                
                print(f"\n🔍 FIELD EXTRACTION SUCCESS RATES:")
                for field, count in field_counts.items():
                    percentage = (count / max(1, total_extracted)) * 100
                    status = "✅" if percentage >= 80 else "⚠️" if percentage >= 50 else "❌"
                    print(f"   {status} {field}: {count}/{total_extracted} ({percentage:.1f}%)")
                
                print(f"\n🎯 KEY SUCCESS RATES:")
                critical_fields = ['barcode', 'product_name', 'brand_name', 'weight', 'co2_total']
                for field in critical_fields:
                    count = field_counts.get(field, 0)
                    percentage = (count / max(1, total_extracted)) * 100
                    print(f"   → {field}: {percentage:.1f}% ({count}/{total_extracted})")
                
                return True
            else:
                print(f"❌ Extraction test failed: {results.get('error')}")
                return False
                
        except Exception as e:
            print(f"💥 Test error: {e}")
            return False


if __name__ == "__main__":
    # Run the complete extraction test
    success = asyncio.run(test_complete_extraction())
    if success:
        print(f"\n🎉 All tests passed! Ready for integration.")
    else:
        print(f"\n❌ Tests failed. Check configuration.")