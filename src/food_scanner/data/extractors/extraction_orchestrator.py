"""
src/food_scanner/data/extractors/extraction_orchestrator.py
EXTRACTION ORCHESTRATOR: Coordinates modular extraction components

RESPONSIBILITIES:
- Orchestrates ProductDiscovery, ProductEnrichment, ProductFieldExtraction
- Manages the complete extraction pipeline flow
- Maintains compatibility with existing API
- Delegates business logic to specialized components

This is a pure orchestrator - no business logic, only coordination.
"""

import asyncio
import sys
from datetime import datetime
from typing import Dict, List, Any
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parents[4] / "src"))

from food_scanner.core.constants import (
    MAJOR_CHOCOLATE_BRANDS_FRANCE, 
    CHOCOLATE_CATEGORIES,
)
from food_scanner.infrastructure.external_apis.openfoodfacts import OpenFoodFactsClient

# Import modular components
from food_scanner.data.extractors.discovery.product_discovery import ProductDiscovery
from food_scanner.data.extractors.enrichment.product_enrichment import ProductEnrichment
from food_scanner.data.extractors.field_extraction.product_field_extraction import ProductFieldExtraction
from food_scanner.data.extractors.analysis.extraction_analyzer import ExtractionAnalyzer
from food_scanner.data.extractors.reporting.extraction_reporter import ExtractionReporter


class ExtractionOrchestrator:
    """
    EXTRACTION ORCHESTRATOR: Coordinates modular extraction components
    
    RESPONSIBILITIES:
    1. **Orchestration**: Coordinate the flow between discovery, enrichment, and extraction
    2. **Pipeline Management**: Manage the complete extraction pipeline
    3. **Integration**: Ensure components work together seamlessly
    4. **Compatibility**: Maintain existing API contracts
    
    BUSINESS LOGIC IS DELEGATED TO:
    - ProductDiscovery: Product discovery from brands/categories
    - ProductEnrichment: API enrichment of discovered products
    - ProductFieldExtraction: Field parsing and validation
    - ExtractionAnalyzer: Data quality analysis
    - ExtractionReporter: Report generation
    """
    
    def __init__(self, use_test_env: bool = True):
        self.use_test_env = use_test_env
        self.client = None
        
        # Initialize modular components (will be properly initialized in __aenter__)
        self.product_discovery = None
        self.product_enrichment = None
        self.product_field_extraction = None
        self.extraction_analyzer = None
        self.extraction_reporter = None
        
        # Pipeline statistics
        self.pipeline_stats = {
            "start_time": None,
            "end_time": None,
            "total_products": 0,
            "discovery_products": 0,
            "enrichment_success": 0,
            "extraction_success": 0,
            "analysis_completed": False,
            "reports_generated": False
        }
    
    async def __aenter__(self):
        """Async context manager entry"""
        self.client = OpenFoodFactsClient(use_test_env=self.use_test_env)
        await self.client.__aenter__()
        
        # Initialize components that need the client
        self.product_discovery = ProductDiscovery(self.client)
        self.product_enrichment = ProductEnrichment(self.client)
        
        # Initialize components that don't need the client
        self.product_field_extraction = ProductFieldExtraction()
        self.extraction_analyzer = ExtractionAnalyzer()
        self.extraction_reporter = ExtractionReporter()
        
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self.client:
            await self.client.__aexit__(exc_type, exc_val, exc_tb)

    # ============================================================================
    # MAIN ORCHESTRATION METHOD
    # ============================================================================
    
    async def run_complete_extraction(
        self,
        brands: List[str] = None,
        categories: List[str] = None,
        max_products: int = 100,
        use_test_env: bool = False,
        use_cache: bool = False,
        generate_reports: bool = True
        ) -> Dict[str, Any]:
        """
        Run complete extraction pipeline using modular components
        
        FLOW:
        1. ProductDiscovery → discovers products by brands/categories
        2. ProductEnrichment → enriches discovered products with API data
        3. ProductFieldExtraction → extracts and validates individual fields
        4. ExtractionAnalyzer → analyzes data quality and readiness
        5. ExtractionReporter → generates comprehensive reports (optional)
        
        Args:
            brands: Brands to search (defaults to MAJOR_CHOCOLATE_BRANDS_FRANCE)
            categories: Categories to search (defaults to CHOCOLATE_CATEGORIES)
            max_products: Maximum products to process
            generate_reports: Whether to generate analysis reports
            
        Returns:
            Complete extraction results with all phases and optional analysis
        """
        print("🚀 EXTRACTION ORCHESTRATOR PIPELINE")
        print("=" * 60)
        print(f"🎯 Target: Extract chocolate products from OpenFoodFacts")
        print(f"📊 Maximum: {max_products} products")
        print(f"🔧 Environment: {'TEST' if self.use_test_env else 'PRODUCTION'}")
        print(f"📋 Reports: {'ENABLED' if generate_reports else 'DISABLED'}")
        print("=" * 60)
        
        self.pipeline_stats["start_time"] = datetime.now().isoformat()
        
        try:
            # ========================================================================
            # PHASE 1: PRODUCT DISCOVERY
            # ========================================================================
            print(f"\n🔍 PHASE 1: PRODUCT DISCOVERY")
            print("=" * 40)
            
            discovery_result = await self.product_discovery.discover_products_by_brands(
                brands=brands,
                categories=categories,
                max_products_per_brand=50
            )
            
            if discovery_result["discovered_products"] == {}:
                print("\n❌ No products discovered")
                return {"success": False, "error": "No products discovered"}
            
            discovered_barcodes = list(discovery_result["discovered_products"].keys())
            self.pipeline_stats["discovery_products"] = len(discovered_barcodes)
            
            print(f"✅ Discovery completed: {len(discovered_barcodes)} products found")
            
            # ========================================================================
            # PHASE 2: PRODUCT ENRICHMENT
            # ========================================================================
            print(f"\n🔧 PHASE 2: PRODUCT ENRICHMENT")
            print("=" * 40)
            
            enrichment_result = await self.product_enrichment.enrich_discovered_products(
                discovered_barcodes=discovered_barcodes,
                max_products=max_products
            )
            
            if enrichment_result["enrichment_stats"]["successful_enrichments"] == 0:
                print("\n❌ No products successfully enriched")
                return {"success": False, "error": "No products enriched"}
            
            self.pipeline_stats["enrichment_success"] = enrichment_result["enrichment_stats"]["successful_enrichments"]
            print(f"✅ Enrichment completed: {self.pipeline_stats['enrichment_success']} products enriched")
            
            # ========================================================================
            # PHASE 3: FIELD EXTRACTION
            # ========================================================================
            print(f"\n🔍 PHASE 3: FIELD EXTRACTION")
            print("=" * 40)
            
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
                        # Use ProductFieldExtraction to extract all fields
                        extracted_fields = self.product_field_extraction.extract_all_fields(raw_product)
                        
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
            
            self.pipeline_stats["extraction_success"] = extraction_stats["successful_extractions"]
            self.pipeline_stats["total_products"] = len(extracted_products)
            
            print(f"✅ Field extraction completed: {self.pipeline_stats['extraction_success']} products extracted")
            
            # ========================================================================
            # PHASE 4: DATA ANALYSIS (Optional)
            # ========================================================================
            if generate_reports:
                print(f"\n📊 PHASE 4: DATA ANALYSIS & REPORTING")
                print("=" * 40)
                
                try:
                    # Analyze extraction results
                    analysis_results = self.extraction_analyzer.analyze_extraction_results(
                        extracted_products, extraction_stats, self.pipeline_stats
                    )
                    
                    # Generate comprehensive reports
                    report_paths = self.extraction_reporter.generate_all_reports(analysis_results)
                    
                    self.pipeline_stats["analysis_completed"] = True
                    self.pipeline_stats["reports_generated"] = True
                    
                    print(f"✅ Analysis completed: {len(analysis_results.field_analyses)} fields analyzed")
                    print(f"✅ Reports generated: {len(report_paths)} report files")
                    
                except Exception as e:
                    print(f"⚠️ Analysis/Reporting failed: {e}")
                    self.pipeline_stats["analysis_completed"] = False
                    self.pipeline_stats["reports_generated"] = False
            
            # ========================================================================
            # PIPELINE COMPLETION
            # ========================================================================
            self.pipeline_stats["end_time"] = datetime.now().isoformat()
            
            print(f"\n🎉 EXTRACTION PIPELINE COMPLETED SUCCESSFULLY!")
            print("=" * 60)
            print(f"📊 Pipeline Statistics:")
            print(f"   → Discovery: {self.pipeline_stats['discovery_products']} products")
            print(f"   → Enrichment: {self.pipeline_stats['enrichment_success']} successful")
            print(f"   → Extraction: {self.pipeline_stats['extraction_success']} successful")
            print(f"   → Analysis: {'✅ Completed' if self.pipeline_stats['analysis_completed'] else '❌ Skipped'}")
            print(f"   → Reports: {'✅ Generated' if self.pipeline_stats['reports_generated'] else '❌ Skipped'}")
            print("=" * 60)
            
            # Return comprehensive results
            return {
                "success": True,
                "extracted_products": extracted_products,
                "extraction_stats": extraction_stats,
                "pipeline_stats": self.pipeline_stats,
                "discovery_result": discovery_result,
                "enrichment_result": enrichment_result
            }
            
        except Exception as e:
            print(f"💥 Pipeline execution failed: {e}")
            import traceback
            traceback.print_exc()
            return {"success": False, "error": str(e)}
    
    # ============================================================================
    # UTILITY METHODS
    # ============================================================================
    
    def get_pipeline_stats(self) -> Dict[str, Any]:
        """Get current pipeline statistics"""
        return self.pipeline_stats.copy()
    
    def reset_pipeline_stats(self):
        """Reset pipeline statistics"""
        self.pipeline_stats = {
            "start_time": None,
            "end_time": None,
            "total_products": 0,
            "discovery_products": 0,
            "enrichment_success": 0,
            "extraction_success": 0,
            "analysis_completed": False,
            "reports_generated": False
        }
    
    def get_component_status(self) -> Dict[str, bool]:
        """Get status of all modular components"""
        return {
            "product_discovery": hasattr(self.product_discovery, 'client'),
            "product_enrichment": hasattr(self.product_enrichment, 'client'),
            "product_field_extraction": hasattr(self.product_field_extraction, 'weight_parser'),
            "extraction_analyzer": hasattr(self.extraction_analyzer, 'co2_analyzer'),
            "extraction_reporter": hasattr(self.extraction_reporter, 'output_base_dir')
        }


if __name__ == "__main__":
    print("🚀 EXTRACTION ORCHESTRATOR")
    print("=" * 60)
    print("📋 Running comprehensive test suite...")
    print("=" * 60)
    
    try:
        # Import and run the comprehensive test suite
        from food_scanner.data.extractors.tests.extraction_orchestrator_tests import run_all_tests
        
        # Run all tests
        success = asyncio.run(run_all_tests())
        
        if success:
            print("\n🎉 EXTRACTION ORCHESTRATOR IS READY FOR PRODUCTION!")
        else:
            print("\n⚠️ EXTRACTION ORCHESTRATOR HAS ISSUES - CHECK TESTS ABOVE")
            
    except ImportError as e:
        print(f"⚠️ Could not import test suite: {e}")
        print("🔄 No fallback available - check test file path")
        
    except Exception as e:
        print(f"💥 Unexpected error: {e}")
        import traceback
        traceback.print_exc()
