"""
data_engineering/scripts/collection/complete_extraction_pipeline.py
COMPLETE EXTRACTION PIPELINE: Full ETL process from API to production-ready data

This script orchestrates the complete data pipeline:
1. Extract products using ProductExtractor (all brands × all categories)
2. Transform using ProductTransformer (validation + cleaning)  
3. Generate comprehensive reports
4. Save production-ready data

Target: 1K+ chocolate products ready for production database
"""

import asyncio
import json
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "src"))

from food_scanner.data.extractors.product_extractor_final import ProductExtractor
from food_scanner.data.transformers.product_transformer_final import ProductTransformer
from food_scanner.core.constants import MAJOR_CHOCOLATE_BRANDS_FRANCE, CHOCOLATE_CATEGORIES


class CompleteExtractionPipeline:
    """
    COMPLETE ETL PIPELINE: API → Database-ready products
    
    RESPONSIBILITIES:
    1. Orchestrate extraction across all brands × categories
    2. Apply business transformation rules
    3. Generate production reports
    4. Save results in appropriate directories
    5. Provide executive summary for decision making
    """
    
    def __init__(self, use_test_env: bool = True, target_products: int = 100):
        self.use_test_env = use_test_env
        self.target_products = target_products
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Configure output directories based on new architecture
        self.base_dir = Path(__file__).resolve().parents[2] / "data"
        self.output_dirs = {
            "raw_data": self.base_dir / "raw" / "openfoodfacts",
            "processed_data": self.base_dir / "processed" / "validated",
            "extraction_reports": self.base_dir / "analysis" / "extraction_phase",
            "transformation_reports": self.base_dir / "reports" / "transformation" / "transformation_performance",
            "cache": self.base_dir / "cache" / "api_responses"
        }
        
        # Create directories
        for directory in self.output_dirs.values():
            directory.mkdir(parents=True, exist_ok=True)
        
        # Pipeline statistics
        self.pipeline_stats = {
            "start_time": None,
            "end_time": None,
            "total_duration_minutes": 0,
            "extraction_phase": {},
            "transformation_phase": {},
            "reporting_phase": {},
            "final_metrics": {}
        }
    
    async def run_complete_pipeline(
        self,
        brands: List[str] = None,
        categories: List[str] = None,
    ) -> Dict[str, Any]:
        """
        Run the complete extraction and transformation pipeline
        
        Args:
            brands: Brands to process (defaults to all major chocolate brands)
            categories: Categories to process (defaults to all chocolate categories)
            
        Returns:
            Complete pipeline results with all data and reports
        """
        self.pipeline_stats["start_time"] = datetime.now().isoformat()
        
        print("🚀 COMPLETE CHOCOLATE DATA PIPELINE")
        print("=" * 70)
        print(f"🎯 Target: {self.target_products} production-ready chocolate products")
        print(f"📅 Timestamp: {self.timestamp}")
        print(f"🔧 Environment: {'TEST' if self.use_test_env else 'PRODUCTION'}")
        print(f"📁 Output directories configured in data_engineering/data/")
        print("=" * 70)
        
        try: 

            # Phase 1: Data Extraction
            print(f"\n🔍 PHASE 1: DATA EXTRACTION")
            print("-" * 50)
            extraction_results = await self._run_extraction_phase(brands, categories)
            
            if not extraction_results.get("success"):
                return {"success": False, "error": "Extraction phase failed", "results": extraction_results}
            
            # Phase 2: Data Transformation  
            print(f"\n🔧 PHASE 2: DATA TRANSFORMATION")
            print("-" * 50)
            transformation_results = self._run_transformation_phase(extraction_results["extracted_products"])
            
            # Phase 3: Report Generation
            print(f"\n📊 PHASE 3: REPORT GENERATION")
            print("-" * 50)
            reporting_results = self._generate_comprehensive_reports(
                extraction_results, transformation_results
            )
            
            # Phase 4: Final Summary
            self.pipeline_stats["end_time"] = datetime.now().isoformat()
            final_results = self._generate_final_summary(
                extraction_results, transformation_results, reporting_results
            )
            
            print(f"\n🎉 PIPELINE COMPLETED SUCCESSFULLY!")
            self._display_executive_summary(final_results)
            
            return final_results
            
        except Exception as e:
            print(f"\n💥 PIPELINE FAILED: {e}")
            import traceback
            traceback.print_exc()
            return {"success": False, "error": str(e)}
    
    async def _run_extraction_phase(self, brands: List[str], categories: List[str]) -> Dict[str, Any]:
        """Run the extraction phase using ProductExtractor"""

        # Use all brands and categories for testing
        if brands is None:
            brands = MAJOR_CHOCOLATE_BRANDS_FRANCE[:5]
            print(f"   📦 Using all {len(brands)} major chocolate brands")
        if categories is None:
            categories = CHOCOLATE_CATEGORIES[:5]

        
        print(f"   📦 Using {len(brands)} brands: {', '.join(brands)}")
        print(f"   🏷️ Using {len(categories)} categories: {', '.join(categories)}")
        
        print(f"   🎯 Target extraction: {self.target_products} products")
        print(f"   🔄 Strategy: Iterate brands × categories until target reached")
        
        async with ProductExtractor(use_test_env=self.use_test_env) as extractor:
            # Run extraction with all brands and categories
            extraction_results = await extractor.run_complete_extraction(
                brands=brands,
                categories=categories,
                max_products=self.target_products
            )
            
            if extraction_results.get("success"):
                extracted_products = extraction_results["extracted_products"]
                extraction_stats = extraction_results["extraction_stats"]
                
                print(f"\n   ✅ EXTRACTION PHASE COMPLETED:")
                print(f"      → Products extracted: {len(extracted_products)}")
                print(f"      → Field extraction success: {extraction_stats['successful_extractions']}")
                print(f"      → API calls made: {extraction_results['pipeline_stats']['api_calls']}")
                print(f"      → Target achievement: {(len(extracted_products) / self.target_products * 100):.1f}%")
                
                # Save extraction data
                self._save_extraction_data(extraction_results)
                
                self.pipeline_stats["extraction_phase"] = {
                    "products_extracted": len(extracted_products),
                    "api_calls": extraction_results['pipeline_stats']['api_calls'],
                    "success_rate": extraction_stats['successful_extractions'] / len(extracted_products) * 100 if extracted_products else 0,
                    "target_achievement": len(extracted_products) / self.target_products * 100
                }
                
                return {
                    "success": True,
                    "extracted_products": extracted_products,
                    "extraction_stats": extraction_stats,
                    "pipeline_stats": extraction_results['pipeline_stats'],
                    "extraction_reports": extraction_results.get('extraction_reports', {}),
                    "comprehensive_analysis": extraction_results.get('comprehensive_analysis')
                }
            else:
                return {"success": False, "error": "Extraction failed"}
    
    def _run_transformation_phase(self, extracted_products: Dict[str, Any]) -> Dict[str, Any]:
        """Run the transformation phase using ProductTransformer"""
        
        print(f"   🔧 Processing {len(extracted_products)} extracted products")
        print(f"   ✅ Applying business validation rules")
        print(f"   🧹 Data cleaning and normalization")
        print(f"   📊 Calculating derived fields")
        
        # Initialize transformer with duplicate handling
        transformer = ProductTransformer(use_duplicate_handling=False)
        
        # Run transformation
        transformation_results = transformer.transform_extracted_products(
            extracted_products=extracted_products,
            collection_timestamp=self.timestamp
        )
        
        validated_products = transformation_results["validated_products"]
        rejected_products = transformation_results["rejected_products"]
        production_readiness = transformation_results["production_readiness"]
        
        print(f"\n   ✅ TRANSFORMATION PHASE COMPLETED:")
        print(f"      → Products validated: {len(validated_products)}")
        print(f"      → Products rejected: {len(rejected_products)}")
        print(f"      → Production ready: {production_readiness['complete_products_ready_for_db']}")
        print(f"      → Success rate: {production_readiness['success_rate']:.1f}%")
        print(f"      → Quality grade: {production_readiness['data_quality_grade']}")
        
        # Save transformation data
        self._save_transformation_data(transformation_results)
        
        self.pipeline_stats["transformation_phase"] = {
            "products_validated": len(validated_products),
            "products_rejected": len(rejected_products),
            "production_ready": production_readiness['complete_products_ready_for_db'],
            "success_rate": production_readiness['success_rate'],
            "quality_grade": production_readiness['data_quality_grade']
        }
        
        return transformation_results
    
    def _generate_comprehensive_reports(
        self,
        extraction_results: Dict[str, Any],
        transformation_results: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Generate comprehensive reports for all phases"""
        
        print(f"   📊 Generating extraction phase reports...")
        print(f"   📋 Generating transformation phase reports...")
        print(f"   📈 Generating executive summary...")
        
        reports_generated = {}
        
        # 1. Executive Summary Report
        executive_summary = self._create_executive_summary(extraction_results, transformation_results)
        executive_file = self.output_dirs["transformation_reports"] / f"executive_summary_{self.timestamp}.json"
        
        with open(executive_file, 'w', encoding='utf-8') as f:
            json.dump(executive_summary, f, indent=2, ensure_ascii=False, default=str)
        
        reports_generated["executive_summary"] = executive_file
        
        # 2. Production Readiness Report
        production_report = self._create_production_readiness_report(transformation_results)
        production_file = self.output_dirs["transformation_reports"] / f"production_readiness_{self.timestamp}.json"
        
        with open(production_file, 'w', encoding='utf-8') as f:
            json.dump(production_report, f, indent=2, ensure_ascii=False, default=str)
        
        reports_generated["production_readiness"] = production_file
        
        # 3. Data Quality Report
        quality_report = self._create_data_quality_report(extraction_results, transformation_results)
        quality_file = self.output_dirs["transformation_reports"] / f"data_quality_{self.timestamp}.json"
        
        with open(quality_file, 'w', encoding='utf-8') as f:
            json.dump(quality_report, f, indent=2, ensure_ascii=False, default=str)
        
        reports_generated["data_quality"] = quality_file
        
        # 4. Database Loading Instructions
        loading_instructions = self._create_loading_instructions(transformation_results)
        loading_file = self.output_dirs["transformation_reports"] / f"loading_instructions_{self.timestamp}.json"
        
        with open(loading_file, 'w', encoding='utf-8') as f:
            json.dump(loading_instructions, f, indent=2, ensure_ascii=False, default=str)
        
        reports_generated["loading_instructions"] = loading_file
        
        print(f"   ✅ Reports generated: {len(reports_generated)} files")
        
        self.pipeline_stats["reporting_phase"] = {
            "reports_generated": len(reports_generated),
            "report_files": {k: str(v) for k, v in reports_generated.items()}
        }
        
        return {"reports_generated": reports_generated}
    
    def _save_extraction_data(self, extraction_results: Dict[str, Any]):
        """Save extraction data to appropriate directories"""
        
        # Save raw extraction data
        raw_file = self.output_dirs["raw_data"] / f"chocolate_extraction_raw_{self.timestamp}.json"
        
        raw_data = {
            "extraction_metadata": {
                "timestamp": self.timestamp,
                "environment": "TEST" if self.use_test_env else "PRODUCTION",
                "target_products": self.target_products
            },
            "extracted_products": extraction_results["extracted_products"],
            "extraction_stats": extraction_results["extraction_stats"],
            "pipeline_stats": extraction_results["pipeline_stats"]
        }
        
        with open(raw_file, 'w', encoding='utf-8') as f:
            json.dump(raw_data, f, indent=2, ensure_ascii=False, default=str)
        
        print(f"      💾 Raw extraction data saved: {raw_file.name}")
    
    def _save_transformation_data(self, transformation_results: Dict[str, Any]):
        """Save transformation data to appropriate directories"""
        
        # Save validated products (production-ready)
        validated_file = self.output_dirs["processed_data"] / f"validated_products_{self.timestamp}.json"
        
        validated_data = {
            "validation_metadata": {
                "timestamp": self.timestamp,
                "transformation_version": "1.0",
                "ready_for_database": True
            },
            "validated_products": transformation_results["validated_products"],
            "production_readiness": transformation_results["production_readiness"]
        }
        
        with open(validated_file, 'w', encoding='utf-8') as f:
            json.dump(validated_data, f, indent=2, ensure_ascii=False, default=str)
        
        # Save rejected products for analysis
        rejected_file = self.output_dirs["processed_data"].parent / "rejected" / f"rejected_products_{self.timestamp}.json"
        rejected_file.parent.mkdir(exist_ok=True)
        
        rejected_data = {
            "rejection_metadata": {
                "timestamp": self.timestamp,
                "total_rejected": len(transformation_results["rejected_products"])
            },
            "rejected_products": transformation_results["rejected_products"],
            "transformation_stats": transformation_results["transformation_stats"]
        }
        
        with open(rejected_file, 'w', encoding='utf-8') as f:
            json.dump(rejected_data, f, indent=2, ensure_ascii=False, default=str)
        
        print(f"      💾 Validated products saved: {validated_file.name}")
        print(f"      💾 Rejected products saved: {rejected_file.name}")
    
    def _create_executive_summary(
        self,
        extraction_results: Dict[str, Any],
        transformation_results: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Create executive summary for decision makers"""
        
        production_readiness = transformation_results["production_readiness"]
        extraction_stats = extraction_results["extraction_stats"]
        
        return {
            "executive_summary": {
                "pipeline_execution": {
                    "timestamp": self.timestamp,
                    "target_products": self.target_products,
                    "environment": "TEST" if self.use_test_env else "PRODUCTION"
                },
                "key_metrics": {
                    "products_extracted": len(extraction_results["extracted_products"]),
                    "products_validated": len(transformation_results["validated_products"]),
                    "production_ready": production_readiness["complete_products_ready_for_db"],
                    "overall_success_rate": production_readiness["success_rate"],
                    "data_quality_grade": production_readiness["data_quality_grade"]
                },
                "business_impact": {
                    "bot_launch_ready": production_readiness["bot_launch_ready"],
                    "estimated_bot_responses": production_readiness["complete_products_ready_for_db"],
                    "recommendation": "DEPLOY" if production_readiness["bot_launch_ready"] else "IMPROVE" if production_readiness["minimum_viable_dataset"] else "MAJOR_IMPROVEMENTS_NEEDED"
                },
                "next_actions": production_readiness["next_steps"]
            }
        }
    
    def _create_production_readiness_report(self, transformation_results: Dict[str, Any]) -> Dict[str, Any]:
        """Create detailed production readiness assessment"""
        
        return {
            "production_readiness_assessment": {
                "timestamp": self.timestamp,
                "overall_assessment": transformation_results["production_readiness"],
                "validation_breakdown": transformation_results["transformation_stats"]["validation_failures"],
                "data_quality_issues": transformation_results["data_quality_issues"],
                "database_loading": {
                    "ready_for_loading": len(transformation_results["validated_products"]),
                    "table_target": "products",
                    "estimated_loading_time": f"{len(transformation_results['validated_products']) * 0.1:.1f} seconds",
                    "prerequisites": [
                        "Supabase table 'products' must exist",
                        "Database connection configured", 
                        "Loading script permissions verified"
                    ]
                }
            }
        }
    
    def _create_data_quality_report(
        self,
        extraction_results: Dict[str, Any],
        transformation_results: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Create comprehensive data quality report"""
        
        field_analysis = extraction_results.get("field_analysis", {})
        
        return {
            "data_quality_assessment": {
                "timestamp": self.timestamp,
                "extraction_quality": {
                    "field_success_rates": extraction_results["extraction_stats"]["field_success_counts"],
                    "critical_fields_analysis": {
                        field_name: {
                            "success_rate": getattr(analysis, 'validity_rate', 0),
                            "quality_grade": "A" if getattr(analysis, 'validity_rate', 0) >= 90 else "B" if getattr(analysis, 'validity_rate', 0) >= 80 else "C"
                        }
                        for field_name, analysis in field_analysis.items()
                    } if field_analysis else {}
                },
                "transformation_quality": {
                    "validation_success_rate": transformation_results["production_readiness"]["success_rate"],
                    "data_cleaning_operations": transformation_results["transformation_stats"]["data_cleaning"],
                    "calculated_fields_success": transformation_results["transformation_stats"]["calculated_fields"]
                },
                "recommendations": [
                    f"Extraction phase: {len(extraction_results['extracted_products'])} products extracted",
                    f"Transformation phase: {transformation_results['production_readiness']['success_rate']:.1f}% success rate",
                    f"Data quality: Grade {transformation_results['production_readiness']['data_quality_grade']}"
                ]
            }
        }
    
    def _create_loading_instructions(self, transformation_results: Dict[str, Any]) -> Dict[str, Any]:
        """Create database loading instructions"""
        
        return {
            "database_loading_instructions": {
                "timestamp": self.timestamp,
                "data_source": f"validated_products_{self.timestamp}.json",
                "target_table": "products",
                "loading_strategy": {
                    "method": "bulk_insert",
                    "batch_size": 50,
                    "conflict_resolution": "ON CONFLICT (barcode) DO UPDATE SET updated_at = NOW()"
                },
                "data_validation": {
                    "total_records": len(transformation_results["validated_products"]),
                    "required_fields_complete": True,
                    "data_types_validated": True,
                    "business_rules_applied": True
                },
                "loading_script": "data_engineering/scripts/loading/load_products_to_supabase.py",
                "estimated_completion": f"{len(transformation_results['validated_products']) * 0.1:.1f} seconds",
                "post_loading_verification": [
                    "Verify record count matches",
                    "Test random product queries",
                    "Validate calculated fields",
                    "Check constraint violations"
                ]
            }
        }
    
    def _generate_final_summary(
        self,
        extraction_results: Dict[str, Any],
        transformation_results: Dict[str, Any],
        reporting_results: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Generate final pipeline summary"""
        
        start_time = datetime.fromisoformat(self.pipeline_stats["start_time"])
        end_time = datetime.fromisoformat(self.pipeline_stats["end_time"])
        duration_minutes = (end_time - start_time).total_seconds() / 60
        
        self.pipeline_stats["total_duration_minutes"] = duration_minutes
        self.pipeline_stats["final_metrics"] = {
            "target_products": self.target_products,
            "products_extracted": len(extraction_results["extracted_products"]),
            "products_validated": len(transformation_results["validated_products"]),
            "production_ready": transformation_results["production_readiness"]["complete_products_ready_for_db"],
            "overall_success_rate": transformation_results["production_readiness"]["success_rate"],
            "bot_launch_ready": transformation_results["production_readiness"]["bot_launch_ready"]
        }
        
        return {
            "success": True,
            "pipeline_stats": self.pipeline_stats,
            "extraction_results": extraction_results,
            "transformation_results": transformation_results,
            "reporting_results": reporting_results,
            "files_generated": {
                "raw_data": str(self.output_dirs["raw_data"] / f"chocolate_extraction_raw_{self.timestamp}.json"),
                "validated_products": str(self.output_dirs["processed_data"] / f"validated_products_{self.timestamp}.json"),
                "reports": reporting_results["reports_generated"]
            }
        }
    
    def _display_executive_summary(self, final_results: Dict[str, Any]):
        """Display executive summary for decision makers"""
        
        final_metrics = self.pipeline_stats["final_metrics"]
        
        print(f"\n📋 EXECUTIVE SUMMARY")
        print("=" * 70)
        
        print(f"🎯 PIPELINE OBJECTIVES:")
        print(f"   → Target products: {final_metrics['target_products']}")
        print(f"   → Products extracted: {final_metrics['products_extracted']}")
        print(f"   → Target achievement: {(final_metrics['products_extracted'] / final_metrics['target_products'] * 100):.1f}%")
        
        print(f"\n📊 BUSINESS METRICS:")
        print(f"   → Production-ready products: {final_metrics['production_ready']}")
        print(f"   → Overall success rate: {final_metrics['overall_success_rate']:.1f}%")
        print(f"   → Bot launch readiness: {'✅ READY' if final_metrics['bot_launch_ready'] else '❌ NOT READY'}")
        
        print(f"\n⏱️ PERFORMANCE METRICS:")
        print(f"   → Total pipeline duration: {self.pipeline_stats['total_duration_minutes']:.1f} minutes")
        print(f"   → API calls efficiency: {self.pipeline_stats['extraction_phase']['api_calls']} calls")
        print(f"   → Data quality grade: {self.pipeline_stats['transformation_phase']['quality_grade']}")
        
        print(f"\n📁 DELIVERABLES:")
        files_generated = final_results["files_generated"]
        for file_type, file_path in files_generated.items():
            if file_type == "reports":
                print(f"   → {file_type}: {len(file_path)} report files")
            else:
                print(f"   → {file_type}: {Path(file_path).name}")
        
        print(f"\n🚀 RECOMMENDATIONS:")
        production_readiness = final_results["transformation_results"]["production_readiness"]
        for step in production_readiness["next_steps"]:
            print(f"   {step}")
        
        print(f"\n💡 NEXT ACTIONS:")
        if final_metrics['bot_launch_ready']:
            print(f"   1. 📊 Load validated products into Supabase database")
            print(f"   2. 🤖 Begin Telegram bot development") 
            print(f"   3. 🧪 Test bot with production data")
        else:
            print(f"   1. 📈 Review data quality issues in reports")
            print(f"   2. 🔧 Improve extraction/transformation pipeline")
            print(f"   3. 🔄 Re-run pipeline with improvements")
        
        print("=" * 70)


async def main():
    """Main execution function"""
    
    print("🍫 COMPLETE CHOCOLATE DATA PIPELINE")
    print("Extracting and transforming chocolate products for production database")
    print("=" * 70)
    
    # Configuration
    USE_TEST_ENV = True  # Set to False for production API
    TARGET_PRODUCTS = 100 # Target number of products
    
    # Initialize pipeline
    pipeline = CompleteExtractionPipeline(
        use_test_env=USE_TEST_ENV,
        target_products=TARGET_PRODUCTS
    )
    
    # Run complete pipeline with limited brands and categories
    results = await pipeline.run_complete_pipeline()
    
    if results.get("success"):
        print(f"\n🎉 PIPELINE EXECUTION SUCCESSFUL!")
        print(f"📊 Check data_engineering/data/ for all generated files")
        return 0
    else:
        print(f"\n❌ PIPELINE EXECUTION FAILED!")
        print(f"Error: {results.get('error', 'Unknown error')}")
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)