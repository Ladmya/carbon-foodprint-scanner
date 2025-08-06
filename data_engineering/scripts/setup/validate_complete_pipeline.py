"""
data_engineering/scripts/setup/validate_complete_pipeline.py
FINAL VALIDATION: Test complet du pipeline
"""

import asyncio
import sys
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, Any

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "src"))

try:
    from food_scanner.data.extractors.product_extractor_final import ProductExtractor
    from food_scanner.data.transformers.product_transformer_final import ProductTransformer
    from food_scanner.data.transformers.field_transformers.weight_parser import WeightParser
    from food_scanner.data.utils.duplicate_handler import DuplicateHandler
    IMPORTS_OK = True
except ImportError as e:
    IMPORTS_OK = False
    IMPORT_ERROR = str(e)


class PipelineValidator:
    """
    COMPREHENSIVE PIPELINE VALIDATION
    - Test all components individually
    - Run mini end-to-end pipeline
    - Validate data quality outputs
    - Performance benchmarking
    """
    
    def __init__(self):
        self.validation_results = {
            "timestamp": datetime.now().isoformat(),
            "validation_version": "v1.0",
            "component_tests": {},
            "integration_tests": {},
            "performance_tests": {},
            "overall_status": "UNKNOWN"
        }

    async def run_complete_validation(self) -> Dict[str, Any]:
        """Run comprehensive validation for interview demo"""
        print("🧪 PIPELINE VALIDATION - COMPLETE TEST SUITE")
        print("=" * 60)
        print("🎯 Goal: Validate entire pipeline for interview demonstration")
        print("🕒 Started:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        print("=" * 60)
        
        try:
            # 1. COMPONENT TESTS
            print("\n1️⃣ COMPONENT VALIDATION")
            await self._test_components()
            
            # 2. INTEGRATION TESTS
            print("\n2️⃣ INTEGRATION TESTING")
            await self._test_integration()
            
            # 3. PERFORMANCE TESTS
            print("\n3️⃣ PERFORMANCE BENCHMARKING")
            await self._test_performance()
            
            # 4. FINAL ASSESSMENT
            print("\n4️⃣ FINAL ASSESSMENT")
            self._assess_overall_status()
            
            # 5. SAVE VALIDATION REPORT
            self._save_validation_report()
            
            return self.validation_results
            
        except Exception as e:
            print(f"\n💥 Validation failed: {e}")
            import traceback
            traceback.print_exc()
            self.validation_results["overall_status"] = "FAILED"
            self.validation_results["fatal_error"] = str(e)
            return self.validation_results

    async def _test_components(self):
        """Test individual components"""
        component_results = {}
        
        # Test 1: Import validation
        print("   🔍 Testing imports...")
        if IMPORTS_OK:
            component_results["imports"] = {"status": "PASS", "message": "All imports successful"}
            print("      ✅ All imports successful")
        else:
            component_results["imports"] = {"status": "FAIL", "message": f"Import error: {IMPORT_ERROR}"}
            print(f"      ❌ Import failed: {IMPORT_ERROR}")
            self.validation_results["component_tests"] = component_results
            return
        
        # Test 2: Weight Parser
        print("   🔍 Testing WeightParser...")
        weight_test = await self._test_weight_parser()
        component_results["weight_parser"] = weight_test
        print(f"      {'✅' if weight_test['status'] == 'PASS' else '❌'} WeightParser: {weight_test['message']}")
        
        # Test 3: Product Transformer
        print("   🔍 Testing ProductTransformer...")
        transformer_test = await self._test_transformer()
        component_results["transformer"] = transformer_test
        print(f"      {'✅' if transformer_test['status'] == 'PASS' else '❌'} Transformer: {transformer_test['message']}")
        
        # Test 4: Duplicate Handler
        print("   🔍 Testing DuplicateHandler...")
        duplicate_test = await self._test_duplicate_handler()
        component_results["duplicate_handler"] = duplicate_test
        print(f"      {'✅' if duplicate_test['status'] == 'PASS' else '❌'} DuplicateHandler: {duplicate_test['message']}")
        
        # Test 5: Data directory structure
        print("   🔍 Testing data directory structure...")
        directory_test = self._test_directory_structure()
        component_results["directory_structure"] = directory_test
        print(f"      {'✅' if directory_test['status'] == 'PASS' else '❌'} Directories: {directory_test['message']}")
        
        self.validation_results["component_tests"] = component_results

    async def _test_weight_parser(self) -> Dict[str, Any]:
        """Test weight parser with various inputs"""
        try:
            parser = WeightParser()
            
            test_cases = [
                (400, 400.0, "g"),           # Direct int
                (1.5, 1.5, "g"),            # Direct float  
                ("400g", 400.0, "g"),       # String with unit
                ("1.5kg", 1500.0, "g"),     # Conversion
                ("2 x 250g", 500.0, "g"),   # Multiplication
                ("500ml", 500.0, "ml"),     # Volume
                (None, None, None),         # None handling
                ("invalid", None, None)     # Invalid input
            ]
            
            passed = 0
            for input_val, expected_weight, expected_unit in test_cases:
                weight, unit = parser.parse_weight_and_unit(input_val)
                if weight == expected_weight and unit == expected_unit:
                    passed += 1
            
            success_rate = (passed / len(test_cases)) * 100
            
            if success_rate >= 90:
                return {"status": "PASS", "message": f"Success rate: {success_rate:.1f}%", "details": {"passed": passed, "total": len(test_cases)}}
            else:
                return {"status": "FAIL", "message": f"Low success rate: {success_rate:.1f}%", "details": {"passed": passed, "total": len(test_cases)}}
                
        except Exception as e:
            return {"status": "ERROR", "message": f"Exception: {str(e)}"}

    async def _test_transformer(self) -> Dict[str, Any]:
        """Test product transformer with sample data"""
        try:
            transformer = ProductTransformer()
            
            # Sample valid product
            sample_product = {
                "test_barcode": {
                    "extraction_success": True,
                    "raw_product_data": {
                        "code": "test_barcode",
                        "product_name_fr": "Test Product FR",
                        "product_name": "Test Product EN",
                        "brands": "Test Brand",
                        "product_quantity": "400g",
                        "nutriscore_grade": "C",
                        "nutriscore_score": 8,
                        "agribalyse": {"co2_total": 450.0}
                    }
                }
            }
            
            # Transform
            result = transformer.transform_extracted_products(sample_product)
            
            validated = result.get("validated_products", {})
            rejected = result.get("rejected_products", {})
            
            if len(validated) == 1 and len(rejected) == 0:
                # Check transformed fields
                transformed = validated["test_barcode"]["transformed_data"]
                
                checks = [
                    transformed.get("product_name") == "Test Product FR",
                    transformed.get("brand_name") == "Test Brand",
                    transformed.get("weight") == 400.0,
                    transformed.get("product_quantity_unit") == "g",
                    transformed.get("nutriscore_grade") == "C",
                    transformed.get("co2_total") == 450.0
                ]
                
                if all(checks):
                    return {"status": "PASS", "message": "All transformations correct", "details": {"validated": 1, "rejected": 0}}
                else:
                    return {"status": "FAIL", "message": "Transformation errors", "details": {"checks_passed": sum(checks), "total_checks": len(checks)}}
            else:
                return {"status": "FAIL", "message": f"Validation failed: {len(validated)} validated, {len(rejected)} rejected"}
                
        except Exception as e:
            return {"status": "ERROR", "message": f"Exception: {str(e)}"}

    async def _test_duplicate_handler(self) -> Dict[str, Any]:
        """Test duplicate handler functionality"""
        # Use temporary cache directory => gets deleted automatically if the test is successful
        base_dir = Path(__file__).resolve().parents[3]
        temp_cache_dir = base_dir / "data_engineering/data/cache/temp_test"
        
        try:
            # Ensure temp directory exists and is clean
            if temp_cache_dir.exists():
                import shutil
                shutil.rmtree(temp_cache_dir, ignore_errors=True)
            temp_cache_dir.mkdir(parents=True, exist_ok=True)
            
            # Verify directory was created
            if not temp_cache_dir.exists():
                return {"status": "ERROR", "message": f"Failed to create temp directory: {temp_cache_dir}"}
            
            with DuplicateHandler(cache_dir=temp_cache_dir) as handler:
                # Sample products
                sample_products = {
                    "test1": {
                        "raw_discovery_data": {
                            "code": "test1",
                            "product_name": "Test Product 1"
                        }
                    },
                    "test2": {
                        "raw_discovery_data": {
                            "code": "test2", 
                            "product_name": "Test Product 2"
                        }
                    }
                }
                
                timestamp = datetime.now().isoformat()
                
                # First run
                result1 = handler.process_discovered_products(sample_products, timestamp)
                
                # Second run (should detect duplicates)
                result2 = handler.process_discovered_products(sample_products, timestamp)
                
                # Validate results
                if (len(result1["new_products"]) == 2 and 
                    len(result1["duplicate_products"]) == 0 and
                    len(result2["new_products"]) == 0 and 
                    len(result2["duplicate_products"]) == 2):
                    
                    return {"status": "PASS", "message": "Duplicate detection working", "details": {"first_run_new": 2, "second_run_duplicates": 2}}
                else:
                    return {"status": "FAIL", "message": "Duplicate detection failed", "details": result2}
                    
        except Exception as e:
            return {"status": "ERROR", "message": f"Exception: {str(e)}"}
        finally:
            # Always clean up temp cache
            try:
                if temp_cache_dir.exists():
                    import shutil
                    shutil.rmtree(temp_cache_dir, ignore_errors=True)
            except Exception as cleanup_error:
                print(f"Warning: Failed to clean up temp directory: {cleanup_error}")

    def _test_directory_structure(self) -> Dict[str, Any]:
        """Test required directory structure exists"""
        try:
            base_dir = Path(__file__).resolve().parents[3]
            
            required_dirs = [
                "src/food_scanner/data/extractors",
                "src/food_scanner/data/transformers",
                "src/food_scanner/data/loaders",
                "data_engineering/data/raw/openfoodfacts",
                "data_engineering/data/processed/validated",
                "data_engineering/data/reports/extraction/raw_data_quality",
                "data_engineering/data/reports/loading/loading_quality",
                "data_engineering/data/reports/transformation/transformed_data_quality",
                "data_engineering/data/reports/quality/quality_reports",
                "data_engineering/data/reports/production/production_readiness"
            ]
            
            missing_dirs = []
            for dir_path in required_dirs:
                full_path = base_dir / dir_path
                if not full_path.exists():
                    missing_dirs.append(dir_path)
            
            if not missing_dirs:
                return {"status": "PASS", "message": "All required directories exist", "details": {"checked": len(required_dirs)}}
            else:
                return {"status": "FAIL", "message": f"Missing directories: {missing_dirs}", "details": {"missing": len(missing_dirs)}}
                
        except Exception as e:
            return {"status": "ERROR", "message": f"Exception: {str(e)}"}

    async def _test_integration(self):
        """Test end-to-end integration with mini pipeline"""
        integration_results = {}
        
        print("   🔍 Testing mini end-to-end pipeline...")
        
        try:
            # Test with 2 products from well-known brands
            async with ProductExtractor(use_test_env=True) as extractor:
                # Small discovery test
                discovery_result = await extractor.discover_products_by_search(
                    brands=["Nutella"],
                    categories=["Chocolates"], 
                    max_products_per_brand=3
                )
                
                discovered_count = len(discovery_result.get("discovered_products", {}))
                
                if discovered_count >= 1:
                    print(f"      ✅ Discovery: {discovered_count} products found")
                    
                    # Test enrichment
                    barcodes = list(discovery_result["discovered_products"].keys())[:2]  # First 2
                    
                    enrichment_result = await extractor.enrich_products_with_full_data(
                        discovered_barcodes=barcodes,
                        max_products=2
                    )
                    
                    enriched_count = len([p for p in enrichment_result["enriched_products"].values() 
                                        if p.get("extraction_success", False)])
                    
                    if enriched_count >= 1:
                        print(f"      ✅ Enrichment: {enriched_count} products enriched")
                        
                        # Test transformation
                        transformer = ProductTransformer()
                        transformation_result = transformer.transform_extracted_products(
                            enrichment_result["enriched_products"]
                        )
                        
                        validated_count = len(transformation_result["validated_products"])
                        
                        if validated_count >= 1:
                            print(f"      ✅ Transformation: {validated_count} products validated")
                            
                            integration_results["end_to_end"] = {
                                "status": "PASS",
                                "message": f"Mini pipeline successful: {discovered_count}→{enriched_count}→{validated_count}",
                                "details": {
                                    "discovered": discovered_count,
                                    "enriched": enriched_count,
                                    "validated": validated_count
                                }
                            }
                        else:
                            integration_results["end_to_end"] = {"status": "FAIL", "message": "No products validated"}
                    else:
                        integration_results["end_to_end"] = {"status": "FAIL", "message": "No products enriched"}
                else:
                    integration_results["end_to_end"] = {"status": "FAIL", "message": "No products discovered"}
                    
        except Exception as e:
            integration_results["end_to_end"] = {"status": "ERROR", "message": f"Integration test failed: {str(e)}"}
            print(f"      ❌ Integration test error: {str(e)}")
        
        self.validation_results["integration_tests"] = integration_results

    async def _test_performance(self):
        """Basic performance benchmarking"""
        performance_results = {}
        
        print("   🔍 Testing performance benchmarks...")
        
        # Test 1: Weight parser performance
        start_time = datetime.now()
        parser = WeightParser()
        
        test_inputs = ["400g", "1.5kg", "2 x 250g", "500ml"] * 250  # 1000 parsing operations
        
        for input_val in test_inputs:
            parser.parse_weight_and_unit(input_val)
        
        end_time = datetime.now()
        parsing_duration = (end_time - start_time).total_seconds()
        parsing_rate = len(test_inputs) / parsing_duration
        
        performance_results["weight_parsing"] = {
            "operations": len(test_inputs),
            "duration_seconds": round(parsing_duration, 3),
            "operations_per_second": round(parsing_rate, 1),
            "assessment": "EXCELLENT" if parsing_rate > 1000 else "GOOD" if parsing_rate > 500 else "SLOW"
        }
        
        print(f"      📊 Weight parsing: {parsing_rate:.1f} ops/sec")
        
        # Test 2: Transformation performance
        transformer = ProductTransformer()
        
        # Create sample dataset
        sample_products = {}
        for i in range(100):
            sample_products[f"test{i}"] = {
                "extraction_success": True,
                "raw_product_data": {
                    "code": f"test{i}",
                    "product_name_fr": f"Test Product {i}",
                    "brands": "Test Brand",
                    "product_quantity": f"{100 + i}g",
                    "nutriscore_grade": "C",
                    "agribalyse": {"co2_total": 400.0 + i}
                }
            }
        
        start_time = datetime.now()
        transformation_result = transformer.transform_extracted_products(sample_products)
        end_time = datetime.now()
        
        transformation_duration = (end_time - start_time).total_seconds()
        transformation_rate = len(sample_products) / transformation_duration
        
        performance_results["transformation"] = {
            "products": len(sample_products),
            "duration_seconds": round(transformation_duration, 3),
            "products_per_second": round(transformation_rate, 1),
            "validated": len(transformation_result["validated_products"]),
            "assessment": "EXCELLENT" if transformation_rate > 100 else "GOOD" if transformation_rate > 50 else "SLOW"
        }
        
        print(f"      📊 Transformation: {transformation_rate:.1f} products/sec")
        
        self.validation_results["performance_tests"] = performance_results

    def _assess_overall_status(self):
        """Assess overall validation status"""
        component_tests = self.validation_results.get("component_tests", {})
        integration_tests = self.validation_results.get("integration_tests", {})
        performance_tests = self.validation_results.get("performance_tests", {})
        
        # Count passes and fails
        all_tests = []
        
        for test_group in [component_tests, integration_tests]:
            for test_name, test_result in test_group.items():
                all_tests.append(test_result.get("status", "UNKNOWN"))
        
        pass_count = all_tests.count("PASS")
        fail_count = all_tests.count("FAIL") + all_tests.count("ERROR")
        total_tests = len(all_tests)
        
        if total_tests == 0:
            status = "NO_TESTS"
        elif fail_count == 0:
            status = "ALL_PASS"
        elif pass_count >= total_tests * 0.8:  # 80% pass rate
            status = "MOSTLY_PASS"
        else:
            status = "MULTIPLE_FAILURES"
        
        # Performance assessment
        performance_summary = {}
        for perf_test, perf_data in performance_tests.items():
            assessment = perf_data.get("assessment", "UNKNOWN")
            performance_summary[perf_test] = assessment
        
        overall_assessment = {
            "status": status,
            "test_summary": {
                "total_tests": total_tests,
                "passed": pass_count,
                "failed": fail_count,
                "pass_rate": round((pass_count / max(1, total_tests)) * 100, 1)
            },
            "performance_summary": performance_summary,
            "interview_readiness": self._assess_interview_readiness(status, pass_count, total_tests)
        }
        
        self.validation_results["overall_status"] = overall_assessment
        
        print(f"   🎯 Overall Status: {status}")
        print(f"   📊 Pass Rate: {overall_assessment['test_summary']['pass_rate']}%")
        print(f"   🚀 Interview Ready: {overall_assessment['interview_readiness']}")

    def _assess_interview_readiness(self, status: str, pass_count: int, total_tests: int) -> str:
        """Assess if system is ready for interview demonstration"""
        if status == "ALL_PASS" and pass_count >= 5:
            return "EXCELLENT - Demo ready!"
        elif status in ["ALL_PASS", "MOSTLY_PASS"] and pass_count >= 3:
            return "GOOD - Minor issues to address"
        elif pass_count >= 2:
            return "FAIR - Some components working"
        else:
            return "POOR - Major fixes needed"

    def _save_validation_report(self):
        """Save validation report"""
        base_dir = Path(__file__).resolve().parents[3]
        report_dir = base_dir / "data_engineering/data/reports/environment"
        report_dir.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_file = report_dir / f"pipeline_validation_{timestamp}.json"
        
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(self.validation_results, f, indent=2, ensure_ascii=False, default=str)
        
        print(f"\n💾 Validation report saved: {report_file.name}")


async def main():
    """Run complete pipeline validation"""
    validator = PipelineValidator()
    
    results = await validator.run_complete_validation()
    
    # Final summary
    overall = results.get("overall_status", {})
    status = overall.get("status", "UNKNOWN")
    interview_readiness = overall.get("interview_readiness", "UNKNOWN")
    
    print(f"\n🎯 VALIDATION COMPLETE")
    print("=" * 40)
    print(f"📊 Status: {status}")
    print(f"🚀 Interview Readiness: {interview_readiness}")
    
    if status in ["ALL_PASS", "MOSTLY_PASS"]:
        print(f"\n✅ SYSTEM VALIDATED - READY FOR INTERVIEW!")
        print(f"🎯 Next steps:")
        print(f"   1. Run full collection with: python collect_chocolate_data_final.py")
        print(f"   2. Analyze results with: python analyze_data_quality.py")
        print(f"   3. Demo pipeline during interview")
        return 0
    else:
        print(f"\n⚠️ VALIDATION ISSUES DETECTED")
        print(f"🔧 Check component test results and fix issues")
        print(f"📋 Review validation report for details")
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    exit(exit_code)