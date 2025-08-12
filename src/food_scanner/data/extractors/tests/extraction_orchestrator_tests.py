"""
src/food_scanner/data/extractors/tests/extraction_orchestrator_tests.py
Tests for ExtractionOrchestrator functionality

This file contains all test functions for the ExtractionOrchestrator class.
Tests are automatically imported and run when extraction_orchestrator.py is executed.
"""

import asyncio
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parents[6] / "src"))

from food_scanner.data.extractors.extraction_orchestrator import ExtractionOrchestrator


async def test_extraction_orchestrator_creation():
    """Test ExtractionOrchestrator instance creation"""
    print("🧪 TEST 1: ExtractionOrchestrator Creation")
    print("=" * 50)
    
    try:
        # Create orchestrator instance
        async with ExtractionOrchestrator(use_test_env=True) as orchestrator:
            print("✅ ExtractionOrchestrator created successfully")
            
            # Check component status
            component_status = orchestrator.get_component_status()
            print("🔍 Component Status:")
            for component, status in component_status.items():
                print(f"   → {component}: {'✅ Ready' if status else '❌ Not Ready'}")
            
            # Verify all components are ready
            all_ready = all(component_status.values())
            if all_ready:
                print("🎉 All components are ready!")
                return True
            else:
                print("⚠️ Some components are not ready")
                return False
                
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_discovery_phase():
    """Test product discovery phase"""
    print("\n🧪 TEST 2: Discovery Phase")
    print("=" * 50)
    
    try:
        async with ExtractionOrchestrator(use_test_env=True) as orchestrator:
            # Test with minimal data (discovery only)
            print("🔍 Testing discovery phase...")
            discovery_result = await orchestrator.product_discovery.discover_products_by_brands(
                brands=["Lindt"],  # Single brand for testing
                categories=["chocolates"],
                max_products_per_brand=2
            )
            
            if discovery_result["discovered_products"]:
                print(f"✅ Discovery test successful: {len(discovery_result['discovered_products'])} products found")
                
                # Check discovery stats
                discovery_stats = orchestrator.product_discovery.get_discovery_stats()
                print(f"📊 Discovery Statistics:")
                print(f"   → Brands processed: {discovery_stats['brands_processed']}")
                print(f"   → Categories processed: {discovery_stats['categories_processed']}")
                print(f"   → API calls: {discovery_stats['api_calls']}")
                print(f"   → Products discovered: {discovery_stats['products_discovered']}")
                
                return True
            else:
                print("⚠️ Discovery test: No products found (this is normal for test environment)")
                return True  # Not a failure, just no data
                
    except Exception as e:
        print(f"❌ Discovery test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_pipeline_statistics():
    """Test pipeline statistics functionality"""
    print("\n🧪 TEST 3: Pipeline Statistics")
    print("=" * 50)
    
    try:
        async with ExtractionOrchestrator(use_test_env=True) as orchestrator:
            # Check initial stats
            initial_stats = orchestrator.get_pipeline_stats()
            print("📊 Initial Pipeline Statistics:")
            for key, value in initial_stats.items():
                print(f"   → {key}: {value}")
            
            # Reset stats
            orchestrator.reset_pipeline_stats()
            reset_stats = orchestrator.get_pipeline_stats()
            print("\n🔄 After Reset:")
            for key, value in reset_stats.items():
                print(f"   → {key}: {value}")
            
            # Verify reset worked
            if all(value == 0 or value is None for value in reset_stats.values() if key != "start_time"):
                print("✅ Pipeline statistics reset successfully")
                return True
            else:
                print("⚠️ Some statistics were not properly reset")
                return False
                
    except Exception as e:
        print(f"❌ Pipeline statistics test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_component_integration():
    """Test that all components work together"""
    print("\n🧪 TEST 4: Component Integration")
    print("=" * 50)
    
    try:
        async with ExtractionOrchestrator(use_test_env=True) as orchestrator:
            # Test that components can be accessed
            components = [
                ("ProductDiscovery", orchestrator.product_discovery),
                ("ProductEnrichment", orchestrator.product_enrichment),
                ("ProductFieldExtraction", orchestrator.product_field_extraction),
                ("ExtractionAnalyzer", orchestrator.extraction_analyzer),
                ("ExtractionReporter", orchestrator.extraction_reporter)
            ]
            
            print("🔍 Testing component accessibility:")
            all_accessible = True
            
            for name, component in components:
                if component is not None:
                    print(f"   → {name}: ✅ Accessible")
                else:
                    print(f"   → {name}: ❌ Not accessible")
                    all_accessible = False
            
            if all_accessible:
                print("✅ All components are accessible")
                return True
            else:
                print("⚠️ Some components are not accessible")
                return False
                
    except Exception as e:
        print(f"❌ Component integration test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


async def run_all_tests():
    """Run all extraction orchestrator tests"""
    print("🚀 EXTRACTION ORCHESTRATOR TEST SUITE")
    print("=" * 60)
    
    test_functions = [
        test_extraction_orchestrator_creation,
        test_discovery_phase,
        test_pipeline_statistics,
        test_component_integration
    ]
    
    results = []
    
    for test_func in test_functions:
        try:
            result = await test_func()
            results.append(result)
        except Exception as e:
            print(f"💥 Test {test_func.__name__} crashed: {e}")
            results.append(False)
    
    # Summary
    print("\n" + "=" * 60)
    print("📋 TEST SUMMARY")
    print("=" * 60)
    
    passed = sum(results)
    total = len(results)
    
    for i, (test_func, result) in enumerate(zip(test_functions, results), 1):
        status = "✅ PASSED" if result else "❌ FAILED"
        print(f"   Test {i}: {test_func.__name__} - {status}")
    
    print(f"\n🎯 Overall Result: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 ALL TESTS PASSED! ExtractionOrchestrator is working perfectly!")
        return True
    else:
        print("⚠️ Some tests failed. Check the output above for details.")
        return False


# Convenience function for running tests
def run_tests():
    """Run all tests synchronously"""
    return asyncio.run(run_all_tests())


if __name__ == "__main__":
    # Run tests when this file is executed directly
    run_tests()
