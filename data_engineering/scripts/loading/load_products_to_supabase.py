"""
data_engineering/scripts/loading/load_products_to_supabase.py
MAIN LOADING SCRIPT: Load validated products to Supabase production database

This script takes validated products from the transformation pipeline
and loads them into the Supabase products table with proper error handling,
batching, and progress monitoring.
"""

import json
import sys

from datetime import datetime
from pathlib import Path 
from typing import Dict, List, Any, Optional

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "src"))

from food_scanner.infrastructure.database.repositories.supabase_client import get_supabase_client
from src.food_scanner.data.loaders.batch_loader import ProductBatchLoader
from src.food_scanner.data.loaders.loading_monitor import LoadingMonitor


class ProductsToSupabaseLoader:
    """
    Main class for loading validated products to Supabase
    
    RESPONSIBILITIES:
    1. Load and validate input JSON files
    2. Transform data to Supabase format
    3. Coordinate batch loading with error handling
    4. Monitor progress and generate reports
    5. Verify data integrity after loading
    """
    
    def __init__(self, environment: str = "test", batch_size: int = 50):
        self.environment = environment
        self.batch_size = batch_size
        self.supabase = get_supabase_client(environment)
        self.batch_loader = ProductBatchLoader(self.supabase, batch_size)
        self.monitor = LoadingMonitor()
        
        print(f"🔗 Connected to Supabase ({environment.upper()} environment)")
        print(f"📦 Batch size: {batch_size} products per batch")
    
    def load_from_file(self, file_path: Path) -> Dict[str, Any]:
        """
        Main entry point: Load products from validated JSON file
        
        Args:
            file_path: Path to validated_products_*.json file
            
        Returns:
            Loading results with statistics and any errors
        """
        start_time = datetime.now()
        
        print(f"🚀 LOADING PRODUCTS TO SUPABASE")
        print("=" * 60)
        print(f"📁 Source file: {file_path.name}")
        print(f"🌍 Environment: {self.environment.upper()}")
        print(f"📅 Started: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60)
        
        try:
            # Step 1: Load and validate input data
            print(f"\n📖 Step 1: Loading input data...")
            validated_products = self._load_validated_products(file_path)
            
            if not validated_products:
                return {"success": False, "error": "No products found in input file"}
            
            print(f"   ✅ Loaded {len(validated_products)} products for processing")
            
            # Step 2: Transform data to Supabase format
            print(f"\n🔄 Step 2: Transforming data format...")
            supabase_records = self._transform_to_supabase_format(validated_products)
            
            print(f"   ✅ Transformed {len(supabase_records)} records")
            
            # Step 3: Load data in batches
            print(f"\n📤 Step 3: Loading to Supabase database...")
            loading_results = self.batch_loader.load_products_in_batches(
                supabase_records, 
                monitor=self.monitor
            )
            
            # Step 4: Verify loaded data
            print(f"\n✅ Step 4: Verifying loaded data...")
            verification_results = self._verify_loaded_data(supabase_records)
            
            # Step 5: Generate final report
            end_time = datetime.now()
            duration = end_time - start_time
            
            final_results = {
                "success": True,
                "loading_summary": {
                    "start_time": start_time.isoformat(),
                    "end_time": end_time.isoformat(),
                    "duration_seconds": duration.total_seconds(),
                    "environment": self.environment,
                    "source_file": str(file_path),
                    "products_processed": len(supabase_records),
                    "batches_processed": loading_results["batches_processed"],
                    "successful_loads": loading_results["successful_loads"],
                    "failed_loads": loading_results["failed_loads"],
                    "retry_count": loading_results["retry_count"]
                },
                "loading_results": loading_results,
                "verification_results": verification_results,
                "performance_metrics": self.monitor.get_final_metrics()
            }
            
            self._display_final_summary(final_results)
            return final_results
            
        except Exception as e:
            error_msg = f"Loading failed: {str(e)}"
            print(f"\n❌ {error_msg}")
            import traceback
            traceback.print_exc()
            
            return {
                "success": False,
                "error": error_msg,
                "duration_seconds": (datetime.now() - start_time).total_seconds()
            }
    
    def _load_validated_products(self, file_path: Path) -> Dict[str, Any]:
        """Load and validate the input JSON file"""
        
        if not file_path.exists():
            raise FileNotFoundError(f"Input file not found: {file_path}")
        
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Handle different JSON structures
        if "validated_products" in data:
            return data["validated_products"]
        elif isinstance(data, dict) and len(data) > 0:
            # Assume the entire dict is validated products
            return data
        else:
            raise ValueError("Invalid JSON structure: expected validated_products")
    
    def _transform_to_supabase_format(self, validated_products: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Transform validated products to Supabase table format"""
        
        supabase_records = []
        
        for barcode, product_info in validated_products.items():
            try:
                # Extract transformed data
                if "transformed_data" in product_info:
                    product_data = product_info["transformed_data"]
                else:
                    product_data = product_info
                
                # Create Supabase record with required fields
                record = self._create_supabase_record(barcode, product_data)
                
                if record:
                    supabase_records.append(record)
                    
            except Exception as e:
                print(f"   ⚠️ Skipping product {barcode}: {e}")
                continue
        
        return supabase_records
    
    def _create_supabase_record(self, barcode: str, product_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Create a single Supabase record from product data"""
        
        # Validate required fields (based on schema constraints)
        required_fields = {
            "barcode": barcode,
            "product_name": product_data.get("product_name"),
            "brand_name": product_data.get("brand_name"),
            "weight": product_data.get("weight"),
            "co2_total": product_data.get("co2_total")
        }
        
        # Check for missing required fields
        missing_fields = [field for field, value in required_fields.items() if value is None]
        if missing_fields:
            raise ValueError(f"Missing required fields: {missing_fields}")
        
        # Build complete record
        record = {
            # Required fields
            "barcode": str(barcode),
            "product_name": str(product_data["product_name"]),
            "brand_name": str(product_data["brand_name"]),
            "weight": float(product_data["weight"]),
            "co2_total": float(product_data["co2_total"]),
            
            # Optional fields with defaults
            "brand_tags": product_data.get("brand_tags", []),
            "product_quantity_unit": product_data.get("product_quantity_unit", "g"),
            "nutriscore_grade": product_data.get("nutriscore_grade"),
            "nutriscore_score": product_data.get("nutriscore_score"),
            "eco_score": product_data.get("eco_score"),
            
            # System fields
            "collection_timestamp": product_data.get("collection_timestamp"),
            "transformation_version": product_data.get("transformation_version", "1.0"),
            "raw_data": product_data.get("raw_data", {})
        }
        
        # Validate nutriscore requirement (at least one field)
        if not record["nutriscore_grade"] and record["nutriscore_score"] is None:
            raise ValueError("At least one nutriscore field required")
        
        return record
    
    def _verify_loaded_data(self, expected_records: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Verify that data was loaded correctly"""
        
        verification_results = {
            "total_expected": len(expected_records),
            "total_in_database": 0,
            "verification_passed": False,
            "sample_checks": [],
            "missing_barcodes": []
        }
        
        try:
            # Count total records in database
            count_result = self.supabase.table('products').select('barcode', count='exact').execute()
            verification_results["total_in_database"] = count_result.count
            
            # Sample verification: check first 5 records
            sample_barcodes = [record["barcode"] for record in expected_records[:5]]
            
            for barcode in sample_barcodes:
                db_result = self.supabase.table('products').select('*').eq('barcode', barcode).execute()
                
                if db_result.data:
                    verification_results["sample_checks"].append({
                        "barcode": barcode,
                        "found": True,
                        "product_name": db_result.data[0]["product_name"]
                    })
                else:
                    verification_results["sample_checks"].append({
                        "barcode": barcode,
                        "found": False
                    })
                    verification_results["missing_barcodes"].append(barcode)
            
            # Overall verification
            verification_results["verification_passed"] = (
                len(verification_results["missing_barcodes"]) == 0
            )
            
            print(f"   📊 Database total: {verification_results['total_in_database']} products")
            print(f"   🔍 Sample verification: {len(sample_barcodes)} products checked")
            
            if verification_results["verification_passed"]:
                print(f"   ✅ Verification PASSED - all sample products found")
            else:
                print(f"   ❌ Verification FAILED - {len(verification_results['missing_barcodes'])} products missing")
        
        except Exception as e:
            print(f"   ⚠️ Verification failed: {e}")
            verification_results["verification_error"] = str(e)
        
        return verification_results
    
    def _display_final_summary(self, results: Dict[str, Any]):
        """Display comprehensive loading summary"""
        
        summary = results["loading_summary"]
        loading = results["loading_results"]
        verification = results["verification_results"]
        
        print(f"\n🎉 LOADING COMPLETED!")
        print("=" * 60)
        
        print(f"⏱️ PERFORMANCE:")
        print(f"   → Duration: {summary['duration_seconds']:.1f} seconds")
        print(f"   → Products processed: {summary['products_processed']}")
        print(f"   → Batches processed: {summary['batches_processed']}")
        print(f"   → Processing rate: {summary['products_processed'] / summary['duration_seconds']:.1f} products/sec")
        
        print(f"\n📊 LOADING RESULTS:")
        print(f"   → Successful loads: {loading['successful_loads']}")
        print(f"   → Failed loads: {loading['failed_loads']}")
        print(f"   → Retry operations: {loading['retry_count']}")
        print(f"   → Success rate: {(loading['successful_loads'] / (loading['successful_loads'] + loading['failed_loads']) * 100):.1f}%")
        
        print(f"\n✅ VERIFICATION:")
        print(f"   → Database total: {verification['total_in_database']} products")
        print(f"   → Sample check: {'✅ PASSED' if verification['verification_passed'] else '❌ FAILED'}")
        
        if verification.get("missing_barcodes"):
            print(f"   → Missing products: {len(verification['missing_barcodes'])}")
        
        print(f"\n🎯 NEXT STEPS:")
        if loading["failed_loads"] == 0 and verification["verification_passed"]:
            print(f"   ✅ All products loaded successfully")
            print(f"   🤖 Ready for Telegram bot integration")
            print(f"   📊 Database contains {verification['total_in_database']} products")
        else:
            print(f"   ⚠️ Review failed loads and missing products")
            print(f"   🔄 Consider re-running failed batches")
            print(f"   📋 Check logs for detailed error information")
        
        print("=" * 60)


def find_latest_validated_products_file(data_dir: Path = None) -> Optional[Path]:
    """Find the most recent validated_products_*.json file"""
    
    if data_dir is None:
        # Default search locations
        search_paths = [
            Path(__file__).resolve().parents[3] / "data_engineering" / "data" / "processed" / "validated",
            Path(__file__).resolve().parents[3] / "data" / "processed" / "validated",
            Path.cwd() / "data_engineering" / "data" / "processed" / "validated"
        ]
    else:
        search_paths = [Path(data_dir)]
    
    latest_file = None
    latest_time = 0
    
    for search_path in search_paths:
        if not search_path.exists():
            continue
        
        pattern = "validated_products_*.json"
        for file_path in search_path.glob(pattern):
            file_time = file_path.stat().st_mtime
            if file_time > latest_time:
                latest_time = file_time
                latest_file = file_path
    
    return latest_file


def main():
    """Main execution function"""
    
    import argparse
    
    parser = argparse.ArgumentParser(description="Load validated products to Supabase")
    parser.add_argument("--file", type=str, help="Path to validated_products JSON file")
    parser.add_argument("--environment", choices=["test", "prod"], default="test", 
                        help="Supabase environment (default: test)")
    parser.add_argument("--batch-size", type=int, default=50, 
                        help="Batch size for loading (default: 50)")
    parser.add_argument("--auto-find", action="store_true", 
                        help="Automatically find latest validated_products file")
    
    args = parser.parse_args()
    
    # Determine input file
    if args.file:
        input_file = Path(args.file)
    elif args.auto_find:
        input_file = find_latest_validated_products_file()
        if not input_file:
            print("❌ No validated_products_*.json file found")
            print("💡 Try specifying --file path/to/validated_products_*.json")
            return 1
        print(f"🔍 Auto-found: {input_file}")
    else:
        print("❌ Please specify --file or use --auto-find")
        return 1
    
    if not input_file.exists():
        print(f"❌ File not found: {input_file}")
        return 1
    
    # Initialize loader and run
    loader = ProductsToSupabaseLoader(
        environment=args.environment,
        batch_size=args.batch_size
    )
    
    results = loader.load_from_file(input_file)
    
    if results["success"]:
        print(f"\n🎉 Loading completed successfully!")
        return 0
    else:
        print(f"\n❌ Loading failed: {results.get('error', 'Unknown error')}")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)