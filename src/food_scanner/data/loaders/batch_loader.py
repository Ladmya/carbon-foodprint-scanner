"""
src/food_scanner/data/loaders/batch_loader.py
BATCH LOADING LOGIC: Handle batching, retries, and error recovery

This module manages the actual loading of products to Supabase in batches,
with proper error handling, retry logic, and progress tracking.
"""

import time
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime
from supabase import Client


def debug_paths():
    """Debug function to check paths after file relocation"""
    print("🔍 DEBUG: Checking paths after batch_loader.py relocation")
    print(f"   📁 Script location: {Path(__file__).resolve()}")
    print(f"   🏠 Project root: {Path(__file__).resolve().parents[3]}")
    print(f"   📦 Src path: {Path(__file__).resolve().parents[3] / 'src'}")
    print(f"   🗄️  Supabase client: {Path(__file__).resolve().parents[3] / 'src' / 'food_scanner' / 'infrastructure' / 'database' / 'repositories' / 'supabase_client.py'}")
    
    # Check if supabase_client.py exists
    supabase_path = Path(__file__).resolve().parents[3] / 'src' / 'food_scanner' / 'infrastructure' / 'database' / 'repositories' / 'supabase_client.py'
    print(f"   ✅ Supabase client exists: {supabase_path.exists()}")
    
    # Check relative paths from current location
    print(f"   📂 Current file: {Path(__file__).name}")
    print(f"   📂 Parent directory: {Path(__file__).parent.name}")
    print(f"   📂 Grandparent directory: {Path(__file__).parent.parent.name}")
    print("=" * 60)


class ProductBatchLoader:
    """
    Handles batch loading of products to Supabase with error recovery
    
    RESPONSIBILITIES:
    1. Split products into manageable batches
    2. Execute batch UPSERT operations
    3. Handle errors and retry failed batches
    4. Track progress and performance metrics
    """
    
    def __init__(self, supabase_client: Client, batch_size: int = 50, max_retries: int = 3):
        # Debug paths on initialization, to comment out when not needed
        debug_paths()
        
        self.supabase = supabase_client
        self.batch_size = batch_size
        self.max_retries = max_retries
        
        # Statistics tracking
        self.stats = {
            "batches_processed": 0,
            "successful_loads": 0,
            "failed_loads": 0,
            "retry_count": 0,
            "total_duration": 0,
            "errors": []
        }
    
    def load_products_in_batches(
        self, 
        products: List[Dict[str, Any]], 
        monitor: Optional[Any] = None
    ) -> Dict[str, Any]:
        """
        Main method to load products in batches with error handling
        
        Args:
            products: List of product records ready for Supabase
            monitor: Optional LoadingMonitor for progress tracking
            
        Returns:
            Dict with loading results and statistics
        """
        start_time = time.time()
        
        # Create batches
        batches = self._create_batches(products)
        total_batches = len(batches)
        
        print(f"   📦 Created {total_batches} batches of {self.batch_size} products each")
        print(f"   🔄 Starting batch loading with UPSERT strategy...")
        
        if monitor:
            monitor.start_loading(total_products=len(products), total_batches=total_batches)
        
        # Process each batch
        for batch_index, batch in enumerate(batches, 1):
            batch_start_time = time.time()
            
            print(f"\n   📤 Batch {batch_index}/{total_batches} ({len(batch)} products)", end="")
            
            try:
                # Attempt to load batch
                success = self._load_batch_with_retry(batch, batch_index)
                
                batch_duration = time.time() - batch_start_time
                
                if success:
                    self.stats["successful_loads"] += len(batch)
                    print(f" ✅ Success ({batch_duration:.1f}s)")
                else:
                    self.stats["failed_loads"] += len(batch)
                    print(f" ❌ Failed after {self.max_retries} retries")
                
                self.stats["batches_processed"] += 1
                
                if monitor:
                    monitor.update_progress(batch_index, len(batch), success, batch_duration)
                
                # Small delay to avoid overwhelming Supabase
                time.sleep(0.1)
                
            except Exception as e:
                self.stats["failed_loads"] += len(batch)
                self.stats["errors"].append({
                    "batch_index": batch_index,
                    "error": str(e),
                    "timestamp": datetime.now().isoformat()
                })
                print(f" 💥 Exception: {str(e)[:50]}...")
        
        self.stats["total_duration"] = time.time() - start_time
        
        if monitor:
            monitor.finish_loading()
        
        return self.stats.copy()
    
    def _create_batches(self, products: List[Dict[str, Any]]) -> List[List[Dict[str, Any]]]:
        """Split products into batches of specified size"""
        
        batches = []
        for i in range(0, len(products), self.batch_size):
            batch = products[i:i + self.batch_size]
            batches.append(batch)
        
        return batches
    
    def _load_batch_with_retry(self, batch: List[Dict[str, Any]], batch_index: int) -> bool:
        """Load a single batch with retry logic"""
        
        for attempt in range(self.max_retries + 1):
            try:
                # Attempt batch UPSERT
                result = self._execute_batch_upsert(batch)
                
                if result:
                    if attempt > 0:
                        self.stats["retry_count"] += 1
                        print(f" (retry {attempt})", end="")
                    return True
                
            except Exception as e:
                if attempt < self.max_retries:
                    # Retry with exponential backoff
                    wait_time = (2 ** attempt) * 0.5  # 0.5s, 1s, 2s
                    print(f" (retry {attempt + 1} in {wait_time}s)", end="")
                    time.sleep(wait_time)
                    continue
                else:
                    # Final failure - log and continue
                    self.stats["errors"].append({
                        "batch_index": batch_index,
                        "error": str(e),
                        "attempt": attempt + 1,
                        "timestamp": datetime.now().isoformat()
                    })
                    raise e
        
        return False
    
    def _execute_batch_upsert(self, batch: List[Dict[str, Any]]) -> bool:
        """Execute the actual UPSERT operation for a batch"""
        
        try:
            # Use Supabase UPSERT with conflict resolution on barcode
            result = self.supabase.table('products').upsert(
                batch,
                on_conflict='barcode',  # Update if barcode exists
                count='exact'
            ).execute()
            
            # Check if operation was successful
            if result.data is not None:
                return True
            else:
                return False
                
        except Exception as e:
            # Log specific error for debugging
            error_msg = str(e)
            
            # Handle common Supabase errors
            if "duplicate key" in error_msg.lower():
                raise ValueError(f"Duplicate key error in batch: {error_msg}")
            elif "constraint" in error_msg.lower():
                raise ValueError(f"Constraint violation in batch: {error_msg}")
            elif "timeout" in error_msg.lower():
                raise TimeoutError(f"Database timeout: {error_msg}")
            else:
                raise Exception(f"Database error: {error_msg}")
    
    def get_failed_products(self) -> List[str]:
        """Get list of barcodes for products that failed to load"""
        
        failed_barcodes = []
        
        for error in self.stats["errors"]:
            if "batch_index" in error:
                # Would need to track which products were in failed batches
                # This is a simplified implementation
                pass
        
        return failed_barcodes
    
    def get_performance_metrics(self) -> Dict[str, Any]:
        """Get detailed performance metrics"""
        
        total_products = self.stats["successful_loads"] + self.stats["failed_loads"]
        
        if total_products == 0:
            return {"error": "No products processed"}
        
        return {
            "total_products_processed": total_products,
            "successful_loads": self.stats["successful_loads"],
            "failed_loads": self.stats["failed_loads"],
            "success_rate": (self.stats["successful_loads"] / total_products) * 100,
            "batches_processed": self.stats["batches_processed"],
            "retry_operations": self.stats["retry_count"],
            "total_duration_seconds": self.stats["total_duration"],
            "products_per_second": total_products / self.stats["total_duration"] if self.stats["total_duration"] > 0 else 0,
            "average_batch_time": self.stats["total_duration"] / self.stats["batches_processed"] if self.stats["batches_processed"] > 0 else 0,
            "error_count": len(self.stats["errors"])
        }
    
    def retry_failed_batches(self, products: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Retry loading products that previously failed"""
        
        print(f"\n🔄 RETRYING FAILED PRODUCTS")
        print(f"   → Attempting to load {len(products)} previously failed products")
        
        # Reset error tracking for retry
        original_errors = self.stats["errors"].copy()
        self.stats["errors"] = []
        
        # Load with smaller batch size for better success rate
        original_batch_size = self.batch_size
        self.batch_size = min(10, self.batch_size)  # Smaller batches for retry
        
        try:
            results = self.load_products_in_batches(products)
            
            print(f"   → Retry completed: {results['successful_loads']} succeeded, {results['failed_loads']} failed")
            
            return results
        
        finally:
            # Restore original settings
            self.batch_size = original_batch_size
            # Combine errors
            self.stats["errors"] = original_errors + self.stats["errors"]


class BatchValidationError(Exception):
    """Custom exception for batch validation errors"""
    pass


class SupabaseConnectionError(Exception):
    """Custom exception for Supabase connection issues"""
    pass