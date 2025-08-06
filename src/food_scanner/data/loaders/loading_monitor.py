"""
scripts/loading/loading_monitor.py
LOADING MONITOR: Progress tracking and performance metrics

This module provides real-time monitoring of the loading process with
progress bars, ETA calculations, and performance metrics.
"""

from datetime import datetime, timedelta
from typing import Dict, Any


class LoadingMonitor:
    """
    Monitor loading progress and collect performance metrics
    
    RESPONSIBILITIES:
    1. Display real-time progress information
    2. Calculate ETA and performance metrics
    3. Track success/failure rates per batch
    4. Generate final performance report
    """
    
    def __init__(self):
        self.start_time = None
        self.end_time = None
        self.total_products = 0
        self.total_batches = 0
        self.processed_batches = 0
        self.processed_products = 0
        self.successful_products = 0
        self.failed_products = 0
        
        # Performance tracking
        self.batch_times = []
        self.batch_sizes = []
        self.batch_success_rates = []
        
        # Progress tracking
        self.last_update_time = None
        self.products_per_second_history = []
    
    def start_loading(self, total_products: int, total_batches: int):
        """Initialize monitoring for a loading session"""
        
        self.start_time = datetime.now()
        self.total_products = total_products
        self.total_batches = total_batches
        self.processed_batches = 0
        self.processed_products = 0
        self.successful_products = 0
        self.failed_products = 0
        
        print(f"\n📊 LOADING PROGRESS MONITOR STARTED")
        print(f"   → Total products: {total_products}")
        print(f"   → Total batches: {total_batches}")
        print(f"   → Started at: {self.start_time.strftime('%H:%M:%S')}")
    
    def update_progress(
        self, 
        batch_number: int, 
        batch_size: int, 
        batch_success: bool, 
        batch_duration: float
    ):
        """Update progress after each batch completion"""
        
        self.processed_batches += 1
        self.processed_products += batch_size
        
        if batch_success:
            self.successful_products += batch_size
        else:
            self.failed_products += batch_size
        
        # Track performance metrics
        self.batch_times.append(batch_duration)
        self.batch_sizes.append(batch_size)
        self.batch_success_rates.append(100 if batch_success else 0)
        
        # Calculate current performance
        current_time = datetime.now()
        elapsed_time = (current_time - self.start_time).total_seconds()
        
        if elapsed_time > 0:
            current_rate = self.processed_products / elapsed_time
            self.products_per_second_history.append(current_rate)
        
        # Display progress every 5 batches or every 10 seconds
        if (self.processed_batches % 5 == 0 or 
            (self.last_update_time and (current_time - self.last_update_time).total_seconds() > 10)):
            
            self._display_progress_update(current_time, elapsed_time)
            self.last_update_time = current_time
    
    def _display_progress_update(self, current_time: datetime, elapsed_time: float):
        """Display current progress information"""
        
        # Calculate progress percentages
        batch_progress = (self.processed_batches / self.total_batches) * 100
        product_progress = (self.processed_products / self.total_products) * 100
        
        # Calculate ETA
        if self.processed_batches > 0:
            avg_batch_time = sum(self.batch_times) / len(self.batch_times)
            remaining_batches = self.total_batches - self.processed_batches
            eta_seconds = remaining_batches * avg_batch_time
            eta = current_time + timedelta(seconds=eta_seconds)
        else:
            eta = None
        
        # Calculate current rate
        current_rate = self.processed_products / elapsed_time if elapsed_time > 0 else 0
        
        # Success rate
        success_rate = (self.successful_products / self.processed_products * 100) if self.processed_products > 0 else 0
        
        print(f"\n   📈 Progress Update:")
        print(f"      Batches: {self.processed_batches}/{self.total_batches} ({batch_progress:.1f}%)")
        print(f"      Products: {self.processed_products}/{self.total_products} ({product_progress:.1f}%)")
        print(f"      Success rate: {success_rate:.1f}%")
        print(f"      Current rate: {current_rate:.1f} products/sec")
        
        if eta:
            print(f"      ETA: {eta.strftime('%H:%M:%S')} (in {eta_seconds/60:.1f} min)")
        
        # Progress bar
        self._display_progress_bar(product_progress)
    
    def _display_progress_bar(self, progress_percent: float, width: int = 40):
        """Display a simple progress bar"""
        
        filled_width = int(width * progress_percent / 100)
        bar = "█" * filled_width + "░" * (width - filled_width)
        print(f"      [{bar}] {progress_percent:.1f}%")
    
    def finish_loading(self):
        """Complete the loading session and display final metrics"""
        
        self.end_time = datetime.now()
        total_duration = (self.end_time - self.start_time).total_seconds()
        
        print(f"\n📊 LOADING COMPLETED")
        print(f"   → Finished at: {self.end_time.strftime('%H:%M:%S')}")
        print(f"   → Total duration: {total_duration:.1f} seconds")
        print(f"   → Final success rate: {(self.successful_products / self.processed_products * 100):.1f}%")
    
    def get_final_metrics(self) -> Dict[str, Any]:
        """Get comprehensive final metrics"""
        
        if not self.start_time or not self.end_time:
            return {"error": "Loading session not completed"}
        
        total_duration = (self.end_time - self.start_time).total_seconds()
        
        metrics = {
            "session_info": {
                "start_time": self.start_time.isoformat(),
                "end_time": self.end_time.isoformat(),
                "total_duration_seconds": total_duration,
                "total_duration_formatted": str(timedelta(seconds=int(total_duration)))
            },
            "volume_metrics": {
                "total_products": self.total_products,
                "processed_products": self.processed_products,
                "successful_products": self.successful_products,
                "failed_products": self.failed_products,
                "total_batches": self.total_batches,
                "processed_batches": self.processed_batches
            },
            "performance_metrics": {
                "overall_success_rate": (self.successful_products / self.processed_products * 100) if self.processed_products > 0 else 0,
                "products_per_second": self.processed_products / total_duration if total_duration > 0 else 0,
                "average_batch_duration": sum(self.batch_times) / len(self.batch_times) if self.batch_times else 0,
                "fastest_batch_duration": min(self.batch_times) if self.batch_times else 0,
                "slowest_batch_duration": max(self.batch_times) if self.batch_times else 0,
                "average_batch_size": sum(self.batch_sizes) / len(self.batch_sizes) if self.batch_sizes else 0
            },
            "quality_metrics": {
                "batch_success_rate": sum(self.batch_success_rates) / len(self.batch_success_rates) if self.batch_success_rates else 0,
                "failed_batches": len([rate for rate in self.batch_success_rates if rate == 0]),
                "successful_batches": len([rate for rate in self.batch_success_rates if rate > 0]),
                "data_consistency_score": self._calculate_consistency_score()
            }
        }
        
        return metrics
    
    def _calculate_consistency_score(self) -> float:
        """Calculate a consistency score based on performance variation"""
        
        if len(self.batch_times) < 2:
            return 100.0
        
        # Calculate coefficient of variation for batch times
        mean_time = sum(self.batch_times) / len(self.batch_times)
        variance = sum((t - mean_time) ** 2 for t in self.batch_times) / len(self.batch_times)
        std_dev = variance ** 0.5
        
        if mean_time == 0:
            return 100.0
        
        coefficient_of_variation = std_dev / mean_time
        
        # Convert to consistency score (lower variation = higher score)
        consistency_score = max(0, 100 - (coefficient_of_variation * 100))
        
        return consistency_score
    
    def get_performance_summary(self) -> str:
        """Get a human-readable performance summary"""
        
        metrics = self.get_final_metrics()
        
        if "error" in metrics:
            return "Performance metrics not available"
        
        volume = metrics["volume_metrics"]
        performance = metrics["performance_metrics"]
        
        summary = f"""
🎯 LOADING PERFORMANCE SUMMARY
════════════════════════════════════════════════════════════════════
📊 Volume: {volume['successful_products']}/{volume['total_products']} products loaded successfully
⏱️ Duration: {metrics['session_info']['total_duration_formatted']}
🚀 Speed: {performance['products_per_second']:.1f} products/second
✅ Success Rate: {performance['overall_success_rate']:.1f}%
📦 Batches: {volume['processed_batches']} batches processed
⚡ Average Batch Time: {performance['average_batch_duration']:.2f} seconds
════════════════════════════════════════════════════════════════════
        """
        
        return summary.strip()
    
    def save_metrics_to_file(self, output_path: str):
        """Save detailed metrics to JSON file"""
        
        import json
        
        metrics = self.get_final_metrics()
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(metrics, f, indent=2, ensure_ascii=False, default=str)
        
        print(f"📊 Performance metrics saved to: {output_path}")


class ProgressDisplay:
    """Simple progress display utility"""
    
    @staticmethod
    def show_spinner(message: str, duration: float = 1.0):
        """Show a simple spinner for short operations"""
        
        import sys
        import time
        
        spinner_chars = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"
        
        start_time = time.time()
        i = 0
        
        while time.time() - start_time < duration:
            sys.stdout.write(f"\r{message} {spinner_chars[i % len(spinner_chars)]}")
            sys.stdout.flush()
            time.sleep(0.1)
            i += 1
        
        sys.stdout.write(f"\r{message} ✅\n")
        sys.stdout.flush()
    
    @staticmethod
    def show_progress_bar(current: int, total: int, prefix: str = "", suffix: str = "", length: int = 50):
        """Show a detailed progress bar"""
        
        percent = current / total if total > 0 else 0
        filled_length = int(length * percent)
        
        bar = "█" * filled_length + "░" * (length - filled_length)
        
        print(f"\r{prefix} [{bar}] {percent*100:.1f}% {suffix}", end="", flush=True)
        
        if current >= total:
            print()  # New line when complete