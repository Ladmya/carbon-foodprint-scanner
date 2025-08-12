"""
src/food_scanner/data/transformers/transformation_orchestrator.py

"""

from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple

from food_scanner.data.data_backup_manager import DataBackupManager
from food_scanner.data.transformers.cache.smart_deduplicator import SmartDeduplicator
from food_scanner.data.transformers.cache.cache_config import CacheConfig
from food_scanner.data.transformers.validation import ProductValidator
from food_scanner.data.transformers.cleaning import ProductCleanerProcessor
from food_scanner.data.transformers.calculation import DerivedFieldsCalculator
from food_scanner.data.transformers.processing import MetadataProcessor
from food_scanner.data.transformers.validation import QualityChecker  
from food_scanner.data.transformers.analysis import TransformationAnalyzer
from food_scanner.data.transformers.reporting import TransformationReporter


class RobustProductTransformer:
    """
    PURE ORCHESTRATOR: Coordinates transformation pipeline using specialized modules

    RESPONSIBILITY: 
    - Orchestrate the complete transformation pipeline
    - Delegate ALL business logic to specialized modules
    - Assemble final results from module outputs
    - NO business logic in orchestrator itself
    
    FEATURES:
    - Backup and deduplication with differentiated TTL
    - Business validation rules
    - Modular cleaning pipeline using specialized components
    - Derived fields calculation (transport equivalents, impact levels)
    - Metadata processing (system timestamps, transformation version, collection timestamp)
    - Quality checks (database readiness, critical field completeness, data integrity)
    - Comprehensive monitoring and reporting
    - Graceful fallback (never fails the pipeline)
    - Same behavior on first vs subsequent executions
    """
    
    def __init__(self, cache_config: Optional[CacheConfig] = None):
        # Default configuration if not provided
        if cache_config is None:
            cache_dir = Path(__file__).resolve().parents[3] / "data" / "cache"
            cache_config = CacheConfig(
                enable_backup=True,
                enable_deduplication=True,
                backup_dir=cache_dir,
                deduplication_strategy="time_based",
                deduplication_window_hours=24,
                validated_ttl_hours=90 * 24,  
                rejected_ttl_hours=7 * 24,   
                enable_monitoring=True                  
            )
        
        self.config = cache_config

        # Initialize all specialized modules        
        # Cache and backup components
        self.backup_manager = DataBackupManager(cache_config.backup_dir) if cache_config.enable_backup else None
        self.deduplicator = SmartDeduplicator(cache_config) if cache_config.enable_deduplication else None       
        # Processing components (NEW - using modular approach)
        self.validator = ProductValidator()
        self.cleaner = ProductCleanerProcessor()
        self.derived_calculator = DerivedFieldsCalculator()
        self.metadata_processor = MetadataProcessor()
        self.quality_checker = QualityChecker()
        self.analysis = TransformationAnalyzer()
        self.reporter = TransformationReporter()

        # Orchestrator only tracks delegation, no business logic
        self.stats = {
            "start_time": None,
            "end_time": None,
            "modules_executed": 0,
            "delegation_successful": True
        }
    
    def transform_extracted_products(self, extracted_products: Dict[str, Any], collection_timestamp: str = None) -> Dict[str, Any]:
        """
        PURE ORCHESTRATION: Delegate all transformation steps to specialized modules: extracted products → production-ready products
        
        Args:
            extracted_products: Output from ProductExtractor
            collection_timestamp: ISO timestamp for this collection run
            
        Returns:
            Complete transformation results from all modules
        """
        if collection_timestamp is None:
            collection_timestamp = datetime.now().isoformat()
        
        self.orchestration_stats["start_time"] = collection_timestamp
        original_count = len(extracted_products)
        
        print(f"🔧 ROBUST PRODUCT TRANSFORMATION PIPELINE")
        print(f"   → Processing {original_count} extracted products")
        print(f"   → Delegating to: {self._count_active_modules()} specialized modules")
        print(f"   → Cache strategy: deduplication: {self.config.deduplication_strategy}")
        print(f"   → Collection timestamp: {collection_timestamp}")
        print(f"   → Backup enabled: {self.config.enable_backup}")
        print(f"   → TTL config: {self.config.validated_ttl_hours//24}d validated, {self.config.rejected_ttl_hours//24}d rejected")
        print("=" * 70)
        
        try:            
            # Phase 1: Delegate to BackupManager for Local backup (optional, never fails)
            print(f"   📁 Phase 1: Delegating to BackupManager for data backup and persistence...")
            if self.backup_manager:
                backup_result = self.backup_manager.save_extraction_batch(extracted_products, collection_timestamp)
                self.orchestration_stats["backup_result"] = backup_result
                self.orchestration_stats["modules_executed"] += 1
                
            # Phase 2: Delegate to Deduplicator for Smart deduplication with TTL cache (optional, uniform behavior)  
            print(f"   🔍 Phase 2: Delegating to Deduplicator for Smart deduplication with TTL cache...")
            if self.deduplicator:
                deduplicated_products = self.deduplicator.remove_duplicates(extracted_products, collection_timestamp)
                self.orchestration_stats["modules_executed"] += 1
            else:
                deduplicated_products = extracted_products
                self.orchestration_stats["modules_executed"] += 1
                
            # Phase 3: Delegate to ProductValidator for Business validation
            print(f"   ✅ Phase 3: Delegating to ProductValidator for Business validation against critical rules...")
            validated_products, rejected_products = self.validator.validate_products(deduplicated_products)
            self._update_cache_status_after_validation(validated_products, rejected_products, collection_timestamp)
            self.orchestration_stats["modules_executed"] += 1
            
            # Phase 4: Delegate to ProductCleanerProcessor for Data cleaning and normalization
            print(f"   🧹 Phase 4: Delegating to ProductCleanerProcessor for Data cleaning and normalization...")
            cleaned_products = self.cleaner.delegate_cleaning_to_processor(validated_products)
            self.orchestration_stats["modules_executed"] += 1
            
            # Phase 5: Delegate to DerivedFieldsCalculator for Calculate derived fields
            print(f"   📊 Phase 5: Delegating to DerivedFieldsCalculator for Calculate derived fields and transport equivalents...")
            enhanced_products = self.derived_calculator.calculate_derived_fields(cleaned_products)
            self.orchestration_stats["modules_executed"] += 1
            
            # Phase 6: Delegate to MetadataProcessor for Add metadata and final quality checks
            print(f"   🏷️ Phase 6: Delegating to MetadataProcessor for Add metadata and perform final quality checks...")
            products_with_metadata = self.metadata_processor.add_system_metadata(enhanced_products, collection_timestamp)
            self.orchestration_stats["modules_executed"] += 1
            
            # Phase 7: Delegate to QualityChecker for Final quality checks
            print(f"   🔍 Phase 7: Delegating to QualityChecker for Final quality checks...")
            final_products = self.quality_checker.perform_final_quality_checks(products_with_metadata)
            self.orchestration_stats["modules_executed"] += 1

            # Save rejected products for retry(delegate to BackupManager)
            if self.backup_manager and rejected_products:
                self.backup_manager.save_rejected_products_for_retry(rejected_products, collection_timestamp)

            # Phase 8: Delegate analysis to TransformationAnalyzer
            print(f"   📈 Phase 8: Delegating analysis to TransformationAnalyzer...")
            quality_validation = self.analyzer.validate_transformation_quality(final_products)
            production_readiness = self.analyzer.assess_production_readiness(final_products, rejected_products)
            rejection_analysis = self.analyzer.analyze_rejection_patterns(rejected_products)
            self.orchestration_stats["modules_executed"] += 1

            self.orchestration_stats["end_time"] = datetime.now().isoformat()
            
            # Orchestrator ONLY assembles results from modules (no business logic)
            results = self.assemble_final_results(
                backup_result=backup_result,
                original_count=original_count,
                final_products=final_products,
                rejected_products=rejected_products,
                quality_validation=quality_validation,
                production_readiness=production_readiness,
                rejection_analysis=rejection_analysis,
            )
            
            # Phase 9: Delegate reporting to TransformationReporter
            print(f"   📊 Phase 9: Delegating reporting to TransformationReporter...")
            self.reporter.display_transformation_summary(results)
            self.orchestration_stats["modules_executed"] += 1
            
            return results

        except Exception as e:
            self.orchestration_stats["delegation_successful"] = False
            print(f"🚨 ORCHESTRATION FAILED DURING DELEGATION: {str(e)}")
            raise 
    
    def _delegate_cleaning_to_processor(self, validated_products: Dict[str, Any]) -> Dict[str, Any]:
        """Delegate cleaning to ProductCleanerProcessor"""
        cleaned_products = {}
        
        for barcode, product_data in validated_products.items():
            extracted_fields = product_data.get("extracted_fields", {})
            
            # Delegate cleaning to cleaner processor
            cleaned_data = self.cleaner.process_product_data(extracted_fields, barcode)
            
            # Format the cleaned data for the next pipeline stage
            cleaned_products[barcode] = {
                "transformed_data": cleaned_data,
                "raw_data_backup": product_data,
                "validation_passed": True,
                "transformation_timestamp": datetime.now().isoformat()
            }
       
        return cleaned_products

    def _update_cache_status_after_validation(self, validated_products: Dict[str, Any], rejected_products: Dict[str, Any], timestamp: str):
        """Update cache status based on validation results (delegate to deduplicator)"""
        if not self.deduplicator:
            return
        
        # Update validated products status
        for barcode in validated_products.keys():
            self.deduplicator.update_product_status(barcode, "validated", timestamp)
        
        # Update rejected products status  
        for barcode in rejected_products.keys():
            self.deduplicator.update_product_status(barcode, "rejected", timestamp)
    
    def _assemble_final_results(self, **kwargs) -> Dict[str, Any]:
        """
        PURE ASSEMBLY: Collect results from all modules without business logic
        
        Args:
            **kwargs: Results from all specialized modules
            
        Returns:
            Assembled comprehensive results
        """
        return {
            "validated_products": kwargs["final_products"],
            "rejected_products": kwargs["rejected_products"],
            "transformation_stats": self._collect_transformation_stats(kwargs["original_count"]),
            "cache_stats": self.deduplicator.get_cache_stats() if self.deduplicator else {},
            "quality_validation": kwargs["quality_validation"],
            "production_readiness": kwargs["production_readiness"],
            "rejection_analysis": kwargs["rejection_analysis"],
            "data_quality_report": self._collect_data_quality_report(),
            "orchestration_info": self.orchestration_stats.copy()
        }

    def _collect_transformation_stats(self, original_count: int) -> Dict[str, Any]:
        """Collect statistics from all modules (no calculations, pure collection)"""
        return {
            "pipeline_timing": {
                "start_time": self.orchestration_stats["start_time"],
                "end_time": self.orchestration_stats["end_time"],
                "modules_executed": self.orchestration_stats["modules_executed"]
            },
            "processing_flow": {
                "original_products": original_count,
                "duplicate_products": original_count - len(self.validator.get_validation_stats().get("total_products_processed", 0)) if hasattr(self, 'validator') else 0,
                "successful_transformations": self.validator.get_validation_stats().get("successful_validations", 0),
                "rejected_products": self.validator.get_validation_stats().get("failed_validations", 0),
                "backup_saved": self.backup_manager is not None
            },
            "validation_details": self.validator.get_validation_stats(),
            "cleaning_details": self.cleaner.get_cleaning_stats(),
            "derived_fields_details": self.derived_calculator.get_calculation_stats(),
            "metadata_details": self.metadata_processor.get_metadata_stats(),
            "quality_check_details": self.quality_checker.get_quality_stats()
        }
    
    def _collect_data_quality_report(self) -> Dict[str, Any]:
        """Collect data quality information from all modules"""
        return {
            "validation_issues": self.validator.get_products_missing_co2(),
            "cleaning_issues": self.cleaner.get_data_quality_issues(),
            "quality_check_issues": self.quality_checker.get_quality_issues(),
            "brand_cleaning_details": self.cleaner.get_brand_cleaning_stats(),
            "analyzer_issues": self.analyzer.get_quality_issues(),
            "analyzer_anomalies": self.analyzer.get_anomalies()
        }
    
    def _count_active_modules(self) -> int:
        """Count active modules for orchestration info"""
        active_count = 0
        if self.backup_manager: active_count += 1
        if self.deduplicator: active_count += 1
        active_count += 7  # validator, cleaner, derived_calculator, metadata_processor, quality_checker, analyzer, reporter
        return active_count

    def get_rejected_products_for_retry(self) -> List[str]:
        """Delegate to Deduplicator to retrieve products for re-testing"""
        rejected_barcodes = []
        
        # Method 1: Via the deduplication cache(delegate to deduplicator)
        if self.deduplicator:
            rejected_barcodes.extend(self.deduplicator.get_rejected_products_for_retry_deduplication())
        
        # Method 2: Via the backup(delegate to backup manager)
        if self.backup_manager:
            backup_rejected = self.backup_manager.get_rejected_products_for_retry_backup()
            rejected_barcodes.extend(backup_rejected)
        
        # Deduplicate the list
        return list(set(rejected_barcodes))

    def generate_comprehensive_reports(self, transformation_results: Dict[str, Any], output_dir: Optional[Path] = None) -> Dict[str, Path]:
        """Delegate report generation to TransformationReporter"""
        if output_dir:
            reporter = TransformationReporter(output_dir)
        else:
            reporter = self.reporter
        
        return reporter.generate_all_reports(transformation_results)

# Convenience function for integration
def transform_products_for_production(extracted_products: Dict[str, Any], cache_config: Optional[CacheConfig] = None) -> Dict[str, Any]:
    """
    Convenience function: Delegate to RobustProductTransformer to transform extracted products for production use with robust caching
    
    Usage:
        extraction_results = await extractor.run_complete_extraction()
        transformation_results = transform_products_for_production(
            extraction_results["extracted_products"]
        )
    """
    orchestrator = RobustProductTransformer(cache_config)
    return orchestrator.transform_extracted_products(extracted_products)