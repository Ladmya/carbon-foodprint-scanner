"""
src/food_scanner/data/transformers/product_transformer_final.py
PRODUCT TRANSFORMER: Business logic validation and data transformation 
Converts extracted products to production-ready database records
"""

import re
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from pathlib import Path


# Add src to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parents[4] / "src"))

from food_scanner.data.utils.duplicate_handler import DuplicateHandler
from food_scanner.data.utils.brand_name_cleaner import BrandNameCleaner
from food_scanner.core.constants import CARBON_FACTORS


class  ProductTransformer:
    """
    RESPONSIBILITY: Transform extracted products into production-ready database records
    
    BUSINESS LOGIC:
    1. Apply validation rules (REJECT if critical fields missing)
    2. Clean and normalize data (weight to grams, text cleanup)
    3. Calculate derived fields (transport equivalents, impact levels)
    4. Handle duplicates across collection runs
    5. Generate transformation statistics and reports
    
    INPUT: extracted_products from ProductExtractor
    OUTPUT: validated_products ready for database insertion
    """
    
    def __init__(self, use_duplicate_handling: bool = True):
        self.use_duplicate_handling = use_duplicate_handling
        self.duplicate_handler = DuplicateHandler() if use_duplicate_handling else None
        self.brand_cleaner = BrandNameCleaner()

        # Transformation statistics
        self.stats = {
            "start_time": None,
            "end_time": None,
            "total_products_processed": 0,
            "successful_transformations": 0,
            "rejected_products": 0,
            "duplicate_products": 0,
            "validation_failures": {
                "missing_barcode": 0,
                "missing_product_name": 0,
                "missing_brand_name": 0,
                "missing_weight": 0,
                "missing_co2": 0,
                "missing_nutriscore": 0,
                "invalid_data": 0
            },
            "data_cleaning": {
                "brand_names_cleaned": 0,
                "product_names_cleaned": 0,
                "weights_normalized": 0,
                "units_normalized": 0,
                "brand_names_normalized": 0,
                "brand_consolidations": 0                
            },
            "calculated_fields": {
                "transport_equivalents_calculated": 0,
                "impact_levels_assigned": 0,
                "metadata_added": 0
            }
        }
        
        # Data quality tracking
        self.rejected_products = {}
        self.products_missing_co2 = []
        self.data_quality_issues = []
    
    def transform_extracted_products(
        self,
        extracted_products: Dict[str, Any],
        collection_timestamp: str = None
        ) -> Dict[str, Any]:
        """
        Main transformation pipeline: extracted products → production-ready products
        
        Args:
            extracted_products: Output from ProductExtractor.run_complete_extraction()
            collection_timestamp: ISO timestamp for this collection run
            
        Returns:
            Dict containing validated_products, rejected_products, and transformation_stats
        """
        if collection_timestamp is None:
            collection_timestamp = datetime.now().isoformat()
        
        self.stats["start_time"] = collection_timestamp
        
        print(f"🔧 PRODUCT TRANSFORMATION PIPELINE")
        print(f"   → Processing {len(extracted_products)} extracted products")
        print(f"   → Collection timestamp: {collection_timestamp}")
        print("=" * 60)
        
        # Phase 1: Handle duplicates if enabled
        if self.use_duplicate_handling:
            print(f"   🔍 Phase 1: Duplicate detection and handling...")
            extracted_products = self._handle_duplicates(extracted_products, collection_timestamp)
        
        # Phase 2: Apply business validation rules
        print(f"   ✅ Phase 2: Business validation and data cleaning...")
        validated_products, rejected_products = self._apply_business_validation(extracted_products)
        
        # Phase 3: Calculate derived fields
        print(f"   📊 Phase 3: Calculate derived fields...")
        validated_products = self._calculate_derived_fields(validated_products)
        
        # Phase 4: Final quality checks and metadata
        print(f"   🏷️ Phase 4: Add metadata and final quality checks...")
        validated_products = self._add_metadata_and_quality_checks(validated_products, collection_timestamp)
        
        self.stats["end_time"] = datetime.now().isoformat()
        
        # Generate results summary
        results = {
            "validated_products": validated_products,
            "rejected_products": rejected_products,
            "transformation_stats": self.stats.copy(),
            "products_missing_co2": self.products_missing_co2.copy(),
            "data_quality_issues": self.data_quality_issues.copy(),
            "production_readiness": self._assess_production_readiness(validated_products, rejected_products)
        }
        
        self._display_transformation_summary(results)
        return results
    
    def _handle_duplicates(self, extracted_products: Dict[str, Any], collection_timestamp: str) -> Dict[str, Any]:
        """Handle duplicates using DuplicateHandler"""
        
        # Convert extracted products to format expected by DuplicateHandler
        products_for_dedup = {}
        for barcode, product_data in extracted_products.items():
            extracted_fields = product_data.get("extracted_fields", {})
            
            products_for_dedup[barcode] = {
                "raw_discovery_data": {
                    "code": barcode,
                    "product_name": extracted_fields.get("product_name", ""),
                    "product_name_fr": extracted_fields.get("product_name", ""),
                    "brands": extracted_fields.get("brand_name", ""),
                    "product_quantity": f"{extracted_fields.get('weight', '')} {extracted_fields.get('product_quantity_unit', '')}".strip()
                },
                "discovered_via": {"extraction_timestamp": collection_timestamp}
            }
        
        # Process duplicates
        dedup_result = self.duplicate_handler.process_discovered_products(
            products_for_dedup, collection_timestamp
        )
        
        # Filter out duplicates from extracted_products
        new_product_barcodes = set(dedup_result["new_products"].keys())
        filtered_extracted_products = {
            barcode: product_data 
            for barcode, product_data in extracted_products.items()
            if barcode in new_product_barcodes
        }
        
        self.stats["duplicate_products"] = len(extracted_products) - len(filtered_extracted_products)
        
        print(f"      → Duplicates removed: {self.stats['duplicate_products']}")
        print(f"      → New products for processing: {len(filtered_extracted_products)}")
        
        return filtered_extracted_products
    
    def _apply_business_validation(self, extracted_products: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """Apply business validation rules and data cleaning"""
        
        validated_products = {}
        rejected_products = {}
        
        for barcode, product_data in extracted_products.items():
            self.stats["total_products_processed"] += 1
            
            extracted_fields = product_data.get("extracted_fields", {})
            success_flags = extracted_fields.get("extraction_success", {})
            
            # Apply validation rules
            validation_result = self._validate_product(barcode, extracted_fields, success_flags)
            
            if validation_result["is_valid"]:
                # Clean and normalize the data
                cleaned_product = self._clean_and_normalize_product(extracted_fields, barcode)
                
                validated_products[barcode] = {
                    "transformed_data": cleaned_product,
                    "raw_data_backup": product_data,
                    "validation_passed": True,
                    "transformation_timestamp": datetime.now().isoformat()
                }
                
                self.stats["successful_transformations"] += 1
            else:
                # Reject the product
                rejected_products[barcode] = {
                    "rejection_reasons": validation_result["rejection_reasons"],
                    "partial_data": extracted_fields,
                    "raw_data_backup": product_data,
                    "validation_passed": False,
                    "transformation_timestamp": datetime.now().isoformat()
                }
                
                self.stats["rejected_products"] += 1
                
                # Update validation failure stats
                for reason in validation_result["rejection_reasons"]:
                    if "barcode" in reason.lower():
                        self.stats["validation_failures"]["missing_barcode"] += 1
                    elif "product_name" in reason.lower():
                        self.stats["validation_failures"]["missing_product_name"] += 1
                    elif "brand_name" in reason.lower() or "brand" in reason.lower():
                        self.stats["validation_failures"]["missing_brand_name"] += 1
                    elif "weight" in reason.lower():
                        self.stats["validation_failures"]["missing_weight"] += 1
                    elif "co2" in reason.lower():
                        self.stats["validation_failures"]["missing_co2"] += 1
                    elif "nutriscore" in reason.lower():
                        self.stats["validation_failures"]["missing_nutriscore"] += 1
                    else:
                        self.stats["validation_failures"]["invalid_data"] += 1
        
        print(f"      → Products validated: {self.stats['successful_transformations']}")
        print(f"      → Products rejected: {self.stats['rejected_products']}")
        
        return validated_products, rejected_products
    
    def _validate_product(self, barcode: str, extracted_fields: Dict[str, Any], success_flags: Dict[str, bool]) -> Dict[str, Any]:
        """Apply business validation rules"""
        
        rejection_reasons = []
        
        # Rule 1: Barcode is required (primary key)
        if not success_flags.get("barcode", False) or not extracted_fields.get("barcode"):
            rejection_reasons.append("Missing or invalid barcode (required for primary key)")
        
        # Rule 2: Product name is required (critical for display)
        if not success_flags.get("product_name", False) or not extracted_fields.get("product_name"):
            rejection_reasons.append("Missing product name (required for bot display)")
        
        # Rule 3: Brand name is required (critical for display)  
        if not success_flags.get("brand_name", False) or not extracted_fields.get("brand_name"):
            rejection_reasons.append("Missing brand name (required for bot display)")
        
        # Rule 4: Weight and unit are required (useful for calculations)
        # Note: Making this optional since 86.2% success rate is good but not perfect
        if not success_flags.get("weight", False) or not extracted_fields.get("weight"):
            # Don't reject, but note for improvement
            pass
        
        # Rule 5: CO2 data is required (main functionality)
        if not success_flags.get("co2_total", False):
            co2_sources = extracted_fields.get("co2_sources", {})
            has_co2 = any(value is not None for value in co2_sources.values())
            if not has_co2:
                rejection_reasons.append("Missing CO2 data (required for carbon footprint functionality)")
                
                # Track for missing CO2 report
                self.products_missing_co2.append({
                    "barcode": barcode,
                    "product_name": extracted_fields.get("product_name", "Unknown"),
                    "brand_name": extracted_fields.get("brand_name", "Unknown"),
                    "extraction_timestamp": datetime.now().isoformat()
                })
        
        # Rule 6: At least one nutriscore field required
        has_nutriscore_grade = success_flags.get("nutriscore_grade", False)
        has_nutriscore_score = success_flags.get("nutriscore_score", False)
        
        if not has_nutriscore_grade and not has_nutriscore_score:
            rejection_reasons.append("Missing nutriscore data (need grade OR score)")
        
        return {
            "is_valid": len(rejection_reasons) == 0,
            "rejection_reasons": rejection_reasons
        }
    
    def _clean_and_normalize_product(self, extracted_fields: Dict[str, Any], barcode: str) -> Dict[str, Any]:
        """Clean and normalize product data"""
        
        cleaned_product = {
            "barcode": barcode,
            "product_name": self._clean_product_name(extracted_fields.get("product_name", "")),
            "brand_name": self._clean_brand_name(extracted_fields.get("brand_name", ""), barcode),
            "brand_tags": extracted_fields.get("brand_tags", []),
            "weight": self._normalize_weight(extracted_fields.get("weight")),
            "product_quantity_unit": self._normalize_unit(extracted_fields.get("product_quantity_unit", "")),
            "nutriscore_grade": self._normalize_nutriscore_grade(extracted_fields.get("nutriscore_grade")),
            "nutriscore_score": self._normalize_nutriscore_score(extracted_fields.get("nutriscore_score")),
            "eco_score": self._normalize_ecoscore(extracted_fields.get("eco_score")),
            "co2_total": self._extract_co2_value(extracted_fields.get("co2_sources", {}))
        }
        
        return cleaned_product
    
    def _clean_product_name(self, product_name: str) -> str:
        """Clean and normalize product name"""
        if not product_name:
            return ""
        
        # Basic cleaning
        cleaned = product_name.strip()
        
        # Remove excessive whitespace
        cleaned = re.sub(r'\s+', ' ', cleaned)
        
        # Limit length to reasonable size
        if len(cleaned) > 200:
            cleaned = cleaned[:197] + "..."
            self.data_quality_issues.append(f"Product name truncated: was {len(product_name)} chars")
        
        if cleaned != product_name:
            self.stats["data_cleaning"]["product_names_cleaned"] += 1
        
        return cleaned

    def _clean_brand_name(self, brand_name: str, barcode: str) -> str:
        """Clean and normalize brand name using BrandNameCleaner
        Add stats to track cleaning and normalization
        Args:
            brand_name: Raw brand name
            barcode: Product barcode
            
        Returns:
            Cleaned brand name
        """
        if not brand_name:
            return ""
    
        # Use the dedicated brand cleaner
        cleaned_brand, cleaning_details = self.brand_cleaner.clean_brand_name(brand_name)
    
        # Track cleaning statistics
        if cleaned_brand != brand_name:
            self.stats["data_cleaning"]["brand_names_cleaned"] += 1
        
            # Check if this was a normalization (case/accent change)
            if cleaning_details.get("normalized") or cleaning_details.get("canonical_mapped"):
                self.stats["data_cleaning"]["brand_names_normalized"] += 1
        
            # Check if this was a consolidation (primary brand extraction)
            if cleaning_details.get("primary_extracted"):
                self.stats["data_cleaning"]["brand_consolidations"] += 1
        
            # Log significant transformations
            if len(brand_name) > 30 or "," in brand_name:
                self.data_quality_issues.append(
                    f"Brand transformed for {barcode}: '{brand_name[:50]}...' → '{cleaned_brand}'"
                )
    
        return cleaned_brand
    

    
    def _normalize_weight(self, weight: Any) -> Optional[float]:
        """Normalize weight to grams"""
        if weight is None:
            return None
        
        try:
            normalized_weight = float(weight)
            
            # Validate range
            if 0.1 <= normalized_weight <= 10000:  # Reasonable range for food products
                self.stats["data_cleaning"]["weights_normalized"] += 1
                return round(normalized_weight, 3)
            else:
                self.data_quality_issues.append(f"Weight outside valid range: {normalized_weight}")
                return None
                
        except (ValueError, TypeError):
            self.data_quality_issues.append(f"Invalid weight value: {weight}")
            return None
    
    def _normalize_unit(self, unit: str) -> str:
        """Normalize unit to standard format"""
        if not unit:
            return "g"  # Default to grams
        
        unit_lower = unit.lower().strip()
        
        # Normalize to standard units
        if unit_lower in ['g', 'gr', 'gram', 'grams', 'gramme', 'grammes']:
            normalized = 'g'
        elif unit_lower in ['ml', 'millilitre', 'millilitres', 'milliliter', 'milliliters']:
            normalized = 'ml'
        elif unit_lower in ['kg', 'kilo', 'kilogram', 'kilogramme']:
            # Should have been converted to grams in extraction, but handle just in case
            normalized = 'g'
        elif unit_lower in ['l', 'litre', 'litres', 'liter', 'liters']:
            # Should have been converted to ml in extraction, but handle just in case
            normalized = 'ml'
        else:
            normalized = 'g'  # Default fallback
        
        if normalized != unit:
            self.stats["data_cleaning"]["units_normalized"] += 1
        
        return normalized
    
    def _normalize_nutriscore_grade(self, grade: Any) -> Optional[str]:
        """Normalize nutriscore grade"""
        if not grade:
            return None
        
        if isinstance(grade, str) and grade.upper() in ['A', 'B', 'C', 'D', 'E']:
            return grade.upper()
        
        return None
    
    def _normalize_nutriscore_score(self, score: Any) -> Optional[int]:
        """Normalize nutriscore score"""
        if score is None:
            return None
        
        try:
            score_int = int(score)
            if -15 <= score_int <= 40:
                return score_int
        except (ValueError, TypeError):
            pass
        
        return None
    
    def _normalize_ecoscore(self, ecoscore: Any) -> Optional[str]:
        """Normalize ecoscore"""
        if not ecoscore:
            return None
        
        if isinstance(ecoscore, str) and ecoscore.upper() in ['A', 'B', 'C', 'D', 'E']:
            return ecoscore.upper()
        
        return None
    
    def _extract_co2_value(self, co2_sources: Dict[str, Optional[float]]) -> Optional[float]:
        """Extract CO2 value using priority order"""
        
        # Priority order (same as extractor)
        priority_sources = [
            "agribalyse_total",
            "ecoscore_agribalyse_total", 
            "nutriments_carbon_footprint",
            "nutriments_known_ingredients"
        ]
        
        for source in priority_sources:
            value = co2_sources.get(source)
            if value is not None and 0 <= value <= 10000:
                return round(value, 3)
        
        return None
    
    def _calculate_derived_fields(self, validated_products: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate transport equivalents and impact levels"""
        
        for barcode, product_data in validated_products.items():
            transformed_data = product_data["transformed_data"]
            
            co2_total = transformed_data.get("co2_total")
            weight = transformed_data.get("weight")
            
            if co2_total is not None and weight is not None:
                # Calculate total CO2 for the entire product (co2_total is per 100g)
                total_co2_grams = (co2_total * weight) / 100.0
                
                # Calculate transport equivalents (km)
                transformed_data.update({
                    "co2_vehicle_km": round(total_co2_grams / CARBON_FACTORS["car"], 3),
                    "co2_train_km": round(total_co2_grams / CARBON_FACTORS["train"], 3),
                    "co2_bus_km": round(total_co2_grams / CARBON_FACTORS["bus"], 3),
                    "co2_plane_km": round(total_co2_grams / CARBON_FACTORS["plane"], 3),
                    "total_co2_impact_grams": round(total_co2_grams, 3)
                })
                
                # Calculate impact level based on total CO2
                if total_co2_grams <= 500:
                    impact_level = "LOW"
                elif total_co2_grams <= 1500:
                    impact_level = "MEDIUM"
                elif total_co2_grams <= 3000:
                    impact_level = "HIGH"
                else:
                    impact_level = "VERY_HIGH"
                
                transformed_data["impact_level"] = impact_level
                
                self.stats["calculated_fields"]["transport_equivalents_calculated"] += 1
                self.stats["calculated_fields"]["impact_levels_assigned"] += 1
            
            else:
                # Set defaults for missing data
                transformed_data.update({
                    "co2_vehicle_km": None,
                    "co2_train_km": None,
                    "co2_bus_km": None,
                    "co2_plane_km": None,
                    "total_co2_impact_grams": None,
                    "impact_level": None
                })
        
        print(f"      → Transport equivalents calculated: {self.stats['calculated_fields']['transport_equivalents_calculated']}")
        print(f"      → Impact levels assigned: {self.stats['calculated_fields']['impact_levels_assigned']}")
        
        return validated_products
    
    def _add_metadata_and_quality_checks(self, validated_products: Dict[str, Any], collection_timestamp: str) -> Dict[str, Any]:
        """Add metadata and perform final quality checks"""
        
        current_time = datetime.now()
        cache_expires = current_time + timedelta(days=30)
        
        for barcode, product_data in validated_products.items():
            transformed_data = product_data["transformed_data"]
            
            # Add system metadata
            transformed_data.update({
                "created_at": current_time.isoformat(),
                "updated_at": current_time.isoformat(), 
                "cache_expires_at": cache_expires.isoformat(),
                "collection_timestamp": collection_timestamp,
                "transformation_version": "1.0"
            })
            
            # Add raw data backup as JSONB
            raw_api_data = product_data.get("raw_data_backup", {}).get("raw_api_data", {})
            transformed_data["raw_data"] = raw_api_data.get("raw_api_response", {})
            
            self.stats["calculated_fields"]["metadata_added"] += 1
        
        print(f"      → Metadata added to {self.stats['calculated_fields']['metadata_added']} products")
        
        return validated_products
    
    def _assess_production_readiness(self, validated_products: Dict[str, Any], rejected_products: Dict[str, Any]) -> Dict[str, Any]:
        """Assess production readiness of transformed products"""
        
        total_processed = len(validated_products) + len(rejected_products)
        
        # Count products with complete critical data
        complete_products = 0
        for barcode, product_data in validated_products.items():
            transformed_data = product_data["transformed_data"]
            
            has_critical_fields = all([
                transformed_data.get("barcode"),
                transformed_data.get("product_name"),
                transformed_data.get("brand_name"),
                transformed_data.get("co2_total") is not None
            ])
            
            has_nutriscore = (
                transformed_data.get("nutriscore_grade") or 
                transformed_data.get("nutriscore_score") is not None
            )
            
            if has_critical_fields and has_nutriscore:
                complete_products += 1
        
        success_rate = (complete_products / total_processed * 100) if total_processed > 0 else 0
        
        return {
            "total_products_processed": total_processed,
            "validated_products": len(validated_products),
            "rejected_products": len(rejected_products),
            "complete_products_ready_for_db": complete_products,
            "success_rate": success_rate,
            "bot_launch_ready": complete_products >= 100 and success_rate >= 80,
            "minimum_viable_dataset": complete_products >= 50 and success_rate >= 70,
            "data_quality_grade": self._calculate_quality_grade(success_rate),
            "next_steps": self._generate_next_steps(complete_products, success_rate)
        }
    
    def _calculate_quality_grade(self, success_rate: float) -> str:
        """Calculate overall data quality grade"""
        if success_rate >= 95:
            return "A"
        elif success_rate >= 85:
            return "B"
        elif success_rate >= 75:
            return "C"
        elif success_rate >= 65:
            return "D"
        else:
            return "F"
    
    def _generate_next_steps(self, complete_products: int, success_rate: float) -> List[str]:
        """Generate recommended next steps"""
        steps = []
        
        if complete_products >= 100 and success_rate >= 85:
            steps.append("✅ Ready for production deployment")
            steps.append("📊 Load validated products into Supabase database")
            steps.append("🤖 Begin bot development and testing")
        elif complete_products >= 50 and success_rate >= 70:
            steps.append("⚠️ Minimum viable dataset achieved")
            steps.append("🔧 Address rejection reasons to improve quality")
            steps.append("🧪 Test with limited product set before full deployment")
        else:
            steps.append("❌ Dataset not ready for production")
            steps.append("🚨 Review and fix validation failures")
            steps.append("📈 Improve extraction pipeline before transformation")
        
        return steps
    
    def _display_transformation_summary(self, results: Dict[str, Any]):
        """Display comprehensive transformation summary"""
        
        stats = results["transformation_stats"]
        production_readiness = results["production_readiness"]
        
        print(f"\n🎯 TRANSFORMATION SUMMARY")
        print("=" * 60)
        
        print(f"📊 PROCESSING RESULTS:")
        print(f"   → Total products processed: {stats['total_products_processed']}")
        print(f"   → Successful transformations: {stats['successful_transformations']}")
        print(f"   → Products rejected: {stats['rejected_products']}")
        print(f"   → Duplicates handled: {stats['duplicate_products']}")
        
        print(f"\n✅ VALIDATION RESULTS:")
        validation_failures = stats["validation_failures"]
        total_failures = sum(validation_failures.values())
        if total_failures > 0:
            print(f"   → Total validation failures: {total_failures}")
            for failure_type, count in validation_failures.items():
                if count > 0:
                    print(f"      • {failure_type}: {count}")
        else:
            print(f"   → No validation failures! 🎉")
        
        print(f"\n🧹 DATA CLEANING RESULTS:")
        cleaning_stats = stats["data_cleaning"]
        total_cleaned = sum(cleaning_stats.values())
        print(f"   → Total cleaning operations: {total_cleaned}")
        for operation, count in cleaning_stats.items():
            if count > 0:
                print(f"      • {operation}: {count}")
        brand_cleaning_stats = self.brand_cleaner.get_cleaning_stats()
        if brand_cleaning_stats["brands_processed"] > 0:
            print(f"\n🏷️ BRAND NORMALIZATION DETAILS:")
            print(f"   → Brands processed: {brand_cleaning_stats['brands_processed']}")
            print(f"   → Brands modified: {brand_cleaning_stats['brands_cleaned']} ({brand_cleaning_stats['cleaning_rate']}%)")
            print(f"   → Primary brands extracted: {brand_cleaning_stats['primary_brand_extracted']}")
            print(f"   → Case/accent normalized: {brand_cleaning_stats['case_normalized']}")
            print(f"   → Mapped to canonical: {brand_cleaning_stats['mapped_to_canonical']}")

        print(f"\n📊 CALCULATED FIELDS:")
        calc_stats = stats["calculated_fields"]
        for field_type, count in calc_stats.items():
            print(f"   → {field_type}: {count}")
        
        print(f"\n🚀 PRODUCTION READINESS:")
        print(f"   → Database-ready products: {production_readiness['complete_products_ready_for_db']}")
        print(f"   → Overall success rate: {production_readiness['success_rate']:.1f}%")
        print(f"   → Data quality grade: {production_readiness['data_quality_grade']}")
        print(f"   → Bot launch ready: {'✅ YES' if production_readiness['bot_launch_ready'] else '⚠️ LIMITED' if production_readiness['minimum_viable_dataset'] else '❌ NO'}")
        
        print(f"\n🎯 NEXT STEPS:")
        for step in production_readiness["next_steps"]:
            print(f"   {step}")
        
        if results["products_missing_co2"]:
            print(f"\n🌍 CO2 DATA GAPS:")
            print(f"   → Products missing CO2: {len(results['products_missing_co2'])}")
            print(f"   → Check missing_co2 report for details")
        
        if results["data_quality_issues"]:
            print(f"\n⚠️ DATA QUALITY ISSUES:")
            print(f"   → Total issues found: {len(results['data_quality_issues'])}")
            for issue in results["data_quality_issues"][:3]:
                print(f"      • {issue}")
            if len(results["data_quality_issues"]) > 3:
                print(f"      • ... and {len(results['data_quality_issues']) - 3} more")
        
        print("=" * 60)


# Convenience function for integration
def transform_products_for_production(extracted_products: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convenience function to transform extracted products for production use
    
    Usage:
        extraction_results = await extractor.run_complete_extraction()
        transformation_results = transform_products_for_production(
            extraction_results["extracted_products"]
        )
    """
    transformer = ProductTransformer(use_duplicate_handling=True)
    return transformer.transform_extracted_products(extracted_products)


if __name__ == "__main__":
    print("🧪 TESTING PRODUCT TRANSFORMER")
    print("=" * 50)
    
    # Mock extracted products for testing
    mock_extracted_products = {
        "3017620422003": {
            "extracted_fields": {
                "barcode": "3017620422003",
                "product_name": "Nutella Pâte à tartiner aux noisettes et au cacao",
                "brand_name": "Nutella",
                "weight": 400.0,
                "product_quantity_unit": "g",
                "nutriscore_grade": "D",
                "nutriscore_score": 14,
                "co2_sources": {"agribalyse_total": 539.0},
                "extraction_success": {
                    "barcode": True, "product_name": True, "brand_name": True,
                    "weight": True, "co2_total": True, "nutriscore_grade": True
                }
            },
            "raw_api_data": {"raw_api_response": {"product_name_fr": "Nutella..."}}
        },
        "8000500273470": {  # Problematic Kinder product
            "extracted_fields": {
                "barcode": "8000500273470",
                "product_name": "Kinder White Chocolate",
                "brand_name": "Kinder White Chocolate with Milk and Hazelnuts Premium Quality European Brand Family Pack",  # Too long
                "weight": 100.0,
                "product_quantity_unit": "g",
                "co2_sources": {"agribalyse_total": 450.0},
                "extraction_success": {
                    "barcode": True, "product_name": True, "brand_name": True,
                    "weight": True, "co2_total": True
                }
            },
            "raw_api_data": {"raw_api_response": {}}
        }
    }
    
    # Test transformation
    transformer = ProductTransformer(use_duplicate_handling=False)  # Disable for testing
    results = transformer.transform_extracted_products(mock_extracted_products)
    
    print(f"\n✅ Transformation test completed:")
    print(f"   → Validated products: {len(results['validated_products'])}")
    print(f"   → Rejected products: {len(results['rejected_products'])}")
    print(f"   → Production ready: {results['production_readiness']['bot_launch_ready']}")
    
    # Check if Kinder brand was cleaned
    if "8000500273470" in results["validated_products"]:
        kinder_product = results["validated_products"]["8000500273470"]["transformed_data"]
        print(f"   → Kinder brand cleaned: '{kinder_product['brand_name']}'")