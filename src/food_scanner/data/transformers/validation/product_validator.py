"""
src/food_scanner/data/transformers/validation/product_validator.py
PRODUCT VALIDATOR: Business logic validation for extracted products

RESPONSIBILITIES:
- Apply business validation rules
- Track validation failures and statistics
- Identify products missing critical data
- Generate validation reports
"""

from datetime import datetime
from typing import Dict, List, Any, Tuple

from food_scanner.data.transformers.validation.weight_validator import WeightValidator
from food_scanner.data.transformers.validation.nutriscore_validator import NutriscoreValidator


class ProductValidator:
    """
    RESPONSIBILITY: Validate extracted products against business rules
    """

    def __init__(self):
        # Specialized validators (business logic)
        self.weight_validator = WeightValidator()
        self.nutriscore_validator = NutriscoreValidator()

        self.validation_stats = {
            "total_products_processed": 0,
            "successful_validations": 0,
            "failed_validations": 0,
            "validation_failures": {
                "missing_barcode": 0,
                "missing_product_name": 0,
                "missing_brand_name": 0,
                "missing_weight": 0,
                "missing_co2": 0,
                "missing_nutriscore": 0,
                "invalid_data": 0
            }
        }        
        
        # Data quality tracking
        self.products_missing_co2 = []
        self.validation_failures_details = []

    def _validate_single_product(self, barcode: str, extracted_fields: Dict[str, Any], success_flags: Dict[str, bool]) -> Dict[str, Any]:
        """
        PURE ORCHESTRATION: Delegate to specialized validators
        """
        rejection_reasons = []
        
        # Validation 1: Critical fields 
        if not success_flags.get("barcode", False) or not extracted_fields.get("barcode"):
            rejection_reasons.append("Missing or invalid barcode (required for primary key)")
        
        if not success_flags.get("product_name", False) or not extracted_fields.get("product_name"):
            rejection_reasons.append("Missing product name (required for bot display)")
        
        if not success_flags.get("brand_name", False) or not extracted_fields.get("brand_name"):
            rejection_reasons.append("Missing brand name (required for bot display)")
        
        # Validation 2: CO2 (logique simple gardée ici)
        if not success_flags.get("co2_total", False):
            co2_sources = extracted_fields.get("co2_sources", {})
            has_co2 = any(value is not None for value in co2_sources.values())
            if not has_co2:
                rejection_reasons.append("Missing CO2 data (required for carbon footprint functionality)")
                self.products_missing_co2.append({
                    "barcode": barcode,
                    "product_name": extracted_fields.get("product_name", "Unknown"),
                    "brand_name": extracted_fields.get("brand_name", "Unknown"),
                    "extraction_timestamp": datetime.now().isoformat()
                })
        
        # Validation 3: Delegate weight to WeightValidator
        weight_validation = self.weight_validator.validate_product_weight(extracted_fields, success_flags)
        if not weight_validation["is_valid"]:
            rejection_reasons.extend(weight_validation["rejection_reasons"])
        
        # Validation 4: Delegate nutriscore to NutriscoreValidator  
        nutriscore_validation = self.nutriscore_validator.validate_product_nutriscore(extracted_fields, success_flags)
        if not nutriscore_validation["is_valid"]:
            rejection_reasons.extend(nutriscore_validation["rejection_reasons"])
        
        return {
            "is_valid": len(rejection_reasons) == 0,
            "rejection_reasons": rejection_reasons
        }

    def validate_products(
        self, 
        extracted_products: Dict[str, Any]
        ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """
        Validate all extracted products against business rules
        
        Args:
            extracted_products: Products to validate
            
        Returns:
            Tuple of (validated_products, rejected_products)
        """
        validated_products = {}
        rejected_products = {}
        
        for barcode, product_data in extracted_products.items():
            self.validation_stats["total_products_processed"] += 1
            
            extracted_fields = product_data.get("extracted_fields", {})
            success_flags = extracted_fields.get("extraction_success", {})
            
            # Apply validation rules
            validation_result = self._validate_single_product(barcode, extracted_fields, success_flags)
            
            if validation_result["is_valid"]:
                validated_products[barcode] = product_data
                self.validation_stats["successful_validations"] += 1
            else:
                rejected_products[barcode] = {
                    "rejection_reasons": validation_result["rejection_reasons"],
                    "partial_data": extracted_fields,
                    "raw_data_backup": product_data,
                    "validation_passed": False,
                    "validation_timestamp": datetime.now().isoformat()
                }
                
                self.validation_stats["failed_validations"] += 1
                
                # Update validation failure stats
                self._update_validation_failure_stats(validation_result["rejection_reasons"])
        
        return validated_products, rejected_products
    
    def _update_validation_failure_stats(self, rejection_reasons: List[str]):
        """Update validation failure statistics"""
        for reason in rejection_reasons:
            if "barcode" in reason.lower():
                self.validation_stats["validation_failures"]["missing_barcode"] += 1
            elif "product_name" in reason.lower():
                self.validation_stats["validation_failures"]["missing_product_name"] += 1
            elif "brand_name" in reason.lower() or "brand" in reason.lower():
                self.validation_stats["validation_failures"]["missing_brand_name"] += 1
            elif "weight" in reason.lower():
                self.validation_stats["validation_failures"]["missing_weight"] += 1
            elif "co2" in reason.lower():
                self.validation_stats["validation_failures"]["missing_co2"] += 1
            elif "nutriscore" in reason.lower():
                self.validation_stats["validation_failures"]["missing_nutriscore"] += 1
            else:
                self.validation_stats["validation_failures"]["invalid_data"] += 1
    
    def get_validation_stats(self) -> Dict[str, Any]:
        """Get current validation statistics"""
        return self.validation_stats.copy()
    
    def get_products_missing_co2(self) -> List[Dict[str, Any]]:
        """Get list of products missing CO2 data"""
        return self.products_missing_co2.copy()
    
    def reset_stats(self):
        """Reset validation statistics"""
        self.validation_stats = {
            "total_products_processed": 0,
            "successful_validations": 0,
            "failed_validations": 0,
            "validation_failures": {
                "missing_barcode": 0,
                "missing_product_name": 0,
                "missing_brand_name": 0,
                "missing_weight": 0,
                "missing_co2": 0,
                "missing_nutriscore": 0,
                "invalid_data": 0
            }
        }
        self.products_missing_co2 = []
        self.validation_failures_details = []
