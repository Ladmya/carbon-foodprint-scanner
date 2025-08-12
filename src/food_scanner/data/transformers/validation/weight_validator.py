"""
WeightValidator:
    - Validate weight data (business rules)
    - UnitNormalizer = transformation des données
    - WeightValidator = validation des règles business
"""

from typing import Dict, Any

from food_scanner.data.transformers.cleaning.unit_normalizer import UnitNormalizer

class WeightValidator:
    """
    RESPONSIBILITY: Validate weight data (business rules)
    """
    def __init__(self):
        self.MIN_WEIGHT_GRAMS = 0.1
        self.MAX_WEIGHT_GRAMS = 10000
        
        self.validation_stats = {
            "weights_validated": 0,
            "validation_failures": 0,
            "weight_range_failures": 0,
            "unit_compatibility_failures": 0
        }    

        self.unit_normalizer = UnitNormalizer()

    def validate_product_weight(self, extracted_fields: Dict[str, Any], success_flags: Dict[str, bool]) -> Dict[str, Any]:
        """
        FULL WEIGHT VALIDATION with ALL business logic
        
        Args:
            extracted_fields: Extracted product data
            success_flags: Success flags for extraction
            
        Returns:
            Validation result with rejection_reasons
        """
        self.validation_stats["products_validated"] += 1
        rejection_reasons = []
        
        # Extract data (business logic in the validator)
        weight = extracted_fields.get("weight")
        unit = extracted_fields.get("product_quantity_unit", "")
        weight_success = success_flags.get("weight", False)
        
        # Logic 1: Check if weight is required and present
        if weight_success and weight is None:
            return {
                "is_valid": False,  
                "rejection_reasons": [],
                "warnings": ["Weight data missing or invalid"],
                "weight_available": False
            }
        
        # Logic 2: Validate weight range
        range_validation = self._validate_weight_range(weight)
        if not range_validation["is_valid"]:
            rejection_reasons.append(f"Invalid weight range: {range_validation['reason']}")
            self.validation_stats["weight_range_failures"] += 1
        
        # Logic 3: Validate weight/unit compatibility
        if unit:
            compatibility_validation = self._validate_weight_unit_compatibility(weight, unit)
            if not compatibility_validation["is_valid"]:
                rejection_reasons.append(f"Weight/unit incompatible: {compatibility_validation['reason']}")
                self.validation_stats["unit_compatibility_failures"] += 1
        
        # Statistics
        if rejection_reasons:
            self.validation_stats["validation_failures"] += 1
        
        return {
            "is_valid": len(rejection_reasons) == 0,
            "rejection_reasons": rejection_reasons,
            "weight_available": True,
            "weight_value": weight,
            "unit_value": unit
        }

    def validate_weight_range(self, weight: float) -> Dict[str, Any]: # TO DO: decide to use this method or remove it /
        """
        Validate weight and provide detailed feedback
        
        Args:
            weight: Weight to validate
            
        Returns:
            Validation result with details
        """
        self.validation_stats["weights_validated"] += 1
        
        if weight < self.MIN_WEIGHT_GRAMS:
            self.validation_stats["validation_failures"] += 1
            return {
                "is_valid": False,
                "reason": f"Weight too small: {weight}g (minimum: {self.MIN_WEIGHT_GRAMS}g)"
            }
        
        if weight > self.MAX_WEIGHT_GRAMS:  
            self.validation_stats["validation_failures"] += 1
            return {
                "is_valid": False,
                "reason": f"Weight too large: {weight}g (maximum: {self.MAX_WEIGHT_GRAMS}g)",
            }

        if weight is None:
            self.validation_stats["validation_failures"] += 1
            return {
                "is_valid": False,
                "reason": "Weight is None",
            }    
        
        return {
            "is_valid": True,
            "reason": "Weight is within valid range",
        }

    def validate_weight_unit_compatibility(self, weight: float, unit: str) -> Dict[str, Any]:
        """Validate that weight and unit are compatible"""
        if not self.unit_normalizer.is_weight_unit(unit):
            return {
                "is_valid": False,
                "reason": f"Unit '{unit}' is not a weight unit",
            }
        
        return {
            "is_valid": True,
            "reason": "Weight and unit are compatible",
        }

    def get_validation_stats(self) -> Dict[str, Any]:
        """Get validation statistics"""
        return self.validation_stats.copy()