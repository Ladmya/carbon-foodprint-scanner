"""
src/food_scanner/data/transformers/cleaning/weight_normalizer.py
WEIGHT NORMALIZER: Normalize and validate product weights

RESPONSIBILITIES:
- Normalize weight values to standard format
- Validate weight ranges for food products
- Track weight normalization statistics
- Handle invalid weight data gracefully
"""

from typing import Any, Optional, List, Dict


class WeightNormalizer:
    """
    RESPONSIBILITY: Normalize and validate product weights
    
    BUSINESS LOGIC:
    1. Convert weight values to float
    2. Validate weight ranges (0.1g to 10kg)
    3. Track normalization statistics
    4. Handle invalid data gracefully
    """
    
    def __init__(self):
        # Weight validation constants
        self.MIN_WEIGHT_GRAMS = 0.1
        self.MAX_WEIGHT_GRAMS = 10000  # 10kg
        
        # Normalization statistics
        self.normalization_stats = {
            "weights_normalized": 0,
            "weights_rejected": 0,
            "invalid_values": 0
        }
        
        # Data quality tracking
        self.data_quality_issues = []
    
    def normalize_weight(self, weight: Any) -> Optional[float]:
        """
        Normalize weight to grams with validation
        
        Args:
            weight: Raw weight value (any type)
            
        Returns:
            Normalized weight in grams, or None if invalid
        """
        if weight is None:
            return None
        
        try:
            normalized_weight = float(weight)
            
            # Validate range
            if self._is_valid_weight(normalized_weight):
                self.normalization_stats["weights_normalized"] += 1
                return round(normalized_weight, 3)
            else:
                self.normalization_stats["weights_rejected"] += 1
                self.data_quality_issues.append(f"Weight outside valid range: {normalized_weight}g")
                return None
                
        except (ValueError, TypeError):
            self.normalization_stats["invalid_values"] += 1
            self.data_quality_issues.append(f"Invalid weight value: {weight}")
            return None
    
    def _is_valid_weight(self, weight: float) -> bool:
        """
        Check if weight is within valid range for food products
        
        Args:
            weight: Weight in grams
            
        Returns:
            True if weight is valid, False otherwise
        """
        return self.MIN_WEIGHT_GRAMS <= weight <= self.MAX_WEIGHT_GRAMS
    

    
    def get_normalization_stats(self) -> Dict[str, Any]:
        """Get current weight normalization statistics"""
        return self.normalization_stats.copy()
    
    def get_data_quality_issues(self) -> List[str]:
        """Get data quality issues found during weight normalization"""
        return self.data_quality_issues.copy()
    
    def reset_stats(self):
        """Reset weight normalization statistics"""
        self.normalization_stats = {
            "weights_normalized": 0,
            "weights_rejected": 0,
            "invalid_values": 0
        }
        self.data_quality_issues = []
