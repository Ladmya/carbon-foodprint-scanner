"""
src/food_scanner/data/transformers/field_transformers/weight_parser.py
Correction parsing float/int 
"""

import re
from typing import Tuple, Optional


class WeightParser:
    """
    FIXED: Minimal correction of weight parsing for float/int from the API
    """
    
    def parse_weight_and_unit(self, quantity_input) -> Tuple[Optional[float], Optional[str]]:
        """
        Parse weight and unit from product quantity data.
        
        This function handles all types of quantity inputs and extracts raw weight and unit.
        It can handle:
        - None values (returns None, None)
        - Direct numeric values (int/float) from API responses
        - String representations of quantities
        - Type conversion for non-string inputs
        
        Args:
            quantity_input: Raw quantity data from API (can be None, int, float, or str)
            
        Returns:
            Tuple[Optional[float], Optional[str]]: 
                - weight: Raw weight value (not normalized)
                - unit: Raw unit (not normalized)
                
        Examples:
            >>> parser.parse_weight_and_unit(400)        # (400.0, 'g')
            >>> parser.parse_weight_and_unit("1.5kg")   # (1.5, 'kg')
            >>> parser.parse_weight_and_unit("2 x 100g") # (200.0, 'g')
            >>> parser.parse_weight_and_unit(None)       # (None, None)
        """
        # Handle None
        if quantity_input is None:
            return None, None
        
        # Handle float/int directly
        if isinstance(quantity_input, (int, float)):
            # If it's a direct number, assume grams (most common case)
            if 0 < quantity_input < 10000:  # Range raisonnable
                return float(quantity_input), 'g'
            else:
                return None, None
        
        # Convert to string if needed
        if not isinstance(quantity_input, str):
            quantity_str = str(quantity_input)
        else:
            quantity_str = quantity_input
        
        # Handle empty string
        if not quantity_str:
            return None, None
        
        clean_str = quantity_str.strip().lower()
        
        # Patterns
        patterns = [
            # Multiplication with unit: "2 × 100g", "4 x 25g"
            (r'(\d+(?:\.\d+)?)\s*[×x*]\s*(\d+(?:\.\d+)?)\s*(g|kg|mg|l|ml|cl|dl|oz|lb)\b', 
            "multiply_with_unit"),
            
            # Standard with unit: "400 g", "1.5 kg", "500ml"
            (r'(\d+(?:\.\d+)?)\s*(g|kg|mg|l|ml|cl|dl|oz|lb|gr|grammes?|kilos?|litres?)\b', 
            "standard_weight_unit"),
            
            # Attached unit: "400g", "1.5kg"
            (r'(\d+(?:\.\d+)?)([a-z]+)', 
            "attached_unit"),
        ]
        
        for pattern, pattern_name in patterns:
            match = re.search(pattern, clean_str)
            if match:
                try:
                    if pattern_name == "multiply_with_unit":
                        # Multiplication: 2 × 100g = 200g
                        value1 = float(match.group(1))
                        value2 = float(match.group(2))
                        raw_unit = match.group(3)
                        # Return raw values without normalization
                        return value1 * value2, raw_unit
                    
                    elif pattern_name in ["standard_weight_unit", "attached_unit"]:
                        # Standard: 400g = 400, g
                        value = float(match.group(1))
                        raw_unit = match.group(2)
                        # Return raw values without normalization
                        return value, raw_unit
                
                except (ValueError, IndexError):
                    continue
        
        return None, None
    
    # Note: Normalization and conversion moved to UnitNormalizer
    # This class now focuses only on extraction


# TEST with problematic cases
if __name__ == "__main__":
    parser = WeightParser()
    
    print("🧪 TEST CORRECTION WEIGHT PARSING")
    print("=" * 50)
    
    # Cases that were failing before (float/int directly from the API)
    problematic_cases = [
        (400, "400.0g"),           # Float direct - PROBLEM
        (1.5, "1.5g"),             # Float decimal
        (1500, "1500.0g"),         # Int plus grand
        ("400g", "400.0g"),        # Classic string (must always work)
        ("1.5kg", "1500.0g"),     # Conversion (must always work)
        ("2 x 250g", "500.0g"),   # Multiplication (must always work)
    ]
    
    all_passed = True
    for test_input, expected in problematic_cases:
        weight, unit = parser.parse_weight_and_unit(test_input)
        result = f"{weight}{unit}" if weight else "None"
        
        if result == expected:
            print(f"✅ {test_input} → {result}")
        else:
            print(f"❌ {test_input} → {result} (expected: {expected})")
            all_passed = False
    
    if all_passed:
        print(f"\n🎉 CORRECTION SUCCESS!")
        print(f"✅ The parsing of float/int now works")
        print(f"✅ Success rate should pass from 60% to 85%+")
    else:
        print(f"\n❌ Tests still failing")