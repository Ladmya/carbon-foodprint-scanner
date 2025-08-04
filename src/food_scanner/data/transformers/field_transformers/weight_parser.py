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
        Main entry point for parsing weight and unit from product quantity data.
        
        This function acts as a smart wrapper that handles edge cases before delegating
        to the complex parsing logic. It can handle:
        - None values (returns None, None)
        - Direct numeric values (int/float) from API responses
        - String representations of quantities
        - Type conversion for non-string inputs
        
        Args:
            quantity_input: Raw quantity data from API (can be None, int, float, or str)
            
        Returns:
            Tuple[Optional[float], Optional[str]]: 
                - weight: Normalized weight in grams (or ml for liquids)
                - unit: Normalized unit ('g' for weights, 'ml' for volumes)
                
        Examples:
            >>> parser.parse_weight_and_unit(400)        # (400.0, 'g')
            >>> parser.parse_weight_and_unit("1.5kg")   # (1500.0, 'g')
            >>> parser.parse_weight_and_unit("2 x 100g") # (200.0, 'g')
            >>> parser.parse_weight_and_unit(None)       # (None, None)
        """
        # FIX 1: Handle None
        if quantity_input is None:
            return None, None
        
        # FIX 2: Handle float/int directly (this was the main problem!)
        if isinstance(quantity_input, (int, float)):
            # If it's a direct number, assume grams (most common case)
            if 0 < quantity_input < 10000:  # Range raisonnable
                return float(quantity_input), 'g'
            else:
                return None, None
        
        # FIX 3: Convert to string 
        if not isinstance(quantity_input, str):
            quantity_str = str(quantity_input)
        else:
            quantity_str = quantity_input
        
        return self._parse_weight_and_unit_original_logic(quantity_str)
    
    def _parse_weight_and_unit_original_logic(self, quantity_str: str) -> Tuple[Optional[float], Optional[str]]:
        """
        Core parsing logic for weight and unit extraction from string representations.
        
        This function implements the complex regex-based parsing for various quantity formats.
        It handles multiple patterns including:
        - Multiplication patterns: "2 × 100g", "4 x 25g"
        - Standard weight/volume units: "400 g", "1.5 kg", "500ml"
        - Attached units: "400g", "1.5kg"
        - Various unit formats: grams, kilograms, liters, milliliters, etc.
        
        This is the original parsing logic that was enhanced with the wrapper function
        to handle non-string inputs from API responses.
        
        Args:
            quantity_str: String representation of quantity (must be valid string)
            
        Returns:
            Tuple[Optional[float], Optional[str]]:
                - weight: Converted weight in grams (or ml for liquids)
                - unit: Normalized unit ('g' for weights, 'ml' for volumes)
                
        Examples:
            >>> parser._parse_weight_and_unit_original_logic("400g")      # (400.0, 'g')
            >>> parser._parse_weight_and_unit_original_logic("1.5kg")    # (1500.0, 'g')
            >>> parser._parse_weight_and_unit_original_logic("2 x 100g") # (200.0, 'g')
            >>> parser._parse_weight_and_unit_original_logic("")          # (None, None)
        """
        if not quantity_str or not isinstance(quantity_str, str):
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
                        unit = self._normalize_unit(match.group(3))
                        if unit:
                            weight_grams = self._convert_to_grams(value1 * value2, unit)
                            return weight_grams, 'g' if weight_grams else 'ml'
                    
                    elif pattern_name in ["standard_weight_unit", "attached_unit"]:
                        # Standard: 400g = 400, g
                        value = float(match.group(1))
                        unit = self._normalize_unit(match.group(2))
                        if unit:
                            weight_grams = self._convert_to_grams(value, unit)
                            return weight_grams, 'g' if unit in ['g', 'kg', 'mg', 'gr', 'grammes', 'gramme'] else 'ml'
                
                except (ValueError, IndexError):
                    continue
        
        return None, None
    
    def _normalize_unit(self, unit: str) -> Optional[str]:
        """Handle unit normalization"""
        if not unit:
            return None
        
        unit_lower = unit.lower().strip()
        
        # Unit mapping
        unit_mapping = {
            # Weight units
            'g': 'g', 'gr': 'g', 'gram': 'g', 'grams': 'g', 'gramme': 'g', 'grammes': 'g',
            'kg': 'kg', 'kilo': 'kg', 'kilos': 'kg', 'kilogram': 'kg', 'kilogramme': 'kg',
            'mg': 'mg',
            
            # Volume units  
            'l': 'l', 'litre': 'l', 'litres': 'l', 'liter': 'l', 'liters': 'l',
            'ml': 'ml', 'millilitre': 'ml', 'millilitres': 'ml',
            'cl': 'cl', 'dl': 'dl',
            
            # Anglo-Saxon units
            'oz': 'oz', 'lb': 'lb'
        }
        
        return unit_mapping.get(unit_lower)
    
    def _convert_to_grams(self, weight: float, unit: str) -> Optional[float]:
        """Convert to grams"""
        if not unit:
            return None
        
        # Conversion factors to grams
        conversion_factors = {
            'g': 1,
            'kg': 1000,
            'mg': 0.001,
            'oz': 28.35,
            'lb': 453.59,
            # Volume approximated as mass (1ml ≈ 1g for liquids)
            'ml': 1,
            'cl': 10,
            'dl': 100,
            'l': 1000,
        }
        
        factor = conversion_factors.get(unit.lower())
        if factor:
            return round(weight * factor, 3)
        return None


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