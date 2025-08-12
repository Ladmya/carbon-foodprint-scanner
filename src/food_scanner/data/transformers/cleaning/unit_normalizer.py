"""
src/food_scanner/data/utils/unit_normalizer.py
Unit Normalizer - Data Quality Utility
Normalizes units to standard format for database storage
Handles: weight units, volume units, unit conversions
"""

from typing import Optional


class UnitNormalizer:
    """
    Utility for normalizing units to standard format
    
    Features:
    - Normalize weight units (g, kg, mg, oz, lb)
    - Normalize volume units (l, ml, cl, dl)
    - Convert units to grams for comparison
    - Handle various unit formats and variations
    """
    
    def __init__(self):
        # Unit mapping for normalization
        self.unit_mapping = {
            # Weight units
            'g': 'g', 'gr': 'g', 'gram': 'g', 'grams': 'g', 'gramme': 'g', 'grammes': 'g',
            'kg': 'kg', 'kilo': 'kg', 'kilos': 'kg', 'kilogram': 'kg', 'kilogramme': 'kg',
            'mg': 'mg', 'milligram': 'mg', 'milligramme': 'mg',
            
            # Volume units  
            'l': 'l', 'litre': 'l', 'litres': 'l', 'liter': 'l', 'liters': 'l',
            'ml': 'ml', 'millilitre': 'ml', 'millilitres': 'ml', 'milliliter': 'ml',
            'cl': 'cl', 'centilitre': 'cl', 'centiliter': 'cl',
            'dl': 'dl', 'decilitre': 'dl', 'deciliter': 'dl',
            
            # Anglo-Saxon units
            'oz': 'oz', 'ounce': 'oz', 'ounces': 'oz',
            'lb': 'lb', 'pound': 'lb', 'pounds': 'lb',
            
            # Counting units
            'piece': 'pieces', 'pieces': 'pieces', 'pièce': 'pieces', 'pièces': 'pieces',
            'pcs': 'pieces', 'pc': 'pieces',
            'unit': 'units', 'units': 'units', 'unité': 'units', 'unités': 'units',
            'count': 'count'
        }
        
        # Conversion factors to grams
        self.conversion_factors = {
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
    
    def normalize_unit(self, unit: str) -> Optional[str]:
        """
        Normalize unit to standard format
        
        Args:
            unit: Raw unit string
            
        Returns:
            Normalized unit string or None if unknown
        """
        if not unit:
            return None
        
        unit_lower = unit.lower().strip()
        return self.unit_mapping.get(unit_lower)
    
    def is_weight_unit(self, unit: str) -> bool: # TO DO : decide to use this method or remove it /
        """Check if unit is a weight unit"""
        normalized = self.normalize_unit(unit)
        return normalized in ['g', 'kg', 'mg', 'oz', 'lb']
    
    def is_volume_unit(self, unit: str) -> bool: # TO DO : decide to use this method or remove it /
        """Check if unit is a volume unit"""
        normalized = self.normalize_unit(unit)
        return normalized in ['l', 'ml', 'cl', 'dl']
    
    def is_counting_unit(self, unit: str) -> bool:
        """Check if unit is a counting unit"""
        normalized = self.normalize_unit(unit)
        return normalized in ['pieces', 'units', 'count']

    def convert_to_grams(self, weight: float, unit: str) -> Optional[float]: # Used in tests
        """
        Convert weight to grams for comparison
        
        Args:
            weight: Weight value
            unit: Unit of the weight
            
        Returns:
            Weight in grams or None if conversion not possible
        """
        if not unit:
            return None
        
        factor = self.conversion_factors.get(unit.lower())
        if factor:
            return round(weight * factor, 3)
        return None
    

# Test the normalizer
if __name__ == "__main__":
    normalizer = UnitNormalizer()
    
    print("🧪 TEST UNIT NORMALIZER")
    print("=" * 40)
    
    test_cases = [
        ('g', 'g'), ('gr', 'g'), ('gram', 'g'), ('grams', 'g'),
        ('kg', 'kg'), ('kilo', 'kg'), ('kilogram', 'kg'),
        ('mg', 'mg'), ('milligram', 'mg'),
        ('l', 'l'), ('litre', 'l'), ('liter', 'l'),
        ('ml', 'ml'), ('millilitre', 'ml'),
        ('cl', 'cl'), ('centilitre', 'cl'),
        ('oz', 'oz'), ('ounce', 'oz'),
        ('lb', 'lb'), ('pound', 'lb'),
        ('piece', 'pieces'), ('pieces', 'pieces'),
        ('unknown', None), ('', None)
    ]
    
    all_passed = True
    for input_unit, expected in test_cases:
        result = normalizer.normalize_unit(input_unit)
        result_str = str(result) if result is not None else 'None'
        status = '✅' if result == expected else '❌'
        print(f"{input_unit:12} → {result_str:8} {status}")
        if result != expected:
            all_passed = False
    
    print(f"\n🎯 RESULT: {'PASS' if all_passed else 'FAIL'}")
    
    # Test conversion
    print(f"\n🔄 CONVERSION TEST:")
    conversion_tests = [
        (100, 'g', 100),
        (1.5, 'kg', 1500),
        (500, 'ml', 500),
        (2, 'l', 2000)
    ]
    
    for weight, unit, expected in conversion_tests:
        result = normalizer.convert_to_grams(weight, unit)
        status = '✅' if result == expected else '❌'
        print(f"{weight}{unit} → {result}g {status}") 