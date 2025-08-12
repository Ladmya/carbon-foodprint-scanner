"""
src/food_scanner/data/transformers/calculation/derived_fields_calculator.py
DERIVED FIELDS CALCULATOR: Calculate derived fields for products

RESPONSIBILITIES:
- Calculate transport equivalents (km)
- Calculate total CO2 impact
- Assign impact levels
- Track calculation statistics
"""

from typing import Dict, Any
from food_scanner.core.constants import CARBON_FACTORS


class DerivedFieldsCalculator:
    """
    RESPONSIBILITY: Calculate derived fields for validated products
    
    BUSINESS LOGIC:
    1. Calculate total CO2 impact for entire product
    2. Calculate transport equivalents (car, train, bus, plane km)
    3. Assign impact levels based on total CO2
    4. Track calculation statistics
    """
    
    def __init__(self):
        self.calculation_stats = {
            "transport_equivalents_calculated": 0,
            "impact_levels_assigned": 0,
            "total_products_processed": 0
        }
    
    def calculate_derived_fields(self, validated_products: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculate transport equivalents and impact levels for all validated products
        
        Args:
            validated_products: Products to process
            
        Returns:
            Updated validated_products with derived fields
        """
        self.calculation_stats["total_products_processed"] = len(validated_products)
        
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
                impact_level = self._calculate_impact_level(total_co2_grams)
                transformed_data["impact_level"] = impact_level
                
                self.calculation_stats["transport_equivalents_calculated"] += 1
                self.calculation_stats["impact_levels_assigned"] += 1
            
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

        print(f"      → Transport equivalents calculated: {self.calculation_stats['transport_equivalents_calculated']}")
        print(f"      → Impact levels assigned: {self.calculation_stats['impact_levels_assigned']}")
        
        return validated_products
    
    def _calculate_impact_level(self, total_co2_grams: float) -> str:
        """
        Calculate impact level based on total CO2 grams
        
        Args:
            total_co2_grams: Total CO2 impact in grams
            
        Returns:
            Impact level string (LOW, MEDIUM, HIGH, VERY_HIGH)
        """
        if total_co2_grams <= 500:
            return "LOW"
        elif total_co2_grams <= 1500:
            return "MEDIUM"
        elif total_co2_grams <= 3000:
            return "HIGH"
        else:
            return "VERY_HIGH"
    
    def get_calculation_stats(self) -> Dict[str, Any]:
        """Get current calculation statistics"""
        return self.calculation_stats.copy()
    
    def reset_stats(self):
        """Reset calculation statistics"""
        self.calculation_stats = {
            "transport_equivalents_calculated": 0,
            "impact_levels_assigned": 0,
            "total_products_processed": 0
        }
