"""
src/food_scanner/data/analysis/quantity_analyzer.py
Specialized analyzer for weight and product_quantity_unit extraction
"""

import re
from typing import Dict, List, Any, Tuple, Optional
from collections import Counter, defaultdict
from .base_analyzer import BaseFieldAnalyzer
from ...core.models.data_quality import FieldAnalysisResult, FieldType


class QuantityAnalyzer(BaseFieldAnalyzer):
    """
    Weight et product_quantity_unit dedicated analyzer
    Responsability: Parser product_quantity/quantity → extract weight + unit
    """
    
    def analyze_field(self, products: List[Dict[str, Any]]) -> FieldAnalysisResult:
        result = FieldAnalysisResult(
            field_name=self.field_name,
            field_type=FieldType.NUMERIC,
            total_products=len(products)
        )
        
        extraction_patterns = Counter()
        units_found = Counter()
        weight_ranges = {"0-50g": 0, "50-200g": 0, "200-500g": 0, "500g+": 0}
        examples = defaultdict(list)
        parsing_failures = Counter()
        
        for product in products:
            # Extract quantity chain from available fields
            quantity_str = self._extract_quantity_string(product)
            barcode = self._get_barcode_for_example(product)
            
            if not quantity_str:
                result.missing_count += 1
                self._add_example(examples, "no_quantity", {
                    "barcode": barcode,
                    "product": self._get_product_name_for_example(product),
                    "available_fields": {
                        "product_quantity": product.get('product_quantity', 'N/A'),
                        "quantity": product.get('quantity', 'N/A')
                    }
                })
                continue
            
            result.present_count += 1
            
            # Try extraction with advanced patterns 
            weight, unit, pattern_used = self._extract_quantity_and_unit_advanced(quantity_str)
            
            if weight is not None and unit is not None:
                # Success : weight + unit
                result.valid_count += 1
                result.transformation_success_count += 1
                extraction_patterns["both_extracted"] += 1
                units_found[unit] += 1
                
                # Analyze weight range
                self._categorize_weight_range(weight, unit, weight_ranges)
                
                self._add_example(examples, "successful", {
                    "barcode": barcode,
                    "original": quantity_str,
                    "weight": weight,
                    "unit": unit,
                    "pattern": pattern_used,
                    "normalized_weight_g": self._normalize_to_grams(weight, unit)
                })
                
            elif weight is not None:
                # Only weight
                result.transformation_success_count += 1
                extraction_patterns["weight_only"] += 1
                
                self._add_example(examples, "weight_only", {
                    "barcode": barcode,
                    "original": quantity_str,
                    "weight": weight,
                    "pattern": pattern_used,
                    "issue": "Unit not detected"
                })
                
            elif unit is not None:
                # Only unit 
                extraction_patterns["unit_only"] += 1
                units_found[unit] += 1
                
                self._add_example(examples, "unit_only", {
                    "barcode": barcode,
                    "original": quantity_str,
                    "unit": unit,
                    "pattern": pattern_used,
                    "issue": "Weight value not detected"
                })
                
            else:
                # Total failure
                result.invalid_count += 1
                extraction_patterns["parse_failed"] += 1
                
                # Analyze failed extraction
                failure_reason = self._analyze_parsing_failure(quantity_str)
                parsing_failures[failure_reason] += 1
                
                self._add_example(examples, "failed", {
                    "barcode": barcode,
                    "original": quantity_str,
                    "failure_reason": failure_reason,
                    "product": self._get_product_name_for_example(product)
                })
        
        # Value and pattern distribution
        result.value_distribution = dict(units_found.most_common(15))
        
        # Analyze extraction patterns 
        result.pattern_analysis = {
            "extraction_success": dict(extraction_patterns),
            "weight_ranges": weight_ranges,
            "most_common_units": dict(units_found.most_common(10)),
            "parsing_failures": dict(parsing_failures.most_common()),
            "unit_frequency_analysis": self._analyze_unit_frequencies(units_found),
            "weight_statistics": self._calculate_weight_statistics(examples.get("successful", []))
        }
        
        result.examples = dict(examples)
        
        # Recommendations
        self._generate_quantity_recommendations(result, extraction_patterns, parsing_failures)
        
        self._calculate_base_metrics(result)
        return result
    
    def _extract_quantity_string(self, product: Dict[str, Any]) -> str:
        """Extract quantity chain from available fields"""
        # Priority: product_quantity then quantity
        quantity_str = product.get('product_quantity', '').strip()
        if not quantity_str:
            quantity_str = product.get('quantity', '').strip()
        return quantity_str
    
    def _extract_quantity_and_unit_advanced(self, quantity_str: str) -> Tuple[Optional[float], Optional[str], str]:
        """
        Robust pattern-based extraction and used pattern identification
        Return: (weight, unit, pattern_name)
        """
        if not quantity_str or not isinstance(quantity_str, str):
            return None, None, "empty_input"
        
        clean_str = quantity_str.strip().lower()
        
        # Patterns organized by specificity 
        patterns = [
            # Multiplication avec unité: "2 × 100g", "4 x 25g", "6*50ml"
            (r'(\d+(?:\.\d+)?)\s*[×x*]\s*(\d+(?:\.\d+)?)\s*(g|kg|mg|l|ml|cl|dl|oz|lb|pieces?|pcs?)\b', 
            "multiply_with_unit"),
            
            # Multiplication simple: "2 × 100", "4 x 25"
            (r'(\d+(?:\.\d+)?)\s*[×x*]\s*(\d+(?:\.\d+)?)\b', 
            "multiply_simple"),
            
            # Standard avec unité claire: "400 g", "1.5 l", "250ml"
            (r'(\d+(?:\.\d+)?)\s*(g|kg|mg|l|ml|cl|dl|oz|lb)\b', 
            "standard_weight_unit"),
            
            # Count/pieces format: "24 pieces", "6 units", "12 pcs"
            (r'(\d+(?:\.\d+)?)\s*(pieces?|units?|count|pcs?|pièces?)\b', 
            "count_format"),
            
            # Range avec unité: "100-150g", "2-3 kg"
            (r'(\d+(?:\.\d+)?)\s*[-–]\s*(\d+(?:\.\d+)?)\s*(g|kg|mg|l|ml|cl|dl|oz|lb)\b', 
            "range_with_unit"),
            
            # Collé: "184g", "500ml", "2kg"
            (r'(\d+(?:\.\d+)?)([a-z]+)', 
            "attached_unit"),
            
            # Nombre seul: "400", "60"
            (r'^(\d+(?:\.\d+)?)$', 
            "number_only"),
            
            # Pattern avec séparateurs: "400 gr", "2 litres"
            (r'(\d+(?:\.\d+)?)\s+(gr?|grammes?|kg|kilos?|l|litres?|ml|millilitres?)\b', 
            "french_units"),
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
                        return value1 * value2, unit, pattern_name
                        
                    elif pattern_name == "multiply_simple":
                        # Multiplication sans unité: 2 × 100 = 200
                        value1 = float(match.group(1))
                        value2 = float(match.group(2))
                        return value1 * value2, None, pattern_name
                        
                    elif pattern_name in ["standard_weight_unit", "count_format", "french_units"]:
                        # Standard: 400g = 400, g
                        value = float(match.group(1))
                        unit = self._normalize_unit(match.group(2))
                        return value, unit, pattern_name
                        
                    elif pattern_name == "range_with_unit":
                        # Range: prendre la première valeur
                        value = float(match.group(1))
                        unit = self._normalize_unit(match.group(3))
                        return value, unit, pattern_name
                        
                    elif pattern_name == "attached_unit":
                        # Collé: vérifier si l'unité est valide
                        value = float(match.group(1))
                        potential_unit = match.group(2)
                        normalized_unit = self._normalize_unit(potential_unit)
                        
                        if normalized_unit:
                            return value, normalized_unit, pattern_name
                        else:
                            # Unité non reconnue
                            return value, None, f"{pattern_name}_unknown_unit"
                            
                    elif pattern_name == "number_only":
                        # Juste un nombre
                        value = float(match.group(1))
                        return value, None, pattern_name
                        
                except (ValueError, IndexError) as e:
                    # Matched pattern with FAILED extraction
                    continue
        
        return None, None, "no_pattern_matched"
    
    def _normalize_unit(self, unit: str) -> Optional[str]:
        """Normalize unit in one standard format"""
        if not unit:
            return None
        
        unit_lower = unit.lower().strip()
        
        # Unit mapping to standard format
        unit_mapping = {
            # Poids
            'g': 'g', 'gr': 'g', 'gram': 'g', 'grams': 'g', 'gramme': 'g', 'grammes': 'g',
            'kg': 'kg', 'kilo': 'kg', 'kilos': 'kg', 'kilogram': 'kg', 'kilogramme': 'kg',
            'mg': 'mg', 'milligram': 'mg', 'milligramme': 'mg',
            
            # Volume
            'l': 'l', 'litre': 'l', 'litres': 'l', 'liter': 'l', 'liters': 'l',
            'ml': 'ml', 'millilitre': 'ml', 'millilitres': 'ml', 'milliliter': 'ml',
            'cl': 'cl', 'centilitre': 'cl', 'centiliter': 'cl',
            'dl': 'dl', 'decilitre': 'dl', 'deciliter': 'dl',
            
            # Comptage
            'piece': 'pieces', 'pieces': 'pieces', 'pièce': 'pieces', 'pièces': 'pieces',
            'pcs': 'pieces', 'pc': 'pieces',
            'unit': 'units', 'units': 'units', 'unité': 'units', 'unités': 'units',
            'count': 'count',
            
            # Unités anglo-saxonnes
            'oz': 'oz', 'ounce': 'oz', 'ounces': 'oz',
            'lb': 'lb', 'pound': 'lb', 'pounds': 'lb',
        }
        
        return unit_mapping.get(unit_lower)
    
    def _categorize_weight_range(self, weight: float, unit: str, weight_ranges: Dict[str, int]):
        """Catégoriser le poids dans les ranges (converti en grammes)"""
        weight_grams = self._normalize_to_grams(weight, unit)
        
        if weight_grams is not None:
            if weight_grams <= 50:
                weight_ranges["0-50g"] += 1
            elif weight_grams <= 200:
                weight_ranges["50-200g"] += 1
            elif weight_grams <= 500:
                weight_ranges["200-500g"] += 1
            else:
                weight_ranges["500g+"] += 1
    
    def _normalize_to_grams(self, weight: float, unit: str) -> Optional[float]:
        """Convert weight in grams for comparison"""
        if not unit:
            return None
        
        conversion_factors = {
            'g': 1,
            'kg': 1000,
            'mg': 0.001,
            'oz': 28.35,
            'lb': 453.59,
            # Volume assumé comme masse (approximation)
            'ml': 1,  # Approximation 1ml ≈ 1g pour liquides
            'cl': 10,
            'dl': 100,
            'l': 1000,
        }
        
        factor = conversion_factors.get(unit.lower())
        if factor:
            return weight * factor
        return None
    
    def _analyze_parsing_failure(self, quantity_str: str) -> str:
        """Analyze extraction failure"""
        if not quantity_str:
            return "empty_string"
        
        # Check the different failure causes
        if not any(c.isdigit() for c in quantity_str):
            return "no_numbers"
        
        if len(quantity_str) > 100:
            return "too_long"
        
        if any(char in quantity_str for char in "()[]{}"):
            return "complex_brackets"
        
        if "," in quantity_str and "." in quantity_str:
            return "mixed_separators"
        
        if quantity_str.count(" ") > 5:
            return "too_many_spaces"
        
        # Complex text : too many words
        words = quantity_str.split()
        if len(words) > 8:
            return "too_many_words"
        
        return "unknown_format"
    
    def _analyze_unit_frequencies(self, units_found: Counter) -> Dict[str, Any]:
        """Analyze units frequency and distribution"""
        total_units = sum(units_found.values())
        
        if total_units == 0:
            return {"total": 0}
        
        # Categorize by unit type
        weight_units = {'g', 'kg', 'mg', 'oz', 'lb'}
        volume_units = {'ml', 'cl', 'dl', 'l'}
        count_units = {'pieces', 'units', 'count'}
        
        categories = {
            "weight": sum(count for unit, count in units_found.items() if unit in weight_units),
            "volume": sum(count for unit, count in units_found.items() if unit in volume_units),
            "count": sum(count for unit, count in units_found.items() if unit in count_units),
            "other": sum(count for unit, count in units_found.items() 
                        if unit not in weight_units and unit not in volume_units and unit not in count_units)
        }
        
        return {
            "total": total_units,
            "categories": categories,
            "category_percentages": {
                cat: (count / total_units * 100) if total_units > 0 else 0 
                for cat, count in categories.items()
            },
            "most_frequent": units_found.most_common(5)
        }
    
    def _calculate_weight_statistics(self, successful_examples: List[Dict]) -> Dict[str, Any]:
        """Calculate statistiques for successful weight extraction"""
        if not successful_examples:
            return {"count": 0}
        
        weights_grams = []
        for example in successful_examples:
            weight = example.get("normalized_weight_g")
            if weight is not None:
                weights_grams.append(weight)
        
        if not weights_grams:
            return {"count": 0}
        
        return {
            "count": len(weights_grams),
            "average_grams": sum(weights_grams) / len(weights_grams),
            "median_grams": sorted(weights_grams)[len(weights_grams)//2],
            "min_grams": min(weights_grams),
            "max_grams": max(weights_grams),
            "std_dev": self._calculate_std_dev(weights_grams)
        }
    
    def _calculate_std_dev(self, values: List[float]) -> float:
        """Calculer écart-type simple"""
        if len(values) < 2:
            return 0
        
        mean = sum(values) / len(values)
        variance = sum((x - mean) ** 2 for x in values) / len(values)
        return variance ** 0.5
    
    def _generate_quantity_recommendations(
            self, result: FieldAnalysisResult, 
            extraction_patterns: Counter, 
            parsing_failures: Counter
        ):
        """Generate dedicated recommendations to weight extraction"""
        
        # Successful rates 
        if result.present_count > 0:
            success_rate = (result.transformation_success_count / result.present_count) * 100
            result.transformation_recommendations.append(
                f"Taux de succès d'extraction: {success_rate:.1f}% ({result.transformation_success_count}/{result.present_count})"
            )
        
        # Recommendations based on extraction patterns basées sur les patterns d'extraction
        both_extracted = extraction_patterns.get("both_extracted", 0)
        weight_only = extraction_patterns.get("weight_only", 0)
        
        if both_extracted > 0:
            result.transformation_recommendations.append(
                f"Extraction complète (poids + unité): {both_extracted} produits"
            )
        
        if weight_only > 0:
            result.transformation_recommendations.append(
                f"Poids sans unité: {weight_only} produits - améliorer détection d'unités"
            )
        
        # Recommendations based on failure
        if result.invalid_count > 0:
            result.transformation_recommendations.append(
                f"Améliorer parsing pour {result.invalid_count} formats non reconnus"
            )
            
            # Detail main failure causes 
            top_failures = dict(parsing_failures.most_common(3))
            if top_failures:
                failure_details = ", ".join([f"{reason}: {count}" for reason, count in top_failures.items()])
                result.quality_improvement_suggestions.append(
                    f"Causes d'échec principales: {failure_details}"
                )
        
        # Recommendations on utnits
        unit_analysis = result.pattern_analysis.get("unit_frequency_analysis", {})
        categories = unit_analysis.get("categories", {})
        
        if categories.get("weight", 0) > categories.get("volume", 0):
            result.transformation_recommendations.append(
                "Unités de poids dominantes - bon pour chocolats"
            )
        
        if categories.get("other", 0) > 0:
            result.quality_improvement_suggestions.append(
                f"Unités non standard détectées: {categories['other']} cas à investiguer"
            )
        
        # Recommendations on missing data
        if result.missing_count > 0:
            result.quality_improvement_suggestions.append(
                f"Améliorer complétude: {result.missing_count} produits sans données de quantité"
            )