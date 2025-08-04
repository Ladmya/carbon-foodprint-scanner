"""
src/food_scanner/data/analysis/base_analyzer.py
Base class for all field analyzers with common functionality
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Any
from food_scanner.core.models.data_quality import FieldAnalysisResult


class BaseFieldAnalyzer(ABC):
    """
    Base analyzer for all fields
    Responsability: Shared interface & basic metrics
    No validation_rule dependency - each analyzer manages its own field logic
    """
    
    def __init__(self):
        """Simple initialization - no dependencies"""
        pass
        
    def _calculate_base_metrics(self, result: FieldAnalysisResult) -> None:
        """Calculate basic metrics for each analyzer"""
        total = result.total_products
        if total > 0:
            result.presence_rate = (result.present_count / total) * 100
            result.validity_rate = (result.valid_count / total) * 100
            
            # Quality score based on availability and validity
            presence_weight = 0.6
            validity_weight = 0.4
            result.quality_score = (
                (result.presence_rate * presence_weight) + 
                (result.validity_rate * validity_weight)
            )
    
    def _is_empty_value(self, value: Any) -> bool:
        """Check if a value is considered empty"""
        if value is None:
            return True
        if isinstance(value, str) and not value.strip():
            return True
        if isinstance(value, (list, dict)) and len(value) == 0:
            return True
        return False
    
    def _is_valid_string(self, value: Any) -> bool:
        """Check if a value is a valid String (not empty)"""
        return isinstance(value, str) and value.strip()
    
    def _add_example(self, examples: Dict[str, List], category: str, example: Dict, max_examples: int = 5):
        """Add an example to a category (with limits)"""
        if category not in examples:
            examples[category] = []
        if len(examples[category]) < max_examples:
            examples[category].append(example)
    
    def _get_barcode_for_example(self, product: Dict[str, Any]) -> str:
        """Extract barcode pour examples (with fallback)"""
        return product.get('code', product.get('barcode', 'unknown'))
    
    def _get_product_name_for_example(self, product: Dict[str, Any], max_length: int = 30) -> str:
        """Extract product name for examples (with fallback & troncate)"""
        name = (product.get('product_name_fr') or 
                product.get('product_name') or 
                'Unknown')
        return name[:max_length] if len(name) > max_length else name