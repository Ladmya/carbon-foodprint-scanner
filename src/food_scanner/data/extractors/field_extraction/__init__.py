"""
Field extraction module for product extraction
Responsibility: PHASE 3 - Extract and parse individual fields from enriched API data
"""
from .product_field_extraction import ProductFieldExtraction
from .weight_parser import WeightParser

__all__ = ['ProductFieldExtraction', 'WeightParser']
