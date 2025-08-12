"""
src/food_scanner/data/transformers/cleaning/product_cleaner_processor.py
DATA CLEANER: Clean and normalize product data

RESPONSIBILITIES:
- Orchestrate specialized cleaning components
- Clean and normalize product names
- Process brand names, weights, units, and scores
- Track cleaning statistics
- Coordinate data quality monitoring
"""

import re
from typing import Dict, List, Any, Optional

from food_scanner.data.transformers.cleaning.brand_name_cleaner import BrandNameCleaner
from food_scanner.data.transformers.cleaning.unit_normalizer import UnitNormalizer
from food_scanner.data.transformers.cleaning.weight_normalizer import WeightNormalizer
from food_scanner.data.transformers.cleaning.nutriscore_normalizer import NutriscoreNormalizer


class ProductCleanerProcessor:
    """
    RESPONSIBILITY: Orchestrate the processing of product data cleaning and normalization
    
    BUSINESS LOGIC:
    1. Coordinate specialized cleaning components
    2. Process product names, brands, weights, units, and scores
    3. Track cleaning statistics across all components
    4. Monitor data quality issues
    """
    
    def __init__(self):
        # Initialize specialized cleaning components
        self.brand_cleaner = BrandNameCleaner()
        self.unit_normalizer = UnitNormalizer()
        self.weight_normalizer = WeightNormalizer()
        self.score_normalizer = NutriscoreNormalizer()
        
        # Aggregated cleaning statistics
        self.cleaning_stats = {
            "brand_names_cleaned": 0,
            "product_names_cleaned": 0,
            "weights_normalized": 0,
            "units_normalized": 0,
            "brand_names_normalized": 0,
            "brand_consolidations": 0,
            "nutriscore_grades_normalized": 0,
            "nutriscore_scores_normalized": 0,
            "eco_scores_normalized": 0
        }
        
        # Data quality tracking
        self.data_quality_issues = []
    
    def process_product_data(
        self, 
        extracted_fields: Dict[str, Any], 
        barcode: str
    ) -> Dict[str, Any]:
        """
        Process and track cleaning of all product data fields
        
        Args:
            extracted_fields: Raw extracted fields
            barcode: Product barcode for tracking
            
        Returns:
            Cleaned product data
        """
        processed_product = {
            "barcode": barcode,
            "product_name": self.process_product_name(extracted_fields.get("product_name", "")),
            "brand_name": self.process_brand_name(extracted_fields.get("brand_name", ""), barcode),
            "brand_tags": extracted_fields.get("brand_tags", []),
            "weight": self.process_weight(extracted_fields.get("weight")),
            "product_quantity_unit": self.process_unit(extracted_fields.get("product_quantity_unit", "")),
            "nutriscore_grade": self.process_nutriscore_grade(extracted_fields.get("nutriscore_grade")),
            "nutriscore_score": self.process_nutriscore_score(extracted_fields.get("nutriscore_score")),
            "eco_score": self.process_ecoscore(extracted_fields.get("eco_score")),
            "co2_total": self.process_co2_data(extracted_fields.get("co2_sources", {}))
        }
        
        return processed_product
    
    def process_product_name(self, product_name: str) -> str:
        """
        Process and clean product name
        
        Args:
            product_name: Raw product name
            
        Returns:
            Cleaned product name
        """
        if not product_name:
            return ""
        
        # Basic cleaning
        cleaned = product_name.strip()
        
        # Remove excessive whitespace
        cleaned = re.sub(r'\s+', ' ', cleaned)
        
        # Limit length to reasonable size
        if len(cleaned) > 200:
            cleaned = cleaned[:197] + "..."
            self.data_quality_issues.append(f"Product name truncated: was {len(product_name)} chars")
        
        if cleaned != product_name:
            self.cleaning_stats["product_names_cleaned"] += 1
        
        return cleaned

    def process_brand_name(self, brand_name: str, barcode: str) -> str:
        """
        Process and clean brand name using BrandNameCleaner
        
        Args:
            brand_name: Raw brand name
            barcode: Product barcode for tracking
            
        Returns:
            Cleaned brand name
        """
        if not brand_name:
            return ""
    
        # Use the dedicated brand cleaner
        cleaned_brand, cleaning_details = self.brand_cleaner.clean_brand_name(brand_name)
    
        # Track cleaning statistics
        if cleaned_brand != brand_name:
            self.cleaning_stats["brand_names_cleaned"] += 1
        
            # Check if this was a normalization (case/accent change)
            if cleaning_details.get("normalized") or cleaning_details.get("canonical_mapped"):
                self.cleaning_stats["brand_names_normalized"] += 1
        
            # Check if this was a consolidation (primary brand extraction)
            if cleaning_details.get("primary_extracted"):
                self.cleaning_stats["brand_consolidations"] += 1
        
            # Log significant transformations
            if len(brand_name) > 30 or "," in brand_name:
                self.data_quality_issues.append(
                    f"Brand transformed for {barcode}: '{brand_name[:50]}...' → '{cleaned_brand}'"
                )
    
        return cleaned_brand
    
    def process_weight(self, weight: Any) -> Optional[float]:
        """
        Process and normalize weight using WeightNormalizer
        
        Args:
            weight: Raw weight value
            
        Returns:
            Normalized weight in grams or None if invalid
        """
        normalized_weight = self.weight_normalizer.normalize_weight(weight)
        
        # Update aggregated stats
        weight_stats = self.weight_normalizer.get_normalization_stats()
        self.cleaning_stats["weights_normalized"] = weight_stats["weights_normalized"]
        
        # Collect data quality issues
        self.data_quality_issues.extend(self.weight_normalizer.get_data_quality_issues())
        
        return normalized_weight
    
    def process_unit(self, unit: str) -> str:
        """
        Process and normalize unit using UnitNormalizer
        
        Args:
            unit: Raw unit string
            
        Returns:
            Normalized unit string
        """
        normalized = self.unit_normalizer.normalize_unit(unit)
        
        if normalized != unit:
            self.cleaning_stats["units_normalized"] += 1
        
        return normalized or "g"  # Fallback to grams if normalization fails
    
    def process_nutriscore_grade(self, grade: Any) -> Optional[str]:
        """
        Process and normalize nutriscore grade using NutriscoreNormalizer
        
        Args:
            grade: Raw nutriscore grade
            
        Returns:
            Normalized nutriscore grade or None if invalid
        """
        normalized_grade = self.score_normalizer.normalize_nutriscore_grade(grade)
        
        # Update aggregated stats
        score_stats = self.score_normalizer.get_normalization_stats()
        self.cleaning_stats["nutriscore_grades_normalized"] = score_stats["nutriscore_grades_normalized"]
        
        return normalized_grade
    
    def process_nutriscore_score(self, score: Any) -> Optional[int]:
        """
        Process and normalize nutriscore score using NutriscoreNormalizer
        
        Args:
            score: Raw nutriscore score
            
        Returns:
            Normalized nutriscore score or None if invalid
        """
        normalized_score = self.score_normalizer.normalize_nutriscore_score(score)
        
        # Update aggregated stats
        score_stats = self.score_normalizer.get_normalization_stats()
        self.cleaning_stats["nutriscore_scores_normalized"] = score_stats["nutriscore_scores_normalized"]
        
        return normalized_score
    
    def process_ecoscore(self, ecoscore: Any) -> Optional[str]:
        """
        Process and normalize eco score using NutriscoreNormalizer
        
        Args:
            ecoscore: Raw eco score
            
        Returns:
            Normalized eco score or None if invalid
        """
        normalized_ecoscore = self.score_normalizer.normalize_ecoscore(ecoscore)
        
        # Update aggregated stats
        score_stats = self.score_normalizer.get_normalization_stats()
        self.cleaning_stats["eco_scores_normalized"] = score_stats["eco_scores_normalized"]
        
        return normalized_ecoscore
    
    def process_co2_data(self, co2_sources: Dict[str, Optional[float]]) -> Optional[float]:
        """
        Process and extract CO2 value using priority order
        
        Args:
            co2_sources: Dictionary of CO2 source values
            
        Returns:
            Extracted CO2 value or None if not available
        """
        # Priority order (same as extractor)
        priority_sources = [
            "agribalyse_total",
            "ecoscore_agribalyse_total", 
            "nutriments_carbon_footprint",
            "nutriments_known_ingredients"
        ]
        
        for source in priority_sources:
            value = co2_sources.get(source)
            if value is not None and 0 <= value <= 10000:
                return round(value, 3)
        
        return None
    
    def get_cleaning_stats(self) -> Dict[str, Any]:
        """Get aggregated cleaning statistics from all components"""
        # Update aggregated stats from specialized components
        brand_stats = self.brand_cleaner.get_cleaning_stats()
        weight_stats = self.weight_normalizer.get_normalization_stats()
        score_stats = self.score_normalizer.get_normalization_stats()
        
        # Merge all stats
        all_stats = {
            **self.cleaning_stats,
            "brands_processed": brand_stats.get("brands_processed", 0),
            "brands_cleaned": brand_stats.get("brands_cleaned", 0),
            "cleaning_rate": brand_stats.get("cleaning_rate", 0),
            "primary_brand_extracted": brand_stats.get("primary_brand_extracted", 0),
            "case_normalized": brand_stats.get("case_normalized", 0),
            "mapped_to_canonical": brand_stats.get("mapped_to_canonical", 0),
            "weights_rejected": weight_stats.get("weights_rejected", 0),
            "invalid_weight_values": weight_stats.get("invalid_values", 0),
            "invalid_scores": score_stats.get("invalid_scores", 0)
        }
        
        return all_stats
    
    def get_brand_cleaning_stats(self) -> Dict[str, Any]:
        """Get brand cleaner statistics"""
        return self.brand_cleaner.get_cleaning_stats()
    
    def get_data_quality_issues(self) -> List[str]:
        """Get all data quality issues from all components"""
        all_issues = self.data_quality_issues.copy()
        all_issues.extend(self.weight_normalizer.get_data_quality_issues())
        return all_issues
    
    def reset_stats(self):
        """Reset statistics for all components"""
        self.cleaning_stats = {
            "brand_names_cleaned": 0,
            "product_names_cleaned": 0,
            "weights_normalized": 0,
            "units_normalized": 0,
            "brand_names_normalized": 0,
            "brand_consolidations": 0,
            "nutriscore_grades_normalized": 0,
            "nutriscore_scores_normalized": 0,
            "eco_scores_normalized": 0
        }
        self.data_quality_issues = []
        
        # Reset specialized component stats
        self.weight_normalizer.reset_stats()
        self.score_normalizer.reset_stats()
