"""
src/food_scanner/data/transformers/cleaning/nutriscore_normalizer.py
NUTRISCORE NORMALIZER: Normalize nutrition and eco scores

RESPONSIBILITIES:
- Normalize nutriscore grades (A, B, C, D, E)
- Normalize nutriscore scores (-15 to 40)
- Normalize eco scores (A, B, C, D, E)
- Track score normalization statistics
- Validate score ranges and formats
"""

from typing import Any, Optional, Dict


class NutriscoreNormalizer:
    """
    RESPONSIBILITY: Normalize nutrition and eco scores
    
    BUSINESS LOGIC:
    1. Normalize nutriscore grades to uppercase letters
    2. Validate nutriscore scores within range (-15 to 40)
    3. Normalize eco scores to uppercase letters
    4. Track normalization statistics
    """
    
    def __init__(self):
        # Score validation constants
        self.NUTRISCORE_MIN = -15
        self.NUTRISCORE_MAX = 40
        self.VALID_GRADES = ['A', 'B', 'C', 'D', 'E']
        
        # Normalization statistics
        self.normalization_stats = {
            "nutriscore_grades_normalized": 0,
            "nutriscore_scores_normalized": 0,
            "eco_scores_normalized": 0,
            "invalid_scores": 0
        }
    
    def normalize_nutriscore_grade(self, grade: Any) -> Optional[str]:
        """
        Normalize nutriscore grade to uppercase letter
        
        Args:
            grade: Raw nutriscore grade
            
        Returns:
            Normalized grade (A, B, C, D, E) or None if invalid
        """
        if not grade:
            return None
        
        if isinstance(grade, str):
            normalized_grade = grade.strip().upper()
            
            if normalized_grade in self.VALID_GRADES:
                if normalized_grade != grade:
                    self.normalization_stats["nutriscore_grades_normalized"] += 1
                return normalized_grade
        
        return None
    
    def normalize_nutriscore_score(self, score: Any) -> Optional[int]:
        """
        Normalize nutriscore score to integer within valid range
        
        Args:
            score: Raw nutriscore score
            
        Returns:
            Normalized score (-15 to 40) or None if invalid
        """
        if score is None:
            return None
        
        try:
            score_int = int(score)
            
            if self.NUTRISCORE_MIN <= score_int <= self.NUTRISCORE_MAX:
                if score_int != score:
                    self.normalization_stats["nutriscore_scores_normalized"] += 1
                return score_int
            else:
                self.normalization_stats["invalid_scores"] += 1
                return None
                
        except (ValueError, TypeError):
            self.normalization_stats["invalid_scores"] += 1
            return None
    
    def normalize_ecoscore(self, ecoscore: Any) -> Optional[str]:
        """
        Normalize eco score to uppercase letter
        
        Args:
            ecoscore: Raw eco score
            
        Returns:
            Normalized eco score (A, B, C, D, E) or None if invalid
        """
        if not ecoscore:
            return None
        
        if isinstance(ecoscore, str):
            normalized_ecoscore = ecoscore.strip().upper()
            
            if normalized_ecoscore in self.VALID_GRADES:
                if normalized_ecoscore != ecoscore:
                    self.normalization_stats["eco_scores_normalized"] += 1
                return normalized_ecoscore
        
        return None

    def get_normalization_stats(self) -> Dict[str, Any]:
        """Get current score normalization statistics"""
        return self.normalization_stats.copy()
    
    def reset_stats(self):
        """Reset score normalization statistics"""
        self.normalization_stats = {
            "nutriscore_grades_normalized": 0,
            "nutriscore_scores_normalized": 0,
            "eco_scores_normalized": 0,
            "invalid_scores": 0
        }
