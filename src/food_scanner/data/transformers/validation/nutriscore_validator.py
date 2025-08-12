"""
src/food_scanner/data/transformers/validation/nutriscore_validator.py
NutriscoreValidator: 
    - Validate nutriscore data for grades and scores (business rules)
"""


from typing import Dict, Any

class NutriscoreValidator:
    """
    RESPONSIBILITY: Validate nutriscore data (business rules)
    
    SÉPARATION CLAIRE:
    - NutriscoreNormalizer = transformation des données
    - NutriscoreValidator = validation des règles business
    """
    
    def __init__(self):
        self.NUTRISCORE_MIN = -15
        self.NUTRISCORE_MAX = 40
        self.VALID_GRADES = ['A', 'B', 'C', 'D', 'E']
        
        self.validation_stats = {
            "grades_validated": 0,
            "scores_validated": 0,
            "products_validated": 0,
            "validation_failures": 0,
            "grade_failures": 0,
            "score_failures": 0,
            "missing_nutriscore_failures": 0
        }


    def validate_product_nutriscore(self, extracted_fields: Dict[str, Any], success_flags: Dict[str, bool]) -> Dict[str, Any]:
        """
        FULL NUTRISCORE VALIDATION with ALL business logic
        
        Args:
            extracted_fields: Extracted product data
            success_flags: Success flags for extraction
            
        Returns:
            Validation result with rejection_reasons
        """
        self.validation_stats["products_validated"] += 1
        rejection_reasons = []
        
        # Extract data (business logic in the validator)
        nutriscore_grade = extracted_fields.get("nutriscore_grade")
        nutriscore_score = extracted_fields.get("nutriscore_score")
        grade_success = success_flags.get("nutriscore_grade", False)
        score_success = success_flags.get("nutriscore_score", False)
        
        # Logic 1: At least one nutriscore required 
        has_grade = grade_success and nutriscore_grade
        has_score = score_success and nutriscore_score is not None
        
        if not has_grade and not has_score:
            rejection_reasons.append("Missing nutriscore data (need grade OR score)")
            self.validation_stats["missing_nutriscore_failures"] += 1
        
        # Logic 2: Validate grade if present
        if has_grade:
            grade_validation = self._validate_nutriscore_grade(nutriscore_grade)
            if not grade_validation["is_valid"]:
                rejection_reasons.append(f"Invalid nutriscore grade: {grade_validation['reason']}")
                self.validation_stats["grade_failures"] += 1
        
        # Logic 3: Validate score if present
        if has_score:
            score_validation = self._validate_nutriscore_score(nutriscore_score)
            if not score_validation["is_valid"]:
                rejection_reasons.append(f"Invalid nutriscore score: {score_validation['reason']}")
                self.validation_stats["score_failures"] += 1
        
        # Statistiques
        if rejection_reasons:
            self.validation_stats["validation_failures"] += 1
        
        return {
            "is_valid": len(rejection_reasons) == 0,
            "rejection_reasons": rejection_reasons,
            "has_grade": has_grade,
            "has_score": has_score,
            "grade_value": nutriscore_grade if has_grade else None,
            "score_value": nutriscore_score if has_score else None
        }        

    
    def validate_nutriscore_grade(self, grade: str) -> Dict[str, Any]:  # TODO: decide to use this method or remove it
        """
        Validate nutriscore grade format
        
        Args:
            grade: Grade to validate
            
        Returns:
            Validation result with details
        """
        self.validation_stats["grades_validated"] += 1

        if not grade:
            self.validation_stats["grade_failures"] += 1
            return {
                "is_valid": False,
                "reason": "Grade is empty",
                "suggestion": "Provide a nutriscore grade (A, B, C, D, E)"
            }
        
        if not isinstance(grade, str):
            self.validation_stats["grade_failures"] += 1
            return {
                "is_valid": False,
                "reason": f"Grade is not a string: {type(grade)}",
                "suggestion": "Provide a string value"
            }
        
        normalized = grade.strip().upper()
        if normalized not in self.VALID_GRADES:
            self.validation_stats["grade_failures"] += 1
            return {
                "is_valid": False,
                "reason": f"Invalid grade: {grade}",
                "suggestion": f"Grade must be one of: {', '.join(self.VALID_GRADES)}"
            }
        
        return {
            "is_valid": True,
            "reason": "Grade is valid",
            "suggestion": None
        }
    
    def validate_nutriscore_score(self, score: int) -> Dict[str, Any]: # TODO: decide to use this method or remove it
        """
        Validate nutriscore score range
        
        Args:
            score: Score to validate
            
        Returns:
            Validation result with details
        """
        self.validation_stats["scores_validated"] += 1
        if not isinstance(score, (int, float)):
            self.validation_stats["score_failures"] += 1
            return {
                "is_valid": False,
                "reason": f"Score is not numeric: {type(score)}",
            }
        
        if score < self.NUTRISCORE_MIN:
            self.validation_stats["score_failures"] += 1
            return {
                "is_valid": False,
                "reason": f"Score too low: {score} (minimum: {self.NUTRISCORE_MIN})"            }

        if score > self.NUTRISCORE_MAX:
            self.validation_stats["score_failures"] += 1
            return {
                "is_valid": False,
                "reason": f"Score too high: {score}(maximum: {self.NUTRISCORE_MAX})"
            }

        if score is None:
            self.validation_stats["score_failures"] += 1
            return {
                "is_valid": False,
                "reason": "Score is None",
            }
        
        return {
            "is_valid": True,
            "reason": "Score is within valid range",
        }
            
    def get_validation_stats(self) -> Dict[str, Any]:
        """Get validation statistics"""
        return self.validation_stats.copy()            