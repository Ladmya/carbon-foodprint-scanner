"""
src/food_scanner/data/analysis/nutriscore_analyzer.py
Specialized analyzer for nutriscore_grade and nutriscore_score with cross-validation
"""

from typing import Dict, List, Any, Optional
from collections import Counter, defaultdict
from .base_analyzer import BaseFieldAnalyzer
from ...core.models.data_quality import FieldAnalysisResult, FieldType


class NutriscoreAnalyzer(BaseFieldAnalyzer):
    """
    Analyzer for nutriscore_grade et nutriscore_score
    Responsability: Multi-sources extraction  + grade ↔ score cross validation
    """
    
    def analyze_field(self, products: List[Dict[str, Any]]) -> FieldAnalysisResult:
        # Define the type of the analyzed field
        field_type = FieldType.CATEGORICAL if "grade" in self.field_name else FieldType.NUMERIC
        
        result = FieldAnalysisResult(
            field_name=self.field_name,
            field_type=field_type,
            total_products=len(products)
        )
        
        grade_distribution = Counter()
        score_distribution = []
        extraction_sources = Counter()
        examples = defaultdict(list)
        cross_validation_issues = []
        
        for product in products:
            if self.field_name == "nutriscore_grade":
                self._analyze_nutriscore_grade(product, result, grade_distribution, 
                                                extraction_sources, examples, cross_validation_issues)
            elif self.field_name == "nutriscore_score":
                self._analyze_nutriscore_score(product, result, score_distribution, 
                                                extraction_sources, examples)
        
        # Distribution of values
        if self.field_name == "nutriscore_grade":
            result.value_distribution = dict(grade_distribution)
        elif score_distribution:
            # Create bins for scores
            result.value_distribution = self._create_score_bins(score_distribution)
        
        # Analyze patterns
        result.pattern_analysis = {
            "extraction_sources": dict(extraction_sources),
            "cross_validation_issues": cross_validation_issues,
        }
        
        # Add dedicated statistics according to the type
        if self.field_name == "nutriscore_grade":
            result.pattern_analysis.update({
                "grade_distribution": dict(grade_distribution),
                "grade_completeness_by_source": self._analyze_grade_sources(products)
            })
        elif self.field_name == "nutriscore_score":
            result.pattern_analysis.update({
                "score_statistics": self._calculate_score_statistics(score_distribution),
                "score_grade_consistency": self._analyze_score_grade_consistency(products)
            })
        
        result.examples = dict(examples)
        
        # Recommendations
        self._generate_nutriscore_recommendations(result, extraction_sources, cross_validation_issues)
        
        self._calculate_base_metrics(result)
        return result
    
    def _analyze_nutriscore_grade(
            self, product: Dict[str, Any], 
            result: FieldAnalysisResult,
            grade_distribution: Counter, 
            extraction_sources: Counter,
            examples: Dict[str, List],
            cross_validation_issues: List
        ):
        """Analyze nutriscore grade extraction"""
        barcode = self._get_barcode_for_example(product)
        
        # Try multi-source extraction 
        grade, source = self._extract_nutriscore_grade_with_source(product)
        
        if grade:
            result.present_count += 1
            result.valid_count += 1
            grade_distribution[grade] += 1
            extraction_sources[source] += 1
            
            # Cross validation with score if available 
            score = self._extract_nutriscore_score(product)
            if score is not None:
                expected_grade = self._score_to_grade(score)
                if expected_grade != grade:
                    cross_validation_issues.append({
                        "barcode": barcode,
                        "grade_found": grade,
                        "score_found": score,
                        "expected_grade_from_score": expected_grade,
                        "issue": "Grade/score inconsistency"
                    })
            
            self._add_example(examples, "valid_grades", {
                "barcode": barcode,
                "grade": grade,
                "source": source,
                "product": self._get_product_name_for_example(product)
            })
            
        else:
            result.missing_count += 1
            
            # Check if we can calculate from the score
            score = self._extract_nutriscore_score(product)
            if score is not None:
                calculated_grade = self._score_to_grade(score)
                result.fallback_used_count += 1
                extraction_sources["calculated_from_score"] += 1
                
                self._add_example(examples, "calculated_from_score", {
                    "barcode": barcode,
                    "score": score,
                    "calculated_grade": calculated_grade,
                    "product": self._get_product_name_for_example(product)
                })
            else:
                self._add_example(examples, "missing", {
                    "barcode": barcode,
                    "product": self._get_product_name_for_example(product),
                    "available_fields": self._get_available_nutriscore_fields(product)
                })
    
    def _analyze_nutriscore_score(
            self, product: Dict[str, Any], 
            result: FieldAnalysisResult,
            score_distribution: List[float], 
            extraction_sources: Counter,
            examples: Dict[str, List]
        ):
        """Analyze nutriscore score extraction"""
        barcode = self._get_barcode_for_example(product)
        
        # Tenter extraction depuis plusieurs sources
        score, source = self._extract_nutriscore_score_with_source(product)
        
        if score is not None:
            result.present_count += 1
            
            if self._is_valid_nutriscore_score(score):
                result.valid_count += 1
                score_distribution.append(score)
                extraction_sources[source] += 1
                
                # Calculate corresponding grade
                calculated_grade = self._score_to_grade(score)
                
                self._add_example(examples, "valid_scores", {
                    "barcode": barcode,
                    "score": score,
                    "calculated_grade": calculated_grade,
                    "source": source,
                    "product": self._get_product_name_for_example(product)
                })
                
            else:
                result.invalid_count += 1
                self._add_example(examples, "invalid_range", {
                    "barcode": barcode,
                    "score": score,
                    "issue": f"Score {score} outside valid range (-15 to 40)",
                    "product": self._get_product_name_for_example(product)
                })
        else:
            result.missing_count += 1
            self._add_example(examples, "missing", {
                "barcode": barcode,
                "product": self._get_product_name_for_example(product),
                "available_fields": self._get_available_nutriscore_fields(product)
            })
    
    def _extract_nutriscore_grade_with_source(self, product: Dict[str, Any]) -> tuple[Optional[str], str]:
        """Extract nutriscore grade while identifying the source"""
        
        # Path 1: nutriscore.grade (nested structure)
        nutriscore_data = product.get('nutriscore', {})
        if isinstance(nutriscore_data, dict):
            grade = nutriscore_data.get('grade')
            if self._is_valid_nutriscore_grade(grade):
                return grade.upper(), "nutriscore.grade"
        
        # Path 2: Direct nutriscore_grade field
        grade = product.get('nutriscore_grade')
        if self._is_valid_nutriscore_grade(grade):
            return grade.upper(), "nutriscore_grade"
        
        # Path 3: nutrition_grades (alternative field name)
        nutrition_grades = product.get('nutrition_grades')
        if self._is_valid_nutriscore_grade(nutrition_grades):
            return nutrition_grades.upper(), "nutrition_grades"
        
        # Path 4: nutrition_grade_fr (French version)
        nutrition_grade_fr = product.get('nutrition_grade_fr')
        if self._is_valid_nutriscore_grade(nutrition_grade_fr):
            return nutrition_grade_fr.upper(), "nutrition_grade_fr"
        
        return None, "not_found"
    
    def _extract_nutriscore_score_with_source(self, product: Dict[str, Any]) -> tuple[Optional[float], str]:
        """Extract nutriscore score while identifying the source"""
        
        # Path 1: nutriscore.score (nested structure)
        nutriscore_data = product.get('nutriscore', {})
        if isinstance(nutriscore_data, dict):
            score = nutriscore_data.get('score')
            if score is not None:
                try:
                    return float(score), "nutriscore.score"
                except (ValueError, TypeError):
                    pass
        
        # Path 2: Direct nutriscore_score field
        score = product.get('nutriscore_score')
        if score is not None:
            try:
                return float(score), "nutriscore_score"
            except (ValueError, TypeError):
                pass
        
        # Path 3: nutrition_score_fr (French version)
        score_fr = product.get('nutrition_score_fr')
        if score_fr is not None:
            try:
                return float(score_fr), "nutrition_score_fr"
            except (ValueError, TypeError):
                pass
        
        return None, "not_found"
    
    def _extract_nutriscore_score(self, product: Dict[str, Any]) -> Optional[float]:
        """Extract nutriscore score (no source info)"""
        score, _ = self._extract_nutriscore_score_with_source(product)
        return score
    
    def _is_valid_nutriscore_grade(self, grade: Any) -> bool:
        """Check if the nutriscore grade is valid"""
        if not grade or not isinstance(grade, str):
            return False
        return grade.upper() in ['A', 'B', 'C', 'D', 'E']
    
    def _is_valid_nutriscore_score(self, score: Any) -> bool:
        """Check if the nutriscore score is in validated range"""
        if score is None:
            return False
        try:
            score_val = float(score)
            return -15 <= score_val <= 40
        except (ValueError, TypeError):
            return False
    
    def _score_to_grade(self, score: float) -> str:
        """Convert nutriscore score to grade according to official conversion formula"""
        if score <= -1:
            return 'A'
        elif score <= 2:
            return 'B'
        elif score <= 10:
            return 'C'
        elif score <= 18:
            return 'D'
        else:
            return 'E'
    
    def _create_score_bins(self, score_distribution: List[float]) -> Dict[str, int]:
        """Create bins for score distribution"""
        bins = {
            "A_range(-15:-1)": 0,
            "B_range(-1:2)": 0,
            "C_range(2:10)": 0,
            "D_range(10:18)": 0,
            "E_range(18+)": 0
        }
        
        for score in score_distribution:
            if score <= -1:
                bins["A_range(-15:-1)"] += 1
            elif score <= 2:
                bins["B_range(-1:2)"] += 1
            elif score <= 10:
                bins["C_range(2:10)"] += 1
            elif score <= 18:
                bins["D_range(10:18)"] += 1
            else:
                bins["E_range(18+)"] += 1
        
        return bins
    
    def _calculate_score_statistics(self, score_distribution: List[float]) -> Dict[str, Any]:
        """Calculate statistics for scores"""
        if not score_distribution:
            return {"count": 0}
        
        return {
            "count": len(score_distribution),
            "average": sum(score_distribution) / len(score_distribution),
            "median": sorted(score_distribution)[len(score_distribution)//2],
            "min": min(score_distribution),
            "max": max(score_distribution),
            "std_dev": self._calculate_std_dev(score_distribution),
            "quartiles": self._calculate_quartiles(score_distribution)
        }
    
    def _calculate_std_dev(self, values: List[float]) -> float:
        """Calculer écart-type"""
        if len(values) < 2:
            return 0
        mean = sum(values) / len(values)
        variance = sum((x - mean) ** 2 for x in values) / len(values)
        return variance ** 0.5
    
    def _calculate_quartiles(self, values: List[float]) -> Dict[str, float]:
        """Calculer quartiles"""
        sorted_values = sorted(values)
        n = len(sorted_values)
        
        return {
            "Q1": sorted_values[n//4],
            "Q2": sorted_values[n//2],
            "Q3": sorted_values[3*n//4]
        }
    
    def _analyze_grade_sources(self, products: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyser la complétude par source pour les grades"""
        source_stats = defaultdict(int)
        
        for product in products:
            _, source = self._extract_nutriscore_grade_with_source(product)
            source_stats[source] += 1
        
        return dict(source_stats)
    
    def _analyze_score_grade_consistency(self, products: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyser la cohérence entre scores et grades"""
        consistency_stats = {
            "total_with_both": 0,
            "consistent": 0,
            "inconsistent": 0,
            "inconsistency_examples": []
        }
        
        for product in products:
            grade = self._extract_nutriscore_grade_with_source(product)[0]
            score = self._extract_nutriscore_score(product)
            
            if grade and score is not None:
                consistency_stats["total_with_both"] += 1
                expected_grade = self._score_to_grade(score)
                
                if expected_grade == grade:
                    consistency_stats["consistent"] += 1
                else:
                    consistency_stats["inconsistent"] += 1
                    
                    if len(consistency_stats["inconsistency_examples"]) < 5:
                        consistency_stats["inconsistency_examples"].append({
                            "barcode": self._get_barcode_for_example(product),
                            "grade": grade,
                            "score": score,
                            "expected_grade": expected_grade
                        })
        
        return consistency_stats
    
    def _get_available_nutriscore_fields(self, product: Dict[str, Any]) -> Dict[str, Any]:
        """Get available nutriscore fields for debug"""
        fields = {}
        
        # Champs possibles
        possible_fields = [
            'nutriscore', 'nutriscore_grade', 'nutriscore_score',
            'nutrition_grades', 'nutrition_grade_fr', 'nutrition_score_fr'
        ]
        
        for field in possible_fields:
            value = product.get(field)
            if value is not None:
                fields[field] = value
        
        return fields
    
    def _generate_nutriscore_recommendations(
            self, result: FieldAnalysisResult,
            extraction_sources: Counter,
            cross_validation_issues: List
        ):
        """Generate dedicated recommendations for nutriscore"""
        
        # Recommendations on extraction sources
        calculated_from_score = extraction_sources.get("calculated_from_score", 0)
        if calculated_from_score > 0:
            result.transformation_recommendations.append(
                f"Calcul de grade depuis score utilisé pour {calculated_from_score} produits"
            )
        
        # Recommendations on multi-sources
        if len(extraction_sources) > 1:
            sources_list = ", ".join([f"{source}: {count}" for source, count in extraction_sources.items()])
            result.transformation_recommendations.append(
                f"Sources multiples détectées: {sources_list}"
            )
        
        # Recommendations on cross validation
        if cross_validation_issues:
            result.quality_improvement_suggestions.append(
                f"Incohérences grade/score détectées: {len(cross_validation_issues)} cas"
            )
            result.transformation_recommendations.append(
                "Implémenter validation croisée grade ↔ score pour détecter erreurs"
            )
        
        # Recommendations on missing data
        if result.missing_count > 0:
            if self.field_name == "nutriscore_grade":
                result.quality_improvement_suggestions.append(
                    f"Grades nutriscore manquants: {result.missing_count} produits"
                )
            else:
                result.quality_improvement_suggestions.append(
                    f"Scores nutriscore manquants: {result.missing_count} produits"
                )
        
        # Recommendations for scores
        if self.field_name == "nutriscore_score":
            score_stats = result.pattern_analysis.get("score_statistics", {})
            if score_stats.get("count", 0) > 0:
                avg_score = score_stats.get("average", 0)
                result.transformation_recommendations.append(
                    f"Score moyen: {avg_score:.1f} (distribution: {result.value_distribution})"
                )
        
        # Recommendations general quality
        if result.validity_rate < 90 and result.present_count > 0:
            result.quality_improvement_suggestions.append(
                f"Qualité nutriscore sous-optimale: {result.validity_rate:.1f}% de validité"
            )