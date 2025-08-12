"""
src/food_scanner/data/transformers/analysis/transformation_analyzer.py
TRANSFORMATION ANALYZER: Analyze transformation quality and production readiness

RESPONSIBILITIES:
- Assess production readiness of transformed products
- Calculate data quality grades
- Analyze data completeness
- Generate next steps recommendations
- Provide business intelligence insights
- Validate transformation quality
"""

from typing import Dict, List, Any


class TransformationAnalyzer:
    """
    RESPONSIBILITY: Analyze transformation quality and production readiness
    
    BUSINESS LOGIC:
    1. Assess production readiness based on data quality
    2. Calculate overall quality grades
    3. Analyze data completeness and critical fields
    4. Generate actionable next steps recommendations
    5. Provide business intelligence insights
    6. Validate transformation quality and detect anomalies
    """
    
    def __init__(self):
        self.analysis_stats = {
            "quality_assessments": 0,
            "completeness_analyses": 0,
            "recommendations_generated": 0,
            "quality_validations": 0,
            "anomalies_detected": 0
        }
        
        # Quality validation tracking
        self.quality_issues = []
        self.anomalies = []
    
    def validate_transformation_quality(
        self, 
        validated_products: Dict[str, Any]
        ) -> Dict[str, Any]:
        """
        Validate the quality of transformed products and detect anomalies
        
        Args:
            validated_products: Products to validate
            
        Returns:
            Quality validation results with detected issues
        """
        self.analysis_stats["quality_validations"] += 1
        
        quality_results = {
            "total_products_validated": len(validated_products),
            "quality_issues": [],
            "anomalies": [],
            "field_quality_scores": {},
            "overall_quality_score": 0.0
        }
        
        # Track field quality across all products
        field_quality = {
            "barcode": {"valid": 0, "invalid": 0},
            "product_name": {"valid": 0, "invalid": 0},
            "brand_name": {"valid": 0, "invalid": 0},
            "weight": {"valid": 0, "invalid": 0},
            "co2_total": {"valid": 0, "invalid": 0},
            "nutriscore_grade": {"valid": 0, "invalid": 0},
            "nutriscore_score": {"valid": 0, "invalid": 0},
            "eco_score": {"valid": 0, "invalid": 0}
        }
        
        for barcode, product_data in validated_products.items():
            transformed_data = product_data["transformed_data"]
            
            # Validate individual fields
            field_validation = self._validate_product_fields(transformed_data, barcode)
            
            # Update field quality counters
            for field, is_valid in field_validation.items():
                if is_valid:
                    field_quality[field]["valid"] += 1
                else:
                    field_quality[field]["invalid"] += 1
            
            # Detect anomalies
            product_anomalies = self._detect_product_anomalies(transformed_data, barcode)
            if product_anomalies:
                quality_results["anomalies"].extend(product_anomalies)
                self.anomalies.extend(product_anomalies)
        
        # Calculate field quality scores
        for field, counts in field_quality.items():
            total = counts["valid"] + counts["invalid"]
            if total > 0:
                score = (counts["valid"] / total) * 100
                quality_results["field_quality_scores"][field] = {
                    "score": round(score, 2),
                    "valid_count": counts["valid"],
                    "invalid_count": counts["invalid"],
                    "total_count": total
                }
        
        # Calculate overall quality score
        total_fields = len(field_quality)
        total_score = sum(
            quality_results["field_quality_scores"][field]["score"]
            for field in quality_results["field_quality_scores"]
        )
        quality_results["overall_quality_score"] = round(total_score / total_fields, 2)
        
        # Update statistics
        quality_results["quality_issues"] = self.quality_issues.copy()
        quality_results["anomalies"] = self.anomalies.copy()
        
        return quality_results
    
    def _validate_product_fields(
        self, 
        transformed_data: Dict[str, Any], 
        barcode: str
        ) -> Dict[str, bool]:
        """
        Validate individual product fields for quality
        
        Args:
            transformed_data: Transformed product data
            barcode: Product barcode for tracking
            
        Returns:
            Dictionary of field validation results
        """
        field_validation = {}
        
        # Validate barcode
        field_validation["barcode"] = bool(transformed_data.get("barcode"))
        
        # Validate product name
        product_name = transformed_data.get("product_name", "")
        field_validation["product_name"] = (
            bool(product_name) and 
            len(product_name) <= 200 and 
            not product_name.isspace()
        )
        
        # Validate brand name
        brand_name = transformed_data.get("brand_name", "")
        field_validation["brand_name"] = (
            bool(brand_name) and 
            len(brand_name) <= 100 and 
            not brand_name.isspace()
        )
        
        # Validate weight
        weight = transformed_data.get("weight")
        field_validation["weight"] = (
            weight is not None and 
            0.1 <= weight <= 10000
        )
        
        # Validate CO2 total
        co2_total = transformed_data.get("co2_total")
        field_validation["co2_total"] = (
            co2_total is not None and 
            0 <= co2_total <= 10000
        )
        
        # Validate nutriscore grade
        nutriscore_grade = transformed_data.get("nutriscore_grade")
        field_validation["nutriscore_grade"] = (
            nutriscore_grade is None or 
            nutriscore_grade in ['A', 'B', 'C', 'D', 'E']
        )
        
        # Validate nutriscore score
        nutriscore_score = transformed_data.get("nutriscore_score")
        field_validation["nutriscore_score"] = (
            nutriscore_score is None or 
            (-15 <= nutriscore_score <= 40)
        )
        
        # Validate eco score
        eco_score = transformed_data.get("eco_score")
        field_validation["eco_score"] = (
            eco_score is None or 
            eco_score in ['A', 'B', 'C', 'D', 'E']
        )
        
        return field_validation
    
    def _detect_product_anomalies(
        self, 
        transformed_data: Dict[str, Any], 
        barcode: str
        ) -> List[Dict[str, Any]]:
        """
        Detect anomalies in transformed product data
        
        Args:
            transformed_data: Transformed product data
            barcode: Product barcode for tracking
            
        Returns:
            List of detected anomalies
        """
        anomalies = []
        
        # Check for extremely long product names
        product_name = transformed_data.get("product_name", "")
        if len(product_name) > 150:
            anomalies.append({
                "type": "long_product_name",
                "barcode": barcode,
                "value": product_name[:50] + "...",
                "severity": "warning",
                "description": f"Product name is {len(product_name)} characters long"
            })
        
        # Check for extremely long brand names
        brand_name = transformed_data.get("brand_name", "")
        if len(brand_name) > 50:
            anomalies.append({
                "type": "long_brand_name",
                "barcode": barcode,
                "value": brand_name[:30] + "...",
                "severity": "warning",
                "description": f"Brand name is {len(brand_name)} characters long"
            })
        
        # Check for unusual weight values
        weight = transformed_data.get("weight")
        if weight is not None:
            if weight < 1:  # Less than 1g
                anomalies.append({
                    "type": "very_small_weight",
                    "barcode": barcode,
                    "value": weight,
                    "severity": "warning",
                    "description": f"Weight {weight}g seems unusually small for a food product"
                })
            elif weight > 5000:  # More than 5kg
                anomalies.append({
                    "type": "very_large_weight",
                    "barcode": barcode,
                    "value": weight,
                    "severity": "warning",
                    "description": f"Weight {weight}g seems unusually large for a food product"
                })
        
        # Check for extreme CO2 values
        co2_total = transformed_data.get("co2_total")
        if co2_total is not None:
            if co2_total > 5000:  # More than 5kg CO2 per 100g
                anomalies.append({
                    "type": "extreme_co2_value",
                    "barcode": barcode,
                    "value": co2_total,
                    "severity": "warning",
                    "description": f"CO2 value {co2_total}g/100g seems unusually high"
                })
        
        # Check for missing critical fields
        critical_fields = ["barcode", "product_name", "brand_name", "co2_total"]
        missing_critical = [field for field in critical_fields if not transformed_data.get(field)]
        
        if missing_critical:
            anomalies.append({
                "type": "missing_critical_fields",
                "barcode": barcode,
                "value": missing_critical,
                "severity": "error",
                "description": f"Missing critical fields: {', '.join(missing_critical)}"
            })
        
        return anomalies
    
    def assess_production_readiness(
        self, 
        validated_products: Dict[str, Any], 
        rejected_products: Dict[str, Any]
        ) -> Dict[str, Any]:
        """
        Assess production readiness of transformed products
        
        Args:
            validated_products: Successfully transformed products
            rejected_products: Rejected products
            
        Returns:
            Production readiness assessment with detailed metrics
        """
        total_processed = len(validated_products) + len(rejected_products)
        
        # Analyze data completeness
        completeness_analysis = self._analyze_data_completeness(validated_products)
        complete_products = completeness_analysis["complete_products_count"]
        
        # Calculate success rate
        success_rate = (complete_products / total_processed * 100) if total_processed > 0 else 0
        
        # Generate quality assessment
        quality_assessment = self._calculate_quality_grade(success_rate)
        
        # Generate next steps
        next_steps = self._generate_next_steps(complete_products, success_rate)
        
        # Update analysis statistics
        self.analysis_stats["quality_assessments"] += 1
        self.analysis_stats["completeness_analyses"] += 1
        self.analysis_stats["recommendations_generated"] += 1
        
        return {
            "total_products_processed": total_processed,
            "validated_products": len(validated_products),
            "rejected_products": len(rejected_products),
            "complete_products_ready_for_db": complete_products,
            "success_rate": success_rate,
            "bot_launch_ready": complete_products >= 100 and success_rate >= 80,
            "minimum_viable_dataset": complete_products >= 50 and success_rate >= 70,
            "data_quality_grade": quality_assessment,
            "next_steps": next_steps,
            "completeness_analysis": completeness_analysis
        }
    
    def _analyze_data_completeness(self, validated_products: Dict[str, Any]) -> Dict[str, Any]:
        """
        Analyze completeness of validated products data
        
        Args:
            validated_products: Products to analyze
            
        Returns:
            Detailed completeness analysis
        """
        total_products = len(validated_products)
        complete_products_count = 0
        field_completeness = {
            "barcode": 0,
            "product_name": 0,
            "brand_name": 0,
            "weight": 0,
            "co2_total": 0,
            "nutriscore_grade": 0,
            "nutriscore_score": 0,
            "eco_score": 0
        }
        
        for barcode, product_data in validated_products.items():
            transformed_data = product_data["transformed_data"]
            
            # Check critical fields
            has_critical_fields = all([
                transformed_data.get("barcode"),
                transformed_data.get("product_name"),
                transformed_data.get("brand_name"),
                transformed_data.get("co2_total") is not None
            ])
            
            # Check nutriscore data
            has_nutriscore = (
                transformed_data.get("nutriscore_grade") or 
                transformed_data.get("nutriscore_score") is not None
            )
            
            # Count complete products
            if has_critical_fields and has_nutriscore:
                complete_products_count += 1
            
            # Count field presence
            for field in field_completeness:
                if transformed_data.get(field) is not None:
                    field_completeness[field] += 1
        
        # Calculate field completeness percentages
        field_completeness_percentages = {
            field: (count / total_products * 100) if total_products > 0 else 0
            for field, count in field_completeness.items()
        }
        
        return {
            "complete_products_count": complete_products_count,
            "total_products": total_products,
            "completeness_rate": (complete_products_count / total_products * 100) if total_products > 0 else 0,
            "field_completeness": field_completeness,
            "field_completeness_percentages": field_completeness_percentages,
            "critical_fields_missing": total_products - complete_products_count
        }
    
    def _calculate_quality_grade(self, success_rate: float) -> str:
        """
        Calculate overall data quality grade based on success rate
        
        Args:
            success_rate: Success rate percentage
            
        Returns:
            Quality grade (A, B, C, D, F)
        """
        if success_rate >= 95:
            return "A"
        elif success_rate >= 85:
            return "B"
        elif success_rate >= 75:
            return "C"
        elif success_rate >= 65:
            return "D"
        else:
            return "F"
    
    def _generate_recommendations_actions(  # TODO: decide to use this method or remove it
        self, 
        complete_products: int, 
        success_rate: float
        ) -> List[str]:
        """
        Generate recommended next steps based on data quality
        
        Args:
            complete_products: Number of complete products
            success_rate: Success rate percentage
            
        Returns:
            List of recommended next steps
        """
        steps = []
        
        if complete_products >= 100 and success_rate >= 85:
            steps.append("✅ Ready for production deployment")
            steps.append("📊 Load validated products into Supabase database")
            steps.append("🤖 Begin bot development and testing")
            steps.append("🚀 Deploy to production environment")
        elif complete_products >= 50 and success_rate >= 70:
            steps.append("⚠️ Minimum viable dataset achieved")
            steps.append("🔧 Address rejection reasons to improve quality")
            steps.append("🧪 Test with limited product set before full deployment")
            steps.append("📈 Optimize extraction pipeline for better success rates")
        else:
            steps.append("❌ Dataset not ready for production")
            steps.append("🚨 Review and fix validation failures")
            steps.append("📈 Improve extraction pipeline before transformation")
            steps.append("🔍 Analyze rejection patterns to identify root causes")
        
        return steps
    
    def analyze_rejection_patterns(self, rejected_products: Dict[str, Any]) -> Dict[str, Any]:
        """
        Analyze patterns in rejected products to identify improvement opportunities
        
        Args:
            rejected_products: Rejected products to analyze
            
        Returns:
            Rejection pattern analysis
        """
        total_rejected = len(rejected_products)
        rejection_reasons_count = {}
        field_failure_patterns = {}
        
        for barcode, product_data in rejected_products.items():
            rejection_reasons = product_data.get("rejection_reasons", [])
            
            # Count rejection reasons
            for reason in rejection_reasons:
                rejection_reasons_count[reason] = rejection_reasons_count.get(reason, 0) + 1
            
            # Analyze field failures
            partial_data = product_data.get("partial_data", {})
            success_flags = partial_data.get("extraction_success", {})
            
            for field, success in success_flags.items():
                if not success:
                    field_failure_patterns[field] = field_failure_patterns.get(field, 0) + 1
        
        return {
            "total_rejected_products": total_rejected,
            "rejection_reasons_frequency": rejection_reasons_count,
            "field_failure_patterns": field_failure_patterns,
            "most_common_rejection": max(rejection_reasons_count.items(), key=lambda x: x[1]) if rejection_reasons_count else None,
            "critical_field_failures": {k: v for k, v in field_failure_patterns.items() if v > total_rejected * 0.1}
        }
    
    def get_analysis_stats(self) -> Dict[str, Any]:  # TODO: decide to use this method or remove it
        """Get current analysis statistics"""
        return self.analysis_stats.copy()
    
    def get_quality_issues(self) -> List[Dict[str, Any]]:
        """Get detected quality issues"""
        return self.quality_issues.copy()
    
    def get_anomalies(self) -> List[Dict[str, Any]]:
        """Get detected anomalies"""
        return self.anomalies.copy()
    
    def reset_stats(self):
        """Reset analysis statistics"""
        self.analysis_stats = {
            "quality_assessments": 0,
            "completeness_analyses": 0,
            "recommendations_generated": 0,
            "quality_validations": 0,
            "anomalies_detected": 0
        }
        self.quality_issues = []
        self.anomalies = []
