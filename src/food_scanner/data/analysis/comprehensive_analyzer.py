"""
src/food_scanner/data/analysis/comprehensive_analyzer.py
UPDATED: Comprehensive analyzer adapted for ProductExtractor integration
Analyzes extraction results and generates transformation rules
FIXED: Use updated analyzers without validation_rule dependency
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any
from collections import Counter

# Import the integration functions directly (no class instantiation needed)
from food_scanner.data.analysis.co2_analyzer import analyze_co2_from_extraction_results
from food_scanner.core.models.data_quality import (
    FieldValidationRule, FieldAnalysisResult, ComprehensiveAnalysisReport,
    FieldType, ValidationRule
)


class ComprehensiveExtractionAnalyzer:
    """
    UPDATED Comprehensive analyzer for ProductExtractor results
    
    CHANGES FROM ORIGINAL:
    - Analyzes extracted_products instead of raw products
    - Uses extraction_success flags for validation analysis  
    - Integrates with ExtractionReporter for complete pipeline
    - Focuses on extraction performance rather than raw field presence
    - Generates production-readiness recommendations
    FIXED: Simplified to work with updated analyzers via integration functions
    """
    
    def __init__(self):
        self.field_validation_rules = self._initialize_validation_rules()
    
    def _initialize_validation_rules(self) -> Dict[str, FieldValidationRule]:
        """Initialize validation rules for critical extraction fields"""
        return {
            # Critical extraction fields (must succeed for bot functionality)
            "barcode": FieldValidationRule(
                field_name="barcode",
                field_type=FieldType.IDENTIFIER,
                validation_type=ValidationRule.REJECT_IF_MISSING,
                required=True,
                business_description="Primary key - extraction must succeed"
            ),
            
            "product_name": FieldValidationRule(
                field_name="product_name", 
                field_type=FieldType.TEXT,
                validation_type=ValidationRule.FALLBACK_CHAIN,
                required=True,
                fallback_sources=["product_name_fr", "product_name"],
                business_description="Bot display - extraction must succeed"
            ),
            
            "brand_name": FieldValidationRule(
                field_name="brand_name",
                field_type=FieldType.TEXT, 
                validation_type=ValidationRule.FALLBACK_CHAIN,
                required=True,
                fallback_sources=["brands", "brands_tags[0]", "brands_imported"],
                business_description="Bot display - extraction must succeed"
            ),
            
            "weight": FieldValidationRule(
                field_name="weight",
                field_type=FieldType.NUMERIC,
                validation_type=ValidationRule.OPTIONAL,
                required=False,
                business_description="Useful for calculations - improve extraction"
            ),
            
            "co2_total": FieldValidationRule(
                field_name="co2_total",
                field_type=FieldType.NUMERIC,
                validation_type=ValidationRule.RANGE_VALIDATION,
                required=True,
                business_description="Main bot functionality - extraction must succeed"
            ),
            
            "nutriscore_grade": FieldValidationRule(
                field_name="nutriscore_grade",
                field_type=FieldType.CATEGORICAL,
                validation_type=ValidationRule.ENUMERATION,
                required=False,  # Either grade OR score required
                business_description="Bot display - need at least one nutriscore field"
            ),
            
            "nutriscore_score": FieldValidationRule(
                field_name="nutriscore_score",
                field_type=FieldType.NUMERIC,
                validation_type=ValidationRule.RANGE_VALIDATION,
                required=False,  # Either grade OR score required
                business_description="Bot display - need at least one nutriscore field"
            )
        }
    
    def analyze_extraction_results(
        self,
        extracted_products: Dict[str, Any],
        extraction_stats: Dict[str, Any],
        pipeline_stats: Dict[str, Any],
        source_info: Dict[str, Any] = None
    ) -> ComprehensiveAnalysisReport:
        """
        Analyze complete extraction results from ProductExtractor
        
        Args:
            extracted_products: Output from ProductExtractor.run_complete_extraction()
            extraction_stats: Field extraction statistics
            pipeline_stats: Overall pipeline statistics  
            source_info: Optional source metadata
        """
        timestamp = datetime.now()
        
        report = ComprehensiveAnalysisReport(
            analysis_timestamp=timestamp.isoformat(),
            dataset_info={
                "total_products": len(extracted_products),
                "successful_extractions": extraction_stats.get("successful_extractions", 0),
                "failed_extractions": extraction_stats.get("failed_extractions", 0),
                "extraction_timestamp": source_info.get("timestamp") if source_info else "unknown",
                "analysis_version": "2.0_extraction_focused",
                "environment": source_info.get("environment", "unknown") if source_info else "unknown",
                "pipeline_performance": pipeline_stats
            },
            total_products_analyzed=len(extracted_products)
        )
        
        print(f"🔍 COMPREHENSIVE EXTRACTION ANALYSIS")
        print(f"   → Analyzing {len(extracted_products)} extracted products")
        print(f"   → Timestamp: {timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60)
        
        # Analyze extraction success for each field
        field_extraction_analysis = self._analyze_field_extraction_performance(
            extracted_products, extraction_stats
        )
        
        # Run specialized analysis using integration functions
        specialized_results = self._run_specialized_analysis(extracted_products)
        
        # Combine field analysis results
        report.field_results = {**field_extraction_analysis, **specialized_results}
        
        # Calculate overall quality score
        total_quality_score = sum(result.quality_score for result in report.field_results.values())
        analyzed_fields = len(report.field_results)
        if analyzed_fields > 0:
            report.overall_quality_score = total_quality_score / analyzed_fields
        
        # Production readiness analysis
        print(f"   🎯 Analyzing production readiness...")
        report.rejection_analysis = self._analyze_production_readiness(
            extracted_products, report.field_results
        )
        
        # Generate transformation rules for production
        print(f"   ⚙️ Generating extraction rules...")
        report.generated_transformation_rules = self._generate_extraction_rules(
            report.field_results, extraction_stats, pipeline_stats
        )
        
        # Identify critical issues and priorities
        report.critical_issues = self._identify_extraction_issues(report.field_results, extraction_stats)
        report.improvement_priorities = self._prioritize_extraction_improvements(
            report.field_results, extraction_stats
        )
        
        print(f"✅ Comprehensive analysis completed")
        print(f"   → Overall extraction quality: {report.overall_quality_score:.1f}%")
        print(f"   → Critical issues found: {len(report.critical_issues)}")
        
        return report
    
    def _analyze_field_extraction_performance(
        self,
        extracted_products: Dict[str, Any],
        extraction_stats: Dict[str, Any]
    ) -> Dict[str, FieldAnalysisResult]:
        """Analyze extraction performance for each field"""
        
        field_results = {}
        field_success_counts = extraction_stats.get("field_success_counts", {})
        total_products = len(extracted_products)
        
        for field_name, rule in self.field_validation_rules.items():
            if field_name in ["co2_total"]:  # Skip fields handled by specialized analyzers
                continue
            
            print(f"   📊 Analyzing extraction: {field_name}")
            
            # Create analysis result based on extraction success
            result = FieldAnalysisResult(
                field_name=field_name,
                field_type=rule.field_type,
                total_products=total_products
            )
            
            # Use extraction success counts
            success_count = field_success_counts.get(field_name, 0)
            result.valid_count = success_count
            result.present_count = success_count  # For extracted fields, present = valid
            result.missing_count = total_products - success_count
            
            # Analyze extraction patterns for this field
            extraction_patterns = self._analyze_field_extraction_patterns(
                extracted_products, field_name
            )
            result.pattern_analysis = extraction_patterns
            
            # Generate examples
            result.examples = self._generate_field_examples(
                extracted_products, field_name, success_count > 0
            )
            
            # Generate field-specific recommendations
            result.transformation_recommendations = self._generate_field_recommendations(
                field_name, rule, success_count, total_products
            )
            
            # Calculate metrics
            self._calculate_base_metrics(result)
            
            field_results[field_name] = result
            
            # Status display
            status = "✅" if result.quality_score >= 80 else "⚠️" if result.quality_score >= 60 else "❌"
            print(f"      {status} {success_count}/{total_products} successful ({result.quality_score:.1f}%)")
        
        return field_results
    
    def _analyze_field_extraction_patterns(
        self,
        extracted_products: Dict[str, Any],
        field_name: str
    ) -> Dict[str, Any]:
        """Analyze patterns in field extraction success/failure"""
        
        patterns = {
            "successful_extractions": 0,
            "failed_extractions": 0,
            "extraction_sources": Counter(),
            "failure_reasons": Counter()
        }
        
        for barcode, product_data in extracted_products.items():
            extracted_fields = product_data.get("extracted_fields", {})
            success_flags = extracted_fields.get("extraction_success", {})
            
            if success_flags.get(field_name, False):
                patterns["successful_extractions"] += 1
                
                # Track extraction source if available
                field_value = extracted_fields.get(field_name)
                if field_value is not None:
                    patterns["extraction_sources"]["direct_extraction"] += 1
            else:
                patterns["failed_extractions"] += 1
                patterns["failure_reasons"]["extraction_failed"] += 1
        
        return patterns
    
    def _run_specialized_analysis(self, extracted_products: Dict[str, Any]) -> Dict[str, FieldAnalysisResult]:
        """Run specialized analyzers using integration functions"""
        
        specialized_results = {}
        
        # CO2 specialized analysis using integration function
        print(f"   🌍 Running specialized CO2 analysis...")
        try:
            co2_result = analyze_co2_from_extraction_results(extracted_products)
            specialized_results["co2_total"] = co2_result
            print(f"      ✅ CO2 analysis completed: {co2_result.valid_count} products with CO2 data")
        except Exception as e:
            print(f"      ❌ CO2 analysis failed: {e}")
        
        # Add other specialized analyzers here as needed
        # Example:
        # barcode_result = analyze_barcode_from_extraction_results(extracted_products)
        # specialized_results["barcode"] = barcode_result
        
        return specialized_results
    
    def _analyze_production_readiness(
        self,
        extracted_products: Dict[str, Any],
        field_results: Dict[str, FieldAnalysisResult]
    ) -> Dict[str, Any]:
        """Analyze production readiness based on extraction results"""
        
        total_products = len(extracted_products)
        production_ready_count = 0
        rejection_reasons = Counter()
        
        for barcode, product_data in extracted_products.items():
            extracted_fields = product_data.get("extracted_fields", {})
            success_flags = extracted_fields.get("extraction_success", {})
            
            rejection_reasons_for_product = []
            
            # Check critical extraction requirements
            if not success_flags.get("barcode", False):
                rejection_reasons_for_product.append("barcode_extraction_failed")
            
            if not success_flags.get("product_name", False):
                rejection_reasons_for_product.append("product_name_extraction_failed")
            
            if not success_flags.get("brand_name", False):
                rejection_reasons_for_product.append("brand_name_extraction_failed")
            
            if not success_flags.get("co2_total", False):
                rejection_reasons_for_product.append("co2_extraction_failed")
            
            # Check nutriscore (need at least one)
            has_nutriscore = (success_flags.get("nutriscore_grade", False) or 
                            success_flags.get("nutriscore_score", False))
            if not has_nutriscore:
                rejection_reasons_for_product.append("no_nutriscore_data")
            
            # Count rejection reasons
            for reason in rejection_reasons_for_product:
                rejection_reasons[reason] += 1
            
            # If no rejections, product is production ready
            if not rejection_reasons_for_product:
                production_ready_count += 1
        
        rejection_rate = ((total_products - production_ready_count) / total_products * 100) if total_products > 0 else 0
        
        return {
            "total_products_analyzed": total_products,
            "production_ready_products": production_ready_count,
            "rejected_products": total_products - production_ready_count,
            "production_ready_rate": (production_ready_count / total_products * 100) if total_products > 0 else 0,
            "rejection_rate": rejection_rate,
            "rejection_reasons": dict(rejection_reasons.most_common()),
            "bot_launch_readiness": {
                "ready_for_launch": production_ready_count >= 100 and rejection_rate <= 30,
                "minimum_viable": production_ready_count >= 50 and rejection_rate <= 50,
                "needs_improvement": rejection_rate > 50
            },
            "extraction_performance_summary": {
                "critical_extractions_success": production_ready_count,
                "extraction_blocking_issues": dict(rejection_reasons.most_common(5))
            }
        }
    
    def _generate_extraction_rules(
        self,
        field_results: Dict[str, FieldAnalysisResult], 
        extraction_stats: Dict[str, Any],
        pipeline_stats: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Generate extraction rules and recommendations for production"""
        
        timestamp = datetime.now()
        
        rules = {
            "metadata": {
                "generation_timestamp": timestamp.isoformat(),
                "based_on_extraction_analysis": True,
                "target": "production_ready_extraction_pipeline",
                "extraction_performance_focus": True
            },
            "extraction_quality_rules": {},
            "production_readiness_thresholds": {},
            "field_extraction_rules": {},
            "pipeline_optimization_rules": {}
        }
        
        # Extraction quality rules
        for field_name, result in field_results.items():
            rules["extraction_quality_rules"][field_name] = {
                "current_success_rate": result.validity_rate,
                "production_threshold": 80 if field_name in ["barcode", "product_name", "brand_name", "co2_total"] else 60,
                "is_production_ready": result.validity_rate >= (80 if field_name in ["barcode", "product_name", "brand_name", "co2_total"] else 60),
                "improvement_needed": result.validity_rate < 90,
                "extraction_recommendations": result.transformation_recommendations
            }
        
        # Production readiness thresholds
        rules["production_readiness_thresholds"] = {
            "minimum_products_for_launch": 100,
            "maximum_acceptable_rejection_rate": 30,
            "critical_field_minimum_success_rate": 80,
            "overall_quality_minimum": 75
        }
        
        # Pipeline optimization
        total_discovered = pipeline_stats.get("products_discovered", 0)
        total_enriched = pipeline_stats.get("products_enriched", 0)
        total_extracted = extraction_stats.get("successful_extractions", 0)
        
        rules["pipeline_optimization_rules"] = {
            "discovery_to_enrichment_rate": (total_enriched / total_discovered * 100) if total_discovered > 0 else 0,
            "enrichment_to_extraction_rate": (total_extracted / total_enriched * 100) if total_enriched > 0 else 0,
            "overall_pipeline_efficiency": (total_extracted / total_discovered * 100) if total_discovered > 0 else 0,
            "api_calls_per_successful_extraction": pipeline_stats.get("api_calls", 0) / max(1, total_extracted),
            "optimization_recommendations": self._generate_pipeline_optimization_recommendations(
                pipeline_stats, extraction_stats
            )
        }
        
        return rules
    
    def _generate_pipeline_optimization_recommendations(
        self,
        pipeline_stats: Dict[str, Any],
        extraction_stats: Dict[str, Any]
    ) -> List[str]:
        """Generate pipeline optimization recommendations"""
        
        recommendations = []
        
        api_calls = pipeline_stats.get("api_calls", 0)
        discovered = pipeline_stats.get("products_discovered", 0)
        enriched = pipeline_stats.get("products_enriched", 0)
        extracted = extraction_stats.get("successful_extractions", 0)
        
        # API efficiency
        if api_calls > 0 and extracted > 0:
            calls_per_extraction = api_calls / extracted
            if calls_per_extraction > 3:
                recommendations.append(
                    f"High API usage: {calls_per_extraction:.1f} calls per successful extraction"
                )
        
        # Discovery efficiency  
        if discovered > 0 and enriched > 0:
            discovery_success = (enriched / discovered) * 100
            if discovery_success < 70:
                recommendations.append(
                    f"Low discovery efficiency: {discovery_success:.1f}% products enriched"
                )
        
        # Extraction efficiency
        if enriched > 0 and extracted > 0:
            extraction_success = (extracted / enriched) * 100
            if extraction_success < 80:
                recommendations.append(
                    f"Improve field extraction: {extraction_success:.1f}% success rate"
                )
        
        return recommendations
    
    def _identify_extraction_issues(
        self,
        field_results: Dict[str, FieldAnalysisResult],
        extraction_stats: Dict[str, Any]
    ) -> List[str]:
        """Identify critical extraction issues"""
        
        issues = []
        
        # Check critical field extraction rates
        critical_fields = ["barcode", "product_name", "brand_name", "co2_total"]
        
        for field in critical_fields:
            if field in field_results:
                result = field_results[field]
                if result.validity_rate < 70:
                    issues.append(
                        f"CRITICAL: {field} extraction only {result.validity_rate:.1f}% successful - blocks bot functionality"
                    )
        
        # Check overall extraction performance
        total_extracted = extraction_stats.get("successful_extractions", 0)
        total_failed = extraction_stats.get("failed_extractions", 0)
        total_attempted = total_extracted + total_failed
        
        if total_attempted > 0:
            overall_success = (total_extracted / total_attempted) * 100
            if overall_success < 70:
                issues.append(
                    f"LOW EXTRACTION SUCCESS: Only {overall_success:.1f}% of products successfully extracted"
                )
        
        # Check field-specific issues
        field_counts = extraction_stats.get("field_success_counts", {})
        for field, count in field_counts.items():
            if field in critical_fields and total_extracted > 0:
                field_rate = (count / total_extracted) * 100
                if field_rate < 50:
                    issues.append(
                        f"FIELD EXTRACTION FAILURE: {field} only {field_rate:.1f}% successful"
                    )
        
        return issues
    
    def _prioritize_extraction_improvements(
        self,
        field_results: Dict[str, FieldAnalysisResult],
        extraction_stats: Dict[str, Any]
    ) -> List[str]:
        """Prioritize extraction improvements by impact"""
        
        priorities = []
        
        # Calculate improvement potential for each field
        improvement_scores = []
        
        for field_name, result in field_results.items():
            rule = self.field_validation_rules.get(field_name)
            if not rule:
                continue
            
            # Score based on criticality × improvement potential × bot impact
            criticality_weight = 5 if rule.required else 2
            improvement_potential = 100 - result.validity_rate
            bot_impact_weight = 3 if field_name in ["barcode", "product_name", "co2_total"] else 1
            
            impact_score = criticality_weight * improvement_potential * bot_impact_weight
            improvement_scores.append((impact_score, field_name, result, rule))
        
        # Sort by impact score
        improvement_scores.sort(reverse=True)
        
        # Generate prioritized recommendations
        for impact_score, field_name, result, rule in improvement_scores[:6]:
            improvement_potential = 100 - result.validity_rate
            if improvement_potential > 0:
                if impact_score > 200:
                    priority_level = "🔥 URGENT"
                elif impact_score > 100:
                    priority_level = "⚡ HIGH"
                else:
                    priority_level = "📈 MEDIUM"
                
                priorities.append(
                    f"{priority_level}: Improve {field_name} extraction - "
                    f"potential +{improvement_potential:.1f}% success rate "
                    f"(impact score: {impact_score:.0f})"
                )
        
        return priorities
    
    def _generate_field_examples(
        self,
        extracted_products: Dict[str, Any],
        field_name: str,
        has_successes: bool
    ) -> Dict[str, List]:
        """Generate examples for field extraction analysis"""
        
        examples = {"successful": [], "failed": []}
        success_count = 0
        failure_count = 0
        
        for barcode, product_data in extracted_products.items():
            extracted_fields = product_data.get("extracted_fields", {})
            success_flags = extracted_fields.get("extraction_success", {})
            
            if success_flags.get(field_name, False) and success_count < 3:
                examples["successful"].append({
                    "barcode": barcode,
                    "extracted_value": extracted_fields.get(field_name),
                    "product_name": extracted_fields.get("product_name", "Unknown")
                })
                success_count += 1
            
            elif not success_flags.get(field_name, False) and failure_count < 3:
                examples["failed"].append({
                    "barcode": barcode,
                    "product_name": extracted_fields.get("product_name", "Unknown"),
                    "extraction_attempted": True
                })
                failure_count += 1
        
        return examples
    
    def _generate_field_recommendations(
        self,
        field_name: str,
        rule: FieldValidationRule,
        success_count: int,
        total_products: int
    ) -> List[str]:
        """Generate field-specific extraction recommendations"""
        
        recommendations = []
        success_rate = (success_count / total_products * 100) if total_products > 0 else 0
        
        if rule.required and success_rate < 80:
            recommendations.append(
                f"CRITICAL: {field_name} extraction must improve - only {success_rate:.1f}% success"
            )
        
        if success_rate < 60:
            recommendations.append(
                f"Major extraction issues for {field_name} - review extraction logic"
            )
        elif success_rate < 80:
            recommendations.append(
                f"Optimize {field_name} extraction - current {success_rate:.1f}% success rate"
            )
        
        # Field-specific recommendations
        if field_name == "weight" and success_rate < 80:
            recommendations.append(
                "Consider improving WeightParser for better weight extraction"
            )
        elif field_name == "co2_total" and success_rate < 70:
            recommendations.append(
                "Add additional CO2 data sources or improve fallback logic"
            )
        
        return recommendations
    
    def _calculate_base_metrics(self, result: FieldAnalysisResult) -> None:
        """Calculate basic metrics for field analysis"""
        total = result.total_products
        if total > 0:
            result.presence_rate = (result.present_count / total) * 100
            result.validity_rate = (result.valid_count / total) * 100
            
            # Quality score based on extraction success
            result.quality_score = result.validity_rate  # For extracted fields, quality = validity
    
    def save_extraction_analysis_report(self, report: ComprehensiveAnalysisReport, output_dir: Path) -> Path:
        """Save comprehensive extraction analysis report"""
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_filename = f"extraction_analysis_comprehensive_{timestamp}.json"
        report_file = output_dir / report_filename
        
        # Convert report to dict for JSON serialization
        report_dict = self._convert_report_to_dict(report)
        
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report_dict, f, indent=2, ensure_ascii=False, default=str)
        
        print(f"📊 Comprehensive extraction analysis saved: {report_file}")
        return report_file
    
    def _convert_report_to_dict(self, report: ComprehensiveAnalysisReport) -> Dict[str, Any]:
        """Convert report to dictionary for JSON serialization"""
        return {
            "analysis_timestamp": report.analysis_timestamp,
            "dataset_info": report.dataset_info,
            "total_products_analyzed": report.total_products_analyzed,
            "overall_quality_score": report.overall_quality_score,
            "field_results": {
                field_name: {
                    "field_name": result.field_name,
                    "field_type": result.field_type.value,
                    "total_products": result.total_products,
                    "present_count": result.present_count,
                    "missing_count": result.missing_count,
                    "valid_count": result.valid_count,
                    "validity_rate": result.validity_rate,
                    "quality_score": result.quality_score,
                    "pattern_analysis": result.pattern_analysis,
                    "examples": result.examples,
                    "transformation_recommendations": result.transformation_recommendations
                }
                for field_name, result in report.field_results.items()
            },
            "rejection_analysis": report.rejection_analysis,
            "generated_transformation_rules": report.generated_transformation_rules,
            "critical_issues": report.critical_issues,
            "improvement_priorities": report.improvement_priorities
        }


# Integration function for ProductExtractor
def analyze_extraction_comprehensive(
    extracted_products: Dict[str, Any],
    extraction_stats: Dict[str, Any], 
    pipeline_stats: Dict[str, Any]
) -> ComprehensiveAnalysisReport:
    """
    Convenience function for comprehensive extraction analysis
    
    Usage in ProductExtractor:
        analysis_report = analyze_extraction_comprehensive(
            extracted_products, extraction_stats, pipeline_stats
        )
    """
    analyzer = ComprehensiveExtractionAnalyzer()
    return analyzer.analyze_extraction_results(
        extracted_products, extraction_stats, pipeline_stats
    )