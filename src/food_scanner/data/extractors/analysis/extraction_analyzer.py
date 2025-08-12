"""
src/food_scanner/data/extractors/analysis/extraction_analyzer.py
PURE ANALYSIS: Orchestrates all field analyses and generates insights
RESPONSIBILITY: Analysis logic ONLY - no file generation or formatting
"""

import sys
from typing import Dict, Any, List, Optional
from collections import Counter
from datetime import datetime
from dataclasses import dataclass
from pathlib import Path

# Add src to path for standalone execution
sys.path.insert(0, str(Path(__file__).resolve().parents[5] / "src"))

from food_scanner.data.extractors.analysis.analyzers.comprehensive_analyzer import ComprehensiveExtractionAnalyzer
from food_scanner.data.extractors.analysis.analyzers.co2_analyzer import CO2Analyzer
from food_scanner.data.extractors.analysis.analyzers.barcode_analyzer import BarcodeAnalyzer
from food_scanner.data.extractors.analysis.analyzers.text_field_analyzer import TextFieldAnalyzer
from food_scanner.data.extractors.analysis.analyzers.nutriscore_analyzer import NutriscoreAnalyzer


@dataclass
class ExtractionAnalysisResults:
    """
    Container for all extraction analysis results
    PURE DATA - no formatting logic
    """
    analysis_timestamp: str
    total_products: int
    
    # Field-specific analysis results
    field_analyses: Dict[str, Any]
    
    # Overall metrics
    overall_metrics: Dict[str, Any]
    
    # Production readiness assessment
    production_readiness: Dict[str, Any]
    
    # Critical issues and recommendations
    critical_issues: List[str]
    improvement_priorities: List[str]
    
    # Data quality insights
    quality_insights: Dict[str, Any]
    
    # Comprehensive analysis (from existing analyzer)
    comprehensive_analysis: Optional[Any] = None


class ExtractionAnalyzer:
    """
    PURE ANALYSIS ORCHESTRATOR: Coordinates all extraction analyses
    
    RESPONSIBILITIES:
    1. Orchestrate specialized field analyzers
    2. Calculate overall extraction metrics
    3. Assess production readiness
    4. Generate insights and recommendations
    5. NO file generation or formatting
    
    USAGE:
        analyzer = ExtractionAnalyzer()
        results = analyzer.analyze_extraction_results(
            extracted_products, extraction_stats, pipeline_stats
        )
    """
    
    def __init__(self):
        # Initialize specialized analyzers
        self.co2_analyzer = CO2Analyzer()
        self.barcode_analyzer = BarcodeAnalyzer()
        self.text_analyzer = TextFieldAnalyzer()
        self.nutriscore_analyzer = NutriscoreAnalyzer()
        self.comprehensive_analyzer = ComprehensiveExtractionAnalyzer()
        
        # Analysis statistics
        self.analysis_stats = {
            "total_analyses": 0,
            "successful_analyses": 0,
            "failed_analyses": 0,
            "last_analysis_timestamp": None
        }
    
    def analyze_extraction_results(
        self,
        extracted_products: Dict[str, Any],
        extraction_stats: Dict[str, Any],
        pipeline_stats: Dict[str, Any]
        ) -> ExtractionAnalysisResults:
        """
        MAIN ANALYSIS METHOD: Analyze complete extraction results
        
        Args:
            extracted_products: Output from ProductExtractor
            extraction_stats: Field extraction statistics
            pipeline_stats: Overall pipeline statistics
            
        Returns:
            ExtractionAnalysisResults with all analysis insights
        """
        analysis_timestamp = datetime.now().isoformat()
        total_products = len(extracted_products)
        
        print(f"🔍 EXTRACTION ANALYSIS ENGINE")
        print(f"   → Analyzing {total_products} extracted products")
        print(f"   → Timestamp: {analysis_timestamp}")
        print("=" * 60)
        
        try:
            # Run field-specific analyses
            field_analyses = self._run_field_analyses(extracted_products)
            
            # Calculate overall metrics
            overall_metrics = self._calculate_overall_metrics(
                extracted_products, extraction_stats, pipeline_stats, field_analyses
            )
            
            # Assess production readiness
            production_readiness = self._assess_production_readiness(
                extracted_products, field_analyses, overall_metrics
            )
            
            # Identify critical issues
            critical_issues = self._identify_critical_issues(
                field_analyses, production_readiness, extraction_stats
            )
            
            # Generate improvement priorities
            improvement_priorities = self._generate_improvement_priorities(
                field_analyses, production_readiness, critical_issues
            )
            
            # Generate quality insights
            quality_insights = self._generate_quality_insights(
                field_analyses, overall_metrics, extraction_stats
            )
            
            # Run comprehensive analysis (using existing analyzer)
            comprehensive_analysis = self._run_comprehensive_analysis(
                extracted_products, extraction_stats, pipeline_stats
            )
            
            # Create results container
            results = ExtractionAnalysisResults(
                analysis_timestamp=analysis_timestamp,
                total_products=total_products,
                field_analyses=field_analyses,
                overall_metrics=overall_metrics,
                production_readiness=production_readiness,
                critical_issues=critical_issues,
                improvement_priorities=improvement_priorities,
                quality_insights=quality_insights,
                comprehensive_analysis=comprehensive_analysis
            )
            
            self.analysis_stats["successful_analyses"] += 1
            self.analysis_stats["last_analysis_timestamp"] = analysis_timestamp
            
            print(f"✅ Extraction analysis completed successfully")
            print(f"   → Overall quality: {overall_metrics.get('overall_quality_score', 0):.1f}%")
            print(f"   → Production ready: {production_readiness.get('ready_products', 0)} products")
            print(f"   → Critical issues: {len(critical_issues)}")
            
            return results
            
        except Exception as e:
            print(f"❌ Analysis failed: {e}")
            self.analysis_stats["failed_analyses"] += 1
            
            # Return minimal results on failure
            return ExtractionAnalysisResults(
                analysis_timestamp=analysis_timestamp,
                total_products=total_products,
                field_analyses={},
                overall_metrics={"error": str(e)},
                production_readiness={},
                critical_issues=[f"Analysis failed: {e}"],
                improvement_priorities=[],
                quality_insights={}
            )
    
    def _run_field_analyses(self, extracted_products: Dict[str, Any]) -> Dict[str, Any]:
        """Run all specialized field analyses"""
        
        print(f"   🔍 Running specialized field analyses...")
        field_analyses = {}
        
        try:
            # Barcode analysis (critical for primary key)
            print(f"      📋 Analyzing barcode extraction...")
            barcode_analysis = self.barcode_analyzer.analyze_barcode_from_extracted_products(
                extracted_products
            )
            field_analyses["barcode"] = barcode_analysis
            
            # Product name analysis (critical for display)
            print(f"      📝 Analyzing product name extraction...")
            product_name_analysis = self.text_analyzer.analyze_text_field_from_extracted_products(
                extracted_products, "product_name"
            )
            field_analyses["product_name"] = product_name_analysis
            
            # Brand name analysis (critical for display)
            print(f"      🏷️ Analyzing brand name extraction...")
            brand_name_analysis = self.text_analyzer.analyze_text_field_from_extracted_products(
                extracted_products, "brand_name"
            )
            field_analyses["brand_name"] = brand_name_analysis
            
            # CO2 analysis (critical for functionality)
            print(f"      🌍 Analyzing CO2 extraction...")
            co2_analysis = self.co2_analyzer.analyze_co2_from_extracted_products(
                extracted_products
            )
            field_analyses["co2_total"] = co2_analysis
            
            # Nutriscore analysis (important for display)
            print(f"      📊 Analyzing nutriscore extraction...")
            try:
                # Nutriscore grade
                nutriscore_grade_analyzer = NutriscoreAnalyzer()
                nutriscore_grade_analyzer.field_name = "nutriscore_grade"
                grade_analysis = nutriscore_grade_analyzer.analyze_nutriscore_from_extracted_products(
                    list(extracted_products.values())
                )
                field_analyses["nutriscore_grade"] = grade_analysis
                
                # Nutriscore score
                nutriscore_score_analyzer = NutriscoreAnalyzer()
                nutriscore_score_analyzer.field_name = "nutriscore_score"
                score_analysis = nutriscore_score_analyzer.analyze_nutriscore_from_extracted_products(
                    list(extracted_products.values())
                )
                field_analyses["nutriscore_score"] = score_analysis
                
            except Exception as e:
                print(f"      ⚠️ Nutriscore analysis failed: {e}")
                field_analyses["nutriscore_note"] = f"Analysis failed: {e}"
            
            print(f"      ✅ Field analyses completed")
            
        except Exception as e:
            print(f"      ❌ Field analyses error: {e}")
            field_analyses["error"] = str(e)
        
        return field_analyses
    
    def _calculate_overall_metrics(
        self,
        extracted_products: Dict[str, Any],
        extraction_stats: Dict[str, Any],
        pipeline_stats: Dict[str, Any],
        field_analyses: Dict[str, Any]
        ) -> Dict[str, Any]:
        """Calculate overall extraction metrics"""
        
        total_products = len(extracted_products)
        successful_extractions = extraction_stats.get("successful_extractions", 0)
        field_success_counts = extraction_stats.get("field_success_counts", {})
        
        # Calculate field-level metrics
        field_metrics = {}
        overall_quality_scores = []
        
        for field_name, analysis in field_analyses.items():
            if hasattr(analysis, 'quality_score'):
                field_metrics[field_name] = {
                    "success_rate": analysis.validity_rate,
                    "quality_score": analysis.quality_score,
                    "valid_count": analysis.valid_count,
                    "total_count": analysis.total_products
                }
                overall_quality_scores.append(analysis.quality_score)
        
        # Overall quality score
        overall_quality_score = (
            sum(overall_quality_scores) / len(overall_quality_scores)
            if overall_quality_scores else 0
        )
        
        # Pipeline efficiency metrics
        pipeline_efficiency = self._calculate_pipeline_efficiency(
            pipeline_stats, extraction_stats
        )
        
        # Critical field performance
        critical_fields = ["barcode", "product_name", "brand_name", "co2_total"]
        critical_performance = {}
        
        for field in critical_fields:
            if field in field_metrics:
                critical_performance[field] = field_metrics[field]["success_rate"]
        
        return {
            "total_products_analyzed": total_products,
            "successful_extractions": successful_extractions,
            "overall_quality_score": overall_quality_score,
            "field_metrics": field_metrics,
            "pipeline_efficiency": pipeline_efficiency,
            "critical_field_performance": critical_performance,
            "extraction_success_rate": (successful_extractions / total_products * 100) if total_products > 0 else 0
        }
    
    def _calculate_pipeline_efficiency(
        self,
        pipeline_stats: Dict[str, Any],
        extraction_stats: Dict[str, Any]
        ) -> Dict[str, Any]:
        """Calculate pipeline efficiency metrics"""
        
        discovered = pipeline_stats.get("products_discovered", 0)
        enriched = pipeline_stats.get("products_enriched", 0)
        extracted = extraction_stats.get("successful_extractions", 0)
        api_calls = pipeline_stats.get("api_calls", 0)
        
        return {
            "discovery_to_enrichment_rate": (enriched / discovered * 100) if discovered > 0 else 0,
            "enrichment_to_extraction_rate": (extracted / enriched * 100) if enriched > 0 else 0,
            "overall_pipeline_efficiency": (extracted / discovered * 100) if discovered > 0 else 0,
            "api_calls_per_extraction": api_calls / max(1, extracted)
        }
    
    def _assess_production_readiness(
        self,
        extracted_products: Dict[str, Any],
        field_analyses: Dict[str, Any],
        overall_metrics: Dict[str, Any]
        ) -> Dict[str, Any]:
        """Assess production readiness based on business rules"""
        
        total_products = len(extracted_products)
        ready_products = 0
        rejection_reasons = Counter()
        
        # Critical fields for bot functionality
        critical_fields = ["barcode", "product_name", "brand_name", "co2_total"]
        
        for barcode, product_data in extracted_products.items():
            extracted_fields = product_data.get("extracted_fields", {})
            success_flags = extracted_fields.get("extraction_success", {})
            
            product_rejection_reasons = []
            
            # Check critical field extraction
            for field in critical_fields:
                if not success_flags.get(field, False):
                    product_rejection_reasons.append(f"{field}_extraction_failed")
            
            # Check nutriscore (need at least one)
            has_nutriscore = (
                success_flags.get("nutriscore_grade", False) or 
                success_flags.get("nutriscore_score", False)
            )
            if not has_nutriscore:
                product_rejection_reasons.append("no_nutriscore_data")
            
            # Count rejections
            for reason in product_rejection_reasons:
                rejection_reasons[reason] += 1
            
            # If no rejections, product is ready
            if not product_rejection_reasons:
                ready_products += 1
        
        readiness_rate = (ready_products / total_products * 100) if total_products > 0 else 0
        
        return {
            "total_products": total_products,
            "ready_products": ready_products,
            "rejected_products": total_products - ready_products,
            "readiness_rate": readiness_rate,
            "rejection_reasons": dict(rejection_reasons.most_common()),
            "bot_ready": ready_products >= 100 and readiness_rate >= 70,
            "mvp_ready": ready_products >= 50 and readiness_rate >= 50,
            "needs_improvement": readiness_rate < 50,
            "production_thresholds": {
                "minimum_products": 100,
                "minimum_readiness_rate": 70,
                "current_status": "READY" if readiness_rate >= 70 and ready_products >= 100
                            else "MVP" if readiness_rate >= 50 and ready_products >= 50
                            else "NOT_READY"
            }
        }
    
    def _identify_critical_issues(
        self,
        field_analyses: Dict[str, Any],
        production_readiness: Dict[str, Any],
        extraction_stats: Dict[str, Any]
        ) -> List[str]:
        """Identify critical issues blocking production"""
        
        issues = []
        
        # Check critical field extraction rates
        critical_fields = ["barcode", "product_name", "brand_name", "co2_total"]
        
        for field in critical_fields:
            if field in field_analyses:
                analysis = field_analyses[field]
                if hasattr(analysis, 'validity_rate') and analysis.validity_rate < 70:
                    issues.append(
                        f"CRITICAL: {field} extraction only {analysis.validity_rate:.1f}% successful - blocks bot functionality"
                    )
        
        # Check overall production readiness
        readiness_rate = production_readiness.get("readiness_rate", 0)
        if readiness_rate < 50:
            issues.append(
                f"CRITICAL: Only {readiness_rate:.1f}% products production-ready - major improvements needed"
            )
        
        # Check specific rejection reasons
        rejection_reasons = production_readiness.get("rejection_reasons", {})
        for reason, count in rejection_reasons.items():
            if count > production_readiness.get("total_products", 0) * 0.3:  # >30% affected
                issues.append(
                    f"HIGH IMPACT: {reason} affects {count} products ({count/production_readiness.get('total_products', 1)*100:.1f}%)"
                )
        
        return issues
    
    def _generate_improvement_priorities(
        self,
        field_analyses: Dict[str, Any],
        production_readiness: Dict[str, Any],
        critical_issues: List[str]
        ) -> List[str]:
        """Generate prioritized improvement recommendations"""
        
        priorities = []
        
        # Calculate improvement impact scores
        improvement_scores = []
        
        for field_name, analysis in field_analyses.items():
            if not hasattr(analysis, 'validity_rate'):
                continue
                
            # Impact factors
            is_critical = field_name in ["barcode", "product_name", "brand_name", "co2_total"]
            current_rate = analysis.validity_rate
            improvement_potential = 100 - current_rate
            
            # Calculate impact score
            criticality_weight = 5 if is_critical else 2
            impact_score = criticality_weight * improvement_potential
            
            if improvement_potential > 0:
                improvement_scores.append((impact_score, field_name, current_rate, improvement_potential))
        
        # Sort by impact score
        improvement_scores.sort(reverse=True)
        
        # Generate prioritized recommendations
        for impact_score, field_name, current_rate, potential in improvement_scores[:5]:
            if impact_score > 100:
                priority = "🔥 URGENT"
            elif impact_score > 50:
                priority = "⚡ HIGH"
            else:
                priority = "📈 MEDIUM"
            
            priorities.append(
                f"{priority}: Improve {field_name} extraction - "
                f"current {current_rate:.1f}%, potential +{potential:.1f}% "
                f"(impact: {impact_score:.0f})"
            )
        
        return priorities
    
    def _generate_quality_insights(
        self,
        field_analyses: Dict[str, Any],
        overall_metrics: Dict[str, Any],
        extraction_stats: Dict[str, Any]
        ) -> Dict[str, Any]:
        """Generate data quality insights"""
        
        insights = {
            "top_performing_fields": [],
            "underperforming_fields": [],
            "extraction_patterns": {},
            "data_completeness": {},
            "quality_trends": {}
        }
        
        # Identify top and underperforming fields
        field_scores = []
        
        for field_name, analysis in field_analyses.items():
            if hasattr(analysis, 'quality_score'):
                field_scores.append((analysis.quality_score, field_name))
        
        field_scores.sort(reverse=True)
        
        # Top performing (>80% quality)
        insights["top_performing_fields"] = [
            {"field": field, "score": score}
            for score, field in field_scores if score >= 80
        ]
        
        # Underperforming (<60% quality)
        insights["underperforming_fields"] = [
            {"field": field, "score": score}
            for score, field in field_scores if score < 60
        ]
        
        # Data completeness analysis
        field_success_counts = extraction_stats.get("field_success_counts", {})
        total_products = overall_metrics.get("total_products_analyzed", 0)
        
        for field, count in field_success_counts.items():
            completeness = (count / total_products * 100) if total_products > 0 else 0
            insights["data_completeness"][field] = {
                "count": count,
                "completeness_rate": completeness,
                "missing_count": total_products - count
            }
        
        return insights
    
    def _run_comprehensive_analysis(
        self,
        extracted_products: Dict[str, Any],
        extraction_stats: Dict[str, Any],
        pipeline_stats: Dict[str, Any]
        ) -> Optional[Any]:
        """Run comprehensive analysis using existing analyzer"""
        
        try:
            print(f"   📊 Running comprehensive analysis...")
            comprehensive_result = self.comprehensive_analyzer.analyze_extraction_results(
                extracted_products, extraction_stats, pipeline_stats
            )
            print(f"      ✅ Comprehensive analysis completed")
            return comprehensive_result
        except Exception as e:
            print(f"      ⚠️ Comprehensive analysis failed: {e}")
            return None
    
    def get_analysis_statistics(self) -> Dict[str, Any]:
        """Get analyzer statistics"""
        return self.analysis_stats.copy()
    
    def display_analysis_summary(self, results: ExtractionAnalysisResults):
        """Display analysis summary (console output only)"""
        
        print(f"\n🎯 EXTRACTION ANALYSIS SUMMARY")
        print("=" * 60)
        
        # Overall metrics
        overall = results.overall_metrics
        print(f"📊 OVERALL PERFORMANCE:")
        print(f"   → Products analyzed: {results.total_products}")
        print(f"   → Overall quality: {overall.get('overall_quality_score', 0):.1f}%")
        print(f"   → Extraction success: {overall.get('extraction_success_rate', 0):.1f}%")
        
        # Production readiness
        prod = results.production_readiness
        print(f"\n🚀 PRODUCTION READINESS:")
        print(f"   → Ready products: {prod.get('ready_products', 0)}")
        print(f"   → Readiness rate: {prod.get('readiness_rate', 0):.1f}%")
        print(f"   → Status: {prod.get('production_thresholds', {}).get('current_status', 'UNKNOWN')}")
        
        # Critical issues
        if results.critical_issues:
            print(f"\n🚨 CRITICAL ISSUES ({len(results.critical_issues)}):")
            for i, issue in enumerate(results.critical_issues[:3], 1):
                print(f"   {i}. {issue}")
        
        # Top priorities
        if results.improvement_priorities:
            print(f"\n📈 TOP PRIORITIES ({len(results.improvement_priorities)}):")
            for i, priority in enumerate(results.improvement_priorities[:3], 1):
                print(f"   {i}. {priority}")
        
        print(f"\n💡 Analysis complete - ready for reporting")
        print("=" * 60)


# Convenience function for quick analysis
def analyze_extraction_data(
    extracted_products: Dict[str, Any],
    extraction_stats: Dict[str, Any],
    pipeline_stats: Dict[str, Any]
    ) -> ExtractionAnalysisResults:
    """
    Convenience function for extraction analysis
    
    Usage:
        results = analyze_extraction_data(
            extracted_products, extraction_stats, pipeline_stats
        )
    """
    analyzer = ExtractionAnalyzer()
    return analyzer.analyze_extraction_results(
        extracted_products, extraction_stats, pipeline_stats
    )

# Test function for standalone execution
def test_extraction_analyzer():
    """Test ExtractionAnalyzer functionality"""
    print("🧪 TESTING EXTRACTION ANALYZER")
    print("=" * 50)
    
    try:
        # Create analyzer instance
        analyzer = ExtractionAnalyzer()
        print("✅ ExtractionAnalyzer created successfully")
        
        # Test with mock data
        mock_products = {
            "123456789": {
                "extracted_fields": {
                    "barcode": "123456789",
                    "product_name": "Test Product",
                    "brand_name": "Test Brand",
                    "weight": "100g",
                    "nutriscore_grade": "A",
                    "nutriscore_score": 15,
                    "co2_total": 250.0,
                    "extraction_success": {
                        "barcode": True,
                        "product_name": True,
                        "brand_name": True,
                        "weight": True,
                        "nutriscore_grade": True,
                        "nutriscore_score": True,
                        "co2_total": True
                    }
                }
            },
            "987654321": {
                "extracted_fields": {
                    "barcode": "987654321",
                    "product_name": "Another Product",
                    "brand_name": "Another Brand",
                    "weight": "200g",
                    "nutriscore_grade": "B",
                    "nutriscore_score": 25,
                    "co2_total": 300.0,
                    "extraction_success": {
                        "barcode": True,
                        "product_name": True,
                        "brand_name": True,
                        "weight": True,
                        "nutriscore_grade": True,
                        "nutriscore_score": True,
                        "co2_total": True
                    }
                }
            }
        }
        
        mock_extraction_stats = {
            "total_products": 2,
            "successful_extractions": 2,
            "field_success_counts": {
                "barcode": 2,
                "product_name": 2,
                "brand_name": 2,
                "weight": 2,
                "nutriscore_grade": 2,
                "nutriscore_score": 2,
                "co2_total": 2
            }
        }
        
        mock_pipeline_stats = {
            "discovery_products": 2,
            "enrichment_success": 2,
            "extraction_success": 2
        }
        
        print("✅ Mock data created successfully")
        
        # Run analysis
        results = analyzer.analyze_extraction_results(
            mock_products, mock_extraction_stats, mock_pipeline_stats
        )
        
        print("✅ Analysis completed successfully")
        print(f"   → Total products: {results.total_products}")
        print(f"   → Analysis timestamp: {results.analysis_timestamp}")
        print(f"   → Field analyses: {len(results.field_analyses)}")
        
        # Display summary
        analyzer.display_analysis_summary(results)
        
        print("\n🎉 ALL TESTS PASSED!")
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    test_extraction_analyzer()