"""
src/food_scanner/data/analysis/co2_analyzer.py
UPDATED: CO2 analyzer adapted for new ProductExtractor structure
Analyzes CO2 extraction from extracted_products instead of raw products
"""

from typing import Dict, List, Any, Optional
from collections import Counter, defaultdict

from food_scanner.data.analysis.base_analyzer import BaseFieldAnalyzer
from food_scanner.core.models.data_quality import FieldAnalysisResult, FieldType


class CO2Analyzer(BaseFieldAnalyzer):
    """
    UPDATED CO2 Analyzer for extracted products structure
    
    CHANGES FROM ORIGINAL:
    - Works with extracted_products from ProductExtractor 
    - Uses co2_sources structured data instead of raw navigation
    - Analyzes extraction success flags instead of raw field presence
    - Compatible with ExtractionReporter integration
    """
    

    
    def analyze_extracted_products(self, extracted_products: Dict[str, Any]) -> FieldAnalysisResult:
        """
        Main entry point for analyzing CO2 data from extracted products
        
        Args:
            extracted_products: Output from ProductExtractor.run_complete_extraction()
                Format: {barcode: {"extracted_fields": {...}, "raw_api_data": {...}}}
        """
        result = FieldAnalysisResult(
            field_name="co2_total",
            field_type=FieldType.NUMERIC,
            total_products=len(extracted_products)
        )
        
        co2_values = []
        extraction_sources = Counter()
        co2_ranges = {"0-100": 0, "100-500": 0, "500-1000": 0, "1000-2000": 0, "2000+": 0}
        examples = defaultdict(list)
        data_quality_issues = Counter()
        
        for barcode, product_data in extracted_products.items():
            extracted_fields = product_data.get("extracted_fields", {})
            raw_api_data = product_data.get("raw_api_data", {})
            
            # Use structured co2_sources from extraction
            co2_sources = extracted_fields.get("co2_sources", {})
            success_flags = extracted_fields.get("extraction_success", {})
            
            # Get product identification for examples
            product_name = extracted_fields.get("product_name", "Unknown")
            brand_name = extracted_fields.get("brand_name", "Unknown")
            
            # Analyze CO2 extraction results
            self._analyze_co2_extraction(
                barcode, co2_sources, success_flags, product_name, brand_name,
                result, co2_values, extraction_sources, co2_ranges, 
                examples, data_quality_issues, raw_api_data
            )
        
        # Calculate distributions and patterns
        result.value_distribution = self._create_co2_distribution_bins(co2_values)
        
        result.pattern_analysis = {
            "extraction_sources": dict(extraction_sources),
            "co2_ranges": co2_ranges,
            "data_quality_issues": dict(data_quality_issues),
            "co2_statistics": self._calculate_co2_statistics(co2_values),
            "source_reliability": self._analyze_source_reliability(examples.get("found", [])),
            "extraction_success_analysis": self._analyze_extraction_patterns(extracted_products)
        }
        
        result.examples = dict(examples)
        
        # Generate recommendations adapted to extraction context
        self._generate_co2_extraction_recommendations(result, extraction_sources, data_quality_issues)
        
        self._calculate_base_metrics(result)
        return result
    
    def _analyze_co2_extraction(
        self,
        barcode: str,
        co2_sources: Dict[str, Optional[float]], 
        success_flags: Dict[str, bool],
        product_name: str,
        brand_name: str,
        result: FieldAnalysisResult,
        co2_values: List[float],
        extraction_sources: Counter,
        co2_ranges: Dict[str, int],
        examples: Dict[str, List],
        data_quality_issues: Counter,
        raw_api_data: Dict[str, Any]
    ):
        """Analyze CO2 extraction for a single product"""
        
        # Check if CO2 was successfully extracted
        has_co2 = success_flags.get("co2_total", False)
        
        if has_co2:
            # Find which source provided the CO2 data
            selected_co2_value = None
            selected_source = "not_found"
            
            # Apply same priority logic as ProductExtractor
            for source, value in co2_sources.items():
                if value is not None:
                    selected_co2_value = value
                    selected_source = source
                    break
            
            if selected_co2_value is not None:
                result.present_count += 1
                
                if self._is_valid_co2_value(selected_co2_value):
                    result.valid_count += 1
                    co2_values.append(selected_co2_value)
                    extraction_sources[selected_source] += 1
                    
                    # Categorize by CO2 range
                    self._categorize_co2_range(selected_co2_value, co2_ranges)
                    
                    self._add_example(examples, "found", {
                        "barcode": barcode,
                        "product_name": product_name,
                        "brand_name": brand_name,
                        "co2_total": selected_co2_value,
                        "source": selected_source,
                        "confidence": self._get_source_confidence(selected_source),
                        "co2_category": self._get_co2_category(selected_co2_value),
                        "all_sources_attempted": list(co2_sources.keys()),
                        "extraction_timestamp": raw_api_data.get("enrichment_timestamp")
                    })
                    
                else:
                    result.invalid_count += 1
                    data_quality_issues["invalid_range"] += 1
                    
                    self._add_example(examples, "invalid_range", {
                        "barcode": barcode,
                        "product_name": product_name,
                        "brand_name": brand_name,
                        "co2_total": selected_co2_value,
                        "source": selected_source,
                        "issue": f"CO2 value {selected_co2_value} outside valid range (0-10000)"
                    })
        else:
            result.missing_count += 1
            
            # Analyze why CO2 extraction failed
            missing_reason = self._analyze_extraction_failure(co2_sources, raw_api_data)
            data_quality_issues[missing_reason] += 1
            
            self._add_example(examples, "missing", {
                "barcode": barcode,
                "product_name": product_name,
                "brand_name": brand_name,
                "missing_reason": missing_reason,
                "sources_checked": list(co2_sources.keys()),
                "sources_available": {k: v is not None for k, v in co2_sources.items()},
                "raw_structures_present": self._check_raw_structures(raw_api_data)
            })
    
    def _analyze_extraction_failure(self, co2_sources: Dict[str, Optional[float]], raw_api_data: Dict[str, Any]) -> str:
        """Analyze why CO2 extraction failed using both extracted and raw data"""
        
        # Check if any sources had data
        sources_with_data = {k: v for k, v in co2_sources.items() if v is not None}
        
        if not sources_with_data:
            # Check raw data structures to understand why
            raw_response = raw_api_data.get("raw_api_response", {})
            
            if not raw_response.get('agribalyse'):
                if not raw_response.get('ecoscore_data'):
                    if not raw_response.get('nutriments'):
                        return "no_environmental_data_structures"
                    else:
                        return "nutriments_only_no_agribalyse"
                else:
                    return "ecoscore_data_without_agribalyse"
            else:
                return "agribalyse_present_but_no_co2"
        else:
            # Sources had data but extraction logic failed
            return "extraction_logic_failure"
    
    def _check_raw_structures(self, raw_api_data: Dict[str, Any]) -> Dict[str, bool]:
        """Check which raw structures were present in the API response"""
        raw_response = raw_api_data.get("raw_api_response", {})
        
        return {
            "has_agribalyse": bool(raw_response.get('agribalyse')),
            "has_ecoscore_data": bool(raw_response.get('ecoscore_data')),
            "has_nutriments": bool(raw_response.get('nutriments')),
            "api_response_valid": bool(raw_response)
        }
    
    def _get_source_confidence(self, source: str) -> str:
        """Get confidence level for CO2 source"""
        confidence_mapping = {
            "agribalyse_total": "high",
            "ecoscore_agribalyse_total": "high", 
            "nutriments_carbon_footprint": "medium",
            "nutriments_known_ingredients": "medium"
        }
        return confidence_mapping.get(source, "low")
    
    def _analyze_extraction_patterns(self, extracted_products: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze patterns in extraction success/failure"""
        
        total_products = len(extracted_products)
        extraction_patterns = {
            "total_analyzed": total_products,
            "successful_extractions": 0,
            "failed_extractions": 0,
            "source_usage_patterns": Counter(),
            "failure_patterns": Counter()
        }
        
        for barcode, product_data in extracted_products.items():
            extracted_fields = product_data.get("extracted_fields", {})
            success_flags = extracted_fields.get("extraction_success", {})
            co2_sources = extracted_fields.get("co2_sources", {})
            
            if success_flags.get("co2_total", False):
                extraction_patterns["successful_extractions"] += 1
                
                # Identify which source was used
                for source, value in co2_sources.items():
                    if value is not None:
                        extraction_patterns["source_usage_patterns"][source] += 1
                        break
            else:
                extraction_patterns["failed_extractions"] += 1
                
                # Analyze failure pattern
                available_sources = [k for k, v in co2_sources.items() if v is not None]
                if not available_sources:
                    extraction_patterns["failure_patterns"]["no_sources_available"] += 1
                else:
                    extraction_patterns["failure_patterns"]["sources_available_but_failed"] += 1
        
        return extraction_patterns
    
    def _is_valid_co2_value(self, co2_value: float) -> bool:
        """Check if CO2 value is in reasonable range"""
        return 0 <= co2_value <= 10000  # g CO2/100g
    

    
    def _categorize_co2_range(self, co2_value: float, co2_ranges: Dict[str, int]):
        """Categorize CO2 value into range"""
        if co2_value <= 100:
            co2_ranges["0-100"] += 1
        elif co2_value <= 500:
            co2_ranges["100-500"] += 1
        elif co2_value <= 1000:
            co2_ranges["500-1000"] += 1
        elif co2_value <= 2000:
            co2_ranges["1000-2000"] += 1
        else:
            co2_ranges["2000+"] += 1
    
    def _get_co2_category(self, co2_value: float) -> str:
        """Get carbon impact category"""
        if co2_value <= 100:
            return "LOW"
        elif co2_value <= 500:
            return "MEDIUM"
        elif co2_value <= 1000:
            return "HIGH"
        else:
            return "VERY_HIGH"
    
    def _create_co2_distribution_bins(self, co2_values: List[float]) -> Dict[str, int]:
        """Create detailed CO2 distribution bins"""
        if not co2_values:
            return {}
        
        bins = {
            "very_low(0-50)": 0,
            "low(50-100)": 0,
            "medium_low(100-300)": 0,
            "medium(300-500)": 0,
            "medium_high(500-800)": 0,
            "high(800-1200)": 0,
            "very_high(1200-2000)": 0,
            "extreme(2000+)": 0
        }
        
        for co2 in co2_values:
            if co2 <= 50:
                bins["very_low(0-50)"] += 1
            elif co2 <= 100:
                bins["low(50-100)"] += 1
            elif co2 <= 300:
                bins["medium_low(100-300)"] += 1
            elif co2 <= 500:
                bins["medium(300-500)"] += 1
            elif co2 <= 800:
                bins["medium_high(500-800)"] += 1
            elif co2 <= 1200:
                bins["high(800-1200)"] += 1
            elif co2 <= 2000:
                bins["very_high(1200-2000)"] += 1
            else:
                bins["extreme(2000+)"] += 1
        
        return bins
    
    def _calculate_co2_statistics(self, co2_values: List[float]) -> Dict[str, Any]:
        """Calculate detailed CO2 statistics"""
        if not co2_values:
            return {"count": 0}
        
        sorted_values = sorted(co2_values)
        n = len(sorted_values)
        
        return {
            "count": n,
            "average": sum(co2_values) / n,
            "median": sorted_values[n//2],
            "min": min(co2_values),
            "max": max(co2_values),
            "std_dev": self._calculate_std_dev(co2_values),
            "percentiles": {
                "p10": sorted_values[int(0.1 * n)],
                "p25": sorted_values[int(0.25 * n)],
                "p75": sorted_values[int(0.75 * n)],
                "p90": sorted_values[int(0.9 * n)],
                "p95": sorted_values[int(0.95 * n)]
            },
            "chocolate_context": self._analyze_chocolate_co2_context(co2_values),
            "extraction_performance": {
                "successful_extractions": n,
                "average_per_product": sum(co2_values) / n if co2_values else 0
            }
        }
    
    def _calculate_std_dev(self, values: List[float]) -> float:
        """Calculate standard deviation"""
        if len(values) < 2:
            return 0
        mean = sum(values) / len(values)
        variance = sum((x - mean) ** 2 for x in values) / len(values)
        return variance ** 0.5
    
    def _analyze_chocolate_co2_context(self, co2_values: List[float]) -> Dict[str, Any]:
        """Analyze CO2 values within chocolate context"""
        if not co2_values:
            return {}
        
        chocolate_benchmarks = {
            "chocolate_bar_typical": 500,
            "milk_chocolate_avg": 600,
            "dark_chocolate_avg": 400,
            "chocolate_spread_avg": 300,
        }
        
        avg_co2 = sum(co2_values) / len(co2_values)
        
        analysis = {
            "dataset_average": avg_co2,
            "vs_typical_chocolate": avg_co2 / chocolate_benchmarks["chocolate_bar_typical"],
            "distribution_analysis": {
                "below_typical": len([v for v in co2_values if v < chocolate_benchmarks["chocolate_bar_typical"]]),
                "above_typical": len([v for v in co2_values if v > chocolate_benchmarks["chocolate_bar_typical"]]),
            },
            "extraction_context": {
                "total_extracted": len(co2_values),
                "quality_assessment": "good" if avg_co2 < 800 else "concerning"
            }
        }
        
        return analysis
    
    def _analyze_source_reliability(self, found_examples: List[Dict]) -> Dict[str, Any]:
        """Analyze CO2 source reliability from extraction results"""
        source_reliability = defaultdict(list)
        
        for example in found_examples:
            source = example.get("source", "unknown")
            co2_value = example.get("co2_total")
            confidence = example.get("confidence", "unknown")
            
            if co2_value is not None:
                source_reliability[source].append({
                    "value": co2_value,
                    "confidence": confidence
                })
        
        reliability_analysis = {}
        for source, data_points in source_reliability.items():
            values = [dp["value"] for dp in data_points]
            confidences = [dp["confidence"] for dp in data_points]
            
            reliability_analysis[source] = {
                "count": len(values),
                "avg_value": sum(values) / len(values) if values else 0,
                "value_range": (min(values), max(values)) if values else (0, 0),
                "confidence_distribution": dict(Counter(confidences)),
                "std_dev": self._calculate_std_dev(values) if len(values) > 1 else 0,
                "extraction_success_rate": len(values)  # All these were successful extractions
            }
        
        return reliability_analysis
    
    def _generate_co2_extraction_recommendations(
        self,
        result: FieldAnalysisResult,
        extraction_sources: Counter,
        data_quality_issues: Counter
    ):
        """Generate recommendations adapted to extraction context"""
        
        # Extraction-specific recommendations
        if result.missing_count > 0:
            missing_percentage = (result.missing_count / result.total_products) * 100
            result.transformation_recommendations.append(
                f"EXTRACTION ISSUE: {result.missing_count} products failed CO2 extraction "
                f"({missing_percentage:.1f}% failure rate)"
            )
            
            if missing_percentage > 30:
                result.quality_improvement_suggestions.append(
                    "HIGH PRIORITY: Improve CO2 extraction logic - many products missing CO2 data"
                )
        
        # Source utilization analysis
        if extraction_sources:
            best_source = extraction_sources.most_common(1)[0]
            result.transformation_recommendations.append(
                f"Most successful source: {best_source[0]} ({best_source[1]} products)"
            )
            
            # Check source diversity
            if len(extraction_sources) == 1:
                result.quality_improvement_suggestions.append(
                    "Consider adding fallback CO2 sources for better coverage"
                )
            else:
                sources_list = ", ".join([f"{source}: {count}" for source, count in extraction_sources.items()])
                result.transformation_recommendations.append(
                    f"Multiple sources used: {sources_list}"
                )
        
        # Data quality issues from extraction
        extraction_failures = data_quality_issues.get("extraction_logic_failure", 0)
        if extraction_failures > 0:
            result.quality_improvement_suggestions.append(
                f"Extraction logic failures: {extraction_failures} cases - review extraction code"
            )
        
        no_structures = data_quality_issues.get("no_environmental_data_structures", 0)
        if no_structures > 0:
            result.quality_improvement_suggestions.append(
                f"Products without environmental data: {no_structures} cases - consider different data source"
            )
        
        # Production readiness assessment
        success_rate = (result.valid_count / result.total_products * 100) if result.total_products > 0 else 0
        if success_rate >= 80:
            result.transformation_recommendations.append(
                f"PRODUCTION READY: {success_rate:.1f}% CO2 extraction success rate"
            )
        elif success_rate >= 60:
            result.transformation_recommendations.append(
                f"ACCEPTABLE: {success_rate:.1f}% CO2 extraction success rate (minimum for bot launch)"
            )
        else:
            result.transformation_recommendations.append(
                f"NEEDS IMPROVEMENT: {success_rate:.1f}% CO2 extraction success rate - blocks bot functionality"
            )


# Integration function for use with ProductExtractor
def analyze_co2_from_extraction_results(extracted_products: Dict[str, Any]) -> FieldAnalysisResult:
    """
    Convenience function to analyze CO2 extraction from ProductExtractor results
    
    Usage:
        extraction_results = await extractor.run_complete_extraction()
        co2_analysis = analyze_co2_from_extraction_results(extraction_results["extracted_products"])
    """
    analyzer = CO2Analyzer()
    return analyzer.analyze_extracted_products(extracted_products)


# Testing
if __name__ == "__main__":
    print("🧪 TESTING UPDATED CO2 ANALYZER")
    print("=" * 50)
    
    # Mock extracted products data
    mock_extracted_products = {
        "123456789012": {
            "extracted_fields": {
                "barcode": "123456789012",
                "product_name": "Nutella Chocolate Spread",
                "brand_name": "Nutella",
                "co2_sources": {
                    "agribalyse_total": 539.0,
                    "ecoscore_agribalyse_total": None,
                    "nutriments_carbon_footprint": 539.0,
                    "nutriments_known_ingredients": None
                },
                "extraction_success": {
                    "co2_total": True
                }
            },
            "raw_api_data": {
                "enrichment_timestamp": "2025-01-02T14:30:22",
                "raw_api_response": {
                    "agribalyse": {"co2_total": 539.0},
                    "ecoscore_data": {},
                    "nutriments": {"carbon-footprint_100g": 539.0}
                }
            }
        },
        "987654321098": {
            "extracted_fields": {
                "barcode": "987654321098", 
                "product_name": "Dark Chocolate Bar",
                "brand_name": "Lindt",
                "co2_sources": {
                    "agribalyse_total": None,
                    "ecoscore_agribalyse_total": None,
                    "nutriments_carbon_footprint": None,
                    "nutriments_known_ingredients": None
                },
                "extraction_success": {
                    "co2_total": False
                }
            },
            "raw_api_data": {
                "enrichment_timestamp": "2025-01-02T14:30:23",
                "raw_api_response": {}
            }
        }
    }
    
    # Test the analyzer
    analyzer = CO2Analyzer()
    result = analyzer.analyze_extracted_products(mock_extracted_products)
    
    print(f"✅ Analysis completed:")
    print(f"   → Total products: {result.total_products}")
    print(f"   → Valid CO2 extractions: {result.valid_count}")
    print(f"   → Missing CO2: {result.missing_count}")
    print(f"   → Quality score: {result.quality_score:.1f}%")
    print(f"   → Extraction sources: {result.pattern_analysis['extraction_sources']}")