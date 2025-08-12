"""
src/food_scanner/data/extractors/analysis/analyzers/text_field_analyzer.py
UPDATED: Text field analyzer adapted for ProductExtractor structure
Analyzes text field extraction (product_name, brand_name) from extracted products
"""

from typing import Dict, List, Any
from collections import Counter, defaultdict

from food_scanner.data.extractors.analysis.analyzers.base_field_analyzer import BaseFieldAnalyzer
from food_scanner.core.models.data_quality import FieldAnalysisResult, FieldType


class TextFieldAnalyzer(BaseFieldAnalyzer):
    """
    UPDATED Text field analyzer for extracted products structure
    
    CHANGES FROM ORIGINAL:
    - Analyzes extracted text fields instead of raw field navigation
    - Focuses on extraction success rather than raw field fallback logic
    - Validates extracted text quality and completeness
    - Compatible with ProductExtractor integration
    """
    

    
    def analyze_text_field_from_extracted_products(
        self, 
        extracted_products: Dict[str, Any],
        field_name: str = "product_name"
    ) -> FieldAnalysisResult:
        """
        Analyze text field extraction from ALL extracted products
        
        Args:
            extracted_products: Output from ProductExtractor.run_complete_extraction()
            field_name: Field to analyze ("product_name" or "brand_name")
        """
        result = FieldAnalysisResult(
            field_name=field_name,
            field_type=FieldType.TEXT,
            total_products=len(extracted_products)
        )
        
        extraction_sources = Counter()
        examples = defaultdict(list)
        text_analysis = {
            "lengths": [],
            "language_patterns": {},
            "quality_issues": Counter()
        }
        
        for barcode, product_data in extracted_products.items():
            extracted_fields = product_data.get("extracted_fields", {})
            success_flags = extracted_fields.get("extraction_success", {})
            raw_api_data = product_data.get("raw_api_data", {})
            
            # Analyze extraction for this field
            self._analyze_text_field_from_extracted_product(
                barcode, field_name, extracted_fields, success_flags, raw_api_data,
                result, extraction_sources, examples, text_analysis
            )
        
        # Pattern analysis
        result.pattern_analysis = {
            "extraction_sources": dict(extraction_sources),
            "text_length_analysis": self._analyze_text_field_text_lengths(text_analysis["lengths"]),
            "quality_issues": dict(text_analysis["quality_issues"]),
            "extraction_performance": self._analyze_text_field_extraction_performance(extracted_products, field_name)
        }
        
        # Add field-specific analysis
        if field_name == "product_name":
            result.pattern_analysis["language_analysis"] = self._analyze_text_field_product_name_languages(extracted_products)
        elif field_name == "brand_name":
            result.pattern_analysis["brand_distribution"] = self._analyze_text_field_brand_distribution(extracted_products)
        
        result.examples = dict(examples)
        
        # Generate recommendations
        self._generate_text_field_extraction_recommendations(result, field_name, extraction_sources, text_analysis)
        
        self._calculate_base_metrics(result)
        return result
    
    def _analyze_text_field_from_extracted_product(
        self,
        barcode: str,
        field_name: str,
        extracted_fields: Dict[str, Any],
        success_flags: Dict[str, bool],
        raw_api_data: Dict[str, Any],
        result: FieldAnalysisResult,
        extraction_sources: Counter,
        examples: Dict[str, List],
        text_analysis: Dict[str, Any]
        ):
        """Analyze text field extraction for ONE SINGLE product"""
        
        extracted_value = extracted_fields.get(field_name)
        extraction_successful = success_flags.get(field_name, False)
        
        if extraction_successful and extracted_value:
            result.present_count += 1
            
            # Validate extracted text quality
            if self._validate_extracted_text_field(extracted_value, barcode, field_name, examples, text_analysis):
                result.valid_count += 1
                
                # Analyze extraction source
                source = self._identify_text_field_extraction_source(raw_api_data, field_name, extracted_value)
                extraction_sources[source] += 1
                
                # Collect text analysis data
                text_analysis["lengths"].append(len(extracted_value))
                
                self._add_example(examples, "successful", {
                    "barcode": barcode,
                    "field_name": field_name,
                    "extracted_value": extracted_value,
                    "value_length": len(extracted_value),
                    "extraction_source": source,
                    "quality": self._assess_text_field_text_quality(extracted_value)
                })
                
            else:
                result.invalid_count += 1
        else:
            result.missing_count += 1
            
            # Analyze why extraction failed
            failure_reason = self._analyze_text_field_extraction_failure(raw_api_data, field_name)
            text_analysis["quality_issues"][failure_reason] += 1
            
            self._add_example(examples, "failed", {
                "barcode": barcode,
                "field_name": field_name,
                "extracted_value": extracted_value,
                "extraction_success": extraction_successful,
                "failure_reason": failure_reason,
                "raw_data_available": self._check_text_field_raw_data_availability(raw_api_data, field_name)
            })
    
    def _validate_extracted_text_field(
        self,
        extracted_value: str,
        barcode: str,
        field_name: str,
        examples: Dict[str, List],
        text_analysis: Dict[str, Any]
        ) -> bool:
        """Validate quality of extracted text"""
        
        if not isinstance(extracted_value, str):
            text_analysis["quality_issues"]["non_string_type"] += 1
            self._add_example(examples, "invalid_type", {
                "barcode": barcode,
                "field_name": field_name,
                "extracted_value": str(extracted_value),
                "type": type(extracted_value).__name__,
                "issue": "Extracted value is not a string"
            })
            return False
        
        if not extracted_value.strip():
            text_analysis["quality_issues"]["empty_string"] += 1
            self._add_example(examples, "empty", {
                "barcode": barcode,
                "field_name": field_name,
                "extracted_value": extracted_value,
                "issue": "Extracted value is empty or whitespace"
            })
            return False
        
        # Quality checks
        trimmed_value = extracted_value.strip()
        
        if len(trimmed_value) < 3:
            text_analysis["quality_issues"]["too_short"] += 1
            return False
        
        if len(trimmed_value) > 200:
            text_analysis["quality_issues"]["too_long"] += 1
            return False
        
        # Check for suspicious patterns
        if self.text_field_has_suspicious_patterns(trimmed_value):
            text_analysis["quality_issues"]["suspicious_content"] += 1
            return False
        
        return True
    
    def _identify_text_field_extraction_source(
        self,
        raw_api_data: Dict[str, Any],
        field_name: str,
        extracted_value: str
        ) -> str:
        """Identify which source was likely used for extraction for ONE SINGLE product"""
        
        raw_response = raw_api_data.get("raw_api_response", {})
        
        if field_name == "product_name":
            # Check French vs English sources
            name_fr = raw_response.get('product_name_fr', '').strip()
            name_en = raw_response.get('product_name', '').strip()
            
            if name_fr and extracted_value == name_fr:
                return "product_name_fr"
            elif name_en and extracted_value == name_en:
                return "product_name_fallback"
            else:
                return "unknown_source"
        
        elif field_name == "brand_name":
            # Check brand sources
            brands = raw_response.get('brands', '').strip()
            brands_tags = raw_response.get('brands_tags', [])
            brands_imported = raw_response.get('brands_imported', '').strip()
            
            if brands and extracted_value == brands:
                return "brands_direct"
            elif brands_tags and len(brands_tags) > 0:
                formatted_tag = brands_tags[0].replace('-', ' ').title()
                if extracted_value == formatted_tag:
                    return "brands_tags_formatted"
            elif brands_imported and extracted_value == brands_imported:
                return "brands_imported"
            else:
                return "unknown_source"
        
        return "unknown_field"
    
    def _analyze_text_field_extraction_failure(self, raw_api_data: Dict[str, Any], field_name: str) -> str:
        """Analyze why text extraction failed"""
        
        raw_response = raw_api_data.get("raw_api_response", {})
        
        if not raw_response:
            return "no_raw_api_data"
        
        if field_name == "product_name":
            name_fr = raw_response.get('product_name_fr', '').strip()
            name_en = raw_response.get('product_name', '').strip()
            
            if not name_fr and not name_en:
                return "both_name_fields_empty"
            elif name_fr and not name_en:
                return "only_french_name_available"
            elif name_en and not name_fr:
                return "only_english_name_available"
            else:
                return "extraction_logic_failed"
        
        elif field_name == "brand_name":
            brands = raw_response.get('brands', '').strip()
            brands_tags = raw_response.get('brands_tags', [])
            brands_imported = raw_response.get('brands_imported', '').strip()
            
            if not brands and not brands_tags and not brands_imported:
                return "all_brand_fields_empty"
            else:
                return "extraction_logic_failed"
        
        return "unknown_failure"
    
    def _check_text_field_raw_data_availability(self, raw_api_data: Dict[str, Any], field_name: str) -> Dict[str, bool]:
        """Check availability of raw data fields"""
        
        raw_response = raw_api_data.get("raw_api_response", {})
        
        if field_name == "product_name":
            return {
                "product_name_fr": bool(raw_response.get('product_name_fr', '').strip()),
                "product_name": bool(raw_response.get('product_name', '').strip())
            }
        elif field_name == "brand_name":
            return {
                "brands": bool(raw_response.get('brands', '').strip()),
                "brands_tags": bool(raw_response.get('brands_tags', [])),
                "brands_imported": bool(raw_response.get('brands_imported', '').strip())
            }
        
        return {}
    
    def text_field_has_suspicious_patterns(self, text: str) -> bool:
        """Check for suspicious patterns in extracted text"""
        # Check for excessive special characters
        special_char_ratio = sum(1 for c in text if not c.isalnum() and c != ' ') / len(text)
        if special_char_ratio > 0.3:
            return True
        
        # Check for repeated characters
        if any(char * 5 in text for char in "abcdefghijklmnopqrstuvwxyz"):
            return True
        
        # Check for URL-like patterns
        if any(pattern in text.lower() for pattern in ["http", "www.", ".com", ".fr"]):
            return True
        
        return False
    
    def _assess_text_field_text_quality(self, text: str) -> str:
        """Assess overall quality of extracted text"""
        if len(text.strip()) < 10:
            return "short"
        elif len(text.strip()) > 100:
            return "long"
        elif any(char.isupper() for char in text) and any(char.islower() for char in text):
            return "good_case_mixing"
        else:
            return "acceptable"
    

    
    def _analyze_text_field_text_lengths(self, lengths: List[int]) -> Dict[str, Any]:
        """Analyze distribution of text lengths"""
        if not lengths:
            return {"count": 0, "average_length": 0}
        
        return {
            "count": len(lengths),
            "average_length": sum(lengths) / len(lengths),
            "min_length": min(lengths),
            "max_length": max(lengths),
            "length_distribution": {
                "very_short(1-10)": len([l for l in lengths if 1 <= l <= 10]),
                "short(11-30)": len([l for l in lengths if 11 <= l <= 30]),
                "medium(31-60)": len([l for l in lengths if 31 <= l <= 60]),
                "long(61+)": len([l for l in lengths if l > 60])
            }
        }
    
    def _analyze_text_field_extraction_performance(self, extracted_products: Dict[str, Any], field_name: str) -> Dict[str, Any]:
        """Analyze extraction performance patterns"""
        
        performance = {
            "total_attempted": 0,
            "successful_extractions": 0,
            "failed_extractions": 0,
            "success_rate": 0
        }
        
        for barcode, product_data in extracted_products.items():
            extracted_fields = product_data.get("extracted_fields", {})
            success_flags = extracted_fields.get("extraction_success", {})
            
            performance["total_attempted"] += 1
            
            if success_flags.get(field_name, False):
                performance["successful_extractions"] += 1
            else:
                performance["failed_extractions"] += 1
        
        if performance["total_attempted"] > 0:
            performance["success_rate"] = (performance["successful_extractions"] / performance["total_attempted"]) * 100
        
        return performance
    
    def _analyze_text_field_product_name_languages(self, extracted_products: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze language patterns for product names"""
        
        language_analysis = {
            "french_source_used": 0,
            "english_fallback_used": 0,
            "language_comparison": {
                "fr_longer": 0,
                "en_longer": 0,
                "similar_length": 0
            }
        }
        
        for barcode, product_data in extracted_products.items():
            raw_api_data = product_data.get("raw_api_data", {})
            raw_response = raw_api_data.get("raw_api_response", {})
            extracted_fields = product_data.get("extracted_fields", {})
            
            extracted_name = extracted_fields.get("product_name")
            name_fr = raw_response.get('product_name_fr', '').strip()
            name_en = raw_response.get('product_name', '').strip()
            
            if extracted_name:
                if name_fr and extracted_name == name_fr:
                    language_analysis["french_source_used"] += 1
                elif name_en and extracted_name == name_en:
                    language_analysis["english_fallback_used"] += 1
                
                # Compare lengths
                if name_fr and name_en:
                    if len(name_fr) > len(name_en) * 1.2:
                        language_analysis["language_comparison"]["fr_longer"] += 1
                    elif len(name_en) > len(name_fr) * 1.2:
                        language_analysis["language_comparison"]["en_longer"] += 1
                    else:
                        language_analysis["language_comparison"]["similar_length"] += 1
        
        return language_analysis
    
    def _analyze_text_field_brand_distribution(self, extracted_products: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze brand distribution and extraction patterns"""
        
        brand_counter = Counter()
        extraction_source_analysis = Counter()
        
        for barcode, product_data in extracted_products.items():
            extracted_fields = product_data.get("extracted_fields", {})
            raw_api_data = product_data.get("raw_api_data", {})
            
            brand_name = extracted_fields.get("brand_name")
            if brand_name:
                brand_counter[brand_name] += 1
                
                # Identify extraction source
                source = self._identify_text_field_extraction_source(raw_api_data, "brand_name", brand_name)
                extraction_source_analysis[source] += 1
        
        return {
            "top_brands": dict(brand_counter.most_common(10)),
            "total_unique_brands": len(brand_counter),
            "extraction_source_distribution": dict(extraction_source_analysis),
            "brand_extraction_diversity": len(extraction_source_analysis)
        }
    
    def _generate_text_field_extraction_recommendations(
        self,
        result: FieldAnalysisResult,
        field_name: str,
        extraction_sources: Counter,
        text_analysis: Dict[str, Any]
        ):
        """Generate text field extraction recommendations"""
        
        # Extraction success recommendations
        if result.missing_count > 0:
            missing_percentage = (result.missing_count / result.total_products) * 100
            result.transformation_recommendations.append(
                f"EXTRACTION IMPROVEMENT: {result.missing_count} products failed {field_name} extraction "
                f"({missing_percentage:.1f}% failure rate)"
            )
        
        # Quality recommendations
        quality_issues = text_analysis["quality_issues"]
        if quality_issues:
            most_common_issue = quality_issues.most_common(1)[0]
            result.quality_improvement_suggestions.append(
                f"Most common issue: {most_common_issue[0]} ({most_common_issue[1]} cases)"
            )
        
        # Source utilization recommendations
        if extraction_sources:
            best_source = extraction_sources.most_common(1)[0]
            result.transformation_recommendations.append(
                f"Primary extraction source: {best_source[0]} ({best_source[1]} products)"
            )
            
            if field_name == "product_name":
                french_used = extraction_sources.get("product_name_fr", 0)
                english_used = extraction_sources.get("product_name_fallback", 0)
                
                if french_used > english_used:
                    result.transformation_recommendations.append(
                        f"French names preferred: {french_used} vs {english_used} English fallbacks"
                    )
                elif english_used > 0:
                    result.transformation_recommendations.append(
                        f"English fallback used for {english_used} products - consider improving French data"
                    )
            
            elif field_name == "brand_name":
                direct_brands = extraction_sources.get("brands_direct", 0)
                formatted_tags = extraction_sources.get("brands_tags_formatted", 0)
                
                if formatted_tags > 0:
                    result.transformation_recommendations.append(
                        f"Brand tags formatting used for {formatted_tags} products - good fallback coverage"
                    )
        
        # Production readiness assessment
        success_rate = (result.valid_count / result.total_products * 100) if result.total_products > 0 else 0
        
        if success_rate >= 95:
            result.transformation_recommendations.append(
                f"EXCELLENT: {success_rate:.1f}% {field_name} extraction success rate"
            )
        elif success_rate >= 85:
            result.transformation_recommendations.append(
                f"GOOD: {success_rate:.1f}% {field_name} extraction success rate"
            )
        elif success_rate >= 70:
            result.transformation_recommendations.append(
                f"ACCEPTABLE: {success_rate:.1f}% {field_name} extraction success rate"
            )
        else:
            result.transformation_recommendations.append(
                f"NEEDS IMPROVEMENT: {success_rate:.1f}% {field_name} extraction success rate - "
                f"{field_name} is critical for bot display"
            )
        
        # Length analysis recommendations
        length_analysis = result.pattern_analysis.get("text_length_analysis", {})
        avg_length = length_analysis.get("average_length", 0)
        
        if avg_length > 0:
            if avg_length < 10:
                result.quality_improvement_suggestions.append(
                    f"{field_name} seems too short (average: {avg_length:.1f} characters) - check extraction quality"
                )
            elif avg_length > 80:
                result.quality_improvement_suggestions.append(
                    f"{field_name} seems too long (average: {avg_length:.1f} characters) - may need truncation"
                )


if __name__ == "__main__":
    print("🧪 TESTING UPDATED TEXT FIELD ANALYZER")
    print("=" * 50)
    
    # Mock extracted products with various text scenarios
    mock_extracted_products = {
        "3017620422003": {
            "extracted_fields": {
                "product_name": "Nutella Pâte à tartiner aux noisettes et au cacao",
                "brand_name": "Nutella",
                "extraction_success": {"product_name": True, "brand_name": True}
            },
            "raw_api_data": {
                "raw_api_response": {
                    "product_name_fr": "Nutella Pâte à tartiner aux noisettes et au cacao",
                    "product_name": "Nutella Hazelnut Spread",
                    "brands": "Nutella"
                }
            }
        },
        "failed_extraction": {
            "extracted_fields": {
                "product_name": None,
                "brand_name": None,
                "extraction_success": {"product_name": False, "brand_name": False}
            },
            "raw_api_data": {
                "raw_api_response": {}
            }
        }
    }
    
    # Test product name analysis
    print("📝 Testing product name analysis...")
    product_name_result = TextFieldAnalyzer.analyze_text_field_from_extracted_products(mock_extracted_products, "product_name")
    print(f"   → Success rate: {product_name_result.validity_rate:.1f}%")
    
    # Test brand name analysis
    print("🏷️ Testing brand name analysis...")
    brand_name_result = TextFieldAnalyzer.analyze_text_field_from_extracted_products(mock_extracted_products, "brand_name")
    print(f"   → Success rate: {brand_name_result.validity_rate:.1f}%")
    
    print(f"\n✅ Text field analysis completed")