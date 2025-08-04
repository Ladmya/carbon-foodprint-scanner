"""
src/food_scanner/data/analysis/barcode_analyzer.py
UPDATED: Barcode analyzer adapted for ProductExtractor structure
Analyzes barcode extraction and validates STRING format requirements
"""

from typing import Dict, List, Any
from collections import Counter, defaultdict

from food_scanner.data.analysis.base_analyzer import BaseFieldAnalyzer
from food_scanner.core.models.data_quality import FieldAnalysisResult, FieldType


class BarcodeAnalyzer(BaseFieldAnalyzer):
    """
    UPDATED Barcode analyzer for extracted products structure
    
    CHANGES FROM ORIGINAL:
    - Analyzes extracted barcodes instead of raw code fields
    - Focuses on extraction success rather than raw field validation
    - Validates extracted barcode format and leading zeros
    - Compatible with ProductExtractor integration
    """
    

    
    def analyze_extracted_products(self, extracted_products: Dict[str, Any]) -> FieldAnalysisResult:
        """
        Analyze barcode extraction from extracted products
        
        Args:
            extracted_products: Output from ProductExtractor.run_complete_extraction()
        """
        result = FieldAnalysisResult(
            field_name="barcode",
            field_type=FieldType.IDENTIFIER,
            total_products=len(extracted_products)
        )
        
        valid_lengths = Counter()
        invalid_examples = []
        examples = defaultdict(list)
        leading_zeros_analysis = {
            "total_with_leading_zeros": 0,
            "by_zero_count": Counter(),
            "by_length": Counter(),
            "examples": []
        }
        
        for barcode, product_data in extracted_products.items():
            extracted_fields = product_data.get("extracted_fields", {})
            success_flags = extracted_fields.get("extraction_success", {})
            
            # Get extracted barcode value
            extracted_barcode = extracted_fields.get("barcode")
            product_name = extracted_fields.get("product_name", "Unknown")
            
            # Analyze extraction success
            if success_flags.get("barcode", False):
                result.present_count += 1
                
                # Validate extracted barcode
                if self._validate_extracted_barcode(extracted_barcode, barcode, product_name, 
                                                result, valid_lengths, examples, leading_zeros_analysis):
                    result.valid_count += 1
                else:
                    result.invalid_count += 1
            else:
                result.missing_count += 1
                self._add_example(invalid_examples, "extraction_failed", {
                    "expected_barcode": barcode,
                    "extracted_barcode": extracted_barcode,
                    "product_name": product_name,
                    "issue": "Barcode extraction failed"
                })
        
        # Pattern analysis
        result.pattern_analysis = {
            "length_distribution": dict(valid_lengths.most_common()),
            "most_common_length": valid_lengths.most_common(1)[0] if valid_lengths else None,
            "valid_lengths_found": sorted(valid_lengths.keys()),
            "leading_zeros_analysis": self._finalize_leading_zeros_analysis(leading_zeros_analysis),
            "extraction_consistency": self._analyze_extraction_consistency(extracted_products)
        }
        
        # Examples
        result.examples = {
            "invalid": invalid_examples[:5],
            "valid_patterns": dict(examples),
            "length_examples": self._get_length_examples(extracted_products, valid_lengths)
        }
        
        # Generate recommendations
        self._generate_barcode_extraction_recommendations(result, leading_zeros_analysis)
        
        self._calculate_base_metrics(result)
        return result
    
    def _validate_extracted_barcode(
        self,
        extracted_barcode: str,
        original_barcode: str,
        product_name: str,
        result: FieldAnalysisResult,
        valid_lengths: Counter,
        examples: Dict[str, List],
        leading_zeros_analysis: Dict[str, Any]
    ) -> bool:
        """Validate extracted barcode and collect analysis data"""
        
        if not extracted_barcode:
            self._add_example(examples, "empty_extracted", {
                "original_barcode": original_barcode,
                "extracted_barcode": extracted_barcode,
                "product_name": product_name,
                "issue": "Extracted barcode is empty"
            })
            return False
        
        if not isinstance(extracted_barcode, str):
            self._add_example(examples, "non_string", {
                "original_barcode": original_barcode,
                "extracted_barcode": str(extracted_barcode),
                "extracted_type": type(extracted_barcode).__name__,
                "product_name": product_name,
                "issue": f"Extracted barcode is {type(extracted_barcode).__name__}, not string"
            })
            return False
        
        if not self._is_numeric_string(extracted_barcode):
            self._add_example(examples, "non_numeric", {
                "original_barcode": original_barcode,
                "extracted_barcode": extracted_barcode,
                "product_name": product_name,
                "issue": "Extracted barcode contains non-numeric characters"
            })
            return False
        
        if not self._is_valid_barcode_length(extracted_barcode):
            self._add_example(examples, "invalid_length", {
                "original_barcode": original_barcode,
                "extracted_barcode": extracted_barcode,
                "length": len(extracted_barcode),
                "product_name": product_name,
                "issue": f"Invalid length: {len(extracted_barcode)} (must be 8-18)"
            })
            return False
        
        # Consistency check
        if extracted_barcode != original_barcode:
            self._add_example(examples, "extraction_inconsistency", {
                "original_barcode": original_barcode,
                "extracted_barcode": extracted_barcode,
                "product_name": product_name,
                "issue": "Extracted barcode doesn't match original"
            })
            return False
        
        # Valid barcode - collect analysis data
        valid_lengths[len(extracted_barcode)] += 1
        
        # Analyze leading zeros (CRITICAL for STRING storage)
        if extracted_barcode.startswith('0'):
            leading_zeros_count = len(extracted_barcode) - len(extracted_barcode.lstrip('0'))
            
            leading_zeros_analysis["total_with_leading_zeros"] += 1
            leading_zeros_analysis["by_zero_count"][leading_zeros_count] += 1
            leading_zeros_analysis["by_length"][len(extracted_barcode)] += 1
            
            if len(leading_zeros_analysis["examples"]) < 5:
                leading_zeros_analysis["examples"].append({
                    "barcode": extracted_barcode,
                    "leading_zeros_count": leading_zeros_count,
                    "length": len(extracted_barcode),
                    "product_name": product_name
                })
        
        # Pattern analysis
        self._analyze_barcode_patterns(extracted_barcode, examples)
        
        return True
    
    def _analyze_extraction_consistency(self, extracted_products: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze consistency between original and extracted barcodes"""
        
        consistency_analysis = {
            "total_checked": 0,
            "consistent": 0,
            "inconsistent": 0,
            "extraction_failed": 0,
            "inconsistency_examples": []
        }
        
        for original_barcode, product_data in extracted_products.items():
            extracted_fields = product_data.get("extracted_fields", {})
            success_flags = extracted_fields.get("extraction_success", {})
            extracted_barcode = extracted_fields.get("barcode")
            
            consistency_analysis["total_checked"] += 1
            
            if not success_flags.get("barcode", False):
                consistency_analysis["extraction_failed"] += 1
            elif extracted_barcode == original_barcode:
                consistency_analysis["consistent"] += 1
            else:
                consistency_analysis["inconsistent"] += 1
                
                if len(consistency_analysis["inconsistency_examples"]) < 5:
                    consistency_analysis["inconsistency_examples"].append({
                        "original": original_barcode,
                        "extracted": extracted_barcode,
                        "product_name": extracted_fields.get("product_name", "Unknown")
                    })
        
        return consistency_analysis
    
    def _finalize_leading_zeros_analysis(self, leading_zeros_analysis: Dict[str, Any]) -> Dict[str, Any]:
        """Finalize leading zeros analysis with proper dict conversion"""
        return {
            "total_with_leading_zeros": leading_zeros_analysis["total_with_leading_zeros"],
            "by_zero_count": dict(leading_zeros_analysis["by_zero_count"]),
            "by_length": dict(leading_zeros_analysis["by_length"]),
            "examples": leading_zeros_analysis["examples"],
            "critical_for_database": leading_zeros_analysis["total_with_leading_zeros"] > 0
        }
    
    def _analyze_barcode_patterns(self, barcode: str, examples: Dict[str, List]):
        """Analyze valid barcode patterns"""
        length = len(barcode)
        
        # Common barcode format analysis
        if length == 13:  # EAN-13
            prefix = barcode[:3]
            if "ean13_prefixes" not in examples:
                examples["ean13_prefixes"] = Counter()
            examples["ean13_prefixes"][prefix] += 1
            
        elif length == 12:  # UPC-A
            prefix = barcode[:1]
            if "upc_prefixes" not in examples:
                examples["upc_prefixes"] = Counter()
            examples["upc_prefixes"][prefix] += 1
        
        # Leading zeros examples (CRITICAL)
        if barcode.startswith('0'):
            if "leading_zeros_examples" not in examples:
                examples["leading_zeros_examples"] = []
            if len(examples["leading_zeros_examples"]) < 5:
                examples["leading_zeros_examples"].append({
                    "barcode": barcode,
                    "leading_zeros_count": len(barcode) - len(barcode.lstrip('0')),
                    "length": length
                })
    
    def _get_length_examples(self, extracted_products: Dict[str, Any], valid_lengths: Counter) -> Dict[str, List]:
        """Get examples of barcodes by length"""
        length_examples = {}
        
        for length in sorted(valid_lengths.keys()):
            length_examples[f"length_{length}"] = []
            count = 0
            
            for original_barcode, product_data in extracted_products.items():
                extracted_fields = product_data.get("extracted_fields", {})
                extracted_barcode = extracted_fields.get("barcode")
                
                if (extracted_barcode and 
                    isinstance(extracted_barcode, str) and 
                    self._is_numeric_string(extracted_barcode) and 
                    len(extracted_barcode) == length and
                    count < 3):
                    
                    length_examples[f"length_{length}"].append({
                        "barcode": extracted_barcode,
                        "product_name": extracted_fields.get("product_name", "Unknown"),
                        "starts_with_zero": extracted_barcode.startswith('0')
                    })
                    count += 1
        
        return length_examples
    
    def _generate_barcode_extraction_recommendations(
        self,
        result: FieldAnalysisResult,
        leading_zeros_analysis: Dict[str, Any]
    ):
        """Generate barcode extraction recommendations"""
        
        # Extraction success recommendations
        if result.missing_count > 0:
            result.transformation_recommendations.append(
                f"FIX EXTRACTION: {result.missing_count} products failed barcode extraction"
            )
        
        if result.invalid_count > 0:
            result.transformation_recommendations.append(
                f"VALIDATE EXTRACTION: {result.invalid_count} products have invalid extracted barcodes"
            )
        
        # Critical technical recommendations for leading zeros
        leading_zeros_count = leading_zeros_analysis["total_with_leading_zeros"]
        if leading_zeros_count > 0:
            result.transformation_recommendations.append(
                f"CRITICAL DATABASE REQUIREMENT: {leading_zeros_count} barcodes start with zeros - "
                "MUST use TEXT/VARCHAR data type, NOT INTEGER"
            )
            result.quality_improvement_suggestions.append(
                "MANDATORY: Ensure database schema uses TEXT/VARCHAR for barcode field "
                "to preserve leading zeros during storage"
            )
        
        # Extraction consistency recommendations
        consistency = result.pattern_analysis.get("extraction_consistency", {})
        inconsistent_count = consistency.get("inconsistent", 0)
        if inconsistent_count > 0:
            result.quality_improvement_suggestions.append(
                f"Extraction inconsistency: {inconsistent_count} barcodes don't match original - "
                "review extraction logic"
            )
        
        # Production readiness assessment
        extraction_success_rate = (result.valid_count / result.total_products * 100) if result.total_products > 0 else 0
        if extraction_success_rate >= 95:
            result.transformation_recommendations.append(
                f"EXCELLENT: {extraction_success_rate:.1f}% barcode extraction success rate"
            )
        elif extraction_success_rate >= 90:
            result.transformation_recommendations.append(
                f"GOOD: {extraction_success_rate:.1f}% barcode extraction success rate"
            )
        else:
            result.transformation_recommendations.append(
                f"NEEDS IMPROVEMENT: {extraction_success_rate:.1f}% barcode extraction success rate - "
                "barcodes are critical for primary key"
            )
        
        # Format recommendations
        length_dist = result.pattern_analysis.get("length_distribution", {})
        if length_dist:
            most_common = max(length_dist.items(), key=lambda x: x[1])
            result.transformation_recommendations.append(
                f"Most common format: {most_common[0]} digits ({most_common[1]} products)"
            )
    
    def _is_numeric_string(self, barcode: str) -> bool:
        """Check if barcode contains only digits"""
        return barcode.isdigit()
    
    def _is_valid_barcode_length(self, barcode: str) -> bool:
        """Check if barcode length is valid (8-18 digits)"""
        return 8 <= len(barcode) <= 18
    



# Integration function
def analyze_barcode_from_extraction_results(extracted_products: Dict[str, Any]) -> FieldAnalysisResult:
    """
    Convenience function to analyze barcode extraction from ProductExtractor results
    
    Usage:
        extraction_results = await extractor.run_complete_extraction()
        barcode_analysis = analyze_barcode_from_extraction_results(extraction_results["extracted_products"])
    """
    analyzer = BarcodeAnalyzer()
    return analyzer.analyze_extracted_products(extracted_products)


if __name__ == "__main__":
    print("🧪 TESTING UPDATED BARCODE ANALYZER")
    print("=" * 50)
    
    # Mock extracted products with various barcode scenarios
    mock_extracted_products = {
        "0123456789012": {  # Leading zero case
            "extracted_fields": {
                "barcode": "0123456789012",
                "product_name": "Product with leading zero",
                "extraction_success": {"barcode": True}
            }
        },
        "3017620422003": {  # Normal EAN-13
            "extracted_fields": {
                "barcode": "3017620422003", 
                "product_name": "Nutella",
                "extraction_success": {"barcode": True}
            }
        },
        "invalid_barcode": {  # Extraction failed
            "extracted_fields": {
                "barcode": None,
                "product_name": "Failed extraction",
                "extraction_success": {"barcode": False}
            }
        }
    }
    
    # Test the analyzer
    analyzer = BarcodeAnalyzer()
    result = analyzer.analyze_extracted_products(mock_extracted_products)
    
    print(f"✅ Analysis completed:")
    print(f"   → Total products: {result.total_products}")
    print(f"   → Valid extractions: {result.valid_count}")
    print(f"   → Failed extractions: {result.missing_count}")
    print(f"   → Leading zeros detected: {result.pattern_analysis['leading_zeros_analysis']['total_with_leading_zeros']}")
    print(f"   → Quality score: {result.quality_score:.1f}%")
    
    if result.pattern_analysis['leading_zeros_analysis']['total_with_leading_zeros'] > 0:
        print(f"   🚨 CRITICAL: Use TEXT/VARCHAR for database storage!")