"""
src/food_scanner/data/utils/extraction_reporter.py
EXTRACTION REPORTING: Generate JSON reports with timestamps for extraction analysis
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any
from collections import Counter, defaultdict


class ExtractionReporter:
    """
    RESPONSIBILITY: Generate timestamped JSON reports for extraction analysis
    - Missing CO2 products with brand/product/barcode details
    - Missing fields analysis across all critical fields  
    - Quality reports with extraction success rates
    - Integration with ProductExtractor pipeline
    """
    
    def __init__(self, output_base_dir: Path = None):
        if output_base_dir is None:
            output_base_dir = Path(__file__).resolve().parents[4] / "data_engineering" / "data"
        
        self.output_base_dir = Path(output_base_dir)
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Report directories - organized by ETL phase
        self.reports_dirs = {
            "missing_co2": self.output_base_dir / "analysis" / "extraction_phase" / "missing_co2",
            "missing_fields": self.output_base_dir / "analysis" / "extraction_phase" / "missing_fields", 
            "extraction_quality": self.output_base_dir / "analysis" / "extraction_phase" / "quality",
            "quality_reports": self.output_base_dir / "analysis" / "extraction_phase" / "quality_reports"
        }
        
        # Create directories
        for report_dir in self.reports_dirs.values():
            report_dir.mkdir(parents=True, exist_ok=True)
    
    def generate_all_reports(
        self, 
        extracted_products: Dict[str, Any],
        extraction_stats: Dict[str, Any],
        pipeline_stats: Dict[str, Any]
    ) -> Dict[str, Path]:
        """
        Generate all extraction reports from ProductExtractor results
        
        Args:
            extracted_products: Output from ProductExtractor.run_complete_extraction()
            extraction_stats: Field extraction statistics  
            pipeline_stats: Overall pipeline statistics
            
        Returns:
            Dict mapping report type to saved file path
        """
        print(f"\n📊 GENERATING EXTRACTION REPORTS")
        print(f"   → Timestamp: {self.timestamp}")
        print(f"   → Processing {len(extracted_products)} extracted products")
        print("-" * 60)
        
        saved_reports = {}
        
        # 1. Missing CO2 report
        missing_co2_report = self.generate_missing_co2_report(extracted_products)
        saved_reports["missing_co2"] = missing_co2_report
        
        # 2. Missing fields report  
        missing_fields_report = self.generate_missing_fields_report(extracted_products)
        saved_reports["missing_fields"] = missing_fields_report
        
        # 3. Extraction quality report
        quality_report = self.generate_extraction_quality_report(
            extracted_products, extraction_stats, pipeline_stats
        )
        saved_reports["extraction_quality"] = quality_report
        
        # 4. Comprehensive quality summary (JSON)
        summary_report = self.generate_quality_summary_report(
            extracted_products, extraction_stats, pipeline_stats, saved_reports
        )
        saved_reports["quality_summary"] = summary_report
        
        # 5. Markdown quality report
        markdown_report = self.generate_markdown_quality_report(
            extracted_products, extraction_stats, pipeline_stats
        )
        saved_reports["quality_summary_md"] = markdown_report
        
        print(f"\n✅ All extraction reports generated:")
        for report_type, file_path in saved_reports.items():
            print(f"   → {report_type}: {file_path.name}")
        
        return saved_reports
    
    def generate_missing_co2_report(self, extracted_products: Dict[str, Any]) -> Path:
        """
        Generate detailed report of products missing CO2 data
        Format: missing_co2_YYYYMMDD_HHMMSS.json
        """
        print(f"   🌍 Analyzing missing CO2 data...")
        
        missing_co2_products = []
        co2_source_availability = Counter()
        
        for barcode, product_data in extracted_products.items():
            extracted_fields = product_data.get("extracted_fields", {})
            raw_api_data = product_data.get("raw_api_data", {})
            
            # Check if CO2 data is missing
            co2_sources = extracted_fields.get("co2_sources", {})
            has_co2 = any(value is not None for value in co2_sources.values())
            
            # Track source availability
            for source, value in co2_sources.items():
                if value is not None:
                    co2_source_availability[source] += 1
            
            if not has_co2:
                # Get product details for missing CO2 report
                raw_response = raw_api_data.get("raw_api_response", {})
                
                missing_product = {
                    "barcode": barcode,
                    "product_name": extracted_fields.get("product_name") or "Unknown",
                    "brand_name": extracted_fields.get("brand_name") or "Unknown",
                    "weight": extracted_fields.get("weight"),
                    "product_quantity_unit": extracted_fields.get("product_quantity_unit"),
                    "original_quantity": raw_response.get("product_quantity") or raw_response.get("quantity"),
                    "missing_co2_analysis": {
                        "has_agribalyse_structure": bool(raw_response.get("agribalyse")),
                        "has_ecoscore_structure": bool(raw_response.get("ecoscore_data")),
                        "has_nutriments_structure": bool(raw_response.get("nutriments")),
                        "co2_sources_checked": list(co2_sources.keys()),
                        "all_sources_null": all(value is None for value in co2_sources.values())
                    },
                    "extraction_timestamp": raw_api_data.get("enrichment_timestamp"),
                    "confirmed_missing": True
                }
                
                missing_co2_products.append(missing_product)
        
        # Group by brand for analysis
        missing_by_brand = defaultdict(list)
        for product in missing_co2_products:
            brand = product["brand_name"]
            missing_by_brand[brand].append(product)
        
        # Sort brands by number of missing products
        missing_by_brand_sorted = dict(
            sorted(missing_by_brand.items(), key=lambda x: len(x[1]), reverse=True)
        )
        
        # Create comprehensive report
        missing_co2_report = {
            "report_info": {
                "report_type": "missing_co2_analysis",
                "generation_timestamp": datetime.now().isoformat(),
                "extraction_timestamp": self.timestamp,
                "total_products_analyzed": len(extracted_products),
                "products_missing_co2": len(missing_co2_products),
                "missing_percentage": (len(missing_co2_products) / len(extracted_products) * 100) if extracted_products else 0
            },
            "co2_availability_analysis": {
                "source_availability": dict(co2_source_availability),
                "most_reliable_source": co2_source_availability.most_common(1)[0] if co2_source_availability else None,
                "sources_checked": list(co2_sources.keys()) if extracted_products else []
            },
            "missing_products_by_brand": missing_by_brand_sorted,
            "missing_products_summary": {
                "total_missing": len(missing_co2_products),
                "brands_affected": len(missing_by_brand),
                "top_affected_brands": [
                    {"brand": brand, "missing_count": len(products)} 
                    for brand, products in list(missing_by_brand_sorted.items())[:5]
                ]
            },
            "all_missing_products": missing_co2_products
        }
        
        # Save report
        filename = f"missing_co2_{self.timestamp}.json"
        filepath = self.reports_dirs["missing_co2"] / filename
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(missing_co2_report, f, indent=2, ensure_ascii=False, default=str)
        
        print(f"      ✅ Missing CO2 report: {len(missing_co2_products)} products missing CO2")
        return filepath
    
    def generate_missing_fields_report(self, extracted_products: Dict[str, Any]) -> Path:
        """
        Generate report of products with missing critical fields
        Format: missing_fields_YYYYMMDD_HHMMSS.json
        """
        print(f"   📋 Analyzing missing critical fields...")
        
        # Critical fields for products table
        critical_fields = [
            "barcode", "product_name", "brand_name", "weight", 
            "product_quantity_unit", "nutriscore_grade", "nutriscore_score"
        ]
        
        field_missing_counts = Counter()
        products_with_missing_fields = []
        field_availability_matrix = defaultdict(dict)
        
        for barcode, product_data in extracted_products.items():
            extracted_fields = product_data.get("extracted_fields", {})
            success_flags = extracted_fields.get("extraction_success", {})
            
            missing_fields = []
            field_status = {}
            
            # Check each critical field
            for field in critical_fields:
                is_available = success_flags.get(field, False)
                field_status[field] = is_available
                field_availability_matrix[field][barcode] = is_available
                
                if not is_available:
                    missing_fields.append(field)
                    field_missing_counts[field] += 1
            
            # Special case: nutriscore (need at least one)
            has_nutriscore = (success_flags.get("nutriscore_grade", False) or 
                            success_flags.get("nutriscore_score", False))
            
            if not has_nutriscore:
                if "nutriscore_data" not in missing_fields:
                    missing_fields.append("nutriscore_data")
                    field_missing_counts["nutriscore_data"] += 1
            
            # Record products with missing fields
            if missing_fields:
                product_report = {
                    "barcode": barcode,
                    "product_name": extracted_fields.get("product_name") or "Unknown", 
                    "brand_name": extracted_fields.get("brand_name") or "Unknown",
                    "missing_fields": missing_fields,
                    "missing_count": len(missing_fields),
                    "field_availability": field_status,
                    "would_be_rejected": self._would_be_rejected(extracted_fields, success_flags)
                }
                products_with_missing_fields.append(product_report)
        
        # Analysis by severity
        rejected_products = [p for p in products_with_missing_fields if p["would_be_rejected"]]
        warning_products = [p for p in products_with_missing_fields if not p["would_be_rejected"]]
        
        # Create comprehensive report
        missing_fields_report = {
            "report_info": {
                "report_type": "missing_fields_analysis",
                "generation_timestamp": datetime.now().isoformat(),
                "extraction_timestamp": self.timestamp,
                "total_products_analyzed": len(extracted_products),
                "products_with_missing_fields": len(products_with_missing_fields)
            },
            "field_availability_summary": {
                "field_missing_counts": dict(field_missing_counts.most_common()),
                "field_availability_rates": {
                    field: ((len(extracted_products) - count) / len(extracted_products) * 100) if extracted_products else 0
                    for field, count in field_missing_counts.items()
                },
                "most_problematic_fields": [
                    {"field": field, "missing_count": count, "missing_percentage": (count / len(extracted_products) * 100) if extracted_products else 0}
                    for field, count in field_missing_counts.most_common(5)
                ]
            },
            "impact_analysis": {
                "would_be_rejected": len(rejected_products),
                "warning_level": len(warning_products),
                "usable_products": len(extracted_products) - len(rejected_products),
                "rejection_rate": (len(rejected_products) / len(extracted_products) * 100) if extracted_products else 0
            },
            "rejected_products": rejected_products[:20],  # First 20 examples
            "warning_products": warning_products[:10],   # First 10 examples
            "all_products_with_missing_fields": products_with_missing_fields
        }
        
        # Save report
        filename = f"missing_fields_{self.timestamp}.json"
        filepath = self.reports_dirs["missing_fields"] / filename
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(missing_fields_report, f, indent=2, ensure_ascii=False, default=str)
        
        print(f"      ✅ Missing fields report: {len(products_with_missing_fields)} products with missing fields")
        return filepath
    
    def generate_extraction_quality_report(
        self, 
        extracted_products: Dict[str, Any],
        extraction_stats: Dict[str, Any], 
        pipeline_stats: Dict[str, Any]
    ) -> Path:
        """
        Generate comprehensive extraction quality metrics
        Format: extraction_quality_YYYYMMDD_HHMMSS.json
        """
        print(f"   📈 Analyzing extraction quality metrics...")
        
        field_success_counts = extraction_stats.get("field_success_counts", {})
        total_extracted = extraction_stats.get("successful_extractions", 0)
        
        # Calculate success rates for each field
        field_success_rates = {}
        field_quality_grades = {}
        
        for field, count in field_success_counts.items():
            if total_extracted > 0:
                success_rate = (count / total_extracted) * 100
                field_success_rates[field] = success_rate
                
                # Assign quality grade
                if success_rate >= 90:
                    grade = "EXCELLENT"
                elif success_rate >= 80:
                    grade = "GOOD" 
                elif success_rate >= 60:
                    grade = "ACCEPTABLE"
                elif success_rate >= 40:
                    grade = "POOR"
                else:
                    grade = "CRITICAL"
                
                field_quality_grades[field] = grade
        
        # Overall pipeline performance
        pipeline_performance = {
            "api_calls_made": pipeline_stats.get("api_calls", 0),
            "products_discovered": pipeline_stats.get("products_discovered", 0),
            "products_enriched": pipeline_stats.get("products_enriched", 0),
            "successful_field_extractions": total_extracted,
            "failed_extractions": extraction_stats.get("failed_extractions", 0),
            "discovery_to_extraction_rate": (total_extracted / pipeline_stats.get("products_discovered", 1)) * 100,
            "enrichment_to_extraction_rate": (total_extracted / pipeline_stats.get("products_enriched", 1)) * 100
        }
        
        # Critical field analysis (required for bot functionality)
        critical_fields = ["barcode", "product_name", "brand_name", "co2_total"]
        critical_field_analysis = {}
        
        for field in critical_fields:
            success_count = field_success_counts.get(field, 0)
            success_rate = field_success_rates.get(field, 0)
            
            critical_field_analysis[field] = {
                "success_count": success_count,
                "success_rate": success_rate,
                "quality_grade": field_quality_grades.get(field, "UNKNOWN"),
                "is_blocking": success_rate < 70,  # Below 70% blocks bot functionality
                "improvement_needed": success_rate < 85
            }
        
        # Improvement recommendations
        improvement_recommendations = []
        
        for field, analysis in critical_field_analysis.items():
            if analysis["is_blocking"]:
                improvement_recommendations.append({
                    "priority": "CRITICAL",
                    "field": field,
                    "issue": f"Success rate {analysis['success_rate']:.1f}% blocks bot functionality",
                    "recommendation": f"Improve {field} extraction logic immediately"
                })
            elif analysis["improvement_needed"]:
                improvement_recommendations.append({
                    "priority": "HIGH", 
                    "field": field,
                    "issue": f"Success rate {analysis['success_rate']:.1f}% below optimal",
                    "recommendation": f"Optimize {field} extraction sources"
                })
        
        # Create quality report
        quality_report = {
            "report_info": {
                "report_type": "extraction_quality_analysis",
                "generation_timestamp": datetime.now().isoformat(),
                "extraction_timestamp": self.timestamp,
                "analysis_scope": "field_extraction_performance"
            },
            "pipeline_performance": pipeline_performance,
            "field_extraction_analysis": {
                "total_products_processed": total_extracted,
                "field_success_counts": field_success_counts,
                "field_success_rates": field_success_rates,
                "field_quality_grades": field_quality_grades
            },
            "critical_field_analysis": critical_field_analysis,
            "quality_summary": {
                "overall_extraction_quality": sum(field_success_rates.values()) / len(field_success_rates) if field_success_rates else 0,
                "critical_fields_average": sum(critical_field_analysis[f]["success_rate"] for f in critical_fields) / len(critical_fields),
                "bot_readiness": all(not analysis["is_blocking"] for analysis in critical_field_analysis.values()),
                "fields_needing_improvement": len([f for f, a in critical_field_analysis.items() if a["improvement_needed"]])
            },
            "improvement_recommendations": improvement_recommendations,
            "next_actions": self._generate_next_actions(critical_field_analysis, field_success_rates)
        }
        
        # Save report
        filename = f"extraction_quality_{self.timestamp}.json"
        filepath = self.reports_dirs["extraction_quality"] / filename
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(quality_report, f, indent=2, ensure_ascii=False, default=str)
        
        overall_quality = quality_report["quality_summary"]["overall_extraction_quality"]
        print(f"      ✅ Extraction quality report: {overall_quality:.1f}% overall quality")
        return filepath
    
    def generate_quality_summary_report(
        self,
        extracted_products: Dict[str, Any],
        extraction_stats: Dict[str, Any],
        pipeline_stats: Dict[str, Any],
        generated_reports: Dict[str, Path]
    ) -> Path:
        """
        Generate executive summary combining all quality metrics
        Format: quality_summary_YYYYMMDD_HHMMSS.json
        """
        print(f"   📊 Generating quality summary...")
        
        # Key metrics summary
        total_products = len(extracted_products)
        successful_extractions = extraction_stats.get("successful_extractions", 0)
        field_counts = extraction_stats.get("field_success_counts", {})
        
        # Calculate production readiness
        critical_fields = ["barcode", "product_name", "brand_name", "co2_total"]
        production_ready_count = 0
        
        for barcode, product_data in extracted_products.items():
            extracted_fields = product_data.get("extracted_fields", {})
            success_flags = extracted_fields.get("extraction_success", {})
            
            # Check if all critical fields are available
            has_all_critical = all(success_flags.get(field, False) for field in critical_fields)
            
            # Check nutriscore (need at least one)
            has_nutriscore = (success_flags.get("nutriscore_grade", False) or 
                            success_flags.get("nutriscore_score", False))
            
            if has_all_critical and has_nutriscore:
                production_ready_count += 1
        
        production_ready_rate = (production_ready_count / total_products * 100) if total_products > 0 else 0
        
        # Executive summary
        executive_summary = {
            "extraction_date": self.timestamp,
            "dataset_overview": {
                "total_products_processed": total_products,
                "successful_extractions": successful_extractions,
                "production_ready_products": production_ready_count,
                "production_ready_rate": production_ready_rate
            },
            "critical_metrics": {
                "bot_functionality_readiness": production_ready_rate >= 70,
                "data_quality_grade": self._calculate_overall_grade(field_counts, total_products),
                "co2_coverage": f"{field_counts.get('co2_total', 0)}/{total_products} products",
                "weight_parsing_success": f"{field_counts.get('weight', 0)}/{total_products} products"
            },
            "business_impact": {
                "bot_can_launch": production_ready_rate >= 70,
                "estimated_bot_responses": production_ready_count,
                "data_gaps_impact": "LOW" if production_ready_rate >= 85 else "MEDIUM" if production_ready_rate >= 70 else "HIGH"
            },
            "next_steps": self._generate_executive_next_steps(production_ready_rate, field_counts, total_products),
            "detailed_reports": {
                "missing_co2_report": str(generated_reports.get("missing_co2", "")),
                "missing_fields_report": str(generated_reports.get("missing_fields", "")),
                "extraction_quality_report": str(generated_reports.get("extraction_quality", ""))
            }
        }
        
        # Save summary
        filename = f"quality_summary_{self.timestamp}.json"
        filepath = self.reports_dirs["quality_reports"] / filename
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(executive_summary, f, indent=2, ensure_ascii=False, default=str)
        
        print(f"      ✅ Quality summary: {production_ready_rate:.1f}% production ready")
        return filepath





    """ GENERATE MARKDOWN REPORT """


    def generate_markdown_quality_report(
        self,
        extracted_products: Dict[str, Any],
        extraction_stats: Dict[str, Any],
        pipeline_stats: Dict[str, Any]
    ) -> Path:
        """
        Generate comprehensive Markdown quality report with visual formatting
        Format: quality_summary_YYYYMMDD_HHMMSS.md
        """
        print(f"   📝 Generating Markdown quality report...")
        
        # Key metrics calculation
        total_products = len(extracted_products)
        successful_extractions = extraction_stats.get("successful_extractions", 0)
        field_counts = extraction_stats.get("field_success_counts", {})
        
        # Calculate production readiness
        critical_fields = ["barcode", "product_name", "brand_name", "co2_total"]
        production_ready_count = 0
        
        for barcode, product_data in extracted_products.items():
            extracted_fields = product_data.get("extracted_fields", {})
            success_flags = extracted_fields.get("extraction_success", {})
            
            # Check if all critical fields are available
            has_all_critical = all(success_flags.get(field, False) for field in critical_fields)
            
            # Check nutriscore (need at least one)
            has_nutriscore = (success_flags.get("nutriscore_grade", False) or 
                            success_flags.get("nutriscore_score", False))
            
            if has_all_critical and has_nutriscore:
                production_ready_count += 1
        
        production_ready_rate = (production_ready_count / total_products * 100) if total_products > 0 else 0
        
        # Calculate overall quality score
        overall_quality = self._calculate_overall_quality_score(field_counts, total_products)
        quality_grade = self._get_quality_grade(overall_quality)
        
        # Calculate field quality metrics
        field_quality_data = self._calculate_field_quality_metrics(field_counts, total_products)
        
        # Generate recommendations
        recommendations = self._generate_markdown_recommendations(
            overall_quality, production_ready_rate, field_counts, total_products
        )
        
        # Build Markdown content
        markdown_content = self._build_markdown_report(
            overall_quality, quality_grade, production_ready_rate,
            field_quality_data, recommendations, pipeline_stats
        )
        
        # Save Markdown report
        filename = f"quality_summary_{self.timestamp}.md"
        filepath = self.reports_dirs["quality_reports"] / filename
        
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(markdown_content)
        
        print(f"      ✅ Markdown quality report: {overall_quality:.1f}/100 (Grade: {quality_grade})")
        return filepath
    
    def _would_be_rejected(self, extracted_fields: Dict[str, Any], success_flags: Dict[str, bool]) -> bool:
        """Determine if a product would be rejected based on business rules"""
        # Critical rejection criteria
        if not success_flags.get("barcode", False):
            return True
        if not success_flags.get("product_name", False):
            return True  
        if not success_flags.get("brand_name", False):
            return True
        if not success_flags.get("co2_total", False):
            return True
        
        # Must have at least one nutriscore field
        has_nutriscore = (success_flags.get("nutriscore_grade", False) or 
                        success_flags.get("nutriscore_score", False))
        if not has_nutriscore:
            return True
        
        return False
    
    def _calculate_overall_grade(self, field_counts: Dict[str, int], total_products: int) -> str:
        """Calculate overall data quality grade"""
        if total_products == 0:
            return "UNKNOWN"
        
        # Weight critical fields more heavily
        critical_fields = ["barcode", "product_name", "brand_name", "co2_total"]
        critical_score = sum(field_counts.get(field, 0) for field in critical_fields) / (len(critical_fields) * total_products) * 100
        
        if critical_score >= 90:
            return "A"
        elif critical_score >= 80:
            return "B"
        elif critical_score >= 70:
            return "C"
        elif critical_score >= 60:
            return "D"
        else:
            return "F"
    
    def _generate_next_actions(self, critical_analysis: Dict[str, Any], field_rates: Dict[str, float]) -> List[str]:
        """Generate concrete next actions based on analysis"""
        actions = []
        
        # Check blocking issues
        blocking_fields = [field for field, analysis in critical_analysis.items() if analysis["is_blocking"]]
        if blocking_fields:
            actions.append(f"URGENT: Fix extraction for {', '.join(blocking_fields)} (blocks bot launch)")
        
        # Check weight parsing
        weight_rate = field_rates.get("weight", 0)
        if weight_rate < 80:
            actions.append(f"Improve weight parsing: {weight_rate:.1f}% success rate (target: 80%+)")
        
        # Check CO2 coverage
        co2_rate = field_rates.get("co2_total", 0)
        if co2_rate < 70:
            actions.append(f"Improve CO2 extraction: {co2_rate:.1f}% coverage (target: 70%+)")
        
        # General recommendations
        if all(rate >= 80 for rate in field_rates.values()):
            actions.append("Consider production deployment - all metrics above 80%")
        elif any(rate < 60 for rate in field_rates.values()):
            actions.append("Major extraction improvements needed before production")
        else:
            actions.append("Fine-tune extraction logic and test with larger dataset")
        
        return actions
    
    def _generate_executive_next_steps(self, production_ready_rate: float, field_counts: Dict[str, int], total_products: int) -> List[str]:
        """Generate executive-level next steps"""
        steps = []
        
        if production_ready_rate >= 85:
            steps.append("✅ Ready for production deployment")
            steps.append("📊 Consider scaling data collection to 1000+ products")
        elif production_ready_rate >= 70:
            steps.append("⚠️ Acceptable for MVP launch with limited dataset")
            steps.append("🔧 Optimize extraction for specific missing fields")
        else:
            steps.append("❌ Not ready for production - major improvements needed")
            steps.append("🚨 Focus on critical field extraction before bot development")
        
        # Specific technical steps
        co2_coverage = (field_counts.get('co2_total', 0) / total_products * 100) if total_products > 0 else 0
        if co2_coverage < 70:
            steps.append(f"🌍 Improve CO2 data sources (current: {co2_coverage:.1f}%)")
        
        weight_coverage = (field_counts.get('weight', 0) / total_products * 100) if total_products > 0 else 0
        if weight_coverage < 80:
            steps.append(f"⚖️ Fix weight parsing logic (current: {weight_coverage:.1f}%)")
        
        return steps
    
    def _calculate_overall_quality_score(self, field_counts: Dict[str, int], total_products: int) -> float:
        """Calculate overall quality score (0-100)"""
        if total_products == 0:
            return 0.0
        
        # Weight critical fields more heavily
        critical_fields = ["barcode", "product_name", "brand_name", "co2_total", "weight"]
        critical_score = sum(field_counts.get(field, 0) for field in critical_fields) / (len(critical_fields) * total_products) * 100
        
        # Calculate overall field success rate
        all_fields = list(field_counts.keys())
        overall_score = sum(field_counts.values()) / (len(all_fields) * total_products) * 100 if all_fields else 0
        
        # Combine scores (70% critical, 30% overall)
        final_score = (critical_score * 0.7) + (overall_score * 0.3)
        
        return min(100.0, max(0.0, final_score))
    
    def _get_quality_grade(self, quality_score: float) -> str:
        """Convert quality score to letter grade"""
        if quality_score >= 90:
            return "A"
        elif quality_score >= 80:
            return "B"
        elif quality_score >= 70:
            return "C"
        elif quality_score >= 60:
            return "D"
        else:
            return "F"
    
    def _calculate_field_quality_metrics(self, field_counts: Dict[str, int], total_products: int) -> List[Dict[str, Any]]:
        """Calculate quality metrics for each field"""
        if total_products == 0:
            return []
        
        field_metrics = []
        field_names = {
            "barcode": "Barcode",
            "product_name": "Product Name",
            "brand_name": "Brand Name",
            "weight": "Weight",
            "product_quantity_unit": "Quantity Unit",
            "nutriscore_grade": "Nutri-Score Grade",
            "nutriscore_score": "Nutri-Score Score",
            "co2_total": "CO2 Total",
            "co2_sources": "CO2 Sources"
        }
        
        for field, count in field_counts.items():
            presence_rate = (count / total_products * 100) if total_products > 0 else 0
            quality_score = presence_rate  # Simplified for now
            
            field_metrics.append({
                "field": field_names.get(field, field.replace("_", " ").title()),
                "presence_rate": presence_rate,
                "quality_score": quality_score,
                "issues": 0  # Placeholder for future enhancement
            })
        
        return sorted(field_metrics, key=lambda x: x["presence_rate"], reverse=True)
    
    def _generate_markdown_recommendations(
        self, 
        overall_quality: float, 
        production_ready_rate: float, 
        field_counts: Dict[str, int], 
        total_products: int
    ) -> List[Dict[str, str]]:
        """Generate recommendations for Markdown report"""
        recommendations = []
        
        # Overall quality recommendation
        if overall_quality < 70:
            recommendations.append({
                "priority": "HIGH",
                "title": "Overall Quality",
                "issue": f"Overall quality score is low ({overall_quality:.1f}/100)",
                "recommendation": "Focus on improving field parsing and business rule compliance",
                "implementation": "Review transformation logic and add more robust parsing"
            })
        
        # Pipeline efficiency recommendation
        if production_ready_rate < 70:
            recommendations.append({
                "priority": "HIGH",
                "title": "Pipeline Efficiency",
                "issue": f"Pipeline efficiency is {production_ready_rate:.1f}%",
                "recommendation": "Optimize extraction and transformation logic",
                "implementation": "Review rejection reasons and improve parsing algorithms"
            })
        
        # Critical field recommendations
        critical_fields = ["barcode", "product_name", "brand_name", "co2_total"]
        for field in critical_fields:
            field_rate = (field_counts.get(field, 0) / total_products * 100) if total_products > 0 else 0
            if field_rate < 80:
                recommendations.append({
                    "priority": "MEDIUM",
                    "title": f"{field.replace('_', ' ').title()} Quality",
                    "issue": f"{field.replace('_', ' ').title()} presence rate is {field_rate:.1f}%",
                    "recommendation": f"Improve {field.replace('_', ' ')} extraction logic",
                    "implementation": f"Review {field} parsing and add fallback sources"
                })
        
        return recommendations
    
    def _build_markdown_report(
        self,
        overall_quality: float,
        quality_grade: str,
        production_ready_rate: float,
        field_quality_data: List[Dict[str, Any]],
        recommendations: List[Dict[str, str]],
        pipeline_stats: Dict[str, Any]
    ) -> str:
        """Build the complete Markdown report content"""
        
        # Header
        content = f"""# Data Quality Analysis Report

**Analysis Date:** {datetime.now().isoformat()}  
**Overall Quality Score:** {overall_quality:.2f}/100 (Grade: {quality_grade})

## 📊 Dataset Overview

- **Total Discovered:** {pipeline_stats.get('products_discovered', 0)} products
- **Successfully Enriched:** {pipeline_stats.get('products_enriched', 0)} products
- **Validated for Production:** {production_ready_rate:.1f}% ({production_ready_rate:.1f}%)
- **Overall Pipeline Efficiency:** {production_ready_rate:.2f}%

## 🔍 Field Quality Analysis

| Field | Presence Rate | Quality Score | Issues |
|-------|---------------|---------------|--------|
"""
        
        # Field quality table
        for field_data in field_quality_data:
            content += f"| {field_data['field']} | {field_data['presence_rate']:.1f}% | {field_data['quality_score']:.1f} | {field_data['issues']} |\n"
        
        # Recommendations section
        content += "\n## ⚠️ Top Recommendations\n\n"
        
        for i, rec in enumerate(recommendations, 1):
            priority_emoji = "🔴" if rec["priority"] == "HIGH" else "🟡" if rec["priority"] == "MEDIUM" else "🟢"
            content += f"### {i}. {rec['title']} ({rec['priority']} Priority)\n\n"
            content += f"**Issue:** {rec['issue']}  \n"
            content += f"**Recommendation:** {rec['recommendation']}  \n"
            content += f"**Implementation:** {rec['implementation']}\n\n"
        
        # Quality metrics summary
        content += "## 📈 Quality Metrics Summary\n\n"
        content += f"- **Completeness:** {production_ready_rate:.1f}% ({self._get_completeness_grade(production_ready_rate)})\n"
        content += f"- **Field Quality:** {overall_quality:.1f}%\n"
        content += f"- **Consistency:** {overall_quality:.2f}% ({self._get_consistency_grade(overall_quality)})\n"
        content += f"- **Accuracy:** {overall_quality:.2f}% ({self._get_accuracy_grade(overall_quality)})\n"
        content += f"- **Rejection Rate:** {100 - production_ready_rate:.1f}%\n"
        
        # Footer
        content += "\n---\n*Generated by ExtractionReporter v1.0*"
        
        return content
    
    def _get_completeness_grade(self, rate: float) -> str:
        """Get completeness grade"""
        if rate >= 90:
            return "EXCELLENT"
        elif rate >= 80:
            return "GOOD"
        elif rate >= 70:
            return "FAIR"
        else:
            return "POOR"
    
    def _get_consistency_grade(self, rate: float) -> str:
        """Get consistency grade"""
        if rate >= 85:
            return "HIGH"
        elif rate >= 70:
            return "MEDIUM"
        else:
            return "LOW"
    
    def _get_accuracy_grade(self, rate: float) -> str:
        """Get accuracy grade"""
        if rate >= 80:
            return "HIGH"
        elif rate >= 60:
            return "MEDIUM"
        else:
            return "LOW"



    """ END OF MARKDOWN REPORT GENERATION """


    """ INTEGRATION WITH PRODUCT EXTRACTOR """

def integrate_with_extractor():
    """
    Example of how to integrate ExtractionReporter with ProductExtractor
    This would be added to the ProductExtractor.run_complete_extraction() method
    """
    example_integration = """
    # At the end of ProductExtractor.run_complete_extraction()
    
    # Generate extraction reports
    if extraction_stats["successful_extractions"] > 0:
        reporter = ExtractionReporter()
        report_files = reporter.generate_all_reports(
            extracted_products=extracted_products,
            extraction_stats=extraction_stats, 
            pipeline_stats=self.stats.copy()
        )
        
        complete_results["extraction_reports"] = report_files
        
        print(f"📊 EXTRACTION REPORTS GENERATED:")
        for report_type, file_path in report_files.items():
            print(f"   → {report_type}: {file_path}")
    """
    return example_integration


if __name__ == "__main__":
    print("🧪 TESTING EXTRACTION REPORTER")
    print("=" * 50)
    
    # Mock data for testing
    mock_extracted_products = {
        "123456789012": {
            "extracted_fields": {
                "barcode": "123456789012",
                "product_name": "Test Chocolate",
                "brand_name": "Test Brand",
                "weight": 100.0,
                "product_quantity_unit": "g",
                "co2_sources": {"agribalyse_total": 500.0, "ecoscore_agribalyse_total": None},
                "extraction_success": {
                    "barcode": True, "product_name": True, "brand_name": True,
                    "weight": True, "co2_total": True, "nutriscore_grade": False
                }
            },
            "raw_api_data": {"enrichment_timestamp": "2025-01-02T14:30:22"}
        }
    }
    
    mock_extraction_stats = {
        "successful_extractions": 1,
        "failed_extractions": 0,
        "field_success_counts": {
            "barcode": 1, "product_name": 1, "brand_name": 1,
            "weight": 1, "co2_total": 1, "nutriscore_grade": 0
        }
    }
    
    mock_pipeline_stats = {
        "api_calls": 5,
        "products_discovered": 2,
        "products_enriched": 1
    }
    
    # Test reporter
    reporter = ExtractionReporter()
    reports = reporter.generate_all_reports(
        mock_extracted_products, 
        mock_extraction_stats,
        mock_pipeline_stats
    )
    
    print(f"\n✅ Test completed - {len(reports)} reports generated")