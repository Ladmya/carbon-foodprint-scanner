"""
src/food_scanner/data/transformers/reporting/transformation_reporter.py
PURE REPORTING: Generates formatted reports from transformation results
RESPONSIBILITY: Formatting and file generation ONLY - no analysis logic
"""

import sys
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional

# Add src to path for standalone execution
sys.path.insert(0, str(Path(__file__).resolve().parents[5] / "src"))


class TransformationReporter:
    """
    PURE REPORTING ENGINE: Generates reports from transformation results
    
    RESPONSIBILITIES:
    1. Format transformation results into different output formats
    2. Generate JSON, Markdown reports
    3. Save reports to files with proper organization
    4. NO analysis logic - only presentation and formatting
    5. Generate reports with precise naming convention
    """
    
    def __init__(self, output_base_dir: Path = None):
        if output_base_dir is None:
            output_base_dir = Path(__file__).resolve().parents[4] / "data" / "reports"
        
        self.output_base_dir = Path(output_base_dir)
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Report directories - organized by type
        self.reports_dirs = {
            "json_reports": self.output_base_dir / "json",
            "markdown_reports": self.output_base_dir / "markdown", 
            "summary_reports": self.output_base_dir / "summaries"
        }
        
        # Create directories
        for report_dir in self.reports_dirs.values():
            report_dir.mkdir(parents=True, exist_ok=True)
        
        # Reporting statistics
        self.reporting_stats = {
            "reports_generated": 0,
            "summaries_displayed": 0,
            "json_reports": 0,
            "markdown_reports": 0
        }
    
    def generate_all_reports(
        self,
        transformation_results: Dict[str, Any]
        ) -> Dict[str, Path]:
        """
        Generate all report formats from transformation results
        
        Args:
            transformation_results: Results from transformation pipeline
            
        Returns:
            Dict mapping report type to saved file path
        """
        print(f"\n📊 GENERATING TRANSFORMATION REPORTS")
        print(f"   → Timestamp: {self.timestamp}")
        print(f"   → Source: {transformation_results.get('transformation_stats', {}).get('total_products_processed', 0)} processed products")
        print("-" * 60)
        
        saved_reports = {}
        
        try:
            # 1. Quality validation JSON report
            json_report = self.generate_quality_validation_json_report(transformation_results)
            saved_reports["quality_validation_json"] = json_report
            self.reporting_stats["json_reports"] += 1
            
            # 2. Executive summary (JSON)
            executive_json = self.generate_executive_summary_json(transformation_results)
            saved_reports["executive_summary_json"] = executive_json
            self.reporting_stats["json_reports"] += 1
            
            # 3. Production readiness report (JSON)
            production_json = self.generate_production_readiness_json(transformation_results)
            saved_reports["production_readiness_json"] = production_json
            self.reporting_stats["json_reports"] += 1
            
            # 4. Markdown quality report
            markdown_report = self.generate_quality_summary_markdown(transformation_results)
            saved_reports["quality_summary_md"] = markdown_report
            self.reporting_stats["markdown_reports"] += 1
            
            # 5. Display console summary
            self.display_transformation_summary(transformation_results)
            self.reporting_stats["summaries_displayed"] += 1
            
            # Update total reports generated
            self.reporting_stats["reports_generated"] = len(saved_reports)
            
            print(f"\n✅ All transformation reports generated:")
            for report_type, file_path in saved_reports.items():
                print(f"   → {report_type}: {file_path.name}")
            
            return saved_reports
            
        except Exception as e:
            print(f"❌ Report generation failed: {e}")
            return {"error": str(e)}
    
    def generate_quality_validation_json_report(
        self, 
        transformation_results: Dict[str, Any]
    ) -> Path:
        """
        Generate quality validation JSON report for transformation results
        
        Args:
            transformation_results: Complete transformation results
            
        Returns:
            Path to saved JSON report
        """
        print(f"   📄 Generating quality validation JSON report...")
        
        # Format transformation results for JSON
        json_data = {
            "report_metadata": {
                "report_type": "quality_validation_transformation_analysis",
                "generation_timestamp": datetime.now().isoformat(),
                "transformation_timestamp": transformation_results.get("transformation_stats", {}).get("start_time"),
                "total_products_processed": transformation_results.get("transformation_stats", {}).get("total_products_processed", 0),
                "report_version": "2.0-modular"
            },
            "transformation_stats": transformation_results.get("transformation_stats", {}),
            "production_readiness": transformation_results.get("production_readiness", {}),
            "quality_validation": transformation_results.get("quality_validation", {}),
            "validation_stats": self._format_validation_stats(transformation_results),
            "cleaning_stats": self._format_cleaning_stats(transformation_results),
            "calculation_stats": self._format_calculation_stats(transformation_results),
            "data_quality_issues": transformation_results.get("data_quality_issues", []),
            "products_missing_co2": transformation_results.get("products_missing_co2", [])
        }
        
        # Save quality validation JSON report
        filename = f"transformation_analysis_quality_validation_{self.timestamp}.json"
        filepath = self.reports_dirs["json_reports"] / filename
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(json_data, f, indent=2, ensure_ascii=False, default=str)
        
        print(f"      ✅ Quality validation JSON report saved: {filename}")
        return filepath
    
    def generate_executive_summary_json(
        self, 
        transformation_results: Dict[str, Any]
    ) -> Path:
        """
        Generate executive summary JSON report
        
        Args:
            transformation_results: Complete transformation results
            
        Returns:
            Path to saved executive summary
        """
        print(f"   📋 Generating transformation executive summary...")
        
        stats = transformation_results.get("transformation_stats", {})
        production = transformation_results.get("production_readiness", {})
        quality = transformation_results.get("quality_validation", {})
        
        # Executive-focused data
        executive_data = {
            "executive_summary": {
                "transformation_date": stats.get("start_time"),
                "dataset_overview": {
                    "total_products_processed": stats.get("total_products_processed", 0),
                    "successful_transformations": stats.get("successful_transformations", 0),
                    "rejected_products": stats.get("rejected_products", 0),
                    "duplicates_handled": stats.get("duplicate_products", 0),
                    "success_rate": production.get("success_rate", 0)
                },
                "business_impact": {
                    "bot_ready_for_launch": production.get("bot_launch_ready", False),
                    "mvp_viable": production.get("minimum_viable_dataset", False),
                    "production_ready_products": production.get("complete_products_ready_for_db", 0),
                    "data_quality_grade": production.get("data_quality_grade", "F"),
                    "deployment_recommendation": self._get_deployment_recommendation(production)
                },
                "quality_metrics": {
                    "overall_quality_score": quality.get("overall_quality_score", 0),
                    "critical_issues_count": len(transformation_results.get("data_quality_issues", [])),
                    "anomalies_detected": len(quality.get("anomalies", [])),
                    "field_quality_summary": self._summarize_field_quality(quality)
                },
                "next_steps": production.get("next_steps", [])
            },
            "detailed_reports_available": {
                "quality_validation": f"transformation_analysis_quality_validation_{self.timestamp}.json",
                "quality_summary": f"transformation_reporter_quality_summary_{self.timestamp}.md",
                "field_specific_reports": "Available in field_reports/ directory"
            }
        }
        
        # Save executive summary
        filename = f"transformation_reporter_executive_summary_{self.timestamp}.json"
        filepath = self.reports_dirs["summary_reports"] / filename
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(executive_data, f, indent=2, ensure_ascii=False, default=str)
        
        success_rate = production.get("success_rate", 0)
        print(f"      ✅ Executive summary: {success_rate:.1f}% success rate")
        return filepath
    
    def generate_production_readiness_json(
        self, 
        transformation_results: Dict[str, Any]
    ) -> Path:
        """
        Generate production readiness JSON report
        
        Args:
            transformation_results: Complete transformation results
            
        Returns:
            Path to saved production readiness report
        """
        print(f"   🚀 Generating production readiness report...")
        
        production = transformation_results.get("production_readiness", {})
        quality = transformation_results.get("quality_validation", {})
        
        production_data = {
            "production_readiness": {
                "assessment_timestamp": datetime.now().isoformat(),
                "overall_status": self._get_production_status(production),
                "readiness_metrics": {
                    "total_products": production.get("total_products_processed", 0),
                    "validated_products": production.get("validated_products", 0),
                    "rejected_products": production.get("rejected_products", 0),
                    "complete_products": production.get("complete_products_ready_for_db", 0),
                    "success_rate": production.get("success_rate", 0),
                    "quality_grade": production.get("data_quality_grade", "F")
                },
                "deployment_criteria": {
                    "bot_launch_ready": production.get("bot_launch_ready", False),
                    "minimum_viable_dataset": production.get("minimum_viable_dataset", False),
                    "required_products": 100,
                    "required_success_rate": 80
                },
                "quality_assessment": {
                    "overall_score": quality.get("overall_quality_score", 0),
                    "field_quality_scores": quality.get("field_quality_scores", {}),
                    "anomalies_summary": self._summarize_anomalies(quality.get("anomalies", [])),
                    "critical_issues": transformation_results.get("data_quality_issues", [])
                },
                "recommendations": {
                    "immediate_actions": self._get_immediate_actions(production),
                    "short_term_goals": self._get_short_term_goals(production),
                    "long_term_strategy": self._get_long_term_strategy(production)
                }
            }
        }
        
        # Save production readiness report
        filename = f"transformation_reporter_production_readiness_{self.timestamp}.json"
        filepath = self.reports_dirs["summary_reports"] / filename
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(production_data, f, indent=2, ensure_ascii=False, default=str)
        
        print(f"      ✅ Production readiness report saved: {filename}")
        return filepath
    
    def generate_quality_summary_markdown(
        self, 
        transformation_results: Dict[str, Any]
    ) -> Path:
        """
        Generate comprehensive Markdown quality report
        
        Args:
            transformation_results: Complete transformation results
            
        Returns:
            Path to saved Markdown report
        """
        print(f"   📝 Generating Markdown quality report...")
        
        # Build Markdown content
        markdown_content = self._build_markdown_content(transformation_results)
        
        # Save Markdown report
        filename = f"transformation_reporter_quality_summary_{self.timestamp}.md"
        filepath = self.reports_dirs["markdown_reports"] / filename
        
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(markdown_content)
        
        print(f"      ✅ Markdown quality report saved: {filename}")
        return filepath
    
    def display_transformation_summary(
        self, 
        results: Dict[str, Any]
    ):
        """
        Display comprehensive transformation summary to console
        
        Args:
            results: Transformation results to display
        """
        stats = results.get("transformation_stats", {})
        production_readiness = results.get("production_readiness", {})
        quality_validation = results.get("quality_validation", {})
        
        print(f"\n🎯 TRANSFORMATION SUMMARY")
        print("=" * 60)
        
        print(f"📊 PROCESSING RESULTS:")
        print(f"   → Total products processed: {stats.get('total_products_processed', 0)}")
        print(f"   → Successful transformations: {stats.get('successful_transformations', 0)}")
        print(f"   → Products rejected: {stats.get('rejected_products', 0)}")
        print(f"   → Duplicates handled: {stats.get('duplicate_products', 0)}")
        
        print(f"\n✅ VALIDATION RESULTS:")
        validation_stats = results.get("validation_stats", {})
        validation_failures = validation_stats.get("validation_failures", {})
        total_failures = sum(validation_failures.values())
        if total_failures > 0:
            print(f"   → Total validation failures: {total_failures}")
            for failure_type, count in validation_failures.items():
                if count > 0:
                    print(f"      • {failure_type}: {count}")
        else:
            print(f"   → No validation failures! 🎉")
        
        print(f"\n🧹 DATA CLEANING RESULTS:")
        cleaning_stats = results.get("cleaning_stats", {})
        total_cleaned = sum(cleaning_stats.values())
        print(f"   → Total cleaning operations: {total_cleaned}")
        for operation, count in cleaning_stats.items():
            if count > 0:
                print(f"      • {operation}: {count}")
        
        brand_cleaning_stats = results.get("brand_cleaning_stats", {})
        if brand_cleaning_stats.get("brands_processed", 0) > 0:
            print(f"\n🏷️ BRAND NORMALIZATION DETAILS:")
            print(f"   → Brands processed: {brand_cleaning_stats.get('brands_processed', 0)}")
            print(f"   → Brands modified: {brand_cleaning_stats.get('brands_cleaned', 0)} ({brand_cleaning_stats.get('cleaning_rate', 0)}%)")
            print(f"   → Primary brands extracted: {brand_cleaning_stats.get('primary_brand_extracted', 0)}")
            print(f"   → Case/accent normalized: {brand_cleaning_stats.get('case_normalized', 0)}")
            print(f"   → Mapped to canonical: {brand_cleaning_stats.get('mapped_to_canonical', 0)}")

        print(f"\n📊 CALCULATED FIELDS:")
        calculation_stats = results.get("calculation_stats", {})
        for field_type, count in calculation_stats.items():
            if count > 0:
                print(f"      • {field_type}: {count}")
        
        print(f"\n🔍 QUALITY VALIDATION:")
        overall_score = quality_validation.get("overall_quality_score", 0)
        anomalies_count = len(quality_validation.get("anomalies", []))
        print(f"   → Overall quality score: {overall_score:.1f}%")
        print(f"   → Anomalies detected: {anomalies_count}")
        
        print(f"\n🚀 PRODUCTION READINESS:")
        print(f"   → Database-ready products: {production_readiness.get('complete_products_ready_for_db', 0)}")
        print(f"   → Overall success rate: {production_readiness.get('success_rate', 0):.1f}%")
        print(f"   → Data quality grade: {production_readiness.get('data_quality_grade', 'F')}")
        print(f"   → Bot launch ready: {'✅ YES' if production_readiness.get('bot_launch_ready', False) else '⚠️ LIMITED' if production_readiness.get('minimum_viable_dataset', False) else '❌ NO'}")
        
        print(f"\n🎯 NEXT STEPS:")
        next_steps = production_readiness.get("next_steps", [])
        for step in next_steps:
            print(f"   {step}")
        
        print("=" * 60)
    
    def generate_executive_summary(
        self,
        transformation_results: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Generate executive summary data structure (without saving to file)
        
        Args:
            transformation_results: Complete transformation results
            
        Returns:
            Executive summary data structure
        """
        stats = transformation_results.get("transformation_stats", {})
        production = transformation_results.get("production_readiness", {})
        quality = transformation_results.get("quality_validation", {})
        
        return {
            "executive_summary": {
                "transformation_date": stats.get("start_time"),
                "dataset_overview": {
                    "total_products_processed": stats.get("total_products_processed", 0),
                    "successful_transformations": stats.get("successful_transformations", 0),
                    "rejected_products": stats.get("rejected_products", 0),
                    "duplicates_handled": stats.get("duplicate_products", 0),
                    "success_rate": production.get("success_rate", 0)
                },
                "business_impact": {
                    "bot_ready_for_launch": production.get("bot_launch_ready", False),
                    "mvp_viable": production.get("minimum_viable_dataset", False),
                    "production_ready_products": production.get("complete_products_ready_for_db", 0),
                    "data_quality_grade": production.get("data_quality_grade", "F"),
                    "deployment_recommendation": self._get_deployment_recommendation(production)
                },
                "quality_metrics": {
                    "overall_quality_score": quality.get("overall_quality_score", 0),
                    "critical_issues_count": len(transformation_results.get("data_quality_issues", [])),
                    "anomalies_detected": len(quality.get("anomalies", [])),
                    "field_quality_summary": self._summarize_field_quality(quality)
                },
                "next_steps": production.get("next_steps", [])
            }
        }
    
    def _get_deployment_recommendation(self, production: Dict[str, Any]) -> str:
        """Get deployment recommendation based on production readiness"""
        if production.get("bot_launch_ready", False):
            return "PROCEED TO PRODUCTION - Dataset meets all requirements for bot deployment"
        elif production.get("minimum_viable_dataset", False):
            return "PROCEED WITH CAUTION - Dataset meets minimum requirements but needs improvement"
        else:
            return "DO NOT PROCEED - Dataset quality insufficient for production use"
    
    def _summarize_field_quality(self, quality: Dict[str, Any]) -> Dict[str, Any]:
        """Summarize field quality metrics"""
        field_scores = quality.get("field_quality_scores", {})
        summary = {}
        
        for field, score_data in field_scores.items():
            summary[field] = {
                "quality_score": score_data.get("score", 0),
                "valid_count": score_data.get("valid_count", 0),
                "invalid_count": score_data.get("invalid_count", 0)
            }
        
        return summary
    
    def _format_validation_stats(self, results: Dict[str, Any]) -> Dict[str, Any]:
        """Helper to format validation statistics for JSON report"""
        validation_stats = results.get("validation_stats", {})
        return {
            "validation_failures": validation_stats.get("validation_failures", {}),
            "total_validation_failures": sum(validation_stats.get("validation_failures", {}).values())
        }

    def _format_cleaning_stats(self, results: Dict[str, Any]) -> Dict[str, Any]:
        """Helper to format cleaning statistics for JSON report"""
        cleaning_stats = results.get("cleaning_stats", {})
        return {
            "brand_names_cleaned": cleaning_stats.get("brand_names_cleaned", 0),
            "product_names_cleaned": cleaning_stats.get("product_names_cleaned", 0),
            "weights_normalized": cleaning_stats.get("weights_normalized", 0)
        }

    def _format_calculation_stats(self, results: Dict[str, Any]) -> Dict[str, Any]:
        """Helper to format calculation statistics for JSON report"""
        calculation_stats = results.get("calculation_stats", {})
        return {
            "transport_equivalents_calculated": calculation_stats.get("transport_equivalents_calculated", 0),
            "impact_levels_assigned": calculation_stats.get("impact_levels_assigned", 0)
        }

    def _summarize_anomalies(self, anomalies: list) -> Dict[str, Any]:
        """Helper to summarize anomalies for JSON report"""
        summary = {}
        for anomaly in anomalies:
            anomaly_type = anomaly.get("type")
            if anomaly_type not in summary:
                summary[anomaly_type] = 0
            summary[anomaly_type] += 1
        return summary

    def _get_production_status(self, production: Dict[str, Any]) -> str:
        """Helper to get overall production status"""
        if production.get("bot_launch_ready", False):
            return "Fully Ready"
        elif production.get("minimum_viable_dataset", False):
            return "Partially Ready"
        else:
            return "Not Ready"

    def _get_immediate_actions(self, production: Dict[str, Any]) -> list:
        """Helper to get immediate actions for recommendations"""
        actions = []
        if not production.get("bot_launch_ready", False):
            actions.append("Review and improve data quality to meet bot launch requirements.")
        if not production.get("minimum_viable_dataset", False):
            actions.append("Identify and address specific data quality issues to make the dataset viable.")
        return actions

    def _get_short_term_goals(self, production: Dict[str, Any]) -> list:
        """Helper to get short-term goals for recommendations"""
        goals = []
        if not production.get("bot_launch_ready", False):
            goals.append("Achieve a success rate of 90% or higher for bot launch.")
        if not production.get("minimum_viable_dataset", False):
            goals.append("Reduce the number of critical issues to zero.")
        return goals

    def _get_long_term_strategy(self, production: Dict[str, Any]) -> str:
        """Helper to get long-term strategy for recommendations"""
        if production.get("bot_launch_ready", False):
            return "Maintain high data quality and continue monitoring for anomalies."
        else:
            return "Focus on improving data quality and addressing critical issues to make the dataset viable."

    def _build_markdown_content(self, results: Dict[str, Any]) -> str:
        """Helper to build comprehensive Markdown content"""
        markdown = f"""# Transformation Report

## Overview

**Timestamp:** {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
**Total Products Processed:** {results.get("transformation_stats", {}).get("total_products_processed", 0)}
**Successful Transformations:** {results.get("transformation_stats", {}).get("successful_transformations", 0)}
**Rejected Products:** {results.get("transformation_stats", {}).get("rejected_products", 0)}
**Duplicates Handled:** {results.get("transformation_stats", {}).get("duplicate_products", 0)}

## Processing Results

### Transformation Stats
- **Start Time:** {results.get("transformation_stats", {}).get("start_time")}
- **Total Products Processed:** {results.get("transformation_stats", {}).get("total_products_processed", 0)}
- **Successful Transformations:** {results.get("transformation_stats", {}).get("successful_transformations", 0)}
- **Rejected Products:** {results.get("transformation_stats", {}).get("rejected_products", 0)}
- **Duplicates Handled:** {results.get("transformation_stats", {}).get("duplicate_products", 0)}

### Production Readiness
- **Database-ready Products:** {results.get("production_readiness", {}).get("complete_products_ready_for_db", 0)}
- **Overall Success Rate:** {results.get("production_readiness", {}).get("success_rate", 0):.1f}%
- **Data Quality Grade:** {results.get("production_readiness", {}).get("data_quality_grade", "F")}
- **Bot Launch Ready:** {'✅ YES' if results.get("production_readiness", {}).get("bot_launch_ready", False) else '⚠️ LIMITED' if results.get("production_readiness", {}).get("minimum_viable_dataset", False) else '❌ NO'}

## Validation Results

### Overall Quality Score
- **Score:** {results.get("quality_validation", {}).get("overall_quality_score", 0):.1f}%
- **Anomalies Detected:** {len(results.get("quality_validation", {}).get("anomalies", []))}

### Field-specific Quality
{self._format_field_quality_markdown(results.get("quality_validation", {}).get("field_quality_scores", {}))}

### Critical Issues
{self._format_critical_issues_markdown(results.get("data_quality_issues", []))}

## Data Cleaning

### Total Cleaning Operations
- **Brand Names Cleaned:** {results.get("cleaning_stats", {}).get("brand_names_cleaned", 0)}
- **Product Names Cleaned:** {results.get("cleaning_stats", {}).get("product_names_cleaned", 0)}
- **Weights Normalized:** {results.get("cleaning_stats", {}).get("weights_normalized", 0)}

### Brand Normalization Details
{self._format_brand_normalization_markdown(results.get("brand_cleaning_stats", {}))}

## Calculated Fields

### Transport Equivalents
- **Calculated:** {results.get("calculation_stats", {}).get("transport_equivalents_calculated", 0)}
- **Impact Levels Assigned:** {results.get("calculation_stats", {}).get("impact_levels_assigned", 0)}

## Data Quality Issues
{self._format_data_quality_issues_markdown(results.get("data_quality_issues", []))}

## Products Missing CO2
{self._format_products_missing_co2_markdown(results.get("products_missing_co2", []))}

## Next Steps
{self._format_next_steps_markdown(results.get("production_readiness", {}).get("next_steps", []))}

"""
        return markdown

    def _format_field_quality_markdown(self, field_scores: Dict[str, Any]) -> str:
        """Helper to format field quality metrics for Markdown report"""
        markdown = ""
        for field, score_data in field_scores.items():
            markdown += f"- **{field}:**\n  - **Score:** {score_data.get('score', 0):.1f}%\n  - **Valid Count:** {score_data.get('valid_count', 0)}\n  - **Invalid Count:** {score_data.get('invalid_count', 0)}\n"
        return markdown

    def _format_critical_issues_markdown(self, issues: list) -> str:
        """Helper to format critical issues for Markdown report"""
        if not issues:
            return "No critical issues identified."
        markdown = "**Critical Issues:**\n"
        for issue in issues:
            markdown += f"- {issue.get('type')}: {issue.get('message')}\n"
        return markdown

    def _format_brand_normalization_markdown(self, stats: Dict[str, Any]) -> str:
        """Helper to format brand normalization details for Markdown report"""
        if not stats:
            return "No brand normalization details available."
        markdown = "**Brand Normalization Details:**\n"
        markdown += f"- **Brands Processed:** {stats.get('brands_processed', 0)}\n"
        markdown += f"- **Brands Modified:** {stats.get('brands_cleaned', 0)} ({stats.get('cleaning_rate', 0)}%)\n"
        markdown += f"- **Primary Brands Extracted:** {stats.get('primary_brand_extracted', 0)}\n"
        markdown += f"- **Case/Accent Normalized:** {stats.get('case_normalized', 0)}\n"
        markdown += f"- **Mapped to Canonical:** {stats.get('mapped_to_canonical', 0)}\n"
        return markdown

    def _format_data_quality_issues_markdown(self, issues: list) -> str:
        """Helper to format data quality issues for Markdown report"""
        if not issues:
            return "No data quality issues identified."
        markdown = "**Data Quality Issues:**\n"
        for issue in issues:
            markdown += f"- {issue.get('type')}: {issue.get('message')}\n"
        return markdown

    def _format_products_missing_co2_markdown(self, missing_co2: list) -> str:
        """Helper to format products missing CO2 for Markdown report"""
        if not missing_co2:
            return "No products missing CO2 identified."
        markdown = "**Products Missing CO2:**\n"
        for product in missing_co2:
            markdown += f"- {product.get('barcode')}: {product.get('message')}\n"
        return markdown

    def _format_next_steps_markdown(self, next_steps: list) -> str:
        """Helper to format next steps for Markdown report"""
        if not next_steps:
            return "No specific next steps identified."
        markdown = "**Next Steps:**\n"
        for step in next_steps:
            markdown += f"- {step}\n"
        return markdown
    
    def get_reporting_stats(self) -> Dict[str, Any]:
        """Get current reporting statistics"""
        return self.reporting_stats.copy()
    
    def reset_stats(self):
        """Reset reporting statistics"""
        self.reporting_stats = {
            "reports_generated": 0,
            "summaries_displayed": 0,
            "json_reports": 0,
            "markdown_reports": 0
        }


# Convenience function for integration
def generate_transformation_reports(
    transformation_results: Dict[str, Any],
    output_dir: Optional[Path] = None
    ) -> Dict[str, Path]:
    """
    Convenience function to generate all transformation reports
    
    Args:
        transformation_results: Results from transformation pipeline
        output_dir: Optional output directory for reports
        
    Returns:
        Dict mapping report type to saved file path
    """
    reporter = TransformationReporter(output_dir)
    return reporter.generate_all_reports(transformation_results)


# Test function for standalone execution
def test_transformation_reporter():
    """Test the transformation reporter with mock data"""
    print("🧪 TESTING TRANSFORMATION REPORTER")
    
    # Mock transformation results
    mock_results = {
        "transformation_stats": {
            "start_time": "2024-01-15T10:00:00",
            "total_products_processed": 150,
            "successful_transformations": 135,
            "rejected_products": 15,
            "duplicate_products": 5
        },
        "production_readiness": {
            "complete_products_ready_for_db": 120,
            "success_rate": 80.0,
            "data_quality_grade": "B",
            "bot_launch_ready": True,
            "minimum_viable_dataset": True,
            "next_steps": [
                "Ready for production deployment",
                "Load validated products into database"
            ]
        },
        "quality_validation": {
            "overall_quality_score": 85.5,
            "field_quality_scores": {
                "barcode": {"score": 100.0, "valid_count": 150, "invalid_count": 0},
                "product_name": {"score": 90.0, "valid_count": 135, "invalid_count": 15}
            },
            "anomalies": []
        },
        "validation_stats": {
            "validation_failures": {
                "missing_co2": 10,
                "missing_nutriscore": 5
            }
        },
        "cleaning_stats": {
            "brand_names_cleaned": 25,
            "product_names_cleaned": 15,
            "weights_normalized": 30
        },
        "calculation_stats": {
            "transport_equivalents_calculated": 135,
            "impact_levels_assigned": 135
        },
        "data_quality_issues": [],
        "products_missing_co2": []
    }
    
    # Test report generation
    reporter = TransformationReporter()
    saved_reports = reporter.generate_all_reports(mock_results)
    
    print(f"\n✅ Test completed:")
    print(f"   → Reports generated: {len(saved_reports)}")
    print(f"   → Stats: {reporter.get_reporting_stats()}")


if __name__ == "__main__":
    test_transformation_reporter()
