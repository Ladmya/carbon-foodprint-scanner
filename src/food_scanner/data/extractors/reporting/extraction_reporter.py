"""
src/food_scanner/data/extractors/reporting/extraction_reporter.py
EXTRACTION REPORTING: Generate JSON and Markdownreports with timestamps for extraction analysis
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional

from food_scanner.data.extractors.analysis import ExtractionAnalysisResults


class ExtractionReporter:
    """
    PURE REPORTING ENGINE: Generates reports from analysis results
    
    RESPONSIBILITIES:
    1. Format analysis results into different output formats
    2. Generate JSON, Markdown, CSV reports
    3. Save reports to files with proper organization
    4. NO analysis logic - only presentation and formatting
    
    USAGE:
        reporter = ExtractionReporter()
        report_files = reporter.generate_all_reports(analysis_results)
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
            "csv_reports": self.output_base_dir / "csv",
            "summary_reports": self.output_base_dir / "summaries"
        }
        
        # Create directories
        for report_dir in self.reports_dirs.values():
            report_dir.mkdir(parents=True, exist_ok=True)
    
    def generate_all_reports(
        self,
        analysis_results: ExtractionAnalysisResults
    ) -> Dict[str, Path]:
        """
        Generate all report formats from analysis results
        
        Args:
            analysis_results: Results from ExtractionAnalyzer
            
        Returns:
            Dict mapping report type to saved file path
        """
        print(f"\n📊 GENERATING EXTRACTION REPORTS")
        print(f"   → Timestamp: {self.timestamp}")
        print(f"   → Source: {analysis_results.total_products} analyzed products")
        print("-" * 60)
        
        saved_reports = {}
        
        try:
            # 1. Comprehensive JSON report
            json_report = self.generate_json_report(analysis_results)
            saved_reports["comprehensive_json"] = json_report
            
            # 2. Executive summary (JSON)
            executive_json = self.generate_executive_summary_json(analysis_results)
            saved_reports["executive_summary_json"] = executive_json
            
            # 3. Markdown quality report
            markdown_report = self.generate_markdown_report(analysis_results)
            saved_reports["quality_summary_md"] = markdown_report
            
            # 4. Field-specific reports
            field_reports = self.generate_field_specific_reports(analysis_results)
            saved_reports.update(field_reports)
            
            # 5. Production readiness report
            production_report = self.generate_production_readiness_report(analysis_results)
            saved_reports["production_readiness"] = production_report
            
            print(f"\n✅ All extraction reports generated:")
            for report_type, file_path in saved_reports.items():
                print(f"   → {report_type}: {file_path.name}")
            
            return saved_reports
            
        except Exception as e:
            print(f"❌ Report generation failed: {e}")
            return {"error": str(e)}
    
    def generate_json_report(self, analysis_results: ExtractionAnalysisResults) -> Path:
        """
        Generate comprehensive JSON report
        Format: extraction_analysis_comprehensive_YYYYMMDD_HHMMSS.json
        """
        print(f"   📄 Generating comprehensive JSON report...")
        
        # Format analysis results for JSON
        json_data = {
            "report_metadata": {
                "report_type": "comprehensive_extraction_analysis",
                "generation_timestamp": datetime.now().isoformat(),
                "analysis_timestamp": analysis_results.analysis_timestamp,
                "total_products_analyzed": analysis_results.total_products,
                "report_version": "3.0_pure_reporting"
            },
            "overall_metrics": analysis_results.overall_metrics,
            "production_readiness": analysis_results.production_readiness,
            "field_analyses": self._format_field_analyses_for_json(analysis_results.field_analyses),
            "critical_issues": analysis_results.critical_issues,
            "improvement_priorities": analysis_results.improvement_priorities,
            "quality_insights": analysis_results.quality_insights,
            "comprehensive_analysis": self._format_comprehensive_analysis_for_json(
                analysis_results.comprehensive_analysis
            )
        }
        
        # Save JSON report
        filename = f"extraction_analysis_comprehensive_{self.timestamp}.json"
        filepath = self.reports_dirs["json_reports"] / filename
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(json_data, f, indent=2, ensure_ascii=False, default=str)
        
        print(f"      ✅ Comprehensive JSON report saved: {filename}")
        return filepath
    
    def generate_executive_summary_json(self, analysis_results: ExtractionAnalysisResults) -> Path:
        """
        Generate executive summary report
        Format: executive_summary_YYYYMMDD_HHMMSS.json
        """
        print(f"   📋 Generating executive summary...")
        
        overall = analysis_results.overall_metrics
        production = analysis_results.production_readiness
        
        # Executive-focused data
        executive_data = {
            "executive_summary": {
                "extraction_date": analysis_results.analysis_timestamp,
                "dataset_overview": {
                    "total_products_processed": analysis_results.total_products,
                    "overall_quality_score": overall.get("overall_quality_score", 0),
                    "extraction_success_rate": overall.get("extraction_success_rate", 0),
                    "production_ready_products": production.get("ready_products", 0),
                    "production_readiness_rate": production.get("readiness_rate", 0)
                },
                "business_impact": {
                    "bot_ready_for_launch": production.get("bot_ready", False),
                    "mvp_viable": production.get("mvp_ready", False),
                    "estimated_bot_capacity": production.get("ready_products", 0),
                    "data_quality_grade": self._calculate_quality_grade(overall.get("overall_quality_score", 0)),
                    "deployment_recommendation": self._get_deployment_recommendation(production)
                },
                "key_findings": {
                    "critical_issues_count": len(analysis_results.critical_issues),
                    "top_critical_issue": analysis_results.critical_issues[0] if analysis_results.critical_issues else None,
                    "top_improvement_priority": analysis_results.improvement_priorities[0] if analysis_results.improvement_priorities else None,
                    "field_performance_summary": self._summarize_field_performance(analysis_results.field_analyses)
                },
                "next_steps": self._generate_executive_next_steps(analysis_results)
            },
            "detailed_reports_available": {
                "comprehensive_analysis": f"extraction_analysis_comprehensive_{self.timestamp}.json",
                "quality_summary": f"quality_summary_{self.timestamp}.md",
                "field_specific_reports": "Available in field_reports/ directory"
            }
        }
        
        # Save executive summary
        filename = f"executive_summary_{self.timestamp}.json"
        filepath = self.reports_dirs["summary_reports"] / filename
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(executive_data, f, indent=2, ensure_ascii=False, default=str)
        
        readiness_rate = production.get("readiness_rate", 0)
        print(f"      ✅ Executive summary: {readiness_rate:.1f}% production ready")
        return filepath
    
    def generate_markdown_report(self, analysis_results: ExtractionAnalysisResults) -> Path:
        """
        Generate comprehensive Markdown quality report
        Format: quality_summary_YYYYMMDD_HHMMSS.md
        """
        print(f"   📝 Generating Markdown quality report...")
        
        overall = analysis_results.overall_metrics
        production = analysis_results.production_readiness
        
        # Build Markdown content
        markdown_content = self._build_markdown_content(analysis_results)
        
        # Save Markdown report
        filename = f"quality_summary_{self.timestamp}.md"
        filepath = self.reports_dirs["markdown_reports"] / filename
        
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(markdown_content)
        
        quality_score = overall.get("overall_quality_score", 0)
        grade = self._calculate_quality_grade(quality_score)
        print(f"      ✅ Markdown report: {quality_score:.1f}/100 (Grade: {grade})")
        return filepath
    
    def generate_field_specific_reports(self, analysis_results: ExtractionAnalysisResults) -> Dict[str, Path]:
        """Generate individual reports for each field"""
        
        print(f"   🔍 Generating field-specific reports...")
        field_reports = {}
        
        for field_name, field_analysis in analysis_results.field_analyses.items():
            if field_name == "error":  # Skip error entries
                continue
                
            try:
                # Generate field-specific JSON
                field_data = {
                    "field_name": field_name,
                    "analysis_timestamp": analysis_results.analysis_timestamp,
                    "field_analysis": self._format_single_field_analysis(field_analysis),
                    "recommendations": self._extract_field_recommendations(field_analysis),
                    "examples": self._extract_field_examples(field_analysis)
                }
                
                filename = f"field_analysis_{field_name}_{self.timestamp}.json"
                filepath = self.reports_dirs["json_reports"] / filename
                
                with open(filepath, 'w', encoding='utf-8') as f:
                    json.dump(field_data, f, indent=2, ensure_ascii=False, default=str)
                
                field_reports[f"field_{field_name}"] = filepath
                
            except Exception as e:
                print(f"      ⚠️ Failed to generate report for {field_name}: {e}")
        
        print(f"      ✅ Generated {len(field_reports)} field-specific reports")
        return field_reports
    
    def generate_production_readiness_report(self, analysis_results: ExtractionAnalysisResults) -> Path:
        """Generate production readiness focused report"""
        
        print(f"   🚀 Generating production readiness report...")
        
        production = analysis_results.production_readiness
        
        readiness_data = {
            "production_readiness_assessment": {
                "timestamp": analysis_results.analysis_timestamp,
                "overall_assessment": {
                    "total_products": production.get("total_products", 0),
                    "ready_products": production.get("ready_products", 0),
                    "readiness_rate": production.get("readiness_rate", 0),
                    "deployment_status": production.get("production_thresholds", {}).get("current_status", "UNKNOWN")
                },
                "deployment_gates": {
                    "bot_ready": production.get("bot_ready", False),
                    "mvp_ready": production.get("mvp_ready", False),
                    "needs_improvement": production.get("needs_improvement", True),
                    "blocking_issues": [issue for issue in analysis_results.critical_issues if "CRITICAL" in issue]
                },
                "rejection_analysis": {
                    "rejected_products": production.get("rejected_products", 0),
                    "rejection_reasons": production.get("rejection_reasons", {}),
                    "top_rejection_reasons": list(production.get("rejection_reasons", {}).items())[:5]
                },
                "improvement_roadmap": {
                    "critical_fixes": [p for p in analysis_results.improvement_priorities if "🔥" in p],
                    "high_priority": [p for p in analysis_results.improvement_priorities if "⚡" in p],
                    "medium_priority": [p for p in analysis_results.improvement_priorities if "📈" in p]
                },
                "launch_recommendations": self._generate_launch_recommendations(analysis_results)
            }
        }
        
        filename = f"production_readiness_{self.timestamp}.json"
        filepath = self.reports_dirs["summary_reports"] / filename
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(readiness_data, f, indent=2, ensure_ascii=False, default=str)
        
        ready_count = production.get("ready_products", 0)
        print(f"      ✅ Production readiness: {ready_count} products ready")
        return filepath
    
    def _format_field_analyses_for_json(self, field_analyses: Dict[str, Any]) -> Dict[str, Any]:
        """Format field analyses for JSON serialization"""
        
        formatted_analyses = {}
        
        for field_name, analysis in field_analyses.items():
            if hasattr(analysis, '__dict__'):
                # Convert analysis object to dict
                formatted_analyses[field_name] = {
                    "field_name": getattr(analysis, 'field_name', field_name),
                    "total_products": getattr(analysis, 'total_products', 0),
                    "valid_count": getattr(analysis, 'valid_count', 0),
                    "missing_count": getattr(analysis, 'missing_count', 0),
                    "validity_rate": getattr(analysis, 'validity_rate', 0),
                    "quality_score": getattr(analysis, 'quality_score', 0),
                    "pattern_analysis": getattr(analysis, 'pattern_analysis', {}),
                    "recommendations": getattr(analysis, 'transformation_recommendations', [])
                }
            else:
                # Handle non-object analysis results
                formatted_analyses[field_name] = analysis
        
        return formatted_analyses
    
    def _format_comprehensive_analysis_for_json(self, comprehensive_analysis: Any) -> Optional[Dict[str, Any]]:
        """Format comprehensive analysis for JSON serialization"""
        
        if not comprehensive_analysis:
            return None
        
        try:
            if hasattr(comprehensive_analysis, '__dict__'):
                return {
                    "analysis_timestamp": getattr(comprehensive_analysis, 'analysis_timestamp', ''),
                    "overall_quality_score": getattr(comprehensive_analysis, 'overall_quality_score', 0),
                    "total_products_analyzed": getattr(comprehensive_analysis, 'total_products_analyzed', 0),
                    "rejection_analysis": getattr(comprehensive_analysis, 'rejection_analysis', {}),
                    "critical_issues": getattr(comprehensive_analysis, 'critical_issues', []),
                    "improvement_priorities": getattr(comprehensive_analysis, 'improvement_priorities', [])
                }
            else:
                return comprehensive_analysis
        except Exception as e:
            return {"error": f"Failed to format comprehensive analysis: {e}"}
    
    def _build_markdown_content(self, analysis_results: ExtractionAnalysisResults) -> str:
        """Build comprehensive Markdown report content"""
        
        overall = analysis_results.overall_metrics
        production = analysis_results.production_readiness
        
        markdown = f"""# Extraction Quality Report
        
## Executive Summary

**Report Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  
**Analysis Timestamp:** {analysis_results.analysis_timestamp}  
**Products Analyzed:** {analysis_results.total_products}

### 🎯 Overall Performance
- **Quality Score:** {overall.get('overall_quality_score', 0):.1f}/100
- **Extraction Success:** {overall.get('extraction_success_rate', 0):.1f}%
- **Production Ready:** {production.get('ready_products', 0)} products ({production.get('readiness_rate', 0):.1f}%)

### 🚀 Production Readiness
**Status:** {production.get('production_thresholds', {}).get('current_status', 'UNKNOWN')}

- ✅ **Bot Ready:** {'Yes' if production.get('bot_ready', False) else 'No'}
- ⚠️ **MVP Ready:** {'Yes' if production.get('mvp_ready', False) else 'No'}
- 🔧 **Needs Improvement:** {'Yes' if production.get('needs_improvement', True) else 'No'}

## 📊 Field Performance Analysis

"""
        
        # Add field-by-field analysis
        for field_name, analysis in analysis_results.field_analyses.items():
            if field_name == "error":
                continue
                
            if hasattr(analysis, 'validity_rate'):
                status = "✅" if analysis.validity_rate >= 80 else "⚠️" if analysis.validity_rate >= 60 else "❌"
                markdown += f"### {status} {field_name.replace('_', ' ').title()}\n"
                markdown += f"- **Success Rate:** {analysis.validity_rate:.1f}%\n"
                markdown += f"- **Valid Count:** {analysis.valid_count}/{analysis.total_products}\n"
                markdown += f"- **Quality Score:** {analysis.quality_score:.1f}\n\n"
        
        # Add critical issues
        if analysis_results.critical_issues:
            markdown += "## 🚨 Critical Issues\n\n"
            for i, issue in enumerate(analysis_results.critical_issues, 1):
                markdown += f"{i}. {issue}\n"
            markdown += "\n"
        
        # Add improvement priorities
        if analysis_results.improvement_priorities:
            markdown += "## 📈 Improvement Priorities\n\n"
            for i, priority in enumerate(analysis_results.improvement_priorities, 1):
                markdown += f"{i}. {priority}\n"
            markdown += "\n"
        
        # Add recommendations
        markdown += "## 🎯 Recommendations\n\n"
        recommendations = self._generate_markdown_recommendations(analysis_results)
        for rec in recommendations:
            markdown += f"- {rec}\n"
        
        markdown += f"\n---\n*Report generated by ExtractionReporter v3.0*"
        
        return markdown
    
    def _calculate_quality_grade(self, quality_score: float) -> str:
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
    
    def _get_deployment_recommendation(self, production: Dict[str, Any]) -> str:
        """Get deployment recommendation based on production readiness"""
        
        if production.get("bot_ready", False):
            return "DEPLOY_TO_PRODUCTION"
        elif production.get("mvp_ready", False):
            return "DEPLOY_MVP_LIMITED_DATASET"
        else:
            return "IMPROVE_BEFORE_DEPLOYMENT"
    
    def _summarize_field_performance(self, field_analyses: Dict[str, Any]) -> Dict[str, Any]:
        """Summarize field performance for executive summary"""
        
        performance_summary = {
            "total_fields_analyzed": 0,
            "excellent_fields": 0,  # >90%
            "good_fields": 0,       # 80-90%
            "acceptable_fields": 0, # 60-80%
            "poor_fields": 0,       # <60%
            "critical_field_issues": []
        }
        
        critical_fields = ["barcode", "product_name", "brand_name", "co2_total"]
        
        for field_name, analysis in field_analyses.items():
            if field_name == "error" or not hasattr(analysis, 'validity_rate'):
                continue
                
            performance_summary["total_fields_analyzed"] += 1
            rate = analysis.validity_rate
            
            if rate >= 90:
                performance_summary["excellent_fields"] += 1
            elif rate >= 80:
                performance_summary["good_fields"] += 1
            elif rate >= 60:
                performance_summary["acceptable_fields"] += 1
            else:
                performance_summary["poor_fields"] += 1
                
                # Flag critical field issues
                if field_name in critical_fields:
                    performance_summary["critical_field_issues"].append({
                        "field": field_name,
                        "rate": rate,
                        "impact": "BLOCKS_BOT_FUNCTIONALITY"
                    })
        
        return performance_summary
    
    def _generate_executive_next_steps(self, analysis_results: ExtractionAnalysisResults) -> List[str]:
        """Generate executive-level next steps"""
        
        steps = []
        production = analysis_results.production_readiness
        
        if production.get("bot_ready", False):
            steps.extend([
                "✅ Approve production deployment",
                "📈 Scale data collection to 1000+ products", 
                "🔄 Set up automated quality monitoring"
            ])
        elif production.get("mvp_ready", False):
            steps.extend([
                "⚡ Fix critical issues identified in analysis",
                "🧪 Launch MVP with current dataset",
                "📊 Monitor performance and gather user feedback"
            ])
        else:
            steps.extend([
                "🚨 Address blocking issues before deployment",
                "🔧 Focus on critical field extraction improvements",
                "📋 Re-run analysis after improvements"
            ])
        
        return steps
    
    def _generate_launch_recommendations(self, analysis_results: ExtractionAnalysisResults) -> List[str]:
        """Generate launch-specific recommendations"""
        
        recommendations = []
        production = analysis_results.production_readiness
        
        ready_products = production.get("ready_products", 0)
        readiness_rate = production.get("readiness_rate", 0)
        
        if ready_products >= 100 and readiness_rate >= 70:
            recommendations.extend([
                "🚀 RECOMMENDED: Full production launch",
                "📱 Deploy bot with complete dataset",
                "📊 Monitor user engagement metrics"
            ])
        elif ready_products >= 50 and readiness_rate >= 50:
            recommendations.extend([
                "⚠️ CONDITIONAL: MVP launch possible",
                "🔧 Fix critical issues in parallel",
                "📋 Limited user testing recommended"
            ])
        else:
            recommendations.extend([
                "❌ NOT RECOMMENDED: Delay launch",
                "🚨 Address fundamental extraction issues",
                "📈 Target 70%+ readiness rate before launch"
            ])
        
        return recommendations
    
    def _generate_markdown_recommendations(self, analysis_results: ExtractionAnalysisResults) -> List[str]:
        """Generate recommendations for Markdown report"""
        
        recommendations = []
        overall = analysis_results.overall_metrics
        production = analysis_results.production_readiness
        
        # Quality-based recommendations
        quality_score = overall.get("overall_quality_score", 0)
        if quality_score >= 85:
            recommendations.append("✅ Data quality is excellent - ready for production")
        elif quality_score >= 70:
            recommendations.append("⚠️ Data quality is acceptable - monitor closely in production")
        else:
            recommendations.append("❌ Data quality needs improvement before production")
        
        # Production readiness recommendations
        if production.get("bot_ready", False):
            recommendations.append("🚀 Bot is ready for full production deployment")
        elif production.get("mvp_ready", False):
            recommendations.append("📱 MVP deployment possible with limited dataset")
        else:
            recommendations.append("🔧 Major improvements needed before any deployment")
        
        # Field-specific recommendations
        for field_name, analysis in analysis_results.field_analyses.items():
            if hasattr(analysis, 'validity_rate') and analysis.validity_rate < 70:
                recommendations.append(f"🔧 Priority: Improve {field_name} extraction ({analysis.validity_rate:.1f}% success)")
        
        return recommendations
    
    def _format_single_field_analysis(self, field_analysis: Any) -> Dict[str, Any]:
        """Format single field analysis for JSON"""
        
        if hasattr(field_analysis, '__dict__'):
            return {
                "validity_rate": getattr(field_analysis, 'validity_rate', 0),
                "quality_score": getattr(field_analysis, 'quality_score', 0),
                "total_products": getattr(field_analysis, 'total_products', 0),
                "valid_count": getattr(field_analysis, 'valid_count', 0),
                "missing_count": getattr(field_analysis, 'missing_count', 0),
                "pattern_analysis": getattr(field_analysis, 'pattern_analysis', {})
            }
        else:
            return {"analysis": str(field_analysis)}
    
    def _extract_field_recommendations(self, field_analysis: Any) -> List[str]:
        """Extract recommendations from field analysis"""
        
        if hasattr(field_analysis, 'transformation_recommendations'):
            return field_analysis.transformation_recommendations
        return []
    
    def _extract_field_examples(self, field_analysis: Any) -> Dict[str, Any]:
        """Extract examples from field analysis"""
        
        if hasattr(field_analysis, 'examples'):
            return field_analysis.examples
        return {}


# Convenience function for quick reporting
def generate_extraction_reports(
    analysis_results: ExtractionAnalysisResults,
    output_dir: Optional[Path] = None
) -> Dict[str, Path]:
    """
    Convenience function for report generation
    
    Usage:
        reports = generate_extraction_reports(analysis_results)
    """
    reporter = ExtractionReporter(output_dir)
    return reporter.generate_all_reports(analysis_results)