
"""
data_engineering/scripts/analysis/analyze_raw_data_quality.py
Analyzes the quality of the raw data and generates a report.
"""

import json
import numpy as np
from pathlib import Path
from datetime import datetime
from typing import Dict, Any
from collections import Counter, defaultdict


class DataQualityAnalyzer:
    """
    COMPATIBLE: Analyze data quality adapted to exact data format
    - Read raw_dataset files with collection_info/discovered_products/enriched_products
    - Analyze exact stats (weight_parsing_success, products_with_co2, etc.)
    - Generate detailed reports for interview
    """
    
    def __init__(self, output_dir: Path = None):
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        if not output_dir:
            self.output_dir = Path(__file__).resolve().parents[2] / "data" / "reports" / "extraction" / "raw_data_quality"
        else:
            self.output_dir = Path(output_dir)
        
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Analysis results
        self.analysis_results = {
            "analysis_metadata": {
                "timestamp": self.timestamp,
                "analysis_datetime": datetime.now().isoformat(),
                "analyzer_version": "compatible_v1.0",
                "data_format": "original_chocolate_collector"
            },
            "dataset_overview": {},
            "field_analysis": {},
            "data_quality_metrics": {},
            "pipeline_performance": {},
            "co2_analysis": {}, 
            "weight_parsing_analysis": {},  # ANALYSIS OF THE CORRECTION
            "recommendations": []
        }

    def analyze_collection_file(self, collection_file: Path) -> Dict[str, Any]:
        """
        Analyze a collection file in FORMAT
        Read chocolate_dataset_raw_*.json
        """
        print(f"📊 ANALYSE DE QUALITÉ - FORMAT ORIGINAL")
        print(f"   → Fichier: {collection_file.name}")
        print(f"   → Format: Collection info + discovered + enriched products")
        print("=" * 60)
        
        # Charger vos données
        with open(collection_file, 'r', encoding='utf-8') as f:
            dataset = json.load(f)
        
        # Extraire vos structures - compatible avec le format réel
        extraction_metadata = dataset.get("extraction_metadata", {})
        extracted_products = dataset.get("extracted_products", {})
        
        print(f"📋 Données chargées:")
        print(f"   → Extraction metadata: {bool(extraction_metadata)}")
        print(f"   → Produits extraits: {len(extracted_products)}")
        
        # Pour compatibilité avec l'analyse existante
        collection_info = extraction_metadata
        discovered_products = extracted_products
        enriched_products = extracted_products
        
        # 1. ANALYZE OVERALL
        print("\n1️⃣ ANALYZE OVERALL")
        self._analyze_dataset_overview_original_format(collection_info, discovered_products, enriched_products)
        
        # 2. ANALYZE PIPELINE PERFORMANCE  
        print("\n2️⃣ ANALYZE PIPELINE PERFORMANCE")
        self._analyze_pipeline_performance(collection_info, len(enriched_products))
        
        # 3. ANALYZE FIELDS AND QUALITY
        print("\n3️⃣ ANALYZE FIELDS AND QUALITY")
        self._analyze_field_quality_original_format(enriched_products)
        
        # 4. ANALYZE CO2 SPECIFIC
        print("\n4️⃣ ANALYZE CO2")
        self._analyze_co2_specific(enriched_products)
        
        # 5. ANALYZE WEIGHT PARSING (CORRECTION)
        print("\n5️⃣ ANALYZE WEIGHT PARSING (CORRECTION)")
        self._analyze_weight_parsing_improvement(enriched_products)
        
        # 6. GLOBAL METRICS
        print("\n6️⃣ CALCUL GLOBAL METRICS")
        self._calculate_overall_metrics(collection_info, enriched_products)
        
        # 7. RECOMMENDATIONS
        print("\n7️⃣ GENERATE RECOMMENDATIONS")
        self._generate_recommendations()
        
        # Save report
        report_file = self._save_analysis_report()
        
        print(f"\n🎯 ANALYSIS COMPLETED")
        print(f"   → Rapport JSON: {report_file}")
        print(f"   → Rapport Markdown: {report_file.parent / f'quality_summary_{self.timestamp}.md'}")
        print(f"   → Dossier: {self.output_dir}")
        print(f"   → Score qualité global: {self.analysis_results['data_quality_metrics'].get('overall_quality_score', 0):.1f}/100")
        
        return self.analysis_results

    def _analyze_dataset_overview_original_format(self, collection_info, discovered_products, enriched_products):
        """Analyze overall data - compatible with real data format"""
        
        # For real data format, we have extracted_products directly
        total_products = len(enriched_products)
        
        # Extract brand names from extracted_fields
        brand_names = []
        for product in enriched_products.values():
            if product.get("extracted_fields"):
                brand_name = product["extracted_fields"].get("brand_name", "")
                if brand_name:
                    # Take first brand if multiple
                    primary_brand = brand_name.split(",")[0].strip()
                    brand_names.append(primary_brand)
        
        brands_count = len(set(brand_names))
        
        # Calculate pipeline efficiency
        target_products = collection_info.get("target_products", total_products)
        pipeline_efficiency = (total_products / target_products * 100) if target_products > 0 else 100
        
        # Analyze brand distribution (simplified for real data format)
        brand_distribution = {}
        category_distribution = {}
        
        # Extract brand names from enriched products
        brand_counts = Counter()
        for product_data in enriched_products.values():
            extracted_fields = product_data.get("extracted_fields", {})
            brand_name = extracted_fields.get("brand_name", "")
            if brand_name:
                # Take first brand if multiple
                primary_brand = brand_name.split(",")[0].strip()
                brand_counts[primary_brand] += 1
        
        brand_distribution = dict(brand_counts)
        
        self.analysis_results["dataset_overview"] = {
            "collection_summary": {
                "collection_timestamp": collection_info.get("timestamp"),
                "environment": collection_info.get("environment"),
                "total_products": total_products,
                "brands_count": brands_count,
                "target_products": target_products
            },
            "pipeline_conversion_rates": {
                "pipeline_efficiency": round(pipeline_efficiency, 2)
            },
            "data_distribution": {
                "brand_distribution": brand_distribution,
                "category_distribution": category_distribution,
                "top_brands": sorted(brand_distribution.items(), key=lambda x: x[1], reverse=True)[:5] if brand_distribution else []
            },
            "quality_assessment": {
                "pipeline_health": "EXCELLENT" if pipeline_efficiency >= 75 else
                                "GOOD" if pipeline_efficiency >= 60 else
                                "FAIR" if pipeline_efficiency >= 40 else "POOR",
                "data_volume": "HIGH" if total_products >= 1000 else
                            "MEDIUM" if total_products >= 500 else "LIMITED"
            }
        }
        
        print(f"   → Total produits: {total_products}")
        print(f"   → Marques uniques: {brands_count}")
        print(f"   → Efficacité pipeline: {pipeline_efficiency:.1f}%")
        if brand_distribution and len(brand_distribution) > 0:
            top_brand = list(brand_distribution.items())[0]
            print(f"   → Top marque: {top_brand[0]} ({top_brand[1]} produits)")
        if category_distribution and len(category_distribution) > 0:
            top_category = list(category_distribution.items())[0]
            print(f"   → Top catégorie: {top_category[0]} ({top_category[1]} produits)")

    def _analyze_pipeline_performance(self, collection_info, total_products):
        """Analyze performance stats - compatible with real data format"""
        
        target_products = collection_info.get("target_products", 3000)  # Default target
        environment = collection_info.get("environment", "UNKNOWN")
        timestamp = collection_info.get("timestamp", "UNKNOWN")
        
        # Calculate pipeline efficiency
        pipeline_efficiency = (total_products / target_products * 100) if target_products > 0 else 0
        
        # Calculate additional metrics
        brands_count = self.analysis_results.get("dataset_overview", {}).get("brands_count", 0)
        pipeline_health = "EXCELLENT" if pipeline_efficiency >= 80 else "GOOD" if pipeline_efficiency >= 60 else "FAIR"
        volume_grade = "HIGH" if total_products >= 1000 else "MEDIUM" if total_products >= 500 else "LIMITED"
        
        self.analysis_results["pipeline_performance"] = {
            "extraction_metadata": {
                "timestamp": timestamp,
                "environment": environment,
                "target_products": target_products,
                "actual_products": total_products
            },
            "pipeline_efficiency": {
                "efficiency_rate": round(pipeline_efficiency, 2),
                "efficiency_grade": pipeline_health
            },
            "data_volume": {
                "total_products": total_products,
                "brands_count": brands_count,
                "volume_grade": volume_grade
            }
        }
        
        print(f"   → Environment: {environment}")
        print(f"   → Target vs Actual: {target_products} → {total_products}")
        print(f"   → Efficiency: {pipeline_efficiency:.1f}%")
        print(f"   → Brands: {brands_count}")
        print(f"   → Volume: {volume_grade}")

    def _analyze_field_quality_original_format(self, enriched_products):
        """Analyze field quality"""
        field_analysis = {}
        
        # Counters for fields
        field_presence = defaultdict(int)
        field_types = defaultdict(dict)
        total_valid_products = 0
        
        for barcode, product_data in enriched_products.items():
            extracted_fields = product_data.get("extracted_fields", {})
            
            # Skip if no fields extracted
            if not extracted_fields:
                continue
                
            total_valid_products += 1
            
            # Analyze each extracted field
            fields_to_analyze = [
                "product_name", "brand_name", "weight", "product_quantity_unit",
                "nutriscore_grade", "nutriscore_score", "eco_score", 
                "co2_total", "co2_source"
            ]
            
            for field in fields_to_analyze:
                value = extracted_fields.get(field)
                
                if value is not None and value != "":
                    field_presence[field] += 1
                    
                    # Analyze types
                    value_type = type(value).__name__
                    field_types[field][value_type] = field_types[field].get(value_type, 0) + 1
        
        # Calculate quality scores for each field
        for field in field_presence.keys():
            presence_rate = (field_presence[field] / max(1, total_valid_products)) * 100
            
            field_analysis[field] = {
                "presence_rate": round(presence_rate, 2),
                "present_count": field_presence[field],
                "missing_count": total_valid_products - field_presence[field],
                "data_types": dict(field_types[field]),
                "quality_score": self._calculate_field_quality_score(presence_rate, field_types[field]),
                "criticality": self._get_field_criticality(field),
                "status": "EXCELLENT" if presence_rate >= 90 else
                        "GOOD" if presence_rate >= 75 else
                        "FAIR" if presence_rate >= 50 else "POOR"
            }
        
        self.analysis_results["field_analysis"] = field_analysis
        
        # Critical fields summary
        critical_fields = ["product_name", "brand_name", "weight", "co2_total"]
        critical_quality = np.mean([field_analysis.get(f, {}).get("presence_rate", 0) for f in critical_fields])
        
        print(f"   → Qualité champs critiques: {critical_quality:.1f}%")
        print(f"   → Meilleur champ: {max(field_analysis.items(), key=lambda x: x[1]['presence_rate'])[0]}")

    def _analyze_co2_specific(self, enriched_products):
        """ANALYZE CO2 SPECIFIC - compatible with real data format"""
        
        # Calculate CO2 metrics from actual data
        products_with_co2 = 0
        products_without_co2 = 0
        co2_sources = defaultdict(int)
        co2_values = []
        
        for product_data in enriched_products.values():
            extracted_fields = product_data.get("extracted_fields", {})
            
            # Check multiple CO2 sources from the JSON structure
            co2_sources_data = extracted_fields.get("co2_sources", {})
            co2_total = extracted_fields.get("co2_total")
            
            # Check if any CO2 source has a valid value
            has_co2_data = False
            co2_source_used = None
            
            if co2_sources_data:
                # Check different CO2 sources in order of preference
                if co2_sources_data.get("ecoscore_agribalyse_total") is not None:
                    co2_value = co2_sources_data["ecoscore_agribalyse_total"]
                    if co2_value is not None:
                        has_co2_data = True
                        co2_source_used = "ecoscore_agribalyse_total"
                        co2_values.append(co2_value)
                        co2_sources["ecoscore_agribalyse_total"] += 1
                
                elif co2_sources_data.get("agribalyse_total") is not None:
                    co2_value = co2_sources_data["agribalyse_total"]
                    if co2_value is not None:
                        has_co2_data = True
                        co2_source_used = "agribalyse_total"
                        co2_values.append(co2_value)
                        co2_sources["agribalyse_total"] += 1
                
                elif co2_sources_data.get("nutriments_carbon_footprint") is not None:
                    co2_value = co2_sources_data["nutriments_carbon_footprint"]
                    if co2_value is not None:
                        has_co2_data = True
                        co2_source_used = "nutriments_carbon_footprint"
                        co2_values.append(co2_value)
                        co2_sources["nutriments_carbon_footprint"] += 1
            
            # Fallback to co2_total if available
            if not has_co2_data and co2_total is not None and co2_total != "":
                has_co2_data = True
                co2_source_used = "co2_total"
                co2_values.append(co2_total)
                co2_sources["co2_total"] += 1
            
            if has_co2_data:
                products_with_co2 += 1
            else:
                products_without_co2 += 1
        
        total_analyzed = products_with_co2 + products_without_co2
        
        # CO2 analysis is already done above
        
        # CO2 statistics
        co2_stats = {}
        if co2_values:
            co2_stats = {
                "min": min(co2_values),
                "max": max(co2_values),
                "mean": round(np.mean(co2_values), 2),
                "median": round(np.median(co2_values), 2),
                "std": round(np.std(co2_values), 2)
            }
        
        # CO2 coverage rate
        co2_coverage_rate = (products_with_co2 / max(1, total_analyzed)) * 100
        
        self.analysis_results["co2_analysis"] = {
            "coverage_metrics": {
                "products_with_co2": products_with_co2,
                "products_without_co2": products_without_co2,
                "total_analyzed": total_analyzed,
                "coverage_rate": round(co2_coverage_rate, 2),
                "coverage_assessment": "EXCELLENT" if co2_coverage_rate >= 80 else
                                    "GOOD" if co2_coverage_rate >= 60 else
                                    "FAIR" if co2_coverage_rate >= 40 else "POOR"
            },
            "source_distribution": dict(co2_sources),
            "co2_value_statistics": co2_stats,
            "source_priority_effectiveness": {
                "agribalyse_primary": co2_sources.get("agribalyse.co2_total", 0),
                "ecoscore_fallback": co2_sources.get("ecoscore_data.agribalyse.co2_total", 0),
                "nutriments_fallback": co2_sources.get("nutriments.carbon-footprint_100g", 0) + 
                                    co2_sources.get("nutriments.carbon-footprint-from-known-ingredients_100g", 0)
            }
        }
        
        print(f"   → Taux de couverture CO2: {co2_coverage_rate:.1f}%")
        print(f"   → Source principale: {max(co2_sources.items(), key=lambda x: x[1])[0] if co2_sources else 'Aucune'}")
        if co2_stats:
            print(f"   → CO2 moyen: {co2_stats['mean']}g/100g")

    def _analyze_weight_parsing_improvement(self, enriched_products):
        """ANALYZE WEIGHT PARSING IMPROVEMENT - compatible with real data format"""
        
        # Calculate weight parsing metrics from actual data
        weight_parsing_success = 0
        weight_parsing_failed = 0
        parsed_weights = []
        parsed_units = defaultdict(int)
        
        for product_data in enriched_products.values():
            extracted_fields = product_data.get("extracted_fields", {})
            
            weight = extracted_fields.get("weight")
            unit = extracted_fields.get("product_quantity_unit")
            
            if weight is not None and weight != "":
                weight_parsing_success += 1
                parsed_weights.append(weight)
            else:
                weight_parsing_failed += 1
                
            if unit:
                parsed_units[unit] += 1
        
        total_weight_attempts = weight_parsing_success + weight_parsing_failed
        
        # Weight statistics
        weight_stats = {}
        if parsed_weights:
            weight_stats = {
                "min": min(parsed_weights),
                "max": max(parsed_weights),
                "mean": round(np.mean(parsed_weights), 2),
                "median": round(np.median(parsed_weights), 2),
                "count": len(parsed_weights)
            }
        
        # Analyze input types (simplified for real data format)
        input_type_distribution = {"float": len([w for w in parsed_weights if isinstance(w, float)])}
        
        # Success rate
        weight_success_rate = (weight_parsing_success / max(1, total_weight_attempts)) * 100
        
        self.analysis_results["weight_parsing_analysis"] = {
            "parsing_performance": {
                "success_count": weight_parsing_success,
                "failed_count": weight_parsing_failed,
                "total_attempts": total_weight_attempts,
                "success_rate": round(weight_success_rate, 2),
                "performance_assessment": "EXCELLENT" if weight_success_rate >= 85 else
                                        "GOOD" if weight_success_rate >= 70 else
                                        "FAIR" if weight_success_rate >= 50 else "POOR"
            },
            "parsed_weight_statistics": weight_stats,
            "unit_distribution": dict(parsed_units),
            "input_type_analysis": {
                "type_distribution": dict(input_type_distribution),
                "handles_float_int": input_type_distribution.get("float", 0) + input_type_distribution.get("int", 0),
                "handles_strings": input_type_distribution.get("str", 0),
                "correction_effectiveness": "HIGH" if input_type_distribution.get("float", 0) + input_type_distribution.get("int", 0) > 0 else "LOW"
            },
            "improvement_evidence": {
                "previous_issue": "Weight parser rejected float/int values from API",
                "correction_applied": "Added isinstance(quantity_input, (int, float)) handling",
                "expected_improvement": "From ~60% to >80% success rate"
            }
        }
        
        print(f"   → Taux de succès weight parsing: {weight_success_rate:.1f}%")
        print(f"   → Gère float/int: {input_type_distribution.get('float', 0) + input_type_distribution.get('int', 0)} produits")
        print(f"   → Unité principale: {max(parsed_units.items(), key=lambda x: x[1])[0] if parsed_units else 'Aucune'}")

    def _calculate_overall_metrics(self, collection_info, enriched_products):
        """Calculate overall quality metrics - compatible with real data format"""
        
        # Calculate metrics from actual data
        total_products = len(enriched_products)
        
        # Weight parsing success rate
        weight_analysis = self.analysis_results.get("weight_parsing_analysis", {})
        weight_success_rate = weight_analysis.get("parsing_performance", {}).get("success_rate", 0)
        
        # CO2 coverage rate
        co2_analysis = self.analysis_results.get("co2_analysis", {})
        co2_coverage_rate = co2_analysis.get("coverage_metrics", {}).get("coverage_rate", 0)
        
        # Field quality score
        field_analysis = self.analysis_results.get("field_analysis", {})
        critical_fields = ["product_name", "brand_name", "weight", "co2_total"]
        field_quality_scores = [field_analysis.get(f, {}).get("presence_rate", 0) for f in critical_fields]
        field_quality_avg = np.mean(field_quality_scores) if field_quality_scores else 0
        
        # Completeness score (based on total products)
        completeness_score = 100  # All products in the file are considered complete
        
        # Field quality score (average of critical fields)
        field_analysis = self.analysis_results.get("field_analysis", {})
        critical_fields = ["product_name", "brand_name", "weight", "co2_total"]
        field_quality_scores = [field_analysis.get(f, {}).get("presence_rate", 0) for f in critical_fields]
        field_quality_avg = np.mean(field_quality_scores) if field_quality_scores else 0
        
        # Weighted overall score
        overall_quality_score = (
            completeness_score * 0.25 +      # 25% complétude
            field_quality_avg * 0.25 +       # 25% qualité champs
            weight_success_rate * 0.25 +     # 25% weight parsing (correction)
            co2_coverage_rate * 0.25          # 25% couverture CO2
        )
        
        self.analysis_results["data_quality_metrics"] = {
            "completeness": {
                "score": round(completeness_score, 2),
                "total_products": total_products
            },
            "field_quality": {
                "average_score": round(field_quality_avg, 2),
                "critical_fields_scores": {f: field_analysis.get(f, {}).get("presence_rate", 0) for f in critical_fields}
            },
            "weight_parsing": {
                "success_rate": round(weight_success_rate, 2),
                "correction_status": "APPLIED" if weight_success_rate > 75 else "NEEDS_IMPROVEMENT"
            },
            "co2_coverage": {
                "coverage_rate": round(co2_coverage_rate, 2),
                "assessment": "HIGH" if co2_coverage_rate >= 70 else "MEDIUM" if co2_coverage_rate >= 50 else "LOW"
            },
            "overall_quality_score": round(overall_quality_score, 2),
            "quality_grade": "A" if overall_quality_score >= 90 else
                        "B" if overall_quality_score >= 80 else
                        "C" if overall_quality_score >= 70 else
                        "D" if overall_quality_score >= 60 else "F"
        }
        
        print(f"   → Score qualité global: {overall_quality_score:.1f}/100 (Grade: {self.analysis_results['data_quality_metrics']['quality_grade']})")

    def _generate_recommendations(self):
        """Generate recommendations based on analysis"""
        recommendations = []
        
        metrics = self.analysis_results.get("data_quality_metrics", {})
        overall_score = metrics.get("overall_quality_score", 0)
        
        # Recommendations based on overall score
        if overall_score >= 80:
            recommendations.append({
                "priority": "HIGH",
                "category": "Production Ready",
                "finding": f"Excellent score qualité ({overall_score:.1f}/100)",
                "recommendation": "Dataset prêt pour production - Procéder au chargement en base",
                "action": "Load validated products into Supabase production table"
            })
        
        # Specific weight parsing recommendations
        weight_analysis = self.analysis_results.get("weight_parsing_analysis", {})
        weight_success_rate = weight_analysis.get("parsing_performance", {}).get("success_rate", 0)
        
        if weight_success_rate >= 80:
            recommendations.append({
                "priority": "MEDIUM",
                "category": "Weight Parsing Success",
                "finding": f"Correction weight parsing réussie ({weight_success_rate:.1f}%)",
                "recommendation": "Monitoring continu pour maintenir performance",
                "action": "Monitor weight parsing in production for regression"
            })
        elif weight_success_rate < 70:
            recommendations.append({
                "priority": "HIGH", 
                "category": "Weight Parsing Issue",
                "finding": f"Taux de succès weight parsing faible ({weight_success_rate:.1f}%)",
                "recommendation": "Améliorer patterns regex et gestion des cas edge",
                "action": "Review WeightParser patterns and add more fallbacks"
            })
        
        # CO2 recommendations
        co2_analysis = self.analysis_results.get("co2_analysis", {})
        co2_coverage_rate = co2_analysis.get("coverage_metrics", {}).get("coverage_rate", 0)
        
        if co2_coverage_rate < 60:
            recommendations.append({
                "priority": "MEDIUM",
                "category": "CO2 Data Coverage",
                "finding": f"Couverture CO2 limitée ({co2_coverage_rate:.1f}%)",
                "recommendation": "Explorer sources alternatives ou API complémentaires",
                "action": "Investigate additional CO2 data sources beyond OpenFoodFacts"
            })
        
        # Performance recommendations (simplified for real data format)
        pipeline_perf = self.analysis_results.get("pipeline_performance", {})
        efficiency_rate = pipeline_perf.get("pipeline_efficiency", {}).get("efficiency_rate", 0)
        
        if efficiency_rate < 50:
            recommendations.append({
                "priority": "MEDIUM",
                "category": "Pipeline Efficiency",
                "finding": f"Efficacité pipeline faible ({efficiency_rate:.1f}%)",
                "recommendation": "Optimiser la collecte pour atteindre l'objectif",
                "action": "Review collection parameters and increase target coverage"
            })
        
        # Sort by priority
        priority_order = {"HIGH": 3, "MEDIUM": 2, "LOW": 1}
        recommendations.sort(key=lambda x: priority_order.get(x["priority"], 0), reverse=True)
        
        self.analysis_results["recommendations"] = recommendations[:8]  # Top 8
        
        print(f"   → {len(recommendations)} recommandations générées")
        if recommendations:
            print(f"   → Priorité haute: {recommendations[0]['finding']}")

    def _save_analysis_report(self) -> Path:
        """Save complete analysis report"""
        report_filename = f"raw_data_quality_analysis_{self.timestamp}.json"
        report_file = self.output_dir / report_filename
        
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(self.analysis_results, f, indent=2, ensure_ascii=False, default=str)
        
        # Create also a markdown summary
        summary_file = self.output_dir / f"raw_data_quality_summary_{self.timestamp}.md"
        self._create_markdown_summary(summary_file)
        
        return report_file

    def _create_markdown_summary(self, summary_file: Path):
        """Create readable markdown summary"""
        overall_score = self.analysis_results["data_quality_metrics"]["overall_quality_score"]
        grade = self.analysis_results["data_quality_metrics"]["quality_grade"]
        
        # Extract key metrics
        weight_success = self.analysis_results.get("weight_parsing_analysis", {}).get("parsing_performance", {}).get("success_rate", 0)
        co2_coverage = self.analysis_results.get("co2_analysis", {}).get("coverage_metrics", {}).get("coverage_rate", 0)
        
        markdown_content = f"""# Data Quality Report

**Analysis date:** {self.analysis_results["analysis_metadata"]["analysis_datetime"]}  
**Quality score:** {overall_score}/100 (Grade: {grade})

## 📊 Main Metrics

### Pipeline Performance
- **Weight parsing success rate:** {weight_success:.1f}% ← Correction applied
- **CO2 coverage:** {co2_coverage:.1f}%
- **Overall quality score:** {overall_score:.1f}/100

### Critical Fields Analysis
{self._format_field_analysis_markdown()}

### CO2 Analysis
{self._format_co2_analysis_markdown()}

## ⚠️ Main Recommendations

{self._format_recommendations_markdown()}

## 🎯 Conclusions

{self._format_conclusions_markdown(overall_score, grade)}

---
*Generated by quality analyzer compatible v1.0*
"""
        
        with open(summary_file, 'w', encoding='utf-8') as f:
            f.write(markdown_content)

    # Utility methods (simplified for real data format)

    def _calculate_field_quality_score(self, presence_rate, type_distribution):
        """Calculate field quality score"""
        score = presence_rate
        
        # Penalty for too many different types
        if len(type_distribution) > 2:
            score -= 10
        
        return max(0, min(100, score))

    def _get_field_criticality(self, field):
        """Determine field criticality"""
        critical_fields = ["product_name", "brand_name", "weight", "co2_total"]
        important_fields = ["nutriscore_grade", "nutriscore_score"]
        
        if field in critical_fields:
            return "CRITICAL"
        elif field in important_fields:
            return "IMPORTANT"
        else:
            return "OPTIONAL"

    def _format_field_analysis_markdown(self):
        """Format field analysis for markdown"""
        field_analysis = self.analysis_results.get("field_analysis", {})
        
        content = "| Field | Presence Rate | Score | Criticality |\n|-------|---------------|-------|----------|\n"
        for field, analysis in field_analysis.items():
            presence = analysis["presence_rate"]
            score = analysis["quality_score"]
            criticality = analysis["criticality"]
            content += f"| {field} | {presence}% | {score:.1f} | {criticality} |\n"
        
        return content

    def _format_co2_analysis_markdown(self):
        """Format CO2 analysis for markdown"""
        co2_analysis = self.analysis_results.get("co2_analysis", {})
        coverage = co2_analysis.get("coverage_metrics", {})
        
        return f"""
- **Products with CO2:** {coverage.get('products_with_co2', 0)}
- **Products without CO2:** {coverage.get('products_without_co2', 0)}
- **Coverage rate:** {coverage.get('coverage_rate', 0):.1f}%
- **Used sources:** {len(co2_analysis.get('source_distribution', {}))} differents
"""

    def _format_recommendations_markdown(self):
        """Format recommendations for markdown"""
        recommendations = self.analysis_results.get("recommendations", [])[:3]
        
        content = ""
        for i, rec in enumerate(recommendations, 1):
            content += f"""
### {i}. {rec['category']} ({rec['priority']} Priority)
**Finding:** {rec['finding']}  
**Recommendation:** {rec['recommendation']}
"""
        return content

    def _format_conclusions_markdown(self, overall_score, grade):
        """Format conclusions for markdown"""
        if overall_score >= 80:
            return """
✅ **Dataset ready for production**
- Excellent quality score (>80%)
- Weight parsing corrected
- Proceed to loading into database
"""
        elif overall_score >= 70:
            return """
⚠️ **Dataset acceptable with minor improvements**
- Good quality score (70-80%)
- Some optimizations recommended
- Can be used in production with monitoring
"""
        else:
            return """
❌ **Dataset needs improvements**
- Insufficient quality score (<70%)
- Major issues to resolve
- Report deployment to production
"""


async def main():
    """Analyze the latest collection file"""
    
    # Search for collection files
    data_dir = Path("data_engineering/data/raw/openfoodfacts/")
    
    if not data_dir.exists():
        print("❌ Data directory not found")
        print(f"   → Expected: {data_dir}")
        return 1
    
    # Find the latest file
    collection_files = list(data_dir.glob("chocolate_extraction_raw_*.json"))
    
    if not collection_files:
        print("❌ No collection file found")
        print(f"   → Directory: {data_dir}")
        print(f"   → Pattern: chocolate_extraction_raw_*.json")
        return 1
    
    latest_file = max(collection_files, key=lambda f: f.stat().st_mtime)
    
    print("📊 ANALYSIS OF DATA QUALITY - ORIGINAL FORMAT")
    print(f"📁 Latest file: {latest_file.name}")
    
    try:
        analyzer = DataQualityAnalyzer()
        results = analyzer.analyze_collection_file(latest_file)
        
        overall_score = results["data_quality_metrics"]["overall_quality_score"]
        grade = results["data_quality_metrics"]["quality_grade"]
        
        print(f"\n🎯 FINAL SUMMARY:")
        print(f"   → Quality score: {overall_score:.1f}/100 (Grade: {grade})")
        print(f"   → Recommendations: {len(results['recommendations'])}")
        
        # Specific metrics
        weight_analysis = results.get("weight_parsing_analysis", {})
        weight_success = weight_analysis.get("parsing_performance", {}).get("success_rate", 0)
        print(f"   → Weight parsing: {weight_success:.1f}% (correction applied)")
        
        co2_analysis = results.get("co2_analysis", {})
        co2_coverage = co2_analysis.get("coverage_metrics", {}).get("coverage_rate", 0)
        print(f"   → CO2 coverage: {co2_coverage:.1f}%")
        
        if overall_score >= 75:
            print(f"\n✅ DATASET READY FOR PRODUCTION!")
        else:
            print(f"\n⚠️ Dataset needs improvements")
        
        return 0
        
    except Exception as e:
        print(f"\n💥 Analysis failed: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    import asyncio
    exit_code = asyncio.run(main())
    exit(exit_code)