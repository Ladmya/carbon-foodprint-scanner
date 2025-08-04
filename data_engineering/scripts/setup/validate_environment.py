"""
data_engineering/scripts/setup/validate_environment.py
Validate project environment and configuration for data engineering workflows 
"""

import os
import sys
import importlib
from pathlib import Path
from datetime import datetime
from typing import Dict, Any

# Add src to path for imports
#sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[3]/"src"))


class EnvironmentValidator:
    """Validate complete environment setup for Carbon Foodprint Scanner"""
    
    def __init__(self):
        #self.project_root = Path(__file__).parent.parent.parent
        self.project_root = Path(__file__).resolve().parents[3]
        self.validation_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.validation_results = {
            "timestamp": self.validation_timestamp,
            "project_root": str(self.project_root),
            "validations": {},
            "critical_issues": [],
            "warnings": [],
            "recommendations": []
        }
    
    def run_complete_validation(self) -> Dict[str, Any]:
        """Run complete environment validation"""
        print("🔧 ENVIRONMENT VALIDATION - Carbon Foodprint Scanner")
        print("=" * 65)
        print(f"📁 Project root: {self.project_root}")
        print(f"🕒 Validation timestamp: {self.validation_timestamp}")
        print("=" * 65)
        
        # Run all validation checks
        self._validate_project_structure()
        self._validate_python_environment()
        self._validate_dependencies()
        self._validate_configuration()
        self._validate_data_directories()
        self._validate_external_connections()
        
        # Generate summary
        self._generate_validation_summary()
        
        return self.validation_results
    
    def _validate_project_structure(self):
        """Validate project directory structure"""
        print("\n📁 VALIDATING PROJECT STRUCTURE")
        print("-" * 40)
        
        required_paths = [
            # Core application
            "src/food_scanner/core/config.py",
            "src/food_scanner/core/constants.py",
            "src/food_scanner/core/models/__init__.py",
            "src/food_scanner/core/services/__init__.py",
            "src/food_scanner/core/utils/__init__.py",
            
            # Data engineering
            "src/food_scanner/data/extractors/__init__.py",
            "src/food_scanner/data/transformers/__init__.py",
            "src/food_scanner/data/loaders/__init__.py",
            "src/food_scanner/data/pipelines/__init__.py",
            "src/food_scanner/data/analysis/__init__.py",
            "src/food_scanner/data/utils/__init__.py",
            
            # Infrastructure
            "src/food_scanner/infrastructure/external_apis/__init__.py",
            "src/food_scanner/infrastructure/external_apis/base_client.py",
            "src/food_scanner/infrastructure/external_apis/openfoodfacts.py",
            "src/food_scanner/infrastructure/database/__init__.py",
            "src/food_scanner/infrastructure/monitoring/__init__.py",
            
            # Bot application
            "src/food_scanner/bot/__init__.py",
            "src/food_scanner/bot/run_telegram_bot.py",
            "src/food_scanner/bot/barcode_scanner.py",
            "src/food_scanner/bot/database_service.py",
            
            # API (future)
            "src/food_scanner/api/__init__.py",
            "src/food_scanner/api/routers/__init__.py",
            "src/food_scanner/api/middleware/__init__.py",
            
            # Data directories (moved to data_engineering/data/)
            "data_engineering/data/raw",
            "data_engineering/data/processed",
            "data_engineering/data/analysis",
            "data_engineering/data/cache",
            "data_engineering/data/reports",
            
            # Data engineering scripts
            "data_engineering/scripts/setup",
            "data_engineering/scripts/collection",
            "data_engineering/scripts/analysis",
            "data_engineering/scripts/loading",
            "data_engineering/scripts/maintenance",
            
            # Tests
            "tests/unit",
            "tests/integration",
            "tests/e2e",
            "tests/fixtures",
            
            # Configuration files
            "requirements.txt",
            "pyproject.toml",
            "Makefile"
        ]
        
        structure_results = {
            "required_paths_checked": len(required_paths),
            "existing_paths": 0,
            "missing_paths": [],
            "status": "unknown"
        }
        
        for path_str in required_paths:
            path = self.project_root / path_str
            if path.exists():
                structure_results["existing_paths"] += 1
                print(f"   ✅ {path_str}")
            else:
                structure_results["missing_paths"].append(path_str)
                print(f"   ❌ {path_str}")
        
        # Determine status
        missing_count = len(structure_results["missing_paths"])
        if missing_count == 0:
            structure_results["status"] = "excellent"
            print(f"   🎉 Structure parfaite: {structure_results['existing_paths']}/{structure_results['required_paths_checked']}")
        elif missing_count <= 3:
            structure_results["status"] = "good"
            print(f"   ✅ Structure correcte: {structure_results['existing_paths']}/{structure_results['required_paths_checked']}")
            self.validation_results["warnings"].append(f"{missing_count} chemins manquants")
        else:
            structure_results["status"] = "critical"
            print(f"   ❌ Structure incomplète: {structure_results['existing_paths']}/{structure_results['required_paths_checked']}")
            self.validation_results["critical_issues"].append(
                f"Structure de projet incomplète: {missing_count} chemins manquants"
            )
        
        self.validation_results["validations"]["project_structure"] = structure_results
    
    def _validate_python_environment(self):
        """Validate Python version and environment"""
        print("\n🐍 VALIDATING PYTHON ENVIRONMENT")
        print("-" * 40)
        
        python_results = {
            "python_version": f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}",
            "python_executable": sys.executable,
            "virtual_env": os.environ.get('VIRTUAL_ENV'),
            "pythonpath_includes_src": False,
            "status": "unknown"
        }
        
        print(f"   📍 Python version: {python_results['python_version']}")
        print(f"   📍 Python executable: {python_results['python_executable']}")
        
        # Check Python version (need 3.9+)
        if sys.version_info >= (3, 12):
            print(f"   ✅ Python version : excellent (3.12+ required)")
            python_results["status"] = "excellent"
        # Pylance automatically detects my local env 3.12+ gives a false positive to ELIF & ELSE. Will work with lesser Python version    
        elif sys.version_info >= (3, 9):
            print(f"   ✅Python version : compatible (3.9+, 3.12+ required)")
            python_results["status"] = "good"
            self.validation_results["warnings"].append(
                f"Python 3.12+ required, for better performances, actual version: {python_results['python_version']}")
        else:
            print(f"   ❌ Python version incompatible (3.9+ requis, got {python_results['python_version']})")
            python_results["status"] = "critical"
            self.validation_results["critical_issues"].append(
                f"Python 3.9+ requis, version actuelle: {python_results['python_version']}"
            ) 
        
        # Check virtual environment
        if python_results["virtual_env"]:
            print(f"   ✅ Virtual environment actif: {python_results['virtual_env']}")
        else:
            print(f"   ⚠️  Pas de virtual environment détecté")
            self.validation_results["warnings"].append("Recommandé d'utiliser un virtual environment")
        
        # Check if src in Python path
        src_path = str(self.project_root / "src")
        if src_path in sys.path:
            python_results["pythonpath_includes_src"] = True
            print(f"   ✅ src/ dans PYTHONPATH")
        else:
            print(f"   ℹ️  src/ pas dans PYTHONPATH (sera ajouté dynamiquement)")
        
        python_results["status"] = "good"
        self.validation_results["validations"]["python_environment"] = python_results
    
    def _validate_dependencies(self):
        """Validate required Python dependencies for current development phase"""
        print("\n📦 VALIDATING DEPENDENCIES")
        print("-" * 40)
        
        required_packages = [
            ("telegram", "Telegram Bot API"),
            ("pyzbar", "Barcode scanning"),
            ("supabase", "Supabase client"),
            ("psycopg2", "PostgreSQL adapter"),
            ("postgrest", "PostgREST client"),
            ("httpx", "HTTP client pour APIs"),
            ("python-dotenv", "Variables d'environnement"),
            ("pytest", "Tests unitaires"),
            ("PIL", "Image processing"),
            ("opencv-python", "Computer vision"),
        ]
        
        dependency_results = {
            "total_checked": len(required_packages),
            "installed": 0,
            "missing": [],
            "versions": {},
            "status": "unknown"
        }
        
        for package_name, description in required_packages:
            try:
                # Handle special cases for package names
                import_name = package_name.replace("-", "_")
                if package_name == "python-dotenv":
                    import_name = "dotenv"
                elif package_name == "opencv-python":
                    import_name = "cv2"
                elif package_name == "pillow":
                    import_name = "PIL"
                
                module = importlib.import_module(import_name)
                version = getattr(module, '__version__', 'unknown')
                dependency_results["versions"][package_name] = version
                dependency_results["installed"] += 1
                print(f"   ✅ {package_name} ({version}) - {description}")
            except ImportError:
                dependency_results["missing"].append(package_name)
                print(f"   ❌ {package_name} - {description}")
        
        # Check status
        missing_count = len(dependency_results["missing"])
        if missing_count == 0:
            dependency_results["status"] = "excellent"
            print(f"   🎉 Toutes les dépendances installées!")
        elif missing_count <= 2:
            dependency_results["status"] = "warning"
            print(f"   ⚠️  {missing_count} dépendances manquantes")
            self.validation_results["warnings"].append(f"Dépendances manquantes: {', '.join(dependency_results['missing'])}")
        else:
            dependency_results["status"] = "critical"
            print(f"   ❌ {missing_count} dépendances critiques manquantes")
            self.validation_results["critical_issues"].append(
                f"Dépendances manquantes: {', '.join(dependency_results['missing'])}"
            )
        
        self.validation_results["validations"]["dependencies"] = dependency_results
    
    def _validate_configuration(self):
        """Validate configuration files and environment variables"""
        print("\n⚙️  VALIDATING CONFIGURATION")
        print("-" * 40)
        
        config_results = {
            "env_file_exists": False,
            "env_example_exists": False,
            "config_importable": False,
            "constants_importable": False,
            "critical_env_vars": {},
            "status": "unknown"
        }
        
        # Check .env files
        env_file = self.project_root / ".env"
        env_example = self.project_root / ".env.example"
        
        if env_file.exists():
            config_results["env_file_exists"] = True
            print(f"   ✅ .env file exists")
        else:
            print(f"   ❌ .env file missing")
            self.validation_results["critical_issues"].append(".env file manquant")
        
        if env_example.exists():
            config_results["env_example_exists"] = True
            print(f"   ✅ .env.example exists")
        else:
            print(f"   ⚠️  .env.example missing")
            self.validation_results["warnings"].append(".env.example manquant (recommandé)")
        
        # Try to only test imports configuration modules config_module
        try:
            import food_scanner.core.config as config_module
            config_results["config_importable"] = True
            print(f"   ✅ config.py importable")
            
            # Check critical environment variables
            critical_vars = [
                "DB_ENVIRONMENT",
                "USE_TEST_API",
                "SUPABASE_TEST_DATABASE_URL",
                "SUPABASE_PRODUCTION_DATABASE_URL"
            ]
            
            for var in critical_vars:
                value = os.getenv(var)
                config_results["critical_env_vars"][var] = "set" if value else "missing"
                if value:
                    print(f"   ✅ {var} configuré")
                else:
                    print(f"   ⚠️  {var} non configuré")
            
        except ImportError as e:
            config_results["config_importable"] = False
            print(f"   ❌ Impossible d'importer config.py: {e}")
            self.validation_results["critical_issues"].append(f"Configuration non importable: {e}")
        
        # Try to only test imports configuration modules constants_module
        try:
            import food_scanner.core.constants as constants_module
            config_results["constants_importable"] = True
            print(f"   ✅ constants.py importable")
        except ImportError as e:
            config_results["constants_importable"] = False
            print(f"   ❌ Impossible d'importer constants.py: {e}")
            self.validation_results["critical_issues"].append(f"Constants non importable: {e}")
        
        # Determine status
        if config_results["config_importable"] and config_results["constants_importable"]:
            config_results["status"] = "good"
        else:
            config_results["status"] = "critical"
        
        self.validation_results["validations"]["configuration"] = config_results
    
    def _validate_data_directories(self):
        """Validate data directory structure and permissions"""
        print("\n📊 VALIDATING DATA DIRECTORIES")
        print("-" * 40)
        
        data_dirs = [
            "data_engineering/data/raw/openfoodfacts",
            "data_engineering/data/raw/metadata",
            "data_engineering/data/processed/validated",
            "data_engineering/data/processed/rejected",
            "data_engineering/data/processed/enriched",
            "data_engineering/data/analysis/extraction_phase/missing_co2_field",
            "data_engineering/data/analysis/extraction_phase/missing_critical_fields",
            "data_engineering/data/analysis/loading_phase/error_reports",
            "data_engineering/data/analysis/loading_phase/loading_stats",
            "data_engineering/data/analysis/transformation_phase/transformation_stats",
            "data_engineering/data/cache/api_responses",
            "data_engineering/data/cache/calculations",
            "data_engineering/data/reports/environment/validation",
            "data_engineering/data/reports/extraction/raw_data_quality",
            "data_engineering/data/reports/extraction/extraction_performance",
            "data_engineering/data/reports/loading/loading_quality",
            "data_engineering/data/reports/loading/loading_reports",
            "data_engineering/data/reports/transformation/transformed_data_quality",
            "data_engineering/data/reports/transformation/validation_reports",
            "data_engineering/data/reports/transformation/transformation_performance",
            "data_engineering/data/reports/quality/overall_quality",
            "data_engineering/data/reports/quality/quality_reports",
            "data_engineering/data/reports/production/production_readiness",
            "data_engineering/data/reports/production/executive_summary"
        ]
        
        data_results = {
            "directories_checked": len(data_dirs),
            "existing_directories": 0,
            "writable_directories": 0,
            "missing_directories": [],
            "permission_issues": [],
            "status": "unknown"
        }
        
        for dir_path in data_dirs:
            full_path = self.project_root / dir_path
            
            if full_path.exists():
                data_results["existing_directories"] += 1
                print(f"   ✅ {dir_path}")
                
                # Test write permissions
                try:
                    test_file = full_path / ".write_test"
                    test_file.touch()
                    test_file.unlink()
                    data_results["writable_directories"] += 1
                except PermissionError:
                    data_results["permission_issues"].append(dir_path)
                    print(f"   ⚠️  {dir_path} - permission d'écriture manquante")
                    
            else:
                data_results["missing_directories"].append(dir_path)
                print(f"   ❌ {dir_path}")
        
        # Determine status
        missing_count = len(data_results["missing_directories"])
        permission_count = len(data_results["permission_issues"])
        
        if missing_count == 0 and permission_count == 0:
            data_results["status"] = "excellent"
            print(f"   🎉 Tous les répertoires data prêts!")
        elif missing_count <= 3 and permission_count == 0:
            data_results["status"] = "good"
            print(f"   ✅ Structure data correcte")
        else:
            data_results["status"] = "warning"
            if missing_count > 0:
                self.validation_results["warnings"].append(f"{missing_count} répertoires data manquants")
            if permission_count > 0:
                self.validation_results["warnings"].append(f"{permission_count} problèmes de permissions")
        
        self.validation_results["validations"]["data_directories"] = data_results
    
    def _validate_external_connections(self):
        """Validate external service connections (optional)"""
        print("\n🌐 VALIDATING EXTERNAL CONNECTIONS")
        print("-" * 40)
        
        connection_results = {
            "openfoodfacts_api": "not_tested",
            "supabase_connection": "not_tested",
            "tests_performed": [],
            "status": "skipped"
        }
        
        # Test OpenFoodFacts API (basic connectivity)
        try:
            import httpx
            
            print(f"   🔍 Test OpenFoodFacts API...")
            with httpx.Client(timeout=10) as client:
                response = client.get("https://world.openfoodfacts.org/api/v2/product/3017620422003.json")
                if response.status_code == 200:
                    connection_results["openfoodfacts_api"] = "success"
                    print(f"   ✅ OpenFoodFacts API accessible")
                else:
                    connection_results["openfoodfacts_api"] = "failed"
                    print(f"   ⚠️  OpenFoodFacts API: status {response.status_code}")
            
            connection_results["tests_performed"].append("openfoodfacts")
            
        except Exception as e:
            connection_results["openfoodfacts_api"] = "error"
            print(f"   ⚠️  OpenFoodFacts API test failed: {e}")
        
        # Note: Supabase test would require credentials, skip for now
        print(f"   ℹ️  Supabase connection test skipped (requires credentials)")
        
        connection_results["status"] = "partial"
        self.validation_results["validations"]["external_connections"] = connection_results
    
    def _generate_validation_summary(self):
        """Generate validation summary and recommendations"""
        print("\n" + "=" * 65)
        print("📋 VALIDATION SUMMARY")
        print("=" * 65)
        
        # Count issues
        critical_count = len(self.validation_results["critical_issues"])
        warning_count = len(self.validation_results["warnings"])
        
        # Overall status
        if critical_count == 0 and warning_count == 0:
            overall_status = "🎉 EXCELLENT"
            status_color = "✅"
        elif critical_count == 0:
            overall_status = "👍 GOOD"
            status_color = "✅"
        elif critical_count <= 2:
            overall_status = "⚠️  NEEDS ATTENTION"
            status_color = "⚠️"
        else:
            overall_status = "❌ CRITICAL ISSUES"
            status_color = "❌"
        
        print(f"{status_color} Overall Status: {overall_status}")
        print(f"📊 Critical Issues: {critical_count}")
        print(f"📊 Warnings: {warning_count}")
        
        # Show validation results
        print(f"\n📋 Validation Results:")
        for validation_name, result in self.validation_results["validations"].items():
            status = result.get("status", "unknown")
            status_icon = {"excellent": "🎉", "good": "✅", "warning": "⚠️", "critical": "❌", "partial": "📊"}.get(status, "❓")
            print(f"   {status_icon} {validation_name.replace('_', ' ').title()}: {status}")
        
        # Show critical issues
        if critical_count > 0:
            print(f"\n🚨 CRITICAL ISSUES TO FIX:")
            for issue in self.validation_results["critical_issues"]:
                print(f"   ❌ {issue}")
        
        # Show warnings
        if warning_count > 0:
            print(f"\n⚠️  WARNINGS:")
            for warning in self.validation_results["warnings"]:
                print(f"   ⚠️  {warning}")
        
        # Generate recommendations
        self._generate_recommendations()
        
        if self.validation_results["recommendations"]:
            print(f"\n💡 RECOMMENDATIONS:")
            for i, rec in enumerate(self.validation_results["recommendations"], 1):
                print(f"   {i}. {rec}")
        
        # Next steps
        print(f"\n🎯 NEXT STEPS:")
        if critical_count > 0:
            print(f"   1. Fix critical issues above")
            print(f"   2. Re-run validation: python data_engineering/scripts/setup/validate_environment.py")
            print(f"   3. Once clean, proceed with data collection")
        else:
            print(f"   1. Environment ready for data engineering workflows!")
            print(f"   2. Run data collection: python data_engineering/scripts/collection/complete_extraction_pipeline.py")
            print(f"   3. Analyze data quality: python data_engineering/scripts/collection/generate_production_reports.py")
        
        # Save validation report
        self._save_validation_report()
    
    def _generate_recommendations(self):
        """Generate specific recommendations based on validation results"""
        recs = []
        
        # Structure recommendations
        structure = self.validation_results["validations"].get("project_structure", {})
        if structure.get("status") in ["critical", "warning"]:
            recs.append("Run setup script to create missing directories: python setup_script.py")
        
        # Dependency recommendations
        deps = self.validation_results["validations"].get("dependencies", {})
        if deps.get("missing"):
            recs.append(f"Install missing packages: pip install {' '.join(deps['missing'])}")
        
        # Configuration recommendations
        config = self.validation_results["validations"].get("configuration", {})
        if not config.get("env_file_exists"):
            recs.append("Create .env file from .env.example and configure variables")
        
        # Data directories recommendations
        data_dirs = self.validation_results["validations"].get("data_directories", {})
        if data_dirs.get("missing_directories"):
            recs.append("Create missing data directories with proper permissions")
        
        # General recommendations
        recs.append("Use virtual environment for development: python -m venv carbon_foodprint-venv")
        recs.append("Run tests to verify functionality: make test")
        
        self.validation_results["recommendations"] = recs
    
    def _save_validation_report(self):
        """Save validation report with timestamp"""
        report_dir = self.project_root / "data_engineering" / "data" / "reports" / "environment"
        report_dir.mkdir(parents=True, exist_ok=True)
        
        report_file = report_dir / f"environment_validation_report_{self.validation_timestamp}.json"
        
        with open(report_file, 'w', encoding='utf-8') as f:
            import json
            json.dump(self.validation_results, f, indent=2, ensure_ascii=False, default=str)
        
        print(f"\n💾 Validation report saved: {report_file}")


def main():
    """Run environment validation"""
    validator = EnvironmentValidator()
    results = validator.run_complete_validation()
    
    # Exit with appropriate code
    critical_count = len(results["critical_issues"])
    if critical_count > 0:
        print(f"\n❌ Validation failed with {critical_count} critical issues")
        sys.exit(1)
    else:
        print(f"\n✅ Environment validation successful!")
        sys.exit(0)


if __name__ == "__main__":
    main()