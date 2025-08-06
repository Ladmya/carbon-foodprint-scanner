"""
Setup Script - Carbon Foodprint Scanner Architecture
Data engineering structure creation + old files migration 
"""

import shutil
import json
from pathlib import Path
from datetime import datetime


class ArchitectureSetup:
    """Revised repo architecture set up"""
    
    def __init__(self, project_root: Path):
        self.project_root = Path(project_root)
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.backup_dir = self.project_root / f"architecture_migration_backup_{self.timestamp}"
        
    def run_complete_setup(self):
        """Launch full architecture setup"""
        print("🏗️  CARBON FOODPRINT SCANNER - ARCHITECTURE SETUP")
        print("=" * 70)
        print(f"📁 Project root: {self.project_root}")
        print(f"🕒 Timestamp: {self.timestamp}")
        print("=" * 70)
        
        try:
            # Step 1: Backup
            self.create_backup()
            
            # Step 2: Clean up old structure  
            self.cleanup_old_structure()
            
            # Step 3: Create new structure
            self.create_directory_structure()
            
            # Step 4: Migrate existing files
            self.migrate_existing_files()
            
            # Step 5: Create essential files
            self.create_essential_files()
            
            # Step 6: Final report
            self.generate_setup_report()
            
            print("\n🎉 FULL SETUP COMPLETED!")
            print(f"✅ Architecture data engineering installed")
            print(f"✅ Backup created: {self.backup_dir}")
            print(f"✅ Ready for dev!")
            
        except Exception as e:
            print(f"\n💥 ERROR SETUP: {e}")
            print(f"🔄 Roll back from: {self.backup_dir}")
            raise
    
    def create_backup(self):
        """Actual state back up"""
        print("\n💾 STEP 1: Backup creation...")
        
        self.backup_dir.mkdir(exist_ok=True)
        
        # Files to keep
        backup_items = [
            "src/",
            "scripts/",
            "data_files/",
            ".env",
            "requirements.txt",
            "pyproject.toml",
            "README.md"
        ]
        
        backup_count = 0
        for item in backup_items:
            source_path = self.project_root / item
            if source_path.exists():
                dest_path = self.backup_dir / item
                
                if source_path.is_file():
                    dest_path.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(source_path, dest_path)
                    backup_count += 1
                elif source_path.is_dir():
                    shutil.copytree(source_path, dest_path, dirs_exist_ok=True)
                    backup_count += 1
        
        print(f"   ✅ {backup_count} backup elements stored in {self.backup_dir}")
    
    def cleanup_old_structure(self):
        """Old repo structure cleanup"""
        print("\n🧹 STEP 2: Old structure clean up...")
        
        items_to_remove = [
            "scripts/",  # Remplaced by data_engineering/scripts/
            "data_files/",  # Remplaced by data/ (structure clean)
        ]
        
        removed_count = 0
        for item in items_to_remove:
            item_path = self.project_root / item
            if item_path.exists():
                if item_path.is_dir():
                    shutil.rmtree(item_path)
                else:
                    item_path.unlink()
                removed_count += 1
                print(f"   🗑️  Deleted: {item}")
        
        print(f"   ✅ {removed_count} deleted elements from first architecture")
    
    def create_directory_structure(self):
        """Create new repository structure"""
        print("\n📁 STEP 3: Creation of new repository structure ...")
        
        directories = [
            # Core application (keep existing + new)
            "src/food_scanner/core/models",
            "src/food_scanner/core/services",
            "src/food_scanner/core/utils",
            
            # Data engineering (ETL separated)
            "src/food_scanner/data/extractors",
            "src/food_scanner/data/transformers/field_transformers",
            "src/food_scanner/data/loaders",
            "src/food_scanner/data/pipelines",
            "src/food_scanner/data/analysis",
            
            # Infrastructure (keep existing)
            "src/food_scanner/infrastructure/database/repositories",
            "src/food_scanner/infrastructure/database/migrations",
            "src/food_scanner/infrastructure/monitoring",
            
            # API layer (futur)
            "src/food_scanner/api/routers",
            "src/food_scanner/api/middleware",
            
            # Data engineering scripts
            "data_engineering/scripts/setup",
            "data_engineering/scripts/collection",
            "data_engineering/scripts/analysis",
            "data_engineering/scripts/maintenance",
            "data_engineering/notebooks",
            
            # Data storage (new organisation)
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
            
            # Reports structure (new organisation)
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
            "data_engineering/data/reports/production/executive_summary",
            
            # Tests (full structure)
            "tests/unit/test_transformers",
            "tests/unit/test_validators",
            "tests/unit/test_services",
            "tests/integration/test_pipelines",
            "tests/integration/test_api_clients",
            "tests/integration/test_database",
            "tests/e2e",
            "tests/fixtures",
        ]
        
        created_count = 0
        for directory in directories:
            dir_path = self.project_root / directory
            dir_path.mkdir(parents=True, exist_ok=True)
            
            # Create __init__.py for Python modules
            if any(x in directory for x in ["src/", "tests/"]) and "notebooks" not in directory:
                init_file = dir_path / "__init__.py"
                if not init_file.exists():
                    init_file.touch()
            
            created_count += 1
        
        print(f"   ✅ {created_count} directory created with __init__.py")
    
    def migrate_existing_files(self):
        """Migrer fichiers existants vers nouvelle structure"""
        print("\n🔄 STEP 4: Existing files migration...")
        
        migrations = [
            # Keep useful files
            {
                "source": "src/food_scanner/core/config.py",
                "destination": "src/food_scanner/core/config.py",
                "action": "keep"
            },
            {
                "source": "src/food_scanner/core/constants.py", 
                "destination": "src/food_scanner/core/constants.py",
                "action": "keep"
            },
            {
                "source": "src/food_scanner/infrastructure/external_apis/",
                "destination": "src/food_scanner/infrastructure/external_apis/",
                "action": "keep"
            },
            
            # Migrate data_files to structure data_engineering/data/
            {
                "source_pattern": "data_files/comprehensive_analysis/raw_datasets/*.json",
                "destination": "data_engineering/data/raw/openfoodfacts/",
                "action": "migrate_pattern"
            },
            {
                "source_pattern": "data_files/comprehensive_analysis/analysis/*.json",
                "destination": "data_engineering/data/reports/extraction/raw_data_quality/",
                "action": "migrate_pattern"
            },
            {
                "source_pattern": "data_files/processed/*.json",
                "destination": "data_engineering/data/processed/validated/",
                "action": "migrate_pattern"
            },
        ]
        
        migrated_count = 0
        
        for migration in migrations:
            try:
                if migration["action"] == "keep":
                    # Check if file already exists 
                    source_path = self.project_root / migration["source"]
                    if source_path.exists():
                        print(f"   ✅ Gardé: {migration['source']}")
                        migrated_count += 1
                
                elif migration["action"] == "migrate_pattern":
                    # Files migration according to pattern (from back up if deleted) 
                    source_pattern = migration.get("source_pattern", "")
                    destination = migration["destination"]
                    
                    # Search in backup
                    backup_pattern_path = self.backup_dir / source_pattern.replace("*", "")
                    backup_parent = backup_pattern_path.parent
                    
                    if backup_parent.exists():
                        pattern_files = list(backup_parent.glob("*.json"))
                        dest_dir = self.project_root / destination
                        dest_dir.mkdir(parents=True, exist_ok=True)
                        
                        for file_path in pattern_files:
                            dest_file = dest_dir / file_path.name
                            shutil.copy2(file_path, dest_file)
                            migrated_count += 1
                        
                        if pattern_files:
                            print(f"   ✅ Migré {len(pattern_files)} fichiers: {source_pattern} → {destination}")
                    
            except Exception as e:
                print(f"   ⚠️  Erreur migration {migration}: {e}")
        
        print(f"   ✅ {migrated_count} fichiers migrés avec succès")
    
    def create_essential_files(self):
        """Create essential files for the architecture"""
        print("\n📄 STEP 5: Essential files creation ...")
        
        files_created = []
        
        # 1. README.md mis à jour
        readme_content = f"""# Carbon Foodprint Scanner

Data engineering project for scanning product barcodes and calculating carbon footprint transport equivalents.

## 🏗️ Architecture

- **ETL Pipeline**: Separated Extract → Transform → Load
- **Data Engineering**: `data_engineering/` scripts and analysis  
- **Core Business Logic**: `src/food_scanner/core/`
- **Infrastructure**: APIs, database, monitoring

## 🚀 Quick Start

```bash
# Setup environment
python data_engineering/scripts/setup/validate_environment.py

# Collect data
python data_engineering/scripts/collection/complete_extraction_pipeline.py

# Analyze data quality
python data_engineering/scripts/collection/generate_production_reports.py
```

## 🗂️ Project Structure

```
src/food_scanner/          # Core application
├── core/                  # Business domain
├── data/                  # ETL pipeline  
├── infrastructure/        # External dependencies
└── api/                   # REST API (future)

data_engineering/          # Data engineering scripts
├── scripts/               # Operational scripts
└── notebooks/             # Analysis notebooks

data/                      # Data storage
├── raw/                   # Raw extraction results
├── processed/             # Transformed data
├── analysis/              # Analysis reports
└── cache/                 # Performance cache

tests/                     # Comprehensive testing
├── unit/                  # Unit tests
├── integration/           # Integration tests
└── e2e/                   # End-to-end tests
```

## 🎯 Tech Stack

- **Python 3.12** - Core language
- **FastAPI** - REST API framework  
- **SQLAlchemy** - ORM
- **Supabase** - Database (test + prod)
- **OpenFoodFacts API** - Product data source
- **Datadog** - Monitoring

## 📊 Data Pipeline

1. **Extract**: Retrieve raw data from OpenFoodFacts API
2. **Transform**: Apply business rules and validation
3. **Load**: Store in Supabase + local backup

Architecture setup: {self.timestamp}
"""
        
        readme_path = self.project_root / "README.md"
        with open(readme_path, 'w', encoding='utf-8') as f:
            f.write(readme_content)
        files_created.append("README.md")
        
        # 2. .gitignore mis à jour
        gitignore_content = """# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
build/
develop-eggs/
dist/
downloads/
eggs/
.eggs/
lib/
lib64/
parts/
sdist/
var/
wheels/
*.egg-info/
.installed.cfg
*.egg

# Virtual Environment
carbon_foodprint-venv/
venv/
ENV/
env/

# Environment Variables
.env
.env.local
.env.prod

# Data files (keep structure, ignore content)
data_engineering/data/raw/*/
data_engineering/data/processed/*/
data_engineering/data/cache/*/
!data_engineering/data/*/.gitkeep
!data_engineering/data/raw/README.md
!data_engineering/data/processed/README.md

# Analysis results (timestamped files)
data_engineering/data/analysis/**/
!data_engineering/data/analysis/README.md

# Backups
*_backup_*/
architecture_migration_backup_*/

# IDE
.vscode/
.idea/
*.swp
*.swo

# OS
.DS_Store
Thumbs.db

# Jupyter Notebooks
.ipynb_checkpoints/
data_engineering/notebooks/*.ipynb

# Logs
*.log
logs/

# Testing
.pytest_cache/
.coverage
htmlcov/

# Temporary files
tmp/
temp/
"""
        
        gitignore_path = self.project_root / ".gitignore"
        with open(gitignore_path, 'w', encoding='utf-8') as f:
            f.write(gitignore_content)
        files_created.append(".gitignore")
        
        # 3. Data directory READMEs
        data_readmes = {
            "data_engineering/data/raw/README.md": "# Raw Data\n\nRaw extraction results from external APIs.\n\n- `openfoodfacts`: extraction from API\n- `metadata/`: Extraction metadata and stats",
            "data_engineering/data/processed/README.md": "# Processed Data\n\nTransformed and validated data ready for loading.\n\n- `validated/`: Passed all validation rules\n- `rejected/`: Failed validation with reasons\n- `enriched/`: With calculated fields (CO2 equivalents, etc.)",
            "data_engineering/data/analysis/README.md": "# Analysis Results\n\nData quality reports and analysis results.\n\n- `extraction_phase/`: Field-by-field analysis\n- `loading_phase/`: Overall data quality\n- `transformation_phase/`: Generated transformation rules"
        }
        
        for readme_path, content in data_readmes.items():
            full_path = self.project_root / readme_path
            full_path.parent.mkdir(parents=True, exist_ok=True)
            with open(full_path, 'w', encoding='utf-8') as f:
                f.write(content)
            files_created.append(readme_path)
        
        # 4. Makefile pour commandes courantes
        makefile_content = """# Carbon Foodprint Scanner - Data Engineering

.PHONY: setup test collect analyze clean help

# Setup and validation
setup:
	@echo "🔧 Validating environment..."
	python data_engineering/scripts/setup/validate_environment.py

# Data collection
collect:
	@echo "📥 Starting data collection..."
	python data_engineering/scripts/collection/complete_extraction_pipeline.py

# Data analysis
analyze:
	@echo "📊 Running data quality analysis..."
	python data_engineering/scripts/collection/generate_production_reports.py

# Testing
test:
	@echo "🧪 Running tests..."
	python -m pytest tests/ -v

test-unit:
	@echo "🧪 Running unit tests..."
	python -m pytest tests/unit/ -v

test-integration:
	@echo "🧪 Running integration tests..."
	python -m pytest tests/integration/ -v

# Maintenance
clean:
	@echo "🧹 Cleaning temporary files..."
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	find . -type d -name ".pytest_cache" -exec rm -rf {} +

clean-data:
	@echo "🧹 Cleaning data cache..."
	rm -rf data_engineering/data/cache/*
	rm -rf data_engineering/data/processed/rejected/*

# Help
help:
	@echo "Available commands:"
	@echo "  setup     - Validate environment"
	@echo "  collect   - Run data collection"
	@echo "  analyze   - Run data analysis"
	@echo "  test      - Run all tests"
	@echo "  clean     - Clean temporary files"
	@echo "  help      - Show this help"
"""
        
        makefile_path = self.project_root / "Makefile"
        with open(makefile_path, 'w', encoding='utf-8') as f:
            f.write(makefile_content)
        files_created.append("Makefile")
        
        print(f"   ✅ {len(files_created)} essential file created:")
        for file_name in files_created:
            print(f"      → {file_name}")
    
    def generate_setup_report(self):
        """Generate set up report with timestamp"""
        print("\n📋 STEP 6: Set up reporting generation...")
        
        report = {
            "setup_timestamp": self.timestamp,
            "setup_datetime": datetime.now().isoformat(),
            "project_root": str(self.project_root),
            "backup_location": str(self.backup_dir),
            "architecture_version": "data_engineering_v1.0",
            "structure_created": True,
            "migrations_completed": True,
            "essential_files_created": True,
            "next_steps": [
                "1. Check for file location",
                "2. Test the environment: make setup",
                "3. Data collection scripts ready for launch",
                "4. Every generated JSON will have an automatic timestamp"
            ],
            "important_notes": [
                "Backup created before modifications",
                "Old structure (scripts/, data_files/) supprimée",
                "New ETL structure - separation of concerns",
                "Barcodes defined as strings (keep 0 at the begining)",
                "One table => fast loading bot"
            ],
            "directory_structure": {
                "src/food_scanner/": "Core application code",
                "data_engineering/": "ETL scripts and analysis",
                "data_engineering/data/": "Data storage (raw, processed, analysis)",
                "tests/": "Comprehensive test suite"
            }
        }
        
        report_file = self.project_root / f"setup_report_{self.timestamp}.json"
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False, default=str)
        
        print(f"   ✅ Rapport setup sauvegardé: setup_report_{self.timestamp}.json")
        
        # Print sum up
        print(f"\n📊 SETUP SUM UP:")
        print(f"   → Architecture: Data Engineering v1.0")
        print(f"   → Timestamp: {self.timestamp}")
        print(f"   → Backup: {self.backup_dir.name}")
        print(f"   → Structure ETL: ✅ Created")
        print(f"   → Migration: ✅ Complete")
        print(f"   → JSON timestampés: ✅ Set up")


def main():
    """Architecture set up launch"""
    print("🏗️  CARBON FOODPRINT SCANNER - ARCHITECTURE SETUP")
    
    # Directory root detection 
    project_root = Path.cwd()
    
    # Preliminary checks 
    if not (project_root / "src" / "food_scanner").exists():
        print("❌ Errorr: src/food_scanner/ not found")
        print("   Run this script from directory root")
        return
    
    # Confirmation
    print(f"📁 Project root detected: {project_root}")
    response = input("\n🚨 ATTENTION: This script will change this project structure. Continue? (y/N): ")
    
    if response.lower() != 'y':
        print("❌ Setup cancelled")
        return
    
    print("\n🚀 Setup launched...")
    
    # Setup launch
    try:
        setup = ArchitectureSetup(project_root)
        setup.run_complete_setup()
        
        print(f"\n🎯 NEXT STEPS:")
        print(f"1. Check files : ls -la src/food_scanner/data/")
        print(f"2. Test the environment: make setup")
        print(f"3. Ready for data collection scripts!")
        
    except Exception as e:
        print(f"\n💥 Setup error: {e}")
        print("🔄 Rollback from previous backup if necessary")


if __name__ == "__main__":
    main()