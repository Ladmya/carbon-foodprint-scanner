# 📋 Changelog

All notable changes to Carbon Foodprint Scanner will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Public repository setup with comprehensive documentation
- MIT License for open source distribution
- Contributing guidelines for community participation
- Changelog for version tracking

### Changed
- Updated README.md with new architecture documentation
- Improved .gitignore for public repository
- Enhanced Makefile commands for better usability

## [1.0.0] - 2025-08-04

### Added
- **🍫 Core Features**
  - Telegram bot interface for barcode scanning
  - Real-time carbon footprint calculation
  - Product data extraction from OpenFoodFacts API
  - Comprehensive data quality analysis

- **🏗️ Architecture**
  - Modular ETL pipeline (Extract → Transform → Load)
  - Data engineering scripts for collection and analysis
  - Supabase database integration
  - Automated environment validation

- **📊 Data Quality**
  - Raw data quality analysis (`analyze_raw_data_quality.py`)
  - Missing data tracking (CO2, critical fields)
  - Production reports generation
  - Comprehensive quality metrics

- **🔧 Development Tools**
  - Makefile automation for common tasks
  - Environment validation scripts
  - Basic test structure (future development)
  - Logging system with rotation

### Changed
- **Architecture Migration** (2025-08-04)
  - Reorganized data structure from `data/` to `data_engineering/data/`
  - Consolidated redundant report directories
  - Standardized naming conventions for reports
  - Updated all script paths and references

- **Reports Structure**
  - Created hierarchical report organization
  - Separated raw data vs transformed data reports
  - Added sub-folders for better organization
  - Implemented consistent naming patterns

- **Makefile Updates**
  - Updated all Makefiles to reflect new architecture
  - Added new commands for data analysis
  - Fixed obsolete script references
  - Enhanced help documentation

### Fixed
- **Environment Validation**
  - Fixed Pillow detection in `validate_environment.py`
  - Corrected path references in setup scripts
  - Updated directory validation for new structure

- **Data Analysis**
  - Fixed CO2 source detection logic
  - Corrected data loading for raw extraction format
  - Improved error handling in analysis scripts

### Technical Details
- **Python Version**: 3.12.1
- **Dependencies**: See `requirements.txt`
- **Database**: Supabase (PostgreSQL)
- **APIs**: OpenFoodFacts, Telegram Bot API
- **Image Processing**: Pillow, OpenCV, pyzbar

## [0.9.0] - 2025-07-30

### Added
- Initial project setup
- Basic ETL pipeline structure
- Telegram bot foundation
- Database schema design

### Changed
- Project architecture planning
- Data flow design
- Component separation

---

## 📝 Version History

### Version Naming Convention
- **Major.Minor.Patch** (e.g., 1.0.0)
- **Major**: Breaking changes or major features
- **Minor**: New features, backward compatible
- **Patch**: Bug fixes and minor improvements

### Release Schedule
- **Development**: Continuous integration
- **Beta**: Feature complete, testing phase
- **Release**: Stable, production-ready

---

## 🔗 Links

- [GitHub Repository](https://github.com/Ladmya/carbon-foodprint-scanner)
- [Documentation](README.md)
- [Contributing Guidelines](CONTRIBUTING.md)
- [License](LICENSE)

---

*Changelog maintained by the Carbon Foodprint Scanner team* 