# 🍫 Carbon Foodprint Scanner

A comprehensive data engineering project that scans product barcodes and calculates carbon footprint transport equivalents. Built with Python 3.12, featuring a modular ETL pipeline and Telegram bot interface.

## 🌟 Features

- **📱 Telegram Bot Interface** - Scan barcodes and get carbon footprint data instantly
- **🔍 Barcode Scanning** - Real-time product identification using camera
- **📊 Data Quality Analysis** - Comprehensive quality reports and missing data tracking
- **🏗️ Modular ETL Pipeline** - Separated Extract → Transform → Load architecture
- **📈 Production Reports** - Executive summaries and business insights
- **🔧 Automated Setup** - One-command environment validation and setup

## 🚀 Quick Start

### 1. **Environment Setup**

```bash
# Clone the repository
git clone https://github.com/Ladmya/carbon-foodprint-scanner.git
cd carbon-foodprint-scanner

# Create and activate virtual environment
python -m venv carbon_foodprint-venv
source carbon_foodprint-venv/bin/activate  # On Windows: carbon_foodprint-venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Setup and validate environment
make setup
```

### 2. **Data Collection & Analysis**

```bash
# Collect data from OpenFoodFacts API
make collect

# Analyze data quality
make analyze-raw

# Generate production reports
make reports
```

### 3. **Launch the Telegram Bot**

```bash
# Start the bot interface
make run-bot
```

## 🏗️ Architecture

### **Modular ETL Pipeline**
- **Extract**: `product_extractor_final.py` - Data extraction from OpenFoodFacts API
- **Transform**: `product_transformer_final.py` - Business rules and data validation
- **Load**: `load_products_to_supabase.py` - Database loading with error handling

### **Data Engineering Structure**
```
data_engineering/
├── scripts/               # Operational scripts
│   ├── setup/            # Environment setup and validation
│   ├── collection/       # Data collection orchestrator
│   ├── analysis/         # Data quality analysis
│   └── loading/          # Database loading
├── data/                 # Data storage
│   ├── raw/             # Raw data from APIs
│   ├── processed/       # Validated and enriched data
│   ├── analysis/        # Missing data tracking
│   └── reports/         # Production reports
└── notebooks/           # Analysis notebooks
```

### **Core Application**
```
src/food_scanner/
├── bot/                 # Telegram bot interface
├── data/               # ETL pipeline components
├── infrastructure/     # External APIs and database
└── api/               # REST API (future)
```

## 📊 Data Quality & Reports

### **Quality Analysis**
- **Raw Data Quality**: `analyze_raw_data_quality.py` - Analyzes extraction quality
- **Missing Data Tracking**: Tracks missing CO2 data and critical fields
- **Production Reports**: Executive summaries and business insights

### **Report Structure**
```
data_engineering/data/reports/
├── environment/         # Environment validation reports
├── extraction/         # Raw data quality reports
├── loading/           # Loading performance reports
├── transformation/    # Transformed data quality reports
├── quality/          # Overall quality reports
└── production/       # Production readiness reports
```

## 🔧 Available Commands

### **Data Operations**
```bash
make collect         # Run complete data collection pipeline
make analyze         # Run data quality analysis
make analyze-raw     # Analyze raw data quality specifically
make reports         # Generate production reports
make test-extraction # Test extraction component
make setup-data      # Setup data structure
make clean-data      # Clean cache and temporary files
```

### **Development**
```bash
make setup          # Validate environment
make setup-complete # Validate complete pipeline
make test           # Run tests (basic structure)
make test-unit      # Run unit tests (future)
make test-integration # Run integration tests (future)
make clean          # Clean temporary files
```

### **Bot Operations**
```bash
make run-bot        # Start Telegram bot
make test-bot       # Test bot functionality
```

## 🛠️ Technical Stack

### **Current Stack**
- **Python 3.12** - Main language
- **python-telegram-bot** - Telegram bot interface
- **pyzbar** - Barcode scanning
- **Supabase** - PostgreSQL database
- **OpenFoodFacts API** - Product data source
- **Pillow & OpenCV** - Image processing
- **httpx** - HTTP client for APIs

### **Future Stack**
- **FastAPI** - REST API framework
- **SQLAlchemy** - ORM
- **Pydantic** - Data validation
- **Datadog** - Monitoring
- **pytest** - Comprehensive test suite

## 📈 Data Pipeline

1. **Extract**: Collect product data from OpenFoodFacts API
2. **Transform**: Apply business rules and validate data quality
3. **Load**: Store validated data in Supabase database
4. **Analyze**: Generate quality reports and insights
5. **Interface**: Provide Telegram bot for user interaction

## 🌍 Environment Variables

Create a `.env` file with the following variables:

```bash
# Telegram Bot
TELEGRAM_BOT_TOKEN=your_bot_token

# Supabase Database
SUPABASE_TEST_DATABASE_URL=your_test_db_url
SUPABASE_TEST_DATABASE_SERVICE_KEY=your_test_service_key
SUPABASE_PRODUCTION_DATABASE_URL=your_prod_db_url
SUPABASE_PRODUCTION_DATABASE_SERVICE_KEY=your_prod_service_key

# Environment
DB_ENVIRONMENT=test
USE_TEST_API=true
```

## 📝 Weekly Workflow

1. **Setup**: `make setup` - Validate environment
2. **Collect**: `make collect` - Retrieve new products
3. **Analyze**: `make analyze-raw` - Check data quality
4. **Reports**: `make reports` - Generate insights
5. **Bot**: `make run-bot` - Launch user interface

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- **OpenFoodFacts** - Product database and API
- **Supabase** - Database infrastructure
- **Python Telegram Bot** - Bot framework
- **pyzbar** - Barcode scanning library

---

**Built with ❤️ for sustainable consumption awareness**

*Last updated: 2025-08-04*






