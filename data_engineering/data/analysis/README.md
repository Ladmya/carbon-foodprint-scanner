# Analysis Directory

This directory contains specific analysis files for tracking missing data and detailed statistics from the Carbon Foodprint data pipeline.

## 📁 Directory Structure

```
data_engineering/data/analysis/
├── 📊 extraction_phase/
│   ├── missing_co2_field/           # ✅ CO2 missing data tracking
│   │   ├── missing_co2_field_*.json
│   │   ├── missing_co2_field_summary_*.md
│   │   └── README.md
│   ├── missing_critical_fields/     # ✅ Critical fields missing data tracking
│   │   ├── missing_critical_fields_*.json
│   │   ├── missing_critical_fields_summary_*.md
│   │   └── README.md
│   └── README.md
├── 🚀 loading_phase/
│   ├── error_reports/               # ✅ Loading error tracking
│   │   ├── loading_error_report_*.json
│   │   ├── loading_error_summary_*.md
│   │   └── README.md
│   ├── loading_stats/               # ✅ Loading performance statistics
│   │   ├── loading_statistics_*.json
│   │   ├── loading_statistics_summary_*.md
│   │   └── README.md
│   └── README.md
├── 🔄 transformation_phase/
│   ├── transformation_stats/        # ✅ Transformation performance statistics
│   │   ├── transformation_statistics_*.json
│   │   ├── transformation_statistics_summary_*.md
│   │   └── README.md
│   └── README.md
└── 📋 README.md
```

## 🔍 Extraction Phase - Missing Data Tracking

### `missing_co2_field/`
**Purpose:** Track products missing CO2 data for analysis and improvement.

**Content:**
- `missing_co2_field_*.json` - Detailed list of products without CO2 data
- `missing_co2_field_summary_*.md` - Analysis of missing CO2 sources

**CO2 Sources Tracked:**
- `co2_total` - Main CO2 field
- `ecoscore_agribalyse_total` - Ecoscore CO2 data
- `agribalyse_total` - Agribalyse CO2 data

**Usage:**
```bash
# View missing CO2 data
cat data_engineering/data/analysis/extraction_phase/missing_co2_field/missing_co2_field_*.json
```

### `missing_critical_fields/`
**Purpose:** Track products missing critical fields for data quality improvement.

**Content:**
- `missing_critical_fields_*.json` - Detailed list of products with missing critical fields
- `missing_critical_fields_summary_*.md` - Analysis of missing field types

**Critical Fields Tracked:**
- `product_name` - Product name (CRITICAL)
- `brand_name` - Brand name (CRITICAL)
- `barcode` - Product barcode (CRITICAL)
- `weight` - Product weight (CRITICAL)

**Usage:**
```bash
# View missing critical fields
cat data_engineering/data/analysis/extraction_phase/missing_critical_fields/missing_critical_fields_*.json
```

## 🚀 Loading Phase - Error Tracking

### `error_reports/`
**Purpose:** Track database loading errors for debugging and improvement.

**Content:**
- `loading_error_report_*.json` - Detailed loading error reports
- `loading_error_summary_*.md` - Summary of error types and frequencies

**Error Types Tracked:**
- Database connection errors
- Constraint validation failures
- Duplicate key violations
- Data type conversion errors

**Usage:**
```bash
# View loading errors
cat data_engineering/data/analysis/loading_phase/error_reports/loading_error_report_*.json
```

### `loading_stats/`
**Purpose:** Track loading performance metrics for optimization.

**Content:**
- `loading_statistics_*.json` - Detailed loading performance metrics
- `loading_statistics_summary_*.md` - Performance summary and trends

**Metrics Tracked:**
- Insertion success rates
- Batch processing times
- Database connection performance
- Memory usage during loading

**Usage:**
```bash
# View loading statistics
cat data_engineering/data/analysis/loading_phase/loading_stats/loading_statistics_*.json
```

## 🔄 Transformation Phase - Performance Statistics

### `transformation_stats/`
**Purpose:** Track transformation performance and quality metrics.

**Content:**
- `transformation_statistics_*.json` - Detailed transformation metrics
- `transformation_statistics_summary_*.md` - Performance summary and trends

**Metrics Tracked:**
- Transformation success rates
- Field mapping accuracy
- Data type conversion results
- Business rule validation outcomes

**Usage:**
```bash
# View transformation statistics
cat data_engineering/data/analysis/transformation_phase/transformation_stats/transformation_statistics_*.json
```

## 📋 Analysis Workflow

### 1. Data Collection
```bash
# Collect raw data
python data_engineering/scripts/collection/complete_extraction_pipeline.py
```

### 2. Missing Data Analysis
```bash
# Analyze missing CO2 data
# Files generated in: data_engineering/data/analysis/extraction_phase/missing_co2_field/

# Analyze missing critical fields
# Files generated in: data_engineering/data/analysis/extraction_phase/missing_critical_fields/
```

### 3. Loading Analysis
```bash
# Track loading errors
# Files generated in: data_engineering/data/analysis/loading_phase/error_reports/

# Track loading performance
# Files generated in: data_engineering/data/analysis/loading_phase/loading_stats/
```

### 4. Transformation Analysis
```bash
# Track transformation performance
# Files generated in: data_engineering/data/analysis/transformation_phase/transformation_stats/
```

## 🔍 Data Quality Improvement

### Missing Data Tracking
- **CO2 Coverage:** Target >70% coverage
- **Critical Fields:** Target >95% completion
- **Error Rates:** Target <5% error rate

### Performance Optimization
- **Loading Speed:** Target <1000 products/minute
- **Transformation Success:** Target >90% success rate
- **Memory Usage:** Target <1GB peak usage

## 📊 Reporting Integration

### Integration with Reports
The analysis files in this directory complement the main reports in `data_engineering/data/reports/`:

- **Missing Data Analysis** → **Extraction Reports**
- **Loading Error Tracking** → **Loading Reports**
- **Transformation Statistics** → **Transformation Reports**

### Cross-Reference
- Analysis files provide detailed tracking
- Report files provide high-level summaries
- Both work together for complete data quality assessment

## 🛠️ Maintenance

### Cleanup Old Analysis
```bash
# Remove analysis files older than 30 days
find data_engineering/data/analysis/ -name "*.json" -mtime +30 -delete
find data_engineering/data/analysis/ -name "*.md" -mtime +30 -delete
```

### Archive Important Analysis
```bash
# Archive critical missing data analysis
mkdir -p data_engineering/data/analysis/archive/
mv data_engineering/data/analysis/extraction_phase/missing_* data_engineering/data/analysis/archive/
```

## 📞 Support

For questions about analysis files or data quality issues:
1. Check missing data analysis first
2. Review error reports for patterns
3. Consult performance statistics
4. Contact data engineering team

---

*Last updated: 2025-08-04*
*Analysis version: 2.0 - Organized missing data tracking*