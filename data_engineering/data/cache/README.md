# Cache Directories

This directory contains temporary cache files used during data pipeline execution.

## 📁 Structure

### `api_responses/`
- **Purpose**: Temporary storage of OpenFoodFacts API responses
- **Content**: JSON files with API response data
- **Lifecycle**: Deleted after processing or expiration
- **Usage**: Reduces API calls and improves performance

### `calculations/`
- **Purpose**: Temporary storage of CO2 calculation results
- **Content**: Cached calculation results for carbon impact
- **Lifecycle**: Regenerated on each pipeline execution
- **Usage**: Speeds up repeated calculations

## 🔄 Cache Behavior

### **Temporary Nature**
- Files are created during pipeline execution
- Automatically cleaned up after processing
- Not committed to version control (in `.gitignore`)

### **Performance Benefits**
- Reduces API calls to OpenFoodFacts
- Speeds up repeated calculations
- Improves pipeline execution time

### **When Empty**
- Normal state when no pipeline has been executed recently
- Files are created only during active data processing
- Empty directories are maintained for pipeline structure

## 🚀 Usage

To populate cache directories, run a data pipeline:

```bash
# Run extraction pipeline
python data_engineering/scripts/collection/complete_extraction_pipeline.py

# Or run individual components
python -m food_scanner.data.extractors.product_extractor_final
```

## 📋 Cache Management

### **Automatic Cleanup**
- Cache files expire after 30 days
- Old files are automatically removed
- Storage is optimized for performance

### **Manual Cleanup**
```bash
# Clear all cache files
rm -rf data_engineering/data/cache/api_responses/*
rm -rf data_engineering/data/cache/calculations/*
```

### **Cache Configuration**
- Cache settings in `src/food_scanner/core/config.py`
- Expiration times configurable
- Storage paths customizable 