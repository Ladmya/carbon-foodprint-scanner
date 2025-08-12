# Extractors Module Architecture

## Overview

The `extractors` module is the core data processing component of the Carbon Foodprint system. It handles the complete pipeline from product discovery to data analysis and reporting, following a modular architecture based on the Single Responsibility Principle (SRP).

## Architecture Principles

- **Single Responsibility Principle**: Each class has one clear responsibility
- **Separation of Concerns**: Clear distinction between discovery, enrichment, extraction, analysis, and reporting
- **Modular Design**: Independent components that can be tested and maintained separately
- **Async-First**: Built for non-blocking I/O operations with external APIs
- **Test-Driven**: Comprehensive test suite for each component

## Architectural Patterns

### Hybrid Architecture Approach

Our system implements a **hybrid architecture** that combines multiple design patterns:

#### 1. Data Processing Domains
**Specialized Processing Modules** follow domain separation principles:
- **ProductDiscovery Domain**: Handles product search and discovery via API calls
- **ProductEnrichment Domain**: Manages data enrichment via API calls
- **ProductFieldExtraction Domain**: Implements pure field extraction (no transformation)
- **ExtractionAnalyzer Domain**: Orchestrates data analysis and metrics
- **ExtractionReporter Domain**: Manages report generation and formatting

Each domain module encapsulates:
- **API interaction logic**: Product discovery and data enrichment via external APIs
- **Data extraction logic**: Pure field extraction and parsing (no transformation)
- **Data analysis logic**: Metrics calculation and quality assessment
- **Pipeline coordination**: Sequential data flow between stages
- **Error handling**: Basic error management and fallbacks

#### 2. Pipeline Pattern
**Orchestration Layer** implements the Pipeline pattern:
- **Sequential Processing**: Discovery → Enrichment → Extraction → Analysis → Reporting
- **Data Flow**: Each stage processes data and passes it to the next
- **API Layer**: Discovery and Enrichment stages handle external API interactions
- **Extraction Layer**: Field extraction stage performs pure data extraction (no transformation)
- **Analysis Layer**: Analysis stage processes extracted data for insights
- **Reporting Layer**: Reporting stage formats and outputs results

#### 3. Command Pattern (Partial)
**Component Independence** allows Command-like execution:
- Components can be executed independently
- Each component maintains its own state and statistics
- Components can be composed in different ways
- Supports both pipeline and standalone execution modes

### Pattern Integration Benefits

- **Flexibility**: Can run as a complete pipeline OR as individual components
- **Testability**: Each domain can be tested in isolation
- **Maintainability**: Business logic is separated from orchestration logic
- **Scalability**: Components can be scaled independently
- **Reusability**: Domain modules can be used in different contexts

### Trade-offs and Considerations

#### Advantages of Hybrid Approach
- **Best of Both Worlds**: Combines DDD's business clarity with Pipeline's process clarity
- **Adaptability**: Can evolve from pipeline to event-driven or microservices
- **Incremental Development**: Can develop and test domains independently
- **Mixed Execution Modes**: Supports both batch processing and real-time operations

#### Potential Challenges
- **Complexity**: Multiple patterns can increase cognitive load
- **Pattern Boundaries**: Need clear rules for when to use which pattern
- **Testing Complexity**: Need to test both individual domains and pipeline integration
- **Documentation Overhead**: More complex to document and explain to new developers

#### When This Architecture Makes Sense
- **Data Processing Pipelines**: When you have sequential data transformation stages
- **Modular Development**: When you want to develop and test components independently
- **Team Structure**: When different teams can own different processing stages
- **Evolution Path**: When you need flexibility to change orchestration patterns later

## Folder Structure

```
src/food_scanner/data/extractors/
├── __init__.py
├── extraction_orchestrator.py          # Main orchestrator (refactored from product_extractor_final.py)
├── discovery/
│   ├── __init__.py
│   └── product_discovery.py            # Product discovery logic - API calls
├── enrichment/
│   ├── __init__.py
│   └── product_enrichment.py          # Product enrichment logic - API calls
├── field_extraction/
│   ├── __init__.py
│   ├── product_field_extraction.py    # Field extraction logic
│   └── weight_parser.py               # Weight parsing domain logic
├── analysis/
│   ├── __init__.py
│   ├── extraction_analyzer.py         # Data analysis orchestration
│   └── analyzers/                     # Specialized analyzers
│       ├── __init__.py
│       ├── base_field_analyzer.py
│       ├── barcode_analyzer.py
│       ├── co2_analyzer.py
│       ├── comprehensive_analyzer.py
│       ├── nutriscore_analyzer.py
│       └── text_field_analyzer.py
├── reporting/
│   ├── __init__.py
│   └── extraction_reporter.py         # Report generation and formatting
└── tests/
    ├── __init__.py
    └── extraction_orchestrator_tests.py # Comprehensive test suite
```

## Component Responsibilities

### 1. ExtractionOrchestrator (Main Orchestrator)

**File**: `extraction_orchestrator.py`  
**Responsibility**: Coordinates the entire extraction pipeline

**Key Features**:
- Manages component lifecycle and initialization
- Orchestrates the flow between discovery, enrichment, and extraction phases
- Tracks pipeline statistics and performance metrics
- Provides component status monitoring
- Handles async context management for resource cleanup

**Methods**:
- `run_complete_extraction()`: Main pipeline execution
- `get_pipeline_stats()`: Retrieve pipeline metrics
- `reset_pipeline_stats()`: Reset performance counters
- `get_component_status()`: Check component readiness

### 2. ProductDiscovery

**File**: `discovery/product_discovery.py`  
**Responsibility**: Discovers products by given brands and given categories in constants.py file

**Key Features**:
- Searches OpenFoodFacts API for products by brand/category combinations
- Implements deduplication to avoid processing the same product multiple times in different brands and categories
- Tracks discovery statistics (API calls, products found, etc.)
- Configurable limits per brand and category

**Methods**:
- `discover_products_by_brands()`: Main discovery method
- `get_discovery_stats()`: Retrieve discovery metrics
- `reset_stats()`: Reset discovery counters

### 3. ProductEnrichment

**File**: `enrichment/product_enrichment.py`  
**Responsibility**: Enriches discovered products with detailed information

**Key Features**:
- Fetches complete product data from OpenFoodFacts API
- Handles API rate limiting and error recovery
- Tracks enrichment success rates and API call statistics
- Provides detailed enrichment metrics

**Methods**:
- `enrich_discovered_products()`: Main enrichment method
- Tracks enrichment statistics within the results dictionary

### 4. ProductFieldExtraction

**File**: `field_extraction/product_field_extraction.py`  
**Responsibility**: Extracts and parses specific and critical fields from enriched products

**Key Features**:
- Extracts barcode, product names, brand names, brand tags, weights, nutriscore grade & score, ecoscore grade, and CO2 sources
- Implements priority-based field extraction logic
- Uses specialized parsers (e.g., WeightParser) for complex fields
- Handles missing or malformed data gracefully (fallback to None)

**Methods**:
- `extract_all_fields()`: Main extraction orchestrator
- `extract_product_name()`: Product name extraction
- `extract_brand_name()`: Brand name extraction
- `extract_weight()`: Weight and unit extraction
- `extract_all_co2_sources()`: CO2 emission source extraction

### 5. ExtractionAnalyzer

**File**: `analysis/extraction_analyzer.py`  
**Responsibility**: Analyzes extraction results and generates insights

**Key Features**:
- Orchestrates specialized field analyzers
- Calculates overall data quality metrics
- Assesses production readiness
- Identifies critical issues and improvement priorities
- Generates comprehensive analysis results

**Data Structures**:
- `ExtractionAnalysisResults`: Dataclass containing all analysis outputs

**Methods**:
- `analyze_extraction_results()`: Main analysis method
- Integrates results from specialized analyzers

### 6. ExtractionReporter

**File**: `reporting/extraction_reporter.py`  
**Responsibility**: Generates formatted reports from analysis results

**Key Features**:
- Creates multiple report formats (JSON, Markdown, CSV)
- Organizes reports by type and timestamp
- Handles file I/O and directory management
- Provides configurable output directories

**Methods**:
- `generate_all_reports()`: Generate all report types
- `generate_json_report()`: JSON format reports
- `generate_markdown_report()`: Markdown format reports

### 7. Specialized Analyzers

**Location**: `analysis/analyzers/`  
**Responsibility**: Perform specific field analysis tasks

**Analyzers**:
- `BarcodeAnalyzer`: Barcode format and validity analysis
- `CO2Analyzer`: CO2 emission data analysis
- `ComprehensiveAnalyzer`: Overall data completeness analysis
- `NutriscoreAnalyzer`: Nutritional score analysis
- `TextFieldAnalyzer`: Text field quality analysis

## Data Flow

```
1. Discovery Phase
   Brands/Categories → ProductDiscovery → Barcode List

2. Enrichment Phase
   Barcode List → ProductEnrichment → Enriched Product Data

3. Field Extraction Phase
   Enriched Data → ProductFieldExtraction → Structured Fields

4. Analysis Phase
   Structured Fields → ExtractionAnalyzer → Analysis Results

5. Reporting Phase
   Analysis Results → ExtractionReporter → Formatted Reports
```

## Key Changes Implemented

### 1. Refactoring from Monolithic to Modular

**Before**: Single `product_extractor_final.py` file containing all logic  
**After**: Modular architecture with clear separation of concerns

**Benefits**:
- Easier maintenance and debugging
- Better testability
- Clearer code organization
- Reduced cognitive load per file

### 2. Component Separation

**Extracted Components**:
- `ProductDiscovery`: Handles product search and discovery
- `ProductEnrichment`: Manages data enrichment from APIs
- `ProductFieldExtraction`: Processes and extracts specific fields
- `ExtractionAnalyzer`: Orchestrates data analysis
- `ExtractionReporter`: Generates formatted reports

### 3. Test Infrastructure

**New Test Structure**:
- Dedicated `tests/` directory within extractors
- Comprehensive test suite for `ExtractionOrchestrator`
- Automatic test execution on orchestrator launch
- Individual component testing capabilities

**Test Features**:
- Component creation and initialization tests
- Discovery phase validation
- Pipeline statistics management
- Component integration verification

### 4. Import Management

**Standardized Import Strategy**:
- Uses `sys.path.insert` with `pathlib` for standalone execution
- Relative imports for package-based usage
- Consistent import patterns across all components

### 5. Async Context Management

**Resource Management**:
- Proper async context manager implementation
- Automatic resource cleanup
- Client lifecycle management

## Usage Examples

### Running the Complete Pipeline

```python
async with ExtractionOrchestrator(use_test_env=True) as orchestrator:
    results = await orchestrator.run_complete_extraction(
        brands=["Lindt", "Ferrero"],
        categories=["chocolates"],
        max_products=100,
        generate_reports=True
    )
```

### Running Individual Components

```python
# Discovery only
async with ExtractionOrchestrator(use_test_env=True) as orchestrator:
    discovery_results = await orchestrator.product_discovery.discover_products_by_brands(
        brands=["Lindt"],
        max_products_per_brand=10
    )

# Analysis only
analyzer = ExtractionAnalyzer()
analysis_results = analyzer.analyze_extraction_results(
    extracted_products, extraction_stats, pipeline_stats
)
```

### Testing

```bash
# Run complete test suite
python src/food_scanner/data/extractors/extraction_orchestrator.py

# Run individual component tests
python src/food_scanner/data/extractors/discovery/product_discovery.py
python src/food_scanner/data/extractors/analysis/extraction_analyzer.py
python src/food_scanner/data/extractors/reporting/extraction_reporter.py
```

## Performance Considerations
- **Async processing** to handle multiple brand/category combinations concurrently
- **Elimination of redundant API calls** through intelligent search strategies

### Separation of Performance Concerns
**Extraction Layer** focuses on:
- In-memory deduplication during discovery
- Efficient field extraction algorithms
- Async API interaction patterns
- Memory-conscious data structures

**Other Layers** handle:
- **Transformation**: Caching, smart deduplication, retry logic, and more
- **Infrastructure**: Rate limiting, error handling, connection pooling
- **Loading**: Database optimization, batch processing

## Future Enhancements

### Planned Improvements (when adding more families of products)
1. **Parallel Processing**: Implement concurrent API calls for better performance
2. **Advanced Caching**: Redis-based caching for distributed deployments and deduplication
3. **Metrics Dashboard**: Real-time monitoring of pipeline performance

### Scalability Features
- Horizontal scaling support for multiple orchestrator instances
- Database-backed state persistence for long-running pipelines
- Event-driven architecture for pipeline monitoring

## Conclusion

The refactored extractors module represents a significant improvement in code organization, maintainability, and testability. The modular architecture makes it easier to:

- Debug specific components
- Add new functionality
- Maintain existing code
- Test individual components
- Scale the system

The separation of concerns ensures that each component has a clear, single responsibility, making the codebase more maintainable and easier to understand for new developers joining the project.
