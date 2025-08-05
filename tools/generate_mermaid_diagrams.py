#!/usr/bin/env python3
"""
Carbon Foodprint Scanner - Mermaid Diagram Generator
Generate Mermaid diagrams for Markdown documentation
"""

import os
from typing import Dict

class MermaidDiagramGenerator:
    """Mermaid diagram generator for Carbon Foodprint Scanner"""
    
    def __init__(self):
        self.output_dir = "diagrams/mermaid"
        os.makedirs(self.output_dir, exist_ok=True)
    
    def create_data_flow_diagram(self):
        """Diagramme de flow pour les données en Mermaid"""
        mermaid_code = """```mermaid
flowchart TD
    A[OpenFoodFacts API] -->|Raw Data| B[ProductExtractor]
    B -->|Structured Data| C[ProductTransformer]
    C -->|Validated Data| D[Supabase Database]
    D -->|Product Queries| E[Telegram Bot]
    E -->|CO2 Results| D
    
    B -->|Cache| F[API Responses Cache]
    C -->|Reports| G[Quality Analysis Reports]
    
    style A fill:#FF6B6B
    style B fill:#4ECDC4
    style C fill:#4ECDC4
    style D fill:#96CEB4
    style E fill:#45B7D1
    style F fill:#lightgray
    style G fill:#lightgray
```"""
        
        with open(f"{self.output_dir}/data_flow_diagram.md", "w", encoding="utf-8") as f:
            f.write("# Carbon Foodprint Scanner - Data Flow\n\n")
            f.write(mermaid_code)
        
        print("✅ Diagramme de flow des données Mermaid généré")
        return mermaid_code
    
    def create_database_schema_diagram(self):
        """Database schema diagram for products table in Mermaid"""
        mermaid_code = """```mermaid
erDiagram
    products {
        TEXT barcode PK "8-18 digits"
        TEXT product_name "NOT NULL"
        TEXT brand_name "NOT NULL"
        JSONB brand_tags "Optional"
        DECIMAL weight "Normalized to grams"
        TEXT product_quantity_unit "g or ml only"
        CHAR nutriscore_grade "A-E or NULL"
        INTEGER nutriscore_score "-15 to 40 or NULL"
        CHAR eco_score "A-E or NULL"
        DECIMAL co2_total "CO2 per 100g"
        DECIMAL co2_vehicle_km "GENERATED"
        DECIMAL co2_train_km "GENERATED"
        DECIMAL co2_bus_km "GENERATED"
        DECIMAL co2_plane_km "GENERATED"
        DECIMAL total_co2_impact_grams "GENERATED"
        TEXT impact_level "GENERATED"
        TIMESTAMPTZ created_at "Auto"
        TIMESTAMPTZ updated_at "Auto"
        TIMESTAMPTZ cache_expires_at "Auto"
        TIMESTAMPTZ collection_timestamp "Optional"
        TEXT transformation_version "1.0"
        JSONB raw_data "Complete API response"
    }
```"""
        
        with open(f"{self.output_dir}/database_schema_diagram.md", "w", encoding="utf-8") as f:
            f.write("# Database Schema - Products Table\n\n")
            f.write(mermaid_code)
        
        print("✅ Database schema diagram generated")
        return mermaid_code
    
    def create_etl_pipeline_diagram(self):
        """Pipeline ETL with classes in Mermaid"""
        mermaid_code = """```mermaid
flowchart LR
    A[Point A: OpenFoodFacts API] -->|Raw API Data| B[EXTRACT<br/>ProductExtractor]
    B -->|Structured Data| C[TRANSFORM<br/>ProductTransformer]
    C -->|Validated Data| D[LOAD<br/>BatchLoader]
    D -->|Database Queries| E[Point B: Telegram Bot]
    
    B --> B1[OpenFoodFactsClient<br/>API Client]
    B --> B2[WeightParser<br/>Weight Normalization]
    
    C --> C1[DuplicateHandler<br/>Duplicate Detection]
    C --> C2[BrandNameCleaner<br/>Text Cleaning]
    
    D --> D1[DatabaseService<br/>DB Operations]
    E --> E1[MessageTemplates<br/>Bot Messages]
    E --> E2[BarcodeScanner<br/>Image Processing]
    
    style A fill:#FF6B6B
    style B fill:#4ECDC4
    style C fill:#4ECDC4
    style D fill:#4ECDC4
    style E fill:#45B7D1
    style B1 fill:#lightgray
    style B2 fill:#lightgray
    style C1 fill:#lightgray
    style C2 fill:#lightgray
    style D1 fill:#lightgray
    style E1 fill:#lightgray
    style E2 fill:#lightgray
```"""
        
        with open(f"{self.output_dir}/etl_pipeline_diagram.md", "w", encoding="utf-8") as f:
            f.write("# ETL Pipeline: OpenFoodFacts API → Telegram Bot\n\n")
            f.write(mermaid_code)
        
        print("✅ Pipeline ETL Mermaid généré")
        return mermaid_code
    
    def create_validation_schemas(self):
        """Validation schemas for each field in Mermaid"""
        
        validation_schemas = {
            'barcode': {
                'title': 'Barcode Validation',
                'rules': [
                    'Format Check: 8-18 digits only',
                    'Uniqueness: Primary key constraint',
                    'Presence: NOT NULL required'
                ],
                'fallback': 'No fallback - REJECT if invalid',
                'outcomes': ['ACCEPTED', 'REJECTED']
            },
            'product_name': {
                'title': 'Product Name Validation',
                'rules': [
                    'Presence: NOT NULL required',
                    'Length: Non-empty string',
                    'Language: French preferred'
                ],
                'fallback': 'product_name_fr → product_name → REJECT',
                'outcomes': ['ACCEPTED', 'REJECTED']
            },
            'brand_name': {
                'title': 'Brand Name Validation',
                'rules': [
                    'Presence: NOT NULL required',
                    'Length: Non-empty string',
                    'Cleaning: Remove special chars'
                ],
                'fallback': 'brands → brands_tags[0] → brands_imported → REJECT',
                'outcomes': ['ACCEPTED', 'REJECTED']
            },
            'weight': {
                'title': 'Weight Validation',
                'rules': [
                    'Presence: NOT NULL required',
                    'Range: 0 < weight ≤ 50000g',
                    'Normalization: Convert to grams'
                ],
                'fallback': 'Parse number → NULL if failed → REJECT',
                'outcomes': ['ACCEPTED', 'REJECTED']
            },
            'co2_total': {
                'title': 'CO2 Data Validation',
                'rules': [
                    'Presence: NOT NULL required',
                    'Range: 0 ≤ CO2 ≤ 10000',
                    'Sources: Multiple sources'
                ],
                'fallback': 'agribalyse → ecoscore_data → nutriments → REJECT',
                'outcomes': ['ACCEPTED', 'REJECTED']
            },
            'nutriscore': {
                'title': 'Nutriscore Validation',
                'rules': [
                    'Presence: At least one field',
                    'Grade Range: A-E only',
                    'Score Range: -15 to 40'
                ],
                'fallback': 'grade OR score required (REJECT if both missing)',
                'outcomes': ['ACCEPTED', 'REJECTED']
            }
        }
        
        for field_name, validation_info in validation_schemas.items():
            self._create_field_validation_diagram(field_name, validation_info)
    
    def _create_field_validation_diagram(self, field_name: str, validation_info: Dict):
        """Create a Mermaid validation diagram for a specific field"""
        
        # Generate complete documentation based on field type
        documentation = self._generate_field_documentation(field_name, validation_info)
        rules_text = "\n".join([f"    - {rule}" for rule in validation_info['rules']])
        
        mermaid_code = f"""```mermaid
flowchart TD
    A[Input Data] --> B{{Validation Rules}}
    B --> C[Rule 1: {validation_info['rules'][0]}]
    B --> D[Rule 2: {validation_info['rules'][1]}]
    B --> E[Rule 3: {validation_info['rules'][2]}]
    
    C --> F{{Pass?}}
    D --> G{{Pass?}}
    E --> H{{Pass?}}
    
    F -->|Yes| I[ACCEPTED]
    F -->|No| J[REJECTED]
    G -->|Yes| I
    G -->|No| J
    H -->|Yes| I
    H -->|No| J
    
    I --> K[Valid Data]
    J --> L[Invalid Data]
    
    M[Fallback Chain:<br/>{validation_info['fallback']}] --> B
    
    style I fill:#98D8C8
    style J fill:#DDA0DD
    style K fill:#98D8C8
    style L fill:#DDA0DD
    style M fill:#FFEAA7
```"""
        
        with open(f"{self.output_dir}/validation_schema_{field_name}.md", "w", encoding="utf-8") as f:
            f.write(documentation)
            f.write(f"\n## Rules\n{rules_text}\n\n")
            f.write(f"## Fallback Chain\n{validation_info['fallback']}\n\n")
            f.write(mermaid_code)
        
        print(f"✅ Schéma de validation Mermaid généré: {field_name}")
    
    def _generate_field_documentation(self, field_name: str, validation_info: Dict) -> str:
        """Generate detailed documentation for a validation field"""
        
        field_docs = {
            'barcode': {
                'overview': 'The barcode validation is critical as it serves as the unique primary key for identifying products.',
                'business_impact': 'Validation failure = product not processable',
                'valid_examples': ['3017620422003', '3560070468093', '3228857000906'],
                'invalid_examples': ['123', 'ABC123', '', '1234567890123456789'],
                'technical_details': 'Validation regex for 8-18 digits only'
            },
            'product_name': {
                'overview': 'The product name is essential for display in the Telegram bot.',
                'business_impact': 'Validation failure = product not displayable',
                'valid_examples': ['Chocolat noir', 'Bouchées truffées'],
                'invalid_examples': ['', '   ', None, '###'],
                'technical_details': 'Preference for French names'
            },
            'brand_name': {
                'overview': 'The brand name validation is critical for display in the Telegram bot.',
                'business_impact': 'Validation failure = brand not displayable',
                'valid_examples': ['Twix', 'Nestlé'],
                'invalid_examples': ['', '   ', None, '###'],
                'technical_details': 'Cleaning special characters and normalization'
            },
            'weight': {
                'overview': 'The weight is used for CO2 calculations per portion.',
                'business_impact': 'Validation failure = CO2 calculations impossible',
                'valid_examples': ['250g', '500ml', '1kg'],
                'invalid_examples': ['0g', '60000g', 'abc', ''],
                'technical_details': 'Conversion to grams, range check 0-50000g'
            },
            'co2_total': {
                'overview': 'The CO2 data is essential for environmental impact.',
                'business_impact': 'Validation failure = no CO2 impact displayed',
                'valid_examples': ['2.5', '15.8', '0.1'],
                'invalid_examples': ['-1', '15000', 'abc', ''],
                'technical_details': 'Range check 0-10000, multiple sources'
            },
            'nutriscore': {
                'overview': 'The Nutriscore helps with nutritional evaluation.',
                'business_impact': 'Validation failure = no nutritional score',
                'valid_examples': ['A', 'B', 'C', 'D', 'E'],
                'invalid_examples': ['F', 'Z', '1', ''],
                'technical_details': 'Grade A-E or score -15 to 40'
            }
        }
        
        doc = field_docs.get(field_name, {})
        
        content = f"# Validation Schema: {validation_info['title']}\n\n"
        content += f"## Overview\n{doc.get('overview', 'Validation critical for data quality.')}\n\n"
        content += f"## Business Impact\n{doc.get('business_impact', 'Validation failure impacts functionality.')}\n\n"
        
        content += "## Usage Cases\n\n"
        content += "### ✅ Valid Cases\n"
        for example in doc.get('valid_examples', []):
            content += f"- `{example}`\n"
        
        content += "\n### ❌ Invalid Cases\n"
        for example in doc.get('invalid_examples', []):
            content += f"- `{example}` → REJECT\n"
        
        content += f"\n## Technical Details\n{doc.get('technical_details', 'Validation according to the defined rules.')}\n\n"
        
        return content
    
    def create_comprehensive_documentation(self):
        """Create a complete documentation file with all diagrams"""
        
        content = """# Carbon Foodprint Scanner - Technical Documentation

## 📊 Architecture Diagrams

### 1. Data Flow

"""
        content += self.create_data_flow_diagram()
        
        content += """

### 2. Database Schema

"""
        content += self.create_database_schema_diagram()
        
        content += """

### 3. Pipeline ETL

"""
        content += self.create_etl_pipeline_diagram()
        
        content += """

## 🔍 Validation Schemas

### Barcode Validation

"""
        content += self._get_validation_diagram('barcode')
        
        content += """

### Product Name Validation (French preferred)

"""
        content += self._get_validation_diagram('product_name')
        
        content += """

### Brand Name Validation (Cleaning)

"""
        content += self._get_validation_diagram('brand_name')
        
        content += """

### Weight Validation

"""
        content += self._get_validation_diagram('weight')
        
        content += """

### CO2 Data Validation (Multiple sources)

"""
        content += self._get_validation_diagram('co2_total')
        
        content += """

### Nutriscore Validation (Grade A-E or score -15 to 40)

"""
        content += self._get_validation_diagram('nutriscore')
        
        content += """

    ## 📋 Usage

These Mermaid diagrams can be integrated into:
- Documentation (GitHub/GitLab)
- Wikis
- README.md
- Technical Documentation
- Presentations

## 🔄 Update

To regenerate all diagrams:
```bash
python tools/generate_mermaid_diagrams.py
```
"""
        
        with open(f"{self.output_dir}/complete_documentation.md", "w", encoding="utf-8") as f:
            f.write(content)
        
        print("✅ Complete Mermaid documentation generated")
    
    def _get_validation_diagram(self, field_name: str) -> str:
        """Retrieve the validation diagram for a field"""
        try:
            with open(f"{self.output_dir}/validation_schema_{field_name}.md", "r", encoding="utf-8") as f:
                content = f.read()
                # Extract only the mermaid part
                start = content.find("```mermaid")
                end = content.find("```", start + 10)
                if start != -1 and end != -1:
                    return content[start:end+3]
        except FileNotFoundError:
            return f"Validation diagram for {field_name} not found"
        return ""
    
    def generate_all_diagrams(self):
        """Generate all Mermaid diagrams"""
        print("🎨 Generating Mermaid diagrams for Carbon Foodprint Scanner...")
        
        # Generate all diagrams
        self.create_data_flow_diagram()
        self.create_database_schema_diagram()
        self.create_etl_pipeline_diagram()
        self.create_validation_schemas()
        self.create_comprehensive_documentation()
        
        print("\n🎉 All Mermaid diagrams have been generated in the 'diagrams/mermaid/' folder")
        print("📁 Created files:")
        print("   - data_flow_diagram.md")
        print("   - database_schema_diagram.md")
        print("   - etl_pipeline_diagram.md")
        print("   - validation_schema_*.md (one per field)")
        print("   - complete_documentation.md (complete documentation)")
        print("\n💡 You can now copy-paste these diagrams into your Markdown files!")

if __name__ == "__main__":
    generator = MermaidDiagramGenerator()
    generator.generate_all_diagrams() 