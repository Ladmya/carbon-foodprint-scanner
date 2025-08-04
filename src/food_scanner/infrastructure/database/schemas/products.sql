-- ==================================================================
-- CARBON FOODPRINT SCANNER - FINAL TEST TABLE
-- All weights normalized to grams during extraction for accurate calculations
-- Fallback chain logic: REJECT → NOT NULL, NULL si échec → NULL OK
-- ==================================================================

-- Drop/ DELETE THE PRODUCTS TABLE if exists (be careful in production!)
/* DROP TABLE IF EXISTS products CASCADE; */

/*
-- Create main products table with fully corrected logic
CREATE TABLE products (
    
    -- 1. BARCODE (PRIMARY KEY - CRITICAL)
    -- Rule: Validation 8-18 digits → REJECT si invalide
    -- Fallback: NONE → NOT NULL (reject if invalid)
    barcode TEXT PRIMARY KEY NOT NULL
        CONSTRAINT barcode_format CHECK (
            LENGTH(barcode) BETWEEN 8 AND 18 AND 
            barcode ~ '^[0-9]+$'
        ),
    
    -- 2. PRODUCT_NAME (CRITICAL for display)
    -- Rule: product_name_fr → product_name → REJECT
    -- Fallback: FR→EN → NOT NULL (reject if both missing)
    product_name TEXT NOT NULL
        CONSTRAINT product_name_not_empty CHECK (LENGTH(TRIM(product_name)) > 0),
    
    -- 3. BRAND_NAME (CRITICAL for display)
    -- Rule: brands → brands_tags[0] → brands_imported → REJECT
    -- Fallback: Multiple sources → NOT NULL (reject if all missing)
    brand_name TEXT NOT NULL
        CONSTRAINT brand_name_not_empty CHECK (LENGTH(TRIM(brand_name)) > 0),
    
    -- 4. BRAND_TAGS (OPTIONAL)
    -- Rule: Allow NULL
    -- Fallback: NONE → NULL OK
    brand_tags JSONB DEFAULT NULL,
    
    -- 5. WEIGHT (CRITICAL - NORMALIZED TO GRAMS)
    -- Rule: Extract number → NULL si échec → CORRECTED TO NOT NULL
    -- CRITICAL: Required for accurate CO2 calculations
    -- NORMALIZED: All weights converted to grams during extraction
    -- 1kg → 1000, 500ml → 500, 250g → 250
    weight DECIMAL(10,3) NOT NULL
        CONSTRAINT weight_positive CHECK (weight > 0 AND weight <= 50000),
    
    -- 6. PRODUCT_QUANTITY_UNIT (CRITICAL - NORMALIZED)
    -- Rule: Extract unit → NULL si échec → CORRECTED TO NOT NULL
    -- CRITICAL: Required for verification, but normalized during extraction
    -- NORMALIZED: Only 'g' or 'ml' allowed (all converted during extraction)
    product_quantity_unit TEXT NOT NULL DEFAULT 'g'
        CONSTRAINT unit_normalized CHECK (product_quantity_unit IN ('g', 'ml')),
    
    -- 7. NUTRISCORE_GRADE (REQUIRED - one of grade/score)
    -- Rule: A-E only → Calculate si missing
    -- Fallback: Can be NULL if score exists
    nutriscore_grade CHAR(1) DEFAULT NULL
        CONSTRAINT nutriscore_grade_valid CHECK (
            nutriscore_grade IS NULL OR 
            nutriscore_grade IN ('A', 'B', 'C', 'D', 'E')
        ),
    
    -- 8. NUTRISCORE_SCORE (REQUIRED - one of grade/score)
    -- Rule: -15 to 40 → REJECT si both missing
    -- Fallback: Can be NULL if grade exists
    nutriscore_score INTEGER DEFAULT NULL
        CONSTRAINT nutriscore_score_range CHECK (
            nutriscore_score IS NULL OR 
            (nutriscore_score BETWEEN -15 AND 40)
        ),
    
    -- CONSTRAINT: At least one nutriscore field must exist (REJECT si both missing)
    CONSTRAINT nutriscore_required CHECK (
        nutriscore_grade IS NOT NULL OR nutriscore_score IS NOT NULL
    ),
    
    -- 9. ECO_SCORE (OPTIONAL)
    -- Rule: A-E only → NULL si missing
    -- Fallback: NONE → NULL OK (not critical)
    eco_score CHAR(1) DEFAULT NULL
        CONSTRAINT eco_score_valid CHECK (
            eco_score IS NULL OR 
            eco_score IN ('A', 'B', 'C', 'D', 'E')
        ),
    
    -- 10. CO2_TOTAL (CRITICAL - Main functionality)
    -- Rule: CRITICAL → REJECT si missing
    -- Fallback: agribalyse → ecoscore_data → nutriments → NOT NULL (reject if all missing)
    co2_total DECIMAL(10,3) NOT NULL
        CONSTRAINT co2_total_range CHECK (co2_total >= 0 AND co2_total <= 10000),
    
    -- 11-16. CALCULATED FIELDS (Auto-computed from co2_total + weight)
    -- SIMPLIFIED: All weights normalized to grams, calculations are straightforward
    -- Formula: (co2_total × weight ÷ 100) ÷ transport_factor
    -- Total CO2 impact = co2_per_100g × (product_weight_in_grams ÷ 100)
    
    co2_vehicle_km DECIMAL(10,3) GENERATED ALWAYS AS (
        ROUND(((co2_total * weight / 100.0) / 120.0)::numeric, 3)
    ) STORED,
    
    co2_train_km DECIMAL(10,3) GENERATED ALWAYS AS (
        ROUND(((co2_total * weight / 100.0) / 14.0)::numeric, 3)
    ) STORED,
    
    co2_bus_km DECIMAL(10,3) GENERATED ALWAYS AS (
        ROUND(((co2_total * weight / 100.0) / 68.0)::numeric, 3)
    ) STORED,
    
    co2_plane_km DECIMAL(10,3) GENERATED ALWAYS AS (
        ROUND(((co2_total * weight / 100.0) / 255.0)::numeric, 3)
    ) STORED,

    -- Total CO2 impact for the entire product (not per 100g)
    total_co2_impact_grams DECIMAL(10,3) GENERATED ALWAYS AS (
        ROUND((co2_total * weight / 100.0)::numeric, 3)
    ) STORED,
    
    -- Impact level based on TOTAL CO2 for the entire product
    -- Thresholds adjusted for product-level impact (not per 100g)
    impact_level TEXT GENERATED ALWAYS AS (
        CASE 
            WHEN (co2_total * weight / 100.0) <= 500 THEN 'LOW'        -- ≤500g total CO2
            WHEN (co2_total * weight / 100.0) <= 1500 THEN 'MEDIUM'    -- 501-1500g total CO2  
            WHEN (co2_total * weight / 100.0) <= 3000 THEN 'HIGH'      -- 1501-3000g total CO2
            ELSE 'VERY_HIGH'                                           -- >3000g total CO2
        END
    ) STORED,
    
    -- 17-22. SYSTEM METADATA (Automatic)
    created_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    cache_expires_at TIMESTAMPTZ DEFAULT (NOW() + INTERVAL '30 days') NOT NULL,
    collection_timestamp TIMESTAMPTZ,
    transformation_version TEXT DEFAULT '1.0',

    -- Complete API response backup (JSONB)
    raw_data JSONB DEFAULT NULL
);
*/
-- ==================================================================
-- INDEXES FOR FAST BOT QUERIES
-- ==================================================================
/*
-- Primary lookup by barcode (already indexed as PRIMARY KEY)

-- Bot queries by brand (frequent filter)
CREATE INDEX idx_products_brand_name ON products(brand_name);

-- Bot queries by impact level (meaningful with normalized weights)
CREATE INDEX idx_products_impact_level ON products(impact_level);

-- Bot queries by nutriscore
CREATE INDEX idx_products_nutriscore_grade ON products(nutriscore_grade);

-- Range queries for total CO2 impact (product-level)
CREATE INDEX idx_products_total_co2_impact ON products(total_co2_impact_grams);
CREATE INDEX idx_products_co2_intensity ON products(co2_total);

-- Queries by weight range (useful for portion analysis)
CREATE INDEX idx_products_weight ON products(weight);

-- Full-text search on product names
CREATE INDEX idx_products_product_name_search ON products 
    USING gin(to_tsvector('english', product_name));

-- Cache management
CREATE INDEX idx_products_cache_expires ON products(cache_expires_at);

-- Data management
CREATE INDEX idx_products_created_at ON products(created_at);
*/
-- ==================================================================
-- TRIGGERS FOR AUTOMATIC UPDATES
-- ==================================================================
/*
-- Function to update timestamp on modifications
CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger to update timestamp on any change
CREATE TRIGGER trigger_update_timestamp
    BEFORE UPDATE ON products
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at();
*/
-- ==================================================================
-- DATA VALIDATION FUNCTIONS
-- ==================================================================
/*
-- Function to validate product completeness with corrected fallback chain logic
CREATE OR REPLACE FUNCTION validate_product_completeness()
RETURNS TRIGGER AS $$
BEGIN
    -- Fields that end with REJECT in fallback chain = NOT NULL required
    
    IF NEW.barcode IS NULL OR LENGTH(TRIM(NEW.barcode)) = 0 THEN
        RAISE EXCEPTION 'Barcode is required (validation 8-18 digits → REJECT si invalide)';
    END IF;
    
    IF NEW.product_name IS NULL OR LENGTH(TRIM(NEW.product_name)) = 0 THEN
        RAISE EXCEPTION 'Product name is required (fallback: product_name_fr → product_name → REJECT)';
    END IF;
    
    IF NEW.brand_name IS NULL OR LENGTH(TRIM(NEW.brand_name)) = 0 THEN
        RAISE EXCEPTION 'Brand name is required (fallback: brands → brands_tags[0] → brands_imported → REJECT)';
    END IF;
    
    -- Weight is critical and must be normalized to grams
    IF NEW.weight IS NULL OR NEW.weight <= 0 THEN
        RAISE EXCEPTION 'Weight is required and must be normalized to grams during extraction';
    END IF;
    
    -- Unit is critical for verification (should be normalized)
    IF NEW.product_quantity_unit IS NULL OR NEW.product_quantity_unit NOT IN ('g', 'ml') THEN
        RAISE EXCEPTION 'Product quantity unit must be normalized to g or ml during extraction';
    END IF;
    
    IF NEW.co2_total IS NULL THEN
        RAISE EXCEPTION 'CO2 total is required (CRITICAL → REJECT si missing)';
    END IF;
    
    -- Nutriscore: At least one field required (REJECT si both missing)
    IF NEW.nutriscore_grade IS NULL AND NEW.nutriscore_score IS NULL THEN
        RAISE EXCEPTION 'At least one nutriscore field is required (REJECT si both missing)';
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger to validate completeness
CREATE TRIGGER trigger_validate_completeness
    BEFORE INSERT OR UPDATE ON products
    FOR EACH ROW
    EXECUTE FUNCTION validate_product_completeness();
*/
-- ==================================================================
-- MONITORING VIEWS FOR PRODUCTION
-- ==================================================================
/*
-- Real-time bot functionality assessment
CREATE VIEW bot_functionality_stats AS
SELECT 
    COUNT(*) as total_products,
    COUNT(*) as products_with_full_functionality,  -- All products have full functionality
    ROUND(AVG(co2_total), 2) as avg_co2_intensity,
    ROUND(AVG(total_co2_impact_grams), 2) as avg_total_co2_impact,
    ROUND(AVG(co2_vehicle_km), 2) as avg_car_equivalent_km,
    COUNT(CASE WHEN impact_level = 'LOW' THEN 1 END) as low_impact_products,
    COUNT(CASE WHEN impact_level = 'MEDIUM' THEN 1 END) as medium_impact_products,
    COUNT(CASE WHEN impact_level = 'HIGH' THEN 1 END) as high_impact_products,
    COUNT(CASE WHEN impact_level = 'VERY_HIGH' THEN 1 END) as very_high_impact_products
FROM products;

-- Brand quality distribution
CREATE VIEW brand_impact_analysis AS
SELECT 
    brand_name,
    COUNT(*) as product_count,
    ROUND(AVG(co2_total), 2) as avg_co2_intensity,
    ROUND(AVG(total_co2_impact_grams), 2) as avg_total_impact,
    ROUND(AVG(weight), 1) as avg_weight_grams,
    mode() WITHIN GROUP (ORDER BY impact_level) as most_common_impact_level
FROM products
GROUP BY brand_name
HAVING COUNT(*) >= 3  -- Only brands with 3+ products
ORDER BY avg_total_impact ASC;

-- Weight distribution analysis for portion insights
CREATE VIEW weight_distribution_analysis AS
SELECT 
    CASE 
        WHEN weight <= 100 THEN '0-100g (snack size)'
        WHEN weight <= 300 THEN '101-300g (individual)'
        WHEN weight <= 500 THEN '301-500g (family small)'
        WHEN weight <= 1000 THEN '501-1000g (family large)'
        ELSE '1000g+ (bulk)'
    END AS weight_category,
    COUNT(*) as product_count,
    ROUND(AVG(co2_total), 2) as avg_co2_intensity,
    ROUND(AVG(total_co2_impact_grams), 2) as avg_total_impact,
    ROUND(AVG(co2_vehicle_km), 2) as avg_car_km_equivalent
FROM products
GROUP BY 
    CASE 
        WHEN weight <= 100 THEN '0-100g (snack size)'
        WHEN weight <= 300 THEN '101-300g (individual)'
        WHEN weight <= 500 THEN '301-500g (family small)'
        WHEN weight <= 1000 THEN '501-1000g (family large)'
        ELSE '1000g+ (bulk)'
    END
ORDER BY AVG(weight);
*/




-- ==================================================================
-- EXAMPLE DATA INSERTIONS (with normalized weights)
-- ==================================================================
/*
-- Test 1: Nutella 400g jar (typical family size)
INSERT INTO products (
    barcode,
    product_name,
    brand_name,
    brand_tags,
    weight,                    -- NORMALIZED: 400g → 400
    product_quantity_unit,     -- NORMALIZED: always 'g' or 'ml'
    nutriscore_grade,
    nutriscore_score,
    co2_total,                -- per 100g
    raw_data
) VALUES (
    '3017620422003',
    'Nutella Pâte à tartiner aux noisettes et au cacao',
    'Nutella',
    '["nutella", "ferrero"]'::jsonb,
    400.0,                    -- 400g normalized
    'g',                      -- normalized unit
    'D',
    14,
    539.0,                    -- 539g CO2/100g
    '{"source": "openfoodfacts", "original_quantity": "400g"}'::jsonb
) ON CONFLICT (barcode) DO NOTHING;

-- Test 2: Nutella 1kg jar (for comparison)
INSERT INTO products (
    barcode,
    product_name,
    brand_name,
    weight,                    -- NORMALIZED: 1kg → 1000g
    product_quantity_unit,
    nutriscore_grade,
    nutriscore_score,
    co2_total,
    raw_data
) VALUES (
    '3017620422004',
    'Nutella Pâte à tartiner aux noisettes et au cacao 1kg',
    'Nutella',
    1000.0,                   -- 1kg → 1000g normalized
    'g',
    'D',
    14,
    539.0,                    -- Same CO2 intensity per 100g
    '{"source": "openfoodfacts", "original_quantity": "1kg"}'::jsonb
) ON CONFLICT (barcode) DO NOTHING;

-- Verify calculations are accurate
SELECT 
    barcode,
    product_name,
    weight || 'g' as weight_formatted,
    co2_total as co2_per_100g,
    -- Total CO2 for entire product
    total_co2_impact_grams,
    -- Transport equivalents (should be different for 400g vs 1kg)
    co2_vehicle_km || ' km' as car_equivalent,
    co2_train_km || ' km' as train_equivalent,
    impact_level,
    created_at,
    -- Manual verification
    ROUND((co2_total * weight / 100.0)::numeric, 1) as manual_total_co2_check,
    ROUND((co2_total * weight / 100.0 / 120.0)::numeric, 3) as manual_car_km_check    
FROM products 
WHERE barcode IN ('3017620422004')
ORDER BY weight;

-- Expected results for Nutella examples:
-- 52g:   280g total CO2,  2.3 km car,  LOW impact
-- 400g:  2156g total CO2, 18.0 km car, MEDIUM impact  
-- 1000g: 5390g total CO2, 44.9 km car, VERY_HIGH impact
*/

-- ==================================================================
-- EXTRACTION NORMALIZATION EXAMPLES
-- ==================================================================
/*
WEIGHT NORMALIZATION DURING EXTRACTION:

Input → Normalized Output:
"400g" → weight=400, unit="g"
"1.5kg" → weight=1500, unit="g"  
"500ml" → weight=500, unit="ml"
"2 × 250g" → weight=500, unit="g" (multiply)
"Famille 1L" → weight=1000, unit="ml"
"300 gr" → weight=300, unit="g"

REJECTION RULES:
- Cannot parse number → REJECT (weight required)
- Cannot determine unit → REJECT (unit required)
- Invalid barcode → REJECT
- No product name → REJECT  
- No brand → REJECT
- No CO2 data → REJECT
- No nutriscore → REJECT
*/

-- ==================================================================
-- BOT VALIDATION TRIGGER
-- ==================================================================
/*
CREATE OR REPLACE FUNCTION validate_bot_completeness()
RETURNS TRIGGER AS $$
BEGIN
    -- Validate that all required fields for full bot functionality are present
    
    IF NEW.barcode IS NULL OR LENGTH(TRIM(NEW.barcode)) = 0 THEN
        RAISE EXCEPTION 'Barcode required for product identification';
    END IF;
    
    IF NEW.product_name IS NULL OR LENGTH(TRIM(NEW.product_name)) = 0 THEN
        RAISE EXCEPTION 'Product name required for bot display';
    END IF;
    
    IF NEW.brand_name IS NULL OR LENGTH(TRIM(NEW.brand_name)) = 0 THEN
        RAISE EXCEPTION 'Brand name required for bot display';
    END IF;
    
    IF NEW.weight IS NULL OR NEW.weight <= 0 THEN
        RAISE EXCEPTION 'Weight required for transport equivalents calculation';
    END IF;
    
    IF NEW.co2_total IS NULL THEN
        RAISE EXCEPTION 'CO2 data required for impact calculations';
    END IF;
    
    IF NEW.nutriscore_grade IS NULL AND NEW.nutriscore_score IS NULL THEN
        RAISE EXCEPTION 'Nutriscore data required for health information';
    END IF;
    
    -- Success: product has all data needed for complete bot functionality
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_validate_bot_completeness
    BEFORE INSERT OR UPDATE ON products
    FOR EACH ROW
    EXECUTE FUNCTION validate_bot_completeness();
*/
-- ==================================================================
-- USEFUL QUERIES FOR TELEGRAM BOT
-- ==================================================================

-- 1. Primary bot query: Complete product information
/*
SELECT 
    barcode,
    product_name,
    brand_name,
    weight || product_quantity_unit as formatted_weight,
    nutriscore_grade,
    co2_total as co2_per_100g,
    total_co2_impact_grams,
    co2_vehicle_km,
    co2_train_km,
    co2_bus_km,
    co2_plane_km,
    impact_level,
    'COMPLETE_FUNCTIONALITY' as bot_feature_level
FROM products 
WHERE barcode = $1;
*/

-- 2. Find eco-friendly alternatives in same weight category
/*
SELECT 
    product_name,
    brand_name,
    weight,
    total_co2_impact_grams,
    co2_vehicle_km,
    impact_level
FROM products 
WHERE weight BETWEEN $1 * 0.8 AND $1 * 1.2  -- Similar weight range
  AND total_co2_impact_grams < $2              -- Lower impact than scanned product
ORDER BY total_co2_impact_grams ASC
LIMIT 5;
*/

-- 3. Brand comparison for same product type
/*
SELECT 
    brand_name,
    COUNT(*) as products_in_db,
    ROUND(AVG(total_co2_impact_grams), 1) as avg_impact,
    ROUND(AVG(co2_vehicle_km), 1) as avg_car_km,
    mode() WITHIN GROUP (ORDER BY impact_level) as typical_impact_level
FROM products 
WHERE product_name ILIKE '%chocolate%'
GROUP BY brand_name
ORDER BY avg_impact ASC;
*/

-- Main bot query: Get product info by barcode scan
/*
SELECT 
    barcode,
    product_name,
    brand_name,
    weight,
    product_quantity_unit,
    nutriscore_grade,
    co2_total,
    ROUND((co2_total * weight / 100.0)::numeric, 1) as total_co2_grams,
    co2_vehicle_km,
    co2_train_km,
    co2_bus_km,
    co2_plane_km,
    impact_level
FROM products 
WHERE barcode = $1;
*/

-- Compare products by CO2 impact per gram (intensity)
/*
SELECT 
    product_name,
    brand_name,
    weight,
    co2_total as co2_per_100g,
    ROUND((co2_total * weight / 100.0)::numeric, 1) as total_co2_grams,
    co2_vehicle_km,
    impact_level
FROM products 
ORDER BY co2_total ASC  -- Lowest carbon intensity first
LIMIT 10;
*/

-- Find products with similar weight but different impact
/*
SELECT 
    product_name,
    brand_name,
    weight,
    ROUND((co2_total * weight / 100.0)::numeric, 1) as total_co2_grams,
    co2_vehicle_km,
    impact_level
FROM products 
WHERE weight BETWEEN 350 AND 450  -- Similar to 400g
ORDER BY (co2_total * weight / 100.0) ASC;
*/

-- ==================================================================
-- ADMIN QUERIES
-- ==================================================================

-- Data quality overview
/*
SELECT 
    COUNT(*) as total_products,
    COUNT(nutriscore_grade) as with_grade,
    COUNT(nutriscore_score) as with_score,
    COUNT(eco_score) as with_ecoscore,
    ROUND(AVG(co2_total), 2) as avg_co2_per_100g,
    ROUND(AVG(co2_total * weight / 100.0), 2) as avg_total_co2,
    COUNT(CASE WHEN impact_level = 'HIGH' THEN 1 END) as high_impact_count,
    COUNT(CASE WHEN weight > 1000 THEN 1 END) as large_products
FROM products;
*/

-- Weight distribution analysis
/*
SELECT 
    CASE 
        WHEN weight <= 100 THEN '0-100g'
        WHEN weight <= 300 THEN '101-300g'
        WHEN weight <= 500 THEN '301-500g'
        WHEN weight <= 1000 THEN '501-1000g'
        ELSE '1000g+'
    END AS weight_category,
    COUNT(*) as count,
    ROUND(AVG(co2_total), 2) as avg_co2_intensity,
    ROUND(AVG(co2_total * weight / 100.0), 2) as avg_total_co2
FROM products
GROUP BY 
    CASE 
        WHEN weight <= 100 THEN '0-100g'
        WHEN weight <= 300 THEN '101-300g'
        WHEN weight <= 500 THEN '301-500g'
        WHEN weight <= 1000 THEN '501-1000g'
        ELSE '1000g+'
    END
ORDER BY 
    CASE 
        WHEN weight <= 100 THEN 1
        WHEN weight <= 300 THEN 2
        WHEN weight <= 500 THEN 3
        WHEN weight <= 1000 THEN 4
        ELSE 5
    END;
*/

-- ==================================================================
-- TEST QUERIES
-- ==================================================================
/*
    -- First 10 lines
SELECT * FROM products
LIMIT 10;

    -- Sort by Nutriscore or Co2
SELECT * FROM products
ORDER BY nutriscore_score, total_co2_impact_grams DESC
LIMIT 5;

    -- Fast stats request
SELECT COUNT(*) FROM products;

    -- Average co2 impact in grams
SELECT AVG(total_co2_impact_grams) AS avg_co2_impact FROM products;

    -- Maximum and minimum co2 impact in grams 

SELECT 
  MAX(total_co2_impact_grams) AS max_o2_impact,
  MIN(total_co2_impact_grams) AS min_o2_impact
FROM products;

    -- Search product by barcode
SELECT * FROM products
WHERE barcode = '3017620422003';

-- Products with maximum impact CO2 
SELECT product_name, total_co2_impact_grams
FROM products
WHERE total_co2_impact_grams = (
  SELECT MAX(total_co2_impact_grams) FROM products
)
LIMIT 1;

-- Products with minimum impact CO2 
SELECT product_name, total_co2_impact_grams
FROM products
WHERE total_co2_impact_grams = (
  SELECT MIN(total_co2_impact_grams) FROM products
)
LIMIT 1;

*/
