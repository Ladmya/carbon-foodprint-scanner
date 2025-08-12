"""
src/food_scanner/core/constants.py
Constants used throughout the application.
"""

# OpenFoodFacts API Constants
OPENFOODFACTS_CONFIG = {
    "base_url": "https://world.openfoodfacts.org",
    "test_url": "https://world.openfoodfacts.net",
    "timeout": 30.0,
    "connect_timeout": 10.0,
    "max_keepalive_connections": 5,
    "max_connections": 10,
}

# Rate limiting (per OpenFoodFacts documentation)
RATE_LIMITS = {
    "product": 1.0,    # 1 second between product calls (60/min)
    "search": 6.0,     # 6 seconds between search calls (10/min)
    "facet": 30.0,     # 30 seconds between facet calls (2/min) - not used
}

# Chocolate product categories for OpenFoodFacts API
CHOCOLATE_CATEGORIES = [
    # Main chocolate categories
    "en:chocolates",
    "en:milk-chocolates", 
    "en:dark-chocolates",
    "en:white-chocolates",
    "en:milk-chocolate-bar",

    # Cocoa/chocolate specific products
    "en:cocoa-and-its-products",
    "en:cacao-et-derives",  # French version

    # Filled chocolates/with ingredients
    "en:chocolates-with-hazelnuts",
    "en:milk-chocolates-with-hazelnuts",

    # Chocolate spreads
    "en:chocolate-spreads", 
    "en:cocoa-and-hazelnuts-spreads",

    # Chocolate BISCUITS (not all biscuits)
    "en:chocolate-biscuits",
    "en:milk-chocolate-biscuits",

    # Specific chocolate confectionery
    "en:chocolate-candies",
    "en:bars-covered-with-chocolate"
]

# Major chocolate brands in France (for testing and data collection)
MAJOR_CHOCOLATE_BRANDS_FRANCE = [
    "nutella",
    "kinder", 
    "ferrero-rocher",
    "milka",
    "cote-d-or",
    "oreo",
    "toblerone",
    "m&ms",
    "snickers",
    "twix",
    "bounty",
    "kit-kat",
    "smarties",
    "crunch",
    "lion",
    "lindt",
    "poulain",
    "suchard",
    "carambar",
    "cemoi",
    "alter-eco",
    "valrhona",
    "jeff-de-bruges",
    "leonidas",
    "ritter-sport",
]

# API Fields to retrieve from OpenFoodFacts
OPENFOODFACTS_FIELDS = [
    "code",
    "product_name",
    "product_name_fr",
    "brands",
    "brands_tags",
    "categories_tags",
    "nutriscore_grade",
    "nutrition_grades", 
    "nutriscore_score",
    "nutriments",
    "carbon_footprint_from_known_ingredients_debug",
    "ecoscore_data",
    "ecoscore_score",
    "quantity",
    "image_url",
    "allergens_tags",
    "countries_tags",
    "manufacturing_places_tags",
]

# Carbon footprint calculation constants
CARBON_FACTORS = {
    # Transportation CO2 equivalent (grams CO2 per km per person)  
    "car": 120,      # Average car
    "train": 14,     # High-speed train
    "bus": 68,       # City bus
    "plane": 255,    # Domestic flight
}

# Data quality thresholds
DATA_QUALITY_THRESHOLDS = {
    "min_barcode_length": 8,
    "max_barcode_length": 18,
    "required_fields": ["code", "product_name"],
    "carbon_max_reasonable": 10000,  # Max reasonable CO2 in grams
}

# Pagination defaults
PAGINATION_DEFAULTS = {
    "page_size": 100,
    "max_products_per_brand": 500,
    "max_page_size": 1000,  # API limit
}