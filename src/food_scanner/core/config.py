"""
src/food_scanner/core/config.py
"""

import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

""" Access the environment variables """

""" DATABASES   """

supabase_test_db_url : str = os.getenv('SUPABASE_TEST_DATABASE_URL')
supabase_test_db_anon_key : str = os.getenv('SUPABASE_TEST_DATABASE_ANNON_KEY')
supabase_test_db_service_key : str = os.getenv('SUPABASE_TEST_DATABASE_SERVICE_KEY')

supabase_prod_db_url : str = os.getenv('SUPABASE_PRODUCTION_DATABASE_URL')
supabase_prod_db_anon_key : str = os.getenv('SUPABASE_PRODUCTION_DATABASE_ANNON_KEY')
supabase_prod_db_service_key : str = os.getenv('SUPABASE_PRODUCTION_DATABASE_SERVICE_KEY')

#SQLAlchemy direct connection to Supabase

supabase_prod_db_password : str = os.getenv('SUPABASE_PRODUCTION_DATABASE_PASSWORD')
supabase_test_db_password : str = os.getenv('SUPABASE_TEST_DATABASE_PASSWORD')

production_database_name : str = os.getenv('PRODUCTION_DATABASE_NAME')
test_database_name : str = os.getenv('TEST_DATABASE_NAME')
db_environment = os.getenv("DB_ENVIRONMENT", "production")

# SERVICES 
telegram_test_bot_token : str = os.getenv('TELEGRAM_TEST_TOKEN')
telegram_prod_bot_token : str = os.getenv('TELEGRAM_PROD_TOKEN')

# OpenFoodFacts API Configuration (nouvelles variables)
use_test_api: str = os.getenv('USE_TEST_API', 'false')
openfoodfacts_api_key: str = os.getenv('OPENFOODFACTS_API_KEY', '')
openfoodfacts_user_agent: str = os.getenv('OPENFOODFACTS_USER_AGENT', 'FoodprintBot/1.0')

# Testing environment
test_environment: str = os.getenv('TESTING', 'false')
pytest_environment: str = os.getenv('PYTEST_CURRENT_TEST', '')

# Rate limiting overrides (optionnel)
rate_limit_product: str = os.getenv('RATE_LIMIT_PRODUCT', '')
rate_limit_search: str = os.getenv('RATE_LIMIT_SEARCH', '')