# src/food_scanner/infrastructure/database/repositories/supabase_client.py

import os
from supabase import create_client, Client

from food_scanner.core.config import (
    supabase_prod_db_url,
    supabase_prod_db_service_key,
    supabase_test_db_url,
    supabase_test_db_service_key,
)

def get_supabase_client(environment: str = "test") -> Client:
    if environment == "prod":
        url = supabase_prod_db_url
        key = supabase_prod_db_service_key
    else:
        url = supabase_test_db_url
        key = supabase_test_db_service_key

    if not url or not key:
        raise ValueError("Missing Supabase credentials")

    return create_client(url, key)
