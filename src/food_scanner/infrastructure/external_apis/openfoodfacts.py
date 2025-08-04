"""
src/food_scanner/infrastructure/external_apis/oopenfoodfacts.py
OpenFoodFacts API client for retrieving product data.
"""

import os
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any
import httpx
from httpx import HTTPStatusError, RequestError

from .base_client import BaseAPIClient
from ...core.constants import (
    OPENFOODFACTS_CONFIG,  # Public constants (URLs, defaults)
    CHOCOLATE_CATEGORIES,
    OPENFOODFACTS_FIELDS,
    PAGINATION_DEFAULTS,
    RATE_LIMITS
)

logger = logging.getLogger(__name__)


class OpenFoodFactsClient(BaseAPIClient):
    """
    Async client for OpenFoodFacts API with rate limiting and error handling.

    Rate limits (per OpenFoodFacts documentation):
    - Product queries: 100 req/min 
    - Search queries: 10 req/min
    - Facet queries: 2 req/min (not used)
    """
    
    def __init__(self, use_test_env: Optional[bool] = None):
        """Initialize OpenFoodFacts client."""
        # Base class handles all the auto-detection logic
        super().__init__(use_test_env=use_test_env)
        
        # Initialize rate limiting tracking (specific to OpenFoodFacts)
        self._last_requests = {}
    
    @property
    def base_url(self) -> str:
        return OPENFOODFACTS_CONFIG["base_url"]
    
    @property
    def test_url(self) -> str:
        return OPENFOODFACTS_CONFIG["test_url"]
    
    @property
    def timeout(self) -> httpx.Timeout:
        return httpx.Timeout(
            OPENFOODFACTS_CONFIG["timeout"], 
            connect=OPENFOODFACTS_CONFIG["connect_timeout"]
        )
    
    @property
    def headers(self) -> Dict[str, str]:
        """Return headers with configurable User-Agent."""
        user_agent = os.getenv("OPENFOODFACTS_USER_AGENT", "FoodprintBot/1.0")
        
        headers = {
            "User-Agent": user_agent,
            "Accept": "application/json",
            "Accept-Language": "en",
        }
        
        # Add API key if configured
        api_key = os.getenv("OPENFOODFACTS_API_KEY")
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"
            
        return headers

    def _get_rate_limit(self, request_type: str) -> float:
        """Get rate limit with environment variable override."""
        # Check for environment overrides first
        if request_type == "product":
            override = os.getenv("RATE_LIMIT_PRODUCT")
            if override:
                try:
                    return float(override)
                except ValueError:
                    pass
        elif request_type == "search":
            override = os.getenv("RATE_LIMIT_SEARCH")
            if override:
                try:
                    return float(override)
                except ValueError:
                    pass
        
        # Fall back to constants
        return RATE_LIMITS.get(request_type, 1.0)

    async def _apply_rate_limit(self, request_type: str):
        """Apply rate limiting with configurable limits."""
        import asyncio
        
        rate_limit = self._get_rate_limit(request_type)
        now = datetime.now()
        
        if request_type in self._last_requests:
            time_since_last = (now - self._last_requests[request_type]).total_seconds()
            if time_since_last < rate_limit:
                sleep_time = rate_limit - time_since_last
                logger.debug(f"Rate limiting: sleeping {sleep_time:.1f}s for {request_type} request")
                await asyncio.sleep(sleep_time)
        
        self._last_requests[request_type] = now

    async def get_product(
        self,
        barcode: str,
        use_test_env: Optional[bool] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Retrieve a single product by barcode.
        
        Args:
            barcode: Product barcode (EAN-13, UPC, etc...)
            use_test_env: Override environment detection if specified

        Returns:
            Product data dict or None if not found

        Raises:
            HTTPStatusError: For HTTP errors
            RequestError: For connection errors
        """
        if not barcode or len(barcode.strip()) < 8:
            logger.warning(f"Invalid barcode format: {barcode}")
            return None

        # Clean barcode
        clean_barcode = barcode.strip()

        # Apply rate limiting for product requests
        await self._apply_rate_limit('product')

        # Use instance environment if not overridden
        if use_test_env is None:
            use_test_env = self.use_test_env

        # Build URL
        base_url = self.test_url if use_test_env else self.base_url
        url = f"{base_url}/api/v2/product/{clean_barcode}.json"

        env_name = "TEST" if use_test_env else "PROD"
        logger.info(f"Fetching product: {clean_barcode} ({env_name})")

        try:
            response = await self._make_request("GET", url)

            if response.status_code == 200:
                data = response.json()

                # Check if product was found
                if data.get('status') == 1 and data.get('product'):
                    product_name = data['product'].get('product_name', 'Unknown')
                    logger.info(f"✅ Product found: {product_name}")
                    return data
                else:
                    logger.info(f"😢 Product not found: {clean_barcode}")
                    return None

            elif response.status_code == 404:
                logger.info(f"😭 Product not found: {clean_barcode}")
                return None

            else:
                logger.error(f"Unexpected status code {response.status_code} for barcode {clean_barcode}")
                response.raise_for_status()

        except HTTPStatusError as e:
            logger.error(f"HTTP error for barcode {clean_barcode}: {e.response.status_code}")
            raise
        except RequestError as e:
            logger.error(f"Request error for barcode {clean_barcode}: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error for barcode {clean_barcode}: {str(e)}")
            raise

    async def search_products(
        self,
        brand: str = "",
        categories: Optional[List[str]] = None,
        page: int = 1,
        page_size: int = PAGINATION_DEFAULTS["page_size"],
        use_test_env: Optional[bool] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Search products by brand and categories.

        Args:
            brand: Brand name to search for
            categories: Product categories filter (defaults to main chocolate category)
            page: Page number (1-based)
            page_size: Number of results per page (max 1000)
            use_test_env: Override environment detection if specified

        Returns:
            Search results dict with products list and pagination info
        """
        if not brand or len(brand.strip()) < 2:
            logger.warning(f"Invalid brand: {brand}")
            return None

        # Use main chocolate category if none provided, but allow empty list for no filter
        if categories is None:
            categories = ["en:chocolates"]  # Default to main chocolate category

        # Apply rate limiting for search requests
        await self._apply_rate_limit('search')

        # Use instance environment if not overridden
        if use_test_env is None:
            use_test_env = self.use_test_env

        # Build URL and parameters
        base_url = self.test_url if use_test_env else self.base_url
        url = f"{base_url}/api/v2/search"

        params = {
            "brands_tags": brand.lower().strip(),  # ✅ Correct parameter name
            "page": max(1, page),
            "page_size": min(PAGINATION_DEFAULTS["max_page_size"], max(1, page_size)),
            "fields": ",".join(OPENFOODFACTS_FIELDS)
        }
        
        # Add category filter only if categories provided and not empty
        if categories and len(categories) > 0:
            params["categories_tags"] = categories[0]  # Use main category only

        category_info = categories[0] if categories and len(categories) > 0 else "no-filter"
        env_name = "TEST" if use_test_env else "PROD"
        logger.info(f"Searching products: brand={brand}, category={category_info}, page={page} ({env_name})")

        try:
            response = await self._make_request("GET", url, params=params)

            if response.status_code == 200:
                data = response.json()

                products_count = data.get('count', 0)
                page_count = data.get('page_count', 0)
                products = data.get('products', [])

                logger.info(
                    f"✅ Search results: {len(products)} products "
                    f"(page {page}/{page_count}, total: {products_count})"
                )

                return data

            else:
                logger.error(f"Search failed with status {response.status_code}")
                response.raise_for_status()

        except HTTPStatusError as e:
            logger.error(f"HTTP error for search {brand}: {e.response.status_code}")
            raise
        except RequestError as e:
            logger.error(f"Request error for search {brand}: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error for search {brand}: {str(e)}")
            raise

    async def discover_brand_products(
        self,
        brand: str,
        max_products: int = PAGINATION_DEFAULTS["max_products_per_brand"],
        use_test_env: Optional[bool] = None
    ) -> List[Dict[str, Any]]:
        """
        Discover all products for a brand using pagination.

        Args:
            brand: Brand name
            max_products: Maximum number of products to retrieve
            use_test_env: Override environment detection if specified

        Returns:
            List of all products found for the brand
        """
        # Use instance environment if not overridden
        if use_test_env is None:
            use_test_env = self.use_test_env
            
        all_products = []
        page = 1
        page_size = PAGINATION_DEFAULTS["page_size"]

        env_name = "TEST" if use_test_env else "PROD"
        logger.info(f"🔍 Discovering products for brand: {brand} (max: {max_products}) ({env_name})")

        while len(all_products) < max_products:
            try:
                search_result = await self.search_products(
                    brand=brand,
                    page=page,
                    page_size=page_size,
                    use_test_env=use_test_env
                )

                if not search_result or not search_result.get('products'):
                    logger.info(f"No more products found for {brand} at page {page}")
                    break

                products = search_result['products']
                all_products.extend(products)

                # Check if we've reached the last page
                if page >= search_result.get('page_count', 1):
                    logger.info(f"Reached last page ({page}) for brand {brand}")
                    break

                page += 1

                # Respect max_products limit
                if len(all_products) >= max_products:
                    all_products = all_products[:max_products]
                    logger.info(f"Reached max_products limit ({max_products}) for brand {brand}")
                    break

            except Exception as e:
                logger.error(f"Error discovering products for {brand} at page {page}: {str(e)}")
                break

        logger.info(f"✅ Discovery complete for {brand}: {len(all_products)} products found")
        return all_products

    async def get_multiple_products(
        self,
        barcodes: List[str],
        use_test_env: Optional[bool] = None
    ) -> Dict[str, Optional[Dict[str, Any]]]:
        """
        Retrieve multiple products by barcode with rate limiting.

        Args:
            barcodes: List of product barcodes
            use_test_env: Override environment detection if specified

        Returns:
            Dict mapping barcode -> product data (or None if not found)
        """
        # Use instance environment if not overridden
        if use_test_env is None:
            use_test_env = self.use_test_env
            
        results = {}

        env_name = "TEST" if use_test_env else "PROD"
        logger.info(f"Fetching {len(barcodes)} products... ({env_name})")

        for i, barcode in enumerate(barcodes):
            try:
                product = await self.get_product(barcode, use_test_env)
                results[barcode] = product

                # Progress logging
                if (i + 1) % 10 == 0:
                    logger.info(f"Progress: {i + 1}/{len(barcodes)} products fetched")

            except Exception as e:
                logger.error(f"Failed to fetch product {barcode}: {str(e)}")
                results[barcode] = None

        successful = sum(1 for v in results.values() if v is not None)
        logger.info(f"✅ Batch complete: {successful}/{len(barcodes)} products retrieved")

        return results

    # Convenience methods for testing
    async def get_product_test(self, barcode: str) -> Optional[Dict[str, Any]]:
        """Convenience method to force test environment for get_product."""
        return await self.get_product(barcode, use_test_env=True)
    
    async def search_products_test(
        self,
        brand: str = "",
        categories: Optional[List[str]] = None,
        page: int = 1,
        page_size: int = PAGINATION_DEFAULTS["page_size"]
    ) -> Optional[Dict[str, Any]]:
        """Convenience method to force test environment for search_products."""
        return await self.search_products(
            brand=brand,
            categories=categories,
            page=page,
            page_size=page_size,
            use_test_env=True
        )
    
    async def discover_brand_products_test(
        self,
        brand: str,
        max_products: int = PAGINATION_DEFAULTS["max_products_per_brand"]
    ) -> List[Dict[str, Any]]:
        """Convenience method to force test environment for discover_brand_products."""
        return await self.discover_brand_products(
            brand=brand,
            max_products=max_products,
            use_test_env=True
        )