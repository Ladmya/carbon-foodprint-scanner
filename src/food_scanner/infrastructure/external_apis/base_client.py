"""
src/food_scanner/infrastructure/external_apis/base_client.py
Abstract base class for external API clients.
"""

import os
import sys
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional, Dict, Any
import httpx
from httpx import AsyncClient

from ...core.constants import RATE_LIMITS


class BaseAPIClient(ABC):
    """Abstract base class for external API clients with common functionality."""
    
    def __init__(self, use_test_env: Optional[bool] = None):
        """
        Initialize the API client.
        
        Args:
            use_test_env: Override environment detection if specified
        """
        # Auto-detect environment if not specified
        if use_test_env is None:
            use_test_env = self._auto_detect_environment()
        
        self.use_test_env = use_test_env
        self._client: Optional[AsyncClient] = None
        
        # Rate limiting tracking
        self._last_requests = {}
        
    def _auto_detect_environment(self) -> bool:
        """Auto-detect if we should use test environment."""
        # 1. Check USE_TEST_API environment variable (highest priority) 
        use_test_api = os.getenv("USE_TEST_API", "").lower()
        if use_test_api == "true":
            return True
        elif use_test_api == "false":
            return False
            
        # 2. Check DB_ENVIRONMENT 
        db_environment = os.getenv("DB_ENVIRONMENT", "").lower()
        if db_environment == "test":
            return True
        elif db_environment == "production":
            return False
            
        # 3. Check if running in pytest context
        if self._is_running_tests():
            return True
            
        # 4. Default to production (safe default)
        return False
        
    def _is_running_tests(self) -> bool:
        """Detect if we're running in a test context."""
        return (
            "pytest" in sys.modules or
            "PYTEST_CURRENT_TEST" in os.environ or
            os.environ.get("TESTING", "").lower() == "true" or
            os.environ.get("USE_TEST_API", "").lower() == "true"
        )
    
    @property
    @abstractmethod
    def base_url(self) -> str:
        """Return the base URL for the API."""
        pass
    
    @property
    @abstractmethod
    def test_url(self) -> str:
        """Return the test URL for the API."""
        pass
    
    @property
    def current_url(self) -> str:
        """Return the current URL based on environment."""
        return self.test_url if self.use_test_env else self.base_url
    
    @property
    @abstractmethod
    def timeout(self) -> httpx.Timeout:
        """Return the timeout configuration."""
        pass
    
    @property
    @abstractmethod
    def headers(self) -> Dict[str, str]:
        """Return the default headers."""
        pass
    
    async def __aenter__(self):
        """Async context manager entry."""
        if self._client is None:
            self._client = AsyncClient(
                timeout=self.timeout,
                headers=self.headers,
                follow_redirects=True,
                limits=httpx.Limits(
                    max_keepalive_connections=5,
                    max_connections=10
                )
            )
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        if self._client:
            await self._client.aclose()
            self._client = None
    
    async def _make_request(self, method: str, url: str, **kwargs) -> httpx.Response:
        """
        Make HTTP request with error handling.
        
        Args:
            method: HTTP method
            url: Request URL
            **kwargs: Additional arguments for httpx
            
        Returns:
            HTTP Response
            
        Raises:
            RuntimeError: If client not initialized
            httpx.RequestError: For connection errors
            httpx.HTTPStatusError: For HTTP errors
        """
        if not self._client:
            raise RuntimeError("Client not initialized. Use async context manager.")
        
        try:
            response = await self._client.request(method, url, **kwargs)
            return response
            
        except httpx.TimeoutException as e:
            raise httpx.RequestError(f"Request timeout for {url}") from e
        except httpx.ConnectError as e:
            raise httpx.RequestError(f"Connection failed for {url}") from e
    
    async def _apply_rate_limit(self, request_type: str):
        """
        Apply rate limiting based on request type.
        
        Args:
            request_type: Type of request for rate limiting
        """
        import asyncio
        
        if request_type not in RATE_LIMITS:
            return
            
        now = datetime.now()
        rate_limit = RATE_LIMITS[request_type]
        
        if request_type in self._last_requests:
            time_since_last = (now - self._last_requests[request_type]).total_seconds()
            if time_since_last < rate_limit:
                sleep_time = rate_limit - time_since_last
                await asyncio.sleep(sleep_time)
        
        self._last_requests[request_type] = datetime.now()
    
    def get_client_info(self) -> Dict[str, Any]:
        """Get client configuration info for debugging."""
        return {
            "environment": "TEST" if self.use_test_env else "PRODUCTION",
            "base_url": self.base_url,
            "test_url": self.test_url,
            "current_url": self.current_url,
            "timeout": str(self.timeout),
            "headers": self.headers,
        }