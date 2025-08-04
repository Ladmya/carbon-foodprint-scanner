"""
Pytest configuration and fixtures.
"""

import os
import pytest
import asyncio
from pathlib import Path
from datetime import datetime


@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(autouse=True)
def setup_test_environment():
    """Force test environment for all tests."""
    os.environ["USE_TEST_API"] = "true"
    os.environ["TESTING"] = "true"
    yield
    # Cleanup
    os.environ.pop("USE_TEST_API", None)
    os.environ.pop("TESTING", None)


@pytest.fixture
def test_output_dir():
    """Create and return test output directory."""
    base_dir = Path(__file__).resolve().parent
    output_dir = base_dir / "data_files/test_output"
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir


@pytest.fixture
def timestamp():
    """Return current timestamp for file naming."""
    return datetime.now().strftime("%Y%m%d_%H%M%S")


@pytest.fixture
def sample_barcode():
    """Return a sample barcode for testing."""
    return "3017624010701"  # Milka product


@pytest.fixture
def sample_brand():
    """Return a sample brand for testing."""
    return "milka"


@pytest.fixture
def chocolate_test_brands():
    """Return a subset of chocolate brands for testing."""
    return [
        "nutella",
        "kinder", 
        "milka",
        "lindt",
        "poulain"
    ]