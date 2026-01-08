"""
Pytest configuration and shared fixtures for Tomorrow.io pipeline tests.

This file is automatically discovered by pytest and provides fixtures
that can be used across all test modules.
"""

import pytest
from datetime import datetime
from typing import Dict, Any


# =============================================================================
# HELPER FIXTURES (Validation utilities)
# =============================================================================


@pytest.fixture
def assert_valid_timestamp():
    """Helper fixture to validate ISO 8601 timestamp format."""

    def _validate(timestamp_str: str) -> bool:
        """Validate ISO 8601 timestamp string."""
        try:
            datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
            return True
        except ValueError:
            return False

    return _validate


@pytest.fixture
def assert_valid_location():
    """Helper fixture to validate location data structure."""

    def _validate(location: Dict[str, Any]) -> bool:
        """Validate location has required fields."""
        required_fields = {"id", "name", "lat", "lon"}
        return required_fields.issubset(location.keys())

    return _validate
