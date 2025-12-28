"""
Mock HTTP responses for Tomorrow.io API testing.

This module provides utilities to mock external API calls without
hitting the actual Tomorrow.io API during tests.
"""

import json
from pathlib import Path
from typing import Dict, Any, List
from unittest.mock import Mock
import responses


# Load sample data
FIXTURES_DIR = Path(__file__).parent
with open(FIXTURES_DIR / "sample_data.json") as f:
    SAMPLE_DATA = json.load(f)


def get_sample_intervals(count: int = 3) -> List[Dict[str, Any]]:
    """
    Get sample weather intervals.

    Args:
        count: Number of intervals to return

    Returns:
        List of weather interval dictionaries
    """
    intervals = SAMPLE_DATA["weather_intervals"]
    return intervals[:count]


def get_mock_api_success_response(
    intervals: List[Dict[str, Any]] | None = None,
) -> Dict[str, Any]:
    """
    Get mock successful API response.

    Args:
        intervals: Optional list of weather intervals. If None, uses sample data.

    Returns:
        Mock API response dictionary
    """
    if intervals is None:
        intervals = get_sample_intervals()

    response = SAMPLE_DATA["api_response_success"].copy()
    response["data"]["timelines"][0]["intervals"] = intervals

    return response


def get_mock_api_error_response(error_type: str = "rate_limit") -> Dict[str, Any]:
    """
    Get mock API error response.

    Args:
        error_type: Type of error ('rate_limit', 'invalid_location', etc.)

    Returns:
        Mock API error response dictionary
    """
    error_map = {
        "rate_limit": SAMPLE_DATA["api_response_rate_limit"],
        "invalid_location": SAMPLE_DATA["api_response_invalid_location"],
    }

    return error_map.get(error_type, SAMPLE_DATA["api_response_rate_limit"])


class MockTomorrowIOAPI:
    """
    Mock Tomorrow.io API for testing.

    This class uses the `responses` library to intercept HTTP requests
    and return mock data without hitting the real API.
    """

    BASE_URL = "https://api.tomorrow.io/v4"

    @staticmethod
    @responses.activate
    def mock_success_response(
        location_count: int = 2,
        interval_count: int = 3,
    ):
        """
        Mock successful API response for timeline requests.

        Args:
            location_count: Number of locations to simulate
            interval_count: Number of time intervals per location
        """
        mock_response = get_mock_api_success_response(
            get_sample_intervals(interval_count)
        )

        # Register mock response for POST requests to timelines endpoint
        responses.add(
            responses.POST,
            f"{MockTomorrowIOAPI.BASE_URL}/timelines",
            json=mock_response,
            status=200,
        )

    @staticmethod
    @responses.activate
    def mock_rate_limit_error():
        """Mock API rate limit error (429)."""
        error_response = get_mock_api_error_response("rate_limit")

        responses.add(
            responses.POST,
            f"{MockTomorrowIOAPI.BASE_URL}/timelines",
            json=error_response,
            status=429,
        )

    @staticmethod
    @responses.activate
    def mock_invalid_location_error():
        """Mock invalid location error (400)."""
        error_response = get_mock_api_error_response("invalid_location")

        responses.add(
            responses.POST,
            f"{MockTomorrowIOAPI.BASE_URL}/timelines",
            json=error_response,
            status=400,
        )


def create_mock_rest_client():
    """
    Create a mock REST client for DLT testing.

    Returns:
        Mock REST client object
    """
    mock_client = Mock()

    # Mock paginate method
    def mock_paginate(*args, **kwargs):
        """Mock pagination that returns sample intervals."""
        mock_response = Mock()
        mock_response.json.return_value = get_mock_api_success_response()
        yield mock_response

    mock_client.paginate = mock_paginate

    return mock_client
