"""
Pytest configuration and shared fixtures for Tomorrow.io pipeline tests.

This file is automatically discovered by pytest and provides fixtures
that can be used across all test modules.
"""

import pytest
import tempfile
import shutil
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, Any, List
import duckdb
import dlt


# =============================================================================
# TEMPORAL FIXTURES
# =============================================================================


@pytest.fixture
def fixed_datetime():
    """Fixed datetime for deterministic testing."""
    return datetime(2025, 12, 23, 13, 0, 0, tzinfo=timezone.utc)


@pytest.fixture
def normalized_datetime(fixed_datetime):
    """Normalized datetime (floored to hour boundary)."""
    return fixed_datetime.replace(minute=0, second=0, microsecond=0)


# =============================================================================
# LOCATION FIXTURES
# =============================================================================


@pytest.fixture
def sample_locations():
    """Sample location data for testing."""
    return [
        {"id": 1, "name": "Test Location 1", "lat": 25.90, "lon": -97.50},
        {"id": 2, "name": "Test Location 2", "lat": 26.00, "lon": -97.60},
    ]


@pytest.fixture
def single_location(sample_locations):
    """Single location for focused testing."""
    return sample_locations[0]


# =============================================================================
# WEATHER DATA FIXTURES
# =============================================================================


@pytest.fixture
def sample_weather_values():
    """Sample weather measurement values."""
    return {
        "temperature": 25.5,
        "temperatureApparent": 24.8,
        "humidity": 65,
        "windSpeed": 5.2,
        "windDirection": 180,
        "precipitationIntensity": 0,
        "precipitationProbability": 10,
        "precipitationType": 0,
        "weatherCode": 1000,
        "cloudCover": 25,
        "pressureSurfaceLevel": 1013.25,
        "visibility": 10.0,
    }


@pytest.fixture
def sample_weather_interval(sample_weather_values):
    """Sample weather interval (one hour of data)."""
    return {
        "startTime": "2025-12-23T13:00:00Z",
        "values": sample_weather_values,
    }


@pytest.fixture
def sample_weather_intervals(sample_weather_values):
    """Multiple weather intervals for testing."""
    return [
        {
            "startTime": "2025-12-23T13:00:00Z",
            "values": sample_weather_values,
        },
        {
            "startTime": "2025-12-23T14:00:00Z",
            "values": {**sample_weather_values, "temperature": 26.0},
        },
        {
            "startTime": "2025-12-23T15:00:00Z",
            "values": {**sample_weather_values, "temperature": 26.5},
        },
    ]


# =============================================================================
# API RESPONSE FIXTURES
# =============================================================================


@pytest.fixture
def mock_api_response(sample_weather_intervals):
    """Mock Tomorrow.io API response structure."""
    return {
        "data": {
            "timelines": [
                {
                    "timestep": "1h",
                    "startTime": "2025-12-23T13:00:00Z",
                    "endTime": "2025-12-23T16:00:00Z",
                    "intervals": sample_weather_intervals,
                }
            ]
        }
    }


@pytest.fixture
def mock_api_error_response():
    """Mock API error response."""
    return {
        "type": "https://api.tomorrow.io/errors/rate-limit",
        "title": "Rate Limit Exceeded",
        "status": 429,
        "detail": "You have exceeded the rate limit of 25 requests per hour",
    }


# =============================================================================
# DATABASE FIXTURES
# =============================================================================


@pytest.fixture
def temp_duckdb():
    """Temporary DuckDB database for testing."""
    temp_dir = tempfile.mkdtemp()
    db_path = Path(temp_dir) / "test_weather.db"

    yield str(db_path)

    # Cleanup
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def duckdb_connection(temp_duckdb):
    """DuckDB connection fixture."""
    conn = duckdb.connect(temp_duckdb)
    yield conn
    conn.close()


# =============================================================================
# DLT PIPELINE FIXTURES
# =============================================================================


@pytest.fixture
def test_pipeline_name():
    """Test pipeline name to avoid conflicts."""
    return "test_tomorrow_io_pipeline"


@pytest.fixture
def temp_dlt_dir():
    """Temporary DLT data directory for isolated testing."""
    temp_dir = tempfile.mkdtemp(prefix="dlt_test_")
    yield temp_dir
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def test_pipeline(test_pipeline_name, temp_duckdb, temp_dlt_dir):
    """
    DLT pipeline instance for testing with isolated temp database.

    This fixture ensures:
    - Each test gets a fresh pipeline
    - No pollution between tests
    - Automatic cleanup after tests
    """
    pipeline = dlt.pipeline(
        pipeline_name=test_pipeline_name,
        destination=dlt.destinations.duckdb(temp_duckdb),
        dataset_name="test_weather_data",
        dev_mode=True,  # Use dev mode for testing
        pipelines_dir=temp_dlt_dir,
    )

    yield pipeline

    # Cleanup: drop pipeline state
    try:
        pipeline.drop()
    except Exception:
        pass


# =============================================================================
# MOCK ENVIRONMENT FIXTURES
# =============================================================================


@pytest.fixture
def mock_api_token():
    """Mock API token for testing."""
    return "test_api_token_12345"


@pytest.fixture
def mock_env_vars(monkeypatch, mock_api_token):
    """Set up mock environment variables."""
    monkeypatch.setenv("TOMORROW_IO_API_KEY", mock_api_token)
    monkeypatch.setenv("RUNTIME__LOG_LEVEL", "WARNING")
    return {
        "api_key": mock_api_token,
        "log_level": "WARNING",
    }


# =============================================================================
# PARAMETRIZED TEST DATA FIXTURES
# =============================================================================


@pytest.fixture(
    params=[
        {"hours": 1, "expected_count": 1},
        {"hours": 3, "expected_count": 3},
        {"hours": 24, "expected_count": 24},
    ]
)
def interval_count_params(request):
    """Parametrized fixture for testing different interval counts."""
    return request.param


# =============================================================================
# HELPER FIXTURES
# =============================================================================


@pytest.fixture
def assert_valid_timestamp():
    """Helper fixture to validate timestamp format."""

    def _validate(timestamp_str: str):
        """Validate ISO 8601 timestamp string."""
        try:
            datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
            return True
        except ValueError:
            return False

    return _validate


@pytest.fixture
def assert_valid_location():
    """Helper fixture to validate location data."""

    def _validate(location: Dict[str, Any]):
        """Validate location has required fields."""
        required_fields = {"id", "name", "lat", "lon"}
        return required_fields.issubset(location.keys())

    return _validate
