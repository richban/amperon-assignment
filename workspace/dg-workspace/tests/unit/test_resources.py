"""
Unit tests for DLT resources in the Tomorrow.io pipeline.

Tests the locations() resource and helper functions in isolation.
"""

import pytest
import sys
from pathlib import Path
from datetime import datetime, timedelta, timezone

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from dg_amperon.defs.weather_ingestion.tomorrow_io_pipeline import (
    locations,
    get_normalized_observation_timestamp,
    get_absolute_time_params,
    LOCATIONS,
)


# =============================================================================
# LOCATIONS RESOURCE TESTS
# =============================================================================


@pytest.mark.unit
class TestLocationsResource:
    """Test suite for the locations() DLT resource."""

    def test_locations_yields_items(self):
        """Test that locations() yields location items."""
        # DLT unpacks the list and yields each location individually
        location_resource = locations()
        result = list(location_resource)

        # Should have yielded multiple locations
        assert len(result) > 0
        # Each item should be a dict
        assert isinstance(result[0], dict)

    def test_locations_contains_required_fields(self):
        """Test that each location has required fields."""
        location_resource = locations()
        result = list(location_resource)

        required_fields = {"id", "name", "lat", "lon"}

        for location in result:
            assert required_fields.issubset(location.keys()), (
                f"Location missing required fields: {location}"
            )

    def test_locations_field_types(self):
        """Test that location fields have correct data types."""
        location_resource = locations()
        result = list(location_resource)

        for location in result:
            assert isinstance(location["id"], int), "ID must be integer"
            assert isinstance(location["name"], str), "Name must be string"
            assert isinstance(location["lat"], (int, float)), "Lat must be numeric"
            assert isinstance(location["lon"], (int, float)), "Lon must be numeric"

    def test_locations_lat_lon_ranges(self):
        """Test that latitude and longitude are within valid ranges."""
        location_resource = locations()
        result = list(location_resource)

        for location in result:
            lat, lon = location["lat"], location["lon"]

            assert -90 <= lat <= 90, f"Invalid latitude: {lat}"
            assert -180 <= lon <= 180, f"Invalid longitude: {lon}"

    def test_locations_unique_ids(self):
        """Test that all location IDs are unique."""
        location_resource = locations()
        result = list(location_resource)

        ids = [loc["id"] for loc in result]
        assert len(ids) == len(set(ids)), "Location IDs must be unique"

    def test_locations_returns_correct_count(self):
        """Test that locations() returns the expected number of locations."""
        location_resource = locations()
        result = list(location_resource)

        # Should match LOCATIONS constant from the module
        assert len(result) == len(LOCATIONS)

    def test_locations_data_matches_constant(self):
        """Test that locations() yields the same data as LOCATIONS constant."""
        location_resource = locations()
        result = list(location_resource)

        # Should yield the same items as LOCATIONS list
        assert result == LOCATIONS


# =============================================================================
# TIME UTILITY FUNCTION TESTS
# =============================================================================


@pytest.mark.unit
class TestTimeUtilityFunctions:
    """Test suite for time utility functions."""

    def test_get_normalized_observation_timestamp_without_argument(self):
        """Test timestamp normalization with current time."""
        result = get_normalized_observation_timestamp()

        # Should be a datetime object
        assert isinstance(result, datetime)

        # Should be normalized (minute, second, microsecond = 0)
        assert result.minute == 0
        assert result.second == 0
        assert result.microsecond == 0

    def test_get_normalized_observation_timestamp_with_iso_string(self):
        """Test timestamp normalization with ISO string input."""
        test_input = "2025-12-23T13:45:30"
        result = get_normalized_observation_timestamp(test_input)

        # Should normalize to 13:00:00
        assert result.hour == 13
        assert result.minute == 0
        assert result.second == 0

    def test_get_normalized_observation_timestamp_with_utc_suffix(self):
        """Test timestamp normalization with UTC 'Z' suffix."""
        test_input = "2025-12-23T13:45:30Z"
        result = get_normalized_observation_timestamp(test_input)

        assert result.hour == 13
        assert result.minute == 0
        assert result.second == 0

    def test_get_normalized_observation_timestamp_floors_correctly(self):
        """Test that timestamps at different minutes floor to the same hour."""
        test_cases = [
            "2025-12-23T13:00:00",
            "2025-12-23T13:15:00",
            "2025-12-23T13:30:00",
            "2025-12-23T13:59:59",
        ]

        results = [get_normalized_observation_timestamp(tc) for tc in test_cases]

        # All should normalize to 13:00:00
        assert all(r.hour == 13 for r in results)
        assert all(r.minute == 0 for r in results)
        assert all(r.second == 0 for r in results)

    def test_get_absolute_time_params_returns_dict(self):
        """Test that get_absolute_time_params returns a dictionary."""
        observation_ts = datetime(2025, 12, 23, 13, 0, 0, tzinfo=timezone.utc)
        result = get_absolute_time_params(observation_ts)

        assert isinstance(result, dict)
        assert "startTime" in result
        assert "endTime" in result

    def test_get_absolute_time_params_time_window(self):
        """Test that time window is calculated correctly."""
        observation_ts = datetime(2025, 12, 23, 13, 0, 0, tzinfo=timezone.utc)
        result = get_absolute_time_params(observation_ts)

        # Parse the ISO strings
        start = datetime.fromisoformat(result["startTime"].replace("Z", "+00:00"))
        end = datetime.fromisoformat(result["endTime"].replace("Z", "+00:00"))

        # Start should be 24 hours before observation_ts
        expected_start = observation_ts - timedelta(hours=24)
        assert start == expected_start

        # End should be 5 days after observation_ts
        expected_end = observation_ts + timedelta(days=5)
        assert end == expected_end

    def test_get_absolute_time_params_iso_format(self):
        """Test that returned timestamps are in ISO format with Z suffix."""
        observation_ts = datetime(2025, 12, 23, 13, 0, 0, tzinfo=timezone.utc)
        result = get_absolute_time_params(observation_ts)

        # Should end with 'Z'
        assert result["startTime"].endswith("Z")
        assert result["endTime"].endswith("Z")

        # Should contain 'T' separator
        assert "T" in result["startTime"]
        assert "T" in result["endTime"]

    def test_get_absolute_time_params_total_duration(self):
        """Test that the total time window is correct (24h backwards + 5 days forward)."""
        observation_ts = datetime(2025, 12, 23, 13, 0, 0, tzinfo=timezone.utc)
        result = get_absolute_time_params(observation_ts)

        start = datetime.fromisoformat(result["startTime"].replace("Z", "+00:00"))
        end = datetime.fromisoformat(result["endTime"].replace("Z", "+00:00"))

        # Total duration should be 6 days (144 hours)
        duration = end - start
        assert duration == timedelta(days=6)  # 24h back + 5 days forward = 6 days total


# =============================================================================
# PARAMETRIZED TESTS
# =============================================================================


@pytest.mark.unit
@pytest.mark.parametrize(
    "hour,minute,second",
    [
        (0, 0, 0),
        (12, 30, 45),
        (23, 59, 59),
    ],
)
def test_normalized_timestamp_various_times(hour, minute, second):
    """Test timestamp normalization across various times of day."""
    test_input = f"2025-12-23T{hour:02d}:{minute:02d}:{second:02d}"
    result = get_normalized_observation_timestamp(test_input)

    # Should always normalize to the hour boundary
    assert result.hour == hour
    assert result.minute == 0
    assert result.second == 0


@pytest.mark.unit
@pytest.mark.parametrize("location_id", [1, 2])
def test_locations_contains_specific_ids(location_id):
    """Test that specific location IDs exist in the dataset."""
    location_resource = locations()
    result = list(location_resource)

    ids = [loc["id"] for loc in result]
    assert location_id in ids, f"Location ID {location_id} not found"
