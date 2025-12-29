"""
Unit test for tomorrow_io_source function.
"""

import pytest
import sys
from pathlib import Path
import responses

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from dg_amperon.defs.weather_ingestion.tomorrow_io_pipeline import tomorrow_io_source


@pytest.mark.unit
@responses.activate
def test_weather_observations_match_mocked_api_response():
    """Test that weather observations output exactly matches the mocked API response values."""

    # Define the exact values we expect from the mock
    expected_temperature = 25.5
    expected_wind_speed = 5.2
    expected_humidity = 65
    expected_start_time = "2025-12-23T13:00:00Z"

    # Mock the Tomorrow.io API response
    responses.add(
        responses.POST,
        "https://api.tomorrow.io/v4/timelines",
        json={
            "data": {
                "timelines": [
                    {
                        "timestep": "1h",
                        "intervals": [
                            {
                                "startTime": expected_start_time,
                                "values": {
                                    "temperature": expected_temperature,
                                    "windSpeed": expected_wind_speed,
                                    "humidity": expected_humidity,
                                },
                            }
                        ],
                    }
                ]
            }
        },
        status=200,
    )

    # Call the source
    source = tomorrow_io_source(tomorrow_io_access_token="test_token")

    # Get weather observations
    weather_data = list(source.resources["weather_observations"])

    # Should have one observation per location (10 total - all Brownsville locations)
    assert len(weather_data) == 10, (
        f"Expected 10 observations (one per location), got {len(weather_data)}"
    )

    # ALL observations should have the SAME values from the mocked API response
    for obs in weather_data:
        # Check startTime matches
        assert obs["startTime"] == expected_start_time, (
            f"startTime mismatch: expected {expected_start_time}, got {obs['startTime']}"
        )

        # Check values dict exists and has the exact same data as the mock
        assert "values" in obs, "Missing 'values' field"
        assert isinstance(obs["values"], dict), "values should be a dict"

        # Check each weather value matches the mock exactly
        assert obs["values"]["temperature"] == expected_temperature, (
            f"Temperature mismatch: expected {expected_temperature}, got {obs['values']['temperature']}"
        )

        assert obs["values"]["windSpeed"] == expected_wind_speed, (
            f"Wind speed mismatch: expected {expected_wind_speed}, got {obs['values']['windSpeed']}"
        )

        assert obs["values"]["humidity"] == expected_humidity, (
            f"Humidity mismatch: expected {expected_humidity}, got {obs['values']['humidity']}"
        )

        # Check that _locations_id exists and is valid
        assert "_locations_id" in obs, "Missing '_locations_id' field"
        assert obs["_locations_id"] in range(1, 11), (
            f"Invalid location_id: {obs['_locations_id']}, expected 1-10"
        )

        # Check that observation_timestamp was added by transformer
        assert "observation_timestamp" in obs, "Missing 'observation_timestamp' field"
        assert obs["observation_timestamp"].endswith(":00:00Z"), (
            "observation_timestamp should be normalized to hour boundary"
        )

    # Verify all locations got the same weather data
    first_values = weather_data[0]["values"]
    for obs in weather_data[1:]:
        assert obs["values"] == first_values, (
            "All locations should have identical weather values from the same API response"
        )

    # Verify location IDs are all unique and in expected range
    location_ids = [obs["_locations_id"] for obs in weather_data]
    assert sorted(location_ids) == list(range(1, 11)), (
        f"Expected location IDs [1, 2, ..., 10], got {sorted(location_ids)}"
    )
