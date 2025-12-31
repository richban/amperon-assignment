"""
End-to-end integration test for bitemporal weather pipeline.
"""

import pytest
import sys
from pathlib import Path
import dlt
import duckdb
import tempfile
import responses

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from dg_amperon.defs.weather_ingestion.tomorrow_io_pipeline import tomorrow_io_source


@pytest.fixture
def temp_pipeline():
    """Create temporary pipeline with DuckDB for testing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = f"{tmpdir}/test_weather.duckdb"
        pipeline = dlt.pipeline(
            pipeline_name="test_weather_pipeline",
            destination=dlt.destinations.duckdb(db_path),
            dataset_name="test_weather_data",
            pipelines_dir=tmpdir,
            dev_mode=False,  # Use production mode for stable schema names
        )
        yield pipeline, db_path
        # Cleanup happens automatically when tmpdir context exits


@pytest.mark.integration
@responses.activate
def test_bitemporal_weather_pipeline_end_to_end(temp_pipeline):
    """
    Test bitemporal data modeling end-to-end:
    - Two locations get weather data
    - Data is stored with forecast_timestamp (start_time) and observation_timestamp (observation_timestamp)
    - Running pipeline twice with different observation_timestamp creates versioned snapshots
    """

    pipeline, db_path = temp_pipeline

    # Mock API response: -2h to +2h (5 hours total) with only temperature
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
                                "startTime": "2025-12-23T11:00:00Z",
                                "values": {"temperature": 20.0},
                            },
                            {
                                "startTime": "2025-12-23T12:00:00Z",
                                "values": {"temperature": 21.0},
                            },
                            {
                                "startTime": "2025-12-23T13:00:00Z",
                                "values": {"temperature": 22.0},
                            },
                            {
                                "startTime": "2025-12-23T14:00:00Z",
                                "values": {"temperature": 23.0},
                            },
                            {
                                "startTime": "2025-12-23T15:00:00Z",
                                "values": {"temperature": 24.0},
                            },
                        ],
                    }
                ]
            }
        },
        status=200,
    )

    # Run 1: Backfill at 13:00 (observation_timestamp = 13:00)
    source_run1 = tomorrow_io_source(
        tomorrow_io_access_token="test_token", backfill_datetime="2025-12-23T13:00:00Z"
    )
    load_info_1 = pipeline.run(source_run1)

    # Verify load success
    assert load_info_1 is not None
    assert len(load_info_1.loads_ids) > 0

    # Query the data
    with duckdb.connect(db_path) as conn:
        # Check weather_observations table
        obs_count = conn.execute(
            "SELECT COUNT(*) FROM test_weather_data.weather_observations"
        ).fetchone()[0]
        assert obs_count == 50, (
            f"Should have 50 observations (5 hours × 10 locations), got {obs_count}"
        )

        # Verify bitemporal structure
        bitemporal_check = conn.execute("""
            SELECT
                start_time,
                observation_timestamp,
                _locations_id,
                values__temperature
            FROM test_weather_data.weather_observations
            ORDER BY _locations_id, start_time
            LIMIT 5
        """).fetchall()

        # All should have observation_timestamp with hour = 13
        for row in bitemporal_check:
            start_time, observation_timestamp, location_id, temperature = row
            # Check hour is 13 or 14 (depending on timezone conversion)
            assert "13:00:00" in str(observation_timestamp) or "14:00:00" in str(
                observation_timestamp
            ), (
                f"observation_timestamp should be on hour boundary, got {observation_timestamp}"
            )

        # Verify temperature values match mock
        temps = conn.execute("""
            SELECT DISTINCT values__temperature
            FROM test_weather_data.weather_observations
            ORDER BY values__temperature
        """).fetchall()

        expected_temps = [20.0, 21.0, 22.0, 23.0, 24.0]
        actual_temps = [t[0] for t in temps]
        assert actual_temps == expected_temps, (
            f"Expected temps {expected_temps}, got {actual_temps}"
        )

        # Verify all 10 locations have all 5 time points
        location_counts = conn.execute("""
            SELECT _locations_id, COUNT(*) as cnt
            FROM test_weather_data.weather_observations
            GROUP BY _locations_id
            ORDER BY _locations_id
        """).fetchall()

        assert len(location_counts) == 10, "Should have data for 10 locations"
        for idx, (location_id, count) in enumerate(location_counts):
            assert count == 5, (
                f"Location {location_id} should have 5 observations, got {count}"
            )

    # Run 2: Different observation time (14:00) with slightly different temperatures
    responses.add(
        responses.POST,
        "https://api.tomorrow.io/v4/timelines",
        json={
            "data": {
                "timelines": [
                    {
                        "intervals": [
                            {
                                "startTime": "2025-12-23T12:00:00Z",
                                "values": {"temperature": 21.5},
                            },  # Updated
                            {
                                "startTime": "2025-12-23T13:00:00Z",
                                "values": {"temperature": 22.5},
                            },  # Updated
                            {
                                "startTime": "2025-12-23T14:00:00Z",
                                "values": {"temperature": 23.5},
                            },  # Updated
                            {
                                "startTime": "2025-12-23T15:00:00Z",
                                "values": {"temperature": 24.5},
                            },  # Updated
                            {
                                "startTime": "2025-12-23T16:00:00Z",
                                "values": {"temperature": 25.5},
                            },  # Shifted  +1h
                        ]
                    }
                ]
            }
        },
        status=200,
    )

    source_run2 = tomorrow_io_source(
        tomorrow_io_access_token="test_token", backfill_datetime="2025-12-23T14:00:00Z"
    )
    load_info_2 = pipeline.run(source_run2)

    assert load_info_2 is not None

    # Verify bitemporal versioning: should now have 100 observations (50 from run1 + 50 from run2)
    with duckdb.connect(db_path) as conn:
        total_obs = conn.execute(
            "SELECT COUNT(*) FROM test_weather_data.weather_observations"
        ).fetchone()[0]
        assert total_obs == 100, (
            f"Should have 100 observations (2 runs × 5 hours × 10 locations), got {total_obs}"
        )

        # Verify we have 2 different observation_timestamps
        distinct_observation_timestamps = conn.execute("""
            SELECT DISTINCT observation_timestamp
            FROM test_weather_data.weather_observations
            ORDER BY observation_timestamp
        """).fetchall()

        assert len(distinct_observation_timestamps) == 2, (
            f"Should have 2 distinct observation_timestamps, got {len(distinct_observation_timestamps)}"
        )

        # Verify forecast drift: same start_time, different observation_timestamp, different temperature
        # Use HOUR() to find the 13:00 timestamp regardless of timezone offset
        forecast_drift = conn.execute("""
            SELECT
                start_time,
                observation_timestamp,
                values__temperature
            FROM test_weather_data.weather_observations
            WHERE _locations_id = 1
              AND EXTRACT(HOUR FROM start_time) = 13
              AND EXTRACT(DAY FROM start_time) = 23
            ORDER BY observation_timestamp
        """).fetchall()

        assert len(forecast_drift) == 2, (
            f"Should have 2 forecasts for the same time from different runs, got {len(forecast_drift)}"
        )

        # First run and second run should have 0.5 degree difference (forecast drift)
        temp_diff = forecast_drift[1][2] - forecast_drift[0][2]
        assert temp_diff == 0.5, (
            f"Temperature difference should be 0.5 degrees (forecast drift), got {temp_diff}"
        )

        # Verify actual temperatures are reasonable (within expected range)
        assert 20.0 <= forecast_drift[0][2] <= 25.0, (
            "Temperature should be in reasonable range"
        )
        assert 20.0 <= forecast_drift[1][2] <= 25.0, (
            "Temperature should be in reasonable range"
        )

        # Verify composite primary key works: (location_id, start_time, observation_timestamp) is unique
        pk_check = conn.execute("""
            SELECT
                COUNT(*) as total_rows,
                COUNT(DISTINCT _locations_id || start_time || observation_timestamp) as unique_keys
            FROM test_weather_data.weather_observations
        """).fetchone()

        assert pk_check[0] == pk_check[1], (
            f"Primary key should be unique: {pk_check[0]} rows, {pk_check[1]} unique keys"
        )


@pytest.mark.integration
@responses.activate
def test_idempotency_same_hour_replaces_data(temp_pipeline):
    """
    Test idempotency: Running pipeline twice in the same hour (e.g., 14:00 and 14:59)
    should REPLACE data, not duplicate it, because both normalize to 14:00:00.
    """

    pipeline, db_path = temp_pipeline

    # Mock API response for first run
    responses.add(
        responses.POST,
        "https://api.tomorrow.io/v4/timelines",
        json={
            "data": {
                "timelines": [
                    {
                        "intervals": [
                            {
                                "startTime": "2025-12-23T12:00:00Z",
                                "values": {"temperature": 21.0},
                            },
                            {
                                "startTime": "2025-12-23T13:00:00Z",
                                "values": {"temperature": 22.0},
                            },
                            {
                                "startTime": "2025-12-23T14:00:00Z",
                                "values": {"temperature": 23.0},
                            },
                            {
                                "startTime": "2025-12-23T15:00:00Z",
                                "values": {"temperature": 24.0},
                            },
                            {
                                "startTime": "2025-12-23T16:00:00Z",
                                "values": {"temperature": 25.0},
                            },
                        ]
                    }
                ]
            }
        },
        status=200,
    )

    # Run 1: Observe at 14:00:00 (will normalize to 14:00:00)
    source_run1 = tomorrow_io_source(
        tomorrow_io_access_token="test_token", backfill_datetime="2025-12-23T14:00:00Z"
    )
    load_info_1 = pipeline.run(source_run1)
    assert load_info_1 is not None

    # Verify initial data
    with duckdb.connect(db_path) as conn:
        count_after_run1 = conn.execute(
            "SELECT COUNT(*) FROM test_weather_data.weather_observations"
        ).fetchone()[0]
        assert count_after_run1 == 50, (
            f"After run 1: expected 50 records (5 hours × 10 locations), got {count_after_run1}"
        )

        # Get a sample temperature from run 1 (any hour)
        temp_run1 = conn.execute("""
            SELECT values__temperature
            FROM test_weather_data.weather_observations
            LIMIT 1
        """).fetchone()[0]
        # Just verify it's one of the run 1 temps (21.0-25.0 range)
        assert 21.0 <= temp_run1 <= 25.0, (
            f"Run 1 temp should be in range 21-25, got {temp_run1}"
        )

    # Mock API response for second run with DIFFERENT temperatures
    responses.add(
        responses.POST,
        "https://api.tomorrow.io/v4/timelines",
        json={
            "data": {
                "timelines": [
                    {
                        "intervals": [
                            {
                                "startTime": "2025-12-23T12:00:00Z",
                                "values": {"temperature": 21.9},
                            },  # Changed
                            {
                                "startTime": "2025-12-23T13:00:00Z",
                                "values": {"temperature": 22.9},
                            },  # Changed
                            {
                                "startTime": "2025-12-23T14:00:00Z",
                                "values": {"temperature": 23.9},
                            },  # Changed
                            {
                                "startTime": "2025-12-23T15:00:00Z",
                                "values": {"temperature": 24.9},
                            },  # Changed
                            {
                                "startTime": "2025-12-23T16:00:00Z",
                                "values": {"temperature": 25.9},
                            },  # Changed
                        ]
                    }
                ]
            }
        },
        status=200,
    )

    # Run 2: Observe at 14:59:00 (will ALSO normalize to 14:00:00 - same hour!)
    source_run2 = tomorrow_io_source(
        tomorrow_io_access_token="test_token",
        backfill_datetime="2025-12-23T14:59:00Z",  # Different minute, same hour
    )
    load_info_2 = pipeline.run(source_run2)
    assert load_info_2 is not None

    # Verify idempotency: count should STILL be 50 (replaced, not duplicated)
    with duckdb.connect(db_path) as conn:
        count_after_run2 = conn.execute(
            "SELECT COUNT(*) FROM test_weather_data.weather_observations"
        ).fetchone()[0]
        assert count_after_run2 == 50, (
            f"After run 2: expected 50 records (replaced, not duplicated), got {count_after_run2}"
        )

        # Verify only ONE distinct observation_timestamp exists (14:00:00)
        distinct_observation_timestamps = conn.execute("""
            SELECT DISTINCT observation_timestamp
            FROM test_weather_data.weather_observations
        """).fetchall()
        assert len(distinct_observation_timestamps) == 1, (
            f"Should have only 1 observation_timestamp (both runs normalized to 14:00), got {len(distinct_observation_timestamps)}"
        )

        # Verify temperatures were REPLACED with run 2 data (run 2 temps are in 21.9-25.9 range)
        all_temps = conn.execute("""
            SELECT DISTINCT values__temperature
            FROM test_weather_data.weather_observations
            ORDER BY values__temperature
        """).fetchall()

        # Run 2 temperatures should be present (21.9, 22.9, 23.9, 24.9, 25.9)
        temps = [t[0] for t in all_temps]
        # Check that we have run 2 temps (which end in .9), not run 1 temps (which are whole numbers)
        assert any(str(t).endswith(".9") for t in temps), (
            f"Should have run 2 temperatures (ending in .9), got {temps}"
        )

        # Verify composite primary key is still unique
        pk_check = conn.execute("""
            SELECT
                COUNT(*) as total_rows,
                COUNT(DISTINCT _locations_id || start_time || observation_timestamp) as unique_keys
            FROM test_weather_data.weather_observations
        """).fetchone()
        assert pk_check[0] == pk_check[1], (
            f"Primary key should be unique: {pk_check[0]} rows, {pk_check[1]} unique keys"
        )
