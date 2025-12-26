"""Tomorrow.io Weather Data Pipeline using DLT REST API Source

Fetches weather data (hourly forecasts from now to +5d) for multiple locations
in the Port of Brownsville area using the Tomorrow.io Timelines API.

Configuration:
- API key: .dlt/secrets.toml -> sources.tomorrow_io_access_token
- Destination: DuckDB (.dlt/config.toml)
- Locations: Static seed resource (non-REST endpoint)

Architecture:
- Uses dlt rest_api_source with declarative RESTAPIConfig
- locations resource: Non-REST seed data (yields list of locations)
- weather_timelines resource: Dependent REST endpoint that uses location data
- POST requests to /v4/timelines with location coordinates in JSON body
- Single page responses (no pagination needed)

To run:
    python etl/tomorrow_io_pipeline.py
"""

import dlt
import logging
import os
from dlt.sources.rest_api import rest_api_source
from dlt.sources.rest_api.typing import RESTAPIConfig
from typing import List, Dict, Any, Generator, Optional
from datetime import datetime, timedelta
from etl.utils import get_duckdb_path

# Configure DLT logging level
os.environ.setdefault("RUNTIME__LOG_LEVEL", "WARNING")

# Get DLT logger
logger = logging.getLogger("dlt")

# =============================================================================
# DATA CONFIGURATION
# =============================================================================

LOCATIONS = [
    {"id": 1, "name": "Port Brownsville 1", "lat": 25.8600, "lon": -97.4200},
    {"id": 2, "name": "Port Brownsville 2", "lat": 25.9000, "lon": -97.5200},
    # {"id": 3, "name": "Port Brownsville 3", "lat": 25.9000, "lon": -97.4800},
    # {"id": 4, "name": "Port Brownsville 4", "lat": 25.9000, "lon": -97.4400},
    # {"id": 5, "name": "Port Brownsville 5", "lat": 25.9000, "lon": -97.4000},
    # {"id": 6, "name": "Port Brownsville 6", "lat": 25.9200, "lon": -97.3800},
    # {"id": 7, "name": "Port Brownsville 7", "lat": 25.9400, "lon": -97.5400},
    # {"id": 8, "name": "Port Brownsville 8", "lat": 25.9400, "lon": -97.5200},
    # {"id": 9, "name": "Port Brownsville 9", "lat": 25.9400, "lon": -97.4800},
    # {"id": 10, "name": "Port Brownsville 10", "lat": 25.9400, "lon": -97.4400},
]

# Core Weather Fields (Free tier)
WEATHER_FIELDS = [
    "temperature",  # Ambient air temperature (°C)
    "temperatureApparent",  # Feels-like temperature
    "humidity",  # Relative humidity (%)
    "windSpeed",  # Wind speed (m/s)
    "windDirection",  # Wind direction (degrees)
    "precipitationIntensity",  # Precipitation rate (mm/h)
    "precipitationProbability",  # Chance of precipitation (%)
    "precipitationType",  # 0=none, 1=rain, 2=snow, 3=freezing rain, 4=ice
    "weatherCode",  # Weather condition code
    "cloudCover",  # Cloud coverage (%)
    "pressureSurfaceLevel",  # Atmospheric pressure (hPa)
    "visibility",  # Visibility distance (km)
]

# =============================================================================
# TIME UTILITY FUNCTIONS
# =============================================================================


def get_normalized_run_timestamp(backfill_datetime: Optional[str] = None) -> datetime:
    """
    Get normalized run timestamp (floored to hour boundary).

    This ensures idempotent pipeline runs by normalizing execution time
    to the start of the hour. Running at 13:05, 13:23, or 13:59 all use
    13:00:00 as the run_timestamp.

    Args:
        backfill_datetime: Optional ISO format datetime string for backfill
                          Format: "YYYY-MM-DDTHH:MM:SS" or "YYYY-MM-DDTHH:MM:SSZ"
                          Example: "2025-12-23T13:00:00"

    Returns:
        datetime: Normalized to hour boundary (minute=0, second=0, microsecond=0)

    Examples:
        >>> get_normalized_run_timestamp()  # Called at 13:47:22
        datetime(2025, 12, 23, 13, 0, 0)

        >>> get_normalized_run_timestamp("2025-12-22T15:30:00Z")
        datetime(2025, 12, 22, 15, 0, 0)
    """
    if backfill_datetime:
        # Parse backfill datetime, removing trailing 'Z' if present
        ts = datetime.fromisoformat(backfill_datetime.replace("Z", ""))
    else:
        # Use current UTC time
        ts = datetime.utcnow()

    # Floor to hour boundary
    return ts.replace(minute=0, second=0, microsecond=0)


def get_absolute_time_params(run_ts: datetime) -> Dict[str, str]:
    """
    Calculate absolute startTime/endTime from normalized run timestamp.

    This creates a fixed time window for the API call, ensuring idempotency.
    The same run_timestamp always produces the same time window.

    Args:
        run_ts: Normalized run timestamp (from get_normalized_run_timestamp)

    Returns:
        Dict with ISO 8601 formatted startTime and endTime:
        - startTime: run_ts - 24 hours (historical observations)
        - endTime: run_ts + 5 days (forecast window)

    Examples:
        >>> run_ts = datetime(2025, 12, 23, 13, 0, 0)
        >>> get_absolute_time_params(run_ts)
        {
            'startTime': '2025-12-22T13:00:00Z',  # -24 hours
            'endTime': '2025-12-28T13:00:00Z'     # +5 days
        }
    """
    start_time = run_ts - timedelta(hours=24)
    end_time = run_ts + timedelta(days=5)

    return {
        "startTime": start_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "endTime": end_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


@dlt.resource(name="locations", write_disposition="replace")
def locations() -> Generator[List[Dict[str, Any]], Any, None]:
    """
    Seed resource: Static list of locations to fetch weather data for.

    This is NOT a REST endpoint - it's a non-REST resource that yields
    a list of locations. The weather_timelines resource will process
    each location to make API calls.

    IMPORTANT: Must yield a List[Dict], not yield from an iterable!
    """
    yield LOCATIONS


@dlt.source
def tomorrow_io_source(
    tomorrow_io_access_token: str = dlt.secrets.value,
    backfill_datetime: Optional[str] = None,
):
    """
    Tomorrow.io REST API source with versioned snapshots.

    This source creates a bitemporal data model where each pipeline run
    captures a "snapshot" of forecasts at a specific observation time.

    Args:
        tomorrow_io_access_token: API key from secrets
        backfill_datetime: Optional ISO datetime string for backfill mode
                          Format: "YYYY-MM-DDTHH:MM:SS" or with 'Z' suffix
                          Example: "2025-12-23T13:00:00"

    Returns:
        DLT transformer resource with run_timestamp enrichment

    Architecture:
        1. locations (seed) → weather_timelines (REST API) → weather_observations (transformer)
        2. Each run creates ~145-146 versioned records per location
        3. Composite PK [_locations_id, start_time, run_timestamp] enables:
           - Idempotent reruns (same hour replaces snapshot)
           - Version tracking (different hours create new snapshots)
    """
    # 1. Calculate normalized run timestamp
    run_ts = get_normalized_run_timestamp(backfill_datetime)
    run_ts_iso = run_ts.isoformat() + "Z"

    # 2. Calculate absolute time window (for API idempotency)
    time_params = get_absolute_time_params(run_ts)

    # 3. Log execution context
    logger.info(f"Pipeline Observation Time: {run_ts_iso}")
    logger.info(
        f"API Time Window: {time_params['startTime']} to {time_params['endTime']}"
    )
    logger.info(
        f"Expected Records: ~{145 * len(LOCATIONS)} (145h × {len(LOCATIONS)} locations)"
    )

    # 4. Build REST API config with absolute timestamps
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://api.tomorrow.io/v4/",
            "auth": {
                "type": "api_key",
                "name": "apikey",
                "api_key": tomorrow_io_access_token,
                "location": "query",
            },
            "headers": {"Accept-Encoding": "gzip", "Content-Type": "application/json"},
        },
        "resources": [
            {
                "name": "weather_timelines",
                "endpoint": {
                    "path": "timelines",
                    "method": "POST",
                    "json": {
                        "location": "{resources.locations.lat},{resources.locations.lon}",
                        "fields": WEATHER_FIELDS,
                        "timesteps": ["1h"],
                        "units": "metric",
                        "startTime": time_params["startTime"],
                        "endTime": time_params["endTime"],
                        "timezone": "UTC",
                    },
                    "data_selector": "data.timelines[0].intervals",
                    "paginator": {"type": "single_page"},
                },
                "include_from_parent": ["id"],
            },
            locations(),
        ],
    }

    source = rest_api_source(config)

    @dlt.transformer(
        data_from=source.resources["weather_timelines"],
        name="weather_observations",
        write_disposition="merge",
        primary_key=["_locations_id", "start_time", "run_timestamp"],
    )
    def add_versioning_metadata(item):
        """
        Inject run_timestamp into each weather observation.

        This creates the bitemporal model:
        - start_time: WHEN the weather event occurs (forecast_timestamp)
        - run_timestamp: WHEN we observed/predicted it (observation_timestamp)

        Normalized schema:
        - Only includes _locations_id (foreign key to locations table)
        - Location details (name, lat, lon) stored in separate locations dimension table
        """
        # Handle both dict items and list of items
        if isinstance(item, list):
            # If item is a list, process each element
            for record in item:
                if isinstance(record, dict):
                    record["run_timestamp"] = run_ts_iso
                    yield record
        elif isinstance(item, dict):
            # Single dict item
            item["run_timestamp"] = run_ts_iso
            yield item
        else:
            # Unexpected type
            logger.warning(f"Unexpected item type: {type(item)}")
            yield item

    return source.resources["locations"], add_versioning_metadata


def run_pipeline(backfill_datetime: Optional[str] = None):
    """
    Run the Tomorrow.io weather pipeline.

    Args:
        backfill_datetime: Optional ISO datetime string for backfill mode
                          Format: "YYYY-MM-DDTHH:MM:SS" (seconds/minutes optional)
                          Example: "2025-12-23T13:00:00"

    Returns:
        LoadInfo: DLT load information

    Examples:
        # Scheduled mode (uses current time, normalized to hour)
        >>> run_pipeline()

        # Backfill mode (uses specific hour)
        >>> run_pipeline(backfill_datetime="2025-12-22T13:00:00")
    """
    # Determine run timestamp for logging
    run_ts = get_normalized_run_timestamp(backfill_datetime)
    mode = "BACKFILL" if backfill_datetime else "SCHEDULED"

    # Create pipeline
    pipeline = dlt.pipeline(
        pipeline_name="tomorrow_io",
        destination=dlt.destinations.duckdb(str(get_duckdb_path())),
        dataset_name="weather_data",  # Avoid ambiguity with database name
        dev_mode=False,  # Production mode (stable schema)
        progress="log",
    )

    load_info = pipeline.run(tomorrow_io_source(backfill_datetime=backfill_datetime))

    logger.info(f"Load info: {load_info}")
    logger.info(f"Database: {get_duckdb_path()}")

    return load_info


if __name__ == "__main__":
    import sys

    # Support backfill via command line argument
    # Usage: python etl/tomorrow_io_pipeline.py "2025-12-23T13:00:00"
    backfill_dt = sys.argv[1] if len(sys.argv) > 1 else None

    run_pipeline(backfill_datetime=backfill_dt)
