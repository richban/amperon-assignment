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
from dlt.sources.rest_api import rest_api_source
from dlt.sources.rest_api.typing import RESTAPIConfig
from typing import List, Dict, Any, Generator
from utils import get_duckdb_path


LOCATIONS = [
    {"id": 1, "name": "Port Brownsville 1", "lat": 25.8600, "lon": -97.4200},
    {"id": 2, "name": "Port Brownsville 2", "lat": 25.9000, "lon": -97.5200},
    {"id": 3, "name": "Port Brownsville 3", "lat": 25.9000, "lon": -97.4800},
    {"id": 4, "name": "Port Brownsville 4", "lat": 25.9000, "lon": -97.4400},
    {"id": 5, "name": "Port Brownsville 5", "lat": 25.9000, "lon": -97.4000},
    {"id": 6, "name": "Port Brownsville 6", "lat": 25.9200, "lon": -97.3800},
    {"id": 7, "name": "Port Brownsville 7", "lat": 25.9400, "lon": -97.5400},
    {"id": 8, "name": "Port Brownsville 8", "lat": 25.9400, "lon": -97.5200},
    {"id": 9, "name": "Port Brownsville 9", "lat": 25.9400, "lon": -97.4800},
    {"id": 10, "name": "Port Brownsville 10", "lat": 25.9400, "lon": -97.4400},
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
def tomorrow_io_source(tomorrow_io_access_token: str = dlt.secrets.value):
    """
    Tomorrow.io Weather Data Source

    Uses RESTAPIConfig pattern with:
    1. locations() - Non-REST seed resource with location data
    2. weather_timelines - REST endpoint that references locations

    The weather_timelines resource uses location data in its JSON body
    via placeholder syntax: {resources.locations.lat}

    Args:
        tomorrow_io_access_token: Tomorrow.io API key (auto-injected from secrets)

    Returns:
        REST API source with locations and weather_timelines resources
    """

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
                        # Reference parent resource fields using placeholders
                        "location": "{resources.locations.lat},{resources.locations.lon}",
                        "fields": WEATHER_FIELDS,
                        "timesteps": ["1h"],
                        "units": "metric",
                        "startTime": "now",
                        "endTime": "nowPlus5d",
                        "timezone": "UTC",
                    },
                    # Extract intervals array from nested response
                    "data_selector": "data.timelines[0].intervals",
                    # Single page response
                    "paginator": {"type": "single_page"},
                },
                # Include parent location metadata in each weather record
                "include_from_parent": ["id", "name", "lat", "lon"],
                "write_disposition": "append",
            },
            # Include the non-REST locations resource
            locations(),
        ],
    }

    return rest_api_source(config)


def run_pipeline():
    """Run the Tomorrow.io weather pipeline"""
    duckdb_path = str(get_duckdb_path())

    # Create DLT pipeline pointing to DuckDB
    pipeline = dlt.pipeline(
        pipeline_name="tomorrow_io",
        destination=dlt.destinations.duckdb(duckdb_path),
        dataset_name="weather",
        dev_mode=True,
        progress="log",
    )

    # Run the source
    load_info = pipeline.run(tomorrow_io_source())

    print("\n" + "=" * 60)
    print("Pipeline completed successfully!")
    print("=" * 60)
    print(f"Load info: {load_info}")
    print(f"Working directory: {pipeline.working_dir}")

    return load_info


if __name__ == "__main__":
    run_pipeline()
