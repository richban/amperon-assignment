"""DLT pipeline to fetch weather data from Tomorrow.io API"""

import dlt
import requests
import time
import logging
from datetime import datetime
from typing import Iterator, Dict, List

from etl.config import (
    TOMORROW_API_KEY,
    TOMORROW_API_URL,
    LOCATIONS,
    WEATHER_FIELDS,
)

logger = logging.getLogger(__name__)


def fetch_weather_for_location(location: Dict) -> List[Dict]:
    """
    Fetch weather timeline from Tomorrow.io API for a single location

    Returns flattened list of weather records
    """
    payload = {
        "location": f"{location['lat']}, {location['lon']}",
        "fields": WEATHER_FIELDS,
        "units": "metric",
        "timesteps": ["1h"],
        "startTime": "nowMinus24h",
        "endTime": "nowPlus5d",
        "timezone": "UTC",
    }

    headers = {"accept": "application/json", "content-type": "application/json"}

    url = f"{TOMORROW_API_URL}?apikey={TOMORROW_API_KEY}"

    logger.info(
        f"Fetching weather for location {location['id']} ({location['lat']}, {location['lon']})"
    )

    try:
        response = requests.post(url, json=payload, headers=headers, timeout=30)
        response.raise_for_status()

        # Check rate limit headers
        remaining = response.headers.get("X-RateLimit-Remaining-hour")
        if remaining:
            logger.info(f"Remaining API calls this hour: {remaining}")

        data = response.json()

        # Flatten the response
        records = []
        scraped_at = datetime.utcnow().isoformat()

        for timeline in data.get("data", {}).get("timelines", []):
            for interval in timeline.get("intervals", []):
                record = {
                    "location_id": location["id"],
                    "latitude": location["lat"],
                    "longitude": location["lon"],
                    "timestamp": interval["startTime"],
                    "scraped_at": scraped_at,
                }
                # Add all weather field values
                record.update(interval.get("values", {}))
                records.append(record)

        logger.info(f"Fetched {len(records)} records for location {location['id']}")
        return records

    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch data for location {location['id']}: {e}")
        return []


@dlt.source
def weather_source():
    """DLT source for weather data"""

    @dlt.resource(name="weather_timelines", write_disposition="append")
    def weather_timelines() -> Iterator[Dict]:
        """Fetch weather data for all locations"""

        for location in LOCATIONS:
            records = fetch_weather_for_location(location)

            for record in records:
                yield record

            # Rate limiting: 2 second delay between locations
            time.sleep(2)

    return weather_timelines


def run_pipeline():
    """Run the DLT pipeline"""
    logger.info("Starting weather data pipeline")

    # Create pipeline
    pipeline = dlt.pipeline(
        pipeline_name="weather_ingestion",
        destination="filesystem",
        dataset_name="weather",
    )

    # Run pipeline
    load_info = pipeline.run(weather_source(), loader_file_format="parquet")

    logger.info(f"Pipeline completed: {load_info}")
    return load_info


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_pipeline()
