"""DLT loads for Tomorrow.io weather data ingestion.

This module defines the DLT sources and pipelines used by the
DltLoadCollectionComponent. The component automatically converts these
into Dagster assets.
"""

import dlt
from .tomorrow_io_pipeline import tomorrow_io_source, create_pipeline


# Weather data source - fetches from Tomorrow.io API
# This creates a bitemporal data model with locations and weather_observations
weather_source = tomorrow_io_source()

# DLT pipeline configuration - loads to DuckDB
weather_pipeline = create_pipeline()
