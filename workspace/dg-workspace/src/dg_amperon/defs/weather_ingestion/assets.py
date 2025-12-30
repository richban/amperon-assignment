"""
Dagster assets for Weather Data Ingestion Pipeline.

This module defines Dagster assets and asset checks for the Tomorrow.io weather pipeline.

Configuration:
    Database path can be configured via environment variable:
    - DUCKDB_DATABASE: Path to DuckDB database file
    - If not set, defaults to <project_root>/data/weather.db

Asset Checks:
    - check_locations_count: Validates that exactly 10 locations are configured
"""

from dagster import (
    AssetExecutionContext,
    AssetCheckExecutionContext,
    AssetKey,
    AssetCheckResult,
    asset_check,
    HourlyPartitionsDefinition,
    BackfillPolicy,
)
from dagster_duckdb import DuckDBResource
from dagster_dlt import DagsterDltResource, dlt_assets, DagsterDltTranslator
from dg_amperon.defs.weather_ingestion.tomorrow_io_pipeline import (
    tomorrow_io_source,
    locations_source,
    create_pipeline,
)


class CustomDagsterDltTranslator(DagsterDltTranslator):
    def get_asset_key(self, resource: DagsterDltResource) -> AssetKey:
        """Overrides asset key to be the dlt resource name."""
        return AssetKey(["weather_data", resource.name])


# Hourly partition for weather observations
# end_offset=1 makes the current hour partition available immediately (not waiting for hour to complete)
hourly_partition = HourlyPartitionsDefinition(
    start_date="2025-12-23-00:00", timezone="UTC", end_offset=1
)


# ============================================================================
# ASSET 1: Static Locations (Non-Partitioned)
# ============================================================================
@dlt_assets(
    name="locations_bronze",
    group_name="weather_ingestion",
    dlt_source=locations_source(),
    dlt_pipeline=create_pipeline(),
    dagster_dlt_translator=CustomDagsterDltTranslator(),
)
def locations_bronze_asset(
    context: AssetExecutionContext,
    dlt: DagsterDltResource,
):
    """Static locations table - no partitioning needed."""
    context.log.info("Materializing static locations table")
    yield from dlt.run(context=context)


# ============================================================================
# ASSET 2: Weather Observations (Hourly Partitioned)
# ============================================================================
@dlt_assets(
    name="weather_observations_bronze",
    group_name="weather_ingestion",
    dlt_source=tomorrow_io_source(),
    dlt_pipeline=create_pipeline(),
    dagster_dlt_translator=CustomDagsterDltTranslator(),
    partitions_def=hourly_partition,
    backfill_policy=BackfillPolicy.single_run(),
)
def weather_observations_bronze_asset(
    context: AssetExecutionContext,
    dlt: DagsterDltResource,
):
    """Hourly partitioned weather observations."""
    # Check if we're doing a backfill (multiple partitions)
    if hasattr(context, "partition_key_range") and context.partition_key_range:
        # For backfills with multiple partitions
        start_key = context.partition_key_range.start
        end_key = context.partition_key_range.end
        context.log.info(f"Executing backfill from {start_key} to {end_key}")

        # For now, process the start partition (can be enhanced for multi-partition backfills)
        # Convert partition key format: "2025-12-30-14:00" -> "2025-12-30T14:00:00"
        parts = start_key.rsplit("-", 1)  # ["2025-12-30", "14:00"]
        backfill_datetime = f"{parts[0]}T{parts[1]}:00"  # "2025-12-30T14:00:00"
        context.log.info(
            f"Converted start partition to ISO format: {backfill_datetime}"
        )
        source = tomorrow_io_source(backfill_datetime=backfill_datetime)
    elif hasattr(context, "partition_key") and context.partition_key:
        # For single partition
        partition_key = context.partition_key
        context.log.info(f"Processing hourly partition: {partition_key}")

        # Convert partition key to ISO datetime format expected by tomorrow_io_source
        # partition_key format: "2025-12-30-14:00" -> "2025-12-30T14:00:00"
        # Split on last hyphen to separate date from time
        parts = partition_key.rsplit("-", 1)  # ["2025-12-30", "14:00"]
        backfill_datetime = f"{parts[0]}T{parts[1]}:00"  # "2025-12-30T14:00:00"
        context.log.info(f"Converted partition key to ISO format: {backfill_datetime}")
        source = tomorrow_io_source(backfill_datetime=backfill_datetime)
    else:
        # Non-partitioned run (scheduled mode)
        context.log.info("Running in scheduled mode (no partition)")
        source = tomorrow_io_source()

    # Run DLT pipeline via DagsterDltResource
    yield from dlt.run(context=context, dlt_source=source)


@asset_check(asset=AssetKey(["weather_data", "locations"]))
def check_locations_count(
    context: AssetCheckExecutionContext, duckdb_resource: DuckDBResource
) -> AssetCheckResult:
    """Check that we have exactly 10 locations configured."""
    with duckdb_resource.get_connection() as conn:
        result = conn.execute(
            """
            SELECT COUNT(*) as location_count
            FROM weather_data.locations
        """
        ).fetchone()

        location_count = result[0]
        expected_count = 10
        passed = location_count == expected_count

        return AssetCheckResult(
            passed=passed,
            metadata={
                "location_count": int(location_count),
                "expected_count": expected_count,
            },
            description=f"Found {location_count}/{expected_count} locations",
        )
