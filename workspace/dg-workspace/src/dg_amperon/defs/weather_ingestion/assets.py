from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetCheckResult,
    AssetCheckSeverity,
    asset_check,
)
from dagster_dlt import DagsterDltResource, dlt_assets, DagsterDltTranslator
from dg_amperon.defs.weather_ingestion.tomorrow_io_pipeline import (
    tomorrow_io_source,
    create_pipeline,
)
import duckdb
from pathlib import Path

# Get database path
DB_PATH = (
    Path(__file__).parent.parent.parent.parent.parent.parent / "data" / "weather.db"
)

class CustomDagsterDltTranslator(DagsterDltTranslator):
    def get_asset_key(self, resource: DagsterDltResource) -> AssetKey:
        """Overrides asset key to be the dlt resource name."""
        return AssetKey(["weather_data", resource.name])


@dlt_assets(
    name="weather_bronze",
    group_name="weather_ingestion",
    dlt_source=tomorrow_io_source(),
    dlt_pipeline=create_pipeline(),
    dagster_dlt_translator=CustomDagsterDltTranslator(),
)
def weather_bronze_assets(
    context: AssetExecutionContext,
    dlt: DagsterDltResource,
):
    # Get backfill datetime from run config if provided
    run_config = context.run.run_config
    backfill_datetime = None

    if run_config and "ops" in run_config:
        op_config = (
            run_config.get("ops", {}).get("weather_bronze", {}).get("config", {})
        )
        backfill_datetime = op_config.get("backfill_datetime")

    # Create source with optional backfill support
    if backfill_datetime:
        context.log.info(f"Running in backfill mode: {backfill_datetime}")
        source = tomorrow_io_source(backfill_datetime=backfill_datetime)
    else:
        context.log.info("Running in scheduled mode")
        source = tomorrow_io_source()

    # Run DLT pipeline via DagsterDltResource
    yield from dlt.run(context=context, dlt_source=source)


@asset_check(asset=AssetKey(["weather_data", "locations"]))
def check_locations_count(context: AssetExecutionContext) -> AssetCheckResult:
    """Check that we have exactly 10 locations configured."""
    conn = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        result = conn.execute("""
            SELECT COUNT(*) as location_count
            FROM weather_data.locations
        """).fetchone()

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
    finally:
        conn.close()
