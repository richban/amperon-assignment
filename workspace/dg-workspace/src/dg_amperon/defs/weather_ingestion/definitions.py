from dagster import (
    AssetSelection,
    DefaultScheduleStatus,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_modules,
    load_asset_checks_from_modules,
)
from dagster_dlt import DagsterDltResource
from dg_amperon.defs.weather_ingestion import assets
from dg_amperon.defs.weather_ingestion.resources import create_duckdb_resource


weather_bronze_job = define_asset_job(
    name="weather_bronze_job",
    selection=AssetSelection.groups("weather_ingestion"),
    description="Ingest weather data from Tomorrow.io API to DuckDB bronze tables",
)

# Hourly schedule (runs at :00 minute every hour)
weather_hourly_schedule = ScheduleDefinition(
    name="weather_hourly_schedule",
    job=weather_bronze_job,
    cron_schedule="0 * * * *",
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.STOPPED,  # Start manually
    description="Runs weather ingestion pipeline every hour at :00 minute (UTC)",
)
# Export definitions
defs = Definitions(
    assets=load_assets_from_modules([assets]),
    asset_checks=load_asset_checks_from_modules([assets]),
    jobs=[weather_bronze_job],
    schedules=[weather_hourly_schedule],
    resources={"dlt": DagsterDltResource(), "duckdb_resource": create_duckdb_resource()},
)
