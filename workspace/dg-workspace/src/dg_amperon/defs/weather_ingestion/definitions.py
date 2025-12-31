from dagster import (
    AssetSelection,
    DefaultScheduleStatus,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_modules,
    load_asset_checks_from_modules,
    build_schedule_from_partitioned_job,
    RunRequest,
)
from dagster_dlt import DagsterDltResource
from dg_amperon.defs.weather_ingestion import assets
from dg_amperon.defs.weather_ingestion.resources import create_duckdb_resource


# Job for weather observations (partitioned)
weather_observations_job = define_asset_job(
    name="weather_observations_job",
    selection=AssetSelection.assets(["weather_data", "weather_observations"]),
    description="Ingest weather observations from Tomorrow.io API (partitioned)",
    partitions_def=assets.hourly_partition,
)

# Hourly schedule for weather observations (partition-aware)
weather_hourly_schedule = build_schedule_from_partitioned_job(
    job=weather_observations_job,
    name="weather_hourly_schedule",
    description="Materializes latest weather observations partition every hour",
    default_status=DefaultScheduleStatus.STOPPED,
)
# Export definitions
defs = Definitions(
    assets=load_assets_from_modules([assets]),
    asset_checks=load_asset_checks_from_modules([assets]),
    jobs=[weather_observations_job],
    schedules=[weather_hourly_schedule],
    resources={
        "dlt": DagsterDltResource(),
        "duckdb_resource": create_duckdb_resource(),
    },
)
