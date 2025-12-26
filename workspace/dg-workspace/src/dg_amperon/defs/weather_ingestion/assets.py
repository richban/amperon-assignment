from dagster import AssetExecutionContext
from dagster_dlt import DagsterDltResource, dlt_assets
from dg_amperon.defs.weather_ingestion.tomorrow_io_pipeline import tomorrow_io_source, create_pipeline
from dagster_dlt import DagsterDltTranslator
from dagster_dlt.translator import DltResourceTranslatorData

from dagster import AssetKey, AssetSpec

class CustomDagsterDltTranslator(DagsterDltTranslator):
    def get_asset_spec(self, data: DltResourceTranslatorData) -> AssetSpec:
        """Overrides asset spec to override asset key to be the dlt resource name."""
        default_spec = super().get_asset_spec(data)
        return default_spec.replace_attributes(
            key=AssetKey(f"{data.resource.name}"),
        )

@dlt_assets(
    name="weather_bronze",
    group_name="weather_ingestion",
    dlt_source=tomorrow_io_source(),
    dlt_pipeline=create_pipeline(),
    dagster_dlt_translator=CustomDagsterDltTranslator()
)
def weather_bronze_assets(
    context: AssetExecutionContext,
    dlt: DagsterDltResource,
):
    # Get backfill datetime from run config if provided
    run_config = context.run.run_config
    backfill_datetime = None

    if run_config and "ops" in run_config:
        op_config = run_config.get("ops", {}).get("weather_bronze", {}).get("config", {})
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
