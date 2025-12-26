# Dagster Weather Pipeline - Pythonic Approach with Hourly Schedule ✓

## Implementation Complete

Successfully implemented the Tomorrow.io weather data pipeline using the **Dagster Pythonic approach** with `dagster-dlt` integration, including an **hourly schedule**.

**Reference**: https://docs.dagster.io/integrations/libraries/dlt/dlt-pythonic

## Why Pythonic Approach?

The Pythonic approach provides **full Dagster feature support**:

✅ **Schedules** - Define cron-based schedules for automated runs
✅ **Jobs** - Create custom jobs with asset selections
✅ **Resources** - Configure DagsterDltResource for pipeline execution
✅ **Sensors** - Add event-driven triggers (future capability)
✅ **Python code** - Full programmatic control and customization

## Architecture

```
Tomorrow.io API
      ↓
DLT Source (tomorrow_io_source)
      ↓
@dlt_assets decorator
      ↓
DagsterDltResource.run()
      ↓
Dagster Assets (auto-generated from DLT resources)
      ├── dlt_tomorrow_io_source_locations
      ├── dlt_tomorrow_io_source_weather_observations
      └── tomorrow_io_source_locations (upstream)
      ↓
DuckDB (/data/weather.db)
```

## File Structure

```
dg-workspace/
├── src/dg_amperon/
│   ├── defs/
│   │   ├── etl/
│   │   │   ├── __init__.py
│   │   │   ├── assets.py             ← @dlt_assets decorator
│   │   │   ├── definitions.py        ← Jobs, schedules, resources
│   │   │   ├── tomorrow_io_pipeline.py ← DLT implementation
│   │   │   └── utils.py              ← Path utilities
│   │   └── __init__.py
│   └── definitions.py                ← Root (auto-loads from defs/)
├── .dlt/
│   ├── config.toml                   ← DLT config
│   └── secrets.toml                  ← API keys
└── pyproject.toml
```

## Key Files

### 1. `assets.py` - DLT Assets with @dlt_assets Decorator

```python
from dagster import AssetExecutionContext
from dagster_dlt import DagsterDltResource, dlt_assets
from .tomorrow_io_pipeline import tomorrow_io_source, create_pipeline

@dlt_assets(
    name="weather_bronze",
    group_name="weather_ingestion",
    dlt_source=tomorrow_io_source(),
    dlt_pipeline=create_pipeline(),
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
```

### 2. `definitions.py` - Jobs, Schedules, and Resources

```python
from dagster import (
    AssetSelection,
    DefaultScheduleStatus,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
)
from dagster_dlt import DagsterDltResource
from .assets import weather_bronze_assets

# DLT resource
dlt_resource = DagsterDltResource()

# Job for bronze layer ingestion
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
    assets=[weather_bronze_assets],
    jobs=[weather_bronze_job],
    schedules=[weather_hourly_schedule],
    resources={"dlt": dlt_resource},
)
```

### 3. `tomorrow_io_pipeline.py` - DLT Implementation

Key functions:
- `tomorrow_io_source(backfill_datetime)` - DLT source with bitemporal logic
- `create_pipeline()` - Creates DLT pipeline instance
- `get_normalized_run_timestamp()` - Normalizes time to hour boundary
- `get_absolute_time_params()` - Calculates API time window

## Generated Assets

Dagster automatically creates these assets from the DLT resources:

1. **`dlt_tomorrow_io_source_locations`**
   - DLT resource table: `weather_data.locations`
   - Schema: `id, name, lat, lon`

2. **`dlt_tomorrow_io_source_weather_observations`**
   - DLT resource table: `weather_data.weather_observations`
   - Schema: `_locations_id, start_time, run_timestamp, values__*`
   - Bitemporal model with composite PK

3. **`tomorrow_io_source_locations`**
   - Upstream source asset (dependency tracking)

## Hourly Schedule Configuration

**Schedule**: `weather_hourly_schedule`
- **Cron**: `0 * * * *` (every hour at :00 minute)
- **Timezone**: UTC
- **Default status**: STOPPED (start manually for safety)
- **Job**: `weather_bronze_job`
- **Target**: All assets in `weather_ingestion` group

## Usage

### List Definitions

```bash
dg list defs
```

Output:
```
┃ Schedules │ ┏━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━┓
┃           │ ┃ Key                     ┃ Cron      ┃
┃           │ ┡━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━┩
┃           │ │ weather_hourly_schedule │ 0 * * * * │
```

### Start the Schedule

```bash
# Via Dagster UI
dg dev
# Then navigate to Automation → Schedules → Toggle "weather_hourly_schedule" ON

# Or via CLI (when UI is running)
dagster schedule start weather_hourly_schedule
```

### Manual Run (One-off)

```bash
# Run the job immediately
dg launch --job weather_bronze_job

# Or run specific assets
dg launch --assets "dlt_tomorrow_io_source_locations,dlt_tomorrow_io_source_weather_observations"
```

### Backfill Historical Data

```bash
# Via launchpad config
dg launch --job weather_bronze_job --config-json '{
  "ops": {
    "weather_bronze": {
      "config": {"backfill_datetime": "2025-12-25T13:00:00"}
    }
  }
}'
```

## Schedule Behavior

### Idempotency Protection

The pipeline normalizes execution time to the hour boundary:

- Schedule triggers at **14:00:00** → `run_timestamp=14:00:00`
- Manual run at **14:23:45** → `run_timestamp=14:00:00` (same!)
- Retry at **14:58:12** → `run_timestamp=14:00:00` (overwrites previous)

**Result**: Multiple runs in the same hour **replace** data (no duplicates).

### Deterministic API Windows

Each `run_timestamp` maps to a fixed API window:

```
run_timestamp=2025-12-26T14:00:00
  ↓
API window: 2025-12-25T14:00:00 to 2025-12-31T14:00:00
            (run_ts - 24h)     (run_ts + 5d)
```

**Same input → Same output** (idempotent!)

### Expected Data

Each hourly run creates:
- **~145 hours** of forecast data per location
- **2 locations** × 145 hours = **~290 records** per run
- New run creates **new snapshot** with incremented `run_timestamp`

## Bitemporal Data Model

The pipeline tracks **two time dimensions**:

1. **`start_time`** (Forecast Timestamp)
   - WHEN the weather event actually occurs
   - Example: `2025-12-27T15:00:00` (3pm tomorrow)

2. **`run_timestamp`** (Observation Timestamp)
   - WHEN we made the forecast/observation
   - Example: `2025-12-26T14:00:00` (today at 2pm)

**Composite Primary Key**: `[_locations_id, start_time, run_timestamp]`

### Time-Travel Queries

```sql
-- What did we think the temperature would be at 3pm tomorrow,
-- as of the noon forecast today?
SELECT
    l.name,
    wo.values__temperature,
    wo.start_time as forecast_for,
    wo.run_timestamp as forecast_made_at
FROM weather_data.weather_observations wo
JOIN weather_data.locations l ON l.id = wo._locations_id
WHERE wo.start_time = '2025-12-27 15:00:00'
  AND wo.run_timestamp = '2025-12-26 12:00:00';

-- How has the forecast for tomorrow changed over the past 24 hours?
SELECT
    l.name,
    wo.run_timestamp as forecast_version,
    wo.values__temperature,
    wo.values__wind_speed
FROM weather_data.weather_observations wo
JOIN weather_data.locations l ON l.id = wo._locations_id
WHERE wo.start_time = '2025-12-27 15:00:00'
ORDER BY wo.run_timestamp DESC;
```

## Testing

### Verify Schedule Exists

```bash
$ dg list defs
┃ Schedules │ weather_hourly_schedule │ 0 * * * * │
```

### Test Manual Run

```bash
$ dg launch --job weather_bronze_job
✓ RUN_SUCCESS
```

### Verify Data in DuckDB

```bash
python -c "
import duckdb
conn = duckdb.connect('data/weather.db', read_only=True)
stats = conn.execute('''
    SELECT
        COUNT(*) as total_records,
        COUNT(DISTINCT run_timestamp) as unique_runs
    FROM weather_data.weather_observations
''').fetchone()
print(f'Total: {stats[0]}, Runs: {stats[1]}')
"
```

## Schedule Management

### Start Schedule (Production)

```bash
# 1. Start Dagster daemon
dg dev

# 2. In Dagster UI
# Navigate to: Automation → Schedules
# Find: weather_hourly_schedule
# Toggle: OFF → ON

# 3. Monitor executions
# Navigate to: Runs
# Filter by: Job = weather_bronze_job
```

### Stop Schedule

```bash
# In Dagster UI
# Navigate to: Automation → Schedules
# Toggle: ON → OFF
```

### Change Schedule Timing

Edit `definitions.py`:

```python
weather_hourly_schedule = ScheduleDefinition(
    name="weather_hourly_schedule",
    job=weather_bronze_job,
    cron_schedule="*/30 * * * *",  # Every 30 minutes
    # OR
    cron_schedule="0 */2 * * *",   # Every 2 hours
    # OR
    cron_schedule="0 0 * * *",     # Daily at midnight
    execution_timezone="UTC",
)
```

## Advantages of Pythonic Approach

| Feature | Pythonic ✅ | Component |
|---------|------------|-----------|
| Schedules | Full support | ❌ Limited |
| Jobs | Custom job definitions | Basic |
| Sensors | Full support | ❌ Not available |
| Resources | Explicit configuration | Automatic |
| Customization | Full Python control | YAML translation |
| Backfills | Via config | Via config |
| Code reuse | Import and compose | Reference strings |

## Summary

✅ **Hourly schedule created** - Cron: `0 * * * *` (UTC)
✅ **Job defined** - `weather_bronze_job`
✅ **Assets registered** - 3 DLT-generated assets
✅ **Resource configured** - `DagsterDltResource`
✅ **Bitemporal model** - Tracks forecast time + observation time
✅ **Idempotency** - Hour normalization prevents duplicates
✅ **Backfill support** - Via run config parameter

The pipeline is production-ready with automated hourly execution! 🚀

## Next Steps

1. **Start the schedule** in Dagster UI (currently STOPPED by default)
2. **Monitor first few runs** to verify behavior
3. **Scale to 10 locations** (currently 2 for testing)
4. **Add SQLMesh transformation layer** for marts
5. **Create data quality checks**
6. **Set up alerting** for failures
