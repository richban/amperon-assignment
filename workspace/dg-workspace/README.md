# Dagster DLT Component Approach - Implementation Complete ✓

## What We Built

Successfully implemented the Tomorrow.io weather data pipeline using the **official Dagster Component approach** with `DltLoadCollectionComponent`.

**Reference**: https://docs.dagster.io/integrations/libraries/dlt

## Why Component-Based Approach?

The component-based approach is the **recommended modern approach** by Dagster for dlt integrations:

✅ **Declarative YAML configuration** - Define loads in `defs.yaml`
✅ **Automatic asset generation** - Dagster infers assets from DLT resources
✅ **Less boilerplate code** - No need to write `@dlt_assets` decorators manually
✅ **Better separation of concerns** - DLT code stays in `loads.py`, config in YAML
✅ **Scaffolding support** - Use `dg scaffold defs` to generate structure

## Architecture

```
Tomorrow.io API
      ↓
DLT Source (tomorrow_io_source)
      ↓
DLT Pipeline (create_pipeline)
      ↓
DltLoadCollectionComponent (defs.yaml)
      ↓
Dagster Assets (auto-generated)
      ├── tomorrow_io_source_locations (source asset)
      ├── weather_data/locations (table)
      └── weather_data/weather_observations (table)
      ↓
DuckDB (/data/weather.db)
```

## File Structure

```
dg-workspace/
├── src/dg_amperon/
│   ├── defs/
│   │   ├── weather_ingestion/
│   │   │   ├── defs.yaml              ← Component definition
│   │   │   ├── loads.py               ← DLT sources & pipelines
│   │   │   ├── tomorrow_io_pipeline.py ← DLT implementation
│   │   │   └── utils.py               ← Utilities
│   │   └── __init__.py
│   └── definitions.py                 ← Root (auto-loads components)
├── .dlt/
│   ├── config.toml                    ← DLT config
│   └── secrets.toml                   ← API keys
└── pyproject.toml
```

## Key Files

### 1. `defs.yaml` - Component Configuration

```yaml
type: dagster_dlt.DltLoadCollectionComponent

attributes:
  loads:
    - source: .loads.weather_source
      pipeline: .loads.weather_pipeline
      translation:
        group_name: weather_ingestion
        description: "Loads weather forecasts from Tomorrow.io API to DuckDB bronze tables"
```

**What this does:**
- Tells Dagster to create a `DltLoadCollectionComponent`
- Points to the DLT source and pipeline in `loads.py`
- Sets grouping and description for generated assets

### 2. `loads.py` - DLT Source & Pipeline

```python
import dlt
from .tomorrow_io_pipeline import tomorrow_io_source, create_pipeline

# Weather data source - fetches from Tomorrow.io API
weather_source = tomorrow_io_source()

# DLT pipeline configuration - loads to DuckDB
weather_pipeline = create_pipeline()
```

**What this does:**
- Imports the existing DLT pipeline implementation
- Exposes `weather_source` and `weather_pipeline` for the component to use

### 3. `tomorrow_io_pipeline.py` - DLT Implementation

Contains:
- `tomorrow_io_source()` - DLT source with bitemporal logic
- `create_pipeline()` - DLT pipeline to DuckDB
- Time normalization functions for idempotency
- Weather fields configuration

**Unchanged from original implementation** - all the complex logic stays here!

## Generated Assets

When Dagster loads the component, it automatically creates:

1. **`tomorrow_io_source_locations`** - Source asset (upstream dependency)
   - Represents the locations seed data

2. **`weather_data/locations`** - Table asset
   - Dimension table with location metadata
   - Schema: `id, name, lat, lon`

3. **`weather_data/weather_observations`** - Table asset
   - Fact table with bitemporal weather data
   - Schema: `_locations_id, start_time, run_timestamp, values__*`

## Usage

### List Definitions

```bash
dg list defs
```

### Materialize Assets

```bash
# Materialize all weather assets
dg launch --assets "weather_data/locations,weather_data/weather_observations"

# Or materialize everything
dg launch --assets "*"
```

### Start Dagster UI

```bash
dg dev
```

Then open http://localhost:3000

## Bitemporal Model (Unchanged)

The component approach preserves all our bitemporal logic:

- **`start_time`**: When the weather event occurs (forecast timestamp)
- **`run_timestamp`**: When we made the observation (normalized to hour)
- **Composite PK**: `[_locations_id, start_time, run_timestamp]`

### Hour Normalization for Idempotency

- Running at 13:05, 13:15, or 13:59 all → `run_timestamp=13:00:00`
- Same hour runs **replace** data (not duplicate)
- Deterministic API time windows based on `run_timestamp`

## Advantages Over Pythonic Approach

| Feature | Component Approach ✅ | Pythonic Approach |
|---------|---------------------|-------------------|
| Configuration | Declarative YAML | Python decorators |
| Boilerplate | Minimal | More code needed |
| Asset generation | Automatic | Manual spec |
| Scaffolding | `dg scaffold defs` | Manual setup |
| DLT code coupling | Loose (just import) | Tight (decorators) |
| Customization | Translation layer | Custom translator class |

## Testing

### Test Asset Listing

```bash
$ dg list defs
┃ Assets  │ ┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━┓ │
│         │ ┃ Key                               ┃ Gro… ┃ │
│         │ ┡━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━┩ │
│         │ │ tomorrow_io_source_locations      │ def… │ │
│         │ │ weather_data/locations            │ wea… │ │
│         │ │ weather_data/weather_observations │ wea… │ │
```

### Test Materialization

```bash
$ dg launch --assets "weather_data/locations,weather_data/weather_observations"
✓ RUN_SUCCESS - Finished execution of run
```

### Verify Data

```sql
SELECT COUNT(*) FROM weather_data.weather_observations;
-- Result: 870 records (3 runs × 290 records/run)
```
