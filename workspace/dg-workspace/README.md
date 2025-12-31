# Dagster Workspace - Weather Pipeline Implementation

> **Detailed technical documentation for the Dagster orchestration layer**

## Overview

This Dagster workspace implements the orchestration layer for the weather forecasting pipeline, managing:
- **Hourly partitioned ingestion** from Tomorrow.io API
- **Asset dependency tracking** between DLT bronze tables and SQLMesh marts
- **Automated scheduling** with partition-aware execution
- **Data quality checks** via asset checks
- **Retry policies** and error handling


## Architecture

### Asset Dependency Graph

![/DAG](../../docs/img/dag.png)

### Asset Definitions

```bash
┏━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ Section      ┃ Definitions                                                                                                                                       ┃
┡━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
│ Assets       │ ┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓ │
│              │ ┃ Key                               ┃ Group                   ┃ Deps                              ┃ Kinds   ┃ Description                       ┃ │
│              │ ┡━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩ │
│              │ │ /silver_weather                   │ sqlmesh_transformations │ weather_data/weather_observations │ duckdb  │ SQLMesh transformation assets for │ │
│              │ │                                   │                         │                                   │ sqlmesh │ weather data.                     │ │
│              │ ├───────────────────────────────────┼─────────────────────────┼───────────────────────────────────┼─────────┼───────────────────────────────────┤ │
│              │ │ /weather_current                  │ sqlmesh_transformations │ /silver_weather                   │ duckdb  │ SQLMesh transformation assets for │ │
│              │ │                                   │                         │ weather_data/locations            │ sqlmesh │ weather data.                     │ │
│              │ ├───────────────────────────────────┼─────────────────────────┼───────────────────────────────────┼─────────┼───────────────────────────────────┤ │
│              │ │ /weather_timeseries               │ sqlmesh_transformations │ /silver_weather                   │ duckdb  │ SQLMesh transformation assets for │ │
│              │ │                                   │                         │ weather_data/locations            │ sqlmesh │ weather data.                     │ │
│              │ ├───────────────────────────────────┼─────────────────────────┼───────────────────────────────────┼─────────┼───────────────────────────────────┤ │
│              │ │ locations_source_locations        │ default                 │                                   │         │                                   │ │
│              │ ├───────────────────────────────────┼─────────────────────────┼───────────────────────────────────┼─────────┼───────────────────────────────────┤ │
│              │ │ weather_data/locations            │ weather_ingestion       │ locations_source_locations        │ dlt     │                                   │ │
│              │ │                                   │                         │                                   │ duckdb  │     Seed resource: Static list of │ │
│              │ │                                   │                         │                                   │         │ locations to fetch weather data   │ │
│              │ │                                   │                         │                                   │         │ for.                              │ │
│              │ │                                   │                         │                                   │         │                                   │ │
│              │ │                                   │                         │                                   │         │     This is NOT a REST endpoi…    │ │
│              │ ├───────────────────────────────────┼─────────────────────────┼───────────────────────────────────┼─────────┼───────────────────────────────────┤ │
│              │ │ weather_data/weather_observations │ weather_ingestion       │ weather_data/locations            │ dlt     │                                   │ │
│              │ │                                   │                         │                                   │ duckdb  │         Inject                    │ │
│              │ │                                   │                         │                                   │         │ observation_timestamp into each   │ │
│              │ │                                   │                         │                                   │         │ weather observation.              │ │
│              │ │                                   │                         │                                   │         │                                   │ │
│              │ │                                   │                         │                                   │         │         Adds:                     │ │
│              │ │                                   │                         │                                   │         │         - observati…              │ │
│              │ └───────────────────────────────────┴─────────────────────────┴───────────────────────────────────┴─────────┴───────────────────────────────────┘ │
│ Asset Checks │ ┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓                   │
│              │ ┃ Key                                          ┃ Deps                   ┃ Description                                         ┃                   │
│              │ ┡━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩                   │
│              │ │ weather_data/locations:check_locations_count │ weather_data/locations │ Check that we have exactly 10 locations configured. │                   │
│              │ └──────────────────────────────────────────────┴────────────────────────┴─────────────────────────────────────────────────────┘                   │
│ Jobs         │ ┏━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓                                                     │
│              │ ┃ Key                      ┃ Description                                                    ┃                                                     │
│              │ ┡━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩                                                     │
│              │ │ weather_observations_job │ Ingest weather observations from Tomorrow.io API (partitioned) │                                                     │
│              │ └──────────────────────────┴────────────────────────────────────────────────────────────────┘                                                     │
│ Schedules    │ ┏━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━┓                                                                                                           │
│              │ ┃ Key                     ┃ Cron      ┃                                                                                                           │
│              │ ┡━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━┩                                                                                                           │
│              │ │ weather_hourly_schedule │ 0 * * * * │                                                                                                           │
│              │ └─────────────────────────┴───────────┘                                                                                                           │
│ Resources    │ ┏━━━━━━━━━━━━━━━━━┓                                                                                                                               │
│              │ ┃ Key             ┃                                                                                                                               │
│              │ ┡━━━━━━━━━━━━━━━━━┩                                                                                                                               │
│              │ │ dlt             │                                                                                                                               │
│              │ ├─────────────────┤                                                                                                                               │
│              │ │ duckdb_resource │                                                                                                                               │
│              │ ├─────────────────┤                                                                                                                               │
│              │ │ sqlmesh         │                                                                                                                               │
│              │ └─────────────────┘                                                                                                                               │
└──────────────┴───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```





### Partition Management

#### Hourly Partition Definition

- **`start_date=2025-12-30-12:00`**: First partition available (historical backfill start point)
- **`timezone="UTC"`**: Critical for consistent scheduling across environments
- **`end_offset=1`**: Makes partition available **immediately** when hour begins
  - Without this: Hour 16:00 only available at 17:00
  - With this: Hour 16:00 available at 16:00 (real-time processing)
- **Dagster format**: `"YYYY-MM-DD-HH:MM"`

## Installation

### Local Development

```bash
# 1. Clone repository
git clone <repo-url>
cd amperon

# 2. Set up Dagster workspace
cp .env.example .env
# Edit .env and add your TOMORROW_API_KEY and SOURCES__TOMORROW_IO_PIPELINE__TOMORROW_IO_ACCESS_TOKEN
source .env

# 3. Install dependencies
uv pip install -e .
uv pip install -e .[dev]
```

## Usage

```bash
# 4. Start Dagster UI
dg dev

# 5. Run pipeline manually via Web Console
# Open http://localhost:3000

# 6. Materialize all assets via dg cli
dg launch --assets "*" --partition-key "2025-12-30-12:00"
```

## Visualization

```bash
cd workspace/dg-workspace/src &&  marimo edit notebooks/weather_viz.py

# Open http://localhost:2718
```

![/MARIMO_VIZ](../../docs/img/marimo.png)


## Testing

### Unit Tests

```bash
# Run all tests
pytest tests/

# Unit Tests
pytest tests/unit/

# Integration Tests
pytest tests/integration/
```

## File Structure

```bash
dg-workspace/
├── src/
│   └── dg_amperon/
│       ├── defs/
│       │   ├── weather_ingestion/
│       │   │   ├── __init__.py
│       │   │   ├── assets.py                 # @dlt_assets definitions
│       │   │   ├── definitions.py            # Jobs, schedules, resources
│       │   │   ├── tomorrow_io_pipeline.py   # DLT source implementation
│       │   │   ├── resources.py              # DuckDB resource factory
│       │   │   └── utils.py                  # Helper functions
│       │   └── __init__.py
│       └── definitions.py                    # Root definitions (auto-loads)
├── tests/
│   ├── unit/
│   │   ├── test_partition_conversion.py
│   │   └── test_resources.py
│   ├── integration/
│   │   └── test_pipeline_e2e.py
│   └── conftest.py                           # Pytest fixtures
├── .dlt/
│   ├── config.toml                           # DLT configuration
│   └── secrets.toml                          # API credentials
├── .env.example                              # Environment template
├── pyproject.toml                            # Dependencies (uv)
└── README.md                                 # This file
```


## References

- [Dagster Docs - Schedules](https://docs.dagster.io/concepts/partitions-schedules-sensors/schedules)
- [Dagster Docs - Partitions](https://docs.dagster.io/concepts/partitions-schedules-sensors/partitions)
- [DLT Docs - Dagster Integration](https://dlthub.com/docs/dlt-ecosystem/verified-sources/dagster)
- [SQLMesh Docs](https://sqlmesh.readthedocs.io/)

---

**For high-level architecture overview, see** [/README.md](../../README.md)
