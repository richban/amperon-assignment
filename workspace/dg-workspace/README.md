# Dagster Workspace - Weather Pipeline Implementation

> **Detailed technical documentation for the Dagster orchestration layer**

## Overview

This Dagster workspace implements the orchestration layer for the weather forecasting pipeline, managing:
- **Hourly partitioned ingestion** from Tomorrow.io API
- **Asset dependency tracking** between DLT bronze tables and SQLMesh marts
- **Automated scheduling** with partition-aware execution
- **Data quality checks** via asset checks
- **Retry policies** and error handling

---

## Architecture

### Asset Dependency Graph


### Partition Management

#### Hourly Partition Definition

- **`start_date`**: First partition available (historical backfill start point)
- **`timezone="UTC"`**: Critical for consistent scheduling across environments
- **`end_offset=1`**: Makes partition available **immediately** when hour begins
  - Without this: Hour 16:00 only available at 17:00
  - With this: Hour 16:00 available at 16:00 (real-time processing)
- **Dagster format**: `"YYYY-MM-DD-HH:MM"`
- **Example**: `"2025-12-30-16:00"`


## Testing

### Unit Tests
```bash
-- Unit Tests
pytest tests/unit/

-- Integration Tests
pytest tests/integration/test_pipeline_e2e.py
```


## File Structure

```
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
