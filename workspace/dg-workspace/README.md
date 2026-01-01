# Dagster Workspace - Orchestration Layer

> **Technical documentation for the Dagster orchestration layer with focus on design decisions, development workflow, and operational patterns**

## Introduction

This workspace implements the **orchestration layer** for the weather forecasting pipeline using Dagster. Rather than just scheduling jobs, it treats **data assets as first-class citizens**, enabling:

- **Declarative dependency management**: Assets declare what they depend on; Dagster figures out execution order
- **Partition-aware scheduling**: Automatically tracks which time slices have been processed
- **Asset lineage visualization**: See the complete data flow from API to marts
- **Incremental materialization**: Only process what's changed or missing
- **Built-in observability**: Logs, metadata, and execution history without custom instrumentation

### Why Dagster for This Pipeline?

**Traditional schedulers** (cron, Airflow) think in terms of **jobs**: "Run this script at 9am."

**Dagster** thinks in terms of **data assets**: "The weather_timeseries mart depends on silver_weather. If silver_weather is updated, downstream assets become stale."

This asset-oriented mindset provides:
- **Automatic backfill intelligence**: Dagster knows which partitions are missing
- **Cross-job dependencies**: Schedule-driven runs can depend on sensor-driven runs
- **Type-safe resource management**: DuckDB connections are managed by Dagster, not manual open/close
- **Built-in testing**: Mock resources in tests without changing production code

### Overall Pipeline

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


This workspace bridges **data generation** (DLT ingestion) and **data transformation** (SQLMesh marts) while providing a unified interface for monitoring and backfill management.


## Orchestration Philosophy

### 1. Asset-Oriented Thinking

**Traditional approach** (operation-based):
- "Run `ingest_weather.py` every hour"
- "Then run `transform_silver.sql`"
- "Finally run `build_marts.sql`"

**Dagster approach** (asset-based):
- "The `weather_observations` asset has hourly partitions"
- "The `silver_weather` asset depends on `weather_observations`"
- "When `weather_observations` updates, `silver_weather` becomes stale"

**Why this matters**: Assets **declare dependencies**; Dagster **resolves execution order**. You never manually chain tasks—Dagster determines the correct DAG automatically. No manual chaining. No `depends_on` flags. **Declarative dependency management.**


### 2. Partition-Aware Execution

**Core concept**: Each hourly partition is an **independent unit of work**. Dagster tracks materialization state **per partition**, not just per asset.

**Partition definition** (assets.py:55):
```python
hourly_partition = HourlyPartitionsDefinition(
    start_date="2025-12-30-12:00",
    timezone="UTC",
    end_offset=1  # ← Critical for real-time processing
)
```

**What `end_offset=1` means**:
- **Without it**: Partition `2025-12-30-16:00` only becomes available at `17:00` (after hour completes)
- **With it**: Partition `2025-12-30-16:00` becomes available at `16:00` (immediately)

**Why this matters**: Weather forecasts are published in real-time. If we wait for the hour to complete, we're always 1 hour behind. With `end_offset=1`, we can process forecasts as soon as they're published.

---

### 3. Backfill Strategy

**Decision**: Use `BackfillPolicy.single_run()` for weather observations (assets.py:89).

**What this means**: When backfilling multiple partitions, Dagster processes **one partition at a time sequentially**, not in parallel.

**Why this decision**:
1. **API rate limits**: Tomorrow.io API has rate limits; parallel requests would cause 429 errors
2. **DuckDB single-writer**: DuckDB can only handle one write transaction at a time (see Execution Strategy below)
3. **Predictable resource usage**: Sequential processing ensures consistent memory/CPU usage

**Alternative considered**: `BackfillPolicy.multi_run()` would launch parallel runs for each partition. This would be faster but:
- Risk API throttling
- Risk DuckDB write conflicts
- Harder to debug (interleaved logs)

**Tradeoff**: Slower backfills (10 partitions = 10 sequential runs) but **guaranteed reliability**.

---

### 4. Execution Strategy: In-Process Executor

**Critical decision** (definitions.py:41):
```python
executor=in_process_executor
```

**What this means**: All assets within a single run execute **sequentially in the same process**, not in parallel subprocesses.

**Why this is necessary**: **DuckDB is single-writer**. If two assets try to write to the same DuckDB file simultaneously, one will fail with a lock error.

**Without in_process_executor**:
```
[locations] → DuckDB write (process 1)
[check_locations_count] → DuckDB read (process 2)  ← Lock conflict!
[weather_observations] → DuckDB write (process 3)  ← Lock conflict!
```

**With in_process_executor**:
```
[locations] → DuckDB write → completes
  ↓
[check_locations_count] → DuckDB read → completes
  ↓
[weather_observations] → DuckDB write → completes
```

**Tradeoff**: **Slower execution** (sequential, not parallel) but **zero lock conflicts**. This is acceptable for this MVP:


### 5. Asset Separation: Locations vs. Observations

**Decision**: Define `locations` and `weather_observations` as **two separate `@dlt_assets`** (assets.py:63-77, 82-118).

**Why not one asset?**:
- **Locations is static** (10 rows, no partitions, materialized once)
  - Contains enrichment fields like `timezone`, `name`, `lat`, `lon`
  - Local timestamp conversion: Using the timezone field from locations to convert UTC timestamps to local time
- **Observations is dynamic** (hourly partitions, ~1,450 rows per run, materialized continuously)

**If combined**:
- Locations would inherit hourly partitioning (conceptually wrong—locations don't change per hour)

**Separate assets provide**:
- **Correct semantics**: Locations materialized once or on-demand; observations materialized hourly
- **Clear lineage**: Dependency graph shows `locations` → `weather_observations`


## Key Design Decisions

### 1. Schedule vs. Sensor

**Decision**: Use `build_schedule_from_partitioned_job()` (definitions.py:25-30) instead of sensors.

**Schedule approach** (current):
- **Cron-based**: Runs at `0 * * * *` (top of every hour)
- **Partition-aware**: Automatically materializes latest partition
- **Predictable**: Execution time is deterministic

**Sensor approach** (alternative):
- **Event-driven**: Poll Tomorrow.io API or check for new data
- **Reactive**: Trigger immediately when data available
- **Unpredictable**: Execution time depends on external events

**Why schedule was chosen**:
- Tomorrow.io publishes forecasts on a predictable hourly cadence
- No advantage to sub-hourly polling (data freshness is hourly)
- Simpler operational model (no sensor backpressure, no poll interval tuning)

**When to use sensors instead**:
- Data source publishes at irregular intervals
- Need to trigger downstream jobs only when upstream completes (cross-job dependencies)
- Want to react to external events (S3 uploads, webhooks)


### 2. Asset Checks vs. Audits

**Decision**: Use Dagster asset checks (assets.py:121-145) for immediate validation; use SQLMesh audits for batch validation.

**Asset checks** (Dagster):
- Run **during materialization** as part of the asset execution
- **Blocking**: If check fails, downstream assets don't run
- **Immediate feedback**: Know within seconds if data is invalid

**Audits** (SQLMesh):
- Run **after transformation** as part of `sqlmesh plan`
- **Non-blocking**: Audit failures don't stop pipeline (unless configured)
- **Batch validation**: Check entire dataset, not just new partitions

**Current usage**:
- **Asset check**: `check_locations_count` ensures 10 locations before processing observations
- **SQLMesh audits**: `assert_timeseries_window_complete` validates 144 rows per location after transformation

**Complementary relationship**: Asset checks protect against bad inputs; audits validate transformation logic.


## Development Workflow

### Understanding the Dagster UI

The Dagster web interface provides several critical views:

**Asset Catalog** (`/assets`):
- See all assets and their dependencies
- Check last materialization time (data freshness)
- Identify stale assets (dependencies updated but asset not re-materialized)

**Lineage Graph** (`/asset-groups`):
- Visual dependency graph (DAG)
- Click assets to see metadata, checks, partition status
- Understand data flow from source to marts

**Partition Grid** (`/assets/weather_data/weather_observations?view=partitions`):
- Matrix view: partitions (columns) × materialization status (rows)
- Colors: Green (success), Red (failure), Gray (not run)
- Identify gaps in partition coverage at a glance

**Runs** (`/runs`):
- Execution history for all jobs
- Filter by status, asset, partition
- Click into individual runs to see logs, timings, metadata

---

### Testing Changes Locally

**Unit tests** (`tests/unit/`):
- Test partition key conversion logic (no Dagster)
- Test resource configuration (no database)
- Fast feedback (milliseconds)

**Integration tests** (`tests/integration/`):
- Test full asset materialization (with Dagster)
- Use in-memory DuckDB (`:memory:`)
- Mock Tomorrow.io API responses (fixtures/mock_responses.py)

**Local materialization** (via UI):
- Start webserver with `dg dev`
- Materialize specific partitions
- Inspect database directly with DuckDB CLI

**Philosophy**: Test transformations in isolation (unit tests), then validate integration (integration tests), finally verify end-to-end (local materialization).

---

### Debugging Failed Runs

**Step 1: Check run logs**:
- Navigate to Runs → Click failed run
- Expand logs for specific asset
- Look for exception traceback

**Step 2: Identify failure type**:
- **API error** (4xx/5xx): Check API credentials, rate limits
- **Schema error** (DLT): Check DLT schema evolution logs
- **SQL error** (SQLMesh): Check query syntax, missing columns
- **Lock error** (DuckDB): Check for concurrent writes (should not happen with `in_process_executor`)

**Step 3: Reproduce locally**:
- Re-materialize same partition via UI
- Check if failure is transient (API timeout) or persistent (code bug)

**Step 4: Fix and retry**:
- If transient: Use "Re-execute" in UI (retries same partition)
- If persistent: Fix code, re-materialize

**Common errors**:
- **Partition not found**: Backfill hasn't run yet (expected for historical partitions)
- **Dependency not materialized**: Upstream asset missing (run upstream first)
- **Asset check failed**: Data quality issue (inspect check metadata for details)

---

## Operational Patterns

### Managing Schedules

**Starting the webserver**:
The local development server (`dg dev`) runs Dagster's web interface and daemon. The daemon is responsible for:
- Evaluating schedules on cron intervals
- Launching runs for new partitions
- Managing sensors (if configured)

**Schedule status**:
By default, `weather_hourly_schedule` has `default_status=DefaultScheduleStatus.STOPPED` (definitions.py:29). This prevents automatic execution in local development.

**Enabling schedule**:
In the UI, navigate to Schedules → `weather_hourly_schedule` → Toggle "On". The daemon will now launch runs every hour.

**Disabling schedule**:
Toggle "Off" to stop automatic execution. Existing runs will complete; new runs won't launch.

**Manual execution**:
Even with schedule stopped, you can manually materialize partitions via the UI or CLI. The schedule only controls **automatic** execution.


### Handling Failed Partitions

**Transient failures** (API timeout, network error):
- Re-execute the run via UI
- Dagster will retry the same partition with same configuration

**Persistent failures** (bad data, code bug):
- Fix the underlying issue
- Delete failed partition data from DuckDB (if necessary)
- Re-materialize partition

**Partial failures** (some assets succeeded, some failed):
- Dagster tracks materialization per asset, not per run
- If `locations` succeeded but `weather_observations` failed, retrying will skip `locations` (already materialized)

**Blocking failures** (asset check failed):
- If `check_locations_count` fails, `weather_observations` won't run (check is `blocking=True`)
- Fix upstream issue (e.g., add missing locations)
- Re-run check + downstream assets


### Partition Mapping

**Current partitioning**:
- `weather_observations`: Hourly partitions
- `silver_weather`: Inherits hourly partitions from `weather_observations`
- `weather_current`, `weather_timeseries`: Not partitioned (FULL refresh)

**Why marts aren't partitioned**:
- `weather_current`: Snapshot table (1 row per location, upsert strategy)
- `weather_timeseries`: Sliding window (rebuilds entire 144-hour window each run)


## Asset Dependency Graph

![/DAG](../../docs/img/dag.png)

**Reading the graph**:
- **Nodes**: Data assets (tables, views)
- **Edges**: Dependencies (arrows point from upstream to downstream)
- **Colors**: Asset groups (weather_ingestion, sqlmesh_transformations)
- **Shapes**: Asset types (DLT sources, SQLMesh models)


## Getting Started (Local Development)

### Installation

The workspace requires Python 3.11+ and uses `uv` for dependency management. The environment is configured via `.env` file with database path and API credentials.

**Initial setup**:
1. Copy environment template and configure credentials
2. Install Python dependencies (base + dev extras)
3. Verify DuckDB database exists or will be created automatically

**Starting the webserver**:
Run `dg dev` in this directory to start both the web UI and the scheduler daemon. The UI will be available at `http://localhost:3000`.

**First materialization**:
Navigate to Assets → `weather_data/locations` → Materialize (no partition needed). This creates the location master table. Then materialize a single partition of `weather_data/weather_observations` to verify end-to-end flow.


## Workspace Structure

```bash
dg-workspace/
├── src/dg_amperon/
│   ├── defs/
│   │   ├── weather_ingestion/
│   │   │   ├── assets.py                 # Asset definitions (@dlt_assets, @asset_check)
│   │   │   ├── definitions.py            # Jobs, schedules, resources, executor config
│   │   │   ├── tomorrow_io_pipeline.py   # DLT source implementation (API client)
│   │   │   ├── resources.py              # DuckDB resource factory
│   │   │   └── utils.py                  # Partition conversion helpers
│   │   ├── sqlmesh/
│   │   │   ├── assets.py                 # SQLMesh asset integration
│   │   │   └── definitions.py            # SQLMesh-specific configuration
│   │   └── __init__.py
│   ├── definitions.py                    # Root definitions (auto-discovery)
│   └── notebooks/
│       ├── weather_observations.py       # Data exploration (Marimo)
│       └── weather_viz.py                # Interactive visualization (Marimo + Lonboard)
├── tests/
│   ├── unit/
│   │   ├── test_resources.py             # Resource configuration tests
│   │   └── test_source.py                # DLT source logic tests
│   ├── integration/
│   │   └── test_pipeline_e2e.py          # Full pipeline tests (mocked API)
│   ├── fixtures/
│   │   ├── mock_responses.py             # Tomorrow.io API fixtures
│   │   └── sample_data.json              # Test data
│   └── conftest.py                       # Shared fixtures (in-memory DB)
├── .dlt/
│   ├── config.toml                       # DLT pipeline configuration
│   └── secrets.toml                      # API credentials (gitignored)
├── .env.example                          # Environment variable template
├── pyproject.toml                        # Dependencies (uv managed)
└── README.md                             # This file
```

**Key files explained**:

- **assets.py**: Where assets are defined using decorators (`@dlt_assets`, `@asset_check`). This is where you declare what data artifacts exist and how they depend on each other.

- **definitions.py**: Where jobs, schedules, and resources are configured. This is where you define execution policies (`in_process_executor`), backfill strategies, and cron schedules.

- **tomorrow_io_pipeline.py**: The DLT source implementation. This contains the actual API client logic—rate limiting, retry logic, response parsing. Dagster orchestrates this, but doesn't know its internals.

- **resources.py**: Resource factories that create configured instances of DuckDB connections, DLT pipelines, etc. Resources are dependency-injected into assets at runtime.

- **conftest.py**: Pytest fixtures for testing. Provides in-memory databases, mocked API responses, and shared test utilities. **Critical for fast test feedback** (no real API calls, no disk I/O).

---

**For architecture overview and data model details**, see [Top-level README](../../README.md)
**For transformation logic and audit framework**, see [SQLMesh README](../sqlmesh/README.md)
