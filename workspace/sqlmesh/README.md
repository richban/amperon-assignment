## Data Quality Audits

SQLMesh enforces data quality through **declarative audits**:

### Model: `silver_weather`
```sql
audits [
  ASSERT_NOT_NULL(column_name := location_id),
  ASSERT_NOT_NULL(column_name := forecast_timestamp_utc),
  ASSERT_NOT_NULL(column_name := observation_timestamp_utc),
  ASSERT_NOT_NULL(column_name := temperature_celsius),
  ASSERT_NOT_NULL(column_name := wind_speed_mps),
  ASSERT_UNIQUE_KEY(key_column := _dlt_id)
]
```

### Model: `weather_current`
```sql
audits [
  ASSERT_UNIQUE_KEY(key_column := location_id),
  ASSERT_NOT_NULL(column_name := current_temperature_c),
  ASSERT_NOT_NULL(column_name := current_wind_speed_mps),
  assert_all_locations_present,      -- Custom: ensures 10 locations
  assert_unique_location_snapshot    -- Custom: no duplicates per location
]
```

### Model: `weather_timeseries`
```sql
audits [
  ASSERT_NOT_NULL(column_name := location_id),
  ASSERT_NOT_NULL(column_name := forecast_timestamp_utc),
  ASSERT_NOT_NULL(column_name := temperature_celsius),
  ASSERT_NOT_NULL(column_name := wind_speed_mps),
  assert_all_locations_present,          -- 10 locations required
  assert_timeseries_window_complete      -- 144 rows per location (±10 tolerance)
]
```

**Custom Audits** (see `workspace/sqlmesh/audits/`):
- `assert_all_locations_present.sql`: Validates all 10 locations exist
- `assert_timeseries_window_complete.sql`: Validates 144-hour window per location
- `assert_unique_location_snapshot.sql`: Validates 1 row per location in weather_current

---