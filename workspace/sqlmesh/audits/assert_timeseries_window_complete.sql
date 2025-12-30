AUDIT (
  name assert_timeseries_window_complete,
  blocking FALSE
);

/*
  Critical Audit: Forecast Window Completeness

  Validates that each location has the expected 144 hourly observations:
  - 24 hours of historical data (backcasted)
  - 120 hours of future forecasts (5 days)

  Tolerance: ±10 rows allowed to handle:
  - API occasionally returning slightly different ranges
  - Timezone edge cases
  - Data arriving with minor delays

  Expected: 144 rows per location
  Acceptable: 134-154 rows per location
  Fails if: < 134 rows (missing significant data)

  This audit WARNS but does not block the pipeline.
*/

WITH location_row_counts AS (
  SELECT
    location_id,
    COUNT(*) as row_count,
    MIN(forecast_timestamp_utc) as min_forecast_time,
    MAX(forecast_timestamp_utc) as max_forecast_time,
    -- Calculate hours coverage
    EXTRACT(EPOCH FROM (MAX(forecast_timestamp_utc) - MIN(forecast_timestamp_utc)))/3600 as hours_coverage
  FROM @this_model
  GROUP BY location_id
)

SELECT
  location_id,
  row_count,
  144 - row_count as missing_rows,
  min_forecast_time,
  max_forecast_time,
  hours_coverage,
  CASE
    WHEN row_count < 134 THEN 'CRITICAL: Missing ' || (144 - row_count)::VARCHAR || ' rows'
    WHEN row_count < 144 THEN 'WARNING: Slightly incomplete, missing ' || (144 - row_count)::VARCHAR || ' rows'
    WHEN row_count > 154 THEN 'WARNING: Too many rows, ' || (row_count - 144)::VARCHAR || ' extra'
    ELSE 'OK'
  END as status
FROM location_row_counts
WHERE row_count < 134 OR row_count > 154;
