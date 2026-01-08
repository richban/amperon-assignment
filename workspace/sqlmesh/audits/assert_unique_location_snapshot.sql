AUDIT (
  name assert_unique_location_snapshot,
  blocking FALSE
);

/*
  Critical Audit: Unique Location Snapshot

  Validates that weather_current contains exactly ONE row per location.
  This is a snapshot table showing "current" (nowcast) conditions.

  Expected: 1 row per location (10 total)

  If this audit fails, it indicates:
  - INCREMENTAL_BY_UNIQUE_KEY strategy is broken
  - Multiple observation timestamps leaked into the query
  - Data corruption or logic error

  This audit BLOCKS the pipeline if it fails.
*/

WITH location_counts AS (
  SELECT
    location_id,
    COUNT(*) as row_count,
    STRING_AGG(observed_at_utc::VARCHAR, ', ' ORDER BY observed_at_utc) as observation_times
  FROM @this_model
  GROUP BY location_id
  HAVING COUNT(*) > 1
)

SELECT
  location_id,
  row_count,
  row_count - 1 as duplicate_count,
  observation_times,
  'CRITICAL: Location ' || location_id::VARCHAR || ' has ' || row_count::VARCHAR || ' rows (expected 1)' as error_message
FROM location_counts;
