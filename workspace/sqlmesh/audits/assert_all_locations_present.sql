AUDIT (
  name assert_all_locations_present,
  blocking TRUE
);

/*
  Critical Audit: Location Completeness

  Ensures that all 10 locations have data in the model.
  If any location is missing, this indicates:
  - API returned partial data
  - Location-specific pipeline failure
  - Data quality filters removed entire location

  This audit BLOCKS the pipeline if it fails.
*/

WITH location_counts AS (
  SELECT COUNT(DISTINCT location_id) as location_count
  FROM @this_model
)

SELECT
  location_count,
  10 - location_count AS missing_location_count,
  'Expected 10 locations, found ' || location_count::VARCHAR AS error_message
FROM location_counts
WHERE location_count < 10;
