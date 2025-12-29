MODEL (
  name weather_current,
  kind FULL,
  dialect duckdb,
  description 'Mart: Latest weather conditions per location (answers Q1)',
  owner 'data_team',
  cron '@hourly'
);

/*
  Mart Model: Current Weather per Location

  Answers Assignment Question 1:
  "What is the current temperature and wind speed for each location?"

  This model provides the most recent weather observation for each location,
  using the latest observation_timestamp to get the freshest forecast.
*/

WITH latest_per_location AS (
  SELECT
    bw.location_id,
    bw.forecast_timestamp_utc,
    bw.observation_timestamp_utc,
    bw.temperature_celsius,
    bw.wind_speed_mps,
    bw.humidity_percent,
    bw.weather_code,
    bw.precipitation_type_label,
    l.timezone,
    ROW_NUMBER() OVER (
      PARTITION BY bw.location_id
      ORDER BY bw.observation_timestamp_utc DESC, bw.forecast_timestamp_utc DESC
    ) AS recency_rank
  FROM bronze_weather bw
  JOIN weather_data.locations l ON bw.location_id = l.id
  WHERE bw.has_invalid_temperature = false
    AND bw.has_invalid_wind = false
)

SELECT
  location_id,
  
  -- UTC timestamps (source of truth)
  forecast_timestamp_utc AS forecast_time_utc,
  observation_timestamp_utc AS observed_at_utc,
  
  -- Local timestamps (converted using location timezone)
  timezone(timezone, forecast_timestamp_utc) AS forecast_time_local,
  timezone(timezone, observation_timestamp_utc) AS observed_at_local,
  
  -- Weather metrics
  temperature_celsius AS current_temperature_c,
  wind_speed_mps AS current_wind_speed_mps,
  ROUND(wind_speed_mps * 3.6, 1) AS current_wind_speed_kmh,  -- Convert m/s to km/h
  humidity_percent AS current_humidity_pct,
  weather_code,
  precipitation_type_label,
  
  -- Metadata
  CURRENT_TIMESTAMP AS refreshed_at
FROM latest_per_location
WHERE recency_rank = 1
ORDER BY location_id;
