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
    location_id,
    forecast_timestamp_utc,
    observation_timestamp_utc,
    temperature_celsius,
    wind_speed_mps,
    humidity_percent,
    weather_code,
    precipitation_type_label,
    ROW_NUMBER() OVER (
      PARTITION BY location_id
      ORDER BY observation_timestamp_utc DESC, forecast_timestamp_utc DESC
    ) AS recency_rank
  FROM bronze_weather
  WHERE has_invalid_temperature = false
    AND has_invalid_wind = false
)

SELECT
  location_id,
  forecast_timestamp_utc AS forecast_time,
  observation_timestamp_utc AS observed_at,
  temperature_celsius AS current_temperature_c,
  wind_speed_mps AS current_wind_speed_mps,
  ROUND(wind_speed_mps * 3.6, 1) AS current_wind_speed_kmh,  -- Convert m/s to km/h
  humidity_percent AS current_humidity_pct,
  weather_code,
  precipitation_type_label,
  CURRENT_TIMESTAMP AS refreshed_at
FROM latest_per_location
WHERE recency_rank = 1
ORDER BY location_id;
