MODEL (
  name weather_timeseries,
  kind FULL,
  dialect duckdb,
  description 'Mart: Hourly weather timeseries per location (answers Q2)',
  owner 'data_team',
  cron '@hourly'
);

/*
  Mart Model: Weather Timeseries per Location

  Answers Assignment Question 2:
  "What is the hourly forecast for each location for the next 5 days?"

  This model provides hourly weather data using the latest observation snapshot
  for each location, ensuring users get the freshest forecast data.
*/

WITH latest_observation AS (
  SELECT MAX(observation_timestamp_utc) AS latest_obs_time
  FROM bronze_weather
)

SELECT
  location_id,
  forecast_timestamp_utc AS forecast_hour,
  observation_timestamp_utc AS observed_at,

  -- Temperature metrics
  temperature_celsius,
  feels_like_celsius,

  -- Wind metrics
  wind_speed_mps,
  ROUND(wind_speed_mps * 3.6, 1) AS wind_speed_kmh,  -- Convert to km/h
  wind_direction_degrees,
  CASE
    WHEN wind_direction_degrees BETWEEN 0 AND 22.5 THEN 'N'
    WHEN wind_direction_degrees BETWEEN 22.5 AND 67.5 THEN 'NE'
    WHEN wind_direction_degrees BETWEEN 67.5 AND 112.5 THEN 'E'
    WHEN wind_direction_degrees BETWEEN 112.5 AND 157.5 THEN 'SE'
    WHEN wind_direction_degrees BETWEEN 157.5 AND 202.5 THEN 'S'
    WHEN wind_direction_degrees BETWEEN 202.5 AND 247.5 THEN 'SW'
    WHEN wind_direction_degrees BETWEEN 247.5 AND 292.5 THEN 'W'
    WHEN wind_direction_degrees BETWEEN 292.5 AND 337.5 THEN 'NW'
    ELSE 'N'
  END AS wind_direction_cardinal,

  -- Precipitation
  precipitation_intensity_mmh,
  precipitation_probability_percent,
  precipitation_type_label,

  -- Atmospheric conditions
  humidity_percent,
  cloud_cover_percent,
  pressure_hpa,
  visibility_km,
  weather_code,

  -- Time context helpers
  DATE_TRUNC('day', forecast_timestamp_utc) AS forecast_date,
  EXTRACT(HOUR FROM forecast_timestamp_utc) AS forecast_hour_of_day,
  CASE
    WHEN forecast_timestamp_utc < CURRENT_TIMESTAMP THEN 'Historical'
    ELSE 'Forecast'
  END AS data_type,

  -- Metadata
  CURRENT_TIMESTAMP AS refreshed_at

FROM bronze_weather, latest_observation
WHERE has_invalid_temperature = false
  AND has_invalid_wind = false
  AND observation_timestamp_utc = latest_observation.latest_obs_time
ORDER BY location_id, forecast_timestamp_utc;
