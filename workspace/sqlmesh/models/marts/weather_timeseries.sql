MODEL (
  name weather_timeseries,
  kind FULL,
  grain (location_id, forecast_timestamp_utc),
  description 'Mart: Hourly weather timeseries per location (answers Q2) - sliding 144h window',
  audits [
    ASSERT_NOT_NULL(column_name := location_id),
    ASSERT_NOT_NULL(column_name := forecast_timestamp_utc),
    ASSERT_NOT_NULL(column_name := temperature_celsius),
    ASSERT_NOT_NULL(column_name := wind_speed_mps),
    assert_all_locations_present,
    assert_timeseries_window_complete
  ]
);

/*
  Mart Model: Weather Timeseries per Location

  Answers Assignment Question 2:
  "What is the hourly forecast for each location for the next 5 days?"

  This model maintains a sliding 144-hour window:
  - 24 hours of historical data (backcasted observations)
  - 120 hours of future forecasts (5 days)
  
  Uses FULL refresh strategy:
  - Rebuilds entire table every hour with the latest observation
  - Guarantees clean 144-row window per location (no stale data)
  - Simple and performant for small datasets (~1,440 rows total)
*/

WITH latest_observation AS (
  SELECT MAX(observation_timestamp_utc) AS latest_obs_time
  FROM silver_weather
)

SELECT
  bw.location_id,
  bw.forecast_timestamp_utc,  -- Part of unique key (location_id, forecast_timestamp_utc)

  -- UTC timestamps (source of truth)
  bw.observation_timestamp_utc,

  -- Local timestamps (converted using location timezone)
  timezone(l.timezone, bw.forecast_timestamp_utc) AS forecast_hour_local,
  timezone(l.timezone, bw.observation_timestamp_utc) AS observed_at_local,

  -- Temperature metrics
  bw.temperature_celsius,
  bw.feels_like_celsius,

  -- Wind metrics
  bw.wind_speed_mps,
  ROUND(bw.wind_speed_mps * 3.6, 1) AS wind_speed_kmh,  -- Convert to km/h
  bw.wind_direction_degrees,
  CASE
    WHEN bw.wind_direction_degrees BETWEEN 0 AND 22.5 THEN 'N'
    WHEN bw.wind_direction_degrees BETWEEN 22.5 AND 67.5 THEN 'NE'
    WHEN bw.wind_direction_degrees BETWEEN 67.5 AND 112.5 THEN 'E'
    WHEN bw.wind_direction_degrees BETWEEN 112.5 AND 157.5 THEN 'SE'
    WHEN bw.wind_direction_degrees BETWEEN 157.5 AND 202.5 THEN 'S'
    WHEN bw.wind_direction_degrees BETWEEN 202.5 AND 247.5 THEN 'SW'
    WHEN bw.wind_direction_degrees BETWEEN 247.5 AND 292.5 THEN 'W'
    WHEN bw.wind_direction_degrees BETWEEN 292.5 AND 337.5 THEN 'NW'
    ELSE 'N'
  END AS wind_direction_cardinal,

  -- Precipitation
  bw.precipitation_intensity_mmh,
  bw.precipitation_probability_percent,
  bw.precipitation_type_label,

  -- Atmospheric conditions
  bw.humidity_percent,
  bw.cloud_cover_percent,
  bw.pressure_hpa,
  bw.visibility_km,
  bw.weather_code,

  -- Time context helpers (using LOCAL time for business logic)
  DATE_TRUNC('day', timezone(l.timezone, bw.forecast_timestamp_utc)) AS forecast_date_local,
  EXTRACT(HOUR FROM timezone(l.timezone, bw.forecast_timestamp_utc)) AS forecast_hour_of_day_local,
  CASE
    WHEN bw.forecast_timestamp_utc < CURRENT_TIMESTAMP THEN 'Historical'
    ELSE 'Forecast'
  END AS data_type,

  -- Metadata
  CURRENT_TIMESTAMP AS refreshed_at

FROM silver_weather bw
JOIN weather_data.locations l ON bw.location_id = l.id
CROSS JOIN latest_observation
WHERE bw.has_invalid_temperature = false
  AND bw.has_invalid_wind = false
  AND bw.observation_timestamp_utc = latest_observation.latest_obs_time
ORDER BY bw.location_id, bw.forecast_timestamp_utc;
