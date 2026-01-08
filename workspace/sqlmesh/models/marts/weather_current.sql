MODEL (
  name weather_current,
  kind INCREMENTAL_BY_UNIQUE_KEY (
    unique_key location_id
  ),
  grain location_id,
  description 'Mart: Latest weather conditions per location (answers Q1)',
  audits [
    ASSERT_NOT_NULL(column_name := current_temperature_c),
    ASSERT_NOT_NULL(column_name := current_wind_speed_mps),
    ASSERT_NOT_NULL(column_name := observed_at_utc)
    -- assert_all_locations_present,
    -- assert_unique_location_snapshot
  ]
);

/*
  Mart Model: Current Weather per Location

  Answers Assignment Question 1:
  "What is the current temperature and wind speed for each location?"

  This model provides the NOWCAST (T+0 forecast) for each location.
  Nowcast = when forecast_timestamp = observation_timestamp, the API's best estimate
  of current conditions at the moment of observation.
*/

WITH latest_observation AS (
  SELECT MAX(observation_timestamp_utc) AS latest_obs_time
  FROM silver_weather
  WHERE observation_timestamp_utc BETWEEN @start_dt AND @end_dt
),

nowcast_per_location AS (
  SELECT
    bw.location_id,
    bw.forecast_timestamp_utc,
    bw.observation_timestamp_utc,
    bw.temperature_celsius,
    bw.wind_speed_mps,
    bw.humidity_percent,
    bw.weather_code,
    bw.precipitation_type_label,
    l.timezone
  FROM silver_weather bw
  JOIN weather_data.locations l ON bw.location_id = l.id
  CROSS JOIN latest_observation
  WHERE bw.has_invalid_temperature = false
    AND bw.has_invalid_wind = false
    AND bw.observation_timestamp_utc = latest_observation.latest_obs_time
    AND bw.forecast_timestamp_utc = bw.observation_timestamp_utc  -- NOWCAST: T+0 forecast
)

SELECT
  location_id,

  -- UTC timestamps (source of truth)
  forecast_timestamp_utc AS forecast_time_utc,
  observation_timestamp_utc AS observed_at_utc,

  -- Local timestamps (converted using location timezone)
  timezone(timezone, forecast_timestamp_utc) AS forecast_time_local,
  timezone(timezone, observation_timestamp_utc) AS observed_at_local,

  -- Weather metrics (nowcast values)
  temperature_celsius AS current_temperature_c,
  wind_speed_mps AS current_wind_speed_mps,
  ROUND(wind_speed_mps * 3.6, 1) AS current_wind_speed_kmh,  -- Convert m/s to km/h
  humidity_percent AS current_humidity_pct,
  weather_code,
  precipitation_type_label,

  -- Metadata
  CURRENT_TIMESTAMP AS refreshed_at
FROM nowcast_per_location
ORDER BY location_id;
