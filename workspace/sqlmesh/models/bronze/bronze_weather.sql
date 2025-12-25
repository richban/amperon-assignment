MODEL (
  name bronze_weather,
  kind FULL,
  dialect duckdb,
  description 'Staging model: Cleans and standardizes raw weather data from DLT',
  owner 'data_team',
  cron '@hourly'
);

/*
  Staging Model: Weather Raw Data (Versioned Snapshots, Normalized Schema)

  This model reads from DLT-generated tables:
  - weather_data.weather_observations (fact table with weather measurements)
  - weather_data.locations (dimension table with location details)

  Features:
  1. Joins normalized tables (fact + dimension)
  2. Cleans column names (removes prefixes, snake_case normalization)
  3. Casts data types appropriately
  4. Preserves run_timestamp for bitemporal analysis
  5. Adds data quality flags
  6. Prepares data for downstream mart models

  Schema:
  - Normalized: weather_observations contains only _locations_id (FK)
  - Location details (name, lat, lon) joined from locations dimension table
  
  Bitemporal dimensions:
  - start_time (forecast_timestamp): WHEN the weather event occurs
  - run_timestamp (observation_timestamp): WHEN we made the forecast
*/

WITH latest_weather_data AS (
  SELECT
    -- Bitemporal timestamps
    w.start_time AS forecast_timestamp_utc,
    w.run_timestamp AS observation_timestamp_utc,

    -- Location metadata (from dimension table)
    w._locations_id AS location_id,
    l.name AS location_name,
    l.lat AS latitude,
    l.lon AS longitude,

    -- Core weather measurements
    w.values__temperature AS temperature_celsius,
    w.values__temperature_apparent AS feels_like_celsius,
    w.values__humidity AS humidity_percent,
    w.values__wind_speed AS wind_speed_mps,
    w.values__wind_direction AS wind_direction_degrees,

    -- Precipitation
    COALESCE(
      w.values__precipitation_intensity__v_double,
      CAST(w.values__precipitation_intensity AS DOUBLE)
    ) AS precipitation_intensity_mmh,
    w.values__precipitation_probability AS precipitation_probability_percent,
    w.values__precipitation_type AS precipitation_type_code,

    -- Atmospheric conditions
    w.values__weather_code AS weather_code,
    w.values__cloud_cover AS cloud_cover_percent,
    w.values__pressure_surface_level AS pressure_hpa,
    w.values__visibility AS visibility_km,

    -- DLT metadata
    TO_TIMESTAMP(CAST(w._dlt_load_id AS DOUBLE)) as _dlt_load_time,
    w._dlt_load_id,
    w._dlt_id

  FROM weather_data.weather_observations w
  INNER JOIN weather_data.locations l ON w._locations_id = l.id
)

SELECT
  -- Bitemporal timestamps
  forecast_timestamp_utc,
  observation_timestamp_utc,
  
  -- Location metadata
  location_id,
  location_name,
  latitude,
  longitude,
  
  -- Weather measurements
  temperature_celsius,
  feels_like_celsius,
  humidity_percent,
  wind_speed_mps,
  wind_direction_degrees,
  precipitation_intensity_mmh,
  precipitation_probability_percent,
  precipitation_type_code,
  weather_code,
  cloud_cover_percent,
  pressure_hpa,
  visibility_km,
  
  -- DLT metadata
  _dlt_load_time,
  _dlt_load_id,
  _dlt_id,

  -- Add data quality flags
  CASE
    WHEN temperature_celsius IS NULL THEN true
    WHEN temperature_celsius < -50 OR temperature_celsius > 60 THEN true
    ELSE false
  END AS has_invalid_temperature,

  CASE
    WHEN wind_speed_mps IS NULL THEN true
    WHEN wind_speed_mps < 0 THEN true
    ELSE false
  END AS has_invalid_wind,

  -- Add derived fields
  CASE precipitation_type_code
    WHEN 0 THEN 'None'
    WHEN 1 THEN 'Rain'
    WHEN 2 THEN 'Snow'
    WHEN 3 THEN 'Freezing Rain'
    WHEN 4 THEN 'Ice Pellets'
    ELSE 'Unknown'
  END AS precipitation_type_label,

  CURRENT_TIMESTAMP AS transformed_at

FROM latest_weather_data
ORDER BY location_id, forecast_timestamp_utc, observation_timestamp_utc;
