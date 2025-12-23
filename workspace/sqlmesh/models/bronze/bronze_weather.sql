MODEL (
  name bronze_weather,
  kind FULL,
  dialect duckdb,
  description 'Staging model: Cleans and standardizes raw weather data from DLT',
  owner 'data_team',
  cron '@hourly'
);

/*
  Staging Model: Weather Raw Data

  This model reads from the latest DLT-generated weather schema and:
  1. Cleans column names (removes prefixes, snake_case normalization)
  2. Casts data types appropriately
  3. Adds data quality flags
  4. Prepares data for downstream mart models

  Source: DLT pipeline writes to weather.weather_timelines
*/

WITH latest_weather_data AS (
  -- Select from the most recent DLT schema
  -- Note: In production, you'd query information_schema to get the latest schema dynamically
  -- For now, we'll use a specific schema (will need to be updated after each DLT run)
  SELECT
    -- Timestamp
    start_time AS timestamp_utc,

    -- Location metadata
    _locations_id AS location_id,
    _locations_name AS location_name,
    _locations_lat AS latitude,
    _locations_lon AS longitude,

    -- Core weather measurements
    values__temperature AS temperature_celsius,
    values__temperature_apparent AS feels_like_celsius,
    values__humidity AS humidity_percent,
    values__wind_speed AS wind_speed_mps,
    values__wind_direction AS wind_direction_degrees,

    -- Precipitation
    COALESCE(
      values__precipitation_intensity__v_double,
      CAST(values__precipitation_intensity AS DOUBLE)
    ) AS precipitation_intensity_mmh,
    values__precipitation_probability AS precipitation_probability_percent,
    values__precipitation_type AS precipitation_type_code,

    -- Atmospheric conditions
    values__weather_code AS weather_code,
    values__cloud_cover AS cloud_cover_percent,
    values__pressure_surface_level AS pressure_hpa,
    values__visibility AS visibility_km,

    -- DLT metadata
    TO_TIMESTAMP(CAST(_dlt_load_id AS DOUBLE)) as _dlt_load_time,
    _dlt_load_id,
    _dlt_id

  FROM weather.weather_timelines
)

SELECT
  -- All columns from CTE
  timestamp_utc,
  location_id,
  location_name,
  latitude,
  longitude,
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
  _dlt_load_time,
  _dlt_load_id,
  _dlt_record_id,

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
ORDER BY location_id, timestamp_utc;
