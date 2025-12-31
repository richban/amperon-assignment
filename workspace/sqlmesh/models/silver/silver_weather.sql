MODEL (
  name silver_weather,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column observation_timestamp_utc
  ),
  grain (location_id, forecast_timestamp_utc, observation_timestamp_utc),
  audits [
    -- Bitemporal keys must exist
    ASSERT_NOT_NULL(column_name := location_id),
    ASSERT_NOT_NULL(column_name := forecast_timestamp_utc),
    ASSERT_NOT_NULL(column_name := observation_timestamp_utc),

    -- Core metrics must exist
    ASSERT_NOT_NULL(column_name := temperature_celsius),
    ASSERT_NOT_NULL(column_name := wind_speed_mps),
  ],
  description 'Silver model: Cleans and standardizes bronze (raw) ingested weather data from DLT',
);

/*
  Staging Model: Weather Raw Data (Versioned Snapshots, Normalized Schema)

  This model reads from DLT-generated tables:
  - weather_data.weather_observations (fact table with weather measurements)

  Features:
  2. Cleans column names (removes prefixes, snake_case normalization)
  3. Casts data types appropriately
  4. Preserves observation_timestamp for bitemporal analysis
  5. Adds data quality flags
  6. Prepares data for downstream mart models

  Schema:
  - Normalized: weather_observations contains only _locations_id (FK)

  Bitemporal dimensions:
  - start_time (forecast_timestamp): WHEN the weather event occurs
  - observation_timestamp: WHEN we made the forecast
*/

WITH latest_weather_data AS (
  SELECT
    -- Bitemporal timestamps
    start_time AS forecast_timestamp_utc,
    observation_timestamp AS observation_timestamp_utc,

    _locations_id AS location_id,

    -- Core weather measurements
    values__temperature AS temperature_celsius,
    values__temperature_apparent AS feels_like_celsius,
    values__humidity AS humidity_percent,
    values__wind_speed AS wind_speed_mps,
    values__wind_direction AS wind_direction_degrees,

    -- Precipitation
    CAST(values__precipitation_intensity AS DOUBLE) AS precipitation_intensity_mmh,
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

  FROM weather_data.weather_observations
  WHERE observation_timestamp BETWEEN @start_dt AND @end_dt
)

SELECT
  -- Bitemporal timestamps
  forecast_timestamp_utc,
  observation_timestamp_utc,

  location_id,

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
