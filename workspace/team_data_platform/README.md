# Weather Data Platform

Data engineering pipeline for ingesting weather data from Tomorrow.io API.

## Project Structure

```
workspace/team_data_platform/
├── .dlt/
│   ├── config.toml       # DLT configuration (bucket URL)
│   └── secrets.toml      # DLT secrets (MinIO credentials)
├── .env                  # Environment variables (API key)
├── etl/
│   ├── __init__.py
│   ├── __main__.py       # Pipeline entry point
│   ├── config.py         # Configuration (locations, fields)
│   └── pipeline.py       # DLT pipeline implementation
├── tests/
├── pyproject.toml        # Package definition and dependencies
└── README.md
```

## Setup

### 1. Start MinIO (Data Lake)

```bash
cd /Users/melchior/Developer/amperon
docker compose up -d minio minio-init
```

MinIO will be available at:
- API: http://localhost:9000
- Console: http://localhost:9001 (minioadmin / minioadmin)

### 2. Install Dependencies

```bash
cd workspace/team_data_platform
uv pip install -e .
```

### 3. Configure Environment

Make sure `.env` contains your Tomorrow.io API key:
```bash
TOMORROW_API_KEY=your_api_key_here
```

## Running the Pipeline

### From Host Machine

```bash
cd workspace/team_data_platform
python -m etl
```

This will:
1. Fetch weather data from Tomorrow.io API for 10 locations
2. Time range: -24 hours to +5 days
3. Hourly granularity (145 records per location)
4. Write Parquet files to MinIO at `s3://datalake/landing/weather/weather_timelines/`

### Pipeline Output

```
✓ Fetching weather for location 1 (25.86, -97.42)
✓ Fetched 145 records for location 1
✓ Fetching weather for location 2 (25.9, -97.52)
...
✓ Pipeline completed: Load package is LOADED
```

## Data Schema

Each Parquet file contains:

| Column | Type | Description |
|--------|------|-------------|
| `location_id` | int | 1-10 |
| `latitude` | float | Latitude coordinate |
| `longitude` | float | Longitude coordinate |
| `timestamp` | timestamp | ISO 8601 UTC |
| `scraped_at` | timestamp | When data was fetched |
| `temperature` | float | °C |
| `temperatureApparent` | float | Feels like °C |
| `humidity` | float | % |
| `windSpeed` | float | m/s |
| `windDirection` | float | degrees |
| `precipitationIntensity` | float | mm/h |
| `precipitationProbability` | float | % |
| `precipitationType` | int | 0-4 |
| `weatherCode` | int | Tomorrow.io code |
| `cloudCover` | float | % |
| `pressureSurfaceLevel` | float | hPa |
| `visibility` | float | km |

## Locations

10 locations in South Texas (Port of Brownsville area):

| ID | Latitude | Longitude |
|----|----------|-----------|
| 1  | 25.8600  | -97.4200  |
| 2  | 25.9000  | -97.5200  |
| 3  | 25.9000  | -97.4800  |
| 4  | 25.9000  | -97.4400  |
| 5  | 25.9000  | -97.4000  |
| 6  | 25.9200  | -97.3800  |
| 7  | 25.9400  | -97.5400  |
| 8  | 25.9400  | -97.5200  |
| 9  | 25.9400  | -97.4800  |
| 10 | 25.9400  | -97.4400  |

## Verify Data

Check MinIO console or use mc client:

```bash
docker run --rm --network amperon_default --entrypoint /bin/sh minio/mc:latest -c "
  mc alias set minio http://minio:9000 minioadmin minioadmin && 
  mc ls -r minio/datalake/landing/weather/weather_timelines/
"
```

## Next Steps

- [ ] Add SQLMesh for data transformation
- [ ] Add DuckDB for analytics
- [ ] Add Prefect for orchestration
- [ ] Create Jupyter visualizations
