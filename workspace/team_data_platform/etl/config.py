"""Configuration for weather data pipeline"""

import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# API Configuration
TOMORROW_API_KEY = os.getenv("TOMORROW_API_KEY")
TOMORROW_API_URL = "https://api.tomorrow.io/v4/timelines"

# 10 Required Locations (from ASSIGNMENT.md)
LOCATIONS = [
    {"id": 1, "lat": 25.8600, "lon": -97.4200},
    {"id": 2, "lat": 25.9000, "lon": -97.5200},
    {"id": 3, "lat": 25.9000, "lon": -97.4800},
    {"id": 4, "lat": 25.9000, "lon": -97.4400},
    {"id": 5, "lat": 25.9000, "lon": -97.4000},
    {"id": 6, "lat": 25.9200, "lon": -97.3800},
    {"id": 7, "lat": 25.9400, "lon": -97.5400},
    {"id": 8, "lat": 25.9400, "lon": -97.5200},
    {"id": 9, "lat": 25.9400, "lon": -97.4800},
    {"id": 10, "lat": 25.9400, "lon": -97.4400},
]

# Weather fields to fetch (Core layer - Free tier)
WEATHER_FIELDS = [
    "temperature",
    "temperatureApparent",
    "humidity",
    "windSpeed",
    "windDirection",
    "precipitationIntensity",
    "precipitationProbability",
    "precipitationType",
    "weatherCode",
    "cloudCover",
    "pressureSurfaceLevel",
    "visibility",
]
