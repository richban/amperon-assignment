"""Utility functions for handling paths and environment detection.

This module provides centralized path resolution and environment detection
for the entire project, supporting both local development and production
deployments.

Environment Modes:
    - DEV (development): Local filesystem (./data/weather.db, .dlt/pipeline_data)
    - PROD (production): S3 storage (s3://datalake/duckdb/weather.db, s3://dlt-staging)
"""

import os
from pathlib import Path
from typing import Union



def get_environment() -> str:
    """Get the current environment.

    Returns:
        str: "dev" or "prod"

    Environment Variables:
        ENVIRONMENT: Set to "prod" for production deployment (default: "dev")
    """
    return os.getenv("ENVIRONMENT", "dev").lower()


def is_dev() -> bool:
    """Check if running in development mode (local).

    Returns:
        bool: True if development mode, False if production
    """
    return get_environment() == "dev"


def is_prod() -> bool:
    """Check if running in production mode (Docker/cloud).

    Returns:
        bool: True if production mode, False if development
    """
    return get_environment() == "prod"


def get_project_root() -> Path:
    """Get the absolute path to the project root directory.

    Returns:
        Path: Absolute path to the project root directory

    Example:
        /Users/melchior/Developer/amperon
    """
    # Go up from utils.py -> weather_ingestion/ -> defs/ -> dg_amperon/ -> src/ -> dg-workspace/ -> workspace/ -> amperon/
    return Path(__file__).parent.parent.parent.parent.parent.parent.parent


def get_duckdb_path() -> Union[str, Path]:
    """Get the DuckDB database location.

    Priority:
        1. Environment variable: DUCKDB_DATABASE
        2. Production mode: s3://datalake/duckdb/weather.db
        3. Development mode: <project_root>/data/weather.db

    Returns:
        Union[str, Path]: S3 URI string in prod mode, Path object in dev mode

    Environment Variables:
        DUCKDB_DATABASE: Custom database location (optional)
        ENVIRONMENT: "dev" or "prod"

    Example:
        Dev: <project_root>/amperon/data/weather.db
        Prod: s3://datalake/duckdb/weather.db
    """
    # First try explicit environment variable
    if db_path := os.getenv("DUCKDB_DATABASE"):
        # Return as string if S3 URI, otherwise as Path
        return db_path if db_path.startswith("s3://") else Path(db_path)

    # Production mode: use MinIO/S3
    if is_prod():
        return "s3://datalake/duckdb/weather.db"

    # Development mode: use local filesystem
    data_dir = get_project_root() / "data"
    data_dir.mkdir(exist_ok=True)  # Ensure data directory exists
    return data_dir / "weather.db"


def get_dlt_destination() -> str:
    """Get the DLT destination filesystem configuration.

    Priority:
        1. Environment variable: DLT_DESTINATION_FILESYSTEM
        2. Production mode: s3://dlt-staging
        3. Development mode: filesystem (default DLT behavior)

    Returns:
        str: Destination type ("filesystem" or S3 URI)

    Environment Variables:
        DLT_DESTINATION_FILESYSTEM: Custom DLT destination (optional)
        ENVIRONMENT: "dev" or "prod"
    """
    # First try explicit environment variable
    if dest := os.getenv("DLT_DESTINATION_FILESYSTEM"):
        return dest

    # Production mode: use MinIO/S3
    if is_prod():
        return "s3://dlt-staging"

    # Development mode: use default filesystem
    return "filesystem"


def get_sqlmesh_project_path() -> Path:
    """Get the absolute path to the SQLMesh project directory.

    Priority:
        1. Environment variable: SQLMESH_PROJECT_PATH
        2. Default: <project_root>/workspace/sqlmesh (dev) or /app/sqlmesh (prod)

    Returns:
        Path: Absolute path to SQLMesh project directory

    Environment Variables:
        SQLMESH_PROJECT_PATH: Custom path to SQLMesh project (optional)

    Example:
        Dev: /Users/melchior/Developer/amperon/workspace/sqlmesh
        Prod: /app/sqlmesh
    """
    # First try environment variable
    if sqlmesh_path := os.getenv("SQLMESH_PROJECT_PATH"):
        return Path(sqlmesh_path)

    # Production mode: models are copied to /app/sqlmesh in Dockerfile
    if is_prod():
        return Path("/app/sqlmesh")

    # Development mode: workspace/sqlmesh in project root
    return get_project_root() / "workspace" / "sqlmesh"


def get_s3_config() -> dict:
    """Get S3/MinIO configuration for DuckDB and DLT.

    Returns S3 credentials only in production mode. In development mode,
    returns endpoint URL only if AWS_ENDPOINT_URL is set (for local MinIO).

    Returns:
        dict: S3 configuration parameters

    Environment Variables:
        AWS_ACCESS_KEY_ID: S3 access key (required in prod mode)
        AWS_SECRET_ACCESS_KEY: S3 secret key (required in prod mode)
        AWS_ENDPOINT_URL: S3 endpoint URL (required for MinIO in dev)
        AWS_REGION: AWS region (default: us-east-1)
    """
    config = {
        "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
        "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
        "region": os.getenv("AWS_REGION", "us-east-1"),
    }

    # Only set endpoint for dev mode (MinIO) or if explicitly configured
    if is_dev():
        endpoint_url = os.getenv("AWS_ENDPOINT_URL")
        if endpoint_url:
            config["endpoint_url"] = endpoint_url

    return config
