"""Utility functions for handling paths in the project.

This module provides centralized path resolution for the entire project,
supporting both local development and containerized deployments via
environment variables.
"""

import os
from pathlib import Path


def get_project_root() -> Path:
    """Get the absolute path to the project root directory.

    Returns:
        Path: Absolute path to the project root directory

    Example:
        /Users/melchior/Developer/amperon
    """
    # Go up from utils.py -> weather_ingestion/ -> defs/ -> dg_amperon/ -> src/ -> dg-workspace/ -> workspace/ -> amperon/
    return Path(__file__).parent.parent.parent.parent.parent.parent.parent


def get_duckdb_path() -> Path:
    """Get the absolute path to the DuckDB database file.

    Priority:
        1. Environment variable: DUCKDB_DATABASE
        2. Default: <project_root>/data/weather.db

    Returns:
        Path: Absolute path to the DuckDB database file

    Environment Variables:
        DUCKDB_DATABASE: Custom path to database file (optional)

    Example:
        Local: /Users/melchior/Developer/amperon/data/weather.db
        Container: /data/weather.db (via DUCKDB_DATABASE env var)
    """
    # First try environment variable
    if db_path := os.getenv("DUCKDB_DATABASE"):
        return Path(db_path)

    # Default to data/weather.db in project root
    data_dir = get_project_root() / "data"
    data_dir.mkdir(exist_ok=True)  # Ensure data directory exists
    return data_dir / "weather.db"


def get_sqlmesh_project_path() -> Path:
    """Get the absolute path to the SQLMesh project directory.

    Priority:
        1. Environment variable: SQLMESH_PROJECT_PATH
        2. Default: <project_root>/sqlmesh

    Returns:
        Path: Absolute path to SQLMesh project directory

    Environment Variables:
        SQLMESH_PROJECT_PATH: Custom path to SQLMesh project (optional)

    Example:
        Local: /Users/melchior/Developer/amperon/workspace/sqlmesh
        Container: /app/sqlmesh (via SQLMESH_PROJECT_PATH env var)
    """
    # First try environment variable
    if sqlmesh_path := os.getenv("SQLMESH_PROJECT_PATH"):
        return Path(sqlmesh_path)

    # Default to workspace/sqlmesh in project root
    # Project root is amperon/, we want amperon/workspace/sqlmesh
    return get_project_root() / "workspace" / "sqlmesh"
