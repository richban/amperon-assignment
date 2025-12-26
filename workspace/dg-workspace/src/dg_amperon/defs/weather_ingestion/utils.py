"""Utility functions for handling paths in the project."""

import os

from pathlib import Path


def get_project_root() -> Path:
    """Get the absolute path to the project root directory.

    Returns:
        Path: Absolute path to the project root directory
    """
    # Go up from utils.py -> etl/ -> defs/ -> dg_amperon/ -> src/ -> dg-workspace/ -> workspace/ -> amperon/
    return Path(__file__).parent.parent.parent.parent.parent.parent.parent


def get_duckdb_path() -> Path:
    """Get the absolute path to the DuckDB database file.

    Returns:
        Path: Absolute path to the DuckDB database file
    """
    # First try environment variable
    if db_path := os.getenv("DUCKDB_DATABASE"):
        return Path(db_path)

    # Default to data/weather.duckdb in project root
    data_dir = get_project_root() / "data"
    data_dir.mkdir(exist_ok=True)  # Ensure data directory exists
    return data_dir / "weather.db"
