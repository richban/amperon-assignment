"""Utility functions for handling paths and environment detection.

This module provides centralized path resolution for the project.
"""

import os
from pathlib import Path



def get_environment() -> str:
    """Get the current environment.

    Returns:
        str: "dev" or "prod"
    """
    return os.getenv("ENVIRONMENT", "dev").lower()


def is_dev() -> bool:
    """Check if running in development mode."""
    return get_environment() == "dev"


def is_prod() -> bool:
    """Check if running in production mode."""
    return get_environment() == "prod"


def get_project_root() -> Path:
    """Get the absolute path to the project root directory."""
    return Path(__file__).parent.parent.parent.parent.parent.parent.parent


def get_duckdb_path() -> str:
    """Get the DuckDB database location.

    Returns:
        str: Path to DuckDB database file (always as string for DLT)
    """
    if db_path := os.getenv("DUCKDB_DATABASE"):
        return str(db_path)

    data_dir = get_project_root() / "data"
    data_dir.mkdir(exist_ok=True)
    return str(data_dir / "weather.db")


def get_sqlmesh_project_path() -> Path:
    """Get the absolute path to the SQLMesh project directory."""
    if sqlmesh_path := os.getenv("SQLMESH_PROJECT_PATH"):
        return Path(sqlmesh_path)

    if is_prod():
        return Path("/app/sqlmesh")

    return get_project_root() / "workspace" / "sqlmesh"
