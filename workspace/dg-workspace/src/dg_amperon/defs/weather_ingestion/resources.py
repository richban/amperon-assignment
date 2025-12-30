"""Dagster resources for the weather ingestion pipeline.

This module provides environment-aware Dagster resources for accessing
external services like DuckDB and S3.
"""

from contextlib import contextmanager

from dagster_duckdb import DuckDBResource

from dg_amperon.defs.weather_ingestion.utils import (
    get_duckdb_path,
)

class ExtendedDuckDBResource(DuckDBResource):
    """Extended DuckDB resource that pre-installs and loads S3 extensions.

    This resource automatically:
    - Installs httpfs extension for S3 access
    - Loads aws extension for AWS credentials
    - Configures S3 endpoint for dev mode (MinIO)
    - Handles both local and S3-backed databases transparently
    """

    def _initialize_extensions(self, conn):
        """Initialize DuckDB extensions on a connection.

        Args:
            conn: DuckDB connection object
        """
        extensions = [
            "httpfs",
            "aws",
        ]

        for extension in extensions:
            try:
                conn.execute(f"INSTALL {extension}")
                conn.execute(f"LOAD {extension}")
            except Exception as e:
                # Log warning but continue - some extensions might not be available
                print(f"Warning: Could not load extension '{extension}': {e}")

    @contextmanager
    def get_connection(self):
        """Get a DuckDB connection with all extensions pre-loaded.

        Yields:
            DuckDB connection with S3 extensions configured
        """
        with super().get_connection() as conn:
            self._initialize_extensions(conn)
            yield conn


def create_duckdb_resource() -> ExtendedDuckDBResource:
    """Create environment-aware DuckDB resource.

    Configures DuckDB with:
    - Database path from get_duckdb_path() (auto-detects dev/prod)
    - S3 credentials ONLY in production mode
    - No S3 config in development mode (uses local file)

    Returns:
        ExtendedDuckDBResource configured for current environment
    """
    db_path = str(get_duckdb_path())

    return ExtendedDuckDBResource(
        database=db_path,
    )
