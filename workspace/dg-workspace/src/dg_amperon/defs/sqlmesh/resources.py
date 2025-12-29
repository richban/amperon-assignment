from pathlib import Path
from dagster import AssetKey
from dagster_sqlmesh import (
    SQLMeshContextConfig,
    SQLMeshResource,
    SQLMeshDagsterTranslator,
)
from sqlglot import exp
from sqlmesh.core.context import Context

# Get the workspace root directory
WORKSPACE_PATH = Path(__file__).parent.parent.parent.parent.parent.parent

# Define the dbt project path relative to repository root
SQLMESH_PROJECT_PATH = WORKSPACE_PATH / "sqlmesh"


class CustomSQLMeshTranslator(SQLMeshDagsterTranslator):
    """Custom translator to align SQLMesh asset keys with DLT asset keys."""

    def get_asset_key(self, context: Context, fqn: str) -> AssetKey:
        """Override asset key generation to match DLT asset structure."""
        table = exp.to_table(fqn)
        # Skip the catalog/gateway (e.g., duckdb) and use only db.name (schema.table)
        return AssetKey([table.db, table.name])

    def get_group_name(self, context: Context, model) -> str:
        """Group SQLMesh assets together."""
        return "sqlmesh_transformations"


class CustomSQLMeshContextConfig(SQLMeshContextConfig):
    """Custom configuration to inject the custom translator."""

    def get_translator(self) -> SQLMeshDagsterTranslator:
        return CustomSQLMeshTranslator()


sqlmesh_config = CustomSQLMeshContextConfig(
    path=str(SQLMESH_PROJECT_PATH),
    gateway="duckdb",
)

sqlmesh_resource = SQLMeshResource()
