from pathlib import Path

from dagster_sqlmesh import SQLMeshContextConfig, SQLMeshResource


# Get the workspace root directory
WORKSPACE_PATH = Path(__file__).parent.parent.parent.parent.parent.parent

# Define the dbt project path relative to repository root
SQLMESH_PROJECT_PATH = WORKSPACE_PATH / "sqlmesh"


sqlmesh_config = SQLMeshContextConfig(
    path=str(SQLMESH_PROJECT_PATH),
    gateway="duckdb",
    translator_class_name="dg_amperon.defs.sqlmesh.assets.CustomSQLMeshTranslator",
)
sqlmesh_resource = SQLMeshResource(config=sqlmesh_config)
