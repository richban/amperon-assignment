import typing as t

from dagster import (
    AssetExecutionContext,
    MaterializeResult,
)
from dagster_sqlmesh import SQLMeshResource, sqlmesh_assets

from dg_amperon.defs.sqlmesh.resources import sqlmesh_config


@sqlmesh_assets(
    environment="dev",
    config=sqlmesh_config,
    enabled_subsetting=True,
)
def sqlmesh_project(
    context: AssetExecutionContext, sqlmesh: SQLMeshResource
) -> t.Iterator[MaterializeResult]:
    """SQLMesh transformation assets for weather data."""
    yield from sqlmesh.run(
        context,
        config=sqlmesh_config,
        environment="dev",
    )
