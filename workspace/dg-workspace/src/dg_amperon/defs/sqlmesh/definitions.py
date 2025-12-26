from dagster import (
    Definitions,
)

from dg_amperon.defs.sqlmesh.assets import sqlmesh_project
from dg_amperon.defs.sqlmesh.resources import sqlmesh_resource

defs = Definitions(
    assets=[sqlmesh_project],
    resources={
        "sqlmesh": sqlmesh_resource,
    },
)
