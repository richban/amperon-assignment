import dagster as dg

import dg_amperon.defs

defs = dg.Definitions.merge(
    dg.components.load_defs(dg_amperon.defs),
)
