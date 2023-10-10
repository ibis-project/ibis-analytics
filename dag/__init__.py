# imports
import ibis

import logging as log

from dagster import Definitions

from dag.jobs import jobs
from dag.assets import assets
from dag.resources import resources
from dag import functions as f

# configure logging
log.basicConfig(
    level=log.INFO,
)

# config
backend = "duckdb"
ibis.set_backend(backend)

defs = Definitions(
    assets=assets,
    resources=resources,
    jobs=jobs,
)
