# imports
import ibis

from dagster import Definitions

from dag.jobs import jobs
from dag.assets import assets
from dag.resources import resources
from dag import functions as f

# config
backend = "duckdb"
ibis.set_backend(backend)

defs = Definitions(
    assets=assets,
    resources=resources,
    jobs=jobs,
)
