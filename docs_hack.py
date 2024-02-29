"""Hacky manual upload of docs data until rate limiting in CI is resolved."""

# imports
import ibis
import toml
import shutil

import logging as log

from dotenv import load_dotenv
from dag.ingest import ingest_docs
from dag.assets.extract.docs import extract_docs
from dag.assets.transform.docs import transform_docs
from dag.assets.load.docs import load_docs

# load .env
load_dotenv()

# configure log
log.basicConfig(
    level=log.INFO,
)

# load config
config = toml.load("config.toml")["app"]
log.info(f"Deploying to {config['database']}...")

# connect to the database
target = ibis.duckdb.connect(f"{config['database']}")

# ensure fresh data
# shutil.rmtree("data/system/duckdb", ignore_errors=True)

# ingest
# ingest_docs()

# ETL
docs = load_docs(transform_docs(extract_docs()))

# deploy
log.info(f"\tDeploying docs to {config['database']}...")
target.create_table("docs", docs.to_pyarrow(), overwrite=True)
