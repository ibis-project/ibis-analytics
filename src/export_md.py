# imports
import os
import toml
import ibis

import logging as log

from dotenv import load_dotenv

# configure logger
log.basicConfig(level=log.INFO)

# config.toml
config = toml.load("config.toml")["motherduck"]
transform_config = toml.load("config.toml")["transform"]
exclude_tables = config["exclude_tables"] or []

# load env vars
load_dotenv()

# connect to motherduck
log.info(f"Export database: {config['database']}")
log.info(f"Transform database: {transform_config['database']}")
assert f"md:{config['database']}" != transform_config["database"]
con = ibis.connect(f"duckdb://md:{config['database']}")
transform_con = ibis.connect(f"duckdb://{transform_config['database']}")

# source tables
source_tables = transform_con.list_tables()

# overwrite tables
for t in source_tables:
    if t not in exclude_tables:
        log.info(f"Copying {t}...")
        try:
            con.create_table(t, transform_con.table(t).to_pyarrow(), overwrite=True)
        except Exception as e:
            log.warning(f"Failed to copy {t}...")
            log.warning(e)
