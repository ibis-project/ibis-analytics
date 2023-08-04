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
eda_config = toml.load("config.toml")["eda"]
exclude_tables = config["exclude_tables"] or []

# load env vars
load_dotenv()

# connect to motherduck
log.info(f"Export database: {config['database']}")
log.info(f"EDA database: {eda_config['database']}")
assert f"md:{config['database']}" != eda_config["database"]
con = ibis.connect(f"duckdb://md:{config['database']}")
eda_con = ibis.connect(f"duckdb://{eda_config['database']}")

# supported tables in ci
ci_tables = config["ci_tables"]

# source tables
source_tables = eda_con.list_tables()

# exclude tables
if not os.getenv("CI"):
    exclude_tables.extend([t for t in source_tables if t not in ci_tables])
if not eda_config["ci_enabled"]:
    exclude_tables.extend(["jobs", "workflows", "analysis"])

# overwrite tables
for t in source_tables:
    if t not in exclude_tables:
        log.info(f"Copying {t}...")
        try:
            con.create_table(t, eda_con.table(t).to_pyarrow(), overwrite=True)
        except Exception as e:
            log.warning(f"Failed to copy {t}...")
            log.warning(e)
