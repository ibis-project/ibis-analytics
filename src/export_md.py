# imports
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
con = ibis.connect(f"duckdb://md:{config['database']}")
eda_con = ibis.connect(f"duckdb://{eda_config['database']}")

# exclude tables
if not eda_config["ci_enabled"]:
    exclude_tables.extend(["jobs", "workflows", "analysis"])

# overwrite tables
for t in con.list_tables():
    if t not in exclude_tables:
        log.info(f"Copying {t}...")
        try:
            con.create_table(t, eda_con.table(t), overwrite=True)
        except Exception as e:
            log.warning(f"Failed to copy {t}...")
            log.warning(e)
