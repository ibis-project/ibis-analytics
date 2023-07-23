# imoprts
import os
import ibis
import toml
import json
import requests

import logging as log
import ibis.expr.datatypes as dt

from ibis import _
from dotenv import load_dotenv
from datetime import datetime, timedelta, date

# constants
# set DEFAULT_BACKFILL to the number of days
# since July 19th, 2015 until today
DEFAULT_BACKFILL = (datetime.now() - datetime(2015, 7, 19)).days
BQ_URL = "bigquery://ibis-gbq/workflows"

# configure logger
log.basicConfig(level=log.INFO)

# load environment variables
load_dotenv()
project_id = os.getenv("BQ_PROJECT_ID")
log.info(f"Project ID: {project_id}")

# load config
config = toml.load("config.toml")["ci"]

# configure lookback window
backfill = config["backfill"] if "backfill" in config else DEFAULT_BACKFILL
log.info(f"Backfill: {backfill}")


def main():
    os.makedirs("data/ci/ibis", exist_ok=True)
    con = ibis.connect("duckdb://data/ci/ibis/raw.ddb")
    bq_con = ibis.connect(BQ_URL)

    for table in bq_con.list_tables():

        log.info(f"Writing table: {table}")
        con.create_table(table, bq_con.table(table).to_pyarrow(), overwrite=True)


# if __name__ == "__main__":
if __name__ == "__main__":
    # run the main function
    main()
