# imoprts
import os
import ibis
import toml

import logging as log

from ibis import _
from dotenv import load_dotenv
from datetime import datetime, timedelta

# constants
# set DEFAULT_BACKFILL to the number of days
# since July 19th, 2015 until today
DEFAULT_BACKFILL = (datetime.now() - datetime(2015, 7, 19)).days
BIGQUERY_DATASET = "bigquery-public-data.pypi.file_downloads"

# configure logger
log.basicConfig(level=log.INFO)

# load environment variables
load_dotenv()
project_id = os.getenv("BQ_PROJECT_ID")
log.info(f"Project ID: {project_id}")

# load config
config = toml.load("config.toml")["pypi"]
log.info(f"Packages: {config['packages']}")

# configure lookback window
backfill = config["backfill"] if "backfill" in config else DEFAULT_BACKFILL
log.info(f"Backfill: {backfill}")


def main():
    for package in config["packages"]:
        log.info(f"Package: {package}")
        # create output directory
        output_dir = os.path.join("data", "pypi", package)
        os.makedirs(output_dir, exist_ok=True)

        # construct query
        query = f"""
        SELECT *
        FROM `{BIGQUERY_DATASET}`
        WHERE file.project = '{package}'
        AND DATE(timestamp)
        BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL {backfill} DAY)
        AND CURRENT_DATE()
        """.strip()

        # define main function
        # extract config variables
        con = ibis.connect(f"bigquery://{project_id}")

        log.info(f"Executing query: {query}")
        t = con.sql(query)

        filename = f"file_downloads.parquet"
        output_path = os.path.join(output_dir, filename)
        log.info(f"Writing to: {output_path}")

        t.to_parquet(output_path)


# if __name__ == "__main__":
if __name__ == "__main__":
    # run the main function
    main()
