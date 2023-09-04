# imports
import os
import ibis
import fnmatch

import logging as log

from datetime import datetime, timedelta, date

import functions as f

log.basicConfig(
    level=log.INFO,
)


def main():
    log.info("Deploying to staging...")
    upload()
    log.info("Deploying to prod...")
    upload(stage="prod")


def upload(stage: str = "staging") -> None:
    """
    Upload the data.
    """
    target = ibis.duckdb.connect(f"md:{stage}_metrics")
    path = "data/system/duckdb"

    for root, dirs, files in os.walk(path):
        for file in files:
            if fnmatch.fnmatch(file, "load_*.ddb"):
                full_path = os.path.join(root, file)
                con = ibis.duckdb.connect(full_path)
                tablename = file.replace(".ddb", "")
                table = con.table(tablename)
                tablename = tablename.replace("load_", "")

                log.info(f"\tDeploying {tablename} to md:{stage}_metrics...")
                target.create_table(tablename, table.to_pyarrow(), overwrite=True)


if __name__ == "__main__":
    main()
