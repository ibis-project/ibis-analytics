# imports
import os
import toml
import ibis
import fnmatch

import logging as log

from datetime import datetime, timedelta, date

import functions as f

# config.toml
config = toml.load("config.toml")["app"]

# configure logging
log.basicConfig(
    level=log.INFO,
)


def main():
    log.info(f"Deploying to {config['database']}...")
    upload()


def upload(stage: str = "staging") -> None:
    """
    Upload the data.
    """
    target = ibis.duckdb.connect(f"{config['database']}")
    path = "data/system/duckdb"

    for root, dirs, files in os.walk(path):
        for file in files:
            if fnmatch.fnmatch(file, "load_*.ddb"):
                full_path = os.path.join(root, file)
                con = ibis.duckdb.connect(full_path)
                tablename = file.replace(".ddb", "")
                table = con.table(tablename)
                tablename = tablename.replace("load_", "")

                log.info(f"\tDeploying {tablename} to {config['database']}...")
                target.create_table(tablename, table.to_pyarrow(), overwrite=True)


if __name__ == "__main__":
    main()
