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
    backup()
    # backup(storage="azure")


def backup(storage: str = "local") -> None:
    """
    Upload the data.
    """
    path = "data/system/duckdb"
    target = ibis.duckdb.connect(f"backup.ddb")

    os.makedirs(path, exist_ok=True)

    ingested_at = f.now()

    for root, dirs, files in os.walk(path):
        for file in files:
            if fnmatch.fnmatch(file, "load_*.ddb"):
                full_path = os.path.join(root, file)
                con = ibis.duckdb.connect(full_path)
                tablename = file.replace(".ddb", "")
                table = con.table(tablename)
                tablename = tablename.replace("load_", "")

                log.info(f"Backing up {tablename} to backup.ddb...")
                target.create_table(tablename, table.to_pyarrow(), overwrite=True)

                log.info(f"Backing up {tablename} to data/backup/{tablename}.delta...")
                table.to_delta(f"data/backup/{tablename}.delta", mode="append")


if __name__ == "__main__":
    main()
