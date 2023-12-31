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
    Backup the data.
    """

    # backup loaded data as Delta Lake tables
    # and a DuckDB Database
    source_path = "data/system/duckdb"
    target_path = "data/backup.ddb"

    os.makedirs(source_path, exist_ok=True)

    target = ibis.duckdb.connect(target_path)

    ingested_at = f.now()

    for root, dirs, files in os.walk(source_path):
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
                table.mutate(ingested_at=ingested_at).to_delta(
                    f"data/backup/{tablename}.delta", mode="append"
                )


if __name__ == "__main__":
    main()
