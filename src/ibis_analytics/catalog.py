# imports
import os
import ibis

from ibis_analytics.config import CLOUD_STORAGE, CLOUD_BUCKET, DATA_DIR


# functions
def delta_table_filename(table_name: str) -> str:
    return f"{table_name}.delta"


def delta_table_path(table_name: str) -> str:
    return os.path.join(DATA_DIR, delta_table_filename(table_name))


def read_table(table_name: str) -> ibis.Table:
    if CLOUD_STORAGE:
        import gcsfs
        import warnings

        warnings.filterwarnings("ignore")

        fs = gcsfs.GCSFileSystem(token="anon")
        ibis.get_backend().register_filesystem(fs)

        table_path = f"gs://{CLOUD_BUCKET}/{delta_table_path(table_name)}"
    else:
        table_path = delta_table_path(table_name)

    # TODO: remove parquet hack once https://github.com/delta-io/delta-rs/issues/2859 fixed
    return ibis.read_parquet(f"{table_path}/data.parquet")


def write_table(t: ibis.Table, table_name: str) -> None:
    if CLOUD_STORAGE:
        import gcsfs
        import warnings

        warnings.filterwarnings("ignore")

        fs = gcsfs.GCSFileSystem()
        ibis.get_backend().register_filesystem(fs)

        table_path = f"gs://{CLOUD_BUCKET}/{delta_table_path(table_name)}"
    else:
        table_path = delta_table_path(table_name)
        os.makedirs(table_path, exist_ok=True)

    # TODO: remove parquet hack once https://github.com/delta-io/delta-rs/issues/2859 fixed
    t.to_parquet(
        f"{table_path}/data.parquet",
        overwrite=True,
        # mode="overwrite",
        # partition_by=["extracted_at"],
    )


# classes
class Catalog:
    def list_tables(self):
        return [
            d
            for d in os.listdir(DATA_DIR)
            if not (d.startswith("_") or d.startswith("."))
        ]

    def table(self, table_name):
        return read_table(table_name)

    def write_table(self, t, table_name):
        write_table(t, table_name)
