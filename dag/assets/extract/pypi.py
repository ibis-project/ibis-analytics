# imports
import ibis
import dagster

from dag import functions as f


# assets
@dagster.asset
def extract_downloads():
    """
    Extract the ingested PyPI downloads data.
    """
    downloads = f.clean_data(
        ibis.read_parquet("data/ingest/pypi/ibis-framework/*.parquet")
    )
    return downloads
