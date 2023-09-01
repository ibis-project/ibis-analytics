# imports
import ibis
import dagster

from dag import functions as f


# assets
@dagster.asset
def transform_downloads(extract_downloads):
    """
    Transform the PyPI downloads data.
    """
    downloads = extract_downloads.drop("project").unpack("file").unpack("details")
    downloads = f.agg_downloads(downloads)
    return downloads
