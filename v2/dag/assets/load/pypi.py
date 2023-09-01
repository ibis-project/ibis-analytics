# imports
import ibis
import dagster

from dag import functions as f


# assets
@dagster.asset
def load_downloads(transform_downloads):
    """
    Finalize the PyPI downloads data.
    """
    return transform_downloads
