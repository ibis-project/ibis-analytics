# imports
import ibis
import dagster

from dag import functions as f


# assets
@dagster.asset
def transform_backends(extract_backends):
    """
    Transform the backend data.
    """
    return extract_backends
