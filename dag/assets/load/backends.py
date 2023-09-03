# imports
import ibis
import dagster

from dag import functions as f


# assets
@dagster.asset
def load_backends(transform_backends):
    """
    Finalize the backend data.
    """
    return transform_backends
