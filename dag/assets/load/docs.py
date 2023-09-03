# imports
import ibis
import dagster

from dag import functions as f


# assets
@dagster.asset
def load_docs(transform_docs):
    """
    Finalize the docs data.
    """
    return transform_docs
