# imports
import ibis
import dagster

from dag import functions as f


# assets
@dagster.asset
def transform_docs(extract_docs):
    """
    Transform the docs data.
    """
    docs = extract_docs.rename({"path": "2_path", "timestamp": "date"})
    return docs
