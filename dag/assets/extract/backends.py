# imports
import ibis
import dagster

from dag import functions as f


# assets
@dagster.asset
def extract_backends():
    """
    Extract the backend data.
    """
    backends = ibis.util.backend_entry_points()
    backend_data = {
        "backends": [[b.name for b in backends]],
        "num_backends": len(backends),
        "ingested_at": f.now(),
    }
    return ibis.memtable(backend_data)
