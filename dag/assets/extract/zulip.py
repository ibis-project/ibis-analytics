# imports
import ibis
import dagster

from dag import functions as f


# assets
@dagster.asset
def extract_zulip_users():
    """
    Extract the ingested Zulip users data.
    """
    users = f.clean_data(ibis.read_json("data/ingest/zulip/users.json"))
    return users
