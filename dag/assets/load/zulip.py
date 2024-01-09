# imports
import ibis
import dagster

from dag import functions as f


# assets
@dagster.asset
def load_zulip_users(transform_zulip_users):
    """
    Finalize the Zulip users data.
    """
    return transform_zulip_users
