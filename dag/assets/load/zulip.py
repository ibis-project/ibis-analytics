# imports
import ibis
import dagster

from dag import functions as f


# assets
@dagster.asset
def load_zulip_members(transform_zulip_members):
    """
    Finalize the Zulip members data.
    """
    return transform_zulip_members


@dagster.asset
def load_zulip_messages(transform_zulip_messages):
    """
    Finalize the Zulip messages data.
    """
    return transform_zulip_messages
