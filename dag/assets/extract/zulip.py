# imports
import ibis
import dagster

from dag import functions as f


# assets
@dagster.asset
def extract_zulip_members():
    """
    Extract the ingested Zulip members data.
    """
    members = f.clean_data(ibis.read_json("data/ingest/zulip/members.json"))
    return members


@dagster.asset
def extract_zulip_messages():
    """
    Extract the ingested Zulip messages data.
    """
    messages = f.clean_data(ibis.read_json("data/ingest/zulip/messages.json"))
    return messages
