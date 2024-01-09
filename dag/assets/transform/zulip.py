# imports
import ibis
import dagster

from dag import functions as f


# assets
@dagster.asset
def transform_zulip_users(extract_zulip_users):
    """
    Transform the Zulip users data.
    """
    users = extract_zulip_users.mutate(date_joined=ibis._.date_joined.cast("timestamp"))
    users = users.order_by(ibis._.date_joined.desc())
    users = users.relocate("full_name", "date_joined", "timezone")
    return users


@dagster.asset
def transform_zulip_messages(extract_zulip_messages):
    """
    Transform the Zulip messages data.
    """
    messages = extract_zulip_messages.mutate(
        timestamp=ibis._.timestamp.cast("timestamp"),
        last_edit_timestamp=ibis._.last_edit_timestamp.cast("timestamp"),
    )
    messages = messages.order_by(
        [ibis._.timestamp.desc(), ibis._.last_edit_timestamp.desc()]
    )
    messages = messages.relocate(
        "sender_full_name",
        "display_recipient",
        "subject",
        "timestamp",
        "last_edit_timestamp",
    )
    return messages
