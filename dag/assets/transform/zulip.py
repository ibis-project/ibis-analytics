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
