# imports
import ibis

from ibis_analytics.config import (
    PYPI_PACKAGES,
    GH_PRS_TABLE,
    GH_FORKS_TABLE,
    GH_STARS_TABLE,
    GH_ISSUES_TABLE,
    GH_COMMITS_TABLE,
    GH_WATCHERS_TABLE,
    DOCS_TABLE,
    ZULIP_MEMBERS_TABLE,
    ZULIP_MESSAGES_TABLE,
)
from ibis_analytics.catalog import Catalog


# connect to PyPI data in the ClickHouse Cloud playground
host = "clickpy-clickhouse.clickhouse.com"
port = 443
user = "play"
database = "pypi"

ch_con = ibis.clickhouse.connect(
    host=host,
    port=port,
    user=user,
    database=database,
)

# connect to catalog
catalog = Catalog()

# define source tables
pulls_t = catalog.table(GH_PRS_TABLE).cache().alias(GH_PRS_TABLE)
stars_t = catalog.table(GH_STARS_TABLE).cache().alias(GH_STARS_TABLE)
forks_t = catalog.table(GH_FORKS_TABLE).cache().alias(GH_FORKS_TABLE)
issues_t = catalog.table(GH_ISSUES_TABLE).cache().alias(GH_ISSUES_TABLE)
commits_t = catalog.table(GH_COMMITS_TABLE).cache().alias(GH_COMMITS_TABLE)
watchers_t = catalog.table(GH_WATCHERS_TABLE).cache().alias(GH_WATCHERS_TABLE)
downloads_t = ch_con.table(
    "pypi_downloads_per_day_by_version_by_system_by_country"
).filter(ibis._["project"].isin(PYPI_PACKAGES))
docs_t = catalog.table(DOCS_TABLE).cache().alias(DOCS_TABLE)
zulip_members_t = catalog.table(ZULIP_MEMBERS_TABLE).cache().alias(ZULIP_MEMBERS_TABLE)
zulip_messages_t = (
    catalog.table(ZULIP_MESSAGES_TABLE).cache().alias(ZULIP_MESSAGES_TABLE)
)
