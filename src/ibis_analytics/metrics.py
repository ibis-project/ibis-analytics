# imports
import ibis
import ibis.selectors as s

from ibis_analytics.config import (
    PYPI_PACKAGE,
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

# get source tables
# TODO: use table names from config
pulls_t = catalog.table(GH_PRS_TABLE).cache()
stars_t = catalog.table(GH_STARS_TABLE).cache()
forks_t = catalog.table(GH_FORKS_TABLE).cache()
issues_t = catalog.table(GH_ISSUES_TABLE).cache()
commits_t = catalog.table(GH_COMMITS_TABLE).cache()
watchers_t = catalog.table(GH_WATCHERS_TABLE).cache()
downloads_t = ch_con.table(
    "pypi_downloads_per_day_by_version_by_installer_by_type_by_country"
).filter(ibis._["project"] == PYPI_PACKAGE)
docs_t = catalog.table(DOCS_TABLE).cache()
zulip_members_t = catalog.table(ZULIP_MEMBERS_TABLE).cache()
zulip_messages_t = catalog.table(ZULIP_MESSAGES_TABLE).cache()
