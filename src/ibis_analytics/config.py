GH_REPOS = [
    "ibis-project/ibis",
    "ibis-project/ibis-substrait",
    "ibis-project/ibis-ml",
    "ibis-project/ibis-analytics",
]
PYPI_PACKAGES = ["ibis-framework", "ibis-substrait", "ibis-ml", "ibis-analytics"]
ZULIP_URL = "https://ibis-project.zulipchat.com"
DOCS_URL = "https://ibis.goatcounter.com"

CLOUD_STORAGE = True
CLOUD_BUCKET = "ibis-analytics"

DATA_DIR = "datalake"
RAW_DATA_DIR = "_raw"
RAW_DATA_GH_DIR = "github"
RAW_DATA_DOCS_DIR = "docs"
RAW_DATA_ZULIP_DIR = "zulip"

GH_PRS_TABLE = "gh_prs"
GH_FORKS_TABLE = "gh_forks"
GH_STARS_TABLE = "gh_stars"
GH_ISSUES_TABLE = "gh_issues"
GH_COMMITS_TABLE = "gh_commits"
GH_WATCHERS_TABLE = "gh_watchers"
DOCS_TABLE = "docs"
ZULIP_MEMBERS_TABLE = "zulip_members"
ZULIP_MESSAGES_TABLE = "zulip_messages"
