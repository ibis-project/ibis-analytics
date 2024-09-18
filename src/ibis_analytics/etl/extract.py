# imports
import os
import ibis

from datetime import datetime
from ibis_analytics.config import (
    DATA_DIR,
    RAW_DATA_DIR,
    RAW_DATA_GH_DIR,
    RAW_DATA_DOCS_DIR,
    RAW_DATA_ZULIP_DIR,
)

# set extracted_at timestamp
extracted_at = datetime.utcnow().isoformat()


# functions
def add_extracted_at(t):
    """Add extracted_at column to table."""

    # add extracted_at column and relocate it to the first position
    t = t.mutate(extracted_at=ibis.literal(extracted_at)).relocate("extracted_at")

    return t


def constraints(t):
    """Check common constraints for extracted tables."""

    assert t.count().to_pyarrow().as_py() > 0, "table is empty!"

    return t


# extract data assets
def gh_commits():
    """Extract GitHub commits data."""

    # read in raw data
    data_glob = os.path.join(
        DATA_DIR, RAW_DATA_DIR, RAW_DATA_GH_DIR, "repo_name=*", "commits.*.json"
    )
    gh_commits = ibis.read_json(data_glob)

    # add extracted_at column
    gh_commits = gh_commits.pipe(add_extracted_at).pipe(constraints)

    return gh_commits


def gh_issues():
    """Extract GitHub issues data."""

    # read in raw data
    data_glob = os.path.join(
        DATA_DIR, RAW_DATA_DIR, RAW_DATA_GH_DIR, "repo_name=*", "issues.*.json"
    )
    gh_issues = ibis.read_json(data_glob)

    # add extracted_at column
    gh_issues = gh_issues.pipe(add_extracted_at).pipe(constraints)

    return gh_issues


def gh_prs():
    """Extract GitHub pull request (PR) data."""

    # read in raw data
    data_glob = os.path.join(
        DATA_DIR, RAW_DATA_DIR, RAW_DATA_GH_DIR, "repo_name=*", "pullRequests.*.json"
    )
    gh_prs = ibis.read_json(data_glob)

    # add extracted_at column
    gh_prs = gh_prs.pipe(add_extracted_at).pipe(constraints)

    return gh_prs


def gh_forks():
    """Extract GitHub forks data."""

    # read in raw data
    data_glob = os.path.join(
        DATA_DIR, RAW_DATA_DIR, RAW_DATA_GH_DIR, "repo_name=*", "forks.*.json"
    )
    gh_forks = ibis.read_json(data_glob)

    # add extracted_at column
    gh_forks = gh_forks.pipe(add_extracted_at).pipe(constraints)

    return gh_forks


def gh_stars():
    """Extract GitHub stargazers data."""

    # read in raw data
    data_glob = os.path.join(
        DATA_DIR, RAW_DATA_DIR, RAW_DATA_GH_DIR, "repo_name=*", "stargazers.*.json"
    )
    gh_stars = ibis.read_json(data_glob)

    # add extracted_at column
    gh_stars = gh_stars.pipe(add_extracted_at).pipe(constraints)

    return gh_stars


def gh_watchers():
    """Extract GitHub watchers data."""

    # read in raw data
    data_glob = os.path.join(
        DATA_DIR, RAW_DATA_DIR, RAW_DATA_GH_DIR, "repo_name=*", "watchers.*.json"
    )
    gh_watchers = ibis.read_json(data_glob)

    # add extracted_at column
    gh_watchers = gh_watchers.pipe(add_extracted_at).pipe(constraints)

    return gh_watchers


def docs():
    """Extract documentation data."""

    # read in raw data
    data_glob = os.path.join(DATA_DIR, RAW_DATA_DIR, RAW_DATA_DOCS_DIR, "*.csv.gz")
    docs = ibis.read_csv(data_glob)

    # add extracted_at column
    docs = docs.pipe(add_extracted_at).pipe(constraints)

    return docs


def zulip_members():
    """Extract Zulip members data."""

    # read in raw data
    data_glob = os.path.join(DATA_DIR, RAW_DATA_DIR, RAW_DATA_ZULIP_DIR, "members.json")
    zulip_members = ibis.read_json(data_glob)

    # add extracted_at column
    zulip_members = zulip_members.pipe(add_extracted_at).pipe(constraints)

    return zulip_members


def zulip_messages():
    """Extract Zulip messages data."""

    # read in raw data
    data_glob = os.path.join(
        DATA_DIR, RAW_DATA_DIR, RAW_DATA_ZULIP_DIR, "messages.json"
    )
    zulip_messages = ibis.read_json(data_glob)

    # add extracted_at column
    zulip_messages = zulip_messages.pipe(add_extracted_at).pipe(constraints)

    return zulip_messages
