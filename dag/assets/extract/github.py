# imports
import ibis
import dagster

from dag import functions as f


# assets
@dagster.asset
def extract_stars():
    """
    Extract the ingested GitHub stars data.
    """
    stars = f.clean_data(
        ibis.read_json("data/ingest/github/ibis-project/ibis/stargazers.*.json")
    )
    return stars


@dagster.asset
def extract_issues():
    """
    Extract the ingested GitHub issues data.
    """
    issues = f.clean_data(
        ibis.read_json("data/ingest/github/ibis-project/ibis/issues.*.json")
    )
    return issues


@dagster.asset
def extract_pulls():
    """
    Extract the ingested GitHub pull requests data.
    """
    pulls = f.clean_data(
        ibis.read_json("data/ingest/github/ibis-project/ibis/pullRequests.*.json")
    )
    return pulls


@dagster.asset
def extract_forks():
    """
    Extract the ingested GitHub forks data.
    """
    forks = f.clean_data(
        ibis.read_json("data/ingest/github/ibis-project/ibis/forks.*.json")
    )
    return forks


@dagster.asset
def extract_watchers():
    """
    Extract the ingested GitHub watchers data.
    """
    watchers = f.clean_data(
        ibis.read_json("data/ingest/github/ibis-project/ibis/watchers.*.json")
    )
    return watchers


@dagster.asset
def extract_commits():
    """
    Extract the ingested GitHub git commits data.
    """
    commits = f.clean_data(
        ibis.read_json("data/ingest/github/ibis-project/ibis/commits.*.json")
    )
    return commits
