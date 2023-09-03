# imports
import ibis
import dagster

from dag import functions as f


# assets
@dagster.multi_asset(
    outs={
        "extract_stars": dagster.AssetOut(),
        "extract_issues": dagster.AssetOut(),
        "extract_pulls": dagster.AssetOut(),
        "extract_forks": dagster.AssetOut(),
        "extract_watchers": dagster.AssetOut(),
        "extract_commits": dagster.AssetOut(),
    }
)
def extract_github():
    """
    Load the GitHub data.
    """
    # imports
    stars = f.clean_data(
        ibis.read_json("data/ingest/github/ibis-project/ibis/stargazers.*.json")
    )
    issues = f.clean_data(
        ibis.read_json("data/ingest/github/ibis-project/ibis/issues.*.json")
    )
    pulls = f.clean_data(
        ibis.read_json("data/ingest/github/ibis-project/ibis/pullRequests.*.json")
    )
    forks = f.clean_data(
        ibis.read_json("data/ingest/github/ibis-project/ibis/forks.*.json")
    )
    watchers = f.clean_data(
        ibis.read_json("data/ingest/github/ibis-project/ibis/watchers.*.json")
    )
    commits = f.clean_data(
        ibis.read_json("data/ingest/github/ibis-project/ibis/commits.*.json")
    )
    return stars, issues, pulls, forks, watchers, commits
