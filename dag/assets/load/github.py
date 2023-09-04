# imports
import ibis
import dagster

from dag import functions as f


# assets
@dagster.asset
def load_stars(transform_stars):
    """Finalize the GitHub stars data."""
    return transform_stars


@dagster.asset
def load_issues(transform_issues):
    """Finalize the GitHub issues data."""
    return transform_issues


@dagster.asset
def load_pulls(transform_pulls):
    """Finalize the GitHub pull requests data."""
    return transform_pulls


@dagster.asset
def load_forks(transform_forks):
    """Finalize the GitHub forks data."""
    return transform_forks


@dagster.asset
def load_watchers(transform_watchers):
    """Finalize the GitHub watchers data."""
    return transform_watchers


@dagster.asset
def load_commits(transform_commits):
    """Finalize the GitHub git commits data."""
    return transform_commits
