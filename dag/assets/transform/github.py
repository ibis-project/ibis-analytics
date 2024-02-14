# imports
import ibis
import dagster

from dag import functions as f


# assets
@dagster.asset
def transform_stars(extract_stars):
    """Transform the GitHub stars data."""
    stars = f.clean_data(extract_stars.unpack("node"))
    stars = stars.order_by(ibis._.starred_at.desc())
    stars = stars.mutate(ibis._.company.fillna("Unknown").name("company"))
    stars = stars.mutate(total_stars=ibis._.count().over(rows=(0, None)))
    return stars


@dagster.asset
def transform_issues(extract_issues):
    """Transform the GitHub issues data."""
    issues = f.clean_data(extract_issues.unpack("node").unpack("author"))
    issues = issues.order_by(ibis._.created_at.desc())
    issues = issues.mutate((ibis._.closed_at != None).name("is_closed"))
    issues = issues.mutate(ibis._.count().over(rows=(0, None)).name("total_issues"))
    issue_state = ibis.case().when(issues.is_closed, "closed").else_("open").end()
    issues = issues.mutate(issue_state.name("state"))
    return issues


@dagster.asset
def transform_pulls(extract_pulls):
    """Transform the GitHub pull requests data."""
    pulls = f.clean_data(extract_pulls.unpack("node").unpack("author"))
    pulls = pulls.order_by(ibis._.created_at.desc())
    pulls = pulls.mutate((ibis._.merged_at != None).name("is_merged"))
    pulls = pulls.mutate((ibis._.closed_at != None).name("is_closed"))
    pulls = pulls.mutate(total_pulls=ibis._.count().over(rows=(0, None)))
    # to remove bots
    # pulls = pulls.filter(
    #    ~(
    #        (ibis._.login == "ibis-squawk-bot")
    #        | (ibis._.login == "pre-commit-ci")
    #        | (ibis._.login == "renovate")
    #    )
    # )
    pull_state = (
        ibis.case()
        .when(pulls.is_merged, "merged")
        .when(pulls.is_closed, "closed")
        .else_("open")
        .end()
    )
    pulls = pulls.mutate(pull_state.name("state"))
    pulls = pulls.mutate(
        merged_at=ibis._.merged_at.cast("timestamp")
    )  # TODO: temporary fix
    return pulls


@dagster.asset
def transform_forks(extract_forks):
    """Transform the GitHub forks data."""
    forks = f.clean_data(extract_forks.unpack("node").unpack("owner"))
    forks = forks.order_by(ibis._.created_at.desc())
    forks = forks.mutate(total_forks=ibis._.count().over(rows=(0, None)))
    return forks


@dagster.asset
def transform_watchers(extract_watchers):
    """Transform the GitHub watchers data."""
    watchers = f.clean_data(extract_watchers.unpack("node"))
    watchers = watchers.order_by(ibis._.updated_at.desc())
    watchers = watchers.mutate(total_watchers=ibis._.count().over(rows=(0, None)))
    watchers = watchers.order_by(ibis._.updated_at.desc())
    return watchers


@dagster.asset
def transform_commits(extract_commits):
    """Transform the GitHub git commits data."""
    commits = f.clean_data(extract_commits.unpack("node").unpack("author"))
    commits = commits.order_by(ibis._.committed_date.desc())
    commits = commits.mutate(total_commits=ibis._.count().over(rows=(0, None)))
    return commits
