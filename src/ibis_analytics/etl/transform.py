# imports
import ibis
import ibis.selectors as s


# transform functions
def preprocess(t):
    """Common preprocessing steps."""

    # ensure unique records
    t = t.rename("snake_case")
    t = t.distinct(on=~s.c("extracted_at"), keep="first").order_by("extracted_at")

    return t


def postprocess(t):
    """Common postprocessing steps."""

    # ensure consistent column casing
    t = t.rename("snake_case")

    return t


# transform data assets
def gh_commits(gh_commits):
    """Transform GitHub commits data."""

    def transform(t):
        t = t.unpack("node").unpack("author").rename("snake_case")
        t = t.order_by(ibis._["committed_date"].desc())
        t = t.mutate(total_commits=ibis._.count().over(rows=(0, None)))
        return t

    gh_commits = gh_commits.pipe(preprocess).pipe(transform).pipe(postprocess)
    return gh_commits


def gh_issues(gh_issues):
    """Transform GitHub issues data."""

    def transform(t):
        issue_state = (
            ibis.case().when(ibis._["is_closed"], "closed").else_("open").end()
        )

        t = t.unpack("node").unpack("author").rename("snake_case")
        t = t.order_by(ibis._["created_at"].desc())
        t = t.mutate(is_closed=(ibis._["closed_at"] != None))
        t = t.mutate(total_issues=ibis._.count().over(rows=(0, None)))
        t = t.mutate(state=issue_state)
        t = (
            t.mutate(
                is_first_issue=(
                    ibis.row_number().over(
                        ibis.window(group_by="login", order_by=t["created_at"])
                    )
                    == 0
                )
            )
            .relocate("is_first_issue", "login", "created_at")
            .order_by(t["created_at"].desc())
        )
        return t

    gh_issues = gh_issues.pipe(preprocess).pipe(transform).pipe(postprocess)
    return gh_issues


def gh_prs(gh_prs):
    """Transform GitHub pull request (PR) data."""

    def transform(t):
        pull_state = (
            ibis.case()
            .when(ibis._["is_merged"], "merged")
            .when(ibis._["is_closed"], "closed")
            .else_("open")
            .end()
        )

        t = t.unpack("node").unpack("author").rename("snake_case")
        t = t.order_by(ibis._["created_at"].desc())
        t = t.mutate(is_merged=(ibis._["merged_at"] != None))
        t = t.mutate(is_closed=(ibis._["closed_at"] != None))
        t = t.mutate(total_pulls=ibis._.count().over(rows=(0, None)))
        # to remove bots
        # t = t.filter(
        #    ~(
        #        (ibis._.login == "ibis-squawk-bot")
        #        | (ibis._.login == "pre-commit-ci")
        #        | (ibis._.login == "renovate")
        #    )
        # )
        t = t.mutate(state=pull_state)
        t = t.mutate(
            merged_at=ibis._["merged_at"].cast("timestamp")
        )  # TODO: temporary fix

        # add first pull by login
        t = (
            t.mutate(
                is_first_pull=(
                    ibis.row_number().over(
                        ibis.window(group_by="login", order_by=t["created_at"])
                    )
                    == 0
                )
            )
            .relocate("is_first_pull", "login", "created_at")
            .order_by(t["created_at"].desc())
        )
        return t

    gh_prs = gh_prs.pipe(preprocess).pipe(transform).pipe(postprocess)
    return gh_prs


def gh_forks(gh_forks):
    """Transform GitHub forks data."""

    def transform(t):
        t = t.unpack("node").unpack("owner").rename("snake_case")
        t = t.order_by(ibis._["created_at"].desc())
        t = t.mutate(total_forks=ibis._.count().over(rows=(0, None)))
        return t

    gh_forks = gh_forks.pipe(preprocess).pipe(transform).pipe(postprocess)
    return gh_forks


def gh_stars(gh_stars):
    """Transform GitHub stargazers data."""

    def transform(t):
        t = t.unpack("node").rename("snake_case")
        # TODO: fix
        t = t.order_by(ibis._["starred_at"].desc())
        t = t.mutate(company=ibis._["company"].fill_null("Unknown"))
        t = t.mutate(total_stars=ibis._.count().over(rows=(0, None)))
        return t

    gh_stars = gh_stars.pipe(preprocess).pipe(transform).pipe(postprocess)
    return gh_stars


def gh_watchers(gh_watchers):
    """Transform GitHub watchers data."""

    def transform(t):
        t = t.unpack("node").rename("snake_case")
        # TODO: fix this
        t = t.order_by(ibis._["updated_at"].desc())
        t = t.mutate(total_t=ibis._.count().over(rows=(0, None)))
        t = t.order_by(ibis._["updated_at"].desc())
        return t

    gh_watchers = gh_watchers.pipe(preprocess).pipe(transform).pipe(postprocess)
    return gh_watchers


def docs(t):
    """
    Transform the docs data.
    """

    def transform(t):
        t = t.rename({"path": "2_path", "timestamp": "date"})
        return t

    docs = t.pipe(preprocess).pipe(transform).pipe(postprocess)
    return docs


def zulip_members(t):
    """
    Transform the Zulip members data.
    """

    def transform(t):
        # t = t.mutate(date_joined=ibis._["date_joined"].cast("timestamp"))
        t = t.filter(ibis._["is_bot"] == False)
        t = t.mutate(
            total_members=ibis._.count().over(
                rows=(0, None), order_by=ibis.desc("date_joined")
            )
        )
        t = t.relocate("full_name", "date_joined", "timezone")
        return t

    zulip_members = t.pipe(preprocess).pipe(transform).pipe(postprocess)
    return zulip_members


def zulip_messages(t):
    """
    Transform the Zulip messages data.
    """

    def transform(t):
        t = t.mutate(
            timestamp=ibis._["timestamp"].cast("timestamp"),
            last_edit_timestamp=ibis._["last_edit_timestamp"].cast("timestamp"),
        )
        t = t.filter(ibis._["stream_id"] != 405931)
        t = t.mutate(
            total_messages=ibis._.count().over(
                rows=(0, None), order_by=ibis.desc("timestamp")
            )
        )
        t = t.relocate(
            "sender_full_name",
            "display_recipient",
            "subject",
            "timestamp",
            "last_edit_timestamp",
        )
        return t

    zulip_messages = t.pipe(preprocess).pipe(transform).pipe(postprocess)
    return zulip_messages
