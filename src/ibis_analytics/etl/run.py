# imports
import typer

from ibis_analytics.config import (
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
from ibis_analytics.etl.extract import (
    gh_prs as extract_gh_prs,
    gh_forks as extract_gh_forks,
    gh_stars as extract_gh_stars,
    gh_issues as extract_gh_issues,
    gh_commits as extract_gh_commits,
    gh_watchers as extract_gh_watchers,
    docs as extract_docs,
    zulip_members as extract_zulip_members,
    zulip_messages as extract_zulip_messages,
)
from ibis_analytics.etl.transform import (
    gh_prs as transform_gh_prs,
    gh_forks as transform_gh_forks,
    gh_stars as transform_gh_stars,
    gh_issues as transform_gh_issues,
    gh_commits as transform_gh_commits,
    gh_watchers as transform_gh_watchers,
    docs as transform_docs,
    zulip_members as transform_zulip_members,
    zulip_messages as transform_zulip_messages,
)


# functions
def main(gh: bool, docs, zulip: bool):
    # instantiate catalog
    catalog = Catalog()

    # extract
    if gh:
        typer.echo("Extracting GitHub data...")
        extract_gh_prs_t = extract_gh_prs()
        extract_gh_forks_t = extract_gh_forks()
        extract_gh_stars_t = extract_gh_stars()
        extract_gh_issues_t = extract_gh_issues()
        extract_gh_commits_t = extract_gh_commits()
        extract_gh_watchers_t = extract_gh_watchers()

    if docs:
        typer.echo("Extracting docs data...")
        extract_docs_t = extract_docs()

    if zulip:
        typer.echo("Extracting Zulip data...")
        extract_zulip_members_t = extract_zulip_members()
        extract_zulip_messages_t = extract_zulip_messages()

    # transform
    if gh:
        typer.echo("Transforming GitHub data...")
        transform_gh_prs_t = transform_gh_prs(extract_gh_prs_t)
        transform_gh_forks_t = transform_gh_forks(extract_gh_forks_t)
        transform_gh_stars_t = transform_gh_stars(extract_gh_stars_t)
        transform_gh_issues_t = transform_gh_issues(extract_gh_issues_t)
        transform_gh_commits_t = transform_gh_commits(extract_gh_commits_t)
        transform_gh_watchers_t = transform_gh_watchers(extract_gh_watchers_t)

    if docs:
        typer.echo("Transforming docs data...")
        transform_docs_t = transform_docs(extract_docs_t)

    if zulip:
        typer.echo("Transforming Zulip data...")
        transform_zulip_members_t = transform_zulip_members(extract_zulip_members_t)
        transform_zulip_messages_t = transform_zulip_messages(extract_zulip_messages_t)

    # load
    if gh:
        typer.echo("Loading GitHub data into datalake...")
        catalog.write_table(transform_gh_prs_t, GH_PRS_TABLE)
        catalog.write_table(transform_gh_forks_t, GH_FORKS_TABLE)
        catalog.write_table(transform_gh_stars_t, GH_STARS_TABLE)
        catalog.write_table(transform_gh_issues_t, GH_ISSUES_TABLE)
        catalog.write_table(transform_gh_commits_t, GH_COMMITS_TABLE)
        catalog.write_table(transform_gh_watchers_t, GH_WATCHERS_TABLE)
    if docs:
        typer.echo("Loading docs data into datalake...")
        catalog.write_table(transform_docs_t, DOCS_TABLE)
    if zulip:
        typer.echo("Loading Zulip data into datalake...")
        catalog.write_table(transform_zulip_members_t, ZULIP_MEMBERS_TABLE)
        catalog.write_table(transform_zulip_messages_t, ZULIP_MESSAGES_TABLE)
