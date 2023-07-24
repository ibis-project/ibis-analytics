# imports
import os
import sys
import ibis

import seaborn as sns

import plotly.io as pio
import plotly.express as px
import ibis.selectors as s
import matplotlib.pyplot as plt

from datetime import datetime, timedelta, date

# options
## ibis config
con = ibis.connect("duckdb://cache.ddb")
ibis.options.interactive = True

## matplotlib config
plt.style.use("dark_background")

## seaborn config
sns.set(style="darkgrid")
sns.set(rc={"figure.figsize": (12, 10)})

## plotly config
pio.templates.default = "plotly_dark"

## variables
POETRY_MERGED_DATE = date(2021, 10, 15)
TEAMIZATION_DATE = date(2022, 11, 28)

if len(sys.argv) == 1:
    docs = con.table("docs")
    downloads = con.table("downloads")
    stars = con.table("stars")
    issues = con.table("issues")
    pulls = con.table("pulls")
    forks = con.table("forks")
    watchers = con.table("watchers")
    commits = con.table("commits")
    jobs = con.table("jobs")
    workflows = con.table("workflows")
    analysis = con.table("analysis")
else:
    ibis.options.interactive = False

    ## scratch
    def clean_data(t):
        t = t.relabel("snake_case")
        t = t.mutate(
            s.across(s.of_type("timestamp"), lambda x: x.cast("timestamp('UTC')"))
        )
        return t

    def agg_downloads(downloads):
        downloads = (
            downloads.group_by(
                [
                    ibis._.timestamp.truncate("D").name("timestamp"),
                    ibis._.country_code,
                    ibis._.version,
                    ibis._.python,
                    ibis._.system["name"].name("system"),
                ]
            )
            .agg(
                # TODO: fix this
                ibis._.count()
                .name("downloads"),
            )
            .order_by(ibis._.timestamp.desc())
            .mutate(
                ibis._.downloads.sum()
                .over(
                    rows=(0, None),
                    group_by=["country_code", "version", "python", "system"],
                    order_by=ibis._.timestamp.desc(),
                )
                .name("total_downloads")
            )
            .order_by(ibis._.timestamp.desc())
        )
        return downloads

    docs = clean_data(con.read_csv("data/docs/*.csv*"))
    docs = docs.relabel({"2_path": "path", "date": "timestamp"})

    downloads = clean_data(con.read_parquet("data/pypi/ibis-framework/*.parquet"))
    downloads = downloads.drop("project").unpack("file").unpack("details")
    downloads = agg_downloads(downloads)

    stars = clean_data(con.read_json("data/github/ibis-project/ibis/stargazers.*.json"))
    stars = clean_data(stars.unpack("node"))
    stars = stars.order_by(ibis._.starred_at.desc())
    stars = stars.mutate(ibis._.company.fillna("Unknown").name("company"))
    stars = stars.mutate(total_stars=ibis._.count().over(rows=(0, None)))

    issues = clean_data(con.read_json("data/github/ibis-project/ibis/issues.*.json"))
    issues = clean_data(issues.unpack("node").unpack("author"))
    issues = issues.order_by(ibis._.created_at.desc())
    issues = issues.mutate((ibis._.closed_at != None).name("is_closed"))
    issues = issues.mutate(ibis._.count().over(rows=(0, None)).name("total_issues"))
    issue_state = ibis.case().when(issues.is_closed, "closed").else_("open").end()

    pulls = clean_data(
        con.read_json("data/github/ibis-project/ibis/pullRequests.*.json")
    )
    pulls = clean_data(pulls.unpack("node").unpack("author"))
    pulls = pulls.order_by(ibis._.created_at.desc())
    pulls = pulls.mutate((ibis._.merged_at != None).name("is_merged"))
    pulls = pulls.mutate((ibis._.closed_at != None).name("is_closed"))
    pulls = pulls.mutate(total_pulls=ibis._.count().over(rows=(0, None)))

    pull_state = (
        ibis.case()
        .when(pulls.is_merged, "merged")
        .when(pulls.is_closed, "closed")
        .else_("open")
        .end()
    )
    pulls = pulls.mutate(pull_state.name("state"))

    forks = clean_data(con.read_json("data/github/ibis-project/ibis/forks.*.json"))
    forks = clean_data(forks.unpack("node").unpack("owner"))
    forks = forks.order_by(ibis._.created_at.desc())
    forks = forks.mutate(total_forks=ibis._.count().over(rows=(0, None)))

    watchers = clean_data(
        con.read_json("data/github/ibis-project/ibis/watchers.*.json")
    )
    watchers = clean_data(watchers.unpack("node"))
    watchers = watchers.order_by(ibis._.updated_at.desc())
    watchers = watchers.mutate(total_watchers=ibis._.count().over(rows=(0, None)))
    watchers = watchers.order_by(ibis._.updated_at.desc())

    commits = clean_data(con.read_json("data/github/ibis-project/ibis/commits.*.json"))
    commits = clean_data(commits.unpack("node").unpack("author"))
    commits = commits.order_by(ibis._.committed_date.desc())
    commits = commits.mutate(total_commits=ibis._.count().over(rows=(0, None)))

    ci_con = ibis.connect("duckdb://data/ci/ibis/raw.ddb")
    jobs = ci_con.table("jobs")
    workflows = ci_con.table("workflows")
    analysis = ci_con.table("analysis")

    con.create_table("jobs", jobs.to_pyarrow(), overwrite=True)
    con.create_table("workflows", workflows.to_pyarrow(), overwrite=True)
    con.create_table("analysis", analysis.to_pyarrow(), overwrite=True)
    con.create_table("docs", docs, overwrite=True)
    con.create_table("downloads", downloads, overwrite=True)
    con.create_table("stars", stars, overwrite=True)
    con.create_table("issues", issues, overwrite=True)
    con.create_table("pulls", pulls, overwrite=True)
    con.create_table("forks", forks, overwrite=True)
    con.create_table("watchers", watchers, overwrite=True)
    con.create_table("commits", commits, overwrite=True)
