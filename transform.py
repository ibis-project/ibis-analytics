# imports
import os
import re
import sys
import toml
import ibis
import ibis.selectors as s
import logging as log
import plotly.io as pio
import plotly.express as px

from dotenv import load_dotenv
from datetime import datetime, timedelta, date

# configuration
## logger
log.basicConfig(level=log.INFO)

## config.toml
config = toml.load("config.toml")["transform"]

## plotly config
pio.templates.default = "plotly_dark"

## load .env file
load_dotenv()

## variables
POETRY_MERGED_DATE = date(2021, 10, 15)
TEAMIZATION_DATE = date(2022, 11, 28)

con = ibis.connect("duckdb://cache.ddb")
ibis.options.interactive = False

# UDFs
@ibis.udf.scalar.python
def get_major_minor_version(version: str) -> str:
    match = re.match(r'^(\d+\.\d+)', version)
    if match:
        return match.group(1)
    else:
        return version

## scratch
def clean_data(t):
    t = t.relabel("snake_case")
    t = t.mutate(s.across(s.of_type("timestamp"), lambda x: x.cast("timestamp('UTC')")))
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
            ibis._.count().name("downloads"),
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
    downloads = downloads.mutate(ibis._["python"].fillna("").name("python_full"))
    downloads = downloads.mutate(get_major_minor_version(downloads["python_full"]).name("python"))
    return downloads


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
issues = issues.mutate(issue_state.name("state"))

pulls = clean_data(con.read_json("data/github/ibis-project/ibis/pullRequests.*.json"))
pulls = clean_data(pulls.unpack("node").unpack("author"))
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

forks = clean_data(con.read_json("data/github/ibis-project/ibis/forks.*.json"))
forks = clean_data(forks.unpack("node").unpack("owner"))
forks = forks.order_by(ibis._.created_at.desc())
forks = forks.mutate(total_forks=ibis._.count().over(rows=(0, None)))

watchers = clean_data(con.read_json("data/github/ibis-project/ibis/watchers.*.json"))
watchers = clean_data(watchers.unpack("node"))
watchers = watchers.order_by(ibis._.updated_at.desc())
watchers = watchers.mutate(total_watchers=ibis._.count().over(rows=(0, None)))
watchers = watchers.order_by(ibis._.updated_at.desc())

commits = clean_data(con.read_json("data/github/ibis-project/ibis/commits.*.json"))
commits = clean_data(commits.unpack("node").unpack("author"))
commits = commits.order_by(ibis._.committed_date.desc())
commits = commits.mutate(total_commits=ibis._.count().over(rows=(0, None)))

downloads = clean_data(con.read_parquet("data/pypi/ibis-framework/*.parquet"))
downloads = downloads.drop("project").unpack("file").unpack("details")
downloads = agg_downloads(downloads)

# create tables
log.info("processing stars...")
con.create_table("stars", stars, overwrite=True)
log.info("processing issues...")
con.create_table("issues", issues, overwrite=True)
log.info("processing pulls...")
con.create_table("pulls", pulls, overwrite=True)
log.info("processing forks...")
con.create_table("forks", forks, overwrite=True)
log.info("processing watchers...")
con.create_table("watchers", watchers, overwrite=True)
log.info("processing commits...")
con.create_table("commits", commits, overwrite=True)
log.info("processing downloads...")
con.create_table("downloads", downloads, overwrite=True)

if not os.getenv("CI"):
    docs = clean_data(con.read_csv("data/docs/*.csv*"))
    docs = docs.relabel({"2_path": "path", "date": "timestamp"})

    log.info("processing docs...")
    con.create_table("docs", docs, overwrite=True)

if config["ci_enabled"] and not os.getenv("CI"):
    ci_con = ibis.connect("duckdb://data/ci/ibis/raw.ddb")

    # TODO: processing
    jobs = ci_con.table("jobs")
    workflows = ci_con.table("workflows")
    analysis = ci_con.table("analysis")

    log.info("processing jobs...")
    con.create_table("jobs", jobs.to_pyarrow(), overwrite=True)
    log.info("processing workflows...")
    con.create_table("workflows", workflows.to_pyarrow(), overwrite=True)
    log.info("processing analysis...")
    con.create_table("analysis", analysis.to_pyarrow(), overwrite=True)
