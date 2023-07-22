# imports
import os
import sys
import ibis

import seaborn as sns

import plotly.io as pio
import plotly.express as px
import ibis.selectors as s
import matplotlib.pyplot as plt

from datetime import datetime, timedelta

# options
## ibis config
con = ibis.connect("duckdb://eda.ddb")
ibis.options.interactive = True

## matplotlib config
plt.style.use("dark_background")

## seaborn config
sns.set(style="darkgrid")
sns.set(rc={"figure.figsize": (12, 10)})

## plotly config
pio.templates.default = "plotly_dark"

## variables
compare = False

if len(sys.argv) == 1:
    docs = con.table("docs")
    downloads = con.table("downloads")
    if compare:
        downloads_modin = con.table("downloads_modin")
        downloads_polars = con.table("downloads_polars")
        downloads_siuba = con.table("downloads_siuba")
        downloads_fugue = con.table("downloads_fugue")
    stars = con.table("stars")
    issues = con.table("issues")
    pulls = con.table("pulls")
    forks = con.table("forks")
    watchers = con.table("watchers")
    commits = con.table("commits")
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
                .cast("int32")
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
                .cast("int64")
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

    if compare:
        downloads_modin = clean_data(con.read_parquet("data/pypi/modin/*.parquet"))
        downloads_modin = (
            downloads_modin.drop("project").unpack("file").unpack("details")
        )
        downloads_modin = agg_downloads(downloads_modin)

        downloads_polars = clean_data(con.read_parquet("data/pypi/polars/*.parquet"))
        downloads_polars = (
            downloads_polars.drop("project").unpack("file").unpack("details")
        )
        downloads_polars = agg_downloads(downloads_polars)

        downloads_siuba = clean_data(con.read_parquet("data/pypi/siuba/*.parquet"))
        downloads_siuba = (
            downloads_siuba.drop("project").unpack("file").unpack("details")
        )
        downloads_siuba = agg_downloads(downloads_siuba)

        downloads_fugue = clean_data(con.read_parquet("data/pypi/fugue/*.parquet"))
        downloads_fugue = (
            downloads_fugue.drop("project").unpack("file").unpack("details")
        )
        downloads_fugue = agg_downloads(downloads_fugue)

    stars = clean_data(
        con.read_json("data/github/ibis-project/ibis/2023-07-20/stargazers.*.json")
    )
    stars = clean_data(stars.unpack("node"))
    stars = stars.order_by(ibis._.starred_at.desc())
    stars = stars.mutate(total_stars=ibis._.count().over(rows=(0, None)))

    issues = clean_data(
        con.read_json("data/github/ibis-project/ibis/2023-07-20/issues.*.json")
    )
    issues = clean_data(issues.unpack("node"))
    issues = issues.order_by(ibis._.created_at.desc())
    issues = issues.mutate(total_issues=ibis._.count().over(rows=(0, None)))

    pulls = clean_data(
        con.read_json("data/github/ibis-project/ibis/2023-07-20/pullRequests.*.json")
    )
    pulls = clean_data(pulls.unpack("node").unpack("author"))
    pulls = pulls.order_by(ibis._.created_at.desc())
    pulls = pulls.mutate(total_pulls=ibis._.count().over(rows=(0, None)))

    forks = clean_data(
        con.read_json("data/github/ibis-project/ibis/2023-07-20/forks.*.json")
    )
    forks = clean_data(forks.unpack("node").unpack("owner"))
    forks = forks.order_by(ibis._.created_at.desc())
    forks = forks.mutate(total_forks=ibis._.count().over(rows=(0, None)))

    watchers = clean_data(
        con.read_json("data/github/ibis-project/ibis/2023-07-20/watchers.*.json")
    )
    watchers = clean_data(watchers.unpack("node"))
    watchers = watchers.order_by(ibis._.updated_at.desc())
    watchers = watchers.mutate(total_watchers=ibis._.count().over(rows=(0, None)))
    watchers = watchers.order_by(ibis._.updated_at.desc())

    commits = clean_data(
        con.read_json("data/github/ibis-project/ibis/2023-07-20/commits.*.json")
    )
    commits = clean_data(commits.unpack("node").unpack("author"))
    commits = commits.order_by(ibis._.committed_date.desc())
    commits = commits.mutate(total_commits=ibis._.count().over(rows=(0, None)))

    con.create_table("docs", docs, overwrite=True)
    con.create_table("downloads", downloads, overwrite=True)
    if compare:
        con.create_table("downloads_modin", downloads_modin, overwrite=True)
        con.create_table("downloads_polars", downloads_polars, overwrite=True)
        con.create_table("downloads_siuba", downloads_siuba, overwrite=True)
        con.create_table("downloads_fugue", downloads_fugue, overwrite=True)
    con.create_table("stars", stars, overwrite=True)
    con.create_table("issues", issues, overwrite=True)
    con.create_table("pulls", pulls, overwrite=True)
    con.create_table("forks", forks, overwrite=True)
    con.create_table("watchers", watchers, overwrite=True)
    con.create_table("commits", commits, overwrite=True)

