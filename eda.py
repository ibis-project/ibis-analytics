# imports
import os
import ibis

import seaborn as sns

import plotly.io as pio
import plotly.express as px
import ibis.selectors as s
import matplotlib.pyplot as plt

from datetime import datetime, timedelta

# options
## ibis config
ibis.options.interactive = True
con = ibis.connect("duckdb://metrics.ddb")

## matplotlib config
plt.style.use("dark_background")

## seaborn config
sns.set(style="darkgrid")
sns.set(rc={"figure.figsize": (12, 10)})

## plotly config
pio.templates.default = "plotly_dark"


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


timescales = ["D", "W", "M", "Q", "Y"]

dfs = {timescale: {} for timescale in timescales}
figs = {timescale: {} for timescale in timescales}

downloads = clean_data(con.read_parquet("data/pypi/ibis-framework/*.parquet"))
downloads = downloads.drop("project").unpack("file").unpack("details")
downloads = agg_downloads(downloads)

downloads_modin = clean_data(con.read_parquet("data/pypi/modin/*.parquet"))
downloads_modin = downloads_modin.drop("project").unpack("file").unpack("details")
downloads_modin = agg_downloads(downloads_modin)

downloads_polars = clean_data(con.read_parquet("data/pypi/polars/*.parquet"))
downloads_polars = downloads_polars.drop("project").unpack("file").unpack("details")
downloads_polars = agg_downloads(downloads_polars)

downloads_siuba = clean_data(con.read_parquet("data/pypi/siuba/*.parquet"))
downloads_siuba = downloads_siuba.drop("project").unpack("file").unpack("details")
downloads_siuba = agg_downloads(downloads_siuba)

downloads_fugue = clean_data(con.read_parquet("data/pypi/fugue/*.parquet"))
downloads_fugue = downloads_fugue.drop("project").unpack("file").unpack("details")
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

"""
for timescale in timescales:
    dfs[timescale]["downloads"] = (
        downloads.group_by(
            [
                ibis._.timestamp.truncate(timescale).name("timestamp"),
                ibis._.version,
                ibis._.country_code,
            ]
        )
        .agg(
            downloads=ibis._.count(),
            total_downloads=ibis._.total_downloads.sum()
        )
        .order_by(ibis._.timestamp.desc())
    )

    figs[timescale]["downloads"] = px.line(
        dfs[timescale]["downloads"].to_pandas(),
        x="timestamp",
        y="downloads",
        color="version",
        title="Downloads by version",
    )

    figs[timescale]["total_downloads"] = px.line(
        dfs[timescale]["downloads"].to_pandas(),
        x="timestamp",
        y="total_downloads",
        title="Total downloads",
    )

    dfs[timescale]["stars"] = (
        stars.group_by([ibis._.starred_at.truncate(timescale).name("starred_at")])
        .agg(
            [ibis._.count().name("stars"), ibis._.total_stars.max().name("total_stars")]
        )
        .order_by([ibis._.starred_at.desc()])
    )

    figs[timescale]["stars"] = px.line(
        dfs[timescale]["stars"].to_pandas(),
        x="starred_at",
        y="stars",
        title="Stars added",
    )

    figs[timescale]["total_stars"] = px.line(
        dfs[timescale]["stars"].to_pandas(),
        x="starred_at",
        y="total_stars",
        markers=True,
        title="Total stars",
    )
"""

if __name__ == "__main__":
    con.create_table("downloads", downloads.to_pyarrow(), overwrite=True)
    con.create_table("downloads_modin", downloads_modin.to_pyarrow(), overwrite=True)
    con.create_table("downloads_polars", downloads_polars.to_pyarrow(), overwrite=True)
    con.create_table("downloads_siuba", downloads_siuba.to_pyarrow(), overwrite=True)
    con.create_table("downloads_fugue", downloads_fugue.to_pyarrow(), overwrite=True)
    con.create_table("stars", stars, overwrite=True)
    con.create_table("issues", issues, overwrite=True)
    con.create_table("pulls", pulls, overwrite=True)
    con.create_table("forks", forks, overwrite=True)
    con.create_table("watchers", watchers, overwrite=True)
    con.create_table("commits", commits, overwrite=True)
