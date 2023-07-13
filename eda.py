# imports
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
    return t


timescales = ["D", "W", "M", "Q", "Y"]

dfs = {timescale: {} for timescale in timescales}
figs = {timescale: {} for timescale in timescales}

downloads = clean_data(ibis.read_delta("data/raw/pypi/ibis-framework/file_downloads"))
downloads = downloads.drop("project").unpack("file").unpack("details")
downloads = downloads.order_by(ibis._.timestamp.desc())

stars = clean_data(
    ibis.read_json("data/raw/github/ibis-project/ibis/2023-07-10/stargazers.*.json")
)
stars = clean_data(stars.unpack("node"))
stars = stars.order_by(ibis._.starred_at.desc())
stars = stars.mutate(total_stars=ibis._.count().over(rows=(0, None)))

issues = clean_data(
    ibis.read_json("data/raw/github/ibis-project/ibis/2023-07-10/issues.*.json")
)
issues = clean_data(issues.unpack("node"))
issues = issues.order_by(ibis._.created_at.desc())
issues = issues.mutate(total_issues=ibis._.count().over(rows=(0, None)))

pulls = clean_data(
    ibis.read_json("data/raw/github/ibis-project/ibis/2023-07-10/pullRequests.*.json")
)
pulls = clean_data(pulls.unpack("node").unpack("author"))
pulls = pulls.order_by(ibis._.created_at.desc())
pulls = pulls.mutate(total_pulls=ibis._.count().over(rows=(0, None)))

forks = clean_data(
    ibis.read_json("data/raw/github/ibis-project/ibis/2023-07-10/forks.*.json")
)
forks = clean_data(forks.unpack("node").unpack("owner"))
forks = forks.order_by(ibis._.created_at.desc())
forks = forks.mutate(total_forks=ibis._.count().over(rows=(0, None)))

watchers = clean_data(
    ibis.read_json("data/raw/github/ibis-project/ibis/2023-07-10/watchers.*.json")
)
watchers = clean_data(watchers.unpack("node"))
# watchers = watchers.order_by(ibis._.updated_at.desc())
# watchers = watchers.mutate(total_watchers=ibis._.count().over(rows=(0, None)))

commits = clean_data(
    ibis.read_json("data/raw/github/ibis-project/ibis/2023-07-10/commits.*.json")
)
commits = clean_data(commits.unpack("node").unpack("author"))
commits = commits.order_by(ibis._.committed_date.desc())

for timescale in timescales:
    dfs[timescale]["downloads"] = (
        downloads.group_by(
            [ibis._.timestamp.truncate(timescale).name("timestamp"), ibis._.version]
        )
        .agg(
            downloads=ibis._.count(),
        )
        .order_by([ibis._.timestamp.desc(), ibis._.downloads.desc()])
    )
    dfs[timescale]["downloads"] = dfs[timescale]["downloads"].mutate(
        total_downloads=ibis._.downloads.sum().over(group_by="version", rows=(0, None))
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
        color="version",
        title="Total downloads by version",
    )

    dfs[timescale]["stars"] = (
        stars.group_by([ibis._.starred_at.truncate(timescale).name("starred_at")])
        .agg(
            [ibis._.count().name("stars"), ibis._.total_stars.min().name("total_stars")]
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
