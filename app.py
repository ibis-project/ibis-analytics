# imports
import ibis

import streamlit as st
import plotly.io as pio
import plotly.express as px
import ibis.selectors as s

from datetime import datetime, timedelta

# options
## streamlit config
st.set_page_config(layout="wide")

## ibis config
ibis.options.interactive = True
con = ibis.connect("duckdb://metrics.ddb")

## plotly config
pio.templates.default = "plotly_dark"

# functions
def compare_downloads(downloads):
    t = (
        downloads.filter(ibis._.system.contains("Windows") | ibis._.system.contains("Darwin"))
        .group_by([ibis._.timestamp.truncate(timescale).name("timestamp"), ibis._.version])
        .agg(ibis._.downloads.sum().name("downloads"))
        .order_by(ibis._.timestamp.desc())
        .mutate(
            ibis._.downloads.sum()
            .over(rows=(0, None), group_by="version", order_by=ibis._.timestamp.desc())
            .name("total_downloads")
        )
        .order_by([ibis._.timestamp.desc(), ibis._.downloads.desc()])
    )

    return t



# variables
timescale = "M"

# use precomputed data
downloads = con.tables.downloads
downloads_modin = con.tables.downloads_modin
downloads_polars = con.tables.downloads_polars
issues = con.tables.issues
pulls = con.tables.pulls
stars = con.tables.stars
forks = con.tables.forks
commits = con.tables.commits
watchers = con.tables.watchers

# compute metrics
total_stars = stars.select("login").distinct().count().to_pandas()
total_forks = forks.select("login").distinct().count().to_pandas()
total_issues = issues.select("number").distinct().count().to_pandas()
total_pulls = pulls.select("number").distinct().count().to_pandas()
total_downloads = downloads.downloads.sum().to_pandas()
total_contributors = (
    pulls.filter(ibis._.merged_at != None)
    .select("login")
    .distinct()
    .count()
    .to_pandas()
)

# display metrics
"""
##  Key metrics
"""
col1, col2, col3 = st.columns(3)
with col1:
    st.metric(label="Total stars", value=f"{total_stars:,}")
    st.metric(label="Total forks", value=f"{total_forks:,}")
with col2:
    st.metric(label="Total issues", value=f"{total_issues:,}")
    st.metric(label="Total pull requests", value=f"{total_pulls:,}")
with col3:
    st.metric(label="Total contributors", value=f"{total_contributors:,}")
    st.metric(label="Total downloads", value=f"{total_downloads:,}")

"""
## Important plots
"""
c1 = px.bar(
    downloads.filter(ibis._.timestamp > datetime.now() - timedelta(days=30))
    .group_by(ibis._.system)
    .agg(ibis._.downloads.sum().name("downloads"))
    .order_by(ibis._.downloads.desc()),
    x="system",
    y="downloads",
    title="Downloads in the last 30 days by system",
)
st.plotly_chart(c1, use_container_width=True)

c1a = px.bar(
    downloads.filter(ibis._.timestamp > datetime.now() - timedelta(days=30))
    .group_by(ibis._.version)
    .agg(ibis._.downloads.sum().name("downloads"))
    .order_by(ibis._.downloads.desc()),
    x="version",
    y="downloads",
    title="Downloads in the last 30 days by version",
)
st.plotly_chart(c1a, use_container_width=True)

c1b = px.bar(
    downloads.filter(ibis._.timestamp > datetime.now() - timedelta(days=30))
    .group_by([ibis._.system, ibis._.version])
    .agg(ibis._.downloads.sum().name("downloads"))
    .order_by(ibis._.downloads.desc()),
    x="version",
    y="downloads",
    color="system",
    title="Downloads in the last 30 days by system and version",
)
st.plotly_chart(c1b, use_container_width=True)

c2 = px.bar(
    downloads.group_by(
        [ibis._.timestamp.truncate(timescale).name("timestamp"), ibis._.version]
    )
    .agg(ibis._.downloads.sum().name("downloads"))
    .order_by(ibis._.timestamp.desc())
    .mutate(
        ibis._.downloads.sum()
        .over(rows=(0, None), group_by="version", order_by=ibis._.timestamp.desc())
        .name("total_downloads")
    )
    .order_by([ibis._.timestamp.desc(), ibis._.downloads.desc()]),
    x="timestamp",
    y="downloads",
    color="version",
    title="Downloads by version",
)
st.plotly_chart(c2, use_container_width=True)

c3 = px.bar(
    downloads.group_by(
        [ibis._.timestamp.truncate(timescale).name("timestamp"), ibis._.system]
    )
    .agg(ibis._.downloads.sum().name("downloads"))
    .order_by(ibis._.timestamp.desc())
    .mutate(
        ibis._.downloads.sum()
        .over(rows=(0, None), group_by="system", order_by=ibis._.timestamp.desc())
        .name("total_downloads")
    )
    .order_by([ibis._.timestamp.desc(), ibis._.downloads.desc()]),
    x="timestamp",
    y="downloads",
    color="system",
    title="Downloads by system",
)
st.plotly_chart(c3, use_container_width=True)


c4 = px.bar(
    compare_downloads(downloads),
    x="timestamp",
    y="downloads",
    color="version",
    title="Downloads for Windows/MacOS users by version",
)
st.plotly_chart(c4, use_container_width=True)

c4a = px.bar(
    compare_downloads(downloads_modin),
    x="timestamp",
    y="downloads",
    color="version",
    title="Modin downloads for Windows/MacOS users by version",
)
st.plotly_chart(c4a, use_container_width=True)

c4b = px.bar(
    compare_downloads(downloads_polars),
    x="timestamp",
    y="downloads",
    color="version",
    title="Polars downloads for Windows/MacOS users by version",
)
st.plotly_chart(c4b, use_container_width=True)

c5 = px.line(
    stars,
    x="starred_at",
    y="total_stars",
    title="Total stars",
)
st.plotly_chart(c5, use_container_width=True)

c6 = px.line(
    forks,
    x="created_at",
    y="total_forks",
    title="Total forks",
)
st.plotly_chart(c6, use_container_width=True)

c7 = px.line(
    pulls,
    x="created_at",
    y="total_pulls",
    title="Total pull requests",
)
st.plotly_chart(c7, use_container_width=True)

c8 = px.line(
    issues,
    x="created_at",
    y="total_issues",
    title="Total issues",
)
st.plotly_chart(c8, use_container_width=True)

c10 = px.line(
    commits,
    x="committed_date",
    y="total_commits",
    title="Total commits",
)
st.plotly_chart(c10, use_container_width=True)

# warn that this data is probably off
"""
:red[Warning:] this data is likely wrong
"""

c9 = px.line(
    watchers,
    x="updated_at",
    y="total_watchers",
    title="Total watchers",
)
st.plotly_chart(c9, use_container_width=True)
