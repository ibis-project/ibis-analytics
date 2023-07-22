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
con = ibis.connect("duckdb://metrics.ddb", read_only=str(True))

## plotly config
pio.templates.default = "plotly_dark"

# variables
timescale = "M"

# use precomputed data
docs = con.tables.docs
downloads = con.tables.downloads
# downloads_modin = con.tables.downloads_modin
# downloads_polars = con.tables.downloads_polars
# downloads_siuba = con.tables.downloads_siuba
# downloads_fugue = con.tables.downloads_fugue
issues = con.tables.issues
pulls = con.tables.pulls
stars = con.tables.stars
forks = con.tables.forks
commits = con.tables.commits
watchers = con.tables.watchers


# functions
def compare_downloads(downloads):
    t = (
        downloads.filter(
            ibis._.system.contains("Windows") | ibis._.system.contains("Darwin")
        )
        .group_by(
            [ibis._.timestamp.truncate(timescale).name("timestamp"), ibis._.version]
        )
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


# viz
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
:red[Warning:] this chart is likely wrong/misleading
"""

c9 = px.line(
    watchers,
    x="updated_at",
    y="total_watchers",
    title="Total watchers",
)
st.plotly_chart(c9, use_container_width=True)
