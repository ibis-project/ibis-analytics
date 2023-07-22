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

# c4a = px.bar(
#    compare_downloads(downloads_modin),
#    x="timestamp",
#    y="downloads",
#    color="version",
#    title="Modin downloads for Windows/MacOS users by version",
# )
# st.plotly_chart(c4a, use_container_width=True)
#
# c4b = px.bar(
#    compare_downloads(downloads_polars),
#    x="timestamp",
#    y="downloads",
#    color="version",
#    title="Polars downloads for Windows/MacOS users by version",
# )
# st.plotly_chart(c4b, use_container_width=True)
#
# c4c = px.bar(
#    compare_downloads(downloads_siuba),
#    x="timestamp",
#    y="downloads",
#    color="version",
#    title="Siuba downloads for Windows/MacOS users by version",
# )
# st.plotly_chart(c4c, use_container_width=True)
#
# c4d = px.bar(
#    compare_downloads(downloads_fugue),
#    x="timestamp",
#    y="downloads",
#    color="version",
#    title="Fugue downloads for Windows/MacOS users by version",
# )
# st.plotly_chart(c4d, use_container_width=True)
