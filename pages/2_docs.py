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
timescale = "W"

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

# viz
c0 = px.bar(
    docs.group_by(ibis._.timestamp.truncate(timescale).name("timestamp"))
    .agg([ibis._.count().name("visits"), ibis._.first_visit.sum().name("first_visits")])
    .order_by(ibis._.timestamp.desc())
    .mutate(
        (ibis._.first_visits / ibis._.visits).name("percent_first_visit"),
    )
    .order_by(ibis._.timestamp.desc()),
    x="timestamp",
    y="visits",
    title="Ibis docs visits",
)
st.plotly_chart(c0, use_container_width=True)

c1 = px.line(
    docs.group_by(ibis._.timestamp.truncate(timescale).name("timestamp"))
    .agg([ibis._.count().name("visits"), ibis._.first_visit.sum().name("first_visits")])
    .order_by(ibis._.timestamp.desc())
    .mutate(
        (100 * (ibis._.first_visits / ibis._.visits)).name("percent_first_visit"),
    )
    .order_by(ibis._.timestamp.desc()),
    x="timestamp",
    y="percent_first_visit",
    title="Ibis docs first visit %",
)
st.plotly_chart(c1, use_container_width=True)

c2 = (
    docs.group_by([ibis._.timestamp.truncate(timescale).name("timestamp"), ibis._.path])
    .agg([ibis._.count().name("visits"), ibis._.first_visit.sum().name("first_visits")])
    .order_by(ibis._.timestamp.desc())
    .mutate(
        (100 * (ibis._.first_visits / ibis._.visits)).name("percent_first_visit"),
    )
    .order_by([ibis._.timestamp.desc(), ibis._.visits.desc()])
)
st.dataframe(c2, use_container_width=True)
# st.plotly_chart(c2, use_container_width=True)
