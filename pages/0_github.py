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

# use precomputed data
issues = con.tables.issues
pulls = con.tables.pulls
stars = con.tables.stars
forks = con.tables.forks
commits = con.tables.commits
watchers = con.tables.watchers

# display metrics
"""
# GitHub  metrics
"""
# variables
with st.form(key="pypi"):
    days = st.number_input(
        "X days",
        min_value=1,
        max_value=3650,
        value=3650,
        step=30,
        format="%d",
    )
    timescale = st.selectbox(
        "timescale (plots)",
        ["D", "W", "M", "Q", "Y"],
        index=2,
    )
    exclude_voda = st.checkbox("exclude VoDa", value=False)
    update_button = st.form_submit_button(label="update")

# viz
c0 = px.line(
    stars.filter(ibis._.starred_at > datetime.now() - timedelta(days=days)),
    x="starred_at",
    y="total_stars",
    title="cumulative stars",
)
st.plotly_chart(c0, use_container_width=True)

c1 = px.bar(
    stars.filter(ibis._.starred_at > datetime.now() - timedelta(days=days))
    .mutate(ibis._.company[:20].name("company"))
    .group_by(
        [ibis._.starred_at.truncate(timescale).name("starred_at"), ibis._.company]
    )
    .agg(ibis._.count().name("stars")),
    x="starred_at",
    y="stars",
    color="company",
    title="stars by company",
)
st.plotly_chart(c1, use_container_width=True)

# cumulative stars by company
st.dataframe(
    stars.filter(ibis._.starred_at > datetime.now() - timedelta(days=days))
    .group_by([ibis._.company])
    .agg(ibis._.count().name("stars"))
    .order_by(ibis._.stars.desc()),
    use_container_width=True,
)

c3 = px.line(
    forks,
    x="created_at",
    y="total_forks",
    title="total forks",
)
st.plotly_chart(c3, use_container_width=True)

c4 = px.line(
    pulls.filter(ibis._.created_at > datetime.now() - timedelta(days=days)),
    x="created_at",
    y="total_pulls",
    title="total pull requests",
)
st.plotly_chart(c4, use_container_width=True)

c5 = px.line(
    issues.filter(ibis._.created_at > datetime.now() - timedelta(days=days)),
    x="created_at",
    y="total_issues",
    title="total issues",
)
st.plotly_chart(c5, use_container_width=True)

c6 = px.line(
    commits.filter(ibis._.committed_date > datetime.now() - timedelta(days=days)),
    x="committed_date",
    y="total_commits",
    title="total commits",
)
st.plotly_chart(c6, use_container_width=True)
