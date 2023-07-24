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
con = ibis.connect("duckdb://md:metrics", read_only=str(True))

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
    exclude_voda = st.checkbox("exclude VoDa (TODO: implement)", value=False)
    update_button = st.form_submit_button(label="update")

# compute metrics
total_stars = (
    stars.filter(ibis._.starred_at >= datetime.now() - timedelta(days=days))
    .select("login")
    .distinct()
    .count()
    .to_pandas()
)
total_stars_prev = (
    stars.filter(ibis._.starred_at <= datetime.now() - timedelta(days=days))
    .filter(ibis._.starred_at >= datetime.now() - timedelta(days=days * 2))
    .select("login")
    .distinct()
    .count()
    .to_pandas()
)
total_forks = (
    forks.filter(ibis._.created_at >= datetime.now() - timedelta(days=days))
    .select("login")
    .distinct()
    .count()
    .to_pandas()
)
total_forks_prev = (
    forks.filter(ibis._.created_at <= datetime.now() - timedelta(days=days))
    .filter(ibis._.created_at >= datetime.now() - timedelta(days=days * 2))
    .select("login")
    .distinct()
    .count()
    .to_pandas()
)
total_issues = (
    issues.filter(ibis._.created_at >= datetime.now() - timedelta(days=days))
    .select("number")
    .distinct()
    .count()
    .to_pandas()
)
total_issues_prev = (
    issues.filter(ibis._.created_at <= datetime.now() - timedelta(days=days))
    .filter(ibis._.created_at >= datetime.now() - timedelta(days=days * 2))
    .select("number")
    .distinct()
    .count()
    .to_pandas()
)
total_pulls = (
    pulls.filter(ibis._.created_at >= datetime.now() - timedelta(days=days))
    .select("number")
    .distinct()
    .count()
    .to_pandas()
)
total_pulls_prev = (
    pulls.filter(ibis._.created_at <= datetime.now() - timedelta(days=days))
    .filter(ibis._.created_at >= datetime.now() - timedelta(days=days * 2))
    .select("number")
    .distinct()
    .count()
    .to_pandas()
)
total_contributors = (
    pulls.filter(ibis._.merged_at != None)
    .filter(ibis._.merged_at >= datetime.now() - timedelta(days=days))
    .select("login")
    .distinct()
    .count()
    .to_pandas()
)
total_contributors_prev = (
    pulls.filter(ibis._.merged_at != None)
    .filter(ibis._.merged_at <= datetime.now() - timedelta(days=days))
    .filter(ibis._.merged_at >= datetime.now() - timedelta(days=days * 2))
    .select("login")
    .distinct()
    .count()
    .to_pandas()
)
total_watchers = (
    watchers.filter(ibis._.updated_at >= datetime.now() - timedelta(days=days))
    .select("login")
    .distinct()
    .count()
    .to_pandas()
)
total_watchers_prev = (
    watchers.filter(ibis._.updated_at <= datetime.now() - timedelta(days=days))
    .filter(ibis._.updated_at >= datetime.now() - timedelta(days=days * 2))
    .select("login")
    .distinct()
    .count()
    .to_pandas()
)

f"""
## totals (last {days} days)
"""
col1, col2, col3 = st.columns(3)
with col1:
    st.metric(
        label="stars",
        value=f"{total_stars:,}",
        delta=f"{total_stars - total_stars_prev:,}",
    )
    st.metric(
        label="forks",
        value=f"{total_forks:,}",
        delta=f"{total_forks - total_forks_prev:,}",
    )
with col2:
    st.metric(
        label="issues",
        value=f"{total_issues:,}",
        delta=f"{total_issues - total_issues_prev:,}",
    )
    st.metric(
        label="pull requests",
        value=f"{total_pulls:,}",
        delta=f"{total_pulls - total_pulls_prev:,}",
    )
with col3:
    st.metric(
        label="contributors",
        value=f"{total_contributors:,}",
        delta=f"{total_contributors - total_contributors_prev:,}",
    )
    st.metric(
        label="watchers",
        value=f"{total_watchers:,}",
        delta=f"{total_watchers - total_watchers_prev:,}",
    )

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
