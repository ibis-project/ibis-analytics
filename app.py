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
docs = con.tables.docs
downloads = con.tables.downloads
issues = con.tables.issues
pulls = con.tables.pulls
stars = con.tables.stars
forks = con.tables.forks
commits = con.tables.commits
watchers = con.tables.watchers

# display metrics
"""
# last X days metrics
"""

# variables
with st.form(key="timescale"):
    days = st.number_input(
        "X days",
        min_value=1,
        max_value=3650,
        value=28,
        step=7,
        format="%d",
    )
    timescale = st.selectbox(
        "timescale (plots)",
        ["D", "W", "M", "Y"],
        index=0,
    )
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
total_downloads = (
    downloads.filter(ibis._.timestamp >= datetime.now() - timedelta(days=days))
    .select("version")
    .count()
    .to_pandas()
)
total_downloads_prev = (
    downloads.filter(ibis._.timestamp <= datetime.now() - timedelta(days=days))
    .filter(ibis._.timestamp >= datetime.now() - timedelta(days=days * 2))
    .select("version")
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
total_docs_visits = (
    docs.filter(ibis._.timestamp >= datetime.now() - timedelta(days=days))
    .count()
    .to_pandas()
)
total_docs_visits_prev = (
    docs.filter(ibis._.timestamp <= datetime.now() - timedelta(days=days))
    .filter(ibis._.timestamp >= datetime.now() - timedelta(days=days * 2))
    .count()
    .to_pandas()
)
total_docs_return_visits = (
    docs.filter(ibis._.timestamp >= datetime.now() - timedelta(days=days)).count()
    - docs.filter(
        ibis._.timestamp >= datetime.now() - timedelta(days=days)
    ).first_visit.sum()
).to_pandas()
total_docs_return_visits_prev = (
    docs.filter(ibis._.timestamp <= datetime.now() - timedelta(days=days))
    .filter(ibis._.timestamp >= datetime.now() - timedelta(days=days * 2))
    .count()
    - docs.filter(ibis._.timestamp <= datetime.now() - timedelta(days=days))
    .filter(ibis._.timestamp >= datetime.now() - timedelta(days=days * 2))
    .first_visit.sum()
).to_pandas()

f"""
## totals (last {days} days)
"""
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric(
        label="stars",
        value=f"{total_stars:,}",
        delta=f"{total_stars - total_stars_prev:,}",
    )
    st.metric(
        label="Total forks",
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
        label="downloads",
        value=f"{total_downloads:,}",
        delta=f"{total_downloads - total_downloads_prev:,}",
    )
with col4:
    st.metric(
        label="docs visits",
        value=f"{total_docs_visits:,}",
        delta=f"{total_docs_visits - total_docs_visits_prev:,}",
    )
    st.metric(
        label="return docs visits",
        value=f"{total_docs_return_visits:,}",
        delta=f"{total_docs_return_visits - total_docs_return_visits_prev:,}",
    )

f"""
## plots (last {days} days)
"""

f"""
### Downloads by system and version
"""
c0 = px.bar(
    downloads.filter(ibis._.timestamp > datetime.now() - timedelta(days=days))
    .group_by([ibis._.system, ibis._.version])
    .agg(ibis._.downloads.sum().name("downloads"))
    .order_by(ibis._.downloads.desc()),
    x="version",
    y="downloads",
    color="system",
    title=f"Downloads by system and version",
)
st.plotly_chart(c0, use_container_width=True)
