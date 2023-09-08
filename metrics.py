# imports
import toml
import ibis

import streamlit as st
import plotly.io as pio
import plotly.express as px
import ibis.selectors as s

from dotenv import load_dotenv
from datetime import datetime, timedelta

# options
## load .env
load_dotenv()

## config.toml
config = toml.load("config.toml")["app"]

## streamlit config
st.set_page_config(layout="wide")

## ibis config
con = ibis.connect(f"duckdb://{config['database']}", read_only=True)

# use precomputed data
stars = con.tables.stars
forks = con.tables.forks
pulls = con.tables.pulls
issues = con.tables.issues
commits = con.tables.commits
watchers = con.tables.watchers
backends = con.tables.backends
downloads = con.tables.downloads

# display metrics
f"""
# Analytics on Ibis

This is an end-to-end analytics project ingesting and processing >10M rows of data at little to no cost.

Built with [Ibis](https://ibis-project.org) and other OSS:

- [DuckDB](https://duckdb.org) (databases, query engine)
- [Streamlit](https://streamlit.io) (dashboard)
- [Plotly](https://plotly.com/python) (plotting)
- [Dagster](https://dagster.io) (DAG pipeline)
- [justfile](https://github.com/casey/just) (command runner)
- [TOML](https://toml.io) (configuration)

plus some freemium cloud services:

- [GitHub](https://github.com/lostmygithubaccount/ibis-analytics) (source control, CI/CD, source data)
- [Google BigQuery](https://cloud.google.com/free/docs/free-cloud-features#bigquery) (source data)
- [Azure](https://azure.microsoft.com) (VM, storage backups)
- [Streamlit Community Cloud](https://docs.streamlit.io/streamlit-community-cloud) (cloud hosting for dashboard)
- [MotherDuck](https://motherduck.com/) (cloud hosting for production databases)


:red[**Warning**]: GitHub and PyPI data is refreshed every 3 hours. Check [the CI/CD pipeline for the latest run](https://github.com/lostmygithubaccount/ibis-analytics/actions/workflows/cicd.yaml).

___
"""

with open("requirements.txt") as f:
    metrics_code = f.read()

with st.expander("show `requirements.txt`", expanded=False):
    st.code(metrics_code, line_numbers=True, language="python")

with open("config.toml") as f:
    config_code = f.read()

with st.expander("show `config.toml`", expanded=False):
    st.code(config_code, line_numbers=True, language="toml")

with open("justfile") as f:
    justfile_code = f.read()

with st.expander("show `justfile`", expanded=False):
    st.code(justfile_code, line_numbers=True, language="makefile")

with open(".github/workflows/cicd.yaml") as f:
    cicd_code = f.read()

with st.expander("show `cicd.yaml`", expanded=False):
    st.code(cicd_code, line_numbers=True, language="yaml")

with open("metrics.py") as f:
    metrics_code = f.read()

with st.expander("show `metrics.py` (source for this page)", expanded=False):
    st.code(metrics_code, line_numbers=True, language="python")

f"""
---
"""

f"""
## supported backends
"""

current_backends_total = (
    backends.filter(backends.ingested_at == backends.ingested_at.max())
    .num_backends.max()
    .to_pandas()
)
current_backends = ibis.memtable(backends.backends.unnest()).rename(backends="col0")

st.metric("Total", f"{current_backends_total:,}")
st.dataframe(current_backends, use_container_width=True)

f"""
## totals (all time)
"""

total_stars_all_time = stars.select("login").distinct().count().to_pandas()
total_forks_all_time = forks.select("login").distinct().count().to_pandas()
total_closed_issues_all_time = (
    issues.filter(ibis._.state == "closed")
    .select("number")
    .distinct()
    .count()
    .to_pandas()
)
total_merged_pulls_all_time = (
    pulls.filter(ibis._.state == "merged")
    .select("number")
    .distinct()
    .count()
    .to_pandas()
)
total_contributors_all_time = (
    pulls.filter(ibis._.merged_at != None)
    .select("login")
    .distinct()
    .count()
    .to_pandas()
)
total_watchers_all_time = watchers.select("login").distinct().count().to_pandas()
open_issues = (
    issues.filter(ibis._.is_closed != True)
    .select("number")
    .distinct()
    .count()
    .to_pandas()
)
open_pulls = (
    pulls.filter(ibis._.state == "open").select("number").distinct().count().to_pandas()
)

downloads_all_time = downloads["downloads"].sum().to_pandas()


col0, col1, col2 = st.columns(3)
with col0:
    st.metric(
        label="GitHub stars",
        value=f"{total_stars_all_time:,}",
    )
    st.metric("PyPI downloads", f"{downloads_all_time:,}")
with col1:
    st.metric("GitHub contributors", f"{total_contributors_all_time:,}")
    st.metric("GitHub forks", f"{total_forks_all_time:,}")
with col2:
    st.metric("GitHub PRs merged", f"{total_merged_pulls_all_time:,}")
    st.metric("GitHub issues closed", f"{total_closed_issues_all_time:,}")


# variables
with st.form(key="app"):
    days = st.number_input(
        "X days",
        min_value=1,
        max_value=365,
        value=90,
        step=30,
        format="%d",
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
total_issues_closed = (
    issues.filter(ibis._.closed_at != None)
    .filter(ibis._.closed_at >= datetime.now() - timedelta(days=days))
    .select("number")
    .distinct()
    .count()
    .to_pandas()
)
total_issues_closed_prev = (
    issues.filter(ibis._.closed_at != None)
    .filter(ibis._.closed_at <= datetime.now() - timedelta(days=days))
    .filter(ibis._.closed_at >= datetime.now() - timedelta(days=days * 2))
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
total_pulls_merged = (
    pulls.filter(ibis._.merged_at != None)
    .filter(ibis._.merged_at >= datetime.now() - timedelta(days=days))
    .select("number")
    .distinct()
    .count()
    .to_pandas()
)
total_pulls_merged_prev = (
    pulls.filter(ibis._.merged_at != None)
    .filter(ibis._.merged_at <= datetime.now() - timedelta(days=days))
    .filter(ibis._.merged_at >= datetime.now() - timedelta(days=days * 2))
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
total_downloads = (
    downloads.filter(ibis._.timestamp >= datetime.now() - timedelta(days=days))
    .downloads.sum()
    .to_pandas()
)
total_downloads_prev = (
    downloads.filter(ibis._.timestamp <= datetime.now() - timedelta(days=days))
    .filter(ibis._.timestamp >= datetime.now() - timedelta(days=days * 2))
    .downloads.sum()
    .to_pandas()
)

f"""
## totals (last {days} days)
"""
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric(
        label="GitHub stars",
        value=f"{total_stars:,}",
        delta=f"{total_stars - total_stars_prev:,}",
    )
    st.metric(
        label="PyPI downloads",
        value=f"{total_downloads:,}",
        delta=f"{total_downloads - total_downloads_prev:,}",
    )
with col2:
    st.metric(
        label="GitHub contributors",
        value=f"{total_contributors:,}",
        delta=f"{total_contributors - total_contributors_prev:,}",
    )
    st.metric(
        label="GitHub forks created",
        value=f"{total_forks:,}",
        delta=f"{total_forks - total_forks_prev:,}",
    )
with col3:
    st.metric(
        label="GitHub PRs opened",
        value=f"{total_pulls:,}",
        delta=f"{total_pulls - total_pulls_prev:,}",
    )
    st.metric(
        label="GitHub issues opened",
        value=f"{total_issues:,}",
        delta=f"{total_issues - total_issues_prev:,}",
    )
with col4:
    st.metric(
        label="GitHub PRs merged",
        value=f"{total_pulls_merged:,}",
        delta=f"{total_pulls_merged - total_pulls_merged_prev:,}",
    )
    st.metric(
        label="GitHub issues closed",
        value=f"{total_issues_closed:,}",
        delta=f"{total_issues_closed - total_issues_closed_prev:,}",
    )

f"""
## data (last {days} days)
"""

f"""
### downloads by system and version
"""
c0 = px.bar(
    downloads.filter(ibis._.timestamp > datetime.now() - timedelta(days=days))
    .group_by([ibis._.system, ibis._.version])
    .agg(ibis._.downloads.sum().name("downloads"))
    .order_by(ibis._.version.desc()),
    x="version",
    y="downloads",
    color="system",
    title=f"downloads by system and version",
)
st.plotly_chart(c0, use_container_width=True)

f"""
### stars by company
"""
st.dataframe(
    stars.filter(ibis._.starred_at > datetime.now() - timedelta(days=days))
    .group_by([ibis._.company])
    .agg(ibis._.count().name("stars"))
    .order_by(ibis._.stars.desc())
    .to_pandas(),
    use_container_width=True,
)

f"""
### issues by login
"""
c1 = px.bar(
    issues.filter(ibis._.created_at > datetime.now() - timedelta(days=days))
    .group_by([ibis._.login, ibis._.state])
    .agg(ibis._.count().name("issues"))
    .order_by(ibis._.issues.desc()),
    x="login",
    y="issues",
    color="state",
    title=f"issues by login",
)
st.plotly_chart(c1, use_container_width=True)

f"""
### PRs by login
"""
c2 = px.bar(
    pulls.filter(ibis._.created_at > datetime.now() - timedelta(days=days))
    .group_by([ibis._.login, ibis._.state])
    .agg(ibis._.count().name("pulls"))
    .order_by(ibis._.pulls.desc()),
    x="login",
    y="pulls",
    color="state",
    title=f"PRs by login",
)
st.plotly_chart(c2, use_container_width=True)
