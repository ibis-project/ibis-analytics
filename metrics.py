# imports
import toml
import ibis

import streamlit as st
import plotly.express as px
from prefixed import Float

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
backends = con.tables.backends
downloads = con.tables.downloads

# display header stuff
with open("readme.md") as f:
    readme_code = f.read()

f"""
{readme_code}
"""

col0, col1 = st.columns(2)
with col0:
    """
    Built with [Ibis](https://ibis-project.org) and other OSS:

    - [DuckDB](https://duckdb.org) (databases, query engine)
    - [Streamlit](https://streamlit.io) (dashboard)
    - [Plotly](https://plotly.com/python) (plotting)
    - [Dagster](https://dagster.io) (DAG pipeline)
    - [justfile](https://github.com/casey/just) (command runner)
    - [TOML](https://toml.io) (configuration)"""

with col1:
    """
    And some freemium cloud services:

    - [GitHub](https://github.com/ibis-project/ibis-analytics) (source control, CI/CD, source data)
    - [Google BigQuery](https://cloud.google.com/free/docs/free-cloud-features#bigquery) (source data)
    - [Azure](https://azure.microsoft.com) (VM, storage backups)
    - [Streamlit Community Cloud](https://docs.streamlit.io/streamlit-community-cloud) (cloud hosting for dashboard)
    - [MotherDuck](https://motherduck.com/) (cloud hosting for production databases)"""


"""

:violet[**Note**]: data is refreshed every 3 hours. Check [the CI/CD pipeline for the latest run](https://github.com/ibis-project/ibis-analytics/actions/workflows/cicd.yaml).

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

"""
---
"""

"""
## supported backends
"""


def fmt_number(value):
    return f"{Float(value):.2H}"


current_backends_total = (
    backends.filter(backends.ingested_at == backends.ingested_at.max())
    .num_backends.max()
    .to_pandas()
)
current_backends = backends.backends.unnest().name("backends").as_table()

st.metric("Total", f"{current_backends_total:,}")
st.dataframe(current_backends, use_container_width=True)

"""
## totals (all time)
"""

total_stars_all_time = stars.login.nunique().to_pandas()
total_forks_all_time = forks.login.nunique().to_pandas()

total_closed_issues_all_time = issues.number.nunique(
    where=issues.state == "closed"
).to_pandas()

total_merged_pulls_all_time, total_contributors_all_time = (
    pulls.agg(
        total_merged_pulls_all_time=pulls.number.nunique(where=pulls.state == "merged"),
        total_contributors_all_time=pulls.login.nunique(
            where=pulls.merged_at.notnull()
        ),
    )
    .to_pandas()
    .squeeze()
)

downloads_all_time = downloads["downloads"].sum().to_pandas()


col0, col1, col2 = st.columns(3)
with col0:
    st.metric(
        label="GitHub stars",
        value=fmt_number(total_stars_all_time),
        help=f"{total_stars_all_time:,}",
    )
    st.metric(
        label="PyPI downloads",
        value=fmt_number(downloads_all_time),
        help=f"{downloads_all_time:,}",
    )
with col1:
    st.metric(
        label="GitHub contributors",
        value=fmt_number(total_contributors_all_time),
        help=f"{total_contributors_all_time:,}",
    )
    st.metric(
        label="GitHub forks",
        value=fmt_number(total_forks_all_time),
        help=f"{total_forks_all_time:,}",
    )
with col2:
    st.metric(
        label="GitHub PRs merged",
        value=fmt_number(total_merged_pulls_all_time),
        help=f"{total_merged_pulls_all_time:,}",
    )
    st.metric(
        label="GitHub issues closed",
        value=fmt_number(total_closed_issues_all_time),
        help=f"{total_closed_issues_all_time:,}",
    )


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


START = datetime.now() - timedelta(days=days * 2)
STOP = datetime.now() - timedelta(days=days)


# compute metrics
total_stars, total_stars_prev = (
    stars.agg(
        total_stars=stars.login.nunique(where=stars.starred_at >= STOP),
        total_stars_prev=stars.login.nunique(
            where=stars.starred_at.between(START, STOP)
        ),
    )
    .to_pandas()
    .squeeze()
)

total_forks, total_forks_prev = (
    forks.agg(
        total_forks=forks.login.nunique(where=forks.created_at >= STOP),
        total_forks_prev=forks.login.nunique(
            where=forks.created_at.between(START, STOP)
        ),
    )
    .to_pandas()
    .squeeze()
)

(
    total_issues,
    total_issues_prev,
    total_issues_closed,
    total_issues_closed_prev,
) = (
    issues.agg(
        total_issues=issues.login.nunique(where=issues.created_at >= STOP),
        total_issues_prev=issues.login.nunique(
            where=issues.created_at.between(START, STOP)
        ),
        total_issues_closed=issues.number.nunique(where=issues.closed_at >= STOP),
        total_issues_closed_prev=issues.number.nunique(
            where=issues.closed_at.between(START, STOP)
        ),
    )
    .to_pandas()
    .squeeze()
)

(
    total_pulls,
    total_pulls_prev,
    total_pulls_merged,
    total_pulls_merged_prev,
    total_contributors,
    total_contributors_prev,
) = (
    pulls.agg(
        total_pulls=pulls.number.nunique(where=pulls.created_at >= STOP),
        total_pulls_prev=pulls.number.nunique(
            where=pulls.created_at.between(START, STOP)
        ),
        total_pulls_merged=pulls.number.nunique(where=pulls.merged_at >= STOP),
        total_pulls_merged_prev=pulls.number.nunique(
            where=pulls.merged_at.between(START, STOP)
        ),
        total_contributors=pulls.login.nunique(where=pulls.merged_at >= STOP),
        total_contributors_prev=pulls.login.nunique(
            where=pulls.merged_at.between(START, STOP)
        ),
    )
    .to_pandas()
    .squeeze()
)

total_downloads, total_downloads_prev = (
    downloads.agg(
        total_downloads=downloads.downloads.sum(where=downloads.timestamp >= STOP),
        total_downloads_prev=downloads.downloads.sum(
            where=downloads.timestamp.between(START, STOP)
        ),
    )
    .to_pandas()
    .squeeze()
)


def delta(current, previous):
    delta = current - previous
    pct_change = int(round(100.0 * delta / previous, 0))
    return f"{fmt_number(delta)} ({pct_change:d}%)"


f"""
## totals (last {days} days)
"""
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric(
        label="GitHub stars",
        value=fmt_number(total_stars),
        delta=delta(total_stars, total_stars_prev),
        help=f"{total_stars:,}",
    )
    st.metric(
        label="PyPI downloads",
        value=fmt_number(total_downloads),
        delta=delta(total_downloads, total_downloads_prev),
        help=f"{total_downloads:,}",
    )
with col2:
    st.metric(
        label="GitHub contributors",
        value=fmt_number(total_contributors),
        delta=delta(total_contributors, total_contributors_prev),
        help=f"{total_contributors:,}",
    )
    st.metric(
        label="GitHub forks created",
        value=fmt_number(total_forks),
        delta=delta(total_forks, total_forks_prev),
        help=f"{total_forks:,}",
    )
with col3:
    st.metric(
        label="GitHub PRs opened",
        value=fmt_number(total_pulls),
        delta=delta(total_pulls, total_pulls_prev),
        help=f"{total_pulls:,}",
    )
    st.metric(
        label="GitHub issues opened",
        value=fmt_number(total_issues),
        delta=delta(total_issues, total_issues_prev),
        help=f"{total_issues:,}",
    )
with col4:
    st.metric(
        label="GitHub PRs merged",
        value=fmt_number(total_pulls_merged),
        delta=delta(total_pulls_merged, total_pulls_merged_prev),
        help=f"{total_pulls_merged:,}",
    )
    st.metric(
        label="GitHub issues closed",
        value=fmt_number(total_issues_closed),
        delta=delta(total_issues_closed, total_issues_closed_prev),
        help=f"{total_issues_closed:,}",
    )

f"""
## data (last {days} days)
"""

"""
### downloads by system and version
"""
c0 = px.bar(
    downloads.group_by([ibis._.system, ibis._.version])
    .agg(downloads=lambda t: t.downloads.sum(where=t.timestamp > STOP))
    .order_by(ibis._.version.desc()),
    x="version",
    y="downloads",
    color="system",
    title="downloads by system and version",
)
st.plotly_chart(c0, use_container_width=True)

"""
### stars by company
"""
st.dataframe(
    stars.group_by(ibis._.company)
    .agg(stars=lambda t: t.count(where=t.starred_at > STOP))
    .filter(ibis._.stars > 0)
    .order_by(ibis._.stars.desc())
    .to_pandas(),
    use_container_width=True,
)

"""
### issues by login
"""
c1 = px.bar(
    issues.group_by([ibis._.login, ibis._.state])
    .agg(issues=lambda t: t.count(where=t.created_at > STOP))
    .filter(ibis._.issues > 0)
    .order_by(ibis._.issues.desc()),
    x="login",
    y="issues",
    color="state",
    title="issues by login",
)
st.plotly_chart(c1, use_container_width=True)

"""
### PRs by login
"""
c2 = px.bar(
    pulls.group_by([ibis._.login, ibis._.state])
    .agg(pulls=lambda t: t.count(where=t.created_at > STOP))
    .filter(ibis._.pulls > 0)
    .order_by(ibis._.pulls.desc()),
    x="login",
    y="pulls",
    color="state",
    title="PRs by login",
)
st.plotly_chart(c2, use_container_width=True)
