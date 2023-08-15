# imports
import toml
import ibis

import streamlit as st
import plotly.io as pio
import plotly.express as px
import ibis.selectors as s

from datetime import datetime, timedelta

# options
## config.toml
config = toml.load("config.toml")["app"]

## streamlit config
st.set_page_config(layout="wide")

## ibis config
con = ibis.connect(f"duckdb://{config['database']}", read_only=True)

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
with st.form(key="app"):
    days = st.number_input(
        "X days",
        min_value=1,
        max_value=365,
        value=7,
        step=7,
        format="%d",
    )
    update_button = st.form_submit_button(label="update")

# metric
def metricfy(val):
    """
    The trillion dollar mistake.
    """
    if val == None:
        return 0
    else:
        return val


# compute metrics
total_stars = metricfy(
    stars.filter(ibis._.starred_at >= datetime.now() - timedelta(days=days))
    .select("login")
    .distinct()
    .count()
    .to_pandas()
)
total_stars_prev = metricfy(
    stars.filter(ibis._.starred_at <= datetime.now() - timedelta(days=days))
    .filter(ibis._.starred_at >= datetime.now() - timedelta(days=days * 2))
    .select("login")
    .distinct()
    .count()
    .to_pandas()
)
total_forks = metricfy(
    forks.filter(ibis._.created_at >= datetime.now() - timedelta(days=days))
    .select("login")
    .distinct()
    .count()
    .to_pandas()
)
total_forks_prev = metricfy(
    forks.filter(ibis._.created_at <= datetime.now() - timedelta(days=days))
    .filter(ibis._.created_at >= datetime.now() - timedelta(days=days * 2))
    .select("login")
    .distinct()
    .count()
    .to_pandas()
)
total_issues = metricfy(
    issues.filter(ibis._.created_at >= datetime.now() - timedelta(days=days))
    .select("number")
    .distinct()
    .count()
    .to_pandas()
)
total_issues_prev = metricfy(
    issues.filter(ibis._.created_at <= datetime.now() - timedelta(days=days))
    .filter(ibis._.created_at >= datetime.now() - timedelta(days=days * 2))
    .select("number")
    .distinct()
    .count()
    .to_pandas()
)
total_pulls = metricfy(
    pulls.filter(ibis._.created_at >= datetime.now() - timedelta(days=days))
    .select("number")
    .distinct()
    .count()
    .to_pandas()
)
total_pulls_prev = metricfy(
    pulls.filter(ibis._.created_at <= datetime.now() - timedelta(days=days))
    .filter(ibis._.created_at >= datetime.now() - timedelta(days=days * 2))
    .select("number")
    .distinct()
    .count()
    .to_pandas()
)
total_contributors = metricfy(
    pulls.filter(ibis._.merged_at != None)
    .filter(ibis._.merged_at >= datetime.now() - timedelta(days=days))
    .select("login")
    .distinct()
    .count()
    .to_pandas()
)
total_contributors_prev = metricfy(
    pulls.filter(ibis._.merged_at != None)
    .filter(ibis._.merged_at <= datetime.now() - timedelta(days=days))
    .filter(ibis._.merged_at >= datetime.now() - timedelta(days=days * 2))
    .select("login")
    .distinct()
    .count()
    .to_pandas()
)
total_downloads = metricfy(
    downloads.filter(ibis._.timestamp >= datetime.now() - timedelta(days=days))
    .downloads.sum()
    .to_pandas()
)
total_downloads_prev = metricfy(
    downloads.filter(ibis._.timestamp <= datetime.now() - timedelta(days=days))
    .filter(ibis._.timestamp >= datetime.now() - timedelta(days=days * 2))
    .downloads.sum()
    .to_pandas()
)
total_docs_visits = metricfy(
    docs.filter(ibis._.timestamp >= datetime.now() - timedelta(days=days))
    .count()
    .to_pandas()
)
total_docs_visits_prev = metricfy(
    docs.filter(ibis._.timestamp <= datetime.now() - timedelta(days=days))
    .filter(ibis._.timestamp >= datetime.now() - timedelta(days=days * 2))
    .count()
    .to_pandas()
)
total_docs_return_visits = metricfy(
    (
        docs.filter(
            ibis._.timestamp >= datetime.now() - timedelta(days=days)
        ).first_visit.sum()
    ).to_pandas()
)
total_docs_return_visits_prev = metricfy(
    (
        docs.filter(ibis._.timestamp <= datetime.now() - timedelta(days=days))
        .filter(ibis._.timestamp >= datetime.now() - timedelta(days=days * 2))
        .first_visit.sum()
    ).to_pandas()
)

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

f"""
### docs visits by path and referrer
"""
st.dataframe(
    docs.filter(ibis._.timestamp > datetime.now() - timedelta(days=days))
    .group_by([ibis._.path, ibis._.referrer])
    .agg(ibis._.count().name("visits"))
    # .mutate(ibis._.referrer.cast(str)[:20].name("referrer"))
    .order_by(ibis._.visits.desc()),
    use_container_width=True,
)
