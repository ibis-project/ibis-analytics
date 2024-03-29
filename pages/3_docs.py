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

# TODO: remove after docs are working
st.warning(
    "Documentation metrics are broken as I fight with rate limiting, check back soon!",
    icon="⚠️",
)

## ibis config
con = ibis.connect(f"duckdb://{config['database']}", read_only=True)

# use precomputed data
docs = con.table("docs")

# display metrics
"""
# Documentation metrics
"""

f"""
---
"""

with open("pages/3_docs.py") as f:
    docs_code = f.read()

with st.expander("Show source code", expanded=False):
    st.code(docs_code, line_numbers=True, language="python")

f"""
---

## totals (all time)
"""
total_visits_all_time = docs.count().to_pandas()
total_first_visits_all_time = docs.first_visit.sum().to_pandas()

st.metric("visits", f"{total_visits_all_time:,}")
st.metric("first visits", f"{total_first_visits_all_time:,}")

# variables
with st.form(key="docs"):
    days = st.number_input(
        "X days",
        min_value=1,
        max_value=3650,
        value=90,
        step=30,
        format="%d",
    )
    timescale = st.selectbox(
        "timescale (plots)",
        ["D", "W", "M", "Q", "Y"],
        index=0,
    )
    grouper = st.selectbox(
        "grouper",
        ["location", "browser", "referrer", "bot", "user_agent", "event"],
        index=2,
    )
    update_button = st.form_submit_button(label="update")

f"""
## last X days
"""
total_visits = (
    docs.filter(ibis._.timestamp >= datetime.now() - timedelta(days=days))
    .count()
    .to_pandas()
)
total_first_visits = (
    docs.filter(ibis._.timestamp >= datetime.now() - timedelta(days=days))
    .first_visit.sum()
    .to_pandas()
)

st.metric("visits", f"{total_visits:,}")
st.metric("first visits", f"{total_first_visits:,}")

# viz
c0 = px.bar(
    docs.filter(ibis._.timestamp >= datetime.now() - timedelta(days=days))
    .group_by([ibis._.timestamp.truncate(timescale).name("timestamp"), ibis._[grouper]])
    .agg([ibis._.count().name("visits"), ibis._.first_visit.sum().name("first_visits")])
    .order_by(ibis._.timestamp.desc())
    .mutate(
        (ibis._.first_visits / ibis._.visits).name("percent_first_visit"),
    )
    .mutate(ibis._[grouper].cast(str)[:20].name(grouper))
    .order_by([ibis._.timestamp.desc(), ibis._.visits.desc()]),
    x="timestamp",
    y="visits",
    color=grouper,
    title="docs visits",
)
st.plotly_chart(c0, use_container_width=True)

c2 = (
    docs.filter(ibis._.timestamp >= datetime.now() - timedelta(days=days))
    .group_by(
        [
            ibis._.timestamp.truncate(timescale).name("timestamp"),
            ibis._.path,
            ibis._.referrer,
        ]
    )
    .agg([ibis._.count().name("visits"), ibis._.first_visit.sum().name("first_visits")])
    .order_by(ibis._.timestamp.desc())
    .mutate(
        (100 * (ibis._.first_visits / ibis._.visits)).name("percent_first_visit"),
    )
    .order_by([ibis._.timestamp.desc(), ibis._.visits.desc()])
)
st.dataframe(c2, use_container_width=True)

c1 = px.line(
    docs.filter(ibis._.timestamp >= datetime.now() - timedelta(days=days))
    .group_by([ibis._.timestamp.truncate(timescale).name("timestamp")])
    .agg([ibis._.count().name("visits"), ibis._.first_visit.sum().name("first_visits")])
    .order_by(ibis._.timestamp.desc())
    .mutate((100 * (ibis._.first_visits / ibis._.visits)).name("percent_first_visit"))
    .order_by(ibis._.timestamp.desc()),
    x="timestamp",
    y="percent_first_visit",
    title="docs first visit %",
)
st.plotly_chart(c1, use_container_width=True)
