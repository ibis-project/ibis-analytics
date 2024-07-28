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
downloads = con.table("downloads")

# display metrics
"""
# PyPI metrics
"""

f"""
---
"""

with open("pages/1_pypi.py") as f:
    pypi_code = f.read()

with st.expander("Show source code", expanded=False):
    st.code(pypi_code, line_numbers=True, language="python")

f"""
---

## totals (all time)
"""
downloads_all_time = downloads["downloads"].sum().to_pandas()
st.metric("downloads", f"{downloads_all_time:,}")

with st.form(key="groupers"):
    groupers = st.multiselect(
        "groupers",
        ["version", "system", "country_code", "python"],
        ["version"],
    )
    update_button = st.form_submit_button(label="update")

f"""
downloads grouped by {groupers}
"""
grouped = (
    downloads.group_by(groupers)
    .aggregate(ibis._.downloads.sum().name("downloads"))
    .order_by(ibis._.downloads.desc())
)

st.dataframe(grouped, use_container_width=True)

# variables
with st.form(key="pypi"):
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
    exclude_linux = st.checkbox("exclude linux", value=False)
    update_button = st.form_submit_button(label="update")

if exclude_linux:
    downloads = downloads.filter(~ibis._.system.contains("Linux"))

f"""
## totals (last {days} days)
"""
downloads_last_x_days = (
    downloads.filter(ibis._.timestamp >= datetime.now() - timedelta(days=days))[
        "downloads"
    ]
    .sum()
    .to_pandas()
)
st.metric("downloads", f"{downloads_last_x_days:,}")

f"""
downloads grouped by {groupers}
"""
downloads_last_x_days_by_groupers = (
    downloads.filter(ibis._.timestamp >= datetime.now() - timedelta(days=days))
    .group_by(groupers)
    .agg(ibis._.downloads.sum().name("downloads"))
    .order_by(ibis._.downloads.desc())
)
st.dataframe(downloads_last_x_days_by_groupers, use_container_width=True)

# viz
t = (
    downloads.mutate(
        timestamp=downloads["timestamp"].truncate("D"),
    )
    .group_by("timestamp")
    .agg(downloads=ibis._["downloads"].sum())
    .select(
        "timestamp",
        downloads=ibis._["downloads"]
        .sum()
        .over(ibis.window(order_by="timestamp", preceding=28, following=0)),
    )
    .filter((ibis._["timestamp"] >= datetime.now() - timedelta(days=days)))
    .order_by("timestamp")
)

c00 = px.line(
    t,
    x="timestamp",
    y="downloads",
    title="downloads (28 days rolling)",
)
st.plotly_chart(c00, use_container_width=True)

t = (
    downloads.mutate(
        timestamp=downloads["timestamp"].truncate("D"),
        version=downloads["version"].split(".")[0],
    )
    .group_by("timestamp", "version")
    .agg(downloads=ibis._["downloads"].sum())
    .select(
        "timestamp",
        "version",
        downloads=ibis._["downloads"]
        .sum()
        .over(
            ibis.window(
                order_by="timestamp", group_by="version", preceding=28, following=0
            )
        ),
    )
    .filter((ibis._["timestamp"] >= datetime.now() - timedelta(days=days)))
    .order_by("timestamp")
)

c00 = px.line(
    t,
    x="timestamp",
    y="downloads",
    color="version",
    category_orders={
        "version": reversed(
            sorted(
                t.distinct(on="version")["version"].to_pyarrow().to_pylist(),
                key=lambda x: int(x),
            )
        )
    },
    title="downloads (28 days rolling) by version",
)
st.plotly_chart(c00, use_container_width=True)

c0 = px.line(
    downloads.filter(ibis._.timestamp >= datetime.now() - timedelta(days=days))
    .group_by(ibis._.timestamp.truncate(timescale).name("timestamp"))
    .agg(ibis._.downloads.sum().name("downloads"))
    .order_by(ibis._.timestamp.desc())
    .mutate(
        ibis._.downloads.sum()
        .over(rows=(0, None), order_by=ibis._.timestamp.desc())
        .name("total_downloads")
    )
    .order_by(ibis._.timestamp.desc()),
    x="timestamp",
    y="total_downloads",
    # log_y=True,
    title="cumulative downloads",
)
st.plotly_chart(c0, use_container_width=True)

c1 = px.bar(
    downloads.filter(ibis._.timestamp >= datetime.now() - timedelta(days=days))
    .group_by(
        [ibis._.timestamp.truncate(timescale).name("timestamp"), ibis._[groupers[0]]]
    )
    .agg(ibis._.downloads.sum().name("downloads"))
    .order_by(ibis._.timestamp.desc())
    .mutate(
        ibis._.downloads.sum()
        .over(rows=(0, None), group_by=groupers[0], order_by=ibis._.timestamp.desc())
        .name("total_downloads")
    )
    .order_by([ibis._.timestamp.desc(), ibis._.downloads.desc()]),
    x="timestamp",
    y="downloads",
    color=groupers[0],
    title=f"downloads by {groupers[0]}",
)
st.plotly_chart(c1, use_container_width=True)

c2 = px.line(
    downloads.filter(ibis._.timestamp >= datetime.now() - timedelta(days=days))
    .group_by(
        [ibis._.timestamp.truncate(timescale).name("timestamp"), ibis._[groupers[0]]]
    )
    .agg(ibis._.downloads.sum().name("downloads"))
    .order_by(ibis._.timestamp.desc())
    .mutate(
        ibis._.downloads.sum()
        .over(rows=(0, None), group_by=groupers[0], order_by=ibis._.timestamp.desc())
        .name("total_downloads")
    )
    .order_by([ibis._.timestamp.desc(), ibis._.downloads.desc()]),
    x="timestamp",
    y="total_downloads",
    log_y=True,
    color=groupers[0],
    title=f"cumulative downloads by {groupers[0]}",
)
st.plotly_chart(c2, use_container_width=True)
