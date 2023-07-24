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
con = ibis.connect("duckdb://md:metrics", read_only=True)

# use precomputed data
downloads = con.tables.downloads

# display metrics
"""
# PyPI metrics
"""
# variables
with st.form(key="pypi"):
    days = st.number_input(
        "X days",
        min_value=1,
        max_value=3650,
        value=365,
        step=30,
        format="%d",
    )
    timescale = st.selectbox(
        "timescale (plots)",
        ["D", "W", "M", "Q", "Y"],
        index=1,
    )
    grouper = st.selectbox(
        "grouper",
        ["version", "system", "country_code", "python"],
        index=0,
    )
    exclude_linux = st.checkbox("exclude linux", value=True)
    update_button = st.form_submit_button(label="update")

if exclude_linux:
    downloads = downloads.filter(~ibis._.system.contains("Linux"))

# viz
c0 = px.bar(
    downloads.filter(ibis._.timestamp >= datetime.now() - timedelta(days=days))
    .group_by([ibis._.timestamp.truncate(timescale).name("timestamp"), ibis._[grouper]])
    .agg(ibis._.downloads.sum().name("downloads"))
    .order_by(ibis._.timestamp.desc())
    .mutate(
        ibis._.downloads.sum()
        .over(rows=(0, None), group_by=grouper, order_by=ibis._.timestamp.desc())
        .name("total_downloads")
    )
    .order_by([ibis._.timestamp.desc(), ibis._.downloads.desc()]),
    x="timestamp",
    y="downloads",
    color=grouper,
    title=f"downloads by {grouper}",
)
st.plotly_chart(c0, use_container_width=True)

c1 = px.line(
    downloads.filter(ibis._.timestamp >= datetime.now() - timedelta(days=days))
    .group_by([ibis._.timestamp.truncate(timescale).name("timestamp"), ibis._[grouper]])
    .agg(ibis._.downloads.sum().name("downloads"))
    .order_by(ibis._.timestamp.desc())
    .mutate(
        ibis._.downloads.sum()
        .over(rows=(0, None), group_by=grouper, order_by=ibis._.timestamp.desc())
        .name("total_downloads")
    )
    .order_by([ibis._.timestamp.desc(), ibis._.downloads.desc()]),
    x="timestamp",
    y="total_downloads",
    color=grouper,
    title=f"cumulative downloads by {grouper}",
)
st.plotly_chart(c1, use_container_width=True)
