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
members = con.table("zulip_members")
messages = con.table("zulip_messages")

# display metrics
"""
# Zulip  metrics
"""


f"""
---
"""

with open("pages/2_zulip.py") as f:
    zulip_code = f.read()

with st.expander("Show source code", expanded=False):
    st.code(zulip_code, line_numbers=True, language="python")

"""
---
"""

total_members_all_time = members.select("user_id").nunique().to_pandas()
total_messages_all_time = messages.select("id").nunique().to_pandas()

f"""
## totals (all time)
"""

col0, col1 = st.columns(2)

with col0:
    st.metric(label="members", value=total_members_all_time)
with col1:
    st.metric(label="messages", value=total_messages_all_time)

# viz
c0 = px.line(
    members.filter(ibis._.date_joined > datetime.now() - timedelta(days=days)),
    x="date_joined",
    y="total_members",
    title=f"members over time",
)
st.plotly_chart(c0, use_container_width=True)

c1 = px.line(
    # TODO: investigate hack here
    messages.filter(ibis._.timestamp > datetime.now() - timedelta(days=days)).order_by(
        ibis._.timestamp.desc()
    ),
    x="timestamp",
    y="total_messages",
    title=f"messages over time",
)
st.plotly_chart(c1, use_container_width=True)

## variables
# with st.form(key="zulip"):
#    days = st.number_input(
#        "X days",
#        min_value=1,
#        max_value=3650,
#        value=90,
#        step=30,
#        format="%d",
#    )
#    timescale = st.selectbox(
#        "timescale",
#        options=["days", "weeks", "months", "years"],
#        index=2,
#    )
#    update_button = st.form_submit_button(label="update")
