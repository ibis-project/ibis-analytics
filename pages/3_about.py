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
st.set_page_config()

## ibis config
con = ibis.connect(f"duckdb://{config['database']}", read_only=True)

f"""
# Hybrid analytics with Ibis, Streamlit, DuckDB, and MotherDuck

The what, why, and how of this project.

**Work in progress**

## What?

This project is a dashboard that shows the health of the OSS project [Ibis](https://github.com/ibis-project/ibis).

It also serves as an end-to-end data project with Ibis, from ETL to BI to ML using as many industry buzz words as possible.

## Why?

## How?

### Overview

### Ingesting data

### Exploratory data analysis

### Defining metrics

### Setting up BI

### Onto ML

"""

# read in files
with open("metrics.py") as f:
    metrics_code = f.read()

with open("eda.py") as f:
    eda_code = f.read()

with open("config.toml") as f:
    config_code = f.read()

with open("justfile") as f:
    justfile_code = f.read()

with open("requirements.txt") as f:
    requirements_code = f.read()

with open("src/export_md.py") as f:
    export_md_code = f.read()

with open("src/ingest_gh.py") as f:
    ingest_gh_code = f.read()

with open("src/ingest_pypi.py") as f:
    ingest_pypi_code = f.read()

with open("src/ingest_ci.py") as f:
    ingest_ci_code = f.read()

with open("src/ingest_docs.py") as f:
    ingest_docs_code = f.read()

# show code
with st.expander("Show metrics.py", expanded=False):
    st.code(metrics_code, line_numbers=True, language="python")

with st.expander("Show eda.py", expanded=False):
    st.code(eda_code, line_numbers=True, language="python")

with st.expander("Show config.toml", expanded=False):
    st.code(config_code, line_numbers=True, language=None)

with st.expander("Show justfile", expanded=False):
    st.code(justfile_code, line_numbers=True, language=None)

with st.expander("Show requirements.txt", expanded=False):
    st.code(requirements_code, line_numbers=True, language=None)

with st.expander("Show export_md.py", expanded=False):
    st.code(export_md_code, line_numbers=True, language="python")

with st.expander("Show ingest_gh.py", expanded=False):
    st.code(ingest_gh_code, line_numbers=True, language="python")

with st.expander("Show ingest_pypi.py", expanded=False):
    st.code(ingest_pypi_code, line_numbers=True, language="python")

with st.expander("Show ingest_ci.py", expanded=False):
    st.code(ingest_ci_code, line_numbers=True, language="python")

with st.expander("Show ingest_docs.py", expanded=False):
    st.code(ingest_docs_code, line_numbers=True, language="python")
