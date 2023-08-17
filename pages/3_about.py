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

f"""
# Hybrid analytics with Ibis, Streamlit, DuckDB, and MotherDuck

## What?

This project is a dashboard that shows the health of the OSS project [Ibis](https://github.com/ibis-project/ibis).

## Why?

As a Technical Product Manager at Voltron Data, I want to ensure the health of Ibis as an open-source standard Python dataframe library. We had a project tracking metrics but it was written in R and and this seemed like a good excuse to do an end-to-end analytics project with Ibis and try both Streamlit and MotherDuck.

## How?

The source code can be found here: https://github.com/lostmygithubaccount/ibis-analytics.

The tools and services used include:

- Ibis (dataframe library)
- BigQuery (source data)
- GitHub web APIs (source data)
- DuckDB (database and query engine)
- MotherDuck (cloud service for DuckDB)
- Streamlit (dashboard)
- Streamlit Community Cloud (cloud service for Streamlit)
- GitHub (source control, CI/CD)
- justfile (command runner)
- TOML (configuration)

The Python requirements are:
"""

with open("requirements.txt") as f:
    requirements_code = f.read()

with st.expander("Show requirements.txt", expanded=False):
    st.code(requirements_code, line_numbers=True, language="text")

"""

The `justfile` defines the commands -- let's get an overview and dig into how the project is used:

"""

with open("justfile") as f:
    justfile_code = f.read()

with st.expander("Show justfile", expanded=False):
    st.code(justfile_code, line_numbers=True, language="makefile")

"""
The `config.toml` file defines the configuration for the project:
"""

with open("config.toml") as f:
    config_code = f.read()

with st.expander("Show config.toml", expanded=False):
    st.code(config_code, line_numbers=True, language="toml")


"""
### Ingesting data

The first step is to ingest data from:

- the GitHub GraphQL API
- the PyPI downloads BigQuery project
- the Ibis CI BigQuery project
- the GoatCounter documentation metrics

The data is stored in the local filesystem in the `data/` directory in:

- JSON files for GitHub response data
- a Parquet file for the PyPI downloads from BigQuery
- a DuckDB database file for the Ibis CI data from BigQuery
- a gzipped CSV file for the GoatCounter documentation metrics

You can view the Python scripts for ingesting data:

"""

with open("src/ingest_gh.py") as f:
    ingest_gh_code = f.read()

with open("src/ingest_pypi.py") as f:
    ingest_pypi_code = f.read()

with open("src/ingest_ci.py") as f:
    ingest_ci_code = f.read()

with open("src/ingest_docs.py") as f:
    ingest_docs_code = f.read()

with st.expander("Show ingest_gh.py", expanded=False):
    st.code(ingest_gh_code, line_numbers=True, language="python")

with st.expander("Show ingest_pypi.py", expanded=False):
    st.code(ingest_pypi_code, line_numbers=True, language="python")

with st.expander("Show ingest_ci.py", expanded=False):
    st.code(ingest_ci_code, line_numbers=True, language="python")

with st.expander("Show ingest_docs.py", expanded=False):
    st.code(ingest_docs_code, line_numbers=True, language="python")


"""
:red[Note:] documentation data ingestion is not finished.

### Exploratory data analysis

Having a quick iteration loop for exploring and analyzing the data, then transforming it into the proper shape for BI and ML, is critical. To achieve this, I setup an `eda.py` file that contains code I run every new Python session:
"""

with open("eda.py") as f:
    eda_code = f.read()

with st.expander("Show eda.py", expanded=False):
    st.code(eda_code, line_numbers=True, language="python")

"""

I can then run `ipython -i eda.py` to get a Python REPL with all the data loaded and ready to go. This makes it easy to explore and visualize the data.

### Data transformation

A lot of the ingested data isn't needed downstream and there's a bit of cleaning to do. I save code in `transform.py` as I develop it in the previous stage and can iteratively build my transforms.

"""

with open("transform.py") as f:
    transform_code = f.read()

with st.expander("Show transform.py", expanded=False):
    st.code(transform_code, line_numbers=True, language="python")

"""

### Defining metrics

Metrics are easily defined (and reusable) as Ibis expressions. Since we're deploying this with Streamlit and the project isn't huge, we simply keep the metrics definition in the BI layer:

"""
with open("metrics.py") as f:
    metrics_code = f.read()

with st.expander("Show metrics.py", expanded=False):
    st.code(metrics_code, line_numbers=True, language="python")
"""

Notice it was easy to iterate with EDA, transform and cache the data into a local database, and take my exploration and visualization directly to a locally deployed dashboard.

### Deploying and automating

Now I need to automate the data pipeline and create a public dashboard. Time to move to a hybrid local + cloud approach Fortunately, this is easy with GitHub, Ibis, DuckDB, MotherDuck, Streamlit, and Streamlit Community Cloud.

We first copy over local DuckDB tables to MotherDuck:
"""

with open("src/export_md.py") as f:
    export_md_code = f.read()

with st.expander("Show export_md.py", expanded=False):
    st.code(export_md_code, line_numbers=True, language="python")

"""
 Notice that we can just switch the value in the `config.toml` to switch between a local DuckDB cache and the production MotherDuck database.

Since our tasks are defined in the `justfile` and we have a `requirements.txt` file, we can easily automate the end-to-end pipeline in a simple GitHub Action:
"""

with open(".github/workflows/cicd.yaml") as f:
    cicd_code = f.read()

with st.expander("Show cicd.yaml", expanded=False):
    st.code(cicd_code, line_numbers=True, language="yaml")


"""

This checks out the repository, authenitcates with Google Cloud Platform, sets up the Python environment, ingests the data sources, transforms the data into clean tables, and syncs those tables to MotherDuck.

It runs on PRs (that change relevant files), every 3 hours, and on a manual trigger.

With that, data ingestion and transformation is automated. All that's left is to deploy with app to Streamlit Community Cloud, which is just a few clicks in their GUI!

## Next steps

ML (LLMs???) probably.
"""
