# imports
import streamlit as st

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
    - [Zulip](https://zulip.com) (source data)
    - [Azure](https://azure.microsoft.com) (VM, storage backups)
    - [Streamlit Community Cloud](https://docs.streamlit.io/streamlit-community-cloud) (cloud hosting for dashboard)
    - [MotherDuck](https://motherduck.com/) (cloud hosting for production databases)"""


"""

:violet[**Note**]: data is refreshed every 3 hours. Check [the CI/CD pipeline for the latest run](https://github.com/ibis-project/ibis-analytics/actions/workflows/cicd.yaml).
"""
