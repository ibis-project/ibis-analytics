# imports
import streamlit as st

## streamlit config
st.set_page_config(layout="wide")

# display header stuff
with open("readme.md") as f:
    readme_code = f.read()

f"""
{readme_code}
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

:violet[**Note**]: data is refreshed every day. Check [the CI/CD pipeline for the latest run](https://github.com/ibis-project/ibis-analytics/actions/workflows/cicd.yaml).
"""
