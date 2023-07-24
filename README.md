# ibis-analytics

This repository is intended to be an analytics project for [Ibis](https://github.com/ibis-project/ibis) to understand key metrics.

## Setup

**Work in progress -- some manual setup/data upload required.**

To run this project, you need to install clone and install dependencies:

```bash
gh repo clone ibis-project/ibis-analytics
cd ibis-analytics
pip install -r requirements.txt
```

First, you need to create a `.env` file with the required variables:

```txt
BQ_PROJECT_ID="ibis-gbq"
GITHUB_TOKEN="github_pat_XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
MOTHERDUCK_TOKEN="XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
```

Then ingest the data (some access and manual data needed):

```bash
python src/ingest_gh.py
python src/ingest_pypi.py
python src/ingest_ci.py
```

Then transform and cache the data in a DuckDB database:

```bash
python eda.py run
```

Then export the cached DuckDB database to MotherDuck:

```bash
python src/export_md.py
```

Finally, run the Streamlit app that connects to MotherDuck:

```bash
streamlit run metrics.py
```

