# ibis-analytics

This repository is intended to be an analytics project for [Ibis](https://github.com/ibis-project/ibis) to understand key metrics including:

- GitHub interactions (stars, forks, issues, PRs, etc.)
- Downloads (PyPI, Conda, etc.)
- Documentation interactions

## Setup

To run this project, you need to install clone and install dependencies:

```bash
gh repo clone ibis-project/ibis-analytics
cd ibis-analytics
pip install -r requirements.txt
```

First, you need to create a `.env` file with the required variables:

```txt
GITHUB_TOKEN="github_pat_XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
BQ_PROJECT_ID="voltrondata-demo"
```

Then, you need to ingest the data (currently only GitHub and PyPI):

```bash
python src/ingest_gh.py
python src/ingest_pypi.py
```

Use the `config.toml` to control ingestion settings.

The `eda.py` file should be run to generate a `metrics.ddb` DuckDB file.

```bash
python src/eda.py
```

You can then use an Ibis connection to the DuckDB database to query data, as shown in `app.py`. You can run the app with `streamlit run app.py`.

## Notes

This is a work-in-progress and very manual to setup. 
