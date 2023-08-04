# Justfile

# load environment variables
set dotenv-load

# aliases

# list justfile recipes
default:
    just --list

# run
run:
    @time python eda.py run

# ingest GitHub data
ingest-gh:
    @python src/ingest_gh.py

# ingest PyPI data
ingest-pypi:
    @python src/ingest_pypi.py

# ingest CI data
ingest-ci:
    @python src/ingest_ci.py

# ingest docs data
ingest-docs:
    @python src/ingest_docs.py

# ingest all data # TODO: add docs
ingest: ingest-gh ingest-pypi ingest-ci

# copy local database to MotherDuck
export:
    @python src/export_md.py

# cleanup daata
clean-data:
    -rm -r data/pypi || true
    -rm -r data/github || true

# remove DuckDB databases
clean-dbs:
    -rm *.ddb* || true

# streamlit stuff
app:
    @streamlit run metrics.py
