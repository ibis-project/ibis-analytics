# Justfile

# load environment variables
set dotenv-load

# aliases
alias preview:=app

# list justfile recipes
default:
    just --list

# setup
setup:
    @pip install -r requirements.txt

# eda
eda:
    ipython -i eda.py

# run
run:
    @time python transform.py

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

# load a backup
load-backup:
    @python src/ingest_az.py

# ingest all data # TODO: add docs
ingest: ingest-gh ingest-pypi ingest-ci

# copy local database to MotherDuck
export-md:
    @python src/export_md.py

# copy local database to Azure
export-backup:
    @python src/export_az.py

# export all data
export: export-md export-backup

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

# temp
goat:
    #!/bin/bash +x
    api="https://ibis.goatcounter.com/api/v0"
    curl -v -X POST \
        --header 'Content-Type: application/json' \
        --header "Authorization: Bearer $GOAT_TOKEN" \
        "$api/export"

#ingest-docs:
#    #!/bin/bash +x
#    api="https://ibis.goatcounter.com/api/v0/export/791705491/download"
#    curl -v -X POST \
#        --header 'Content-Type: application/json' \
#        --header "Authorization: Bearer $GOAT_TOKEN" \
#        "$api"
#
#load-backup:
#    az storage azcopy blob download \
#        --account-name notonedrive \
#        --account-key $AZURE_STORAGE_SECRET \
#        -c metrics-backup \
#        -s cache.ddb \
#        -d cache.ddb

upload-goat:
    az storage azcopy blob upload \
        --account-name notonedrive \
        --account-key $AZURE_STORAGE_SECRET \
        -c goat-export \
        -s data/docs/*.csv.gz \
        -d goatcounter-export-ibis.csv.gz
