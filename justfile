# Justfile

# load environment variables
set dotenv-load

# variables
module := "dag"

# aliases
alias preview:=app

# list justfile recipes
default:
    just --list

# setup
setup:
    @pip install -r requirements.txt

# install
install:
    @pip install -e '.[dev]'

# ingest
ingest:
    @python {{module}}/ingest.py

# deploy
deploy:
    @python {{module}}/deploy.py

# backup
backup:
    @python {{module}}/backup.py

# upload
upload:
    az storage azcopy blob upload \
        --account-name ibisanalytics \
        --container $AZURE_STORAGE_CONTAINER \
        --source 'data/backup/cloud/data' \
        --destination . \
        --recursive

# DANGER
upload-prod:
    az storage azcopy blob upload \
        --account-name ibisanalytics \
        --container prod \
        --source 'data/backup/cloud/data/' \
        --destination . \
        --recursive

# download
download:
    az storage azcopy blob download \
        --account-name ibisanalytics \
        --container $AZURE_STORAGE_CONTAINER \
        --source 'data' \
        --destination 'data/backup/cloud' \
        --recursive
    mkdir -p data/ingest
    cp -r data/backup/cloud/data/ingest/* data/ingest

# upload
sync:
    az storage azcopy blob sync \
        --account-name ibisanalytics \
        --container $AZURE_STORAGE_CONTAINER \
        --source 'data/backup/cloud'

dag:
    @dagster dev -m {{module}}

# run
run:
    @dagster job execute -j all_assets -m {{module}}

# test
test:
    @python metrics.py
    @python pages/0_github.py
    @python pages/1_pypi.py
    @python pages/2_docs.py
    @python pages/3_about.py

# streamlit stuff
app:
    @streamlit run metrics.py

# smoke-test
smoke-test:
    @black --check .

# clean
clean:
    @rm -r *.ddb* || true
    @rm -r data/system || true
    @rm -r data/backup || true

# open
open-dag:
    @open http://localhost:3000/asset-groups

open-dash:
    @open https://ibis-analytics.streamlit.app

cicd:
    @gh workflow run cicd.yaml

vm-start:
    @az vm start -n cicd -g ibis-analytics

# wip below here
goat:
    #!/bin/bash +x
    api="https://ibis.goatcounter.com/api/v0"
    curl -v -X POST \
        --header 'Content-Type: application/json' \
        --header "Authorization: Bearer $GOAT_TOKEN" \
        "$api/export"

#
