# justfile

# load environment variables
set dotenv-load

# variables
module := "dag"

# aliases
alias fmt:=format
alias open:=open-dash
alias dag-open:=open-dag
alias preview:=app

# list justfile recipes
default:
    just --list

# setup
setup:
    @pip install --upgrade -r requirements.txt

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
        --source 'data/*' \
        --destination 'data' \
        --recursive

# DANGER
upload-prod:
    az storage azcopy blob upload \
        --account-name ibisanalytics \
        --container prod \
        --source 'data/*' \
        --destination 'data' \
        --recursive

# download
download:
    rm -r data |ta| true
    az storage azcopy blob download \
        --account-name ibisanalytics \
        --container $AZURE_STORAGE_CONTAINER \
        --source 'data/*' \
        --destination 'data' \
        --recursive

download-prod:
    rm -r data || true
    az storage azcopy blob download \
        --account-name ibisanalytics \
        --container prod \
        --source 'data/*' \
        --destination 'data' \
        --recursive


# sync
sync:
    az storage azcopy blob sync \
        --account-name ibisanalytics \
        --container $AZURE_STORAGE_CONTAINER \
        --source 'data' \
        --destination 'data'

sync-prod:
    az storage azcopy blob sync \
        --account-name ibisanalytics \
        --container prod \
        --source 'data' \
        --destination 'data'

# dag
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
    @python pages/2_about.py

# streamlit stuff
app:
    @streamlit run metrics.py

# format
format:
    @ruff format .

# smoke-test
smoke-test:
    @ruff format --check .

# clean
clean:
    @rm -r *.ddb* || true
    @rm -r data/system || true
    @rm -r data/backup || true
    @rm data/backup.ddb || true

# open dag
open-dag:
    @open http://localhost:3000/asset-groups

# open dash
open-dash:
    @open https://ibis-analytics.streamlit.app

# cicd
cicd:
    @gh workflow run cicd.yaml

# temp
temp:
    @gh workflow run temp.yaml


# start vm
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
