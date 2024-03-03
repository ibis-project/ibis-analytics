# justfile

# load environment variables
set dotenv-load

# variables
module := "dag"

# aliases
alias fmt:=format
alias etl:=run
alias open:=open-dash
alias dag-open:=open-dag
alias preview:=app

# format
format:
    @ruff format .

# smoke-test
smoke-test:
    @ruff format --check .

# list justfile recipes
default:
    just --list

# setup
setup:
    @pip install --upgrade -r requirements.txt
    @pip install -e .

# eda
eda:
    @ipython -i eda.py

# ingest
ingest:
    @python {{module}}/ingest.py

# run
run:
    @dagster job execute -j all_assets -m {{module}}

# postprocess
postprocess:
    @python {{module}}/postprocess.py

# deploy
deploy:
    @python {{module}}/deploy.py

# test
test:
    @python metrics.py
    @python pages/0_github.py
    @python pages/1_pypi.py
    @python pages/2_zulip.py
    @python pages/3_docs.py
    @python pages/4_about.py

# dag
dag:
    @dagster dev -m {{module}}

# streamlit stuff
app:
    @streamlit run metrics.py

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

# docs-hack: manually upload docs data
docs-hack:
    @python docs_hack.py
