# justfile

# load environment variables
set dotenv-load

# variables
package := "ibis-analytics"

# aliases
alias fmt:=format
alias render:=docs-build
alias preview:=docs-preview

# list justfile recipes
default:
    just --list

# build
build:
    just clean-dist
    @python -m build

# setup
setup:
    @uv venv
    @. .venv/bin/activate
    @uv pip install --upgrade --resolution=highest -r dev-requirements.txt

# install
install:
    @uv pip install -r dev-requirements.txt

# uninstall
uninstall:
    @pip uninstall -y {{package}}

# format
format:
    @ruff format .

# publish-test
release-test:
    just build
    @twine upload --repository testpypi dist/* -u __token__ -p ${PYPI_TEST_TOKEN}

# publish
release:
    just build
    @twine upload dist/* -u __token__ -p ${PYPI_TOKEN}

# clean dist
clean-dist:
    @rm -rf dist

# docs-build
docs-build:
    @quarto render website

# docs-preview
docs-preview:
    @quarto preview website

# open
open:
    @open https://ibis-project.github.io/ibis-analytics

# run
run:
    @gh workflow run etl.yaml

# run-docs
run-docs:
    @gh workflow run etl-docs.yaml
