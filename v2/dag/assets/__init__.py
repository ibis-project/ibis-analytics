# imports
from dagster import load_assets_from_modules

from dag.assets.extract import pypi as extract_pypi
from dag.assets.extract import github as extract_github
from dag.assets.extract import backends as extract_backends

from dag.assets.transform import pypi as transform_pypi
from dag.assets.transform import github as transform_github
from dag.assets.transform import backends as transform_backends

from dag.assets.load import pypi as load_pypi
from dag.assets.load import github as load_github
from dag.assets.load import backends as load_backends

from dag.constants import EXTRACT, TRANSFORM, LOAD

# load assets
extract_modules = [extract_pypi, extract_github, extract_backends]
extract_assets = load_assets_from_modules(extract_modules, group_name=EXTRACT)

transform_modules = [transform_pypi, transform_github, transform_backends]
transform_assets = load_assets_from_modules(transform_modules, group_name=TRANSFORM)

load_modules = [load_pypi, load_github, load_backends]
load_assets = load_assets_from_modules(load_modules, group_name=LOAD)

assets = [*extract_assets, *transform_assets, *load_assets]
