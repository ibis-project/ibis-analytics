from dagster import define_asset_job, AssetSelection

from dag.constants import EXTRACT, TRANSFORM, LOAD

all_job = define_asset_job("all_assets")
extract_job = define_asset_job(
    "extract_assets", selection=AssetSelection.groups(EXTRACT)
)
transform_job = define_asset_job(
    "transform_assets", selection=AssetSelection.groups(TRANSFORM)
)
load_job = define_asset_job("load_assets", selection=AssetSelection.groups(LOAD))
jobs = [all_job, extract_job, transform_job, load_job]
