"""Dagster asset configuration."""

from dagster import Definitions, define_asset_job, load_assets_from_modules

from . import assets

all_assets = load_assets_from_modules([assets])
all_assets_job = define_asset_job(name="meertrap_pipeline")

defs = Definitions(
    assets=all_assets,
    jobs=[all_assets_job],
)
