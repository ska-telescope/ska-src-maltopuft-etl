"""Dagster asset configuration."""

from dagster import Definitions, load_assets_from_modules

all_assets = load_assets_from_modules([])

defs = Definitions(
    assets=all_assets,
)
