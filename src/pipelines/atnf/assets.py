"""Dagster assets for the ATNF catalogue ETL pipeline."""

import polars as pl
from dagster import asset
from ska_src_maltopuft_etl.atnf.atnf import extract, load, transform


@asset
def raw_atnf_cat() -> pl.DataFrame:
    """Extract raw ATNF catalogue data."""
    return extract()


@asset
def transformed_atnf_cat(raw_atnf_cat: pl.DataFrame) -> pl.DataFrame:
    """Transform raw ATNF catalogue data to MALTOPUFTDB schema."""
    return transform(df=raw_atnf_cat)


@asset
def load_atnf_cat(transformed_atnf_cat: pl.DataFrame) -> None:
    """Load transformed ATNF catalogue data to MALTOPUFTDB."""
    return load(df=transformed_atnf_cat)
