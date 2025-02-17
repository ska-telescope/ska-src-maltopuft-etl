"""Dagster assets for the MeerTRAP ETL pipeline."""

# pylint: disable=redefined-outer-name

import base64
from io import BytesIO

import matplotlib.pyplot as plt
import polars as pl
from ska_src_maltopuft_etl.core.config import config
from ska_src_maltopuft_etl.meertrap.meertrap import load, parse, transform

from dagster import (
    AssetExecutionContext,
    DailyPartitionsDefinition,
    MaterializeResult,
    MetadataValue,
    asset,
)

daily_sb_id_partitions = DailyPartitionsDefinition(
    start_date="2023-11-12",
    end_date="2023-12-12",
)


@asset(partitions_def=daily_sb_id_partitions)
def parse_meertrap_data(
    context: AssetExecutionContext,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Parse MeerTRAP observation and candidate data."""
    # pylint: disable=duplicate-code
    config.partition_key = context.partition_key
    return parse()


@asset(partitions_def=daily_sb_id_partitions)
def transform_meertrap_data(
    context: AssetExecutionContext,
    parse_meertrap_data: tuple[pl.DataFrame, pl.DataFrame],
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Transform MeerTRAP observation and candidate data to MALTOPUFTDB
    schema.
    """
    config.partition_key = context.asset_partition_key_for_input(
        "parse_meertrap_data",
    )

    obs_df, cand_df = parse_meertrap_data
    return transform(
        obs_df=obs_df,
        cand_df=cand_df,
    )


@asset(partitions_def=daily_sb_id_partitions)
def plot_cand_obs_count(
    transform_meertrap_data: tuple[pl.DataFrame, pl.DataFrame],
) -> MaterializeResult:
    """Plot the number of candidates and observations on a bar chart."""
    obs_df, cand_df = transform_meertrap_data

    try:
        num_obs = len(obs_df["observation_id"].unique())
    except pl.exceptions.ColumnNotFoundError:
        num_obs = 0

    num_cands = len(cand_df)

    plt.figure(figsize=(8, 8), facecolor=None)
    plt.bar(["num_obs", "num_cands"], [num_obs, num_cands])

    buffer = BytesIO()
    plt.savefig(buffer, format="png")
    image_data = base64.b64encode(buffer.getvalue())
    md_content = f"![img](data:image/png;base64,{image_data.decode()})"
    return MaterializeResult(metadata={"plot": MetadataValue.md(md_content)})


@asset(partitions_def=daily_sb_id_partitions)
def load_meertrap_data(
    context: AssetExecutionContext,
    transform_meertrap_data: tuple[pl.DataFrame, pl.DataFrame],
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Load MeerTRAP candidate and observation data into MALTOPUFTDB."""
    config.partition_key = context.asset_partition_key_for_input(
        "transform_meertrap_data",
    )
    obs_df, cand_df = transform_meertrap_data
    return load(
        obs_df=obs_df,
        cand_df=cand_df,
    )
