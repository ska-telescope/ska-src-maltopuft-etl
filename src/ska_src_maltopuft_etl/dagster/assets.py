"""Dagster assets for the MeerTRAP ETL pipeline."""

# pylint: disable=redefined-outer-name

import base64
from io import BytesIO
from pathlib import Path

import matplotlib.pyplot as plt
import polars as pl
from dagster import (
    AssetExecutionContext,
    DailyPartitionsDefinition,
    MaterializeResult,
    MetadataValue,
    asset,
)

from ska_src_maltopuft_etl.core.config import config
from ska_src_maltopuft_etl.meertrap.meertrap import extract, load, transform

daily_sb_id_partitions = DailyPartitionsDefinition(
    start_date="2023-11-12",
    end_date="2023-12-12",
)


@asset(partitions_def=daily_sb_id_partitions)
def extract_meertrap_data(
    context: AssetExecutionContext,
) -> pl.DataFrame:
    """Extract MeerTRAP observation and candidate data."""
    # pylint: disable=duplicate-code

    partition_key = context.partition_key

    output_path: Path = config.get("output_path", Path())
    output_path.mkdir(parents=True, exist_ok=True)
    output_parquet = output_path / f"{partition_key}_raw.parquet"

    try:
        raw_df = pl.read_parquet(output_parquet)
    except FileNotFoundError:
        raw_df = extract(
            root_path=config.get("data_path", "") / partition_key,
        )
        raw_df.write_parquet(output_parquet, compression="gzip")
    return raw_df


@asset(partitions_def=daily_sb_id_partitions)
def transform_meerkat_data(
    context: AssetExecutionContext,
    extract_meertrap_data: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Transform MeerTRAP observation and candidate data to MALTOPUFTDB
    schema.
    """
    partition_key = context.asset_partition_key_for_input(
        "extract_meertrap_data",
    )

    obs_df, cand_df = transform(
        df=extract_meertrap_data,
        partition_key=partition_key,
    )
    return obs_df, cand_df


@asset(partitions_def=daily_sb_id_partitions)
def plot_cand_obs_count(
    transform_meerkat_data: tuple[pl.DataFrame, pl.DataFrame],
) -> MaterializeResult:
    """Plot the number of candidates and observations on a bar chart."""
    obs_df, cand_df = transform_meerkat_data

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
def load_meerkat_data(
    transform_meerkat_data: tuple[pl.DataFrame, pl.DataFrame],
) -> None:
    """Load MeerTRAP candidate and observation data into MALTOPUFTDB."""
    obs_df, cand_df = transform_meerkat_data
    load(
        obs_df=obs_df.to_pandas(),
        cand_df=cand_df.to_pandas(),
    )
