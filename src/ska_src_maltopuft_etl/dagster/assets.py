"""Dagster assets for the MeerTRAP ETL pipeline."""

# pylint: disable=redefined-outer-name

import base64
import logging
from io import BytesIO
from logging import Logger
from pathlib import Path, PosixPath

import matplotlib.pyplot as plt
import polars as pl
from dagster import MaterializeResult, MetadataValue, asset
from ska_ser_logging import configure_logging

from ska_src_maltopuft_etl.core.config import config
from ska_src_maltopuft_etl.meertrap.meertrap import extract, load, transform


@asset
def logger() -> Logger:
    """Configure the logger."""
    configure_logging(logging.INFO)
    return logging.getLogger(__name__)


@asset
def extract_meertrap_data(logger: Logger) -> pl.DataFrame:
    """Extract MeerTRAP observation and candidate data."""
    # pylint: disable=duplicate-code
    output_path: PosixPath = config.get("output_path", Path())
    output_parquet = output_path / "candidates.parquet"
    try:
        raw_df = pl.read_parquet(output_parquet)
    except FileNotFoundError:
        raw_df = extract()
        logger.info(f"Writing parsed data to {output_parquet}")
        raw_df.write_parquet(output_parquet, compression="gzip")
        logger.info(f"Parsed data written to {output_parquet} successfully")
    return raw_df


@asset
def transform_meerkat_data(
    extract_meertrap_data: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Transform MeerTRAP observation and candidate data to MALTOPUFTDB
    schema.
    """
    obs_df, cand_df = transform(df=extract_meertrap_data)
    return obs_df, cand_df


@asset
def plot_cand_obs_count(
    transform_meerkat_data: tuple[pl.DataFrame, pl.DataFrame],
) -> MaterializeResult:
    """Plot the number of candidates and observations on a bar chart."""
    obs_df, cand_df = transform_meerkat_data
    num_obs = len(obs_df["observation_id"].unique())
    num_cands = len(cand_df)

    plt.figure(figsize=(8, 8), facecolor=None)
    plt.bar(["num_obs", "num_cands"], [num_obs, num_cands])

    buffer = BytesIO()
    plt.savefig(buffer, format="png")
    image_data = base64.b64encode(buffer.getvalue())
    md_content = f"![img](data:image/png;base64,{image_data.decode()})"
    return MaterializeResult(metadata={"plot": MetadataValue.md(md_content)})


@asset
def load_meerkat_data(
    transform_meerkat_data: tuple[pl.DataFrame, pl.DataFrame],
) -> None:
    """Load MeerTRAP candidate and observation data into MALTOPUFTDB."""
    obs_df, cand_df = transform_meerkat_data
    load(
        obs_df=obs_df.to_pandas(),
        cand_df=cand_df.to_pandas(),
    )
