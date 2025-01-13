"""Meertrap ETL entrypoint.

This module provides the ETL (Extract, Transform, Load) process for the
MeerTRAP project. It includes functions to parse, transform, and load
single pulse candidate data and observation metadata into the MALTOPUFT
database schema.
"""

import logging
import os
from collections.abc import Callable
from functools import partial
from pathlib import Path
from typing import Any

import polars as pl

from ska_src_maltopuft_etl.core.config import config
from ska_src_maltopuft_etl.core.database import engine
from ska_src_maltopuft_etl.database_loader import DatabaseLoader
from ska_src_maltopuft_etl.meertrap.candidate.targets import candidate_targets
from ska_src_maltopuft_etl.meertrap.observation.targets import (
    observation_targets,
)

from .candidate.extract import parse_candidates
from .candidate.transform import transform_spccl
from .observation.extract import parse_observations
from .observation.transform import transform_observation

logger = logging.getLogger(__name__)


def read_or_parse_parquet(
    file_path: Path,
    parse_func: Callable,
    **kwargs: Any,
) -> pl.DataFrame:
    """Reads a parquet file from the given path if it exists, otherwise
    reparses.

    Reparsed files are optionally written to disk if
    Config.save_output = True.

    Args:
        file_path (Path): Path to the parquet file to read or, if the file
        doesn't exist, to save the parsed output to.
        parse_func (Callable): Function to parse the file with if it doesn't
        exist.
        kwargs (Any): Key word arguments passed to parse_func.

    Returns:
        pl.DataFrame: DataFrame containing the read or parsed Parquet file
        data.

    """
    try:
        logger.info(f"Reading data from {file_path}")
        df = pl.read_parquet(file_path)
        logger.info(f"Successfully read data from {file_path}")
    except FileNotFoundError:
        logger.info(f"No parsed data found at {file_path}, recomputing.")
        df = parse_func(**kwargs)
        if config.save_output:
            df.write_parquet(file_path, compression="gzip")
            logger.info(f"Saved output to {file_path}")

    return df


def parse() -> tuple[pl.DataFrame, pl.DataFrame]:
    """Parse MeerTRAP single pulse candidate data files in nested directories.

    Returns
        tuple[pl.DataFrame, pl.DataFrame]: Parsed observation and candidate
        data, respectively.

    """
    if not config.partition_data_path.exists():
        logger.info(
            f"Directory {config.partition_data_path} does not exist, "
            "early stopping.",
        )
        return pl.DataFrame(), pl.DataFrame()

    n_cand = sum(
        1 for entry in os.scandir(config.partition_data_path) if entry.is_dir()
    )

    parsed_obs_partial = partial(
        read_or_parse_parquet,
        file_path=config.raw_obs_data_path,
        parse_func=parse_observations,
        directory=config.partition_data_path,
        n_file=n_cand,
    )

    parsed_cand_partial = partial(
        read_or_parse_parquet,
        file_path=config.raw_cand_data_path,
        parse_func=parse_candidates,
        directory=config.partition_data_path,
        n_file=n_cand,
    )

    return parsed_obs_partial(), parsed_cand_partial()


def transform(
    obs_df: pl.DataFrame,
    cand_df: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Transform MeerTRAP data to MALTOPUFT DB schema.

    Args:
        obs_df (pl.DataFrame): DataFrame containing observation metadata.
        cand_df (pl.DataFrame): DataFrame containing candidate data.

    Returns:
        tuple[pl.DataFrame, pl.DataFrame]: Transformed observation and
        candidate data, respectively.

    """
    if not (len(obs_df) > 0 and len(cand_df) > 0):
        logger.info("No parsed data to transform, early stopping.")
        return obs_df, cand_df

    transformed_obs_df_partial = partial(
        read_or_parse_parquet,
        file_path=config.transformed_obs_data_path,
        parse_func=transform_observation,
        df=obs_df,
    )

    obs_df = transformed_obs_df_partial()

    transformed_cand_df_partial = partial(
        read_or_parse_parquet,
        file_path=config.transformed_cand_data_path,
        parse_func=transform_spccl,
        cand_df=cand_df,
        obs_df=obs_df,
    )

    return obs_df, transformed_cand_df_partial()


def load(
    obs_df: pl.DataFrame,
    cand_df: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Load MeerTRAP data into the database.

    Args:
        obs_df (pl.DataFrame): DataFrame containing the transformed
        observation metadata.
        cand_df (pl.DataFrame): DataFrame containing the transformed
        candidate data.

    Returns:
        tuple[pl.DataFrame, pl.DataFrame]: DataFrames containing the loaded
        observation and candidate data, respectively.

    """
    if not (len(obs_df) > 0 and len(cand_df) > 0):
        logger.info("No transformed data to load, early stopping.")
        return obs_df, cand_df

    logger.info("Loading MeerTRAP data into the database")
    with engine.connect() as conn, conn.begin():
        db = DatabaseLoader(conn=conn)

        for target in (
            observation_targets.schedule_block,
            observation_targets.meerkat_schedule_block,
            observation_targets.host,
            observation_targets.coherent_beam_config,
            observation_targets.observation,
            observation_targets.tiling_config,
            observation_targets.beam,
        ):
            obs_df = db.load(target=target, df=obs_df)

        # Update the candidate beam ids to match the beam ids in the database
        beam_key_map = db.foreign_keys_map.get("beam_id", {})
        cand_df = cand_df.with_columns(
            pl.col("beam_id").map_elements(
                lambda x: beam_key_map.get(x, x),
                pl.Int32,
            ),
        )

        for target in (
            candidate_targets.candidate,
            candidate_targets.sp_candidate,
        ):
            cand_df = db.load(target=target, df=cand_df)

        logger.info("Data loaded successfully")

        if config.save_output:
            obs_df.write_parquet(config.inserted_obs_data_path)
            cand_df.write_parquet(config.inserted_cand_data_path)

        return obs_df, cand_df
