"""Meertrap ETL entrypoint."""

import logging
from pathlib import Path

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


def parse(
    directory: Path = config.get("data_path", Path()),
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Parse MeerTRAP single pulse candidate data files in nested directories.

    Args:
        directory (Path, optional): The parent directory.
        Defaults to config.get("data_path", Path()).

    Returns:
        tuple[pl.DataFrame, pl.DataFrame]: Parsed observation and candidate
        data, respectively.

    """
    # Number of candidates can be very large, so only calculate it once
    n_cand = sum(1 for x in directory.rglob("*") if x.is_dir())
    obs_df = parse_observations(directory=directory, n_file=n_cand)
    cand_df = parse_candidates(directory=directory, n_file=n_cand)
    return obs_df, cand_df


def transform(
    obs_df: pl.DataFrame,
    cand_df: pl.DataFrame,
    output_path: Path = config.get("output_path", Path()),
    partition_key: str = "",
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Transform MeerTRAP data to MALTOPUFT DB schema.

    Args:
        obs_df (pl.DataFrame): Observation metadata.
        cand_df (pl.DataFrame): Candidate data.
        output_path (Path, optional): Path to write transformed data
        to.
        partition_key (str, optional): Partition key to include in output
        files.

    Returns:
        tuple[pl.DataFrame, pl.DataFrame]: Transformed observation and
        candidate data, respectively.

    """
    num_obs = len(obs_df)
    num_cand = len(cand_df)

    if num_obs == 0 and num_cand == 0:
        # TODO: Check for all the early stop conditions here
        # TODO: Split into function
        return pl.DataFrame(), pl.DataFrame()

    if partition_key != "":
        partition_key += "_"

    obs_df = transform_observation(df=obs_df)

    # TODO: Split into a function
    # TODO: Add boolean save output setting in config
    # obs_df_parquet_path = output_path / f"{partition_key}obs_df.parquet"
    # logger.info(
    #    f"Writing transformed observation data to {obs_df_parquet_path}",
    # )
    # obs_df.write_parquet(obs_df_parquet_path)
    # logger.info(
    #    "Successfully wrote transformed observation data to "
    #    f"{obs_df_parquet_path}",
    # )

    cand_df = transform_spccl(cand_df=cand_df, obs_df=obs_df)

    # TODO: Split into function
    # cand_df_parquet_path = output_path / f"{partition_key}cand_df.parquet"
    # logger.info(f"Writing transformed cand data to {cand_df_parquet_path}")
    # cand_df.write_parquet(cand_df_parquet_path)
    # logger.info(
    #    f"Successfully wrote transformed cand data to {cand_df_parquet_path}",
    # )

    return obs_df, cand_df


def load(
    obs_df: pl.DataFrame,
    cand_df: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame] | None:
    """Load MeerTRAP data into the database."""
    # TODO: Check for all early stopping conditions
    # TODO: Split into function
    if len(obs_df) == 0 or len(cand_df) == 0:
        logger.info("No data to load into the database.")
        return None

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

        # TODO: Option to write loaded data to parquet
        return obs_df, cand_df
