"""Meertrap ETL entrypoint."""

import logging
from pathlib import Path
from typing import Any

import pandas as pd
import polars as pl

from ska_src_maltopuft_etl.core.config import config
from ska_src_maltopuft_etl.core.database import engine
from ska_src_maltopuft_etl.core.exceptions import DuplicateInsertError
from ska_src_maltopuft_etl.database_loader import DatabaseLoader
from ska_src_maltopuft_etl.meertrap.candidate.targets import candidate_targets
from ska_src_maltopuft_etl.meertrap.observation.targets import (
    observation_targets,
)
from ska_src_maltopuft_etl.target_handler.target_handler import (
    TargetDataFrameHandler,
)

from .candidate.extract import extract_spccl
from .candidate.transform import transform_spccl
from .load import bulk_insert_schedule_block, insert_observation_by_row
from .observation.extract import extract_observation
from .observation.transform import transform_observation

logger = logging.getLogger(__name__)


def parse_candidate_dir(candidate_dir: Path) -> dict[str, Any]:
    """Parse files in a candidate directory.

    :param candidate_dir: The absolute path to the candidate directory to
        parse.

    :return: A dictionary containing the normalised candidate data.
    """
    run_summary_data, candidate_data = {}, {}
    for file in candidate_dir.iterdir():
        if file.match("*run_summary.json"):
            logger.debug(f"Parsing observation metadata from {file}")
            run_summary_data = extract_observation(filename=file)
            continue
        if file.match("*spccl.log"):
            logger.debug(f"Parsing candidate data from {file}")
            candidate_data = extract_spccl(filename=file)
            continue
        if file.match("*.jpg"):
            continue

        logger.warning(f"Found file {file} in unexpected format.")
    return {**run_summary_data, **candidate_data}


def extract(
    root_path: Path = config.get("data_path", ""),
) -> pl.DataFrame:
    """Extract MeerTRAP data archive from run_summary.json and spccl files.

    :param root_path: The absolute path to the candidate data directory.
    """
    logger.info("Started extract routine")

    cand_df = pl.DataFrame()
    rows: list[dict[str, Any]] = []

    for idx, candidate_dir in enumerate(root_path.iterdir()):
        if idx % 500 == 0:
            logger.info(f"Parsing candidate #{idx} from {candidate_dir}")
        if idx > 0 and (idx % 1000) == 0:
            cand_df = cand_df.vstack(pl.DataFrame(rows))
            rows = []
        if idx > 0 and (idx % 5000) == 0:
            cand_df = cand_df.rechunk()

        if not candidate_dir.is_dir():
            logger.warning(
                f"Unexpected file {candidate_dir} found in "
                f"{config.get('data_path')}",
            )
            continue

        rows.append(parse_candidate_dir(candidate_dir=candidate_dir))

    cand_df = cand_df.vstack(pl.DataFrame(rows))
    logger.info("Extract routine completed successfully")
    return cand_df


def transform(
    df: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Transform MeerTRAP data to MALTOPUFT DB schema.

    Args:
        df (pl.DataFrame): The raw data.

    Returns:
        tuple[pl.DataFrame, pl.DataFrame]: The observation and candidate
        data, respectively.

    """
    obs_pd = transform_observation(df=df)
    obs_df: pl.DataFrame = pl.from_pandas(obs_pd)

    output_path: Path = config.get("output_path", Path())

    obs_df_parquet_path = output_path / "obs_df.parquet"
    logger.info(
        f"Writing transformed observation data to {obs_df_parquet_path}",
    )
    obs_df.write_parquet(obs_df_parquet_path)
    logger.info(
        "Successfully wrote transformed observation data to "
        f"{obs_df_parquet_path}",
    )

    cand_df = transform_spccl(df=df, obs_df=obs_df)
    cand_df_parquet_path = output_path / "cand_df.parquet"
    logger.info(f"Writing transformed cand data to {cand_df_parquet_path}")
    cand_df.write_parquet(cand_df_parquet_path)
    logger.info(
        f"Successfully wrote transformed cand data to {cand_df_parquet_path}",
    )

    return obs_df, cand_df


def load(
    obs_df: pd.DataFrame,
    cand_df: pd.DataFrame,
) -> None:
    """Load MeerTRAP data into a database.

    Args:
        obs_df (pd.DataFrame): DataFrame containing observation data.
        cand_df (pd.DataFrame): DataFrame containing candidate data.

    Raises:
        RuntimeError: If there is an unrecoverable error while inserting data.

    """
    try:
        with engine.connect() as conn, conn.begin():
            db = DatabaseLoader(conn=conn)
            bulk_insert_schedule_block(
                db=db,
                obs_df=obs_df,
                cand_df=cand_df,
            )
    except DuplicateInsertError as exc:
        logger.warning(
            "Failed to insert data, falling back to inserting observation "
            f"rows. {exc}",
        )

        obs_handler = TargetDataFrameHandler(
            df=obs_df,
            targets=observation_targets,
        )
        cand_handler = TargetDataFrameHandler(
            df=cand_df,
            targets=candidate_targets,
        )

        unique_observations = obs_handler.unique_rows(
            target=observation_targets[3],
        )

        with engine.connect() as conn:
            for obs_idx in unique_observations.index:
                obs_id = unique_observations.loc[obs_idx]["observation_id"]

                with conn.begin():
                    db = DatabaseLoader(conn=conn)
                    insert_observation_by_row(
                        obs_handler,
                        cand_handler,
                        obs_id,
                        db,
                    )
