"""Meertrap ETL entrypoint."""

import logging
from pathlib import Path, PosixPath
from typing import Any

import pandas as pd
import polars as pl

from ska_src_maltopuft_etl.core.config import config
from ska_src_maltopuft_etl.core.database import engine
from ska_src_maltopuft_etl.database_loader import DatabaseLoader
from ska_src_maltopuft_etl.meertrap.candidate.targets import candidate_targets
from ska_src_maltopuft_etl.meertrap.observation.targets import (
    observation_targets,
)

from .candidate.extract import extract_spccl
from .candidate.transform import transform_spccl
from .observation.extract import extract_observation
from .observation.transform import transform_observation

logger = logging.getLogger(__name__)


def parse_candidate_dir(candidate_dir: PosixPath) -> dict[str, Any]:
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
    root_path: PosixPath = config.get("data_path", ""),
) -> pl.DataFrame:
    """Extract MeerTRAP data.

    :param root_path: The absolute path to the candidate data directory.
    """
    logger.info("Started extract routine")

    cand_df = pl.DataFrame()
    rows: list[dict[str, Any]] = []

    for idx, candidate_dir in enumerate(root_path.iterdir()):
        if idx % 500 == 0:
            logger.info(f"Parsing candidate #{idx} from {candidate_dir}")
        if idx > 0 and (idx % 1000) == 0:
            cand_df = cand_df.vstack(pl.DataFrame(rows, orient="row"))
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

    cand_df = cand_df.vstack(pl.DataFrame(rows, orient="row"))
    logger.info("Extract routine completed successfully")
    return cand_df


def transform(
    df: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Transform MeerTRAP data to MALTOPUFT DB schema."""
    obs_pd = transform_observation(df=df)
    obs_df: pl.DataFrame = pl.from_pandas(obs_pd)

    output_path: PosixPath = config.get("output_path", Path())

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
    """Load MeerTRAP data into a database."""
    with engine.connect() as conn, conn.begin():
        db = DatabaseLoader(conn=conn)
        for target in observation_targets:
            obs_df = db.load_target(
                df=obs_df,
                target=target,
            )

        beam_id_map = db.foreign_keys_map["beam_id"]
        cand_df["beam_id"] = cand_df["beam_id"].map(beam_id_map)

        for target in candidate_targets:
            cand_df = db.load_target(df=cand_df, target=target)
