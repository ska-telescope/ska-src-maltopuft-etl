"""Meertrap ETL entrypoint."""

import logging
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

from .candidate.extract import extract_spccl
from .candidate.transform import transform_spccl
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

    if len(list(root_path.glob("*"))) == 0:
        logger.warning(
            "No candidate data found in the specified directory, skipping.",
        )
        return cand_df

    for idx, candidate_dir in enumerate(root_path.iterdir()):
        if not candidate_dir.is_dir():
            logger.warning(
                f"Unexpected file {candidate_dir} found in {root_path}",
            )
            continue

        if idx % 500 == 0:
            logger.info(f"Parsing candidate #{idx} from {candidate_dir}")
        if idx > 0 and (idx % 1000) == 0:
            try:
                cand_df = cand_df.vstack(pl.DataFrame(rows))
            except (
                pl.exceptions.SchemaError,
                pl.exceptions.ComputeError,
            ) as exc:
                # If cand_df.utc_stop or rows.utc_stop contains only nulls
                # and the other contains a legitimate datetime value, then
                # df.concat() and df.vstack() raises an exception, most
                # likely due to https://github.com/pola-rs/polars/issues/14730
                # To work around this, we set all utc_stop values to null which
                # is horrible but fine for now because we can cope with null
                # utc_stop values in the transformation step. This should
                # absolutely be removed when #14730 is fixed.
                msg = (
                    "Error concatenating dataframes, setting all utc_stop to "
                    f"null: {exc}"
                )
                logger.warning(msg)

                for row_idx, _ in enumerate(rows):
                    rows[row_idx]["utc_stop"] = None

                tmp_df = pl.DataFrame(rows)
                cand_df = cand_df.vstack(tmp_df)
            rows = []
        if idx > 0 and (idx % 5000) == 0:
            cand_df = cand_df.rechunk()

        rows.append(parse_candidate_dir(candidate_dir=candidate_dir))

    cand_df = cand_df.vstack(pl.DataFrame(rows))

    # Because utc_stop is often null the utc_stop column in the DataFrame
    # is likely to be naive timezone so we explictly set it to UTC.
    if cand_df.get_column("utc_stop").dtype == pl.Datetime:
        cand_df = cand_df.with_columns(
            pl.col("utc_stop").dt.replace_time_zone("UTC"),
        )

    logger.info("Extract routine completed successfully")
    return cand_df


def transform(
    df: pl.DataFrame,
    output_path: Path = config.get("output_path", Path()),
    partition_key: str = "",
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Transform MeerTRAP data to MALTOPUFT DB schema.

    Args:
        df (pl.DataFrame): The raw data.
        output_path (Path, optional): The path to write the transformed data
        to.
        partition_key (str, optional): The partition key for the data to
        include in the output filename.

    Returns:
        tuple[pl.DataFrame, pl.DataFrame]: The observation and candidate
        data, respectively.

    """
    if len(df) == 0:
        return pl.DataFrame(), pl.DataFrame()

    obs_df = transform_observation(df=df)

    if partition_key != "":
        partition_key += "_"

    obs_df_parquet_path = output_path / f"{partition_key}obs_df.parquet"
    logger.info(
        f"Writing transformed observation data to {obs_df_parquet_path}",
    )
    obs_df.write_parquet(obs_df_parquet_path)
    logger.info(
        "Successfully wrote transformed observation data to "
        f"{obs_df_parquet_path}",
    )

    cand_df = transform_spccl(df=obs_df)
    cand_df_parquet_path = output_path / f"{partition_key}cand_df.parquet"
    logger.info(f"Writing transformed cand data to {cand_df_parquet_path}")
    cand_df.write_parquet(cand_df_parquet_path)
    logger.info(
        f"Successfully wrote transformed cand data to {cand_df_parquet_path}",
    )

    return obs_df, cand_df


def load(
    obs_df: pl.DataFrame,
    cand_df: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame] | None:
    """Load MeerTRAP data into the database."""
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
        return obs_df, cand_df
