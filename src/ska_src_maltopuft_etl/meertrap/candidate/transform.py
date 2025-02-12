"""MeerTRAP candidate data to MALTOPUFT DB transformations."""

import datetime as dt
import logging

import polars as pl
from astropy.time import Time

from ska_src_maltopuft_etl import utils
from ska_src_maltopuft_etl.core.config import config
from ska_src_maltopuft_etl.core.exceptions import UnexpectedShapeError

from .models import SPCCL_FILE_TO_DF_COLUMN_MAP

logger = logging.getLogger(__name__)


def deduplicate_candidates(cand_df: pl.DataFrame) -> pl.DataFrame:
    """Drop candidates with duplicate attributes.

    If duplicate candidates are found, then the candidate that was processed
    first is kept.

    Args:
        cand_df (pl.DataFrame): Candidate data.

    Returns:
        pl.DataFrame: Unique candidate data.

    """
    initial_cand_num = len(cand_df)
    logger.info(
        f"Removing duplicate candidates from {initial_cand_num} records",
    )
    cand_df = (
        cand_df.with_columns(
            pl.col("filename")
            .str.split("_")
            .list.get(1)
            .str.split("/")
            .list.get(0)
            .cast(pl.Int64)
            .alias("processed_at"),
        )
        .sort(by="processed_at")
        .unique(
            subset=[
                "cand.dm",
                "cand.snr",
                "cand.ra",
                "cand.dec",
                "cand.width",
                "cand.observed_at",
                "beam_id",
            ],
            maintain_order=True,
            # Records are sorted by filename and therefore the unix timestamp
            # at which the candidate was processed. Keeping the first record
            # keeps the first candidate processed, which is required to keep
            # things idempotent.
            keep="first",
        )
        .drop("filename", "processed_at")
    )

    logger.info(
        f"Successfully removed {initial_cand_num-len(cand_df)} "
        f"duplicate candidates. {len(cand_df)} records remaining",
    )
    return cand_df


def mjd_2_datetime(mjd: float) -> dt.datetime:
    """Convert an MJD value to a ISO 8601 string.

    Args:
        mjd (float): MJD value

    Returns:
        str: ISO 8061 date

    """
    t = Time([str(mjd)], format="mjd")
    return dt.datetime.strptime(t.isot[0], "%Y-%m-%dT%H:%M:%S.%f").replace(
        tzinfo=dt.timezone.utc,  # noqa: UP017
    )


def get_candidate_beam_ids(
    cand_df: pl.DataFrame,
    obs_df: pl.DataFrame,
) -> pl.DataFrame:
    """Retrieves the beam_id from observation metadata and adds it to
    candidate data.

    Args:
        cand_df (pl.DataFrame): Candidate data.
        obs_df (pl.DataFrame): Observation data.

    Raises:
        UnexpectedShapeError: If a beam_id is not found for any candidate.

    Returns:
        pl.DataFrame: Candidate data with beam_ids.

    """
    logger.info("Retrieving candidate beam ids.")
    n_cand = len(cand_df)
    cand_df = (
        cand_df.sort(by="cand.observed_at")
        .join_asof(
            obs_df.sort(by="obs.t_min"),
            by_left=["cand.beam", "cand.coherent"],
            by_right=["beam.number", "beam.coherent"],
            # cand.observed_at is recorded to millisecond precision, but
            # obs.t_min is only recorded to second precision. Therefore, we
            # round cand.observed_at to the nearest second to ensure the
            # join_asof with backwards strategy works in the case where the
            # candidate is detected in the first 500 milliseconds of an
            # observation.
            left_on=pl.col("cand.observed_at").dt.round("1s"),
            right_on="obs.t_min",
            strategy="backward",
        )
        .select(
            [
                "filename",
                "candidate_id",
                "beam_id",
                *[
                    col
                    for col in cand_df.columns
                    if col.startswith(("cand.", "sp_cand."))
                ],
            ],
        )
        .drop("cand.beam", "cand.coherent")
    )

    if len(cand_df) != n_cand or cand_df["beam_id"].null_count() > 0:
        msg = (
            "Unexpected number of candidates after join. Expected "
            f"{n_cand}, got {len(cand_df)}"
        )
        raise UnexpectedShapeError(msg)

    logger.info(f"Successfully added {n_cand} beam_ids to candidate data")
    return cand_df


def transform_candidate(
    cand_df: pl.DataFrame,
    obs_df: pl.DataFrame,
) -> pl.DataFrame:
    """Transform MeerTRAP candidates to MALTOPUFTDB candidate schema.

    Args:
        cand_df (pl.DataFrame): Raw candidate data.
        obs_df (pl.DataFrame): Transformed observatation metadata.

    Returns:
        pl.DataFrame: Unique Candidate data.

    """
    logger.info("Transforming candidate data")

    cand_df = (
        cand_df.with_row_index(name="candidate_id", offset=1)
        .with_columns(
            pl.when(pl.col("cand.beam_mode") == "C")
            .then(True)  # noqa: FBT003
            .otherwise(False)  # noqa: FBT003
            .cast(pl.Boolean)
            .alias("cand.coherent"),
            pl.col("cand.mjd")
            .map_elements(mjd_2_datetime, pl.Datetime)
            .dt.replace_time_zone("UTC")
            .alias("cand.observed_at"),
            pl.struct(["cand.ra", "cand.dec"])
            .map_elements(
                lambda row: utils.hms_to_degrees(
                    row["cand.ra"],
                    row["cand.dec"],
                ),
                pl.List(pl.Float64),
            )
            .alias("ra_dec_degrees"),
        )
        .drop("cand.mjd", "cand.beam_mode")
        .with_columns(
            pl.col("ra_dec_degrees").list.get(0).alias("cand.ra"),
            pl.col("ra_dec_degrees").list.get(1).alias("cand.dec"),
        )
        .drop("ra_dec_degrees")
    ).with_columns(
        pl.concat_str(["cand.ra", "cand.dec"], separator=",")
        .alias("cand.pos")
        .map_elements(utils.add_parenthesis, pl.String),
    )

    logger.info("Successfully transformed candidate data.")

    cand_df = get_candidate_beam_ids(cand_df=cand_df, obs_df=obs_df)
    return deduplicate_candidates(cand_df=cand_df)


def transform_sp_candidate(cand_df: pl.DataFrame) -> pl.DataFrame:
    """Transform MeerTRAP candidates to MALTOPUFTDB sp_candidate schema.

    Args:
        cand_df (pl.DataFrame): Transformed candidate data.

    Returns:
        pl.DataFrame: Transformed sp_candidate data.

    """
    return cand_df.with_row_index(
        name="sp_candidate_id",
        offset=1,
    ).with_columns(
        pl.concat_str(
            [
                pl.lit(config.remote_file_root_path),
                pl.col("sp_cand.plot_path"),
            ],
            separator="/",
        ).alias("sp_cand.plot_path"),
    )


def transform_spccl(
    cand_df: pl.DataFrame,
    obs_df: pl.DataFrame,
) -> pl.DataFrame:
    """MeerTRAP candidate transformation entrypoint."""
    cand_df = cand_df.rename(SPCCL_FILE_TO_DF_COLUMN_MAP)
    cand_df = transform_candidate(cand_df=cand_df, obs_df=obs_df)
    return transform_sp_candidate(cand_df=cand_df)
