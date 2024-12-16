"""MeerTRAP candidate data to MALTOPUFT DB transformations."""

import datetime as dt
import logging

import polars as pl
from astropy.time import Time

from ska_src_maltopuft_etl import utils
from ska_src_maltopuft_etl.core.exceptions import UnexpectedShapeError

logger = logging.getLogger(__name__)


def transform_spccl(df: pl.DataFrame) -> pl.DataFrame:
    """MeerTRAP candidate transformation entrypoint."""
    candidate_df = transform_candidate(df=df)
    candidate_df = transform_sp_candidate(candidate_df=candidate_df)

    # Deduplicate candidates
    initial_cand_num = len(candidate_df)
    logger.info(
        f"Removing duplicate candidates from {initial_cand_num} records",
    )
    candidate_df = candidate_df.sort(by="candidate").unique(
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
        # Records are sorted by unix timestamp of candidate detection
        keep="first",
    )

    logger.info(
        f"Successfully removed {initial_cand_num-len(candidate_df)} "
        f"duplicate candidates. {len(candidate_df)} records remaining",
    )

    return candidate_df


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


def transform_candidate(df: pl.DataFrame) -> pl.DataFrame:
    """Returns a dataframe whose rows contain unique Candidate model data.

    Args:
        df (pl.DataFrame): Raw, untransformed DataFrame.
        obs_df (pl.DataFrame): Transformed observatation metadata DataFrame.

    Returns:
        pl.DataFrame: Unique Candidate data.

    """
    logger.info("Joining beam and candidate data")
    n_cand = df.select("candidate").n_unique()

    cand_df = (
        df.select(
            "candidate",
            "beam.number",
            "beam_id",
            *[
                col
                for col in df.columns
                if col.startswith(("cand.", "sp_cand."))
            ],
        )
        .unique(subset=["candidate", "cand.beam", "beam.number"])
        .filter(pl.col("cand.beam") == pl.col("beam.number"))
        .drop("beam.number", "cand.beam")
    )

    if len(cand_df) != n_cand:
        msg = (
            "Unexpected number of candidates after join. Expected "
            f"{n_cand}, got {len(cand_df)}"
        )
        raise UnexpectedShapeError(msg)

    logger.info(
        "Successfully joined beam and candidate data for "
        f"{n_cand} candidates",
    )
    logger.info("Transforming candidate data")

    cand_df = (
        cand_df.with_row_index(name="candidate_id", offset=1)
        .with_columns(
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
        .drop("cand.mjd")
        .with_columns(
            [
                pl.col("ra_dec_degrees").list.get(0).alias("cand.ra"),
                pl.col("ra_dec_degrees").list.get(1).alias("cand.dec"),
            ],
        )
        .drop("ra_dec_degrees")
    ).with_columns(
        pl.concat_str(["cand.ra", "cand.dec"], separator=",")
        .alias("cand.pos")
        .map_elements(utils.add_parenthesis, pl.String),
    )

    logger.info("Successfully transformed candidate data.")
    return cand_df


def transform_sp_candidate(candidate_df: pl.DataFrame) -> pl.DataFrame:
    """Returns a dataframe whose rows contain unique SPCandidate model data.

    Args:
        candidate_df (pl.DataFrame): A dataframe whose rows contain unique
            Candidate model data.

    Returns:
        pl.DataFrame: Unique SPCandidate model data.

    """
    return candidate_df.with_row_index(name="sp_candidate_id", offset=1)
