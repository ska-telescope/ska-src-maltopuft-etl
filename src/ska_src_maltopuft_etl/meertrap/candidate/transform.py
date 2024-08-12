"""MeerTRAP candidate data to MALTOPUFT DB transformations."""

import datetime as dt
import logging

import polars as pl
from astropy.time import Time

from ska_src_maltopuft_etl import utils

logger = logging.getLogger(__name__)


def transform_spccl(df: pl.DataFrame, beam_df: pl.DataFrame) -> pl.DataFrame:
    """MeerTRAP candidate transformation entrypoint."""
    candidate_df = transform_candidate(df=df, beam_df=beam_df)
    return transform_sp_candidate(
        candidate_df=candidate_df,
    )


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


def transform_candidate(
    df: pl.DataFrame,
    beam_df: pl.DataFrame,
) -> pl.DataFrame:
    """Returns a dataframe whose rows contain unique Candidate model data."""
    logger.info("Started transforming candidate data")
    logger.info("Joining beam and candidate data")
    cand_df = (
        df.lazy()
        .join(beam_df.lazy(), on=["candidate"], how="cross", coalesce=True)
        .unique(subset=["candidate", "beam", "beam.number"])
        .filter(pl.col("beam") == pl.col("beam.number"))
    ).collect(streaming=True)
    logger.info("Successfully joined beam and candidate data")
    cand_df = cand_df.rename(
        {
            "dm": "cand.dm",
            "snr": "cand.snr",
            "ra": "cand.ra",
            "dec": "cand.dec",
            "width": "cand.width",
        },
    )

    cand_df = cand_df.with_columns(
        pl.col("cand.ra").map_elements(utils.format_ra_hms, pl.String),
    )
    cand_df = cand_df.with_columns(
        pl.col("cand.dec").map_elements(utils.format_dec_dms, pl.String),
    )

    # Add cand.pos=(ra,dec) for querying with pgSphere
    cand_df = cand_df.with_columns(
        pl.concat_str(["cand.ra", "cand.dec"], separator=",")
        .alias("cand.pos")
        .map_elements(utils.add_parenthesis, pl.String),
    )

    cand_df = cand_df.with_row_index(name="candidate_id", offset=1)
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
    sp_df = candidate_df.with_columns(
        pl.col("mjd")
        .map_elements(mjd_2_datetime, pl.Datetime)
        .dt.replace_time_zone("UTC")
        .alias("observed_at"),
    ).drop("mjd")

    sp_df = sp_df.with_row_index(name="sp_candidate_id", offset=1)
    return sp_df.rename(
        {
            "observed_at": "sp_cand.observed_at",
            "plot_file": "sp_cand.plot_path",
        },
    )
