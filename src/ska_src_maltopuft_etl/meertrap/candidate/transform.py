"""MeerTRAP candidate data to MALTOPUFT DB transformations."""

import logging

import polars as pl
from astropy.time import Time

logger = logging.getLogger(__name__)


def transform_spccl(df: pl.DataFrame, beam_df: pl.DataFrame) -> pl.DataFrame:
    """MeerTRAP candidate transformation entrypoint."""
    candidate_df = transform_candidate(df=df, beam_df=beam_df)
    return transform_sp_candidate(
        candidate_df=candidate_df,
    )


def mjd_2_datetime_str(mjd: float) -> str:
    """Convert an MJD value to a ISO 8601 string.

    Args:
        mjd (float): MJD value

    Returns:
        str: ISO 8061 date

    """
    t = Time([str(mjd)], format="mjd")
    return t.isot[0]


def add_parenthesis(s: str) -> str:
    """Wrap a string in parenthesis."""
    return f"({s})"


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

    # Add cand.pos=(ra,dec) for querying with pgSphere
    cand_df = cand_df.with_columns(
        pl.concat_str(["cand.ra", "cand.dec"], separator=",")
        .alias("cand.pos")
        .map_elements(add_parenthesis, pl.String),
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
        .map_elements(mjd_2_datetime_str, pl.String)
        .alias("observed_at"),
    ).drop("mjd")
    sp_df = sp_df.with_row_index(name="sp_candidate_id", offset=1)
    return sp_df.rename(
        {
            "observed_at": "sp_cand.observed_at",
            "plot_file": "sp_cand.plot_path",
        },
    )
