"""MeerTRAP candidate data to MALTOPUFT DB transformations."""

import pandas as pd
from astropy.time import Time


def transform_spccl(df_in: pd.DataFrame) -> pd.DataFrame:
    """MeerTRAP candidate transformation entrypoint."""
    candidate_df = transform_candidate(df=df_in)
    sp_candidate_df = transform_sp_candidate(
        candidate_df=candidate_df,
    )
    return candidate_df.merge(sp_candidate_df, on="candidate_id", how="inner")


def mjd_2_datetime_str(mjd: float) -> str:
    """Convert an MJD value to a ISO 8601 string.

    Args:
        mjd (float): MJD value

    Returns:
        str: ISO 8061 date

    """
    t = Time([str(mjd)], format="mjd")
    return t.isot[0]


def transform_candidate(df: pd.DataFrame) -> pd.DataFrame:
    """Returns a dataframe whose rows contain unique Candidate model data.

    For every candidate, the input dataframe has a row for every beam prcessed
    by the host which identified the candidate. Each candidate is identified
    by one beam only. The candidate data are rows where the absolute beam
    number is equal to the candidate's beam.

    Args:
        df (pd.DataFrame): A dataframe of shape (M*N, ncols) where
            N is the number of candidates and M is the number of beams
            being processed by the subset of hosts which identified the
            candidates.

    Returns:
        pd.DataFrame: Candidate model data.

    """
    cand_df = df.loc[(df["beam"] == df["beam.number"])]
    cand_df["candidate_id"] = cand_df.index.to_numpy()
    return cand_df


def transform_sp_candidate(candidate_df: pd.DataFrame) -> pd.DataFrame:
    """Returns a dataframe whose rows contain unique SPCandidate model data.

    Args:
        candidate_df (pd.DataFrame): A dataframe whose rows contain unique
            Candidate model data.

    Returns:
        pd.DataFrame: Unique SPCandidate model data.

    """
    target_candidate_cols = ["observed_at", "plot_path", "candidate_id"]
    temp_candidate_cols = ["mjd", "plot_file"]
    temp_df = candidate_df[temp_candidate_cols]

    # Convert `mjd` to `observed_at` datetime.
    temp_df["observed_at"] = temp_df["mjd"].map(mjd_2_datetime_str)
    sp_df = temp_df.rename(columns={"plot_file": "plot_path"})

    # Add candidate id
    # ETL only processes single pulses, therefore inserted candidate_ids are
    # guaranteed to map to SPCandidate rows.
    sp_df["candidate_id"] = candidate_df["candidate_id"].to_numpy()
    sp_df["sp_candidate_id"] = sp_df.index.to_numpy()
    return sp_df[target_candidate_cols]
