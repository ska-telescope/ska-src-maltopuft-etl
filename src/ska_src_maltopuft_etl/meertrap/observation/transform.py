"""MeerTRAP observation metadata to MALTOPUFT DB transformations."""

import datetime as dt

import pandas as pd

from ska_src_maltopuft_etl.core.exceptions import UnexpectedShapeError
from ska_src_maltopuft_etl.meertrap.observation.constants import (
    SPEED_OF_LIGHT_M_PER_S,
)


def transform_observation(df: pd.DataFrame) -> pd.DataFrame:
    """MeerTRAP observation stransformation entrypoint."""
    df = df.sort_values(by=["utc_start"]).reset_index()
    out_df = pd.DataFrame(
        data={
            "candidate": df["candidate"],
            "start_at": pd.to_datetime(df["sb.actual_start_time"]),
            "t_min": pd.to_datetime(df["utc_start"]),
        },
    )

    # Schedule block
    sb_uniq_df = df.drop_duplicates(subset=["sb.id"])
    sb_df = get_sb_df(df=sb_uniq_df)
    meerkat_sb_df = get_meerkat_sb_df(df=sb_uniq_df)
    sb_df = sb_df.merge(
        meerkat_sb_df,
        left_index=True,
        right_index=True,
        validate="one_to_one",
    )
    out_df = out_df.merge(
        sb_df,
        on="start_at",
        how="inner",
        validate="many_to_one",
    )

    # Observation
    df["est_end_at"] = out_df["est_end_at"].to_numpy()
    obs_uniq_df = df.sort_values(
        by=["utc_start", "utc_stop"],
        na_position="last",
    ).drop_duplicates(
        subset=["utc_start"],
        keep="first",
    )

    coherent_beam_config_df = get_coherent_beam_config_df(df=obs_uniq_df)
    obs_df = get_obs_df(df=obs_uniq_df, sb_df=sb_df)
    obs_df = obs_df.merge(
        coherent_beam_config_df,
        on="candidate",
        validate="many_to_one",
    ).drop(
        columns="candidate",
    )
    out_df = out_df.merge(
        obs_df,
        on=["t_min"],
        how="inner",
        validate="many_to_one",
    )
    return out_df


def get_sb_df(df: pd.DataFrame) -> pd.DataFrame:
    """Returns a dataframe with unique schedule block rows."""
    sb_df = pd.DataFrame(
        data={
            "expected_duration_seconds": df["sb.expected_duration_seconds"],
            "script_profile_config": df["sb.script_profile_config"],
            "targets": df["sb.targets"],
            "start_at": df["sb.actual_start_time"],
        },
    )

    sb_df = handle_zero_durations(sb_df)

    sb_df["expected_duration_seconds"] = pd.to_timedelta(
        sb_df["expected_duration_seconds"],
        unit="s",
    )
    sb_df["est_end_at"] = (
        sb_df["start_at"] + sb_df["expected_duration_seconds"]
    )
    sb_df = sb_df[["start_at", "est_end_at"]]
    sb_df["schedule_block_id"] = sb_df.index.to_numpy()

    num_sb = df.shape[0]
    if sb_df.shape[0] != num_sb:
        msg = f"Expected {num_sb}, got {sb_df.shape[0]}"
        raise UnexpectedShapeError(msg)

    return sb_df


def handle_zero_durations(sb_df: pd.DataFrame) -> pd.DataFrame:
    """Handles rows with zero durations by extracting duration from schedule
    block configuration script.
    """
    null_duration_idx = sb_df[
        sb_df["expected_duration_seconds"] == 0
    ].index.tolist()
    if null_duration_idx:
        temp_df = sb_df.loc[null_duration_idx]
        durations = temp_df["script_profile_config"].str.extractall(
            r"duration=(?P<duration>\d+(\.\d+)?)\\n",
        )
        durations = durations.drop(
            columns=[col for col in durations.columns if col != "duration"],
        )
        sb_df.loc[null_duration_idx, "expected_duration_seconds"] = (
            durations["duration"].astype(float).groupby(level=0).sum()
        )
    return sb_df


def get_meerkat_sb_df(df: pd.DataFrame) -> pd.DataFrame:
    """Returns a dataframe with unique Meerkat schedule block rows."""
    meerkat_sb_df = pd.DataFrame(
        data={
            "meerkat_id": df["sb.id"],
            "meerkat_id_code": df["sb.id_code"],
            "proposal_id": df["sb.proposal_id"],
        },
    )

    num_sb = df.shape[0]
    if meerkat_sb_df.shape[0] != num_sb:
        msg = f"Expected {num_sb}, got {meerkat_sb_df.shape[0]}"
        raise UnexpectedShapeError(msg)
    return meerkat_sb_df


def get_coherent_beam_config_df(df: pd.DataFrame) -> pd.DataFrame:
    """Returns a dataframe with unique coherent beam configuration rows."""
    uniq_cols = ["angle", "fraction_overlap", "x", "y"]
    cb_config_df = pd.DataFrame(
        data={
            "candidate": df["candidate"].to_numpy(),
            "angle": df["beams.coherent_beam_shape.angle"],
            "fraction_overlap": df["beams.coherent_beam_shape.overlap"],
            "x": df["beams.coherent_beam_shape.x"],
            "y": df["beams.coherent_beam_shape.y"],
        },
    )
    uniq_cb_config_df = cb_config_df.drop_duplicates(subset=uniq_cols).drop(
        columns=["candidate"],
    )
    uniq_cb_config_df["coherent_beam_config_id"] = (
        uniq_cb_config_df.index.to_numpy()
    )

    return cb_config_df.merge(
        uniq_cb_config_df,
        on=uniq_cols,
        how="left",
        validate="many_to_one",
    )


def find_parent_interval(
    child_time: dt.datetime,
    parent_df: pd.DataFrame,
) -> int | None:
    """Returns the schedule_block_id where child_time is in the interval
    `start_time` <= t <= `est_end_time + 1 hour`.

    A buffer of 1 hour is added due to the `est_end_time` being an estimate.
    `est_end_time` is guaranteed to be less than (or equal to) the actual end
    time due to the additional time taken to schedule observations.
    """
    parent_row = parent_df[
        (
            (parent_df["start_at"] <= child_time)
            & (parent_df["est_end_at"] + dt.timedelta(hours=1) >= child_time)
        )
    ]
    if not parent_row.empty:
        return parent_row["schedule_block_id"].to_numpy()[0]
    return None


def get_pol_states(npol: int) -> str | None:
    """Returns a string of comma separated polarisation states for the given
    number of polarisations.

    Args:
        npol (int): The number of polarisations.

    Returns:
        str | None: Polarisation states.

    """
    # ruff: noqa: PLR2004
    if npol == 1:
        return "I"
    if npol == 4:
        return "I,Q,U,V"

    return None


def get_dataproduct_type(npol: int) -> str | None:
    """Returns the data product type for the given number of polarisations.

    Args:
        npol (int): The number of polarisations.

    Returns:
        str | None: The data product type.

    """
    # ruff: noqa: PLR2004
    if npol == 1:
        return "dynamic spectrum"
    if npol == 4:
        return "cube"
    return None


def fill_t_max(row: pd.Series) -> dt.datetime:
    """Observation t_max fill strategy.

    1. If t_max is not null, then use the given value of t_max.
    2. If t_max is null then use the minimum value of the schedule block end
        time or the start time of the next observation.
    """
    if not pd.isna(row["t_max"]):
        return row["t_max"]
    return min(row["est_end_at"], row["next_t_min"])


def handle_null_stop(df: pd.DataFrame) -> pd.DataFrame:
    """Handle null observation t_max."""
    df["next_t_min"] = df[["t_min"]].shift(periods=-1)
    df["t_max"] = df.apply(fill_t_max, axis=1)
    return df


def get_obs_df(
    df: pd.DataFrame,
    sb_df: pd.DataFrame,
) -> pd.DataFrame:
    """Returns a dataframe with unique observation rows."""
    obs_df = pd.DataFrame(
        data={
            "candidate": df["candidate"].to_numpy(),
            "est_end_at": df["est_end_at"].to_numpy(),
            "t_min": df["utc_start"].to_numpy(),
            "t_max": df["utc_stop"].to_numpy(),
            "t_resolution": df["observation.tsamp"].to_numpy(),
            "bw": df["observation.bw"],
            "cfreq": df["observation.cfreq"],
            "nbeam": df["observation.nbeam"],
            "nbit": df["observation.nbit"],
            "em_xel": df["observation.nchan"],
            "pol_xel": df["observation.npol"],
            "facility_name": "MeerTRAP",
            "instrument_name": "Meerkat",
        },
    )

    obs_df["em_min"] = (
        SPEED_OF_LIGHT_M_PER_S / (obs_df["cfreq"] + obs_df["bw"] / 2.0) * 1.0e6
    )
    obs_df["em_max"] = (
        SPEED_OF_LIGHT_M_PER_S / (obs_df["cfreq"] - obs_df["bw"] / 2.0) * 1.0e6
    )

    obs_df["dataproduct_type"] = (
        obs_df["pol_xel"].apply(get_dataproduct_type).astype(str)
    )

    obs_df["pol_states"] = obs_df["pol_xel"].apply(get_pol_states).astype(str)

    obs_df["schedule_block_id"] = (
        obs_df["t_min"]
        .apply(
            find_parent_interval,
            parent_df=sb_df,
        )
        .astype(int)
    )

    # Handle utc_stop NaT
    obs_df = handle_null_stop(df=obs_df)

    obs_df["observation_id"] = obs_df.index.to_numpy()

    if obs_df.shape[0] != df.shape[0]:
        msg = (
            "Observation dataframe has fewer rows than number of unique "
            "observation start times."
        )
        raise UnexpectedShapeError(msg)

    return obs_df[
        [
            "candidate",
            "dataproduct_type",
            "t_min",
            "t_max",
            "t_resolution",
            "em_min",
            "em_max",
            "em_xel",
            "pol_xel",
            "pol_states",
            "facility_name",
            "instrument_name",
            "observation_id",
        ]
    ]
