"""MeerTRAP observation metadata to MALTOPUFT DB transformations."""

import ast
import datetime as dt

import pandas as pd
import polars as pl

from ska_src_maltopuft_etl import utils
from ska_src_maltopuft_etl.core.exceptions import UnexpectedShapeError
from ska_src_maltopuft_etl.meertrap.candidate.extract import SPCCL_COLUMNS

from .constants import MHZ_TO_HZ, SPEED_OF_LIGHT_M_PER_S


def get_base_df(df: pd.DataFrame) -> pd.DataFrame:
    """Initialise the transformed DataFrame."""
    base_df = df[["candidate", *SPCCL_COLUMNS]]
    base_df["sb.start_at"] = pd.to_datetime(df["sb.actual_start_time"])
    base_df["obs.t_min"] = pd.to_datetime(df["utc_start"])
    return base_df


def transform_observation(
    df: pl.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """MeerTRAP observation transformation entrypoint."""
    # Sort raw data by ascending observation time
    raw_df = df.to_pandas().sort_values(by=["utc_start"]).reset_index()
    out_df = get_base_df(df=raw_df)

    # Schedule block
    sb_uniq_df = raw_df.drop_duplicates(subset=["sb.id"])
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
        on="sb.start_at",
        how="inner",
        validate="many_to_one",
    )

    # Observation
    raw_df["sb.est_end_at"] = out_df["sb.est_end_at"].to_numpy()
    obs_uniq_df = raw_df.sort_values(
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
        on=["obs.t_min"],
        how="inner",
        validate="many_to_one",
    )

    tiling_df = get_tiling_config_df(df=obs_uniq_df, obs_df=obs_df)
    out_df = tiling_df.merge(
        out_df,
        on="observation_id",
        how="inner",
        validate="many_to_many",
    )

    beam_df = get_beam_df(df=raw_df, obs_df=obs_df)
    host_df = beam_df.drop_duplicates(
        subset=["host.ip_address", "host.hostname", "host.port"],
    )
    host_df["host_id"] = host_df.index.to_numpy()
    beam_df = beam_df.merge(
        host_df[["host_id", "host.ip_address", "host.hostname"]],
        on=["host.ip_address", "host.hostname"],
        how="left",
    )
    if beam_df["host_id"].isna().to_numpy().any():
        msg = "Merge resulted in null host_id."
        raise UnexpectedShapeError(msg)

    out_df = out_df.merge(
        beam_df,
        on=["candidate", "observation_id"],
        how="left",
        validate="many_to_many",
    )

    return out_df, beam_df


def get_sb_df(df: pd.DataFrame) -> pd.DataFrame:
    """Returns a dataframe with unique schedule block rows."""
    sb_df = pd.DataFrame(
        data={
            "expected_duration_seconds": df["sb.expected_duration_seconds"],
            "script_profile_config": df["sb.script_profile_config"],
            "targets": df["sb.targets"],
            "sb.start_at": df["sb.actual_start_time"],
        },
    )

    sb_df = handle_zero_durations(sb_df)

    sb_df["expected_duration_seconds"] = pd.to_timedelta(
        sb_df["expected_duration_seconds"],
        unit="s",
    )
    sb_df["sb.est_end_at"] = (
        sb_df["sb.start_at"] + sb_df["expected_duration_seconds"]
    )
    sb_df = sb_df[["sb.start_at", "sb.est_end_at"]]
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
            "meerkat_schedule_block_id": df.index.to_numpy(),
            "mk_sb.meerkat_id": df["sb.id"],
            "mk_sb.meerkat_id_code": df["sb.id_code"],
            "mk_sb.proposal_id": df["sb.proposal_id"],
        },
    )

    num_sb = df.shape[0]
    if meerkat_sb_df.shape[0] != num_sb:
        msg = f"Expected {num_sb}, got {meerkat_sb_df.shape[0]}"
        raise UnexpectedShapeError(msg)
    return meerkat_sb_df


def get_coherent_beam_config_df(df: pd.DataFrame) -> pd.DataFrame:
    """Returns a dataframe with unique coherent beam configuration rows."""
    uniq_cols = ["cb.angle", "cb.fraction_overlap", "cb.x", "cb.y"]
    cb_config_df = pd.DataFrame(
        data={
            "candidate": df["candidate"].to_numpy(),
            "cb.angle": df["beams.coherent_beam_shape.angle"],
            "cb.fraction_overlap": df["beams.coherent_beam_shape.overlap"],
            "cb.x": df["beams.coherent_beam_shape.x"],
            "cb.y": df["beams.coherent_beam_shape.y"],
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
            (parent_df["sb.start_at"] <= child_time)
            & (
                parent_df["sb.est_end_at"] + dt.timedelta(hours=1)
                >= child_time
            )
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
    if not pd.isna(row["obs.t_max"]):
        return row["obs.t_max"]
    return min(row["sb.est_end_at"], row["next_t_min"])


def handle_null_stop(df: pd.DataFrame) -> pd.DataFrame:
    """Handle null observation t_max."""
    df["next_t_min"] = df[["obs.t_min"]].shift(periods=-1)
    df["obs.t_max"] = df.apply(fill_t_max, axis=1)
    return df


def get_obs_df(
    df: pd.DataFrame,
    sb_df: pd.DataFrame,
) -> pd.DataFrame:
    """Returns a dataframe with unique observation rows."""
    obs_df = pd.DataFrame(
        data={
            "candidate": df["candidate"].to_numpy(),
            "sb.est_end_at": df["sb.est_end_at"].to_numpy(),
            "obs.t_min": df["utc_start"].to_numpy(),
            "obs.t_max": df["utc_stop"].to_numpy(),
            "obs.t_resolution": df["observation.tsamp"].to_numpy(),
            "obs.bw": df["observation.bw"],
            "obs.cfreq": df["observation.cfreq"],
            "obs.nbeam": df["observation.nbeam"],
            "obs.nbit": df["observation.nbit"],
            "obs.em_xel": df["observation.nchan"],
            "obs.pol_xel": df["observation.npol"],
            "obs.facility_name": "MeerTRAP",
            "obs.instrument_name": "Meerkat",
        },
    )

    obs_df["obs.em_min"] = (
        SPEED_OF_LIGHT_M_PER_S
        / (obs_df["obs.cfreq"] + obs_df["obs.bw"] / 2.0)
        * MHZ_TO_HZ
    )
    obs_df["obs.em_max"] = (
        SPEED_OF_LIGHT_M_PER_S
        / (obs_df["obs.cfreq"] - obs_df["obs.bw"] / 2.0)
        * MHZ_TO_HZ
    )

    obs_df["obs.dataproduct_type"] = (
        obs_df["obs.pol_xel"].apply(get_dataproduct_type).astype(str)
    )

    obs_df["obs.pol_states"] = (
        obs_df["obs.pol_xel"].apply(get_pol_states).astype(str)
    )

    obs_df["schedule_block_id"] = (
        obs_df["obs.t_min"]
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
            "obs.dataproduct_type",
            "obs.t_min",
            "obs.t_max",
            "obs.t_resolution",
            "obs.em_min",
            "obs.em_max",
            "obs.em_xel",
            "obs.pol_xel",
            "obs.pol_states",
            "obs.facility_name",
            "obs.instrument_name",
            "observation_id",
        ]
    ]


def get_tiling_config_df(
    df: pd.DataFrame,
    obs_df: pd.DataFrame,
) -> pd.DataFrame:
    """Returns a dataframe with unique tiling configuration rows.

    Note that `beams.ca_target_request.tilings` is serialized as a string.
    This value is evaluated to list[str] with ast.literal_eval.

    :param df: Pandas DataFrame containing unique observation data which
        includes a `beams.ca_target_request.tilings` column as a string.
    :param obs_df: Pandas DataFrame containing unique observation data,
        including the `observation_id`.

    :return: Pandas DataFrame with expanded tiling configurations and
        normalized target columns.
    """
    tiling_df = pd.DataFrame(
        data={
            "observation_id": obs_df["observation_id"].to_numpy(),
            "tilings": df["beams.ca_target_request.tilings"].apply(
                ast.literal_eval,
            ),
        },
    )

    # Expand tilings list into rows
    tiling_df = tiling_df.explode("tilings")

    # Normalize tilings dict into columns
    normalized_df = pd.json_normalize(tiling_df["tilings"].to_list())
    normalized_df = normalized_df.set_index(tiling_df.index.values)
    tiling_df = tiling_df.join(normalized_df, how="outer").drop(
        columns=["tilings"],
    )

    # Convert reference frequency to MHz
    tiling_df["reference_frequency"] = (
        tiling_df["reference_frequency"].to_numpy() / MHZ_TO_HZ
    )

    # Normalise the target column
    targets = (
        tiling_df["target"]
        .str.split(",", expand=True)
        .rename(
            columns={
                0: "tiling.target",
                1: "mode",
                2: "tiling.ra",
                3: "tiling.dec",
            },
        )
        .drop(columns=["mode"])
    )

    targets["tiling.ra"] = targets["tiling.ra"].apply(utils.format_ra_hms)
    targets["tiling.dec"] = targets["tiling.dec"].apply(
        utils.format_dec_dms,
    )

    tiling_df["tiling_config_id"] = tiling_df.index.to_numpy()
    tiling_df = tiling_df.join(targets, how="outer")
    return tiling_df.rename(
        columns={
            "coordinate_type": "tiling.coordinate_type",
            "epoch": "tiling.epoch",
            "epoch_offset": "tiling.epoch_offset",
            "method": "tiling.method",
            "nbeams": "tiling.nbeams",
            "overlap": "tiling.overlap",
            "reference_frequency": "tiling.reference_frequency",
            "shape": "tiling.shape",
        },
    )


def get_beam_df(df: pd.DataFrame, obs_df: pd.DataFrame) -> pd.DataFrame:
    """Returns a dataframe with unique observation beam rows.

    Note that `beams.host_beams` is serialized as a string.
    This value is evaluated to list[str] with ast.literal_eval.


    :param df: Pandas DataFrame containing the main data with columns such as
        `candidate`, `filename`, `utc_start`, and `beams.host_beams` as a
        string column.
    :param obs_df: Pandas DataFrame containing observation data with columns
        `t_min` and `observation_id`.

    :returns: Pandas DataFrame with expanded beam configurations and extracted
        hostnames.
    """
    merged_df = df.merge(
        obs_df,
        left_on="utc_start",
        right_on="obs.t_min",
        how="left",
    )

    if merged_df["observation_id"].isna().to_numpy().any():
        msg = "Merge resulted in null `observation_id` values"
        raise UnexpectedShapeError(msg)
    if merged_df.shape[0] != df.shape[0]:
        msg = "Merge resulted in unexpected row count"
        raise UnexpectedShapeError(msg)

    # Create initial beam DataFrame
    beam_df = pd.DataFrame(
        {
            "candidate": merged_df["candidate"],
            "filename": merged_df["filename"],
            "beams": merged_df["beams.host_beams"].apply(ast.literal_eval),
            "observation_id": merged_df["observation_id"],
            "utc_start": merged_df["utc_start"],
        },
    )

    # Extract hostname from filename
    beam_df["host.hostname"] = beam_df["filename"].str.extract(
        r"(?P<hostname>tpn-\d+-\d+)",
    )

    # Expand beams list into rows
    beam_df = beam_df.explode("beams").reset_index()

    # Normalize beams dict into columns
    normalized_df = pd.json_normalize(beam_df["beams"].to_list())

    beam_df = beam_df.merge(normalized_df, left_index=True, right_index=True)
    beam_df = beam_df.drop(columns=["beams"])

    beam_df = beam_df.rename(
        columns={
            "absnum": "beam.number",
            "coherent": "beam.coherent",
            "dec_dms": "beam.dec",
            "mc_ip": "host.ip_address",
            "mc_port": "host.port",
            "ra_hms": "beam.ra",
            "relnum": "beam.relnum",
            "source": "beam.source",
        },
    )

    beam_df["beam.ra"] = beam_df["beam.ra"].apply(utils.format_ra_hms)
    beam_df["beam.dec"] = beam_df["beam.dec"].apply(utils.format_dec_dms)

    beam_df = beam_df.drop_duplicates(
        subset=[
            "beam.number",
            "beam.coherent",
            "beam.dec",
            "host.ip_address",
            "host.port",
            "beam.ra",
            "beam.relnum",
            "beam.source",
            "observation_id",
        ],
    )
    beam_df["beam_id"] = beam_df.index.to_numpy()
    return beam_df
