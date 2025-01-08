"""MeerTRAP observation metadata to MALTOPUFT DB transformations."""

import ast
import datetime as dt

import polars as pl

from ska_src_maltopuft_etl import utils
from ska_src_maltopuft_etl.core.exceptions import UnexpectedShapeError

from .constants import MHZ_TO_HZ, SPEED_OF_LIGHT_M_PER_S

RUN_SUMMARY_FILE_TO_DF_COLUMNS = {
    "filename": "filename",
    "beams.ca_target_request.beams": "beams.beams",
    "beams.ca_target_request.tilings": "beams.tilings",
    "beams.coherent_beam_shape.angle": "cb.angle",
    "beams.coherent_beam_shape.overlap": "cb.fraction_overlap",
    "beams.coherent_beam_shape.x": "cb.x",
    "beams.coherent_beam_shape.y": "cb.y",
    "beams.host_beams": "beams.host_beams",
    "observation.bw": "obs.bw",
    "observation.cfreq": "obs.cfreq",
    "observation.nbit": "obs.nbit",
    "observation.nchan": "obs.em_xel",
    "observation.npol": "obs.pol_xel",
    "observation.tsamp": "obs.t_resolution",
    "sb.id": "mk_sb.meerkat_id",
    "sb.id_code": "mk_sb.meerkat_id_code",
    "sb.actual_start_time": "sb.start_at",
    "sb.expected_duration_seconds": "sb.expected_duration_seconds",
    "sb.proposal_id": "mk_sb.proposal_id",
    "sb.script_profile_config": "sb.script_profile_config",
    "sb.targets": "sb.targets",
    "utc_start": "obs.t_min",
    "utc_stop": "obs.t_max",
}


def get_base_df(df: pl.DataFrame) -> pl.DataFrame:
    """Initialise the transformed DataFrame."""
    return df.select(
        "filename",
        "sb.start_at",
        "obs.t_min",
        "obs.t_max",
        "beams.host_beams",
    )


def transform_observation(
    df: pl.DataFrame,
) -> pl.DataFrame:
    """MeerTRAP observation transformation entrypoint."""
    df_in = df.rename(RUN_SUMMARY_FILE_TO_DF_COLUMNS).sort(
        ["obs.t_min", "obs.t_max"],
        nulls_last=True,
    )

    sb_df = get_sb_df(
        df=df_in.unique(subset=["mk_sb.meerkat_id"], keep="first"),
    )
    df = sb_df.join(
        get_base_df(df=df_in),
        on="sb.start_at",
        how="inner",
        validate="1:m",
    )

    df_in = df_in.with_columns(df["sb.est_end_at"])

    obs_uniq_df = df_in.unique(
        subset=["obs.t_min"],
        keep="first",
    )

    obs_df = get_obs_df(df=obs_uniq_df, sb_df=sb_df)
    coherent_beam_config_df = get_coherent_beam_config_df(df=obs_uniq_df)
    tiling_df = get_tiling_config_df(df=obs_uniq_df, obs_df=obs_df)

    df = (
        df.join(
            obs_df.join(
                coherent_beam_config_df,
                on=["obs.t_min"],
                how="inner",
                validate="1:1",
            ).join(
                tiling_df,
                how="left",
                on="observation_id",
                validate="m:m",
            ),
            on=["obs.t_min"],
            how="left",
            validate="m:m",
        )
        .with_columns(pl.col("obs.t_max_right").alias("obs.t_max"))
        .drop(
            "beams.host_beams_right",
            "obs.t_max_right",
            "schedule_block_id_right",
        )
    )

    beam_df = get_beam_df(df=df)
    host_df = get_host_df(df=beam_df)

    beam_df = beam_df.join(
        host_df,
        on=["host.ip_address", "host.hostname", "host.port"],
        how="left",
        validate="m:1",
    )

    df = df.join(
        beam_df,
        on=["observation_id"],
        how="full",
        validate="m:m",
    )

    for col in df.columns:
        if "_id" not in col:
            continue
        if df[col].is_null().any():
            msg = f"Merge resulted in null {col}."
            raise UnexpectedShapeError(msg)

    return df


def get_sb_df(df: pl.DataFrame) -> pl.DataFrame:
    """Returns a dataframe with unique schedule block rows."""

    def handle_zero_durations(sb_df: pl.DataFrame) -> pl.DataFrame:
        """Handles rows with zero durations by extracting duration from
        schedule block configuration script.
        """
        # Extract durations from script_profile_config
        summed_durations = sb_df.select(
            pl.col("sb.script_profile_config")
            .str.extract_all(r"duration=\d+(\.\d+)?\\n")
            .explode()
            .str.extract(r"(\d+(\.\d+)?)")
            .cast(pl.Int32)
            .sum()
            .alias("sb.duration"),
        )

        # Return summed durations if expected_duration_seconds is zero
        return sb_df.with_columns(
            pl.when(pl.col("sb.expected_duration_seconds") == 0)
            .then(summed_durations["sb.duration"])
            .otherwise(pl.col("sb.expected_duration_seconds"))
            .alias("sb.expected_duration_seconds"),
        )

    sb_df = df.select(
        "sb.expected_duration_seconds",
        "sb.script_profile_config",
        "sb.targets",
        "sb.start_at",
        "mk_sb.meerkat_id",
        "mk_sb.meerkat_id_code",
        "mk_sb.proposal_id",
    )

    sb_df = handle_zero_durations(sb_df)
    sb_df = (
        sb_df.with_columns(
            [
                (
                    pl.col("sb.start_at")
                    + pl.duration(seconds="sb.expected_duration_seconds")
                ).alias("sb.est_end_at"),
            ],
        )
        .drop(
            [
                "sb.expected_duration_seconds",
                "sb.script_profile_config",
                "sb.targets",
            ],
        )
        .with_row_index(
            name="schedule_block_id",
            offset=1,
        )
        .with_row_index(
            name="meerkat_schedule_block_id",
            offset=1,
        )
    )

    num_sb = df.shape[0]
    if sb_df.shape[0] != num_sb:
        msg = f"Expected {num_sb}, got {sb_df.shape[0]}"
        raise UnexpectedShapeError(msg)

    return sb_df


def get_coherent_beam_config_df(df: pl.DataFrame) -> pl.DataFrame:
    """Returns a dataframe with unique coherent beam configuration rows."""
    uniq_cols = [
        "obs.t_min",
        "cb.angle",
        "cb.fraction_overlap",
        "cb.x",
        "cb.y",
    ]
    cb_config_df = df.select(uniq_cols)

    uniq_cb_config_df = cb_config_df.unique(subset=uniq_cols).with_row_index(
        name="coherent_beam_config_id",
        offset=1,
    )

    return cb_config_df.join(
        uniq_cb_config_df,
        on=uniq_cols,
        how="left",
        validate="m:1",
    )


def find_parent_interval(
    child_time: dt.datetime,
    parent_df: pl.DataFrame,
) -> int | None:
    """Returns the schedule_block_id where child_time is in the interval
    `start_time` <= t <= `est_end_time + 1 hour`.

    A buffer of 1 hour is added due to the `est_end_time` being an estimate.
    `est_end_time` is guaranteed to be less than (or equal to) the actual end
    time due to the additional time taken to schedule observations.
    """
    parent_row = parent_df.filter(
        (pl.col("sb.start_at") <= child_time)
        & (pl.col("sb.est_end_at") + pl.duration(hours=1) >= child_time),
    )

    if parent_row.height > 0:
        return parent_row["schedule_block_id"][0]
    return None


def handle_null_stop(df: pl.DataFrame) -> pl.DataFrame:
    """Handle null observation t_max."""
    df = df.with_columns(
        [pl.col("obs.t_min").shift(-1).alias("obs.next_t_min")],
    )

    return df.with_columns(
        pl.when(pl.col("obs.t_max").is_not_null())
        .then(pl.col("obs.t_max"))
        .otherwise(
            pl.min_horizontal(
                pl.col("sb.est_end_at"),
                pl.col("obs.next_t_min"),
            ),
        )
        .alias("obs.t_max"),
    ).drop("obs.next_t_min")


def get_obs_df(
    df: pl.DataFrame,
    sb_df: pl.DataFrame,
) -> pl.DataFrame:
    """Returns a dataframe with unique observation rows."""

    def get_em_min() -> pl.Expr:
        return (
            SPEED_OF_LIGHT_M_PER_S
            / (pl.col("obs.cfreq") + pl.col("obs.bw") / 2.0)
            * MHZ_TO_HZ
        )

    def get_em_max() -> pl.Expr:
        return (
            SPEED_OF_LIGHT_M_PER_S
            / (pl.col("obs.cfreq") - pl.col("obs.bw") / 2.0)
            * MHZ_TO_HZ
        )

    def get_pol_states(npol: int) -> str | None:
        """Returns a string of comma separated polarisation states for the
        given number of polarisations.

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

    obs_df = (
        df.select(
            "sb.est_end_at",
            "beams.host_beams",
            *[col for col in df.columns if col.startswith("obs.")],
        )
        .with_columns(
            pl.lit("MeerTRAP").alias("obs.facility_name"),
            pl.lit("Meerkat").alias("obs.instrument_name"),
            get_em_min().alias("obs.em_min"),
            get_em_max().alias("obs.em_max"),
            pl.col("obs.pol_xel")
            .map_elements(get_dataproduct_type, pl.Utf8)
            .alias("obs.dataproduct_type"),
            pl.col("obs.pol_xel")
            .map_elements(get_pol_states, pl.Utf8)
            .alias("obs.pol_states"),
            pl.col("obs.t_min")
            .map_elements(
                lambda x: find_parent_interval(x, sb_df),
                pl.Int64,
            )
            .alias("schedule_block_id"),
        )
        .drop(
            "obs.bw",
            "obs.cfreq",
            "obs.nbit",
        )
        .with_row_index(
            name="observation_id",
            offset=1,
        )
    )

    # Handle utc_stop NaT
    obs_df = handle_null_stop(df=obs_df).drop("sb.est_end_at")

    if obs_df.shape[0] != df.shape[0]:
        msg = (
            "Observation dataframe has fewer rows than number of unique "
            "observation start times."
        )
        raise UnexpectedShapeError(msg)

    return obs_df


def get_tiling_config_df(
    df: pl.DataFrame,
    obs_df: pl.DataFrame,
) -> pl.DataFrame:
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
    tiling_df = (
        df.select(
            "beams.tilings",
        )
        .with_columns(
            obs_df.get_column("observation_id"),
            pl.col("beams.tilings").map_elements(
                ast.literal_eval,
                return_dtype=pl.List(pl.Object),
            ),
        )
        .explode("beams.tilings")
        .with_row_index()
    )

    tiling_df = (
        tiling_df.join(
            pl.json_normalize(
                tiling_df.get_column(
                    #  type: ignore[arg-type]
                    "beams.tilings",
                ),
            ).with_row_index(),
            on="index",
            how="outer",
            validate="1:1",
            coalesce=True,
        )
        .drop(["beams.tilings", "index"])
        .with_columns(
            (pl.col("reference_frequency") / MHZ_TO_HZ),
            pl.col("target").str.split(","),
        )
        .with_columns(
            pl.col("target").list.get(0).alias("tiling.target"),
            pl.col("target").list.get(2).alias("tiling.ra"),
            pl.col("target").list.get(3).alias("tiling.dec"),
        )
        .drop("target")
        .with_columns(
            pl.struct(["tiling.ra", "tiling.dec"])
            .map_elements(
                lambda row: utils.hms_to_degrees(
                    row["tiling.ra"],
                    row["tiling.dec"],
                ),
                pl.List(pl.Float64),
            )
            .alias("ra_dec_degrees"),
        )
        .with_columns(
            pl.col("ra_dec_degrees").list.get(0).alias("tiling.ra"),
            pl.col("ra_dec_degrees").list.get(1).alias("tiling.dec"),
        )
        .drop("ra_dec_degrees")
        .with_columns(
            pl.col("tiling.ra").alias("obs.s_ra"),
            pl.col("tiling.dec").alias("obs.s_dec"),
        )
        .with_row_index(name="tiling_config_id", offset=1)
    )

    return tiling_df.rename(
        {
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


def get_beam_df(df: pl.DataFrame) -> pl.DataFrame:
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
    beam_df = (
        df.select(
            pl.col("filename"),
            pl.col("beams.host_beams")
            .map_elements(ast.literal_eval)
            .alias("beams"),
            pl.col("observation_id"),
        )
        .with_columns(
            pl.col("filename")
            .str.extract(r"(?P<hostname>tpn-\d+-\d+)")
            .alias("host.hostname"),
        )
        .explode("beams")
        .unnest("beams")
    ).rename(
        {
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

    return (
        beam_df.with_columns(
            pl.struct(["beam.ra", "beam.dec"])
            .map_elements(
                lambda row: utils.hms_to_degrees(
                    row["beam.ra"],
                    row["beam.dec"],
                ),
                pl.List(pl.Float64),
            )
            .alias("ra_dec_degrees"),
        )
        .with_columns(
            pl.col("ra_dec_degrees").list.get(0).alias("beam.ra"),
            pl.col("ra_dec_degrees").list.get(1).alias("beam.dec"),
        )
        .drop("ra_dec_degrees")
        .unique(
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
        .drop(["filename", "beam.relnum", "beam.source"])
        .with_row_index(name="beam_id", offset=1)
    )


def get_host_df(df: pl.DataFrame) -> pl.DataFrame:
    """Get unique host rows."""
    return (
        df.unique(
            subset=["host.ip_address", "host.hostname", "host.port"],
        )
        .select(["host.ip_address", "host.hostname", "host.port"])
        .with_row_index(name="host_id", offset=1)
    )
