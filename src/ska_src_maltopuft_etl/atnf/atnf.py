"""ATNF pulsar catalogue ETL entrypoint."""

import datetime as dt
import logging

import pandas as pd
import polars as pl
from psrqpy import ATNF_BASE_URL, QueryATNF

from ska_src_maltopuft_etl import utils
from ska_src_maltopuft_etl.atnf.params import query_param_mapping
from ska_src_maltopuft_etl.atnf.targets import targets
from ska_src_maltopuft_etl.core.database import engine
from ska_src_maltopuft_etl.database_loader import DatabaseLoader

logger = logging.getLogger(__name__)


def extract() -> pl.DataFrame:
    """Extract ATNF pulsar catalogue data.

    Returns
        pl.DataFrame: DataFrame containing ATNF pulsar catalogue and
        visit data.

    """
    query = QueryATNF(params=list(query_param_mapping.keys()), version="2.3.0")
    visited_at = dt.datetime.now(tz=dt.timezone.utc)  # noqa: UP017
    df = pl.from_pandas(query.pandas)
    return (
        df.drop([col for col in df.columns if col.endswith("_ERR")])
        .rename(query_param_mapping)
        .with_row_index(name="known_pulsar_id", offset=1)
        .with_columns(
            pl.lit(visited_at).alias("cat_visit.visited_at"),
        )
    )


def transform(df: pl.DataFrame) -> pl.DataFrame:
    """Transform ATNF pulsar catalogue data into MALTOPUFTDB schema.

    Args:
        df (pl.DataFrame): Raw ATNF catalogue and visit data.

    Returns:
        pl.DataFrame: Transformed ATNF catalogue and visit data.

    """
    return (
        # pylint: disable=duplicate-code
        df.with_columns(
            [
                pl.struct(["known_ps.ra", "known_ps.dec"])
                .map_elements(
                    lambda row: utils.hms_to_degrees(
                        row["known_ps.ra"],
                        row["known_ps.dec"],
                    ),
                )
                .alias("ra_dec_degrees"),
            ],
        )
        .with_columns(
            [
                pl.col("ra_dec_degrees")
                .list.get(0)
                .cast(pl.Float64)
                .alias("known_ps.ra"),
                pl.col("ra_dec_degrees")
                .list.get(1)
                .cast(pl.Float64)
                .alias("known_ps.dec"),
            ],
        )
        .drop("ra_dec_degrees")
        .with_columns(
            [
                pl.concat_str(["known_ps.ra", "known_ps.dec"], separator=",")
                .alias("known_ps.pos")
                .map_elements(
                    utils.add_parenthesis,
                    return_dtype=pl.String,
                ),
                # Catalogue columns
                pl.lit("ATNF pulsar catalogue").alias("cat.name"),
                pl.lit(ATNF_BASE_URL).alias("cat.url"),
                pl.lit(1).alias("catalogue_id"),
                # CatalogueVisit columns
                pl.lit(1).alias("catalogue_visit_id"),
            ],
        )
    )


def load(df: pd.DataFrame) -> None:
    """Load ATNF pulsar catalogue data into the database.

    Args:
        df (pd.DataFrame): ATNF catalogue and visit data.

    """
    with engine.connect() as conn, conn.begin():
        db = DatabaseLoader(conn=conn)
        for target in targets:
            df = db.insert_target(
                df=df,
                target=target,
            )
