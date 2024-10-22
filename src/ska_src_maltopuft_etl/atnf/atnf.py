"""ATNF pulsar catalogue ETL entrypoint."""

import datetime as dt
import logging

import polars as pl
from psrqpy import ATNF_BASE_URL, QueryATNF

from ska_src_maltopuft_etl import utils
from ska_src_maltopuft_etl.atnf.params import query_param_mapping
from ska_src_maltopuft_etl.atnf.targets import targets
from ska_src_maltopuft_etl.core.database import engine
from ska_src_maltopuft_etl.database_loader import DatabaseLoader

logger = logging.getLogger(__name__)


def trim_ra_dec_str(coord: str, length: int = 11) -> str:
    """Trims a string to the specified length."""
    if len(coord) > length:
        return coord[:length]
    return coord


def main() -> None:
    """ETL routine for ATNF pulsar catalogue to MALTOPUFT DB."""
    query = QueryATNF(params=list(query_param_mapping.keys()), version="2.3.0")
    visited_at = dt.datetime.now(tz=dt.timezone.utc)  # noqa: UP017

    df = pl.from_pandas(query.pandas)
    df = df.drop([col for col in df.columns if col.endswith("_ERR")])
    df = df.rename(query_param_mapping)
    df = df.with_row_index(name="known_pulsar_id", offset=1)

    # Trim ra and dec strings to ensure they meet
    # database column length constraints
    df = df.with_columns(
        pl.col("known_ps.ra").map_elements(trim_ra_dec_str, pl.String),
        pl.col("known_ps.dec").map_elements(trim_ra_dec_str, pl.String),
    )
    df = df.with_columns(
        pl.col("known_ps.ra").map_elements(utils.format_ra_hms, pl.String),
        pl.col("known_ps.dec").map_elements(
            utils.format_dec_dms,
            pl.String,
        ),
    )

    # Add known_ps.pos=(ra,dec) for querying with pgSphere
    df = df.with_columns(
        pl.concat_str(["known_ps.ra", "known_ps.dec"], separator=",")
        .alias("known_ps.pos")
        .map_elements(utils.add_parenthesis, pl.String),
    )

    df = df.with_columns(
        # Catalogue columns
        pl.lit("ATNF pulsar catalogue").alias("cat.name"),
        pl.lit(ATNF_BASE_URL).alias("cat.url"),
        pl.lit(1).alias("catalogue_id"),
        # CatalogueVisit columns
        pl.lit(visited_at).alias("cat_visit.visited_at"),
        pl.lit(1).alias("catalogue_visit_id"),
    )

    # Load catalogue data into the database
    df = df.to_pandas()
    with engine.connect() as conn, conn.begin():
        db = DatabaseLoader(conn=conn)
        for target in targets:
            df = db.insert_target(
                df=df,
                target=target,
            )


if __name__ == "__main__":
    main()
