"""ska-src-maltopuft-etl entrypoint."""

import logging
from pathlib import Path

import polars as pl
from ska_ser_logging import configure_logging

from ska_src_maltopuft_etl.core.config import config
from ska_src_maltopuft_etl.meertrap.meertrap import extract, load, transform


def main() -> None:
    """ska-src-maltopuft-etl entrypoint."""
    configure_logging(logging.INFO)
    logger = logging.getLogger(__name__)

    output_path: Path = config.get("output_path", Path())
    output_parquet = output_path / "candidates.parquet"

    try:
        raw_df = pl.read_parquet(output_parquet)
        logger.info(f"Read parsed data from {output_parquet}")
    except FileNotFoundError:
        logger.warning(f"No parsed data found at {output_parquet}")
        raw_df = extract()
        logger.debug(f"Writing parsed data to {output_parquet}")
        output_path.mkdir(parents=True, exist_ok=True)
        raw_df.write_parquet(output_parquet, compression="gzip")
        logger.info(f"Parsed data written to {output_parquet} successfully")

    obs_parquet = output_path / "obs_df.parquet"
    cand_parquet = output_path / "cand_df.parquet"

    try:
        obs_df = pl.read_parquet(obs_parquet)
        cand_df = pl.read_parquet(cand_parquet)
        logger.info(
            f"Read transformed data from {obs_parquet} and {cand_parquet}",
        )
    except FileNotFoundError:
        logger.warning(
            f"No cached transformed data found at {obs_parquet} and "
            f"{cand_parquet}",
        )
        output_path.mkdir(parents=True, exist_ok=True)
        obs_df, cand_df = transform(df=raw_df)

    load(
        obs_df=obs_df,
        cand_df=cand_df,
    )


if __name__ == "__main__":
    main()
