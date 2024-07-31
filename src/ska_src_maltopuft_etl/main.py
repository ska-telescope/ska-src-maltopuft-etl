"""ska-src-maltopuft-etl entrypoint."""

import logging
from pathlib import Path, PosixPath

import polars as pl
from ska_ser_logging import configure_logging

from ska_src_maltopuft_etl.core.config import config
from ska_src_maltopuft_etl.meertrap.meertrap import extract, load, transform


def main() -> None:
    """ska-src-maltopuft-etl entrypoint."""
    configure_logging(logging.INFO)
    logger = logging.getLogger(__name__)

    output_path: PosixPath = config.get("output_path", Path())
    raw_df: pl.DataFrame = extract()

    output_parquet = output_path / "candidates.parquet"
    logger.info(f"Writing parsed data to {output_parquet}")
    raw_df.write_parquet(output_parquet, compression="gzip")
    logger.info(f"Parsed data written to {output_parquet} successfully")
    raw_df = pl.read_parquet(output_parquet)

    obs_df, beam_df, cand_df = transform(df=raw_df)
    load(
        obs_df=obs_df.to_pandas(),
        beam_df=beam_df.to_pandas(),
        cand_df=cand_df.to_pandas(),
    )


if __name__ == "__main__":
    main()
