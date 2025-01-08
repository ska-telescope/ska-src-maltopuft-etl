"""ska-src-maltopuft-etl entrypoint."""

import logging
from pathlib import Path

from ska_ser_logging import configure_logging

from ska_src_maltopuft_etl.core.config import config
from ska_src_maltopuft_etl.meertrap.meertrap import load, parse, transform


def main() -> None:
    """ska-src-maltopuft-etl entrypoint."""
    configure_logging(logging.INFO)
    logger = logging.getLogger(__name__)

    output_path: Path = config.get("output_path", Path())

    # TODO: Move save parquet logic lower down the callstack and control with setting
    # output_parquet = output_path / "candidates.parquet"

    # try:
    #    raw_df = pl.read_parquet(output_parquet)
    #    logger.info(f"Read parsed data from {output_parquet}")
    # except FileNotFoundError:
    #    logger.warning(f"No parsed data found at {output_parquet}")
    #    logger.debug(f"Writing parsed data to {output_parquet}")
    #    output_path.mkdir(parents=True, exist_ok=True)
    #    raw_df.write_parquet(output_parquet, compression="gzip")
    #    logger.info(f"Parsed data written to {output_parquet} successfully")

    obs_df, cand_df = parse()
    obs_df, cand_df = transform(obs_df=obs_df, cand_df=cand_df)
    obs_df, cand_df = load(obs_df=obs_df, cand_df=cand_df)


if __name__ == "__main__":
    main()
