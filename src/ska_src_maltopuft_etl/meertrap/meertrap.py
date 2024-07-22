"""Meertrap ETL entrypoint."""

import logging
from pathlib import PosixPath
from typing import Any

import polars as pl

from ska_src_maltopuft_etl.core.config import config

from .observation.extract import extract_observation

logger = logging.getLogger(__name__)


def parse_candidate_dir(candidate_dir: PosixPath) -> dict[str, Any]:
    """Parse files in a candidate directory.

    :param candidate_dir: The absolute path to the candidate directory to
        parse.

    :return: A dictionary containing the normalised candidate data.
    """
    run_summary_data = {}
    for file in candidate_dir.iterdir():
        if file.match("*run_summary.json"):
            logger.debug(f"Parsing observation metadata from {file}")
            run_summary_data = extract_observation(filename=file)
            continue
        if file.match("*spccl.log"):
            logger.debug(f"Parsing candidate data from {file}")
            continue
        if file.match("*.jpg"):
            continue

        logger.warning(f"Found file {file} in unexpected format.")

    return run_summary_data


def extract(
    root_path: PosixPath = config.get("data_path", ""),
) -> pl.DataFrame:
    """Extract MeerTRAP data.

    :param root_path: The absolute path to the candidate data directory.
    """
    logger.info("Started extract routine")

    cand_df = pl.DataFrame()
    rows: list[dict[str, Any]] = []

    for idx, candidate_dir in enumerate(root_path.iterdir()):
        if idx % 500 == 0:
            logger.info(f"Parsing candidate #{idx} from {candidate_dir}")
        if idx > 0 and (idx % 1000) == 0:
            cand_df = cand_df.vstack(pl.DataFrame(rows, orient="row"))
            rows = []
        if idx > 0 and (idx % 5000) == 0:
            cand_df = cand_df.rechunk()

        if not candidate_dir.is_dir():
            logger.warning(
                f"Unexpected file {candidate_dir} found in "
                f"{config.get('data_path')}",
            )
            continue

        rows.append(parse_candidate_dir(candidate_dir=candidate_dir))

    logger.info("Extract routine completed successfully")
    return cand_df
