"""Extract single pulse candidate observation data."""

import logging
from collections import defaultdict
from pathlib import Path
from typing import Any

import orjson
import polars as pl
from orjson import JSONDecodeError  # pylint: disable=no-name-in-module

from ska_src_maltopuft_etl.core.flatten import flatten
from ska_src_maltopuft_etl.utils.hash import calculate_hash

from .models import MeertrapRunSummary

logger = logging.getLogger(__name__)


def read_json(filename: Path) -> dict[str, Any]:
    """Serialises a JSON file to a dictionary.

    :param filename: The absolute path to the JSON file.
    """
    if not filename.match("*.json"):
        msg = f"File {filename} expects file extension '.json'"
        raise ValueError(msg)

    try:
        with Path.open(  # pylint: disable=unspecified-encoding
            filename,
            "rb",
        ) as f:
            return orjson.loads(  # pylint: disable=maybe-no-member
                f.read(),
            )
    except FileNotFoundError:
        logger.exception(f"File {filename} not found.")
        raise
    except JSONDecodeError:
        logger.exception(f"Error decoding JSON file {filename}")
        raise


def parse_run_summary(filename: Path) -> dict[str, Any]:
    """Parses data from a MeerTRAP run summary file.

    :param filename:  The absolute path to the run summary file.
    """
    candidate_directory = filename.parent.parts[-1]
    data = MeertrapRunSummary(
        **read_json(filename),
        filename=f"{candidate_directory}/{filename.stem}",
    )
    return flatten(data.model_dump())


def parse_observations(directory: Path) -> pl.DataFrame:
    """Parse unique MeerTRAP run summary files in nested directories.

    Args:
        directory (Path): Parent directory to parse.

    Returns:
        pl.DataFrame: Parsed MeerTRAP run summary data.

    """
    # TODO: Refactor
    # TODO: Async
    hash_map = defaultdict(str)
    parsed_data = []
    for idx, file in enumerate(directory.rglob("*.json")):
        if idx % 1000 == 0:
            logger.info(f"Parsing run summary #{idx}")

        file_hash = calculate_hash(file)

        if file_hash in hash_map:
            continue

        parsed_data.append({**parse_run_summary(filename=file)})
        hash_map[file_hash] = file

    return (
        pl.DataFrame(parsed_data)
        # utc_stop can be null, meaning that the column type may not be
        # inferred correctly. Therefore, cast as datetime with microsecond
        # precision and UTC timezone
        .with_columns(pl.col("utc_stop").cast(pl.Datetime("us", "UTC")))
    )
