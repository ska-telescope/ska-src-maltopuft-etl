"""Extract single pulse candidate observation data."""

from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import orjson
import polars as pl
from orjson import JSONDecodeError  # pylint: disable=no-name-in-module
from tqdm import tqdm

from ska_src_maltopuft_etl.core.flatten import flatten
from ska_src_maltopuft_etl.meertrap import logger
from ska_src_maltopuft_etl.utils import calculate_hash

from .models import MeertrapRunSummary

run_summary_hash_map: dict[str, str] = defaultdict(str)


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


def parse_if_unique(filename: Path) -> dict[str, Any] | None:
    """Checks for run summary uniqueness before parsing.

    Args:
        filename (Path): Run summary file path.

    Returns:
        dict[str, Any] | None: Run summary data if file is
        unique, otherwise None.

    """
    file_hash = calculate_hash(filename)
    if file_hash in run_summary_hash_map:
        return None

    parsed_data = parse_run_summary(filename=filename)
    run_summary_hash_map[file_hash] = file_hash
    return parsed_data


def parse_observations(directory: Path, n_file: int) -> pl.DataFrame:
    """Parse unique MeerTRAP run summary files in nested directories.

    Args:
        directory (Path): Parent directory to parse.
        n_file (int): The expected number of files to parse.

    Returns:
        pl.DataFrame: Parsed MeerTRAP run summary data.

    """
    logger.info(
        f"Parsing {n_file} MeerTRAP run summary files from {directory}",
    )

    parsed_data = []
    with ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(
                parse_if_unique,
                file,
            )
            for file in directory.rglob("*.json")
        ]

        n_task_fail = 0
        for future in tqdm(as_completed(futures), total=n_file):
            try:
                result = future.result()
                if result is not None:
                    parsed_data.append(result)
            except (
                Exception  # noqa: BLE001
            ):  # pylint: disable=broad-exception-caught
                n_task_fail += 1
                logger.exception("Task failed. Reason:")

    obs_df = (
        pl.DataFrame(parsed_data)
        # utc_stop can be null, meaning that the column type may not be
        # inferred correctly. Therefore, cast as datetime with microsecond
        # precision and UTC timezone
        .with_columns(pl.col("utc_stop").cast(pl.Datetime("us", "UTC")))
    )

    logger.info(
        f"Executed {n_file - n_task_fail} tasks. "
        f"Parsed {len(obs_df)} files from {directory}",
    )

    return obs_df
