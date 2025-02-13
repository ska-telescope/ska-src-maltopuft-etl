"""Extract single pulse candidate data."""

from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import polars as pl
from tqdm import tqdm

from ska_src_maltopuft_etl.meertrap import logger

from .models import SPCCL_FILE_TO_DF_COLUMN_MAP, MeertrapSpccl


def read_csv(filename: Path) -> list[str]:
    """Read lines from a csv file."""
    try:
        with Path.open(filename, encoding="utf-8") as f:
            return f.readlines()
    except FileNotFoundError as exc:
        msg = f"File {filename} not found."
        raise ValueError(msg) from exc


def read_spccl(filename: Path) -> dict[str, Any]:
    """Extract candidate data from an .spccl (tsv) file.

    Args:
        filename (Path): SPCCL file path.

    Raises:
        ValueError: If the file contains more than one candidate.

    Returns:
        dict[str, Any]: Dictionary containing SPCCL data.

    """
    lines = read_csv(filename=filename)

    if len(lines) != 1:
        msg = f"Expected 1 candidate in file {filename}, found {len(lines)}"
        raise ValueError(msg)

    # Convert tsv to csv
    line = (
        lines[0]
        # Replace tab delimiter with ,
        .replace("\t", ",")
        # Remove newline char
        .rstrip()
    )

    # Get list of each comma separated value (dropping index at element 0)
    split_line = line.split(",")[1:]

    # Create file path with candidate dir + filename for plot and filterbank
    candidate = filename.parent.parts[-1]
    values = [
        (f"{candidate}/{val}" if ".jpg" in val or ".fil" in val else val)
        for val in split_line
    ]

    return dict(zip(SPCCL_FILE_TO_DF_COLUMN_MAP.keys(), values, strict=False))


def parse_spccl(filename: Path) -> dict[str, Any]:
    """Parses candidate data from a MeerTRAP .spccl file.

    :param filename:  The absolute path to the spccl file.
    """
    candidate_directory = filename.parent.parts[-1]
    data = MeertrapSpccl(
        **read_spccl(filename=filename),
        filename=f"{candidate_directory}/{filename.stem}",
    )
    return data.model_dump()


def parse_candidates(directory: Path, n_file: int) -> pl.DataFrame:
    """Parse MeerTRAP single pulse candidate files in nested directories.

    Args:
        directory (Path): Parent directory to parse.
        n_file (int): The expected number of files to parse.

    Returns:
        pl.DataFrame: Parsed MeerTRAP candidate data.

    """
    logger.info(f"Parsing {n_file} MeerTRAP candidate data from {directory}")

    parsed_data = []
    with ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(
                parse_spccl,
                file,
            )
            for file in directory.rglob("*spccl*")
        ]

        n_task_fail = 0
        for future in tqdm(as_completed(futures), total=n_file):
            try:
                parsed_data.append(future.result())
            except (
                Exception  # noqa: BLE001
            ):  # pylint: disable=broad-exception-caught,
                n_task_fail += 1
                logger.exception("Task failed. Reason:")

    cand_df = pl.DataFrame(parsed_data)

    logger.info(
        f"Executed {n_file - n_task_fail} tasks. "
        f"Parsed {len(cand_df)} files from {directory}",
    )

    return cand_df
