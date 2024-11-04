"""Extract single pulse candidate data."""

import logging
from pathlib import Path
from typing import Any

from .models import MeertrapSpccl

logger = logging.getLogger(__name__)

SPCCL_COLUMNS = (
    "mjd",
    "dm",
    "width",
    "snr",
    "beam",
    "beam_mode",
    "ra",
    "dec",
    "label",
    "probability",
    "fil_file",
    "plot_file",
)


def read_csv(filename: Path) -> list[str]:
    """Read lines from a csv file."""
    try:
        with Path.open(filename, encoding="utf-8") as f:
            return f.readlines()
    except FileNotFoundError as exc:
        msg = f"File {filename} not found."
        raise ValueError(msg) from exc


def read_spccl(filename: Path) -> dict[str, Any]:
    """Extract candidate data from an .spccl (tsv) file."""
    lines = read_csv(filename=filename)
    if len(lines) != 1:
        msg = f"Expected 1 candidate in file {filename}, found {len(lines)}"
        raise ValueError(msg)
    # Convert tsv to csv
    line = lines[0].replace("\t", ",")
    # Remove newline char
    line = line.rstrip()
    # Get list of each comma separated value (dropping index at element 0)
    split_line = line.split(",")[1:]

    # Create file path with candidate dir + filename for plot and filterbank
    candidate = filename.parent.parts[-1]
    values = [
        (f"{candidate}/{val}" if ".jpg" in val or ".fil" in val else val)
        for val in split_line
    ]

    return dict(zip(SPCCL_COLUMNS, values, strict=False))


def extract_spccl(filename: Path) -> dict[str, Any]:
    """Extracts candidate data from a MeerTRAP .spccl file.

    :param filename:  The absolute path to the spccl file.
    """
    candidate = filename.parent.parts[-1]
    data = MeertrapSpccl(
        **read_spccl(filename=filename),
        candidate=candidate,
        filename=f"{candidate}/{filename.stem}",
    )
    return data.model_dump()
