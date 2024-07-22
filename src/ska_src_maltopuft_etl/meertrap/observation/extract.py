"""Extract single pulse candidate observation data."""

import logging
from pathlib import Path, PosixPath
from typing import Any

import orjson

from ska_src_maltopuft_etl.core.config import config
from ska_src_maltopuft_etl.core.flatten import flatten

from .models import MeertrapRunSummary

logger = logging.getLogger(__name__)


def read_json(filename: PosixPath) -> dict[str, Any]:
    """Serialises a JSON file to a dictionary.

    :param filename: The absolute path to the JSON file.
    """
    if not filename.match("*.json"):
        msg = f"File {filename} expects file extension '.json'"
        raise ValueError(msg)

    try:
        with Path.open(filename, "rb", encoding="utf-8") as f:
            return orjson.loads(f.read())  # pylint: disable=maybe-no-member
    except FileNotFoundError as exc:
        msg = f"File {filename} not found."
        raise ValueError(msg) from exc


def extract_observation(filename: PosixPath) -> dict[str, Any]:
    """Extracts data from a MeerTRAP run summary file.

    :param filename:  The absolute path to the run summary file.
    """
    rel_filename = filename.relative_to(config.get("data_path", ""))
    candidate = rel_filename.parts[0]
    data = MeertrapRunSummary(
        **read_json(filename),
        candidate=candidate,
        filename=str(rel_filename),
    )
    return flatten(data.model_dump())
