"""Extract single pulse candidate observation data."""

import logging
from pathlib import Path
from typing import Any

import orjson
from orjson import JSONDecodeError

from ska_src_maltopuft_etl.core.flatten import flatten

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


def extract_observation(filename: Path) -> dict[str, Any]:
    """Extracts data from a MeerTRAP run summary file.

    :param filename:  The absolute path to the run summary file.
    """
    data = MeertrapRunSummary(
        **read_json(filename),
        candidate=filename.parent.parts[-1],
        filename=filename.stem,
    )
    return flatten(data.model_dump())
