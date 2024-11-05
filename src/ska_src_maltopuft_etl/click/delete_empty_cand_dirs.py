"""Reorganise a flat candidate data directory into a hierarchical directory
according to the schedule block start date and schedule block ID.
"""

import logging
import shutil
from pathlib import Path

import click
from ska_ser_logging import configure_logging

configure_logging(logging.INFO)
logger = logging.getLogger(__name__)


def delete_dir_if_any_file_empty(dir_: Path) -> None:
    """Deletes a directory if it contains an empty file."""
    for file in dir_.iterdir():
        if file.is_file() and file.stat().st_size == 0:
            logger.info(f"Found empty file {dir_ / file}. Deleting {dir_}.")
            shutil.rmtree(str(dir_), ignore_errors=True)
            break


@click.command()
@click.option("--source", required=True, type=click.Path(exists=True))
def delete_empty_cand_dirs(source: str) -> None:
    """Deletes all sub-directories that contain an empty file from the input
    directory.
    """
    source_ = Path(source)
    for src in source_.iterdir():
        delete_dir_if_any_file_empty(dir_=src)


if __name__ == "__main__":
    delete_empty_cand_dirs()
