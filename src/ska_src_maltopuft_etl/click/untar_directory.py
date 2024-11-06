"""Extract the contents of tar archives in a source directory to a target
directory.
"""

import logging
import tarfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import click
from ska_ser_logging import configure_logging
from tqdm import tqdm

configure_logging(logging.INFO)
logger = logging.getLogger(__name__)


def extract_tar_to_directory(source: Path, target: Path) -> None:
    """Extract tar archive contents to a target directory.

    Args:
        source (Path): The path to the tar archive to extract.
        target (Path): The directory to extract the tar archive contents to.

    """
    dest = target / source.stem
    dest.mkdir(parents=True, exist_ok=True)
    with tarfile.open(source, "r") as tar:
        try:
            tar.extractall(path=dest)  # noqa: S202
        except tarfile.ReadError:
            logger.exception(f"Error reading {source}, skipping")


@click.command()
@click.option("--source", required=True, type=click.Path(exists=True))
@click.option("--target", required=True, type=click.Path())
def untar_directory(source: str, target: str) -> None:
    """Extract the contents of tar archives in a source directory to a target
    directory.

    Args:
        source (str): The path to the source directory containing tar
        archives.
        target (str): The path to the target directory to extract tar
        archives to.

    """
    source_ = Path(source)
    source_length = len(list(source_.glob("*")))
    logger.info(f"Extracting {source_length} archives in {source} to {target}")

    with ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(
                extract_tar_to_directory,
                src,
                Path(target),
            )
            for src in source_.iterdir()
        ]

        n_task_fail = 0
        for future in tqdm(as_completed(futures), total=source_length):
            try:
                future.result()
            except Exception:  # pylint: disable=broad-exception-caught
                n_task_fail += 1
                logger.exception("Task failed. Reason:")

    logger.info(f"Failed to extract {n_task_fail} archives.")
    logger.info("Successfully extracted archives")


if __name__ == "__main__":
    # pylint: disable=no-value-for-parameter
    untar_directory()
