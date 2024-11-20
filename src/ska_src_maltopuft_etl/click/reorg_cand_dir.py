"""Reorganise a flat candidate data directory into a hierarchical directory
according to the schedule block start date and schedule block ID.
"""

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import click
from ska_ser_logging import configure_logging
from tqdm import tqdm

from ska_src_maltopuft_etl.meertrap.observation.extract import read_json
from ska_src_maltopuft_etl.meertrap.observation.models import ScheduleBlockData

configure_logging(logging.INFO)
logger = logging.getLogger(__name__)


def build_target_name(source: Path, target: Path) -> Path:
    """Gets the target directory name for a candidate file.

    The target directory name is <schedule_block_start_date>/
    <schedule_block_id>/<candidate>.

    Args:
        source (str): The name of the candidate directory to build the
        target name for.
        target (Path): The parent directory to hold reorganised data.

    Raises:
        FileNotFoundError: If the run summary file is missing.
        ValueError: If the target directory name cannot be built.

    Returns:
        Path: The built target directory.

    """
    try:
        filename = next(source.glob("*run_summary.json"))
    except StopIteration as exc:
        msg = f"Missing run summary file in {source}"
        raise FileNotFoundError(msg) from exc

    run_summary_data = read_json(
        filename=filename,
    )

    candidate = source.parts[-1]
    sb = ScheduleBlockData(**run_summary_data.get("sb_details", {}))

    if sb.id is None or sb.actual_start_time is None or candidate is None:
        msg = f"Missing schedule block ID or start date in {source}"
        raise ValueError(msg)

    return (  # pylint: disable=no-member
        target
        / sb.actual_start_time.strftime("%Y-%m-%d")
        / str(sb.id)
        / candidate
    )


def hardlink_dir_files_to_target(source: Path, target: Path) -> None:
    """Create hard links to all files in the source directory in the
    target directory.

    Args:
        source (Path): The source directory to hard link files from.
        target (Path): The target directory to hard link files to.

    """
    for file in source.iterdir():
        if file.is_file():
            target_file = target / file.name
            if target_file.exists():
                target_file.unlink()  # Remove existing file or symlink
            logger.debug(
                f"Creating hard link from {file} to {target_file}",
            )
            target_file.hardlink_to(file)


def process_candidate_directory(source: Path, target: Path) -> None:
    """Process a candidate directory by copying its files to the target
    directory.

    Args:
        source (Path): The path to the candidate directory.
        target (Path): The path to the target directory.

    """
    target_ = build_target_name(source=source, target=target)
    target_.mkdir(parents=True, exist_ok=True)
    hardlink_dir_files_to_target(source=source, target=target_)


@click.command()
@click.option("--source", required=True, type=click.Path(exists=True))
@click.option("--target", required=True, type=click.Path())
def reorg_flat_source_to_target(source: str, target: str) -> None:
    """Reorganise a flat candidate data directory into a hierarchical
    directory according to the schedule block start date and schedule
    block ID.

    Args:
        source (str): The path to a flat directory containing candidate
        subdirectories.
        target (str): The path to the target directory to reorganise the data
        to.

    """
    # pylint: disable=duplicate-code

    source_ = Path(source)
    source_length = len(list(source_.glob("*")))
    logger.info(
        f"Reorganising {source_length} candidates in {source} to {target}",
    )

    with ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(
                process_candidate_directory,
                cand_path,
                Path(target),
            )
            for cand_path in source_.iterdir()
        ]

        n_task_fail = 0
        for future in tqdm(as_completed(futures), total=source_length):
            try:
                future.result()
            except Exception:  # pylint: disable=broad-exception-caught
                n_task_fail += 1
                logger.exception("Task failed. Reason:")

    logger.info(f"Failed to reorganise {n_task_fail} candidates.")
    logger.info("Successfully reorganised candidates.")


if __name__ == "__main__":
    # pylint: disable=no-value-for-parameter
    reorg_flat_source_to_target()
