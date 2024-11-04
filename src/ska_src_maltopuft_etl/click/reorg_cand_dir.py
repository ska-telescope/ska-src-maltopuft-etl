"""Reorganise a flat candidate data directory into a hierarchical directory
according to the schedule block start date and schedule block ID.
"""

import asyncio
import logging
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Any

import aiofiles
import click
from ska_ser_logging import configure_logging

from ska_src_maltopuft_etl.meertrap.observation.extract import read_json
from ska_src_maltopuft_etl.meertrap.observation.models import ScheduleBlockData

configure_logging(logging.INFO)
logger = logging.getLogger(__name__)


def build_target_name(src: str, target: Path) -> Path:
    """Gets the target directory name for a candidate file.

    The target directory name is <schedule_block_start_date>/
    <schedule_block_id>/<candidate>.

    Args:
        src (str): The name of the candidate directory to build the
        target name for.
        target (Path): The parent directory to hold reorganised data.

    Raises:
        ValueError: If the target directory name cannot be built.

    Returns:
        Path: The built target directory.

    """
    for file in src.iterdir():
        if file.match("*run_summary.json"):
            run_summary_data = read_json(filename=file)
            break

    candidate = src.parts[-1]
    sb = ScheduleBlockData(**run_summary_data.get("sb_details"))

    if sb.id is None or sb.actual_start_time is None or candidate is None:
        msg = f"Missing schedule block ID or start date in {src}"
        raise ValueError(msg)

    return target / sb.actual_start_time.strftime("%Y-%m-%d") / str(sb.id) / candidate


async def copy_source_to_target(source: Path, target: Path) -> None:
    """Copy a source file to a target directory.

    Args:
        source (Path): The path to the file to be copied.
        target (Path): The destination to copy the file to.

    """
    target.mkdir(parents=True, exist_ok=True)

    async with aiofiles.open(source, "rb") as fsrc, aiofiles.open(
        target / source.name,
        "wb",
    ) as fdst:
        while True:
            chunk = await fsrc.read(1024)
            if not chunk:
                break
            await fdst.write(chunk)


@click.command()
@click.option("--source", required=True, type=click.Path(exists=True))
@click.option("--target", required=True, type=click.Path())
def reorg_flat_source_to_target(source: str, target: str) -> None:
    """Reorganise a flat candidate data directory into a hierarchical directory
    according to the schedule block start date and schedule block ID.

    Args:
        source (str): The path to a flat directory containing candidate
        subdirectories.
        target (str): The path to the target directory to reorganise the data to.

    """
    source_ = Path(source)
    target_ = Path(target)

    async def copy_src_to_target_coros(cand_path: Path) -> AsyncGenerator[Any, Any, None]:
        for file_path in cand_path.iterdir():
            target_path = build_target_name(src=cand_path, target=target_)
            yield copy_source_to_target(
                source=cand_path / file_path,
                target=target_path,
            )

    async def main() -> None:
        tasks = []
        for idx, cand_path in enumerate(source_.iterdir()):
            if idx == 0:
                last_idx = idx

            if idx % 1_000 == 0 and idx != 0:
                logger.info(f"Reorganising files {last_idx}-{idx} files")
                await asyncio.gather(*tasks)
                tasks = []
                last_idx = idx

            async for awaitable in copy_src_to_target_coros(cand_path=cand_path):
               tasks.append(awaitable)

        await asyncio.gather(*tasks)

    asyncio.run(main())


if __name__ == "__main__":
    reorg_flat_source_to_target()
