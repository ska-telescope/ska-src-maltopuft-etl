"""Extract the contents of tar archives in a source directory to a target
directory.
"""

import asyncio
import logging
import tarfile
from pathlib import Path

import click
from ska_ser_logging import configure_logging

configure_logging(logging.INFO)
logger = logging.getLogger(__name__)


async def untar(tar_file: Path, target: Path) -> None:
    """Extract tar archive contents to a target directory."""
    with tarfile.open(tar_file, "r") as tar:
        try:
            tar.extractall(path=target)  # noqa: S202
        except tarfile.ReadError:
            logger.exception(f"Error reading {tar_file}, skipping")


@click.command()
@click.option("--source", required=True, type=click.Path(exists=True))
@click.option("--target", required=True, type=click.Path())
def untar_directory(source: str, target: str) -> None:
    """Extract the contents of tar archives in a source directory to a target
    directory.
    """
    source_ = Path(source)
    target_ = Path(target)

    async def main() -> None:
        tasks = []
        for idx, src in enumerate(source_.iterdir()):
            if idx == 0:
                last_idx = idx

            # Untar batches of 5_000 archives
            if idx % 5_000 == 0 and idx != 0:
                logger.info(f"Extracting {last_idx} to {idx}")
                await asyncio.gather(*tasks)
                tasks = []
                last_idx = idx

            dest = target_ / src.stem
            tasks.append(untar(tar_file=src, target=dest))

        await asyncio.gather(*tasks)

    asyncio.run(main())


if __name__ == "__main__":
    untar_directory()
