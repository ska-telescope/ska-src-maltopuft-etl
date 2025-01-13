"""Hash utilities."""

import hashlib
from collections.abc import Callable
from pathlib import Path


def calculate_hash(
    file_path: Path,
    hash_algorithm: Callable = hashlib.md5,
) -> str:
    """Calculate the hash of a file.

    Args:
        file_path (Path): The file to hash.
        hash_algorithm (Callable, optional): File hash algorithm.
        Defaults to hashlib.md5.

    Returns:
        str: The file hash.

    """
    hash_obj = hash_algorithm()
    with Path.open(  # pylint: disable=unspecified-encoding
        file_path,
        "rb",
    ) as f:
        while chunk := f.read(8192):
            hash_obj.update(chunk)
    return hash_obj.hexdigest()
