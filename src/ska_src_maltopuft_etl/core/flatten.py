"""Flattens nested dictionary into a flattened dictionary."""

import collections
from collections.abc import MutableMapping
from typing import Any


def flatten(
    dictionary: MutableMapping[str, Any],
    exclude_keys: list[str] | None = None,
    parent_key: str | None = None,
    separator: str = ".",
) -> dict[str, Any]:
    """Turn a nested dictionary into a flattened dictionary.

    :param dictionary: The dictionary to flatten
    :param parent_key: The string to prepend to dictionary's keys
    :param separator: The string used to separate flattened keys
    :return: A flattened dictionary
    """
    exclude_keys = exclude_keys or []
    items = []
    for key, value in dictionary.items():
        new_key = str(parent_key) + separator + key if parent_key else key
        if key in exclude_keys or new_key in exclude_keys:
            items.append((key, value))
            continue

        if isinstance(value, collections.abc.MutableMapping):
            items.extend(
                flatten(
                    dictionary=value,
                    exclude_keys=exclude_keys,
                    parent_key=new_key,
                    separator=separator,
                ).items(),
            )
        elif isinstance(value, list):
            items.append((new_key, value))
        else:
            items.append((new_key, value))
    return dict(items)
