"""Database load functions."""

from collections.abc import Mapping, Sequence
from typing import Any, TypeVar

import sqlalchemy as sa

ModelT = TypeVar("ModelT")


def flatten_ids(returned_ids: Any) -> list[int]:
    """Flattens a nested ID data structure."""
    return [id_[0] for id_ in returned_ids]


def insert_(
    conn: sa.Connection,
    model_class: ModelT,
    data: Sequence[Mapping[Any, Any]],
) -> list[int]:
    """Bulk inserts data into a database table.

    Args:
        conn (Session): The database connection.
        model_class (ModelT): The database table SQLAlchemy model.
        data (dict[str, Any]): A dictionary containing lists of rows
            to insert into the database, where keys and values are the table's
            column names and values, respectively.

    Returns:
        list[int]: A list of database primary keys for the inserted rows.

    """
    res = conn.execute(
        sa.insert(model_class).returning(
            model_class.id,
            sort_by_parameter_order=True,
        ),
        parameters=data,
    )
    ids = res.fetchall()
    return flatten_ids(ids)
