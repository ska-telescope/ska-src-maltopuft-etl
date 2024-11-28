"""Database load functions."""

import logging
from collections.abc import Mapping, Sequence
from typing import Any

import sqlalchemy as sa
from psycopg import errors as psycopgexc
from ska_src_maltopuft_backend.core.database.base import Base

from ska_src_maltopuft_etl.core.exceptions import (
    DuplicateInsertError,
    ForeignKeyError,
)

logger = logging.getLogger(__name__)


def flatten_ids(returned_ids: Any) -> list[int]:
    """Flattens a nested ID data structure."""
    return [id_[0] for id_ in returned_ids]


def insert_(
    conn: sa.Connection,
    model_class: type[Base],
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
    try:
        logger.debug(
            f"Inserting {data} into {model_class.__table__.name}",
        )
        res = conn.execute(
            sa.insert(model_class).returning(
                model_class.id,
                sort_by_parameter_order=True,
            ),
            parameters=data,
        )
    except sa.exc.IntegrityError as exc:
        msg = f"Failed to insert data into {model_class.__table__.name}"
        if isinstance(exc.orig, psycopgexc.UniqueViolation):
            msg = f"{msg}, {exc.orig}"
            raise DuplicateInsertError(msg) from exc
        if isinstance(exc.orig, psycopgexc.ForeignKeyViolation):
            msg = f"{msg}, {exc.orig}"
            raise ForeignKeyError(msg) from exc
        raise RuntimeError(msg) from exc

    returned_ids = res.fetchall()
    logger.info(
        f"Inserted {len(returned_ids)} rows into {model_class.__table__.name}",
    )
    ids = flatten_ids(returned_ids=returned_ids)
    logger.debug(f"Inserted parameters are: ({ids},{data})")
    logger.info(f"Inserted IDs {ids} into {model_class.__table__.name}")
    return ids
