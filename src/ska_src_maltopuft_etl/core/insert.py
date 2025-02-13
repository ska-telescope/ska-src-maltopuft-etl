"""Database load functions."""

from collections.abc import Mapping, Sequence
from typing import Any

import sqlalchemy as sa
from psycopg import errors as psycopgexc
from ska_src_maltopuft_backend.core.custom_types import ModelT
from sqlalchemy.dialects.postgresql import insert as pg_insert

from ska_src_maltopuft_etl.core import logger
from ska_src_maltopuft_etl.core.exceptions import (
    DuplicateInsertError,
    ForeignKeyError,
    MissingDataOnConflictError,
)

from .target import TargetInformation


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
    try:
        res = conn.execute(
            sa.insert(model_class).returning(
                model_class.id,
                sort_by_parameter_order=True,
            ),
            parameters=data,
        )
    except sa.exc.IntegrityError as exc:
        msg = (
            f"Failed to insert data into {model_class.__table__.name}, "
            "attempting to insert row-by-row."
        )
        logger.warning(msg)
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


def insert_row_or_get_conflict_id(
    conn: sa.Connection,
    target: TargetInformation,
    data: Mapping[Any, Any],
) -> int:
    """Attempts to insert a row into the database, returning the
    conflicting ID if there is a conflict.

    Args:
        conn (Session): Database connection.
        target (TargetInformation): Database table target information.
        data (dict[str, Any]): Dictionary containing lists the row attributes
            to insert into the database, where keys and values are the table's
            column names and values, respectively.

    Returns:
        int: The inserted or conflicting ID.

    """
    insert_stmt = (
        pg_insert(target.model_class)
        .values(data)
        .on_conflict_do_nothing(constraint=target.unique_constraint)
        .returning(target.model_class.id)
        .cte("e")
    )

    select_stmt = sa.select(target.model_class.id)
    for k, v in data.items():
        if not hasattr(target.model_class, k):
            continue
        select_stmt = select_stmt.where(getattr(target.model_class, k) == v)

    stmt = sa.select(insert_stmt).union(select_stmt)
    res = conn.execute(stmt).fetchone()

    if res is None or len(res) == 0:
        msg = (
            f"No {target.table_name} data returned for conflicting "
            f"parameters {data}"
        )
        raise MissingDataOnConflictError(msg) from None

    return res[0]
