"""Loads transformed data into MALTOPUFTDB."""

import logging
from collections.abc import Hashable
from typing import Any

import numpy as np
import pandas as pd
import sqlalchemy as sa
from ska_src_maltopuft_backend.core.database.base import Base
from ska_src_maltopuft_backend.core.types import ModelT

from ska_src_maltopuft_etl.core.insert import insert_
from ska_src_maltopuft_etl.core.target import TargetInformation

logger = logging.getLogger(__name__)


class DatabaseLoader:
    """Loads transformed data into a MALTOPUFT DB instance.

    The DatabaseLoader instance is initialised with a transactional SQLAlchemy
    connection to allow rollback in the case where an error is encountered
    while making many insertions into the target database.

    Internally, the DatabaseLoader keeps track of the primary keys inserted
    into the database and maps them to the DataFrame indexes.
    """

    def __init__(self, conn: sa.Connection) -> None:
        """DatabaseLoader initialisation."""
        self.conn = conn
        self.foreign_keys_map: dict[str, list[tuple[int, int]]] = {}

    def prepare_data_for_insert(
        self,
        df: pd.DataFrame,
        target: TargetInformation,
    ) -> pd.DataFrame:
        """Prepare the subset of data to insert.

        Gets the target table attributes from the dataframe. A dataframe
        column is considered a target table attribute if it meets any of
        the following criteria:

        1. Columns start with the target table prefix (e.g. `obs`).
        2. The column is the target table primary key.
        3. The column is a target table foreign key.

        Duplicate rows are dropped and the DataFrame column names are updated
        to remove the table prefix, such that they match the target table
        columns. NaN values are replaced with None.

        Args:
            df: DataFrame containing the data.
            target: Dictionary containing target table details.
            foreign_keys_map: DataFrame containing foreign key mappings,
            if any.

        Returns:
            DataFrame ready for insertion.

        """
        table_prefix = target.table_prefix
        primary_key = target.primary_key
        foreign_keys = target.foreign_keys

        columns = [
            col
            for col in df.columns
            if col.startswith(table_prefix)
            or col == primary_key
            or col in foreign_keys
        ]
        target_df = df[columns].drop_duplicates(subset=primary_key)
        target_df.columns = target_df.columns.str.replace(table_prefix, "")
        return target_df.replace({np.nan: None})

    def insert_target(
        self,
        df: pd.DataFrame,
        target: TargetInformation,
    ) -> pd.DataFrame:
        """Insert a subset of DataFrame columns into a target database table.

        Args:
            df: DataFrame containing transformed data.
            target: Dictionary containing target table metadata.

        Returns:
            Updated DataFrame with inserted IDs.

        """
        target_df = self.prepare_data_for_insert(df=df, target=target)

        inserted_ids = insert_(
            conn=self.conn,
            model_class=target.model_class,
            data=target_df.to_dict(orient="records"),
        )

        primary_key = target.primary_key
        self.foreign_keys_map[primary_key] = list(
            zip(target_df[primary_key], inserted_ids, strict=False),
        )

        # We need to update all primary_key values simultaneously
        # so that we don't end up with conflicting values
        df[primary_key] = df[primary_key].map(
            {v[0]: v[1] for v in self.foreign_keys_map[primary_key]},
        )
        return df

    def insert_row(
        self,
        target: TargetInformation,
        data: dict[Hashable, Any],
        *,
        transactional: bool,
    ) -> int:
        """Insert target row(s) into the database.

        Args:
            target (dict[str, Any]): _description_
            data (dict[str, Any]): _description_
            transactional (bool): Perform the insert in a nested
            transaction.

        Returns:
            list[int]: The inserted IDs.

        """
        if not transactional:
            return insert_(
                conn=self.conn,
                model_class=target.model_class,
                data=[data],
            )[0]

        with self.conn.begin_nested():
            return self.insert_row(
                target=target,
                data=data,
                transactional=False,
            )

    def update_foreign_keys_map(
        self,
        key_name: str,
        initial_value: int,
        update_value: int,
    ) -> None:
        """Update a key dict in the foreign keys map.

        Args:
            key_name (str): The key map to update.
            initial_value (int): The initial value.
            update_value (int): The updated value.

        """
        if self.foreign_keys_map.get(key_name) is None:
            self.foreign_keys_map[key_name] = []

        initial_value = int(initial_value)
        update_value = int(update_value)

        self.foreign_keys_map[key_name].append((initial_value, update_value))
        logger.debug(f"Updated foreign key map to: {self.foreign_keys_map}")

    def get_by(
        self,
        model_class: type[Base],
        attributes: dict[Any, Any],
    ) -> sa.Row[ModelT] | None:
        """Fetch a row with given attributes from a table.

        Args:
            model_class (dict[str, Any]): The SQLAlchemy model class for the
            target table.
            attributes (dict[str, Any]): The attributes to filter by.

        Returns:
            sa.Row[ModelT] | None: The fetched row or None if row not found.

        """
        logger.debug(
            f"Fetching row from table {model_class.__table__.name} "
            f"with attributes {attributes}",
        )
        stmt = sa.select(model_class)
        for k, v in attributes.items():
            stmt = stmt.where(getattr(model_class, k) == v)

        logger.debug(stmt)
        res = self.conn.execute(stmt).fetchone()
        if res is None:
            logger.info("Row does not exist in table")
            return res

        logger.info(
            f"Fetched {model_class.__table__.name} row with id {res.id}",
        )
        logger.debug(f"Fetched {model_class.__table__.name}: {res}")
        return res

    def count(self, target: TargetInformation) -> int:
        """Count the number of rows in a table.

        Args:
            target (dict[str, Any]): The table information.

        Returns:
            int: The number of rows in the table.

        """
        count_query = sa.select(
            sa.func.count(),  # pylint: disable = not-callable
        ).select_from(target.model_class)

        logger.debug(count_query)
        count = self.conn.execute(count_query).scalar()
        return count or 0
