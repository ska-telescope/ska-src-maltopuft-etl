"""Loads transformed data into MALTOPUFTDB."""

import logging
from typing import Any

import polars as pl
import sqlalchemy as sa
from ska_src_maltopuft_backend.core.types import ModelT

from ska_src_maltopuft_etl.core.exceptions import DuplicateInsertError
from ska_src_maltopuft_etl.core.insert import insert_
from ska_src_maltopuft_etl.core.target import TargetInformation

logger = logging.getLogger(__name__)


class DatabaseLoader:
    """Loads transformed data into a MALTOPUFT DB instance.

    The DatabaseLoader instance is initialised with a transactional SQLAlchemy
    connection to allow rollback in the case where an error is encountered
    while making many insertions into the target database.
    """

    def __init__(self, conn: sa.Connection) -> None:
        """DatabaseLoader initialisation."""
        self.conn = conn
        self.foreign_keys_map: dict[str, dict[int, int]] = {}

    def prepare_data_for_insert(
        self,
        df: pl.DataFrame,
        target: TargetInformation,
    ) -> pl.DataFrame:
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

        Returns:
            DataFrame ready for insertion into MALTOPUFTDB.

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
        return (
            df.select(columns)
            .unique(primary_key)
            .rename({col: col.replace(table_prefix, "") for col in columns})
        )

    def insert_row(
        self,
        data: dict[str, Any],
        target: TargetInformation,
        *,
        transactional: bool,
    ) -> int:
        """Insert target row(s) into the database.

        Args:
            data (dict[str, Any]): Attributes to insert.
            target (TargetInformation): The target table to fetch from.
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

    def get_by(
        self,
        target: TargetInformation,
        attributes: dict[Any, Any],
    ) -> sa.Row[ModelT] | None:
        """Fetch a row with given attributes from a table.

        Args:
            target (TargetInformation): The target table to fetch from.
            attributes (dict[str, Any]): The attributes to filter by.

        Returns:
            sa.Row[ModelT] | None: The fetched row or None if row not found.

        """
        logger.debug(
            f"Fetching {target.table_name()} row "
            f"with attributes {attributes}",
        )
        stmt = sa.select(target.model_class)
        for k, v in attributes.items():
            if not hasattr(target.model_class, k):
                continue

            stmt = stmt.where(getattr(target.model_class, k) == v)

        logger.debug(stmt)
        res = self.conn.execute(stmt).fetchone()
        if res is None:
            logger.debug(
                f"Row with attributes {attributes} does not exist in table "
                f"{target.table_name()}",
            )
            return res

        logger.debug(
            f"Fetched {target.table_name()} row with id {res.id} "
            f"with attributes {attributes}",
        )
        logger.debug(f"Fetched {target.table_name()}: {res}")
        return res

    def get_or_insert_row_if_not_exist(
        self,
        df: pl.DataFrame,
        target: TargetInformation,
    ) -> dict[int, int]:
        """Gets each records in a dataframe to return a map of df to database
        primary keys.

        Rows are inserted into the database if they do not already exist.

        Args:
            df (pl.DataFrame): Rows to insert.
            target (TargetInformation): Target table information.

        Returns:
            dict[int, int]: Map of dataframe to database primary keys.

        """
        key_map = {}
        for attrs in df.to_dicts():
            local_pk = attrs.pop(target.primary_key)
            res = self.get_by(target=target, attributes=attrs)

            if res is not None:
                key_map[local_pk] = res.id
            else:
                key_map[local_pk] = self.insert_row(
                    data=attrs,
                    target=target,
                    transactional=False,
                )

        return key_map

    def load_target_rows(
        self,
        target: TargetInformation,
        df: pl.DataFrame,
    ) -> dict[int, int]:
        """Load target rows into the database.

        Args:
            target: Dictionary containing target table metadata.
            df: DataFrame containing transformed data.

        Returns:
            dict[int, int]: Map of dataframe to database primary keys.

        """
        rows_df = self.prepare_data_for_insert(
            df=df.unique(target.primary_key),
            target=target,
        )

        key_map = self.get_or_insert_row_if_not_exist(
            df=rows_df,
            target=target,
        )
        self.foreign_keys_map[target.primary_key] = key_map
        return self.foreign_keys_map[target.primary_key]

    def bulk_load_target_rows(
        self,
        target: TargetInformation,
        df: pl.DataFrame,
    ) -> dict[int, int]:
        """Insert a subset of DataFrame columns into a target database table.

        Args:
            target: Dictionary containing target table metadata.
            df: DataFrame containing transformed data.

        Returns:
            dict[int, int]: Map of dataframe to database primary keys.

        """
        rows_df = self.prepare_data_for_insert(
            df=df.unique(target.primary_key),
            target=target,
        )

        with self.conn.begin_nested():
            inserted_ids = insert_(
                conn=self.conn,
                model_class=target.model_class,
                data=rows_df.to_dicts(),
            )

        self.foreign_keys_map[target.primary_key] = dict(
            zip(rows_df[target.primary_key], inserted_ids, strict=False),
        )

        return self.foreign_keys_map[target.primary_key]

    def load(
        self,
        target: TargetInformation,
        df: pl.DataFrame,
    ) -> pl.DataFrame:
        """Load data from a DataFrame into a target database table.

        Bulk inserts data into the target data. If an DupliacteInsertError
        is raised, the data is inserted row-by-row.

        Args:
            target (TargetInformation): Target database table information.
            df (pl.DataFrame): DataFrame containing data to insert.

        Returns:
            pl.DataFrame: DataFrame with updated primary keys.

        """
        try:
            self.bulk_load_target_rows(target=target, df=df)
        except DuplicateInsertError:
            self.load_target_rows(target=target, df=df)

        return df.with_columns(
            pl.col(target.primary_key).map_elements(
                lambda x: (
                    self.foreign_keys_map.get(target.primary_key, {}).get(x, x)
                ),
                pl.Int32,
            ),
        )
