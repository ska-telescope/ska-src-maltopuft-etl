"""Loads transformed data into MALTOPUFTDB."""

import logging

import polars as pl
import sqlalchemy as sa

from ska_src_maltopuft_etl.core.exceptions import DuplicateInsertError
from ska_src_maltopuft_etl.core.insert import (
    insert_,
    insert_row_or_get_conflict_id,
)
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
        columns = [
            col
            for col in df.columns
            if col.startswith(target.table_prefix)
            or col == target.primary_key
            or col in target.foreign_keys
        ]
        return (
            df.select(columns)
            .unique(target.primary_key)
            .rename(
                {col: col.replace(target.table_prefix, "") for col in columns},
            )
        )

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
            db_id = insert_row_or_get_conflict_id(
                conn=self.conn,
                target=target,
                data=attrs,
            )
            key_map[local_pk] = db_id

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
                data=rows_df.drop(target.primary_key).to_dicts(),
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
