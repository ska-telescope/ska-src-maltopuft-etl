"""Loads transformed data into MALTOPUFTDB."""

import logging
from typing import Any

import numpy as np
import pandas as pd
import sqlalchemy as sa

from ska_src_maltopuft_etl.core.insert import insert_

logging.getLogger(__name__)


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
        self.foreign_keys_map: dict[str, dict[int, int]] = {}

    def _get_table_attributes(
        self,
        df: pd.DataFrame,
        table_prefix: str,
        primary_key: str,
        foreign_keys: list[str],
    ) -> pd.DataFrame:
        """Get the target table attributes from the dataframe.

        A dataframe column is considered a target table attribute if it meets
        any of the following criteria:

        1. Columns start with the target table prefix (e.g. `obs`).
        2. The column is the target table primary key.
        3. The column is a target table foreign key.

        Args:
            df: DataFrame containing the data.
            table_prefix: Prefix of the target table columns.
            primary_key: Primary key of the target table.
            foreign_keys: List of foreign keys of the target table.

        Returns:
            DataFrame with target table attributes.

        """
        columns = [
            col
            for col in df.columns
            if col.startswith(table_prefix)
            or col == primary_key
            or col in foreign_keys
        ]
        table_df = df[columns].drop_duplicates(subset=primary_key)
        table_df.columns = table_df.columns.str.replace(table_prefix, "")
        return table_df

    def prepare_data_for_insert(
        self,
        df: pd.DataFrame,
        target: dict[str, Any],
    ) -> pd.DataFrame:
        """Prepare the subset of data to insert.

        Args:
            df: DataFrame containing the data.
            target: Dictionary containing target table details.
            foreign_keys_map: DataFrame containing foreign key mappings,
            if any.

        Returns:
            DataFrame ready for insertion.

        """
        table_prefix = target.get("table_prefix", "")
        primary_key = target.get("primary_key", "")
        foreign_keys = target.get("foreign_keys", [])

        return self._get_table_attributes(
            df=df,
            table_prefix=table_prefix,
            primary_key=primary_key,
            foreign_keys=foreign_keys,
        )

    def _insert_table(
        self,
        df: pd.DataFrame,
        conn: sa.Connection,
        target: dict[str, Any],
    ) -> pd.DataFrame:
        """Insert a subset of pd.DataFrame columns into a database table.

        Args:
            df: DataFrame containing the data.
            conn: Database connection.
            target: Dictionary containing target table details.

        Returns:
            DataFrame with updated primary keys.

        """
        table_df = self.prepare_data_for_insert(df=df, target=target)

        inserted_ids = insert_(
            conn=conn,
            model_class=target["model_class"],
            data=table_df.to_dict(orient="records"),
        )

        primary_key = target["primary_key"]
        self.foreign_keys_map[primary_key] = dict(
            zip(table_df[primary_key], inserted_ids, strict=False),
        )
        df[primary_key] = df[primary_key].map(
            self.foreign_keys_map[primary_key],
        )
        return df

    def load_target(
        self,
        df: pd.DataFrame,
        target: dict[str, Any],
    ) -> pd.DataFrame:
        """Load MeerTRAP observation metadata into MALTOPUFT DB.

        Args:
            df: DataFrame containing transformed data.
            target: Dictionary containing target table metadata.

        Returns:
            Updated DataFrame with inserted IDs.

        """
        df = df.replace({np.nan: None})
        return self._insert_table(
            df=df,
            conn=self.conn,
            target=target,
        )

    def update_foreign_keys_map(
        self,
        primary_key_name: str,
        initial_value: int,
        update_value: int
    ) -> None:
        if self.foreign_keys_map.get(primary_key_name) is None:
            self.foreign_keys_map[primary_key_name] = {}
        self.foreign_keys_map[primary_key_name][initial_value] = update_value
