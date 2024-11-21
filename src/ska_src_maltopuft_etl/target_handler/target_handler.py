"""Handles DataFrame operations while loading transformed Observation
metadata and Candidate data into MALTOPUFTDB target tables.
"""

import logging
from typing import Any

import polars as pl

from ska_src_maltopuft_etl.core.target import TargetInformation

logger = logging.getLogger(__name__)


class TargetDataFrameHandler:
    """TargetDataFrameHandler class module."""

    def __init__(  # noqa: D107, ANN204
        self,
        df: pl.DataFrame,
        targets: list[TargetInformation],
        parent=None,  # noqa: ANN001, "TargetDataFrameHandler" | None
    ):
        self.df = df.clone()
        self.targets = targets
        self.parent = parent

        if parent is None:
            self.df = self.df.with_row_index()

    def unique_rows(
        self,
        target: TargetInformation,
    ) -> pl.DataFrame:
        """Return the unique rows for a target table.

        Args:
            target (dict[str, Any]): The target table details.

        Returns:
            pd.DataFrame: The unique rows for the target table.

        """
        if target not in self.targets:
            msg = f"Target {target} not found in targets"
            raise RuntimeError(msg)

        logger.debug(
            f"Getting unique rows by distinct {target.primary_key} values",
        )
        unique_rows = self.df.unique(
            subset=[target.primary_key],
        )
        logger.info(
            f"Found {len(unique_rows)} unique rows for "
            f"{target.table_name()} table",
        )
        logger.debug(
            f"Unique {target.table_name()} "
            f"rows are {unique_rows.to_dicts()}",
        )
        return unique_rows

    def primary_keys(
        self,
        target: TargetInformation,
        row: pl.DataFrame | None = None,
    ) -> list[int]:
        """Get the primary key of a row in a DataFrame.

        Args:
            target (dict[str, Any]): The target table details.
            row (pd.DataFrame): The row attributes to extract the primary
            key for.

        Returns:
            list[int]: A list of primary keys of the dataframe or row.

        """
        target.primary_key = target.primary_key
        if row is not None:
            primary_keys = row[target.primary_key].to_list()
        else:
            primary_keys = self.df[target.primary_key].unique().to_list()

        logger.debug(
            f"Dataframe primary keys {target.primary_key}={primary_keys}",
        )
        return primary_keys

    def update_df_value(
        self,
        col: str,
        initial_value: int,
        update_value: int,
    ) -> pl.DataFrame:
        """Update DataFrame column values.

        Updates all cells in the Pandas DataFrame where the column value is
        equal to initial_value to the update_value.

        If the TargetDataFrameHandler instance has a parent attribute, then
        the parent DataFrame is also updated in the same way.

        Args:
            col (str): Column name to update.
            initial_value (int): Primary key value to update.
            update_value (int): Updated primary key value.

        Returns:
            pd.DataFrame: The DataFrame with updated primary keys.

        """
        if initial_value == update_value:
            return self.df

        logger.debug(
            f"Updating dataframe {col}={initial_value} to {update_value}",
        )
        self.df = self.df.with_columns(
            [
                pl.when(pl.col(col) == initial_value)
                .then(update_value)
                .otherwise(pl.col(col))
                .alias(col),
            ],
        )
        logger.debug(
            "Successfully updated dataframe "
            f"{col}={initial_value} to {update_value}",
        )

        if self.parent is not None and isinstance(
            self.parent,
            TargetDataFrameHandler,
        ):
            self.parent.update_df_value(
                col=col,
                initial_value=initial_value,
                update_value=update_value,
            )

        return self.df

    def get_sub_handler(
        self,
        column: str,
        values: pl.Series | list[Any],
    ) -> "TargetDataFrameHandler":
        """Filter the TargetDataFrameHandler rows by the given column values
        and return a sub TargetDataFrameHandler instance initialised with
        the filtered data.

        Args:
            column (str): The DataFrame column to filter on.
            values (pd.Series | list[Any]): The values to filter on.

        Returns:
            TargetDataFrameHandler: A new TargetDataFrameHandler instance.

        """
        return TargetDataFrameHandler(
            df=self.df.filter(pl.col(column).is_in(values)),
            targets=self.targets,
            parent=self,
        )
