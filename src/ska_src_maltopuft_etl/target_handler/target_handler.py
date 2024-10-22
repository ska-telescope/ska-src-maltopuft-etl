"""Handles DataFrame operations while loading transformed Observation
metadata and Candidate data into MALTOPUFTDB target tables.
"""

import logging
from typing import Any

import pandas as pd

from ska_src_maltopuft_etl.core.target import TargetInformation

logger = logging.getLogger(__name__)


class TargetDataFrameHandler:
    """TargetDataFrameHandler class module."""

    def __init__(  # noqa: D107, ANN204
        self,
        df: pd.DataFrame,
        targets: list[TargetInformation],
        parent=None,  # noqa: ANN001, "TargetDataFrameHandler" | None
    ):
        self.df = df.copy()
        self.targets = targets
        self.parent = parent

    def unique_rows(
        self,
        target: TargetInformation,
    ) -> pd.DataFrame:
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
        unique_rows = self.df.drop_duplicates(
            subset=[target.primary_key],
        )
        logger.info(
            f"Found {len(unique_rows)} unique rows for "
            f"{target.table_name} table",
        )
        logger.debug(
            f"Unique {target.table_name} "
            f"rows are {unique_rows.to_dict(orient='records')}",
        )
        return unique_rows

    def primary_keys(
        self,
        target: TargetInformation,
        row: pd.DataFrame | None = None,
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
            primary_keys = self.df[target.primary_key].unique().tolist()

        logger.debug(
            f"Dataframe primary keys {target.primary_key}={primary_keys}",
        )
        return primary_keys

    def update_df_value(
        self,
        col: str,
        initial_value: int,
        update_value: int,
    ) -> pd.DataFrame:
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
        logger.debug(
            f"Updating dataframe {col}={initial_value} to {update_value}",
        )
        self.df.loc[self.df[col] == initial_value, [col]] = update_value
        logger.debug(
            "Successfully updated dataframe "
            f"{col}={initial_value} to {update_value}",
        )

        if self.parent is not None:
            self.parent.update_df_value(
                col=col,
                initial_value=initial_value,
                update_value=update_value,
            )
        return self.df

    def get_sub_handler(
        self,
        column: str,
        values: pd.Series | list[Any],
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
        _subdf = self.df[self.df[column].isin(values)].copy()
        return TargetDataFrameHandler(
            df=_subdf,
            targets=self.targets,
            parent=self,
        )
