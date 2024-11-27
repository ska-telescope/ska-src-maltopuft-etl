"""Meertrap load routine."""

import logging

import pandas as pd
from ska_src_maltopuft_backend.observation.models import Observation

from ska_src_maltopuft_etl.core.exceptions import (
    DuplicateInsertError,
    ForeignKeyError,
)
from ska_src_maltopuft_etl.core.target import TargetInformation
from ska_src_maltopuft_etl.database_loader import DatabaseLoader
from ska_src_maltopuft_etl.meertrap.candidate.targets import candidate_targets
from ska_src_maltopuft_etl.meertrap.observation.targets import (
    observation_targets,
)
from ska_src_maltopuft_etl.target_handler.target_handler import (
    TargetDataFrameHandler,
)

logger = logging.getLogger(__name__)


def bulk_insert_schedule_block(
    db: DatabaseLoader,
    obs_df: pd.DataFrame,
    cand_df: pd.DataFrame,
) -> None:
    """The base load function for inserting data into the database.

    Instantiates a DatabaseLoader instance and bulk inserts the observation and
    candidate data into the database.

    Args:
        db (DatabaseLoader): A DatabaseLoader instance.
        obs_df (pd.DataFrame): The observation data.
        cand_df (pd.DataFrame): The candidate data.

    """
    logger.info("Attempting to bulk insert data")
    logger.info("Staging observation data")

    df = obs_df
    for target in observation_targets:
        logger.debug(
            f"Loading data into {target.model_class.__table__.name}",
        )
        df = db.insert_target(
            df=df,
            target=target,
        )
    logger.info("Observation data staged successfully")

    cand_df["beam_id"] = cand_df["beam_id"].map(
        {v[0]: v[1] for v in db.foreign_keys_map["beam_id"]},
    )

    logger.info("Staging candidate data")
    df = cand_df
    for target in candidate_targets:
        df = db.insert_target(df=df, target=target)
    logger.info("Candidate data staged successfully")
    logger.info("Successfully bulk inserted data")


def insert_observation_by_row(
    obs_handler: TargetDataFrameHandler,
    cand_handler: TargetDataFrameHandler,
    obs_id: int,
    db: DatabaseLoader,
) -> None:
    """Insert all data for an observation and its associated candidates.

    Args:
        obs_handler (TargetDataFrameHandler): Observation DataFrame handler.
        cand_handler (TargetDataFrameHandler): Candidate DataFrame handler.
        obs_id (int): The value of `observation_id` in the Observation
        DataFrame to load.
        db (DatabaseLoader): A DatabaseLoader instance.

    """
    # Get sub handlers which only contain the rows for the observation
    sub_obs_handler = obs_handler.get_sub_handler(
        column="observation_id",
        values=[obs_id],
    )
    sub_cand_handler = cand_handler.get_sub_handler(
        column="beam_id",
        values=sub_obs_handler.df["beam_id"],
    )

    for target in observation_targets:
        handler_insert_target(
            handler=sub_obs_handler,
            target=target,
            db=db,
        )

    # Update the beam_id in the candidate DataFrame to the DB primary keys
    for initial, updated in db.foreign_keys_map["beam_id"]:
        sub_cand_handler.update_df_value(
            col="beam_id",
            initial_value=initial,
            update_value=updated,
        )

    for target in candidate_targets:
        handler_insert_target(
            handler=sub_cand_handler,
            target=target,
            db=db,
        )


def handler_insert_target(
    handler: TargetDataFrameHandler,
    target: TargetInformation,
    db: DatabaseLoader,
) -> TargetDataFrameHandler:
    """Load unique rows for the given target in to a MALTOPUFTDB instance.

    If the row already exists in the database, the primary key is fetched and
    the TargetDataFrameHandlers are updated with the fethed primary key.

    Args:
        parent_handler (TargetDataFrameHandler): _description_
        handler (TargetDataFrameHandler): _description_
        target (dict[str, Any]): Insertion target table details.
        db (DatabaseLoader): A DatabaseLoader instance.

    Raises:
        RuntimeError: If an unrecoverable error occurs while inserting data.

    Returns:
        pd.DataFrame: The DataFrame updated with database primary keys.

    """
    primary_key_name = target.primary_key
    unique_rows = handler.unique_rows(
        target=target,
    )

    for df_idx in unique_rows.index:
        row = handler.df.loc[df_idx]
        row = db.prepare_data_for_insert(
            df=row.to_frame().T,
            target=target,
        )
        attrs = row.drop(
            columns=[primary_key_name],
        )

        try:
            db_id = db.insert_row(
                target=target,
                data=attrs.to_dict(orient="records")[0],
                transactional=True,
            )
        except (DuplicateInsertError, ForeignKeyError) as exc:
            logger.warning(exc)

            # Observation.t_max is estimated during data transformations
            # As a result, we can end up with different t_max values for
            # the same observation record. This can happen if the dataset
            # being processed has changed (for example, new candidates
            # added). Therefore, we can't reliably use t_max to identify
            # duplicate observations so it is dropped from the attributes
            # used to fetch duplicate rows.
            if target.model_class == Observation:
                attrs = attrs.drop(
                    columns=["t_max"],
                )

            db_row = db.get_by(
                model_class=target.model_class,
                attributes=attrs.to_dict(orient="records")[0],
            )

            if db_row is None:
                msg = (
                    "Can't find a duplicated record with "
                    f"attributes {attrs.to_dict(orient='records')}. "
                    "This is a bug in the load method. Exiting without "
                    "commiting records."
                )
                raise RuntimeError(msg) from exc

            db_id = db_row.id

        local_primary_key = handler.primary_keys(
            target=target,
            row=row,
        )[0]

        if (
            db_id in handler.parent.primary_keys(target=target)
            and db_id != local_primary_key
        ):
            logger.warning(
                f"Inserted key {primary_key_name}="
                f"{db_id} exists in local df. Swapping keys.",
            )

            swap_key = (
                max(
                    db.count(target=target),
                    handler.parent.df[primary_key_name].max(),
                )
                + 1
            )

            handler.update_df_value(
                col=primary_key_name,
                initial_value=db_id,
                update_value=swap_key,
            )
            db.update_foreign_keys_map(
                key_name=primary_key_name,
                initial_value=db_id,
                update_value=swap_key,
            )

        handler.update_df_value(
            col=primary_key_name,
            initial_value=local_primary_key,
            update_value=db_id,
        )
        db.update_foreign_keys_map(
            key_name=primary_key_name,
            initial_value=local_primary_key,
            update_value=db_id,
        )

    return handler
