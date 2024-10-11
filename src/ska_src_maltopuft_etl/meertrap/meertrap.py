"""Meertrap ETL entrypoint."""

import logging
from pathlib import Path
from typing import Any

import pandas as pd
import polars as pl
import sqlalchemy as sa
from ska_src_maltopuft_backend.core.types import ModelT

from ska_src_maltopuft_etl.core.config import config
from ska_src_maltopuft_etl.core.database import engine
from ska_src_maltopuft_etl.core.exceptions import DuplicateInsertError
from ska_src_maltopuft_etl.core.insert import insert_
from ska_src_maltopuft_etl.database_loader import DatabaseLoader
from ska_src_maltopuft_etl.meertrap.candidate.targets import candidate_targets
from ska_src_maltopuft_etl.meertrap.observation.targets import (
    observation_targets,
)

from .candidate.extract import extract_spccl
from .candidate.transform import transform_spccl
from .observation.extract import extract_observation
from .observation.transform import transform_observation

logger = logging.getLogger(__name__)


def parse_candidate_dir(candidate_dir: Path) -> dict[str, Any]:
    """Parse files in a candidate directory.

    :param candidate_dir: The absolute path to the candidate directory to
        parse.

    :return: A dictionary containing the normalised candidate data.
    """
    run_summary_data, candidate_data = {}, {}
    for file in candidate_dir.iterdir():
        if file.match("*run_summary.json"):
            logger.debug(f"Parsing observation metadata from {file}")
            run_summary_data = extract_observation(filename=file)
            continue
        if file.match("*spccl.log"):
            logger.debug(f"Parsing candidate data from {file}")
            candidate_data = extract_spccl(filename=file)
            continue
        if file.match("*.jpg"):
            continue

        logger.warning(f"Found file {file} in unexpected format.")
    return {**run_summary_data, **candidate_data}


def extract(
    root_path: Path = config.get("data_path", ""),
) -> pl.DataFrame:
    """Extract MeerTRAP data archive from run_summary.json and spccl files.

    :param root_path: The absolute path to the candidate data directory.
    """
    logger.info("Started extract routine")

    cand_df = pl.DataFrame()
    rows: list[dict[str, Any]] = []

    for idx, candidate_dir in enumerate(root_path.iterdir()):
        if idx % 500 == 0:
            logger.info(f"Parsing candidate #{idx} from {candidate_dir}")
        if idx > 0 and (idx % 1000) == 0:
            cand_df = cand_df.vstack(pl.DataFrame(rows))
            rows = []
        if idx > 0 and (idx % 5000) == 0:
            cand_df = cand_df.rechunk()

        if not candidate_dir.is_dir():
            logger.warning(
                f"Unexpected file {candidate_dir} found in "
                f"{config.get('data_path')}",
            )
            continue

        rows.append(parse_candidate_dir(candidate_dir=candidate_dir))

    cand_df = cand_df.vstack(pl.DataFrame(rows))
    logger.info("Extract routine completed successfully")
    return cand_df


def transform(
    df: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Transform MeerTRAP data to MALTOPUFT DB schema.

    Args:
        df (pl.DataFrame): The raw data.

    Returns:
        tuple[pl.DataFrame, pl.DataFrame]: The observation and candidate
        data, respectively.

    """
    obs_pd = transform_observation(df=df)
    obs_df: pl.DataFrame = pl.from_pandas(obs_pd)

    output_path: Path = config.get("output_path", Path())

    obs_df_parquet_path = output_path / "obs_df.parquet"
    logger.info(
        f"Writing transformed observation data to {obs_df_parquet_path}",
    )
    obs_df.write_parquet(obs_df_parquet_path)
    logger.info(
        "Successfully wrote transformed observation data to "
        f"{obs_df_parquet_path}",
    )

    cand_df = transform_spccl(df=df, obs_df=obs_df)
    cand_df_parquet_path = output_path / "cand_df.parquet"
    logger.info(f"Writing transformed cand data to {cand_df_parquet_path}")
    cand_df.write_parquet(cand_df_parquet_path)
    logger.info(
        f"Successfully wrote transformed cand data to {cand_df_parquet_path}",
    )

    return obs_df, cand_df


def base_load(
    conn: sa.Connection,
    obs_df: pd.DataFrame,
    cand_df: pd.DataFrame,
) -> None:
    """The base load function for inserting data into the database.

    Instantiates a DatabaseLoader instance and bulk inserts the observation and
    candidate data into the database.

    Args:
        conn (sa.Connection): A SQLAlchemy connection.
        obs_df (pd.DataFrame): The observation data.
        cand_df (pd.DataFrame): The candidate data.

    """
    db = DatabaseLoader(conn=conn)
    logger.info("Attempting to bulk insert data")
    logger.info("Staging observation data")

    df = obs_df
    for target in observation_targets:
        logger.debug(
            f"Loading data into {target.get('model_class').__table__.name}",
        )
        df = db.load_target(
            df=df,
            target=target,
        )
    logger.info("Observation data staged successfully")

    beam_id_map = db.foreign_keys_map["beam_id"]
    cand_df["beam_id"] = cand_df["beam_id"].map(beam_id_map)

    logger.info("Staging candidate data")
    df = cand_df
    for target in candidate_targets:
        df = db.load_target(df=df, target=target)
    logger.info("Candidate data staged successfully")
    logger.info("Successfully bulk inserted data")


def load(
    obs_df: pd.DataFrame,
    cand_df: pd.DataFrame,
) -> None:
    """Load MeerTRAP data into a database.

    Args:
        obs_df (pd.DataFrame): DataFrame containing observation data.
        cand_df (pd.DataFrame): DataFrame containing candidate data.

    Raises:
        RuntimeError: If there is an unrecoverable error while inserting data.

    """
    with engine.connect() as conn:
        try:
            with conn.begin():
                base_load(conn=conn, obs_df=obs_df, cand_df=cand_df)
        except DuplicateInsertError as exc:
            logger.warning(f"Failed to insert data. {exc}")
            with conn.begin_nested():
                db = DatabaseLoader(conn=conn)
                df, targets = obs_df, observation_targets

                for target in targets:
                    primary_key_name = target.get("primary_key")
                    unique_rows = get_unique_target_rows(
                        target=target,
                        df=df,
                    )

                    for _, row_ in unique_rows.iterrows():
                        row = db.prepare_data_for_insert(
                            df=row_.to_frame().T,
                            target=target,
                        )
                        local_primary_key = get_row_pk(
                            row=row,
                            target=target,
                        )

                        try:
                            attrs = row.drop(
                                columns=[target.get("primary_key")],
                            ).to_dict(orient="records")[0]
                            logger.debug(
                                f"Inserting attributes {attrs} into "
                                f"{target['model_class'].__table__.name}",
                            )
                            with conn.begin_nested():
                                inserted_id: int = insert_(
                                    conn=conn,
                                    model_class=target.get("model_class"),
                                    data=attrs,
                                )[0]
                        except DuplicateInsertError as exc:
                            logger.warning(exc)
                            with conn.begin_nested():
                                db_row = get_by(
                                    conn=conn,
                                    model_class=target["model_class"],
                                    attrs=attrs,
                                )

                            if db_row is None:
                                msg = (
                                    "Can't find a duplicated record with "
                                    f"attributes {attrs}. Exiting."
                                )
                                raise RuntimeError(msg) from exc

                            inserted_id = db_row.id

                        logger.debug(f"Fetched row with id {inserted_id}")

                        if (
                            inserted_id in df[primary_key_name].to_numpy()
                            and inserted_id != local_primary_key
                        ):
                            existing_row = df[
                                df[primary_key_name] == inserted_id
                            ].to_dict(orient="records")
                            logger.warning(
                                f"Inserted key {primary_key_name}="
                                f"{inserted_id} exists in local df "
                                f"at row {existing_row}",
                            )

                            swap_key = df[primary_key_name].max() + 1
                            df = update_df_value(
                                df=df,
                                col=primary_key_name,
                                initial_value=inserted_id,
                                update_value=swap_key,
                            )
                            db.update_foreign_keys_map(
                                primary_key_name=primary_key_name,
                                initial_value=inserted_id,
                                update_value=swap_key,
                            )


                        df = update_df_value(
                            df=df,
                            col=primary_key_name,
                            initial_value=local_primary_key,
                            update_value=inserted_id,
                        )
                        db.update_foreign_keys_map(
                            primary_key_name=primary_key_name,
                            initial_value=local_primary_key,
                            update_value=inserted_id,
                        )

            conn.commit()


def get_by(
    model_class: ModelT,
    attrs: dict[str, Any],
    conn: sa.Connection,
) -> sa.Row[ModelT] | None:
    """Fetch a row from a table by attributes.

    Args:
        model_class (dict[str, Any]): The SQLAlchemy model class for the
        table.
        attrs (dict[str, Any]): The attributes to filter by.
        conn (sa.Connection): A SQLAlchemy connection.

    Returns:
        sa.Row[ModelT] | None: The fetched row or None if row not found.

    """
    logger.debug(
        f"Fetching row from table {model_class.__table__.name} "
        f"with attributes {attrs}",
    )
    stmt = sa.select(model_class)
    for k, v in attrs.items():
        stmt = stmt.where(getattr(model_class, k) == v)

    logger.debug(stmt)
    res = conn.execute(stmt).fetchone()
    logger.debug(f"Fetched {model_class.__table__.name}: {res}")
    return res


def get_unique_target_rows(
    target: list[dict[str, Any]],
    df: pd.DataFrame,
) -> pd.DataFrame:
    """Return the unique rows for a target table.

    Args:
        target (list[dict[str, Any]]): The target table details.
        df (pd.DataFrame): The input DataFrame.

    Returns:
        pd.DataFrame: The unique rows for the target table.

    """
    logger.debug(
        f"Getting unique rows by distinct {target.get('primary_key')} values",
    )
    unique_rows_df = df.drop_duplicates(subset=[target.get("primary_key")])
    logger.info(
        f"Found {len(unique_rows_df)} unique rows for "
        f"{target.get('model_class').__table__.name} table",
    )
    return unique_rows_df


def get_row_pk(row: pd.DataFrame, target: dict[str, Any]) -> int:
    """Get the primary key of a row in a DataFrame.

    Args:
        row (pd.DataFrame): The row attributes to extract the primary key for.
        target (dict[str, Any]): The target table details.

    Returns:
        int: The primary key of the row.

    """
    pk_name = target["primary_key"]
    return row[pk_name].to_numpy()[0]


def update_df_value(
    df: pd.DataFrame,
    col: str,
    initial_value: int,
    update_value: int,
) -> pd.DataFrame:
    """Update local references of a column in a DataFrame.

    Updates all cells in the Pandas DataFrame where the column value is equal
    to initial_value to the update_value.

    If a DatabaseLoader instance is provided, the foreign_keys_map attribute
    is also updated to include the new primary key mapping.

    Args:
        df (pd.DataFrame): Input DataFrame.
        col (str): Column name to update.
        initial_value (int): Primary key value to update.
        update_value (int): Updated primary key value.

    Returns:
        pd.DataFrame: The DataFrame with updated primary keys.

    """

    logger.info(
        f"Updating local references {col}={initial_value} "
        f"to {update_value}",
    )
    df.loc[df[col] == initial_value, [col]] = (
        update_value
    )
    logger.debug(
        "Successfully updated local references of "
        f"{col}={initial_value} to {update_value}",
    )

    return df
