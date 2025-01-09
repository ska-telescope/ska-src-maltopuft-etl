"""Intialise application config."""

import datetime as dt
import logging
import sys
from pathlib import Path
from typing import Any

import yaml
from pydantic import (
    BaseModel,
    Field,
    ValidationInfo,
    computed_field,
    field_validator,
)
from pydantic_settings import BaseSettings

logger = logging.getLogger(__name__)


class Settings(BaseSettings):
    """Initialises application settings."""

    # ruff: noqa: D102, N802

    project_path: Path = Path(sys.path[-1]).parent.absolute()

    @computed_field  # type: ignore[prop-decorator]
    @property
    def default_cfg_path(self) -> Path:
        return self.project_path / "cfg" / "config.default.yml"

    @computed_field  # type: ignore[prop-decorator]
    @property
    def cfg_path(self) -> Path:
        return self.project_path / "cfg" / "config.yml"


class Config(BaseModel):
    """Initialises application config."""

    data_path: Path
    output_path: Path
    partition_key: str = ""
    save_output: bool = True

    start_time_str: dt.datetime = Field(
        default_factory=lambda: (
            dt.datetime.now(tz=dt.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        ),
    )

    def __setattr__(self, name: str, value: Any) -> Any:
        """Config attribute setter.

        Appends "_" to the Config.partition_key during updates.
        This simplifies generating file paths with the optional
        Config.partition_key parameter.

        Args:
            name (str): Name of the attribute being updated.
            value (Any): Updated attribute value.

        Returns:
            Any: The updated value.

        """
        if name == "partition_key" and isinstance(value, str):
            value = value + "_"
        super().__setattr__(name, value)

    @field_validator("save_output", mode="after")
    @classmethod
    def create_output_dir(
        cls,
        save_output: bool,  # noqa: FBT001
        values: ValidationInfo,
    ) -> None:
        """Creates the output directory if save_output is True."""
        if save_output:
            output_path = values.data.get("output_path")
            output_path.mkdir(
                parents=True,
                exist_ok=True,
            )

        return save_output

    @computed_field  # type: ignore[prop-decorator]
    @property
    def output_prefix(self) -> Path:
        return f"{self.partition_key}"

    @computed_field  # type: ignore[prop-decorator]
    @property
    def raw_obs_data_path(self) -> Path:
        return self.output_path / f"{self.output_prefix}obs_raw.parquet"

    @computed_field  # type: ignore[prop-decorator]
    @property
    def raw_cand_data_path(self) -> Path:
        return self.output_path / f"{self.output_prefix}cand_raw.parquet"

    @computed_field  # type: ignore[prop-decorator]
    @property
    def transformed_obs_data_path(self) -> Path:
        return (
            self.output_path / f"{self.output_prefix}obs_transformed.parquet"
        )

    @computed_field  # type: ignore[prop-decorator]
    @property
    def transformed_cand_data_path(self) -> Path:
        return (
            self.output_path / f"{self.output_prefix}cand_transformed.parquet"
        )

    @computed_field  # type: ignore[prop-decorator]
    @property
    def inserted_obs_data_path(self) -> Path:
        return (
            self.output_path / f"{self.start_time_str}_{self.partition_key}"
            f"{self.output_prefix}obs_inserted.parquet"
        )

    @computed_field  # type: ignore[prop-decorator]
    @property
    def inserted_cand_data_path(self) -> Path:
        return (
            self.output_path / f"{self.start_time_str}_{self.partition_key}"
            f"{self.output_prefix}cand_inserted.parquet"
        )


settings = Settings()

try:
    with Path.open(settings.cfg_path, "r", encoding="utf-8") as f:
        config = Config(**yaml.safe_load(f))
except FileNotFoundError:
    with Path.open(settings.default_cfg_path, "r", encoding="utf-8") as f:
        config = Config(**yaml.safe_load(f))
