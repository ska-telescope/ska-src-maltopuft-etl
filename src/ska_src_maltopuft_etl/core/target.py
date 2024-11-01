"""Database target table metadata information."""

from dataclasses import dataclass

from ska_src_maltopuft_backend.core.types import ModelT


@dataclass
class TargetInformation:
    """Class for keeping track of an item in inventory."""

    model_class: ModelT
    table_prefix: str
    primary_key: str
    foreign_keys: list[str]

    def table_name(self) -> str:
        """Return the table name for the model_class attribute."""
        return self.model_class.__table__.name
