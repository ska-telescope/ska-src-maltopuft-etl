"""Database target table metadata information."""

from dataclasses import dataclass

from ska_src_maltopuft_backend.core.custom_types import ModelT
from sqlalchemy import inspect

from .database import engine


@dataclass(frozen=True)
class TargetInformation:
    """Class for keeping track of an item in inventory."""

    model_class: ModelT
    table_prefix: str

    @property
    def table_name(self) -> str:
        """Return the table name for the model_class attribute."""
        return self.model_class.__table__.name

    @property
    def unique_constraint(self) -> str | None:
        """The target table unique constraint name."""
        with engine.connect() as conn:
            ucs = inspect(conn).get_unique_constraints(self.table_name)

        if len(ucs) > 0:
            # We are only interested in `UniqueConstraint`s here, not
            # `UniqueIndex`es. Only one UniqueConstraint can be declared per
            # table. If there's more than one element in ucs then we know
            # each is a `UniqueIndex`.
            return None

        return ucs[0].get("name")

    @property
    def primary_key(self) -> str:
        """DataFrame column name for the target table primary key."""
        return f"{self.table_name}_id"

    @property
    def foreign_keys(self) -> list[str]:
        """List of foreign keys for the target table."""
        with engine.connect() as conn:
            foreign_keys = inspect(conn).get_foreign_keys(self.table_name)

        if len(foreign_keys) == 0:
            return []

        return [
            # There are no composite primary keys in MALTOPUFTDB schema,
            # so we can just access the 0th element.
            fk.get("constrained_columns", [])[0]
            for fk in foreign_keys
        ]
