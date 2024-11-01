"""Custom exceptions."""


class MaltopuftETLError(Exception):
    """Base class for all exceptions raised by this package."""


class UnexpectedShapeError(MaltopuftETLError):
    """Raised if a dataframe doesn't have the expected shape."""


class DuplicateInsertError(MaltopuftETLError):
    """Raised when a duplicate insert error occurs."""


class ForeignKeyError(MaltopuftETLError):
    """Raised when a foreign key constraint is violated."""
