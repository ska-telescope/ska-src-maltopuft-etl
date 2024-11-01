"""Initialise the MALTOPUFTDB config."""

from ska_src_maltopuft_backend.core.config import settings
from sqlalchemy import create_engine

engine = create_engine(
    url=str(settings.MALTOPUFT_POSTGRES_URI),
)
