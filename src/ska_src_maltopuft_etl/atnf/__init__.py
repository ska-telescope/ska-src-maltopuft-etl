"""ATNF to MALTOPUFTDB ETL."""

import logging

from ska_ser_logging import configure_logging

configure_logging(logging.INFO)
logger = logging.getLogger(__name__)
