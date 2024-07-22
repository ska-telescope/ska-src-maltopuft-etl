"""ska-src-maltopuft-etl entrypoint."""

import logging

from ska_ser_logging import configure_logging

from ska_src_maltopuft_etl.meertrap.meertrap import extract

if __name__ == "__main__":
    configure_logging(logging.INFO)
    logger = logging.getLogger(__name__)

    extract()
