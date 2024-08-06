"""Functions to transform MeerTRAP ra and dec coordinates."""


def transform_ra_hms(ra: str) -> str:
    """Transform ra string from `00:00:00.0` to `00h00m00.0s` format."""
    return ra.replace(":", "h", 1).replace(":", "m", 1) + "s"


def transform_dec_dms(dec: str) -> str:
    """Transform dec string from `00:00:00.0` to `00d00m00.0s` format."""
    return dec.replace(":", "d", 1).replace(":", "m", 1) + "s"
