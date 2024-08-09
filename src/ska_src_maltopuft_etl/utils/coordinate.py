"""Functions to transform MeerTRAP ra and dec coordinates."""


def format_ms(coord: str) -> str:
    """Format arc minute and arc second string."""
    if coord.find(".") > 0:
        coord = coord[: coord.find(".")]
    if coord.find(":") > 0:
        coord = coord.replace(":", "m", 1) + "s"
    else:
        coord += "m00s"
    return coord


def format_ra_hms(ra: str) -> str:
    """Transform ra string from `00:00:00.0` to `00h00m00.0s`."""
    ra = ra.replace(":", "h", 1)
    return format_ms(coord=ra)


def format_dec_dms(dec: str) -> str:
    """Format dec string from `00:00:00.0` to `00d00m00.0s`."""
    dec = dec.lstrip("+")
    dec = dec.replace(":", "d", 1)
    return format_ms(coord=dec)


def add_parenthesis(s: str) -> str:
    """Wrap a string in parenthesis."""
    return f"({s})"
