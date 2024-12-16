"""Functions to transform MeerTRAP ra and dec coordinates."""

import astropy.units as u
from astropy.coordinates import SkyCoord


def format_ms(coord: str) -> str:
    """Format arc minute and arc second string."""
    if coord.find(":") > 0:
        coord = coord.replace(":", "m", 1) + "s"
    else:
        coord += "m00s"
    return coord


def format_ra_hms(ra: str) -> str:
    """Transform ra string from `00:00:00.0` to `00h00m00.0s`."""
    ra = ra.replace(" ", "")
    ra = ra.replace(":", "h", 1)
    return format_ms(coord=ra)


def format_dec_dms(dec: str) -> str:
    """Format dec string from `00:00:00.0` to `00d00m00.0s`."""
    dec = dec.replace(" ", "")
    dec = dec.lstrip("+")
    dec = dec.replace(":", "d", 1)
    return format_ms(coord=dec)


def hms_to_degrees(ra: str, dec: str) -> tuple[float, float]:
    """Convert ra and dec from hms and dms to decimal degrees.

    Decimal degrees are rounded to 5 decimal places to allow
    querying to the nearest arcsecond (1/3600).
    """
    c = SkyCoord(
        # pylint: disable=E1101
        ra=ra,
        dec=dec,
        unit=(u.hourangle, u.deg),
    )
    return round(c.ra.deg, 5), round(c.dec.deg, 5)


def add_parenthesis(s: str) -> str:
    """Wrap a string in parenthesis."""
    return f"({s})"
