"""Functions to transform MeerTRAP ra and dec coordinates."""

import astropy.units as u
from astropy.coordinates import SkyCoord


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
