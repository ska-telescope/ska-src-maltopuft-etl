"""Merged dagster definitions from all modules."""

from dagster import Definitions

from .atnf import definitions as atnf
from .meertrap import definitions as meertrap

defs = Definitions.merge(atnf.defs, meertrap.defs)
