"""Target MALTOPUFT DB ATNF pulsar catalogue schema information."""

from ska_src_maltopuft_backend.app import models

from ska_src_maltopuft_etl.core.target import TargetInformation

targets = [
    TargetInformation(
        model_class=models.Catalogue,
        table_prefix="cat.",
    ),
    TargetInformation(
        model_class=models.CatalogueVisit,
        table_prefix="cat_visit.",
    ),
    TargetInformation(
        model_class=models.KnownPulsar,
        table_prefix="known_ps.",
    ),
]
