"""Target MALTOPUFT DB ATNF pulsar catalogue schema information."""

from ska_src_maltopuft_backend.app import models

targets = [
    {
        "model_class": models.Catalogue,
        "table_prefix": "cat.",
        "primary_key": "catalogue_id",
        "foreign_keys": [],
    },
    {
        "model_class": models.CatalogueVisit,
        "table_prefix": "cat_visit.",
        "primary_key": "catalogue_visit_id",
        "foreign_keys": ["catalogue_id"],
    },
    {
        "model_class": models.KnownPulsar,
        "table_prefix": "known_ps.",
        "primary_key": "known_pulsar_id",
        "foreign_keys": ["catalogue_id"],
    },
]
