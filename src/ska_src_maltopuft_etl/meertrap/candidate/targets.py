"""Target MALTOPUFT DB schema candidate information."""

from ska_src_maltopuft_etl.core import models

candidate_targets = [
    {
        "model_class": models.Candidate,
        "table_prefix": "cand.",
        "primary_key": "candidate_id",
        "foreign_keys": ["beam_id"],
    },
    {
        "model_class": models.SPCandidate,
        "table_prefix": "sp_cand.",
        "primary_key": "sp_candidate_id",
        "foreign_keys": ["candidate_id"],
    },
]
