"""Target MALTOPUFT DB schema candidate information."""

from dataclasses import dataclass

from ska_src_maltopuft_backend.app import models

from ska_src_maltopuft_etl.core.target import TargetInformation


@dataclass
class CandidateTargets:
    """MALTOPUFTDB candidate targets."""

    candidate: TargetInformation
    sp_candidate: TargetInformation


candidate_targets = CandidateTargets(
    candidate=TargetInformation(
        model_class=models.Candidate,
        table_prefix="cand.",
        primary_key="candidate_id",
        foreign_keys=["beam_id"],
    ),
    sp_candidate=TargetInformation(
        model_class=models.SPCandidate,
        table_prefix="sp_cand.",
        primary_key="sp_candidate_id",
        foreign_keys=["candidate_id"],
    ),
)
