"""Target MALTOPUFT DB observation metadata schema information."""

from dataclasses import dataclass

from ska_src_maltopuft_backend.app import models

from ska_src_maltopuft_etl.core.target import TargetInformation


@dataclass
class ObservationTargets:
    """MALTOPUFTDB observation metadata targets."""

    schedule_block: TargetInformation
    meerkat_schedule_block: TargetInformation
    coherent_beam_config: TargetInformation
    observation: TargetInformation
    tiling_config: TargetInformation
    host: TargetInformation
    beam: TargetInformation


observation_targets = ObservationTargets(
    schedule_block=TargetInformation(
        model_class=models.ScheduleBlock,
        table_prefix="sb.",
    ),
    meerkat_schedule_block=TargetInformation(
        model_class=models.MeerkatScheduleBlock,
        table_prefix="mk_sb.",
    ),
    coherent_beam_config=TargetInformation(
        model_class=models.CoherentBeamConfig,
        table_prefix="cb.",
    ),
    observation=TargetInformation(
        model_class=models.Observation,
        table_prefix="obs.",
    ),
    tiling_config=TargetInformation(
        model_class=models.TilingConfig,
        table_prefix="tiling.",
    ),
    host=TargetInformation(
        model_class=models.Host,
        table_prefix="host.",
    ),
    beam=TargetInformation(
        model_class=models.Beam,
        table_prefix="beam.",
    ),
)
