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
        primary_key="schedule_block_id",
        foreign_keys=[],
    ),
    meerkat_schedule_block=TargetInformation(
        model_class=models.MeerkatScheduleBlock,
        table_prefix="mk_sb.",
        primary_key="meerkat_schedule_block_id",
        foreign_keys=["schedule_block_id"],
    ),
    coherent_beam_config=TargetInformation(
        model_class=models.CoherentBeamConfig,
        table_prefix="cb.",
        primary_key="coherent_beam_config_id",
        foreign_keys=[],
    ),
    observation=TargetInformation(
        model_class=models.Observation,
        table_prefix="obs.",
        primary_key="observation_id",
        foreign_keys=["coherent_beam_config_id", "schedule_block_id"],
    ),
    tiling_config=TargetInformation(
        model_class=models.TilingConfig,
        table_prefix="tiling.",
        primary_key="tiling_config_id",
        foreign_keys=["observation_id"],
    ),
    host=TargetInformation(
        model_class=models.Host,
        table_prefix="host.",
        primary_key="host_id",
        foreign_keys=[],
    ),
    beam=TargetInformation(
        model_class=models.Beam,
        table_prefix="beam.",
        primary_key="beam_id",
        foreign_keys=["observation_id", "host_id"],
    ),
)
