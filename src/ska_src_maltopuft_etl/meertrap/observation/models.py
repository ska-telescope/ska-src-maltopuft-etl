"""MeerTRAP run summary Pydantic data models."""

# ruff: noqa: N805
# pylint: disable=E0213

import datetime as dt
import json
from typing import Any

from pydantic import BaseModel, Field, field_serializer, field_validator


class Tiling(BaseModel):
    """Beam tiling configuration.

    Specifies how the telescope's beams are tiled. In general, it's possible
    to have several tiling configurations per observation.
    """

    coordinate_type: str
    epoch: float
    epoch_offset: float
    method: str
    nbeams: int
    overlap: float
    reference_frequency: float
    shape: str
    target: str


class ConfigurationAuthorityTargetRequest(BaseModel):
    """Configuration Authority target request model.

    The Configuration Authority (CA) is responsible for configuring
    the beams to execute an observation.
    """

    beams: list[str]
    tilings: list[Tiling]
    unique_id: str | None = None

    @field_serializer("beams", when_used="always")
    def serialize_beams_to_str(self, beams: list[str]) -> str:
        """Serialises a list of the CA beams into string.

        Serialisation is performed because, in general, a CATargetRequest
        can have 0, 1 or many beams. This causes issues when exporting the
        flattened parsed schema to parquet format, which expects a fixed
        schema.
        """
        return str(beams)

    @field_serializer("tilings", when_used="always")
    def serialize_tilings_to_str(self, tilings: list[Tiling]) -> str:
        """Serialises a list of the CA tiling configurations into string.

        Serialisation is performed because, in general, a CATargetRequest
        can have 0, 1 or many tiling configurations. This causes issues when
        exporting the flattened parsed schema to parquet format, which expects
        a fixed schema.
        """
        return str([t.model_dump() for t in tilings])


class CoherentBeamShape(BaseModel):
    """Configuration for the coherent beam shape."""

    angle: float
    overlap: float
    x: float
    y: float


class Beam(BaseModel):
    """Beam metadata."""

    absnum: int = Field(
        ...,
        description=(
            "The absolute beam number in the observation, where the first "
            "beam in the observation has `absnum`=0 and the final beam in an "
            "observation with N beams has `absnum`=N-1."
        ),
    )
    coherent: bool = Field(
        ...,
        description=(
            "True if the beam is coherent, False if the beam is incoherent."
        ),
    )
    dec_dms: str = Field(
        ...,
        description=(
            "The declination of the beam in units of degrees:minutes:seconds."
        ),
    )
    mc_ip: str = Field(
        ...,
        description=(
            "The IP address of the Meerkat server which is responsible for "
            "processing the given beam."
        ),
    )
    mc_port: int = Field(
        ...,
        description=(
            "The exposed port of the Meerkat server which is repsonsible "
            "for processing the given beam."
        ),
    )
    ra_hms: str = Field(
        ...,
        description=(
            "The right ascension of the beam in units of hours : minutes : "
            "seconds."
        ),
    )
    relnum: int = Field(
        ...,
        description=(
            "The relative beam number in the partition of beams being "
            "processed by a given Meerkat server."
        ),
    )
    source: str = Field(
        ...,
        description="The source being targeted by the beam.",
    )


class BeamConfig(BaseModel):
    """Beam configuration."""

    ca_target_request: ConfigurationAuthorityTargetRequest
    cb_antennas: list[str]
    coherent_beam_shape: CoherentBeamShape
    ib_antennas: list[str]
    host_beams: list[Beam] = Field(..., validation_alias="list")

    @field_serializer("cb_antennas", when_used="always")
    def serialize_cb_antennas_to_str(self, cb_antennas: list[str]) -> str:
        """Serialises a list of the coherent beam antenna names into a string.

        Serialisation is performed because, in general, different
        observations will have different antennas with coherent beams. This
        causes issues when exporting the flattened parsed schema to parquet
        format, which expects a fixed schema.
        """
        return str(cb_antennas)

    @field_serializer("ib_antennas", when_used="always")
    def serialize_ib_antennas_to_str(self, ib_antennas: list[str]) -> str:
        """Serialises a list of the incoherent beam antenna names into a
        string.

        Serialisation is performed because, in general, different
        observations will have different antennas with incoherent beams. This
        causes issues when exporting the flattened parsed schema to parquet
        format, which expects a fixed schema.
        """
        return str(ib_antennas)

    @field_serializer("host_beams", when_used="always")
    def serialize_beams_to_str(self, host_beams: list[Beam]) -> str:
        """Serialises a list of beam metadata into a string.

        Serialisation is performed because, in general, different
        observations will be configured with a different number of beams per
        host. This causes issues when exporting the flattened parsed schema to
        parquet format, which expects a fixed schema.
        """
        return str([b.model_dump() for b in host_beams])


class ObservationData(BaseModel):
    """Observation metadata."""

    bw: float = Field(..., description="The bandwidth of the observation.")
    cfreq: float = Field(
        ...,
        description="The centre frequency of the observation bandwidth.",
    )
    nbeam: int = Field(
        ...,
        description="The total number of beams used in the observation.",
    )
    nbit: int = Field(..., description="The number of bits per")
    nchan: int = Field(
        ...,
        description="The number of channels used in the observation.",
    )
    npol: int = Field(
        ...,
        description="The number of polarisations used in the observation.",
    )
    sync_time: float
    tsamp: float = Field(
        ...,
        description="The sample period of the observation data.",
    )


class Target(BaseModel):
    """Observation metadata for target radio sources."""

    track_start_offset: float
    target: str = Field(..., description="The name of the target source.")
    track_duration: float = Field(
        ...,
        description=(
            "The number of seconds the target was tracked during the "
            "observation."
        ),
    )


class ScheduleBlockData(BaseModel):
    """Meerkat schedule block data."""

    id: int
    id_code: str = Field(
        ...,
        description=(
            "A code which is unique for every Meerkat schedule block."
        ),
    )
    actual_start_time: dt.datetime = Field(
        ...,
        description=(
            "The time the scheduled block of observations started. "
            "This may differ from the requested start time due to delays "
            "in scheduling the schedule block."
        ),
    )
    expected_duration_seconds: int = Field(
        ...,
        description=(
            "The expected duration of the schedule block in seconds. This "
            "value is calulated by summing the `Target.track_duration` from "
            "the list of targets associated with the schedule_block."
        ),
    )
    proposal_id: str = Field(
        ...,
        description="The proposal ID for the Meerkat schedule block.",
    )
    script_profile_config: str | None = Field(
        None,
        description="The command used to initiate the schedule block.",
    )
    targets: list[Target] | None = Field(
        None,
        description="Metadata for the schedule block targets.",
    )

    @field_validator("targets", mode="before")
    def parse_targets_str_as_dict(cls, value: str) -> dict:
        """Serialises a list of schedule block target metadata into a dict
        during model initialisation/validation.

        Serialisation is performed because the list of run summary targets is
        formatted as a string, despite this string containing a JSON object.
        Serialising to a dict therefore allows a Target Pydantic object to be
        instantiated.
        """
        if not isinstance(value, str):
            return []
        return json.loads(value)

    @field_validator("actual_start_time", mode="before")
    def str2datetime(cls, value: str) -> dt.datetime:
        """Serialises the `actual_start_time` string to a `datetime`
        instance.
        """
        return dt.datetime.strptime(value, "%Y-%m-%d %H:%M:%S.%f%z")

    @field_serializer("targets", when_used="always")
    def serialize_targets_to_str(self, targets: list[Target]) -> str:
        """Serialises a list of schedule block target metadata into a JSON
        string.

        Serialisation is performed because, in general, different
        observations will be configured with a different number of targets.
        This causes issues when exporting the flattened parsed schema to
        parquet format, which expects a fixed schema.
        """
        if targets is None:
            return None
        return str([t.model_dump() for t in targets])


class MeertrapRunSummary(BaseModel):
    """The MeerTRAP run summary data model."""

    filename: str = Field(..., description="The run summary filename.")
    beams: BeamConfig = Field(..., description="The beam configuration.")
    observation: ObservationData = Field(
        ...,
        validation_alias="data",
        description="The observation data.",
    )
    search_pipeline: Any = Field(
        ...,
        validation_alias="pipeline",
        description="The single pulse search pipeline configuration.",
    )
    sb: ScheduleBlockData = Field(
        ...,
        validation_alias="sb_details",
        description="Meerkat schedule block data.",
    )
    utc_start: dt.datetime = Field(
        ...,
        description="The observation start time in UTC timezone.",
    )
    utc_stop: dt.datetime | None = Field(
        None,
        description="The observation stop time in UTC timezone.",
    )
    version_info: Any = Field(
        ...,
        description=(
            "The version of the application running on Meerkat servers."
        ),
    )

    @field_validator("search_pipeline", mode="before")
    def parse_search_pipeline_dict_from_str(cls, value: dict[str, Any]) -> str:
        """Serialises the search pipeline dictionary object to a string."""
        return json.dumps(value)

    @field_validator("utc_start", mode="before")
    def str2datetime(cls, value: str) -> dt.datetime:
        """Serialises the `utc_start` string to a `datetime` instance."""
        return dt.datetime.strptime(value, "%Y-%m-%d_%H:%M:%S").replace(
            tzinfo=dt.timezone.utc,  # noqa: UP017
        )

    @field_validator("utc_stop", mode="before")
    def str_or_null2datetime(cls, value: str | None) -> dt.datetime | None:
        """Serialises the `utc_stop` string to a `datetime` instance."""
        if value is None:
            return None
        return dt.datetime.strptime(value, "%Y-%m-%d_%H:%M:%S").replace(
            tzinfo=dt.timezone.utc,  # noqa: UP017
        )

    @field_serializer("search_pipeline", when_used="always")
    def serialize_search_pipeline_to_str(self, search_pipeline: dict) -> str:
        """Serialises the search pipeline dictionary to a string.

        Serialisation is performed because the search pipeline config is not
        being explicitly modelled at this stage but it is still desireable to
        associate this configuration with the output data.
        """
        return str(search_pipeline)


RUN_SUMMARY_FILE_TO_DF_COLUMN_MAP = {
    "filename": "filename",
    "beams.ca_target_request.beams": "beams.beams",
    "beams.ca_target_request.tilings": "beams.tilings",
    "beams.coherent_beam_shape.angle": "cb.angle",
    "beams.coherent_beam_shape.overlap": "cb.fraction_overlap",
    "beams.coherent_beam_shape.x": "cb.x",
    "beams.coherent_beam_shape.y": "cb.y",
    "beams.host_beams": "beams.host_beams",
    "observation.bw": "obs.bw",
    "observation.cfreq": "obs.cfreq",
    "observation.nbit": "obs.nbit",
    "observation.nchan": "obs.em_xel",
    "observation.npol": "obs.pol_xel",
    "observation.tsamp": "obs.t_resolution",
    "sb.id": "mk_sb.meerkat_id",
    "sb.id_code": "mk_sb.meerkat_id_code",
    "sb.actual_start_time": "sb.start_at",
    "sb.expected_duration_seconds": "sb.expected_duration_seconds",
    "sb.proposal_id": "mk_sb.proposal_id",
    "sb.script_profile_config": "sb.script_profile_config",
    "sb.targets": "sb.targets",
    "utc_start": "obs.t_min",
    "utc_stop": "obs.t_max",
}
