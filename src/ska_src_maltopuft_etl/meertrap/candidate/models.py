"""MeerTRAP candidate Pydantic data models."""

from pydantic import BaseModel, Field


class MeertrapSpccl(BaseModel):
    """The MeerTRAP candidate data model."""

    candidate: str = Field(
        ...,
        description="The name of the candidate data archive.",
    )
    filename: str = Field(..., description="The spccl filename.")
    mjd: float = Field(
        ...,
        description=(
            "Time of pulse observation in Modified Julian Date time format."
        ),
    )
    dm: float = Field(
        ...,
        description=(
            "The best-fit dispersion measure of the candidate from the single "
            "pulse search pipeline."
        ),
    )
    width: float = Field(
        ...,
        description=(
            "The measured full-width half maxima (FWHM) of the pulse in the "
            "single pulse search pipeline."
        ),
    )
    snr: float = Field(..., description="Pulse signal to noise ratio.")
    beam: int = Field(
        ...,
        description=(
            "The absolute beam number of the beam in which the candidate was "
            "detected."
        ),
    )
    beam_mode: str = Field(
        ...,
        description=(
            "'C' ('I') if the beam in which the candidate was detected was "
            "Coherent (Incoherent)."
        ),
    )
    ra: str = Field(
        ...,
        description=(
            "The right ascension of the detected candidate in "
            "hours:minutes:seconds."
        ),
    )
    dec: str = Field(
        ...,
        description=(
            "The declination of the detected candidate in "
            "degrees:minutes:seconds."
        ),
    )
    label: int = Field(
        ...,
        description=(
            "The (binary) ML classification of the candidate. "
            "0 for RFI and 1 for single pulse."
        ),
    )
    probability: float = Field(
        ...,
        description=(
            "The (binary) ML classifier probability that the candidate is "
            "a single pulse."
        ),
    )
    fil_file: str = Field(
        ...,
        description="The name of the candidate filterbank data file.",
    )
    plot_file: str = Field(
        ...,
        description="The name of the candidate diagnostic subplot file.",
    )
