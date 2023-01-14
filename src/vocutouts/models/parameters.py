"""Representation of request parameters for cutouts."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, validator

from ..uws.models import Job, JobCreate
from .stencils import CircleStencil, PolygonStencil, RangeStencil


class CutoutParameters(BaseModel):
    """The parameters to a cutout request."""

    ids: list[str] = Field(..., title="Dataset IDs on which to operate")

    stencils: list[CircleStencil | PolygonStencil | RangeStencil] = Field(
        ..., title="The cutout stencils to apply"
    )

    @validator("ids", "stencils")
    def _nonempty(cls, v: list[Any]) -> list[Any]:
        """Ensure a list is non-empty."""
        if len(v) < 1:
            raise ValueError("list must be non-empty")
        return v


class CutoutJob(Job):
    """The corresponding job model."""

    parameters: CutoutParameters = Field(..., title="Job parameters")


class CutoutJobCreate(JobCreate):
    """The corresponding job creation model."""

    parameters: CutoutParameters = Field(..., title="Job parameters")
