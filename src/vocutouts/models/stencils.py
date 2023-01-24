"""Parsing and representation of stencil parameters."""

from __future__ import annotations

from abc import ABCMeta
from typing import Literal

from pydantic import BaseModel, Field, validator


class Point(BaseModel):
    """Represents a point in the sky."""

    ra: float = Field(..., title="ICRS ra in degrees")

    dec: float = Field(..., title="ICRS dec in degrees")


class Range(BaseModel):
    """Represents a range of values."""

    min: float = Field(..., title="Minimum value")

    max: float = Field(..., title="Maximum value")


class Stencil(BaseModel, metaclass=ABCMeta):
    """Base class for a stencil parameter."""

    type: str = Field(..., title="Type of stencil")


class CircleStencil(Stencil):
    """Represents a circular stencil."""

    type: Literal["circle"] = "circle"

    center: Point = Field(..., title="Center of circle")

    radius: float = Field(..., title="Radius of circle")


class PolygonStencil(Stencil):
    """Represents a polygon stencil."""

    type: Literal["polygon"] = "polygon"

    vertices: list[Point] = Field(
        ...,
        title="Vertices of polygon",
        description=(
            "Polygon winding must be counter-clockwise when viewed from the"
            " origin towards the sky."
        ),
    )

    @validator("vertices")
    def _at_least_three(cls, v: list[Point]) -> list[Point]:
        """Ensure there are at least three vertices."""
        if len(v) < 3:
            raise ValueError("Polygon must have at least three vertices")
        return v


class RangeStencil(Stencil):
    """Represents a range of ra and dec values."""

    type: Literal["range"] = "range"

    ra: Range = Field(..., title="Range of ICRS ra values")

    dec: Range = Field(..., title="Range of ICRS dec values")
