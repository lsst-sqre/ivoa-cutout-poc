"""Models for capabilities response."""

from __future__ import annotations

from pydantic import BaseModel, Field, HttpUrl


class Capabilities(BaseModel):
    """Capabilities for the SODA service.

    This is only a proof of concept.  A real JSON-based capability system
    would include more metadata, such as URIs for specific implemented
    standards.  The key point this demonstrates is the model representation
    and the ease of writing the handler, not the specific details of the
    resulting JSON.
    """

    availability_url: HttpUrl = Field(..., title="Availability endpoint")

    capabilities_url: HttpUrl = Field(..., title="Capabilities endpoint")

    soda_sync_url: HttpUrl = Field(..., title="SODA cutout sync URL")

    soda_async_url: HttpUrl = Field(..., title="SODA cutout async URL")
