"""Handlers for the app's external root, ``/api/cutout``.

UWS provides general handlers for everything it can, but POST at the top level
to create a new job has to be provided by the application since only the
application knows the job parameters.
"""

from fastapi import APIRouter, Depends, Request
from safir.metadata import get_metadata

from ..config import config
from ..models.capabilities import Capabilities
from ..models.index import Index
from ..models.parameters import (
    CutoutAsyncJobCreate,
    CutoutJob,
    CutoutJobCreate,
)
from ..uws.dependencies import UWSFactory, uws_dependency
from ..uws.handlers import add_uws_routes
from ..uws.models import Availability

__all__ = ["external_router"]

external_router = APIRouter()
"""FastAPI router for all external handlers."""


@external_router.get(
    "",
    response_model=Index,
    response_model_exclude_none=True,
    summary="Application metadata",
    description=(
        "Metadata about the application, returned by a request for the root"
        " of the external API."
    ),
)
async def get_index() -> Index:
    metadata = get_metadata(
        package_name="ivoa-cutout-poc",
        application_name=config.name,
    )
    return Index(metadata=metadata)


@external_router.get(
    "/availability",
    description="VOSI-availability resource for the image cutout service",
    response_model=Availability,
    response_model_exclude_none=True,
    summary="IVOA service availability",
)
async def get_availability(
    request: Request, uws_factory: UWSFactory = Depends(uws_dependency)
) -> Availability:
    job_service = uws_factory.create_job_service()
    return await job_service.availability()


@external_router.get(
    "/capabilities",
    description="VOSI-capabilities resource for the image cutout service",
    response_model=Capabilities,
    summary="IVOA service capabilities",
)
async def get_capabilities(request: Request) -> Capabilities:
    return Capabilities.parse_obj(
        {
            "availability_url": str(request.url_for("get_availability")),
            "capabilities_url": str(request.url_for("get_capabilities")),
            "soda_sync_url": str(request.url_for("post_sync")),
            "soda_async_url": str(request.url_for("create_job")),
        }
    )


# Add the UWS routes to our external routes.
add_uws_routes(
    external_router,
    sync_prefix="/sync",
    async_prefix="/jobs",
    job_model=CutoutJob,
    job_sync_create_model=CutoutJobCreate,
    job_async_create_model=CutoutAsyncJobCreate,
)
