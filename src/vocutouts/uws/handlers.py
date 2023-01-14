"""Handlers for the UWS API to a service.

These handlers should be reusable for any IVOA service that implements UWS.
The user of these handlers must provide an additional handler for POST at the
root of the job list, since that handler has to specify the input parameters
for a job, which will vary by service.

Notes
-----
To use these handlers, include the ``uws_router`` in an appropriate FastAPI
router, generally with a prefix matching the URL root for the async API.  For
example:

.. code-block:: python

   external_router.include_router(uws_router, prefix="/jobs")
"""

from datetime import datetime
from typing import Optional, TypeVar

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import RedirectResponse
from safir.dependencies.gafaelfawr import (
    auth_dependency,
    auth_logger_dependency,
)
from structlog.stdlib import BoundLogger

from .dependencies import UWSFactory, uws_dependency
from .exceptions import ErrorLocation, PermissionDeniedError
from .models import (
    ExecutionPhase,
    Job,
    JobCreate,
    JobDescription,
    JobStart,
    JobUpdate,
)

T = TypeVar("T", bound=JobCreate)

__all__ = ["add_uws_routes"]


def add_uws_routes(
    router: APIRouter,
    *,
    sync_prefix: str,
    async_prefix: str,
    job_model: type[Job],
    job_create_model: type[T],
) -> None:
    """Add the UWS routes to an existing router.

    The routes are defined this way instead of statically in their own router
    because the return types for some routes are dynamically configurable and
    this way should get all the types in the right place for OpenAPI
    generation.

    Parameters
    ----------
    router
        Router to which to attach the routes.
    sync_prefix
        URL prefix under which to put sync routes.
    async_prefix
        URL prefix under which to put async routes.
    job_model
        Type for the job model.
    job_create_model
        Type for the model used to create a job.
    """

    @router.post(
        sync_prefix,
        description=(
            "Synchronously request a cutout. This will wait for the cutout to"
            " be completed and return the resulting image as a FITS file. (The"
            " image will be returned via a redirect to a URL at the underlying"
            " object store.)"
        ),
        response_class=RedirectResponse,
        responses={
            303: {"description": "Redirect to result of successful cutout"},
            400: {
                "description": "Cutout job failed",
                "content": {"text/plain": {}},
            },
        },
        status_code=303,
        summary="Synchronous cutout",
    )
    async def post_sync(
        create: T,
        request: Request,
        user: str = Depends(auth_dependency),
        uws_factory: UWSFactory = Depends(uws_dependency),
        logger: BoundLogger = Depends(auth_logger_dependency),
    ) -> str:
        if create.run_id:
            logger = logger.bind(run_id=create.run_id)
        job_service = uws_factory.create_job_service()
        job = await job_service.create(
            user, create.parameters, run_id=create.run_id
        )
        logger.info(
            "Created job", job_id=job.job_id, params=create.parameters.dict()
        )
        await job_service.start(user, job.job_id)
        logger.info("Started job", job_id=job.job_id)
        return await job_service.get_first_result(user, job.job_id)

    @router.get(
        async_prefix,
        description=(
            "List all existing jobs for the current user. Jobs will be sorted"
            " by creation date, with the most recently created listed first."
        ),
        response_model=list[JobDescription],
        response_model_exclude_none=True,
        summary="Async job list",
    )
    async def get_job_list(
        request: Request,
        phase: Optional[list[ExecutionPhase]] = Query(
            None,
            title="Execution phase",
            description="Limit results to the provided execution phases",
        ),
        after: Optional[datetime] = Query(
            None,
            title="Creation date",
            description="Limit results to jobs created after this date",
        ),
        last: Optional[int] = Query(
            None,
            title="Number of jobs",
            description="Return at most the given number of jobs",
        ),
        user: str = Depends(auth_dependency),
        uws_factory: UWSFactory = Depends(uws_dependency),
    ) -> list[JobDescription]:
        job_service = uws_factory.create_job_service()
        return await job_service.list_jobs(
            user, phases=phase, after=after, count=last
        )

    @router.post(
        async_prefix,
        response_class=RedirectResponse,
        status_code=303,
        summary="Create async job",
    )
    async def create_job(
        request: Request,
        create: T,
        user: str = Depends(auth_dependency),
        uws_factory: UWSFactory = Depends(uws_dependency),
        logger: BoundLogger = Depends(auth_logger_dependency),
    ) -> str:
        if create.run_id:
            logger = logger.bind(run_id=create.run_id)
        job_service = uws_factory.create_job_service()
        job = await job_service.create(
            user, create.parameters, run_id=create.run_id
        )
        logger.info(
            "Created job", job_id=job.job_id, params=create.parameters.dict()
        )
        if create.start:
            await job_service.start(user, job.job_id)
            logger.info("Started job", job_id=job.job_id)
        return request.url_for("get_job", job_id=job.job_id)

    @router.get(
        async_prefix + "/{job_id}",
        response_model=job_model,
        response_model_exclude={"message_id"},
        response_model_exclude_none=True,
        summary="Job details",
    )
    async def get_job(
        job_id: str,
        request: Request,
        wait: int = Query(
            None,
            title="Wait for status changes",
            description=(
                "Maximum number of seconds to wait or -1 to wait for as long"
                " as the server permits"
            ),
        ),
        phase: ExecutionPhase = Query(
            None,
            title="Initial phase for waiting",
            description=(
                "When waiting for status changes, consider this to be the"
                " initial execution phase. If the phase has already changed,"
                " return immediately. This parameter should always be provided"
                " when wait is used."
            ),
        ),
        user: str = Depends(auth_dependency),
        uws_factory: UWSFactory = Depends(uws_dependency),
        logger: BoundLogger = Depends(auth_logger_dependency),
    ) -> Job:
        job_service = uws_factory.create_job_service()
        try:
            return await job_service.get(
                user, job_id, wait=wait, wait_phase=phase
            )
        except PermissionDeniedError as e:
            e.location = ErrorLocation.path
            e.field = "job_id"
            raise

    @router.delete(
        async_prefix + "/{job_id}",
        status_code=204,
        summary="Delete a job",
    )
    async def delete_job(
        job_id: str,
        user: str = Depends(auth_dependency),
        uws_factory: UWSFactory = Depends(uws_dependency),
        logger: BoundLogger = Depends(auth_logger_dependency),
    ) -> None:
        job_service = uws_factory.create_job_service()
        try:
            await job_service.delete(user, job_id)
        except PermissionDeniedError as e:
            e.location = ErrorLocation.path
            e.field = "job_id"
            raise
        logger.info("Deleted job", job_id=job_id)

    @router.patch(
        async_prefix + "/{job_id}",
        status_code=200,
        response_model=job_model,
        response_model_exclude={"message_id"},
        response_model_exclude_none=True,
        summary="Update a job",
    )
    async def patch_job(
        job_id: str,
        update: JobUpdate,
        user: str = Depends(auth_dependency),
        uws_factory: UWSFactory = Depends(uws_dependency),
        logger: BoundLogger = Depends(auth_logger_dependency),
    ) -> Job:
        job_service = uws_factory.create_job_service()
        try:
            await job_service.update(user, job_id, update)
        except PermissionDeniedError as e:
            e.location = ErrorLocation.path
            e.field = "job_id"
            raise
        return await job_service.get(user, job_id)

    @router.post(
        async_prefix + "/{job_id}/start",
        response_class=RedirectResponse,
        status_code=303,
        summary="Start a job",
    )
    async def job_start(
        job_id: str,
        start: JobStart,
        request: Request,
        user: str = Depends(auth_dependency),
        uws_factory: UWSFactory = Depends(uws_dependency),
        logger: BoundLogger = Depends(auth_logger_dependency),
    ) -> str:
        job_service = uws_factory.create_job_service()
        try:
            await job_service.start(user, job_id)
        except PermissionDeniedError as e:
            e.location = ErrorLocation.path
            e.field = "job_id"
            raise
        logger.info("Started job", job_id=job_id)
        return request.url_for("get_job", job_id=job_id)

    # This is deep magic to work around a Pydantic limitation.  Pydantic can't
    # handle TypeVar parameters to routes and instead treats them as
    # equivalent to their bound, which in this case means that it thinks the
    # parameters element of JobCreate is a BaseModel, and thus serializes it
    # to the empty dict.  Work around this by dynamically fixing the type
    # annotation to match the actual job creation model.  This may require
    # changes if type annotations are modified in future versions of Python.
    post_sync.__annotations__["create"] = job_create_model
    create_job.__annotations__["create"] = job_create_model
