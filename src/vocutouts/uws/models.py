"""Models for UWS services.

See https://www.ivoa.net/documents/UWS/20161024/REC-UWS-1.1-20161024.html.
Descriptive language here is paraphrased from this standard.
"""

from datetime import datetime, timedelta
from enum import Enum
from typing import Generic, Literal, Optional, TypeVar

from pydantic import BaseModel, Field, validator
from pydantic.generics import GenericModel

from .utils import isodatetime, validate_isodatetime

T = TypeVar("T", bound=BaseModel)


class Availability(BaseModel):
    """Availability information (from VOSI)."""

    available: bool = Field(
        ...,
        title="Whether the service appears to be available",
        example=False,
    )

    up_since: Optional[datetime] = Field(
        None,
        title="Time service last became available",
        example="2023-01-12T14:52:45Z",
    )

    down_at: Optional[datetime] = Field(
        None,
        title="Time of next scheduled downtime",
        example="2023-02-25T13:00:00Z",
    )

    back_at: Optional[datetime] = Field(
        None,
        title="Time service will become available after downtime",
        example="2023-02-25T17:00:00Z",
    )

    note: Optional[str] = Field(
        None,
        title="Supplemental information",
        description="Usually empty unless the service is not available.",
        example="Database not available",
    )

    class Config:
        json_encoders = {datetime: lambda v: isodatetime(v)}


class ExecutionPhase(Enum):
    """Possible execution phases for a UWS job."""

    PENDING = "pending"
    """Accepted by the service but not yet sent for execution."""

    QUEUED = "queued"
    """Sent for execution but not yet started."""

    EXECUTING = "executing"
    """Currently in progress."""

    COMPLETED = "completed"
    """Completed and the results are available for retrieval."""

    ERROR = "error"
    """Failed and reported an error."""

    ABORTED = "aborted"
    """Aborted before it completed."""

    UNKNOWN = "unknown"
    """In an unknown state."""

    HELD = "held"
    """Similar to PENDING, held and not sent for execution."""

    SUSPENDED = "suspended"
    """Execution has started, is currently suspended, and will be resumed."""

    ARCHIVED = "archived"
    """Execution completed some time ago and the results have been deleted."""


ACTIVE_PHASES = (
    ExecutionPhase.PENDING,
    ExecutionPhase.QUEUED,
    ExecutionPhase.EXECUTING,
)
"""Phases in which the job is active and can be waited on."""


class JobError(BaseModel):
    """Failure information about a job."""

    error_code: str = Field(
        ..., title="Code for the error", example="permission_denied"
    )

    message: str = Field(
        ..., title="Brief error message", example="Permission denied"
    )

    detail: Optional[str] = Field(
        None,
        title="Extended error message",
        example="No access to backend service",
    )

    class Config:
        orm_mode = True


class JobResult(BaseModel):
    """A single result from the job."""

    result_id: str = Field(
        ..., title="Identifier for the result", example="cutout"
    )

    url: str = Field(
        ...,
        title="URL for the result",
        description=(
            "User-facing URL that can be retrieved directly by the user. This"
            " may be a signed URL or similar temporary-use URL that is"
            " different from a persistent internal URL."
        ),
    )

    size: Optional[int] = Field(
        None, title="Size of the result in bytes", example=517135
    )

    mime_type: Optional[str] = Field(
        None, title="MIME type of the result", example="application/fits"
    )

    class Config:
        orm_mode = True


class JobDescription(BaseModel):
    """Brief job description used for the job list."""

    job_id: str = Field(..., title="Unique identifier", example="1478")

    owner: str = Field(..., title="Identity of job owner", example="rra")

    phase: ExecutionPhase = Field(
        ..., title="Current execution phase", example=ExecutionPhase.EXECUTING
    )

    run_id: Optional[str] = Field(
        None,
        title="Opaque string provided by client",
        description=(
            "This field is intended for the client to add a unique identifier"
            " to all jobs that are part of a single operation from the"
            " perspective of the client. This may aid in tracing issues"
            " through a complex system, or identifying which operation a job"
            " is part of."
        ),
        example="processing-run-40",
    )

    creation_time: datetime = Field(
        ..., title="When the job was created", example="2023-01-13T14:53:00Z"
    )

    class Config:
        json_encoders = {
            datetime: lambda v: isodatetime(v),
            timedelta: lambda v: int(v.total_seconds()),
        }
        orm_mode = True


class Job(JobDescription, GenericModel, Generic[T]):
    """Represents a single UWS job.

    Notes
    -----
    Unfortunately, while Pydantic correctly handles the generic
    parameterization here, FastAPI does not.  If one returns a ``Job[T]``
    directly from a route handler, Pydantic omits ``parameters`` entirely from
    the serialization.  Users of the UWS library therefore must define a
    subclass of this class that redeclares ``parameters`` with a concrete
    class inheriting from ``pydantic.BaseModel``, and then pass that derived
    class into `~vocutouts.uws.handlers.add_uws_routes`.
    """

    message_id: Optional[str] = Field(
        None,
        title="Internal message identifier",
        description=(
            "Used by the work queuing system and not included in user-facing"
            " output."
        ),
    )

    start_time: Optional[datetime] = Field(
        None,
        title="When the job started executing",
        example="2023-01-13T14:55:12Z",
    )

    end_time: Optional[datetime] = Field(
        None,
        title="When the job stopped executing",
        example="2023-01-13T15:34:14Z",
    )

    destruction_time: Optional[datetime] = Field(
        None,
        title="Time at which job should be destroyed",
        description=(
            "At this time, the job will be aborted if it is still running,"
            " its results will be deleted, and all record of the job will"
            " be discarded."
        ),
        example="2023-02-13T14:53:00Z",
    )

    execution_duration: Optional[timedelta] = Field(
        None,
        title="Allowed maximum execution duration in seconds",
        description=(
            "Specified in elapsed wall clock time. If not present, there is"
            " no limit. If the job runs for longer than this time period,"
            " it will be aborted."
        ),
        example=60 * 60 * 10,
    )

    quote: Optional[datetime] = Field(
        None,
        title="Expected completion time if started now",
        description=(
            "If not given, the expected duration of the job is not known."
            " If later than the destruction time, the job is not possible"
            " due to resource constraints."
        ),
        example="2023-02-13T14:53:00Z",
    )

    error: Optional[JobError] = Field(None, title="Error information")

    parameters: T = Field(..., title="Parameters of the job")

    results: Optional[list[JobResult]] = Field(
        None, title="Results of the job"
    )


class JobCreate(GenericModel, Generic[T]):
    """Information required to create a new job.

    Notes
    -----
    As with `Job`, users of the UWS library therefore must define a subclass
    of this class that redeclares ``parameters`` with a concrete class
    inheriting from ``pydantic.BaseModel``, and then pass that derived class
    into `~vocutouts.uws.handlers.add_uws_routes`.
    """

    parameters: T = Field(..., title="Parameters of the job")

    run_id: Optional[str] = Field(
        None,
        title="Opaque string provided by client",
        description=(
            "This field is intended for the client to add a unique identifier"
            " to all jobs that are part of a single operation from the"
            " perspective of the client. This may aid in tracing issues"
            " through a complex system, or identifying which operation a job"
            " is part of."
        ),
        example="processing-run-40",
    )


class AsyncJobCreate(GenericModel, Generic[T]):
    """Information required to create a new async job.

    Notes
    -----
    As with `JobCreate`, users of the UWS library therefore must define a
    subclass of this class that redeclares ``parameters`` with a concrete
    class inheriting from ``pydantic.BaseModel``, and then pass that derived
    class into `~vocutouts.uws.handlers.add_uws_routes`.
    """

    parameters: T = Field(..., title="Parameters of the job")

    start: bool = Field(
        False,
        title="Automatically start job",
    )

    run_id: Optional[str] = Field(
        None,
        title="Opaque string provided by client",
        description=(
            "This field is intended for the client to add a unique identifier"
            " to all jobs that are part of a single operation from the"
            " perspective of the client. This may aid in tracing issues"
            " through a complex system, or identifying which operation a job"
            " is part of."
        ),
        example="processing-run-40",
    )


class JobStart(BaseModel):
    """Body for route to start a job.

    Notes
    -----
    This model is required only to force the input to be JSON, and thus force
    a CORS check, preventing CSRF that would otherwise be possible with a
    bodyless POST with any content type.  It contains no semantic content.
    """

    start: Literal[True] = Field(..., title="Must be true")


class JobUpdate(BaseModel):
    """Requested update to a job.

    This represents only the fields of a `Job` that can be changed after job
    creation and can be provided to the PATCH route.
    """

    destruction_time: Optional[datetime] = Field(
        None,
        title="Time at which job should be destroyed",
        description=(
            "At this time, the job will be aborted if it is still running,"
            " its results will be deleted, and all record of the job will"
            " be discarded."
        ),
        example="2023-02-13T14:53:00Z",
    )

    execution_duration: Optional[timedelta] = Field(
        None,
        title="Allowed maximum execution duration in seconds",
        description=(
            "Specified in elapsed wall clock time. If not present, there is"
            " no limit. If the job runs for longer than this time period,"
            " it will be aborted."
        ),
        example=60 * 60 * 10,
    )

    _normalize_destruction_time = validator(
        "destruction_time", allow_reuse=True, pre=True
    )(validate_isodatetime)

    @validator("execution_duration")
    def _validate_execution_duration(
        cls, v: timedelta | None
    ) -> timedelta | None:
        if v is not None and v <= timedelta(seconds=0):
            raise ValueError("execution_duration must be at least 1s")
        return v
