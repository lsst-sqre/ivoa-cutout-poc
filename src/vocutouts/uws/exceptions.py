"""Exceptions for the Universal Worker Service.

The types of exceptions here control the error handling behavior configured in
:py:mod:`vocutouts.uws.errors`.
"""

from __future__ import annotations

import json
from enum import Enum
from typing import Any, ClassVar, Optional

from fastapi import status

from .models import JobError

__all__ = [
    "ErrorLocation",
    "InvalidPhaseError",
    "PermissionDeniedError",
    "TaskError",
    "UnknownJobError",
    "UWSError",
]


class ErrorLocation(Enum):
    """Specifies the request component that triggered a `UWSError`."""

    body = "body"
    header = "header"
    path = "path"
    query = "query"


class UWSError(Exception):
    """An error with an associated error code.

    There is a global handler for this exception and all exceptions derived
    from it that returns an HTTP status code equal to the ``status_code``
    class variable with a body that's consistent with the error messages
    generated internally by FastAPI.  It should be used for all errors from
    the service.

    Parameters
    ----------
    message
        The error message (used as the ``msg`` key).
    location
        The part of the request giving rise to the error.
    field
        The field within that part of the request giving rise to the error.

    Attributes
    ----------
    location
        The part of the request giving rise to the error.  This can be
        modified by the handler before re-raising the error.
    field
        The field within that part of the request giving rise to the error.
        This can be modified by the handler before re-raising the error.

    Notes
    -----
    The FastAPI body format supports returning multiple errors at a time as a
    list in the ``details`` key.  This is currently not implemented.
    """

    error: ClassVar[str] = "unknown_error"
    """Used as the ``type`` field of the error message.

    Should be overriden by any subclass.
    """

    status_code: ClassVar[int] = status.HTTP_422_UNPROCESSABLE_ENTITY
    """HTTP status code for this type of validation error."""

    def __init__(
        self,
        message: str,
        location: Optional[ErrorLocation] = None,
        field: Optional[str] = None,
    ) -> None:
        super().__init__(message)
        self.location = location
        self.field = field

    def to_dict(self) -> dict[str, list[str] | str]:
        """Convert the exception to a dictionary suitable for the exception.

        Returns
        -------
        dict
            Serialized error emssage to pass as the ``detail`` parameter to a
            ``fastapi.HTTPException``.  It is designed to produce the same
            JSON structure as native FastAPI errors.
        """
        error: dict[str, Any] = {"msg": str(self), "type": self.error}
        if self.location and self.field:
            error["loc"] = [self.location.value, self.field]
        return error


class InvalidPhaseError(UWSError):
    """The requeted phase transition is invalid."""

    error = "invalid_phase_transition"
    status_code = status.HTTP_422_UNPROCESSABLE_ENTITY


class ParameterUnsupportedError(UWSError):
    """The job parameters passed in were unsupported.

    This exception is primarily intended to be thrown by the
    ``validate_params`` method of the policy object provided by the
    application.
    """

    error = "unsupported_parameter"
    status_code = status.HTTP_422_UNPROCESSABLE_ENTITY


class PermissionDeniedError(UWSError):
    """User does not have access to this resource."""

    error = "permission_denied"
    status_code = status.HTTP_403_FORBIDDEN


class SyncTimeoutError(UWSError):
    """The job ran for longer than the timeout for synchronous jobs."""

    error = "sync_timeout"
    status_code = status.HTTP_400_BAD_REQUEST


class TaskError(UWSError):
    """An error occurred during background task processing."""

    error = "job_execution_error"
    status_code = status.HTTP_400_BAD_REQUEST

    @classmethod
    def from_callback(cls, exception: dict[str, str]) -> TaskError:
        """Reconstitute the exception passed to an on_failure callback.

        Notes
        -----
        Unfortunately, the ``dramatiq.middleware.Callbacks`` middleware only
        provides the type of the error message and the body as strings, so we
        have to parse the body of the exception to get the structured data we
        want to store in the UWS database.
        """
        exception_type = exception["type"]
        exception_message = exception["message"]
        if exception_type == "TaskError":
            try:
                error = json.loads(exception_message)
                return cls(
                    error_code=error["error_code"],
                    message=error["message"],
                    detail=error.get("detail"),
                )
            except Exception:
                return cls(
                    error_code="unknown_error", message=exception_message
                )
        else:
            return cls(
                error_code="unknown_error",
                message="Unknown error executing task",
                detail=f"{exception_type}: {exception_message}",
            )

    def __init__(
        self,
        error_code: str,
        message: str,
        detail: Optional[str] = None,
    ) -> None:
        data = {"error_code": error_code, "message": message, "detail": detail}
        super().__init__(json.dumps(data))
        self.error_code = error_code
        self.message = message
        self.detail = detail

    def to_dict(self) -> dict[str, list[str] | str]:
        """Convert the exception to a dictionary suitable for the exception.

        Returns
        -------
        dict
            Serialized error emssage to pass as the ``detail`` parameter to a
            ``fastapi.HTTPException``.  It is designed to produce the same
            JSON structure as native FastAPI errors.
        """
        if self.detail:
            msg = f"{self.message}: {self.detail}"
        else:
            msg = self.message
        error: dict[str, Any] = {"msg": msg, "type": self.error_code}
        return error

    def to_job_error(self) -> JobError:
        """Convert to a `~vocutouts.uws.models.JobError`."""
        return JobError(
            error_code=self.error_code,
            message=self.message,
            detail=self.detail,
        )


class UnknownJobError(UWSError):
    """The named job could not be found in the database."""

    error = "unknown_job"
    status_code = status.HTTP_404_NOT_FOUND

    def __init__(self, job_id: str) -> None:
        super().__init__(
            f"Job {job_id} not found", ErrorLocation.path, "job_id"
        )
        self.job_id = job_id
