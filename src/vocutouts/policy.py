"""UWS policy layer for image cutouts."""

from __future__ import annotations

from datetime import datetime, timedelta

from dramatiq import Actor, Message
from pydantic import BaseModel
from structlog.stdlib import BoundLogger

from .actors import job_completed, job_failed
from .models.parameters import CutoutParameters
from .models.stencils import RangeStencil
from .uws.exceptions import ParameterUnsupportedError
from .uws.models import Job
from .uws.policy import UWSPolicy

__all__ = ["ImageCutoutPolicy"]


class ImageCutoutPolicy(UWSPolicy):
    """Policy layer for dispatching and approving changes to UWS jobs.

    For now, rejects all changes to destruction and execution duration by
    returning their current values.

    Parameters
    ----------
    actor
         The actor to call for a job.  This simple mapping is temporary;
         eventually different types of cutouts will dispatch to different
         actors.
    logger
         Logger to use to report errors when dispatching the request.
    """

    def __init__(self, actor: Actor, logger: BoundLogger) -> None:
        super().__init__()
        self._actor = actor
        self._logger = logger

    def dispatch(self, job: Job) -> Message:
        """Dispatch a cutout request to the backend.

        Parameters
        ----------
        job
            The submitted job description.

        Returns
        -------
        dramatiq.Message
            The dispatched message to the backend.

        Notes
        -----
        Currently, only one dataset ID and only one stencil are supported.
        This limitation is expected to be relaxed in a later version.
        """
        duration = None
        if job.execution_duration:
            duration = job.execution_duration.total_seconds() * 1000
        return self._actor.send_with_options(
            args=(job.job_id, job.parameters.dict()),
            time_limit=duration,
            on_success=job_completed,
            on_failure=job_failed,
        )

    def validate_destruction(
        self, destruction: datetime, job: Job
    ) -> datetime:
        return job.destruction_time if job.destruction_time else destruction

    def validate_execution_duration(
        self, execution_duration: timedelta, job: Job
    ) -> timedelta:
        if job.execution_duration:
            return job.execution_duration
        else:
            return execution_duration

    def validate_params(self, params: BaseModel) -> None:
        if not isinstance(params, CutoutParameters):
            raise RuntimeError("Invalid type for parameters")

        # For now, only support a single ID and stencil.
        if len(params.ids) != 1:
            raise ParameterUnsupportedError("Only one ID supported")
        if len(params.stencils) != 1:
            raise ParameterUnsupportedError("Only one stencil is supported")

        # For now, range stencils are not supported.
        if isinstance(params.stencils[0], RangeStencil):
            raise ParameterUnsupportedError("Range stencils are not supported")
