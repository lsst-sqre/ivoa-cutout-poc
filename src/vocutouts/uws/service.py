"""Service layer for a UWS service."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Generic, Optional, TypeVar

from dramatiq import Message
from pydantic import BaseModel
from safir.gcs import SignedURLService
from structlog.stdlib import BoundLogger

from .config import UWSConfig
from .exceptions import (
    InvalidPhaseError,
    PermissionDeniedError,
    SyncTimeoutError,
    TaskError,
)
from .models import (
    ACTIVE_PHASES,
    Availability,
    ExecutionPhase,
    Job,
    JobDescription,
    JobUpdate,
)
from .policy import UWSPolicy
from .storage import FrontendJobStore

T = TypeVar("T", bound=BaseModel)

__all__ = ["JobService"]


class JobService(Generic[T]):
    """Dispatch and track UWS jobs.

    The goal of this layer is to encapsulate the machinery of a service that
    dispatches jobs using Dramatiq, without making assumptions about what the
    jobs do or what outputs they may return.  Workers do not use this layer
    and instead talk directly to the `~vocutouts.uws.storage.WorkerJobStore`.

    Parameters
    ----------
    config
        The UWS configuration.
    policy
        The policy layer for dispatching jobs and validating parameters,
        destruction times, and execution durations.
    storage
        The underlying storage for job metadata and result tracking.
    """

    def __init__(
        self,
        *,
        config: UWSConfig,
        policy: UWSPolicy,
        storage: FrontendJobStore[T],
        logger: BoundLogger,
    ) -> None:
        self._config = config
        self._policy = policy
        self._storage = storage
        self._logger = logger
        self._url_service = SignedURLService(
            service_account=config.signing_service_account,
            lifetime=timedelta(seconds=config.url_lifetime),
        )

    async def availability(self) -> Availability:
        """Check whether the service is up.

        Used for ``/availability`` endpoints.  Currently this only checks the
        database.  Eventually it should push an end-to-end test through the
        job execution system.

        Returns
        -------
        vocutouts.uws.models.Availability
            Service availability information.
        """
        return await self._storage.availability()

    async def create(
        self,
        user: str,
        params: T,
        *,
        run_id: Optional[str] = None,
    ) -> Job[T]:
        """Create a pending job.

        This does not start execution of the job.  That must be done
        separately with `start`.

        Parameters
        ----------
        user
            User on behalf this operation is performed.
        params
            The input parameters to the job.
        run_id
            A client-supplied opaque identifier to record with the job.

        Returns
        -------
        vocutouts.uws.models.Job
            The details of the newly-created job.
        """
        self._policy.validate_params(params)
        return await self._storage.add(
            owner=user,
            run_id=run_id,
            params=params,
            execution_duration=self._config.execution_duration,
            lifetime=self._config.lifetime,
        )

    async def delete(
        self,
        user: str,
        job_id: str,
    ) -> None:
        """Delete a job.

        The UWS standard says that deleting a job should stop the in-progress
        work, but Dramatiq doesn't provide a way to do that.  Settle for
        deleting the database entry, which will cause the task to throw away
        the results when it finishes.
        """
        job = await self._storage.get(job_id)
        if job.owner != user:
            raise PermissionDeniedError(f"Access to job {job_id} denied")
        await self._storage.delete(job_id)

    async def get(
        self,
        user: str,
        job_id: str,
        *,
        wait: Optional[int] = None,
        wait_phase: Optional[ExecutionPhase] = None,
        wait_for_completion: bool = False,
    ) -> Job[T]:
        """Retrieve a job.

        This also supports long-polling, to implement UWS 1.1 blocking
        behavior, and waiting for completion, to use as a building block when
        constructing a sync API.

        Parameters
        ----------
        user
            User on behalf this operation is performed.
        job_id
            Identifier of the job.
        wait
            If given, wait up to this many seconds for the status to change
            before returning.  ``-1`` says to wait the maximum length of time.
            This is done by polling the database with exponential backoff.
            This will only be honored if the phase is ``PENDING``, ``QUEUED``,
            or ``EXECUTING``.
        wait_phase
            If ``wait`` was given, the starting phase for waiting.  Returns
            immediately if the initial phase doesn't match this one.
        wait_for_completion
            If set to true, wait until the job completes (has a phase other
            than ``QUEUED`` or ``EXECUTING``).  Only one of this or
            ``wait_phase`` should be given.  Ignored if ``wait`` was not
            given.

        Returns
        -------
        vocutouts.uws.models.Job
            The corresponding job.

        Raises
        ------
        vocutouts.uws.exceptions.PermissionDeniedError
            If the job ID doesn't exist or is for a user other than the
            provided user.

        Notes
        -----
        ``wait`` and related parameters are relatively inefficient since they
        poll the database using exponential backoff (starting at a 0.1s delay
        and increasing by 1.5x).  There doesn't seem to be a better solution
        without the added complexity of Dramatiq result storage and complex
        use of the Dramatiq message bus.  This may need to be reconsidered if
        it becomes a performance bottleneck.
        """
        job = await self._storage.get(job_id)
        if job.owner != user:
            raise PermissionDeniedError(f"Access to job {job_id} denied")

        # If waiting for a status change was requested and is meaningful, do
        # so, capping the wait time at the configured maximum timeout.
        if wait and job.phase in ACTIVE_PHASES:
            if wait < 0 or wait > self._config.wait_timeout:
                wait = self._config.wait_timeout
            end_time = datetime.now(tz=timezone.utc) + timedelta(seconds=wait)
            if not wait_phase:
                wait_phase = job.phase

            # Determine the criteria to stop waiting.
            def not_done(j: Job) -> bool:
                if wait_for_completion:
                    return j.phase in ACTIVE_PHASES
                else:
                    return j.phase == wait_phase

            # Poll the database with exponential delay starting with 0.1
            # seconds and increasing by 1.5x each time until we reach the
            # maximum duration.
            delay = 0.1
            while not_done(job):
                await asyncio.sleep(delay)
                job = await self._storage.get(job_id)
                now = datetime.now(tz=timezone.utc)
                if now >= end_time:
                    break
                delay *= 1.5
                if now + timedelta(seconds=delay) > end_time:
                    delay = (end_time - now).total_seconds()

        # Convert result URLs to signed URLs.
        if job.results:
            for result in job.results:
                result.url = self._url_service.signed_url(
                    result.url, result.mime_type
                )

        return job

    async def get_first_result(self, user: str, job_id: str) -> str:
        """Wait for a job to complete and get the URL of the first result.

        Used to implement sync routes that return a single result.

        Parameters
        ----------
        user
            User on behalf this operation is performed.
        job_id
            Identifier of the job.

        Returns
        -------
        str
            URL of the first result.

        Raises
        ------
        UWSError
            If synchronous execution of the job failed.
        """
        job = await self.get(
            user,
            job_id,
            wait=self._config.sync_timeout,
            wait_for_completion=True,
        )
        if job.run_id:
            logger = self._logger.bind(run_id=job.run_id)
        else:
            logger = self._logger

        # Check for error states.
        if job.phase not in (ExecutionPhase.COMPLETED, ExecutionPhase.ERROR):
            logger.warning("Job timed out", job_id=job.job_id)
            msg = f"Cutout did not complete in {self._config.sync_timeout}s"
            raise SyncTimeoutError(msg)
        if job.error:
            logger.warning(
                "Job failed",
                job_id=job.job_id,
                error_code=job.error.error_code,
                error=job.error.message,
                error_detail=job.error.detail,
            )
            raise TaskError(
                job.error.error_code, job.error.message, job.error.detail
            )
        if not job.results:
            logger.warning("Job returned no results", job_id=job.job_id)
            msg = "Job did not return any results"
            raise TaskError("no_results", msg)

        # Redirect to the URL of the first result.
        return job.results[0].url

    async def list_jobs(
        self,
        user: str,
        *,
        phases: Optional[list[ExecutionPhase]] = None,
        after: Optional[datetime] = None,
        count: Optional[int] = None,
    ) -> list[JobDescription]:
        """List the jobs for a particular user.

        Parameters
        ----------
        user
            Name of the user whose jobs to load.
        phases
            Limit the result to jobs in this list of possible execution
            phases.
        after
            Limit the result to jobs created after the given datetime.
        count
            Limit the results to the most recent count jobs.

        Returns
        -------
        list of vocutouts.uws.models.JobDescription
            List of job descriptions matching the search criteria.
        """
        return await self._storage.list_jobs(
            user, phases=phases, after=after, count=count
        )

    async def update(self, user: str, job_id: str, update: JobUpdate) -> None:
        """Update a job.

        Parameters
        ----------
        user
            User on behalf of whom this operation is performed.
        job_id
            Identifier of the job to start.
        update
            Job properties to change.

        Raises
        ------
        vocutouts.uws.exceptions.PermissionDeniedError
            If the job ID doesn't exist or is for a user other than the
            provided user.
        """
        job = await self._storage.get(job_id)
        if job.owner != user:
            raise PermissionDeniedError(f"Access to job {job_id} denied")
        if update.destruction_time:
            destruction = self._policy.validate_destruction(
                update.destruction_time, job
            )
            if destruction != job.destruction_time:
                await self._storage.update_destruction(job_id, destruction)
        if update.execution_duration:
            duration = self._policy.validate_execution_duration(
                update.execution_duration, job
            )
            if duration != job.execution_duration:
                await self._storage.update_execution_duration(job_id, duration)

    async def start(self, user: str, job_id: str) -> Message:
        """Start execution of a job.

        Parameters
        ----------
        user
            User on behalf of whom this operation is performed.
        job_id
            Identifier of the job to start.

        Returns
        -------
        dramatiq.Message
            The work queuing message representing this job.

        Raises
        ------
        vocutouts.uws.exceptions.PermissionDeniedError
            If the job ID doesn't exist or is for a user other than the
            provided user.
        """
        job = await self._storage.get(job_id)
        if job.owner != user:
            raise PermissionDeniedError(f"Access to job {job_id} denied")
        if job.phase not in (ExecutionPhase.PENDING, ExecutionPhase.HELD):
            raise InvalidPhaseError("Cannot start job in phase {job.phase}")
        message = self._policy.dispatch(job)
        await self._storage.mark_queued(job_id, message.message_id)
        return message
