"""Test handling of jobs that fail."""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any

import dramatiq
import pytest
from dramatiq import Worker
from dramatiq.middleware import CurrentMessage
from httpx import AsyncClient
from structlog.stdlib import BoundLogger

from vocutouts.uws.config import UWSConfig
from vocutouts.uws.dependencies import UWSFactory, uws_dependency
from vocutouts.uws.exceptions import TaskError
from vocutouts.uws.utils import isodatetime

from ..support.uws import (
    TrivialParameters,
    TrivialPolicy,
    job_started,
    uws_broker,
    wait_for_job,
)


@pytest.mark.asyncio
async def test_temporary_error(
    client: AsyncClient,
    logger: BoundLogger,
    uws_config: UWSConfig,
    uws_factory: UWSFactory,
) -> None:
    job_service = uws_factory.create_job_service()
    job = await job_service.create(
        "user", params=TrivialParameters(id="1:2:a:b")
    )

    # The pending job has no error.
    r = await client.get("/jobs/1", headers={"X-Auth-Request-User": "user"})
    assert r.status_code == 200
    assert not r.json().get("error")

    # Create a backend worker that raises a transient error.
    @dramatiq.actor(broker=uws_broker, queue_name="job")
    def error_transient_job(job_id: str) -> list[dict[str, Any]]:
        message = CurrentMessage.get_current_message()
        now = datetime.now(tz=timezone.utc)
        job_started.send(job_id, message.message_id, isodatetime(now))
        time.sleep(0.5)
        raise TaskError("usage_error", "Something failed")

    # Start the job.
    uws_dependency.override_policy(TrivialPolicy(error_transient_job))
    r = await client.post(
        "/jobs/1/start",
        headers={"X-Auth-Request-User": "user"},
        json={"start": True},
    )
    assert r.status_code == 303
    worker = Worker(uws_broker, worker_timeout=100)
    worker.start()

    # Check the results.
    try:
        job = await wait_for_job(job_service, "user", "1")
        assert job.start_time
        assert job.end_time
        assert job.destruction_time
        r = await client.get(
            "/jobs/1", headers={"X-Auth-Request-User": "user"}
        )
        assert r.status_code == 200
        assert r.json() == {
            "job_id": "1",
            "owner": "user",
            "phase": "error",
            "creation_time": isodatetime(job.creation_time),
            "start_time": isodatetime(job.start_time),
            "end_time": isodatetime(job.end_time),
            "execution_duration": 600,
            "destruction_time": isodatetime(job.destruction_time),
            "parameters": {"id": "1:2:a:b"},
            "error": {
                "error_code": "usage_error",
                "message": "Something failed",
            },
        }
    finally:
        worker.stop()


@pytest.mark.asyncio
async def test_fatal_error(
    client: AsyncClient,
    logger: BoundLogger,
    uws_config: UWSConfig,
    uws_factory: UWSFactory,
) -> None:
    job_service = uws_factory.create_job_service()
    job = await job_service.create(
        "user", params=TrivialParameters(id="1:2:a:b")
    )

    # Create a backend worker that raises a fatal error with detail.
    @dramatiq.actor(broker=uws_broker, queue_name="job")
    def error_fatal_job(job_id: str) -> list[dict[str, Any]]:
        message = CurrentMessage.get_current_message()
        now = datetime.now(tz=timezone.utc)
        job_started.send(job_id, message.message_id, isodatetime(now))
        time.sleep(0.5)
        raise TaskError("something", "Whoops", "Some details")

    # Start the job.
    uws_dependency.override_policy(TrivialPolicy(error_fatal_job))
    r = await client.post(
        "/jobs/1/start",
        headers={"X-Auth-Request-User": "user"},
        json={"start": True},
    )
    assert r.status_code == 303
    worker = Worker(uws_broker, worker_timeout=100)
    worker.start()

    # Check the results.
    try:
        job = await wait_for_job(job_service, "user", "1")
        assert job.start_time
        assert job.end_time
        assert job.destruction_time
        r = await client.get(
            "/jobs/1", headers={"X-Auth-Request-User": "user"}
        )
        assert r.status_code == 200
        assert r.json() == {
            "job_id": "1",
            "owner": "user",
            "phase": "error",
            "creation_time": isodatetime(job.creation_time),
            "start_time": isodatetime(job.start_time),
            "end_time": isodatetime(job.end_time),
            "execution_duration": 600,
            "destruction_time": isodatetime(job.destruction_time),
            "parameters": {"id": "1:2:a:b"},
            "error": {
                "error_code": "something",
                "message": "Whoops",
                "detail": "Some details",
            },
        }
    finally:
        worker.stop()


@pytest.mark.asyncio
async def test_unknown_error(
    client: AsyncClient,
    logger: BoundLogger,
    uws_config: UWSConfig,
    uws_factory: UWSFactory,
) -> None:
    job_service = uws_factory.create_job_service()
    job = await job_service.create(
        "user", params=TrivialParameters(id="1:2:a:b")
    )

    # Create a backend worker that raises a fatal error with detail.
    @dramatiq.actor(broker=uws_broker, queue_name="job")
    def error_unknown_job(job_id: str) -> list[dict[str, Any]]:
        message = CurrentMessage.get_current_message()
        now = datetime.now(tz=timezone.utc)
        time.sleep(0.5)
        job_started.send(job_id, message.message_id, isodatetime(now))
        raise ValueError("Unknown exception")

    # Start the job.
    uws_dependency.override_policy(TrivialPolicy(error_unknown_job))
    r = await client.post(
        "/jobs/1/start",
        headers={"X-Auth-Request-User": "user"},
        json={"start": True},
    )
    assert r.status_code == 303
    worker = Worker(uws_broker, worker_timeout=100)
    worker.start()

    # Check the results.
    try:
        job = await wait_for_job(job_service, "user", "1")
        assert job.start_time
        assert job.end_time
        assert job.destruction_time
        r = await client.get(
            "/jobs/1", headers={"X-Auth-Request-User": "user"}
        )
        assert r.status_code == 200
        assert r.json() == {
            "job_id": "1",
            "owner": "user",
            "phase": "error",
            "creation_time": isodatetime(job.creation_time),
            "start_time": isodatetime(job.start_time),
            "end_time": isodatetime(job.end_time),
            "execution_duration": 600,
            "destruction_time": isodatetime(job.destruction_time),
            "parameters": {"id": "1:2:a:b"},
            "error": {
                "error_code": "unknown_error",
                "message": "Unknown error executing task",
                "detail": "ValueError: Unknown exception",
            },
        }
    finally:
        worker.stop()
