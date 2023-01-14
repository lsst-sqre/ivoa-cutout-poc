"""Test for long polling when retrieving jobs."""

from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from typing import Any

import dramatiq
import pytest
from dramatiq import Worker
from dramatiq.middleware import CurrentMessage
from httpx import AsyncClient
from structlog.stdlib import BoundLogger

from vocutouts.uws.config import UWSConfig
from vocutouts.uws.dependencies import UWSFactory, uws_dependency
from vocutouts.uws.utils import isodatetime

from ..support.uws import (
    TrivialParameters,
    TrivialPolicy,
    job_started,
    uws_broker,
)


@pytest.mark.asyncio
async def test_poll(
    client: AsyncClient,
    logger: BoundLogger,
    uws_config: UWSConfig,
    uws_factory: UWSFactory,
) -> None:
    job_service = uws_factory.create_job_service()
    job = await job_service.create("user", TrivialParameters(id="bar"))

    # Poll for changes for two seconds.  Nothing will happen since there is no
    # worker.
    now = datetime.now(tz=timezone.utc)
    r = await client.get(
        "/jobs/1", headers={"X-Auth-Request-User": "user"}, params={"wait": 2}
    )
    assert (datetime.now(tz=timezone.utc) - now).total_seconds() >= 2
    assert r.status_code == 200
    destruction = isodatetime(
        job.creation_time + timedelta(seconds=24 * 60 * 60)
    )
    assert r.json() == {
        "job_id": "1",
        "owner": "user",
        "phase": "pending",
        "creation_time": isodatetime(job.creation_time),
        "destruction_time": destruction,
        "execution_duration": uws_config.execution_duration,
        "parameters": {"id": "bar"},
    }

    @dramatiq.actor(broker=uws_broker, queue_name="job", store_results=True)
    def wait_job(job_id: str) -> list[dict[str, Any]]:
        message = CurrentMessage.get_current_message()
        now = isodatetime(datetime.now(tz=timezone.utc))
        job_started.send(job_id, message.message_id, now)
        time.sleep(2)
        return [
            {
                "result_id": "cutout",
                "url": "s3://some-bucket/some/path",
                "mime_type": "application/fits",
            }
        ]

    # Start the job and worker.
    uws_dependency.override_policy(TrivialPolicy(wait_job))
    r = await client.post(
        "/jobs/1/start",
        headers={"X-Auth-Request-User": "user"},
        json={"start": True},
        follow_redirects=True,
    )
    assert r.status_code == 200
    assert r.url == "https://example.com/jobs/1"
    assert r.json() == {
        "job_id": "1",
        "owner": "user",
        "phase": "queued",
        "creation_time": isodatetime(job.creation_time),
        "destruction_time": destruction,
        "execution_duration": uws_config.execution_duration,
        "parameters": {"id": "bar"},
    }
    now = datetime.now(tz=timezone.utc)
    worker = Worker(uws_broker, worker_timeout=100)
    worker.start()

    # Now, wait again.  We should get a reply after a couple of seconds when
    # the job finishes.
    try:
        r = await client.get(
            "/jobs/1",
            headers={"X-Auth-Request-User": "user"},
            params={"wait": 2, "phase": "queued"},
        )
        assert r.status_code == 200
        job = await job_service.get("user", "1")
        assert job.start_time
        assert r.json() == {
            "job_id": "1",
            "owner": "user",
            "phase": "executing",
            "creation_time": isodatetime(job.creation_time),
            "start_time": isodatetime(job.start_time),
            "destruction_time": destruction,
            "execution_duration": uws_config.execution_duration,
            "parameters": {"id": "bar"},
        }
        r = await client.get(
            "/jobs/1",
            headers={"X-Auth-Request-User": "user"},
            params={"wait": 2, "phase": "executing"},
        )
        assert r.status_code == 200
        job = await job_service.get("user", "1")
        assert job.start_time
        assert job.end_time
        assert r.json() == {
            "job_id": "1",
            "owner": "user",
            "phase": "completed",
            "creation_time": isodatetime(job.creation_time),
            "start_time": isodatetime(job.start_time),
            "end_time": isodatetime(job.end_time),
            "destruction_time": destruction,
            "parameters": {"id": "bar"},
            "execution_duration": uws_config.execution_duration,
            "results": [
                {
                    "result_id": "cutout",
                    "url": "https://example.com/some/path",
                    "mime_type": "application/fits",
                }
            ],
        }
        assert (datetime.now(tz=timezone.utc) - now).total_seconds() >= 2
    finally:
        worker.stop()
