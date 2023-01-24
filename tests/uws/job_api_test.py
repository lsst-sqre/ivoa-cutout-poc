"""Tests for the UWS job manipulation handlers.

These tests don't assume any given application, and therefore don't use the
API to create a job, instead inserting it directly via the UWSService.
"""

from __future__ import annotations

from datetime import timedelta

import pytest
from dramatiq import Worker
from fastapi import FastAPI
from httpx import AsyncClient

from vocutouts.uws.config import UWSConfig
from vocutouts.uws.dependencies import UWSFactory
from vocutouts.uws.utils import isodatetime

from ..support.uws import TrivialParameters, uws_broker, wait_for_job


@pytest.mark.asyncio
async def test_job_run(
    client: AsyncClient,
    uws_config: UWSConfig,
    uws_factory: UWSFactory,
) -> None:
    job_service = uws_factory.create_job_service()
    job = await job_service.create(
        "user", run_id="some-run-id", params=TrivialParameters(id="bar")
    )

    # Check the retrieval of the job configuration.
    r = await client.get("/jobs/1", headers={"X-Auth-Request-User": "user"})
    assert r.status_code == 200
    destruction = job.creation_time + timedelta(hours=24)
    assert r.json() == {
        "job_id": "1",
        "run_id": "some-run-id",
        "owner": "user",
        "phase": "pending",
        "creation_time": isodatetime(job.creation_time),
        "execution_duration": 600,
        "destruction_time": isodatetime(destruction),
        "parameters": {"id": "bar"},
    }

    # Start the job.
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
        "run_id": "some-run-id",
        "owner": "user",
        "phase": "queued",
        "creation_time": isodatetime(job.creation_time),
        "execution_duration": 600,
        "destruction_time": isodatetime(destruction),
        "parameters": {"id": "bar"},
    }

    # Start the job worker.
    worker = Worker(uws_broker, worker_timeout=100)
    worker.start()

    # Check the job results.
    try:
        job = await wait_for_job(job_service, "user", "1")
        assert job.start_time
        assert job.end_time
        assert job.end_time >= job.start_time >= job.creation_time
        r = await client.get(
            "/jobs/1", headers={"X-Auth-Request-User": "user"}
        )
        assert r.status_code == 200
        assert r.json() == {
            "job_id": "1",
            "run_id": "some-run-id",
            "owner": "user",
            "phase": "completed",
            "creation_time": isodatetime(job.creation_time),
            "start_time": isodatetime(job.start_time),
            "end_time": isodatetime(job.end_time),
            "execution_duration": 600,
            "destruction_time": isodatetime(destruction),
            "parameters": {"id": "bar"},
            "results": [
                {
                    "result_id": "cutout",
                    "url": "https://example.com/some/path",
                    "mime_type": "application/fits",
                }
            ],
        }
    finally:
        worker.stop()


@pytest.mark.asyncio
async def test_job_api(
    client: AsyncClient,
    uws_factory: UWSFactory,
) -> None:
    job_service = uws_factory.create_job_service()
    job = await job_service.create("user", params=TrivialParameters(id="bar"))

    # Check the retrieval of the job configuration.
    destruction = job.creation_time + timedelta(hours=24)
    r = await client.get("/jobs/1", headers={"X-Auth-Request-User": "user"})
    assert r.status_code == 200
    assert r.json() == {
        "job_id": "1",
        "owner": "user",
        "phase": "pending",
        "creation_time": isodatetime(job.creation_time),
        "execution_duration": 600,
        "destruction_time": isodatetime(destruction),
        "parameters": {"id": "bar"},
    }

    # Modify various settings.  These go through the policy layer, which is
    # mocked to do nothing.  Policy rejections will be tested elsewhere.
    destruction = job.creation_time + timedelta(hours=48)
    r = await client.patch(
        "/jobs/1",
        headers={"X-Auth-Request-User": "user"},
        json={
            "destruction_time": isodatetime(destruction),
            "execution_duration": 1200,
        },
    )
    assert r.status_code == 200
    assert r.json() == {
        "job_id": "1",
        "owner": "user",
        "phase": "pending",
        "creation_time": isodatetime(job.creation_time),
        "execution_duration": 1200,
        "destruction_time": isodatetime(destruction),
        "parameters": {"id": "bar"},
    }

    # Delete the job.
    r = await client.delete("/jobs/1", headers={"X-Auth-Request-User": "user"})
    assert r.status_code == 204
    r = await client.get("/jobs/1", headers={"X-Auth-Request-User": "user"})
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_redirects(
    app: FastAPI,
    uws_factory: UWSFactory,
) -> None:
    """Test the scheme in the redirect URLs.

    When running in a Kubernetes cluster behind an ingress that terminates
    TLS, the request as seen by the application will be ``http``, but we want
    the redirect URLs to honor ``X-Forwarded-Proto`` and thus use ``https``.
    We also want to honor the ``Host`` header.
    """
    job_service = uws_factory.create_job_service()
    await job_service.create("user", params=TrivialParameters(id="bar"))

    # Start the job and ensure the resulting redirect is correct.
    async with AsyncClient(app=app, base_url="http://foo.com/") as client:
        r = await client.post(
            "/jobs/1/start",
            headers={
                "X-Auth-Request-User": "user",
                "Host": "example.org",
                "X-Forwarded-For": "10.10.10.10",
                "X-Forwarded-Proto": "https",
                "X-Forwarded-Host": "foo.com",
            },
            json={"start": True},
        )
        assert r.status_code == 303
        assert r.headers["Location"] == "https://example.org/jobs/1"
