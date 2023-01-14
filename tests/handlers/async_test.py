"""Tests for async job creation."""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import ANY

import pytest
from dramatiq import Worker
from fastapi import FastAPI
from httpx import AsyncClient

from vocutouts.broker import broker


@pytest.mark.asyncio
async def test_create_job(client: AsyncClient) -> None:
    r = await client.post(
        "/api/cutout/jobs",
        headers={"X-Auth-Request-User": "someone"},
        json={
            "parameters": {
                "ids": ["1:2:band:value"],
                "stencils": [
                    {
                        "type": "circle",
                        "center": {"ra": 0, "dec": 1},
                        "radius": 2,
                    }
                ],
            }
        },
    )
    assert r.status_code == 303
    assert r.headers["Location"] == "https://example.com/api/cutout/jobs/1"
    r = await client.get(
        "/api/cutout/jobs/1", headers={"X-Auth-Request-User": "someone"}
    )
    assert r.status_code == 200
    assert r.json() == {
        "job_id": "1",
        "owner": "someone",
        "phase": "pending",
        "creation_time": ANY,
        "execution_duration": 600,
        "destruction_time": ANY,
        "parameters": {
            "ids": ["1:2:band:value"],
            "stencils": [
                {
                    "type": "circle",
                    "center": {"ra": 0.0, "dec": 1.0},
                    "radius": 2.0,
                }
            ],
        },
    }

    # Start a worker.
    worker = Worker(broker, worker_timeout=100)
    worker.start()

    # Try again but immediately queuing the job to run.
    try:
        r = await client.post(
            "/api/cutout/jobs",
            headers={"X-Auth-Request-User": "someone"},
            json={
                "parameters": {
                    "ids": ["1:2:band:value"],
                    "stencils": [
                        {
                            "type": "circle",
                            "center": {"ra": 0, "dec": 1},
                            "radius": 2,
                        }
                    ],
                },
                "start": True,
                "run_id": "some-run-id",
            },
        )
        assert r.status_code == 303
        assert r.headers["Location"] == "https://example.com/api/cutout/jobs/2"
        r = await client.get(
            "/api/cutout/jobs/2",
            headers={"X-Auth-Request-User": "someone"},
            params={"wait": 2, "phase": "queued"},
        )
        assert r.status_code == 200
        if r.json()["phase"] == "executing":
            r = await client.get(
                "/api/cutout/jobs/2",
                headers={"X-Auth-Request-User": "someone"},
                params={"wait": 10, "phase": "executing"},
            )
            assert r.status_code == 200

        # Depending on sequencing, it's possible that the start time of the
        # job has not yet been recorded.  If that is the case, wait a bit for
        # that to happen and then request the job again.
        job = r.json()
        if not job.get("start_time"):
            await asyncio.sleep(2.0)
            r = await client.get(
                "/api/cutout/jobs/2",
                headers={"X-Auth-Request-User": "someone"},
                params={"wait": 10, "phase": "executing"},
            )
            assert r.status_code == 200
            job = r.json()

        assert job == {
            "job_id": "2",
            "run_id": "some-run-id",
            "owner": "someone",
            "phase": "completed",
            "creation_time": ANY,
            "start_time": ANY,
            "end_time": ANY,
            "execution_duration": 600,
            "destruction_time": ANY,
            "parameters": {
                "ids": ["1:2:band:value"],
                "stencils": [
                    {
                        "type": "circle",
                        "center": {"ra": 0.0, "dec": 1.0},
                        "radius": 2.0,
                    }
                ],
            },
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
async def test_redirect(app: FastAPI) -> None:
    """Test the scheme in the redirect after creating a job.

    When running in a Kubernetes cluster behind an ingress that terminates
    TLS, the request as seen by the application will be ``http``, but we want
    the redirect to honor ``X-Forwarded-Proto`` and thus use ``https``.  Also
    test that the correct hostname is used if it is different.
    """
    async with AsyncClient(app=app, base_url="http://foo.com/") as client:
        r = await client.post(
            "/api/cutout/jobs",
            headers={
                "Host": "example.org",
                "X-Forwarded-For": "10.10.10.10",
                "X-Forwarded-Host": "example.org",
                "X-Forwarded-Proto": "https",
                "X-Auth-Request-User": "someone",
            },
            json={
                "parameters": {
                    "ids": ["1:2:band:value"],
                    "stencils": [
                        {
                            "type": "circle",
                            "center": {"ra": 0, "dec": 1},
                            "radius": 2,
                        }
                    ],
                }
            },
        )
    assert r.status_code == 303
    assert r.headers["Location"] == "https://example.org/api/cutout/jobs/1"


@pytest.mark.asyncio
async def test_bad_parameters(client: AsyncClient) -> None:
    bad_stencils: list[dict[str, Any]] = [
        {},
        {"type": "pos", "center": {"ra": 0, "dec": 0}, "radius": 1},
        {
            "type": "range",
            "ra": {"min": 0, "max": 360},
            "dec": {"min": -2, "max": 2},
        },
        {"type": "polygon", "vertices": [{"ra": 1, "dec": 2}]},
        {
            "type": "polygon",
            "vertices": [{"ra": 1, "dec": 2}, {"ra": 2, "dec": 3}],
        },
    ]
    for stencil in bad_stencils:
        r = await client.post(
            "/api/cutout/jobs",
            headers={"X-Auth-Request-User": "user"},
            json={
                "ids": ["1:2:band:value"],
                "stencils": [stencil],
            },
        )
        assert r.status_code == 422, f"Stencil {stencil}"

    # Multiple ids with valid stencils aren't allowed.
    r = await client.post(
        "/api/cutout/jobs",
        headers={"X-Auth-Request-User": "someone"},
        json={
            "parameters": {
                "ids": ["1:2:band:value", "2:3:band:value"],
                "stencils": [
                    {
                        "type": "circle",
                        "center": {"ra": 0, "dec": 1},
                        "radius": 2,
                    }
                ],
            }
        },
    )
    assert r.status_code == 422

    # Multiple stencils aren't allowed.
    r = await client.post(
        "/api/cutout/jobs",
        headers={"X-Auth-Request-User": "someone"},
        json={
            "parameters": {
                "ids": ["1:2:band:value"],
                "stencils": [
                    {
                        "type": "circle",
                        "center": {"ra": 0, "dec": 1},
                        "radius": 2,
                    },
                    {
                        "type": "circle",
                        "center": {"ra": 1, "dec": 1},
                        "radius": 1,
                    },
                ],
            }
        },
    )
    assert r.status_code == 422
