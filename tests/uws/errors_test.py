"""Tests for errors from the UWS API."""

from __future__ import annotations

import pytest
from httpx import AsyncClient

from vocutouts.uws.dependencies import UWSFactory

from ..support.uws import TrivialParameters


@pytest.mark.asyncio
async def test_errors(client: AsyncClient, uws_factory: UWSFactory) -> None:
    job_service = uws_factory.create_job_service()
    await job_service.create(
        "user", run_id="some-run-id", params=TrivialParameters(id="bar")
    )

    # No user specified.
    r = await client.get("/jobs/1")
    assert r.status_code == 422

    # Wrong user specified.
    r = await client.get(
        "/jobs/1", headers={"X-Auth-Request-User": "otheruser"}
    )
    assert r.status_code == 403

    # Job does not exist.
    r = await client.get("/jobs/2", headers={"X-Auth-Request-User": "user"})
    assert r.status_code == 404

    # Check no user specified with modification routes.
    r = await client.post("/jobs", json={"parameters": {"id": "foo"}})
    assert r.status_code == 422
    r = await client.post("/jobs/1/start", json={"start": True})
    assert r.status_code == 422
    r = await client.patch("/jobs/1", json={"execution_duration": 100})
    assert r.status_code == 422
    r = await client.delete("/jobs/1")
    assert r.status_code == 422

    # Body required for start.
    r = await client.post("/jobs/1/start")
    assert r.status_code == 422
    r = await client.post("/jobs/1/start", json={"foo": "bar"})
    assert r.status_code == 422
    r = await client.post("/jobs/1/start", json={"start": False})
    assert r.status_code == 422

    # Wrong user specified.
    r = await client.post(
        "/jobs/1/start",
        headers={"X-Auth-Request-User": "otheruser"},
        json={"start": True},
    )
    assert r.status_code == 403
    r = await client.patch(
        "/jobs/1",
        headers={"X-Auth-Request-User": "otheruser"},
        json={"execution_duration": 100},
    )
    assert r.status_code == 403
    r = await client.delete(
        "/jobs/1", headers={"X-Auth-Request-User": "otheruser"}
    )
    assert r.status_code == 403

    # Job does not exist.
    r = await client.post(
        "/jobs/2/start",
        headers={"X-Auth-Request-User": "user"},
        json={"start": True},
    )
    assert r.status_code == 404
    r = await client.patch(
        "/jobs/2",
        headers={"X-Auth-Request-User": "user"},
        json={"execution_duration": 100},
    )
    assert r.status_code == 404
    r = await client.delete("/jobs/2", headers={"X-Auth-Request-User": "user"})
    assert r.status_code == 404

    # Bogus destruction parameters.
    for destruction in ("next tuesday", "2021-09-10T10:01:02"):
        r = await client.patch(
            "/jobs/1",
            headers={"X-Auth-Request-User": "user"},
            json={"destruction_time": destruction},
        )
        assert r.status_code == 422, f"destruction_time = {destruction}"

    # Bogus execution duration parameters.
    for duration in (0, -1, "fred"):
        r = await client.patch(
            "/jobs/1",
            headers={"X-Auth-Request-User": "user"},
            json={"execution_duration": duration},
        )
        assert r.status_code == 422, f"execution_duration = {duration}"
