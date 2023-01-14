"""Tests for the job list.

These tests don't assume any given application, and therefore don't use the
API to create a job, instead inserting it directly via the UWSService.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from httpx import AsyncClient
from safir.database import datetime_to_db
from sqlalchemy import update
from sqlalchemy.ext.asyncio import async_scoped_session

from vocutouts.uws.dependencies import UWSFactory
from vocutouts.uws.schema import Job as SQLJob
from vocutouts.uws.utils import isodatetime

from ..support.uws import TrivialParameters


@pytest.mark.asyncio
async def test_job_list(
    client: AsyncClient, session: async_scoped_session, uws_factory: UWSFactory
) -> None:
    job_service = uws_factory.create_job_service()
    jobs = [
        await job_service.create("user", params=TrivialParameters(id="bar")),
        await job_service.create(
            "user", run_id="some-run-id", params=TrivialParameters(id="bar")
        ),
        await job_service.create("user", params=TrivialParameters(id="foo")),
    ]

    # Create an additional job for a different user, which shouldn't appear in
    # any of the lists.
    await job_service.create("otheruser", params=TrivialParameters(id="user"))

    # Adjust the creation time of the jobs so that searches are more
    # interesting.
    now = datetime.now(tz=timezone.utc)
    async with session.begin():
        for i, job in enumerate(jobs):
            hours = (2 - i) * 2
            creation = now - timedelta(hours=hours)
            stmt = (
                update(SQLJob)
                .where(SQLJob.job_id == int(job.job_id))
                .values(creation_time=datetime_to_db(creation))
            )
            await session.execute(stmt)
            job.creation_time = creation

    # Retrieve the job list and check it.
    r = await client.get("/jobs", headers={"X-Auth-Request-User": "user"})
    assert r.status_code == 200
    expected = [
        {
            "job_id": "3",
            "owner": "user",
            "phase": "pending",
            "creation_time": isodatetime(jobs[2].creation_time),
        },
        {
            "job_id": "2",
            "owner": "user",
            "phase": "pending",
            "run_id": "some-run-id",
            "creation_time": isodatetime(jobs[1].creation_time),
        },
        {
            "job_id": "1",
            "owner": "user",
            "phase": "pending",
            "creation_time": isodatetime(jobs[0].creation_time),
        },
    ]
    assert r.json() == expected

    # Filter by recency.
    threshold = now - timedelta(hours=1)
    r = await client.get(
        "/jobs",
        headers={"X-Auth-Request-User": "user"},
        params={"after": isodatetime(threshold)},
    )
    assert r.status_code == 200
    assert r.json() == expected[:1]

    # Filter by count.
    r = await client.get(
        "/jobs", headers={"X-Auth-Request-User": "user"}, params={"last": 1}
    )
    assert r.status_code == 200
    assert r.json() == expected[:1]

    # Start the job.
    r = await client.post(
        "/jobs/2/start",
        headers={"X-Auth-Request-User": "user"},
        json={"start": True},
    )
    assert r.status_code == 303
    assert r.headers["Location"] == "https://example.com/jobs/2"
    expected[1]["phase"] = "queued"

    # Filter by phase.
    r = await client.get(
        "/jobs",
        headers={"X-Auth-Request-User": "user"},
        params=[("phase", "executing"), ("phase", "queued")],
    )
    assert r.status_code == 200
    assert r.json() == expected[1:2]
