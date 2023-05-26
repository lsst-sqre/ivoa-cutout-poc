"""Test for the UWS policy layer."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from httpx import AsyncClient
from pydantic import BaseModel

from vocutouts.uws.config import UWSConfig
from vocutouts.uws.dependencies import UWSFactory, uws_dependency
from vocutouts.uws.models import Job
from vocutouts.uws.utils import isodatetime, parse_isodatetime

from ..support.uws import TrivialParameters, TrivialPolicy, trivial_job


class Policy(TrivialPolicy):
    def validate_destruction(
        self, destruction: datetime, job: Job
    ) -> datetime:
        maximum = datetime.now(tz=timezone.utc) + timedelta(days=1)
        return maximum if destruction > maximum else destruction

    def validate_execution_duration(
        self, execution_duration: timedelta, job: Job
    ) -> timedelta:
        maximum = timedelta(seconds=200)
        return maximum if execution_duration > maximum else execution_duration

    def validate_params(self, params: BaseModel) -> None:
        assert isinstance(params, TrivialParameters)
        if params.id == "foo":
            raise ValueError("Invalid parameter")


@pytest.mark.asyncio
async def test_policy(
    client: AsyncClient, uws_factory: UWSFactory, uws_config: UWSConfig
) -> None:
    uws_dependency.override_policy(Policy(trivial_job))
    uws_factory._policy = Policy(trivial_job)
    job_service = uws_factory.create_job_service()

    # Check parameter rejection.
    with pytest.raises(ValueError):
        await job_service.create("user", params=TrivialParameters(id="foo"))

    # Create a job that should pass the policy layer.
    await job_service.create("user", params=TrivialParameters(id="bar"))

    # Change the destruction time, first to something that should be honored
    # and then something that should be overridden.
    destruction = datetime.now(tz=timezone.utc) + timedelta(hours=1)
    r = await client.patch(
        "/jobs/1",
        headers={"X-Auth-Request-User": "user"},
        json={"destruction_time": isodatetime(destruction)},
    )
    assert r.status_code == 200
    assert r.json()["destruction_time"] == isodatetime(destruction)
    destruction = datetime.now(tz=timezone.utc) + timedelta(days=5)
    expected = datetime.now(tz=timezone.utc) + timedelta(days=1)
    r = await client.patch(
        "/jobs/1",
        headers={"X-Auth-Request-User": "user"},
        json={"destruction_time": isodatetime(destruction)},
    )
    assert r.status_code == 200
    seen = parse_isodatetime(r.json()["destruction_time"])
    assert seen
    assert seen >= expected - timedelta(seconds=5)
    assert seen <= expected + timedelta(seconds=5)

    # Now do the same thing for execution duration.
    r = await client.patch(
        "/jobs/1",
        headers={"X-Auth-Request-User": "user"},
        json={"execution_duration": 100},
    )
    assert r.status_code == 200
    assert r.json()["execution_duration"] == 100
    r = await client.patch(
        "/jobs/1",
        headers={"X-Auth-Request-User": "user"},
        json={"execution_duration": 250},
    )
    assert r.status_code == 200
    assert r.json()["execution_duration"] == 200
