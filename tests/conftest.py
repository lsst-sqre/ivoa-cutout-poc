"""Test fixtures for vo-cutouts tests."""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from datetime import datetime, timedelta, timezone
from typing import Any

import dramatiq
import pytest
import pytest_asyncio
import structlog
from asgi_lifespan import LifespanManager
from dramatiq.middleware import CurrentMessage
from fastapi import FastAPI
from httpx import AsyncClient
from safir.database import create_database_engine, initialize_database
from safir.testing.gcs import MockStorageClient, patch_google_storage

from vocutouts import main
from vocutouts.actors import job_started
from vocutouts.broker import broker
from vocutouts.config import config
from vocutouts.policy import ImageCutoutPolicy
from vocutouts.uws.dependencies import uws_dependency
from vocutouts.uws.schema import Base
from vocutouts.uws.utils import isodatetime


@dramatiq.actor(queue_name="cutout", store_results=True)
def cutout_test(
    job_id: str,
    params: dict[str, Any],
) -> list[dict[str, Any]]:
    message = CurrentMessage.get_current_message()
    now = isodatetime(datetime.now(tz=timezone.utc))
    job_started.send(job_id, message.message_id, now)
    dataset_ids = params["ids"]
    assert len(dataset_ids) == 1
    return [
        {
            "result_id": "cutout",
            "url": "s3://some-bucket/some/path",
            "mime_type": "application/fits",
        }
    ]


@pytest_asyncio.fixture
async def app() -> AsyncIterator[FastAPI]:
    """Return a configured test application.

    Wraps the application in a lifespan manager so that startup and shutdown
    events are sent during test execution.  Initializes the database before
    creating the app to ensure that data is dropped from a persistent database
    between test cases.
    """
    logger = structlog.get_logger(config.logger_name)
    broker.flush_all()
    broker.emit_after("process_boot")
    engine = create_database_engine(
        config.database_url, config.database_password
    )
    await initialize_database(engine, logger, schema=Base.metadata, reset=True)
    await engine.dispose()
    async with LifespanManager(main.app):
        uws_dependency.override_policy(ImageCutoutPolicy(cutout_test, logger))
        yield main.app


@pytest_asyncio.fixture
async def client(app: FastAPI) -> AsyncIterator[AsyncClient]:
    """Return an ``httpx.AsyncClient`` configured to talk to the test app."""
    async with AsyncClient(app=app, base_url="https://example.com/") as client:
        yield client


@pytest.fixture(autouse=True)
def mock_google_storage() -> Iterator[MockStorageClient]:
    yield from patch_google_storage(
        expected_expiration=timedelta(minutes=15), bucket_name="some-bucket"
    )
