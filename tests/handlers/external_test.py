"""Catch-all tests for miscellaneous external routes."""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from httpx import AsyncClient

from vocutouts.config import config


@pytest.mark.asyncio
async def test_get_index(client: AsyncClient) -> None:
    """Test ``GET /api/cutout``"""
    response = await client.get("/api/cutout")
    assert response.status_code == 200
    data = response.json()
    metadata = data["metadata"]
    assert metadata["name"] == config.name
    assert isinstance(metadata["version"], str)
    assert isinstance(metadata["description"], str)
    assert isinstance(metadata["repository_url"], str)
    assert isinstance(metadata["documentation_url"], str)


@pytest.mark.asyncio
async def test_availability(client: AsyncClient) -> None:
    r = await client.get("/api/cutout/availability")
    assert r.status_code == 200
    assert r.json() == {"available": True}


@pytest.mark.asyncio
async def test_capabilities(client: AsyncClient) -> None:
    r = await client.get("/api/cutout/capabilities")
    assert r.status_code == 200
    assert r.json() == {
        "availability_url": "https://example.com/api/cutout/availability",
        "capabilities_url": "https://example.com/api/cutout/capabilities",
        "soda_sync_url": "https://example.com/api/cutout/sync",
        "soda_async_url": "https://example.com/api/cutout/jobs",
    }


@pytest.mark.asyncio
async def test_capabilities_urls(app: FastAPI) -> None:
    """Test the scheme in the URLs for the capabilities endpoint.

    When running in a Kubernetes cluster behind an ingress that terminates
    TLS, the request as seen by the application will be ``http``, but we want
    the generated URLs to honor ``X-Forwarded-Proto`` and thus use ``https``.
    We also want to honor the ``Host`` header.
    """
    async with AsyncClient(app=app, base_url="http://foo.com/") as client:
        r = await client.get(
            "/api/cutout/capabilities",
            headers={
                "Host": "example.org",
                "X-Forwarded-For": "10.10.10.10",
                "X-Forwarded-Proto": "https",
                "X-Forwarded-Host": "foo.com",
            },
        )
        assert r.status_code == 200
        assert r.json() == {
            "availability_url": "https://example.org/api/cutout/availability",
            "capabilities_url": "https://example.org/api/cutout/capabilities",
            "soda_sync_url": "https://example.org/api/cutout/sync",
            "soda_async_url": "https://example.org/api/cutout/jobs",
        }
