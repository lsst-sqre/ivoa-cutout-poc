"""Tests for the ivoacutoutpoc.handlers.external module and routes."""

from __future__ import annotations

import pytest
from httpx import AsyncClient

from ivoacutoutpoc.config import config


@pytest.mark.asyncio
async def test_get_index(client: AsyncClient) -> None:
    """Test ``GET /ivoa-cutout-poc/``"""
    response = await client.get("/ivoa-cutout-poc/")
    assert response.status_code == 200
    data = response.json()
    metadata = data["metadata"]
    assert metadata["name"] == config.name
    assert isinstance(metadata["version"], str)
    assert isinstance(metadata["description"], str)
    assert isinstance(metadata["repository_url"], str)
    assert isinstance(metadata["documentation_url"], str)
