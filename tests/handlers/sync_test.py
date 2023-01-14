"""Tests for sync cutout requests."""

from __future__ import annotations

from typing import Any

import pytest
from dramatiq import Worker
from httpx import AsyncClient

from vocutouts.broker import broker


@pytest.mark.asyncio
async def test_sync(client: AsyncClient) -> None:
    worker = Worker(broker, worker_timeout=100)
    worker.start()

    try:
        r = await client.post(
            "/api/cutout/sync",
            headers={"X-Auth-Request-User": "someone"},
            json={
                "parameters": {
                    "ids": ["1:2:band:value"],
                    "stencils": [
                        {
                            "type": "circle",
                            "center": {"ra": 0, "dec": -2},
                            "radius": 2,
                        }
                    ],
                }
            },
        )
        assert r.status_code == 303
        assert r.headers["Location"] == "https://example.com/some/path"
    finally:
        worker.stop()


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
            "/api/cutout/sync",
            headers={"X-Auth-Request-User": "user"},
            json={
                "ids": ["1:2:band:value"],
                "stencils": [stencil],
            },
        )
        assert r.status_code == 422, f"Stencil {stencil}"

    # Multiple ids with valid stencils aren't allowed.
    r = await client.post(
        "/api/cutout/sync",
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
        "/api/cutout/sync",
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
