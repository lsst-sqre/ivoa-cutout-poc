"""Error handlers for UWS and DALI services."""

from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from .exceptions import UWSError

__all__ = ["install_error_handlers"]


async def _uws_error_handler(request: Request, exc: UWSError) -> JSONResponse:
    return JSONResponse(
        status_code=exc.status_code, content={"detail": [exc.to_dict()]}
    )


def install_error_handlers(app: FastAPI) -> None:
    """Install error handlers for UWS errors.

    This function must be called during application setup for any FastAPI app
    using the UWS layer for correct error message handling.
    """
    app.exception_handler(UWSError)(_uws_error_handler)
