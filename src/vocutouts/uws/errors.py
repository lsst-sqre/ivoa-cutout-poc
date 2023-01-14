"""Error handlers for UWS and DALI services.

Currently these error handlers return ``text/plain`` errors.  VOTable errors
may be a better choice, but revision 1.0 of the SODA standard only allows
``text/plain`` errors for sync routes.
"""

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
