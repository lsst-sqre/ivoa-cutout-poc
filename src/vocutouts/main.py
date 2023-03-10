"""The main application factory for the vo-cutouts service.

Notes
-----
Be aware that, following the normal pattern for FastAPI services, the app is
constructed when this module is loaded and is not deferred until a function is
called.
"""

from importlib.metadata import metadata, version

import structlog
from fastapi import FastAPI
from safir.dependencies.http_client import http_client_dependency
from safir.logging import configure_logging
from safir.middleware.ivoa import CaseInsensitiveQueryMiddleware
from safir.middleware.x_forwarded import XForwardedMiddleware

from .actors import cutout
from .config import config
from .handlers.external import external_router
from .handlers.internal import internal_router
from .models.parameters import CutoutParameters
from .policy import ImageCutoutPolicy
from .uws.dependencies import uws_dependency
from .uws.errors import install_error_handlers

__all__ = ["app", "config"]


configure_logging(
    profile=config.profile,
    log_level=config.log_level,
    name=config.logger_name,
)

app = FastAPI(
    title="ivoa-cutout-poc",
    description=metadata("ivoa-cutout-poc")["Summary"],
    version=version("ivoa-cutout-poc"),
    openapi_url=f"/api/{config.name}/openapi.json",
    docs_url=f"/api/{config.name}/docs",
    redoc_url=f"/api/{config.name}/redoc",
)
"""The main FastAPI application for vo-cutouts."""

# Attach the routers.
app.include_router(internal_router)
app.include_router(
    external_router,
    prefix=f"/api/{config.name}",
    responses={401: {"description": "Unauthenticated"}},
)

# Install middleware.
app.add_middleware(XForwardedMiddleware)
app.add_middleware(CaseInsensitiveQueryMiddleware)

# Install error handlers.
install_error_handlers(app)


@app.on_event("startup")
async def startup_event() -> None:
    logger = structlog.get_logger(config.logger_name)
    await uws_dependency.initialize(
        config=config.uws_config(),
        policy=ImageCutoutPolicy(cutout, logger),
        param_type=CutoutParameters,
        logger=logger,
    )


@app.on_event("shutdown")
async def shutdown_event() -> None:
    await http_client_dependency.aclose()
    await uws_dependency.aclose()
