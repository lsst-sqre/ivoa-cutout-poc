"""Administrative command-line interface."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Optional

import click
import structlog
import uvicorn
from fastapi.openapi.utils import get_openapi
from safir.asyncio import run_with_asyncio
from safir.database import create_database_engine, initialize_database

from .config import config
from .main import app
from .uws.schema import Base


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
@click.version_option(message="%(version)s")
def main() -> None:
    """vo-cutouts main.

    Administrative command-line interface for vo-cutouts.
    """
    pass


@main.command()
@click.argument("topic", default=None, required=False, nargs=1)
@click.pass_context
def help(ctx: click.Context, topic: str | None) -> None:
    """Show help for any command."""
    # The help command implementation is taken from
    # https://www.burgundywall.com/post/having-click-help-subcommand
    if topic:
        if topic in main.commands:
            click.echo(main.commands[topic].get_help(ctx))
        else:
            raise click.UsageError(f"Unknown help topic {topic}", ctx)
    else:
        assert ctx.parent
        click.echo(ctx.parent.get_help())


@main.command()
@click.option(
    "--port", default=8080, type=int, help="Port to run the application on."
)
def run(port: int) -> None:
    """Run the application (for testing only)."""
    uvicorn.run(
        "vocutouts.main:app", port=port, reload=True, reload_dirs=["src"]
    )


@main.command()
@click.option(
    "--reset", is_flag=True, help="Delete all existing database data."
)
@run_with_asyncio
async def init(reset: bool) -> None:
    """Initialize the database storage."""
    logger = structlog.get_logger(config.logger_name)
    engine = create_database_engine(
        config.database_url,
        config.database_password,
    )
    await initialize_database(
        engine, logger, schema=Base.metadata, reset=reset
    )
    await engine.dispose()


@main.command()
@click.option(
    "--output",
    default=None,
    type=click.Path(path_type=Path),
    help="Output path (output to stdout if not given).",
)
def openapi_schema(output: Optional[Path]) -> None:
    """Generate the OpenAPI schema."""
    description = app.description
    schema = get_openapi(
        title=app.title,
        description=description,
        version=app.version,
        routes=app.routes,
    )
    if output:
        output.parent.mkdir(exist_ok=True)
        with output.open("w") as f:
            json.dump(schema, f)
    else:
        json.dump(schema, sys.stdout)
