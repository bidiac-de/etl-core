from __future__ import annotations

import json
from typing import Optional

import typer

from etl_core.context.environment import Environment
from etl_core.persistance.errors import PersistNotFoundError
from etl_core.api.cli.wiring import pick_clients

execution_app = typer.Typer(help="Start and control executions.")


@execution_app.command("start")
def start_execution(
    job_id: str,
    environment: Optional[Environment] = typer.Option(
        None,
        case_sensitive=False,
        help="Optional environment (e.g. TEST, DEV, PROD).",
    ),
    remote: bool = typer.Option(False),
    base_url: str = typer.Option("http://127.0.0.1:8000"),
) -> None:
    _, execs, _ = pick_clients(remote, base_url)
    try:
        info = execs.start(job_id, environment)
    except PersistNotFoundError:
        typer.echo(f"Job with ID {job_id} not found")
        raise typer.Exit(code=1)
    typer.echo(json.dumps(info, indent=2))
