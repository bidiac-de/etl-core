from __future__ import annotations

import json
from typing import Optional

import typer

from etl_core.context.environment import Environment
from etl_core.persistence.errors import PersistNotFoundError
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
) -> None:
    _, execs, _ = pick_clients()
    try:
        info = execs.start(job_id, environment)
    except PersistNotFoundError:
        typer.echo(f"Job with ID {job_id} not found")
        raise typer.Exit(code=1)
    typer.echo(json.dumps(info, indent=2))


@execution_app.command("list")
def list_executions(
    job_id: Optional[str] = typer.Option(None),
    status: Optional[str] = typer.Option(
        None, help="Filter by status (e.g. RUNNING, SUCCESS, FAILED)."
    ),
    environment: Optional[str] = typer.Option(None, help="Filter by environment."),
    started_after: Optional[str] = typer.Option(
        None, help="ISO datetime filter (e.g. 2025-09-13T12:00:00)."
    ),
    started_before: Optional[str] = typer.Option(
        None, help="ISO datetime filter (e.g. 2025-09-13T18:00:00)."
    ),
    sort_by: str = typer.Option(
        "started_at", help="Sort by: started_at | finished_at | status."
    ),
    order: str = typer.Option("desc", help="asc | desc."),
    limit: int = typer.Option(50, min=1, max=200),
    offset: int = typer.Option(0, min=0),
) -> None:
    _, execs, _ = pick_clients()
    payload = execs.list_executions(
        job_id=job_id,
        status=status,
        environment=environment,
        started_after=started_after,
        started_before=started_before,
        sort_by=sort_by,
        order=order,
        limit=limit,
        offset=offset,
    )
    typer.echo(json.dumps(payload, indent=2))


@execution_app.command("get")
def get_execution(execution_id: str) -> None:
    _, execs, _ = pick_clients()
    try:
        detail = execs.get(execution_id)
    except PersistNotFoundError:
        typer.echo(f"Execution with ID {execution_id} not found")
        raise typer.Exit(code=1)
    typer.echo(json.dumps(detail, indent=2))


@execution_app.command("attempts")
def list_attempts(execution_id: str) -> None:
    _, execs, _ = pick_clients()
    try:
        rows = execs.attempts(execution_id)
    except PersistNotFoundError:
        typer.echo(f"Execution with ID {execution_id} not found")
        raise typer.Exit(code=1)
    typer.echo(json.dumps(rows, indent=2))
