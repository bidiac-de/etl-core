from __future__ import annotations

import json
from typing import Any, Dict

import typer

from etl_core.persistence.errors import PersistNotFoundError
from etl_core.api.cli.wiring import pick_clients

jobs_app = typer.Typer(help="Manage jobs.")


@jobs_app.command("create")
def create_job(path: str) -> None:
    cfg: Dict[str, Any] = json.load(open(path, encoding="utf-8"))
    jobs, _, _ = pick_clients()
    job_id = jobs.create(cfg)
    typer.echo(f"Created job {job_id}")


@jobs_app.command("get")
def get_job(job_id: str) -> None:
    jobs, _, _ = pick_clients()
    try:
        data = jobs.get(job_id)
    except PersistNotFoundError:
        typer.echo(f"Job with ID {job_id} not found")
        raise typer.Exit(code=1)
    typer.echo(json.dumps(data, indent=2))


@jobs_app.command("update")
def update_job(job_id: str, path: str) -> None:
    cfg: Dict[str, Any] = json.load(open(path, encoding="utf-8"))
    jobs, _, _ = pick_clients()
    try:
        updated_id = jobs.update(job_id, cfg)
    except PersistNotFoundError:
        typer.echo(f"Job with ID {job_id} not found")
        raise typer.Exit(code=1)
    typer.echo(f"Updated job {updated_id}")


@jobs_app.command("delete")
def delete_job(job_id: str) -> None:
    jobs, _, _ = pick_clients()
    try:
        jobs.delete(job_id)
    except PersistNotFoundError:
        typer.echo(f"Job with ID {job_id} not found")
        raise typer.Exit(code=1)
    typer.echo(f"Deleted job {job_id}")


@jobs_app.command("list")
def list_jobs() -> None:
    jobs, _, _ = pick_clients()
    typer.echo(json.dumps(jobs.list_brief(), indent=2))
