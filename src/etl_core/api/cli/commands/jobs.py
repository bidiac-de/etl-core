from __future__ import annotations

import json
from typing import Any, Dict

import typer

from etl_core.persistance.errors import PersistNotFoundError
from etl_core.api.cli.wiring import pick_clients

jobs_app = typer.Typer(help="Manage jobs.")

BASE_URL = "http://127.0.0.1:8000"


@jobs_app.command("create")
def create_job(
    path: str,
    remote: bool = typer.Option(False, help="Use HTTP instead of local core."),
    base_url: str = typer.Option(BASE_URL, help="API base URL."),
) -> None:
    cfg: Dict[str, Any] = json.load(open(path, encoding="utf-8"))
    jobs, _, _ = pick_clients(remote, base_url)
    job_id = jobs.create(cfg)
    typer.echo(f"Created job {job_id}")


@jobs_app.command("get")
def get_job(
    job_id: str,
    remote: bool = typer.Option(False),
    base_url: str = typer.Option(BASE_URL),
) -> None:
    jobs, _, _ = pick_clients(remote, base_url)
    try:
        data = jobs.get(job_id)
    except PersistNotFoundError:
        typer.echo(f"Job with ID {job_id} not found")
        raise typer.Exit(code=1)
    typer.echo(json.dumps(data, indent=2))


@jobs_app.command("update")
def update_job(
    job_id: str,
    path: str,
    remote: bool = typer.Option(False),
    base_url: str = typer.Option(BASE_URL),
) -> None:
    cfg: Dict[str, Any] = json.load(open(path, encoding="utf-8"))
    jobs, _, _ = pick_clients(remote, base_url)
    try:
        updated_id = jobs.update(job_id, cfg)
    except PersistNotFoundError:
        typer.echo(f"Job with ID {job_id} not found")
        raise typer.Exit(code=1)
    typer.echo(f"Updated job {updated_id}")


@jobs_app.command("delete")
def delete_job(
    job_id: str,
    remote: bool = typer.Option(False),
    base_url: str = typer.Option(BASE_URL),
) -> None:
    jobs, _, _ = pick_clients(remote, base_url)
    try:
        jobs.delete(job_id)
    except PersistNotFoundError:
        typer.echo(f"Job with ID {job_id} not found")
        raise typer.Exit(code=1)
    typer.echo(f"Deleted job {job_id}")


@jobs_app.command("list")
def list_jobs(
    remote: bool = typer.Option(False),
    base_url: str = typer.Option(BASE_URL),
) -> None:
    jobs, _, _ = pick_clients(remote, base_url)
    typer.echo(json.dumps(jobs.list_brief(), indent=2))
