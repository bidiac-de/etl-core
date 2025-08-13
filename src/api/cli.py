from __future__ import annotations

import json
from typing import Any, Dict, Protocol, List, Optional, Tuple

import requests
import typer

from src.persistance.handlers.job_handler import JobHandler
from src.job_execution.job_execution_handler import JobExecutionHandler
from src.persistance.errors import PersistNotFoundError

app = typer.Typer(help="ETL control CLI using the same core as the API.")


# capabilities CLI needs
class JobsPort(Protocol):
    def create(self, cfg: Dict[str, Any]) -> str: ...
    def get(self, job_id: str) -> Dict[str, Any]: ...
    def update(self, job_id: str, cfg: Dict[str, Any]) -> str: ...
    def delete(self, job_id: str) -> None: ...
    def list_brief(self) -> List[Dict[str, Any]]: ...


class ExecutionPort(Protocol):
    def start(self, job_id: str) -> Dict[str, Any]: ...


# Local adapters (direct handler calls)


class LocalJobsClient(JobsPort):
    def __init__(self, job_handler: Optional[JobHandler] = None) -> None:
        self.job_handler = job_handler or JobHandler()

    def create(self, cfg: Dict[str, Any]) -> str:
        # Accept raw dict so CLI can pass JSON directly;
        from src.persistance.configs.job_config import JobConfig

        row = self.job_handler.create_job_entry(JobConfig(**cfg))
        return row.id

    def get(self, job_id: str) -> Dict[str, Any]:
        job = self.job_handler.load_runtime_job(job_id)
        data = job.model_dump()
        data["id"] = job.id
        return data

    def update(self, job_id: str, cfg: Dict[str, Any]) -> str:
        from src.persistance.configs.job_config import JobConfig

        row = self.job_handler.update(job_id, JobConfig(**cfg))
        return row.id

    def delete(self, job_id: str) -> None:
        self.job_handler.delete(job_id)

    def list_brief(self) -> List[Dict[str, Any]]:
        return self.job_handler.list_jobs_brief()


class LocalExecutionClient(ExecutionPort):
    def __init__(self, exec_handler: Optional[JobExecutionHandler] = None) -> None:
        self.exec_handler = exec_handler or JobExecutionHandler()
        self.jobs = LocalJobsClient()

    def start(self, job_id: str) -> Dict[str, Any]:
        runtime_job = self.jobs.job_handler.load_runtime_job(job_id)
        execution = self.exec_handler.execute_job(runtime_job)
        return {
            "job_id": job_id,
            "status": "started",
            "execution_id": execution.id,
            "max_attempts": execution.max_attempts,
        }


# HTTP adapters (remote control if needed)


class HttpJobsClient(JobsPort):
    def __init__(self, base_url: str) -> None:
        self.base_url = base_url.rstrip("/")

    def create(self, cfg: Dict[str, Any]) -> str:
        r = requests.post(f"{self.base_url}/jobs", json=cfg)
        r.raise_for_status()
        return r.json()

    def get(self, job_id: str) -> Dict[str, Any]:
        r = requests.get(f"{self.base_url}/jobs/{job_id}")
        if r.status_code == 404:
            raise PersistNotFoundError(f"Job {job_id!r} not found")
        r.raise_for_status()
        return r.json()

    def update(self, job_id: str, cfg: Dict[str, Any]) -> str:
        r = requests.put(f"{self.base_url}/jobs/{job_id}", json=cfg)
        if r.status_code == 404:
            raise PersistNotFoundError(f"Job {job_id!r} not found")
        r.raise_for_status()
        return r.json()

    def delete(self, job_id: str) -> None:
        r = requests.delete(f"{self.base_url}/jobs/{job_id}")
        if r.status_code == 404:
            raise PersistNotFoundError(f"Job {job_id!r} not found")
        r.raise_for_status()

    def list_brief(self) -> List[Dict[str, Any]]:
        r = requests.get(f"{self.base_url}/jobs")
        r.raise_for_status()
        return r.json()


class HttpExecutionClient(ExecutionPort):
    def __init__(self, base_url: str) -> None:
        self.base_url = base_url.rstrip("/")

    def start(self, job_id: str) -> Dict[str, Any]:
        r = requests.post(f"{self.base_url}/execution/{job_id}")
        if r.status_code == 404:
            raise PersistNotFoundError(f"Job {job_id!r} not found")
        r.raise_for_status()
        return r.json()


# Wiring helpers


def _pick_clients(remote: bool, base_url: str) -> Tuple[JobsPort, ExecutionPort]:
    if remote:
        return HttpJobsClient(base_url), HttpExecutionClient(base_url)
    return LocalJobsClient(), LocalExecutionClient()


# CLI commands

_DEFAULT_ADDRESS = "http://127.0.0.1:8000"


@app.command()
def create_job(
    path: str,
    remote: bool = typer.Option(False, help="Use HTTP instead of local core."),
    base_url: str = typer.Option(_DEFAULT_ADDRESS, help="API base URL."),
) -> None:
    cfg: Dict[str, Any] = json.load(open(path, encoding="utf-8"))
    jobs, _ = _pick_clients(remote, base_url)
    job_id = jobs.create(cfg)
    typer.echo(f"Created job {job_id}")


@app.command()
def get_job(
    job_id: str,
    remote: bool = typer.Option(False),
    base_url: str = typer.Option(_DEFAULT_ADDRESS),
) -> None:
    jobs, _ = _pick_clients(remote, base_url)
    try:
        data = jobs.get(job_id)
    except PersistNotFoundError:
        typer.echo(f"Job with ID {job_id} not found")
        raise typer.Exit(code=1)
    typer.echo(json.dumps(data, indent=2))


@app.command()
def update_job(
    job_id: str,
    path: str,
    remote: bool = typer.Option(False),
    base_url: str = typer.Option(_DEFAULT_ADDRESS),
) -> None:
    cfg: Dict[str, Any] = json.load(open(path, encoding="utf-8"))
    jobs, _ = _pick_clients(remote, base_url)
    try:
        updated_id = jobs.update(job_id, cfg)
    except PersistNotFoundError:
        typer.echo(f"Job with ID {job_id} not found")
        raise typer.Exit(code=1)
    typer.echo(f"Updated job {updated_id}")


@app.command()
def delete_job(
    job_id: str,
    remote: bool = typer.Option(False),
    base_url: str = typer.Option(_DEFAULT_ADDRESS),
) -> None:
    jobs, _ = _pick_clients(remote, base_url)
    try:
        jobs.delete(job_id)
    except PersistNotFoundError:
        typer.echo(f"Job with ID {job_id} not found")
        raise typer.Exit(code=1)
    typer.echo(f"Deleted job {job_id}")


@app.command()
def list_jobs(
    remote: bool = typer.Option(False),
    base_url: str = typer.Option(_DEFAULT_ADDRESS),
) -> None:
    jobs, _ = _pick_clients(remote, base_url)
    typer.echo(json.dumps(jobs.list_brief(), indent=2))


@app.command()
def start_execution(
    job_id: str,
    remote: bool = typer.Option(False),
    base_url: str = typer.Option(_DEFAULT_ADDRESS),
) -> None:
    _, execs = _pick_clients(remote, base_url)
    try:
        info = execs.start(job_id)
    except PersistNotFoundError:
        typer.echo(f"Job with ID {job_id} not found")
        raise typer.Exit(code=1)
    typer.echo(json.dumps(info, indent=2))


if __name__ == "__main__":
    app()
