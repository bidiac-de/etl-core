from __future__ import annotations

import json
from typing import Any, Dict, Optional

import typer
import requests

from etl_core.api.cli.adapters import api_base_url

from etl_core.persistence.handlers.schedule_handler import ScheduleHandler
from etl_core.persistence.table_definitions import TriggerType
from etl_core.scheduling.commands import (
    CreateScheduleCommand,
    UpdateScheduleCommand,
    DeleteScheduleCommand,
    PauseScheduleCommand,
    ResumeScheduleCommand,
    RunNowScheduleCommand,
)


schedules_app = typer.Typer(help="Manage job schedules")


def _parse_json(s: Optional[str]) -> Dict[str, Any]:
    if not s:
        return {}
    return json.loads(s)


@schedules_app.command("create")
def create(
    name: str = typer.Argument(..., help="Schedule name"),
    job_id: str = typer.Option(..., "--job", help="Target job id"),
    context: str = typer.Option("DEV", help="Execution env: DEV/TEST/PROD"),
    trigger_type: TriggerType = typer.Option(..., case_sensitive=False),
    trigger_args: Optional[str] = typer.Option(
        None, help="JSON string with trigger args"
    ),
    paused: bool = typer.Option(False, help="Create schedule in paused state"),
):
    base = api_base_url()
    if base:
        payload = dict(
            name=name,
            job_id=job_id,
            context=context,
            trigger_type=trigger_type.value if hasattr(trigger_type, "value") else str(trigger_type),
            trigger_args=_parse_json(trigger_args),
            paused=paused,
        )
        r = requests.post(f"{base}/schedules/", json=payload)
        r.raise_for_status()
        typer.echo(r.json())
        return
    body = CreateScheduleCommand(
        name=name,
        job_id=job_id,
        context=context,
        trigger_type=trigger_type,
        trigger_args=_parse_json(trigger_args),
        paused=paused,
    ).execute()
    typer.echo(body.id)


@schedules_app.command("list")
def list_cmd():
    base = api_base_url()
    if base:
        r = requests.get(f"{base}/schedules/")
        r.raise_for_status()
        rows = r.json()
        for rj in rows:
            typer.echo(
                f"{rj['id']}\t{rj['name']}\t{rj['job_id']}\t{rj['context']}\t{rj['trigger_type']}\tpaused={rj['is_paused']}"
            )
        return
    rows = ScheduleHandler().list()
    for r in rows:
        typer.echo(
            f"{r.id}\t{r.name}\t{r.job_id}\t{r.context}\t{r.trigger_type}\tpaused={r.is_paused}"
        )


@schedules_app.command("get")
def get_cmd(schedule_id: str):
    base = api_base_url()
    if base:
        r = requests.get(f"{base}/schedules/{schedule_id}")
        if r.status_code == 404:
            raise typer.Exit(code=1)
        r.raise_for_status()
        typer.echo(json.dumps(r.json(), indent=2))
        return
    r = ScheduleHandler().get(schedule_id)
    if r is None:
        raise typer.Exit(code=1)
    typer.echo(json.dumps({
        "id": r.id,
        "name": r.name,
        "job_id": r.job_id,
        "context": r.context,
        "trigger_type": r.trigger_type,
        "trigger_args": r.trigger_args,
        "is_paused": r.is_paused,
    }, indent=2))


@schedules_app.command("update")
def update_cmd(
    schedule_id: str,
    name: Optional[str] = typer.Option(None),
    job_id: Optional[str] = typer.Option(None, "--job"),
    context: Optional[str] = typer.Option(None),
    trigger_type: Optional[TriggerType] = typer.Option(None, case_sensitive=False),
    trigger_args: Optional[str] = typer.Option(None, help="JSON string"),
    paused: Optional[bool] = typer.Option(None),
):
    base = api_base_url()
    if base:
        payload: Dict[str, Any] = {}
        if name is not None:
            payload["name"] = name
        if job_id is not None:
            payload["job_id"] = job_id
        if context is not None:
            payload["context"] = context
        if trigger_type is not None:
            payload["trigger_type"] = trigger_type.value if hasattr(trigger_type, "value") else str(trigger_type)
        if trigger_args is not None:
            payload["trigger_args"] = _parse_json(trigger_args)
        if paused is not None:
            payload["paused"] = paused
        r = requests.put(f"{base}/schedules/{schedule_id}", json=payload)
        if r.status_code == 404:
            raise typer.Exit(code=1)
        r.raise_for_status()
        typer.echo(r.json())
        return
    body = UpdateScheduleCommand(
        schedule_id=schedule_id,
        name=name,
        job_id=job_id,
        context=context,
        trigger_type=trigger_type,
        trigger_args=_parse_json(trigger_args) if trigger_args is not None else None,
        paused=paused,
    ).execute()
    typer.echo(body.id)


@schedules_app.command("delete")
def delete_cmd(schedule_id: str):
    base = api_base_url()
    if base:
        r = requests.delete(f"{base}/schedules/{schedule_id}")
        if r.status_code == 404:
            raise typer.Exit(code=1)
        r.raise_for_status()
        typer.echo("OK")
        return
    DeleteScheduleCommand(schedule_id).execute()
    typer.echo("OK")


@schedules_app.command("pause")
def pause_cmd(schedule_id: str):
    base = api_base_url()
    if base:
        r = requests.post(f"{base}/schedules/{schedule_id}/pause")
        if r.status_code == 404:
            raise typer.Exit(code=1)
        r.raise_for_status()
        typer.echo("OK")
        return
    PauseScheduleCommand(schedule_id).execute()
    typer.echo("OK")


@schedules_app.command("resume")
def resume_cmd(schedule_id: str):
    base = api_base_url()
    if base:
        r = requests.post(f"{base}/schedules/{schedule_id}/resume")
        if r.status_code == 404:
            raise typer.Exit(code=1)
        r.raise_for_status()
        typer.echo("OK")
        return
    ResumeScheduleCommand(schedule_id).execute()
    typer.echo("OK")


@schedules_app.command("run-now")
def run_now_cmd(schedule_id: str):
    base = api_base_url()
    if base:
        r = requests.post(f"{base}/schedules/{schedule_id}/run-now")
        if r.status_code == 404:
            raise typer.Exit(code=1)
        r.raise_for_status()
        typer.echo(json.dumps(r.json(), indent=2))
        return
    # run asynchronous part in a nested event loop
    import asyncio

    async def _run():
        await RunNowScheduleCommand(schedule_id).execute()

    asyncio.run(_run())
