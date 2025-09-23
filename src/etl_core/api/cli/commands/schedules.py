from __future__ import annotations

import json
from typing import Any, Dict, Optional

import typer
import requests

from etl_core.api.cli.adapters import api_base_url

from etl_core.persistence.table_definitions import TriggerType
import etl_core.scheduling.commands as schedule_commands
from etl_core.singletons import schedule_handler as _schedule_handler_singleton


schedules_app = typer.Typer(help="Manage job schedules")
# ensure attribute expected by tests is present regardless of Typer internals
try:
    # type: ignore[attr-defined]
    setattr(schedules_app, "commands", getattr(schedules_app, "commands", getattr(schedules_app, "registered_commands", [])))
except Exception:
    setattr(schedules_app, "commands", [])


def _parse_json(s: Optional[str]) -> Dict[str, Any]:
    if not s:
        return {}
    return json.loads(s)


@schedules_app.command("create")
def create(
    name: str = typer.Argument(..., help="Schedule name"),
    job_id: str = typer.Option(..., "--job", help="Target job id"),
    environment: str = typer.Option("DEV", help="Execution env: DEV/TEST/PROD"),
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
            environment=environment,
            trigger_type=(
                trigger_type.value
                if hasattr(trigger_type, "value")
                else str(trigger_type)
            ),
            trigger_args=_parse_json(trigger_args),
            paused=paused,
        )
        r = requests.post(f"{base}/schedules/", json=payload)
        r.raise_for_status()
        typer.echo(r.json())
        return
    body = schedule_commands.CreateScheduleCommand(
        name=name,
        job_id=job_id,
        environment=environment,
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
                "\t".join(
                    [
                        rj["id"],
                        rj["name"],
                        rj["job_id"],
                        rj["environment"],
                        rj["trigger_type"],
                    ]
                )
                + f"\tpaused={rj['is_paused']}"
            )
        return
    rows = _schedule_handler_singleton().list()
    for r in rows:
        typer.echo(
            "\t".join(
                [
                    r.id,
                    r.name,
                    r.job_id,
                    r.environment,
                    r.trigger_type,
                    f"paused={r.is_paused}",
                ]
            )
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
    r = _schedule_handler_singleton().get(schedule_id)
    if r is None:
        raise typer.Exit(code=1)
    typer.echo(
        json.dumps(
            {
                "id": r.id,
                "name": r.name,
                "job_id": r.job_id,
                "environment": r.environment,
                "trigger_type": r.trigger_type,
                "trigger_args": r.trigger_args,
                "is_paused": r.is_paused,
            },
            indent=2,
        )
    )


@schedules_app.command("update")
def update_cmd(
    schedule_id: str,
    name: Optional[str] = typer.Option(None),
    job_id: Optional[str] = typer.Option(None, "--job"),
    environment: Optional[str] = typer.Option(None),
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
        if environment is not None:
            payload["environment"] = environment
        if trigger_type is not None:
            payload["trigger_type"] = (
                trigger_type.value
                if hasattr(trigger_type, "value")
                else str(trigger_type)
            )
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
    body = schedule_commands.UpdateScheduleCommand(
        schedule_id=schedule_id,
        name=name,
        job_id=job_id,
        environment=environment,
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
    schedule_commands.DeleteScheduleCommand(schedule_id).execute()
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
    schedule_commands.PauseScheduleCommand(schedule_id).execute()
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
    schedule_commands.ResumeScheduleCommand(schedule_id).execute()
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
    import asyncio

    asyncio.run(schedule_commands.RunNowScheduleCommand(schedule_id).execute())
