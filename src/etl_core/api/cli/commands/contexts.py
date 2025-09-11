from __future__ import annotations

import json
from typing import Optional

import typer

from etl_core.persistance.errors import PersistNotFoundError
from etl_core.api.cli.wiring import pick_clients

BASE_URL = "http://127.0.0.1:8000"

contexts_app = typer.Typer(help="Manage contexts and credentials providers.")


@contexts_app.command("create-context")
def create_context(
    path: str = typer.Argument(..., help="JSON file for Context model."),
    keyring_service: Optional[str] = typer.Option(
        None, help="Override keyring service (defaults to API/endpoint default)."
    ),
    remote: bool = typer.Option(False),
    base_url: str = typer.Option(BASE_URL),
) -> None:
    _, __, ctxs = pick_clients(remote, base_url)
    payload = json.load(open(path, encoding="utf-8"))
    resp = ctxs.create_context(payload, keyring_service)
    typer.echo(json.dumps(resp, indent=2))


@contexts_app.command("create-credentials")
def create_credentials(
    path: str = typer.Argument(..., help="JSON file for Credentials model."),
    keyring_service: Optional[str] = typer.Option(
        None, help="Override keyring service (defaults to API/endpoint default)."
    ),
    remote: bool = typer.Option(False),
    base_url: str = typer.Option(BASE_URL),
) -> None:
    _, __, ctxs = pick_clients(remote, base_url)
    payload = json.load(open(path, encoding="utf-8"))
    resp = ctxs.create_credentials(payload, keyring_service)
    typer.echo(json.dumps(resp, indent=2))


@contexts_app.command("create-context-mapping")
def create_context_mapping(
    path: str = typer.Argument(..., help="JSON file for CredentialsMappingContext."),
    remote: bool = typer.Option(False),
    base_url: str = typer.Option(BASE_URL),
) -> None:
    _, __, ctxs = pick_clients(remote, base_url)
    payload = json.load(open(path, encoding="utf-8"))
    try:
        resp = ctxs.create_context_mapping(payload)
    except PersistNotFoundError as exc:
        typer.echo(str(exc))
        raise typer.Exit(code=1)
    typer.echo(json.dumps(resp, indent=2))


@contexts_app.command("list")
def list_providers(
    remote: bool = typer.Option(False),
    base_url: str = typer.Option(BASE_URL),
) -> None:
    _, __, ctxs = pick_clients(remote, base_url)
    typer.echo(json.dumps(ctxs.list_providers(), indent=2))


@contexts_app.command("get")
def get_provider(
    provider_id: str,
    remote: bool = typer.Option(False),
    base_url: str = typer.Option(BASE_URL),
) -> None:
    _, __, ctxs = pick_clients(remote, base_url)
    try:
        info = ctxs.get_provider(provider_id)
    except PersistNotFoundError:
        typer.echo(f"Provider '{provider_id}' not found")
        raise typer.Exit(code=1)
    typer.echo(json.dumps(info, indent=2))


@contexts_app.command("delete")
def delete_provider(
    provider_id: str,
    remote: bool = typer.Option(False),
    base_url: str = typer.Option(BASE_URL),
) -> None:
    _, __, ctxs = pick_clients(remote, base_url)
    try:
        ctxs.delete_provider(provider_id)
    except PersistNotFoundError:
        typer.echo(f"Provider '{provider_id}' not found")
        raise typer.Exit(code=1)
    typer.echo(f"Deleted provider '{provider_id}'")
