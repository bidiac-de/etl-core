from __future__ import annotations
from uuid import uuid4

from typing import Optional, Literal

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, ConfigDict, Field

from src.context.context import Context
from src.context.environment import Environment
from src.context.credentials import Credentials
from src.context.context_registry import ContextRegistry
from src.context.secure_context_adapter import SecureContextAdapter

from src.context.secrets.secret_provider import SecretProvider
from src.context.secrets.keyring_provider import KeyringSecretProvider

router = APIRouter(prefix="/contexts", tags=["contexts"])

# Dependencies


def get_secret_provider(service: Optional[str] = None) -> SecretProvider:
    """
    Default secret provider using OS keychain. You can override this dependency
    in tests with an in-memory implementation.
    """
    svc = service or "sep-sose-2025/default"
    return KeyringSecretProvider(service=svc)


# Request/response Schemas


class ContextCreateRequest(BaseModel):
    context: Context
    keyring_service: Optional[str] = Field(
        default=None,
        description="Override secret storage service name for this registration.",
    )


class CredentialsCreateRequest(BaseModel):
    credentials: Credentials
    keyring_service: Optional[str] = Field(
        default=None,
        description="Override secret storage service name for this registration.",
    )


class ProviderCreateResponse(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    id: str
    kind: Literal["context", "credentials"]
    environment: Optional[Environment] = None
    parameters_registered: int = 0


class ProviderInfoResponse(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    id: str
    kind: Literal["context", "credentials"]
    environment: Optional[Environment] = None
    provider_class: str = "SecureContextAdapter"


class ProviderListItem(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)
    id: str
    kind: Literal["context", "credentials"]
    name: Optional[str] = None
    environment: Optional[Environment] = None


# register a general Context
@router.post(
    "/context",
    response_model=ProviderCreateResponse,
    status_code=status.HTTP_201_CREATED,
)
def create_context_provider(
    req: ContextCreateRequest,
    default_provider: SecretProvider = Depends(get_secret_provider),
) -> ProviderCreateResponse:
    try:
        ctx = req.context
        context_id = str(uuid4())

        secret_store = (
            default_provider
            if req.keyring_service is None
            else get_secret_provider(req.keyring_service)
        )

        adapter = SecureContextAdapter(
            provider_id=context_id,
            secret_store=secret_store,
            context=ctx,
        )

        # Move secure parameters into the secret store
        adapter.bootstrap_to_store()

        # Register the secure provider facade
        ContextRegistry.register(context_id, adapter)

        secure_count = sum(
            1 for p in ctx.parameters.values() if getattr(p, "is_secure", False)
        )

        return ProviderCreateResponse(
            id=context_id,
            kind="context",
            environment=ctx.environment,
            parameters_registered=secure_count,
        )
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to register context: {exc}",
        ) from exc


# register Credentials
@router.post(
    "/credentials",
    response_model=ProviderCreateResponse,
    status_code=status.HTTP_201_CREATED,
)
def create_credentials_provider(
    req: CredentialsCreateRequest,
    default_provider: SecretProvider = Depends(get_secret_provider),
) -> ProviderCreateResponse:
    try:
        creds = req.credentials
        credentials_id = str(uuid4())

        secret_store = (
            default_provider
            if req.keyring_service is None
            else get_secret_provider(req.keyring_service)
        )

        adapter = SecureContextAdapter(
            provider_id=credentials_id,
            secret_store=secret_store,
            credentials=creds,
        )

        # Store password secure in secret store
        adapter.bootstrap_to_store()

        # reduce in-memory exposure of password
        req.credentials.password = None

        # Register the secure provider facade
        ContextRegistry.register(credentials_id, adapter)

        # secure_count = 1, password is only secure parameter
        return ProviderCreateResponse(
            id=credentials_id,
            kind="credentials",
            environment=None,
            parameters_registered=1,
        )
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to register credentials: {exc}",
        ) from exc


# list all registered providers and their data, no secrets
@router.get(
    "/",
    response_model=list[ProviderListItem],
    status_code=status.HTTP_200_OK,
)
def list_providers() -> list[ProviderListItem]:
    """
    Return a flat list of all registered providers with safe data
    so the GUI can present a picker during component creation.
    """
    items: list[ProviderListItem] = []
    for pid in ContextRegistry.list_ids():
        try:
            provider = ContextRegistry.resolve(pid)
        except KeyError:
            # If concurrently removed, just skip
            continue

        kind: Literal["context", "credentials"] = "credentials"
        name: Optional[str] = None
        env: Optional[Environment] = None

        if isinstance(provider, SecureContextAdapter):
            if provider.context is not None:
                kind = "context"
                name = getattr(provider.context, "name", None)
                env = provider.context.environment
            elif provider.credentials is not None:
                kind = "credentials"
                name = getattr(provider.credentials, "name", None)

        items.append(
            ProviderListItem(
                id=pid,
                kind=kind,
                name=name,
                environment=env,
            )
        )
    return items


# get context infos by id (no secrets)
@router.get(
    "/{id}",
    response_model=ProviderInfoResponse,
    status_code=status.HTTP_200_OK,
)
def get_provider(id: str) -> ProviderInfoResponse:
    try:
        adapter = ContextRegistry.resolve(id)
    except KeyError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)
        ) from exc

    kind: Literal["context", "credentials"] = "credentials"
    environment: Optional[Environment] = None

    if isinstance(adapter, SecureContextAdapter) and adapter.context is not None:
        kind = "context"
        environment = adapter.context.environment

    return ProviderInfoResponse(
        id=id,
        kind=kind,
        environment=environment,
        provider_class=adapter.__class__.__name__,
    )


# unregister and purge secrets by id
@router.delete("/{id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_provider(id: str) -> None:
    try:
        adapter = ContextRegistry.resolve(id)
    except KeyError:
        return  # idempotent 204

    try:
        adapter.delete_from_store()
    finally:
        ContextRegistry.unregister(id)
