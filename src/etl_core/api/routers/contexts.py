from __future__ import annotations
from uuid import uuid4
from typing import Optional, Literal, Iterable

from fastapi import APIRouter, Depends, HTTPException, status, Response
from pydantic import BaseModel, ConfigDict, Field

from src.etl_core.context.context import Context
from src.etl_core.context.environment import Environment
from src.etl_core.context.credentials import Credentials
from src.etl_core.context.context_registry import ContextRegistry
from src.etl_core.context.secure_context_adapter import SecureContextAdapter

from src.etl_core.context.secrets.secret_provider import SecretProvider
from src.etl_core.context.secrets.keyring_provider import KeyringSecretProvider

from src.etl_core.persistance.handlers.credentials_handler import CredentialsHandler
from src.etl_core.persistance.handlers.context_handler import ContextHandler

router = APIRouter(prefix="/contexts", tags=["contexts"])

# Dependencies


def get_secret_provider(service: Optional[str] = None) -> SecretProvider:
    """
    Default secret provider using OS keychain. You can override this dependency
    in tests with an in-memory implementation.
    """
    svc = service or "sep-sose-2025/default"
    return KeyringSecretProvider(service=svc)


def get_credentials_handler() -> CredentialsHandler:
    return CredentialsHandler()


def get_context_handler() -> ContextHandler:
    return ContextHandler()


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


def _choose_secret_store(
    default_provider: SecretProvider,
    override_service: Optional[str],
) -> SecretProvider:
    return (
        default_provider
        if override_service is None
        else get_secret_provider(override_service)
    )


def _append_item(
    items: list[ProviderListItem],
    pid: str,
    kind: Literal["context", "credentials"],
    name: Optional[str],
    env: Optional[Environment],
) -> None:
    items.append(ProviderListItem(id=pid, kind=kind, name=name, environment=env))


def _dedupe(items: Iterable[ProviderListItem]) -> list[ProviderListItem]:
    seen: set[str] = set()
    out: list[ProviderListItem] = []
    for it in items:
        if it.id in seen:
            continue
        seen.add(it.id)
        out.append(it)
    return out


# register a general Context
@router.post(
    "/context",
    response_model=ProviderCreateResponse,
    status_code=status.HTTP_201_CREATED,
)
def create_context_provider(
    req: ContextCreateRequest,
    default_provider: SecretProvider = Depends(get_secret_provider),
    ctx_handler: ContextHandler = Depends(get_context_handler),
) -> ProviderCreateResponse:
    try:
        ctx = req.context
        context_id = str(uuid4())

        secret_store = _choose_secret_store(default_provider, req.keyring_service)

        adapter = SecureContextAdapter(
            provider_id=context_id,
            secret_store=secret_store,
            context=ctx,
        )

        # Move secure parameters into secret store
        adapter.bootstrap_to_store()

        # persist non-secret context metadata + parameter presence
        non_secure = {
            k: p.value for k, p in ctx.parameters.items() if p.is_secure is False
        }
        secure_keys = [k for k, p in ctx.parameters.items() if p.is_secure]
        ctx_handler.upsert(
            provider_id=context_id,
            name=ctx.name,
            environment=ctx.environment.value,
            non_secure_params=non_secure,
            secure_param_keys=secure_keys,
        )

        secure_count = len(secure_keys)

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
    creds_handler: CredentialsHandler = Depends(get_credentials_handler),
) -> ProviderCreateResponse:
    try:
        creds = req.credentials
        credentials_id = str(uuid4())

        secret_store = _choose_secret_store(default_provider, req.keyring_service)

        adapter = SecureContextAdapter(
            provider_id=credentials_id,
            secret_store=secret_store,
            credentials=creds,
        )

        # Store password securely in secret store
        adapter.bootstrap_to_store()

        # reduce in-memory exposure of password
        req.credentials.password = None

        # persist non-secret metadata (password stays in keyring)
        creds_handler.upsert(provider_id=credentials_id, creds=creds)

        # secure_count = 1, password is the only secure parameter
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
def list_providers(
    ctx_handler: ContextHandler = Depends(get_context_handler),
    creds_handler: CredentialsHandler = Depends(get_credentials_handler),
) -> list[ProviderListItem]:
    """
    Return a flat list of providers that are persisted
    """
    items: list[ProviderListItem] = []

    for row in ctx_handler.list_all():
        _append_item(
            items,
            row.provider_id,
            "context",
            row.name,
            Environment(row.environment),
        )
    for row in creds_handler.list_all():
        _append_item(items, row.provider_id, "credentials", row.name, None)

    return _dedupe(items)


# get context infos by id (no secrets)
@router.get(
    "/{id}",
    response_model=ProviderInfoResponse,
    status_code=status.HTTP_200_OK,
)
def get_provider(
    id: str,
    ctx_handler: ContextHandler = Depends(get_context_handler),
    creds_handler: CredentialsHandler = Depends(get_credentials_handler),
) -> ProviderInfoResponse:

    row_ctx = ctx_handler.get_by_provider_id(id)
    if row_ctx is not None:
        return ProviderInfoResponse(
            id=id,
            kind="context",
            environment=Environment(row_ctx.environment),
            provider_class="SecureContextAdapter",
        )
    row_creds = creds_handler.get_by_provider_id(id)
    if row_creds is not None:
        return ProviderInfoResponse(
            id=id,
            kind="credentials",
            environment=None,
            provider_class="SecureContextAdapter",
        )

    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND, detail=f"Provider '{id}' not found"
    )


# unregister and purge secrets by id
@router.delete("/{id}", status_code=status.HTTP_204_NO_CONTENT, response_class=Response)
def delete_provider(
    id: str,
    creds_handler: CredentialsHandler = Depends(get_credentials_handler),
    ctx_handler: ContextHandler = Depends(get_context_handler),
) -> Response:
    # Delete secrets + in-memory registry, if present
    try:
        adapter = ContextRegistry.resolve(id)
    except KeyError:
        adapter = None

    if adapter is not None:
        try:
            adapter.delete_from_store()
        finally:
            ContextRegistry.unregister(id)

    # Also remove persisted metadata (try both kinds, since only one will match)
    try:
        creds_handler.delete_by_provider_id(id)
    except Exception:
        pass
    try:
        ctx_handler.delete_by_provider_id(id)
    except Exception:
        pass

    return Response(status_code=status.HTTP_204_NO_CONTENT)
