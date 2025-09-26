from __future__ import annotations

from uuid import uuid4
from typing import Optional, Literal, Iterable, Union, Annotated, Any, Dict

from fastapi import APIRouter, Depends, HTTPException, status, Response
from pydantic import BaseModel, ConfigDict, Field, SecretStr
from sqlalchemy.exc import IntegrityError

from src.etl_core.context.context import Context
from src.etl_core.context.environment import Environment
from src.etl_core.context.credentials import Credentials
from src.etl_core.context.credentials_mapping_context import (
    CredentialsMappingContext,
)
from src.etl_core.context.context_registry import ContextRegistry
from src.etl_core.context.secure_context_adapter import SecureContextAdapter

from src.etl_core.context.secrets.secret_provider import SecretProvider
from src.etl_core.context.secrets.secret_utils import create_secret_provider

from src.etl_core.persistence.handlers.credentials_handler import CredentialsHandler
from src.etl_core.persistence.handlers.context_handler import ContextHandler

from etl_core.api.dependencies import (
    get_context_handler,
    get_credentials_handler,
)

router = APIRouter(prefix="/contexts", tags=["contexts"])


def get_secret_provider() -> SecretProvider:
    """
    Resolve the configured secret provider (memory or keyring).
    We only fail if initialization itself errors out.
    """
    try:
        provider = create_secret_provider()
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"failed to initialize secret provider: {exc}",
        ) from exc
    return provider


class ContextCreateRequest(BaseModel):
    context: Context


class CredentialsCreateRequest(BaseModel):
    credentials: Credentials


class CredentialsMappingContextCreateRequest(BaseModel):
    context: CredentialsMappingContext


class ContextResponse(BaseModel):
    id: str
    kind: Literal["context"]
    name: str
    environment: str
    parameters: dict[str, Optional[str]] = {}
    credentials_ids: dict[str, str] = {}


class CredentialsResponse(BaseModel):
    id: str
    kind: Literal["credentials"]
    name: str
    user: str
    host: str
    port: int
    database: str
    pool_max_size: Optional[int] = None
    pool_timeout_s: Optional[int] = None
    password: Optional[SecretStr] = None


ProviderGetResponse = Annotated[
    Union[ContextResponse, CredentialsResponse],
    Field(discriminator="kind"),
]


class ProviderCreateResponse(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    id: str
    kind: Literal["context", "credentials"]
    environment: Optional[Environment] = None
    parameters_registered: int = 0


class ProviderListItem(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)
    id: str
    kind: Literal["context", "credentials"]
    name: Optional[str] = None
    environment: Optional[Environment] = None


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

        adapter = SecureContextAdapter(
            provider_id=context_id,
            secret_store=default_provider,
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
            context_id=context_id,
            name=ctx.name,
            environment=ctx.environment.value,
            non_secure_params=non_secure,
            secure_param_keys=secure_keys,
        )

        secure_count = len(secure_keys)

        return ProviderCreateResponse(
            id=context_id,
            kind="context",
            environment=ctx.environment.value,
            parameters_registered=secure_count,
        )
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to register context: {exc}",
        ) from exc


@router.post(
    "/credentials-mapping-context",
    response_model=ProviderCreateResponse,
    status_code=status.HTTP_201_CREATED,
)
def create_credentials_mapping_context(
    req: CredentialsMappingContextCreateRequest,
    ctx_handler: ContextHandler = Depends(get_context_handler),
    creds_handler: CredentialsHandler = Depends(get_credentials_handler),
) -> ProviderCreateResponse:
    """
    Register a CredentialsMappingContext:
      - persists Context metadata
      - validates all referenced credentials providers exist
      - persists env->credentials_id mapping rows
    """
    try:
        cmc = req.context
        context_id = str(uuid4())

        # Validate referenced credentials exist
        missing: list[str] = []
        for cred_id in cmc.credentials_ids.values():
            if creds_handler.get_by_id(cred_id) is None:
                missing.append(cred_id)
        if missing:
            raise ValueError(
                "Unknown credentials provider_id(s): " + ", ".join(sorted(missing))
            )

        mapping = {}
        for env, cred_id in cmc.credentials_ids.items():
            mapping[env] = cred_id

        ctx_handler.upsert_credentials_mapping_context(
            context_id=context_id,
            name=cmc.name,
            environment=cmc.environment.value,
            mapping_env_to_credentials_id=mapping,
        )

        return ProviderCreateResponse(
            id=context_id,
            kind="context",
            environment=cmc.environment.value,
            parameters_registered=len(mapping),
        )
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to register credentials mapping context: {exc}",
        ) from exc


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

        adapter = SecureContextAdapter(
            provider_id=credentials_id,
            secret_store=default_provider,
            credentials=creds,
        )

        # Store password in keyring
        result = adapter.bootstrap_to_store()
        if result.errors:
            problems = ", ".join(f"{k}: {v}" for k, v in result.errors.items())
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Failed to store credentials securely ({problems})",
            )

        saved_id = creds_handler.upsert(creds, credentials_id=credentials_id)

        # Reduce in-memory exposure only after successful persistence
        req.credentials.password = None

        return ProviderCreateResponse(
            id=saved_id,
            kind="credentials",
            environment=None,
            parameters_registered=1,
        )
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to register credentials: {exc}",
        ) from exc


@router.get(
    "/",
    response_model=list[ProviderListItem],
    status_code=status.HTTP_200_OK,
)
def list_providers(
    ctx_handler: ContextHandler = Depends(get_context_handler),
    creds_handler: CredentialsHandler = Depends(get_credentials_handler),
) -> list[ProviderListItem]:
    items: list[ProviderListItem] = []

    for row in ctx_handler.list_all():
        _append_item(
            items,
            row.id,
            "context",
            row.name,
            Environment(row.environment),
        )
    for row in creds_handler.list_all():
        _append_item(items, row.id, "credentials", row.name, None)

    return _dedupe(items)


@router.get(
    "/{id}",
    response_model=ProviderGetResponse,
    status_code=status.HTTP_200_OK,
)
def get_provider(
    id: str,
    ctx_handler: ContextHandler = Depends(get_context_handler),
    creds_handler: CredentialsHandler = Depends(get_credentials_handler),
):
    row_ctx = ctx_handler.get_by_id(id)
    if row_ctx is not None:
        ctx_obj, _ctx_id = row_ctx
        name: Optional[str] = getattr(ctx_obj, "name", None)
        environment: Any = getattr(ctx_obj, "environment", None)

        if name is None:
            try:
                for r in ctx_handler.list_all():
                    if getattr(r, "id", None) == id:
                        name = getattr(r, "name", "")
                        environment = environment or getattr(r, "environment", None)
                        break
            except Exception:  # noqa: BLE001
                name = ""

        return ContextResponse(
            id=id,
            kind="context",
            name=name or "",
            environment=str(environment) if environment is not None else "",
            parameters=getattr(ctx_obj, "parameters", {}),
            credentials_ids=getattr(ctx_obj, "credentials_ids", {}),
        )

    row_creds = creds_handler.get_by_id(id)
    if row_creds is not None:
        creds, _creds_id = row_creds
        return CredentialsResponse(
            id=id,
            kind="credentials",
            name=creds.name,
            user=creds.user,
            host=creds.host,
            port=creds.port,
            database=creds.database,
            pool_max_size=creds.pool_max_size,
            pool_timeout_s=creds.pool_timeout_s,
            password=creds.password,
        )

    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND, detail=f"Provider '{id}' not found"
    )


@router.delete(
    "/{id}",
    response_model=Dict,
    status_code=status.HTTP_200_OK,
    summary="Delete Context or Credentials by ID",
    description=(
        "Deletes the provider (context or credentials) matching the given ID. "
        "On success returns a JSON message, consistent with the Jobs delete endpoint."
    ),
)
def delete_provider(
    id: str,
    creds_handler: CredentialsHandler = Depends(get_credentials_handler),
    ctx_handler: ContextHandler = Depends(get_context_handler),
) -> Dict[str, str]:
    # Best-effort cleanup of secret store (if this ID is registered)
    try:
        adapter = ContextRegistry.resolve(id)
    except KeyError:
        adapter = None

    if adapter is not None:
        try:
            adapter.delete_from_store()
        finally:
            ContextRegistry.unregister(id)

    try:
        deleted_creds = creds_handler.delete_by_id(id)
        deleted_ctx = ctx_handler.delete_by_id(id)
    except IntegrityError as exc:
        # Likely FK reference from jobs/links/etc
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "code": "DB_INTEGRITY_ERROR",
                "message": "Delete blocked by database constraints.",
                "id": id,
            },
        ) from exc
    except Exception as exc:  # noqa: BLE001
        # Unexpected database/runtime error
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "code": "DB_ERROR",
                "message": "Unexpected error while deleting provider.",
                "id": id,
            },
        ) from exc

    if not (deleted_creds or deleted_ctx):
        # Nothing matched this id
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "code": "PROVIDER_NOT_FOUND",
                "message": f"No provider found for id {id!r}.",
                "id": id,
            },
        )

    if deleted_ctx:
        return {"message": f"Context {id!r} deleted successfully"}
    return {"message": f"Credentials {id!r} deleted successfully"}
