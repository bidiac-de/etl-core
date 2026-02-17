from __future__ import annotations

from uuid import uuid4
from typing import Optional, Literal, Iterable, Union, Annotated, Any, Dict

from fastapi import APIRouter, Depends, status
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy.exc import IntegrityError

from etl_core.context.context import Context
from etl_core.context.context_parameter import ContextParameter
from etl_core.context.environment import normalize_environment
from etl_core.context.credentials import Credentials
from etl_core.context.credentials_mapping_context import (
    CredentialsMappingContext,
)
from etl_core.context.context_registry import ContextRegistry
from etl_core.context.secure_context_adapter import SecureContextAdapter

from etl_core.context.secrets.secret_provider import SecretProvider
from etl_core.context.secrets.secret_utils import create_secret_provider

from etl_core.persistence.handlers.credentials_handler import CredentialsHandler
from etl_core.persistence.handlers.context_handler import ContextHandler

from etl_core.api.dependencies import (
    get_context_handler,
    get_credentials_handler,
)

from etl_core.api.http_errors import http_400, http_404, http_409, http_500

router = APIRouter(prefix="/contexts", tags=["contexts"])


def get_secret_provider() -> SecretProvider:
    """
    Resolve the configured secret provider (memory or keyring).
    We only fail if initialization itself errors out.
    """
    try:
        provider = create_secret_provider()
    except Exception as exc:  # noqa: BLE001
        raise http_500(
            "SECRET_PROVIDER_INIT_FAILED",
            f"Failed to initialize secret provider: {exc}",
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
    has_password: bool = False
    password: Optional[str] = None


class ContextKeyItem(BaseModel):
    key: str
    source: Literal["context_parameter", "credentials"]
    value_type: str
    secret: bool


class ContextKeysResponse(BaseModel):
    context_id: str
    environment: str
    keys: list[ContextKeyItem]


ProviderGetResponse = Annotated[
    Union[ContextResponse, CredentialsResponse],
    Field(discriminator="kind"),
]


class ProviderCreateResponse(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    id: str
    kind: Literal["context", "credentials"]
    environment: Optional[str] = None
    parameters_registered: int = 0


class ProviderListItem(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)
    id: str
    kind: Literal["context", "credentials"]
    name: Optional[str] = None
    environment: Optional[str] = None


def _append_item(
    items: list[ProviderListItem],
    pid: str,
    kind: Literal["context", "credentials"],
    name: Optional[str],
    env: Optional[str],
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


def _context_params_payload(ctx_obj: Context) -> dict[str, Optional[str]]:
    params = getattr(ctx_obj, "parameters", {}) or {}
    out: dict[str, Optional[str]] = {}
    if isinstance(params, dict):
        for key, value in params.items():
            if isinstance(value, ContextParameter):
                out[key] = value.value
            elif isinstance(value, dict):
                out[key] = value.get("value")  # type: ignore[assignment]
            else:
                out[key] = value  # type: ignore[assignment]
    return out


def _normalize_mapping_payload(cmc: CredentialsMappingContext) -> dict[str, str]:
    if not cmc.credentials_ids:
        raise ValueError(
            "At least one credentials mapping entry is required for "
            "credentials-mapping-context."
        )

    normalized: dict[str, str] = {}
    for env_key_raw, cred_id_raw in cmc.credentials_ids.items():
        env_key = normalize_environment(env_key_raw)
        cred_id = str(cred_id_raw).strip()
        if cred_id == "":
            raise ValueError(
                f"credentials_ids[{env_key!r}] must reference a credentials id."
            )
        normalized[env_key] = cred_id

    default_env = normalize_environment(cmc.environment)
    if default_env not in normalized:
        raise ValueError(
            "credentials_ids must include an entry for the default environment "
            f"{default_env!r}."
        )

    return normalized


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
            environment=ctx.environment,
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
        raise http_400(
            "CONTEXT_REGISTER_FAILED",
            f"Failed to register context: {exc}",
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
        mapping = _normalize_mapping_payload(cmc)

        # Validate referenced credentials exist
        missing: list[str] = []
        for cred_id in mapping.values():
            if creds_handler.get_by_id(cred_id) is None:
                missing.append(cred_id)
        if missing:
            raise ValueError(
                "Unknown credentials provider_id(s): " + ", ".join(sorted(missing))
            )

        ctx_handler.upsert_credentials_mapping_context(
            context_id=context_id,
            name=cmc.name,
            environment=cmc.environment,
            mapping_env_to_credentials_id=mapping,
        )

        return ProviderCreateResponse(
            id=context_id,
            kind="context",
            environment=cmc.environment,
            parameters_registered=len(mapping),
        )
    except Exception as exc:  # noqa: BLE001
        raise http_400(
            "CREDENTIALS_MAPPING_REGISTER_FAILED",
            f"Failed to register credentials mapping context: {exc}",
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
            raise http_400(
                "CREDENTIALS_STORE_FAILED",
                f"Failed to store credentials securely ({problems})",
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
        raise http_400(
            "CREDENTIALS_REGISTER_FAILED",
            f"Failed to register credentials: {exc}",
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
            row.environment,
        )
    for row in creds_handler.list_all():
        _append_item(items, row.id, "credentials", row.name, None)

    return _dedupe(items)


@router.get(
    "/{id}/keys",
    response_model=ContextKeysResponse,
    status_code=status.HTTP_200_OK,
)
def get_context_keys(
    id: str,
    environment: Optional[str] = None,
    ctx_handler: ContextHandler = Depends(get_context_handler),
    creds_handler: CredentialsHandler = Depends(get_credentials_handler),
) -> ContextKeysResponse:
    row_ctx = ctx_handler.get_by_id(id)
    if row_ctx is None:
        raise http_404(
            "CONTEXT_NOT_FOUND",
            f"Context '{id}' not found.",
            context_id=id,
        )

    ctx_obj, _ctx_id = row_ctx
    key_items: list[ContextKeyItem] = []

    if isinstance(ctx_obj, CredentialsMappingContext):
        resolved_env = ctx_obj.determine_active_environment(environment)
        try:
            creds, _resolved_creds_id = ctx_obj.resolve_active_credentials(
                override_env=environment,
                repo=creds_handler,
            )
        except Exception as exc:  # noqa: BLE001
            raise http_400(
                "CONTEXT_KEYS_RESOLVE_FAILED",
                f"Failed to resolve credentials keys: {exc}",
                context_id=id,
                environment=resolved_env,
            ) from exc

        key_items.extend(
            [
                ContextKeyItem(
                    key="host",
                    source="credentials",
                    value_type="string",
                    secret=False,
                ),
                ContextKeyItem(
                    key="port",
                    source="credentials",
                    value_type="number",
                    secret=False,
                ),
                ContextKeyItem(
                    key="database",
                    source="credentials",
                    value_type="string",
                    secret=False,
                ),
                ContextKeyItem(
                    key="user",
                    source="credentials",
                    value_type="string",
                    secret=False,
                ),
                ContextKeyItem(
                    key="password",
                    source="credentials",
                    value_type="string",
                    secret=True,
                ),
                ContextKeyItem(
                    key="pool_max_size",
                    source="credentials",
                    value_type="number",
                    secret=False,
                ),
                ContextKeyItem(
                    key="pool_timeout_s",
                    source="credentials",
                    value_type="number",
                    secret=False,
                ),
            ]
        )

        # read once so failures happen in this endpoint, not during key assist usage
        _ = creds.decrypted_password
        return ContextKeysResponse(
            context_id=id,
            environment=resolved_env,
            keys=key_items,
        )

    env_value = (
        normalize_environment(environment)
        if environment is not None
        else str(ctx_obj.environment)
    )
    params = getattr(ctx_obj, "parameters", {}) or {}
    if isinstance(params, dict):
        for key, parameter in params.items():
            if isinstance(parameter, ContextParameter):
                key_items.append(
                    ContextKeyItem(
                        key=key,
                        source="context_parameter",
                        value_type=parameter.type or "string",
                        secret=bool(parameter.is_secure),
                    )
                )

    return ContextKeysResponse(context_id=id, environment=env_value, keys=key_items)


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
            environment=(str(environment) if environment is not None else ""),
            parameters=_context_params_payload(ctx_obj),
            credentials_ids=getattr(ctx_obj, "credentials_ids", {}),
        )

    try:
        row_creds = creds_handler.get_by_id(id)
    except Exception as exc:  # noqa: BLE001
        raise http_500(
            "CREDENTIALS_LOAD_FAILED",
            "Failed to load credentials provider.",
            provider_id=id,
        ) from exc
    if row_creds is not None:
        creds, _creds_id = row_creds
        decrypted_password = getattr(creds, "decrypted_password", None)
        if decrypted_password is None:
            raw_password = getattr(creds, "password", None)
            has_password = raw_password not in (None, "", False)
        else:
            has_password = True
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
            has_password=has_password,
            password=None,
        )

    raise http_404(
        "PROVIDER_NOT_FOUND",
        f"Provider '{id}' not found.",
        provider_id=id,
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
    context_ref = ctx_handler.get_by_id(id)
    if isinstance(context_ref, tuple) and len(context_ref) == 2:
        find_refs = getattr(ctx_handler, "find_component_context_references", None)
        refs = find_refs(id) if callable(find_refs) else []
        if isinstance(refs, list) and refs:
            raise http_409(
                "CONTEXT_IN_USE",
                "Delete blocked: context is referenced by one or more components.",
                context_id=id,
                references=refs,
            )

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
        raise http_409(
            "DB_INTEGRITY_ERROR",
            "Delete blocked by database constraints.",
            id=id,
        ) from exc
    except Exception as exc:  # noqa: BLE001
        # Unexpected database/runtime error
        raise http_500(
            "DB_ERROR",
            "Unexpected error while deleting provider.",
            id=id,
        ) from exc

    if not (deleted_creds or deleted_ctx):
        # Nothing matched this id
        raise http_404(
            "PROVIDER_NOT_FOUND",
            f"No provider found for id {id!r}.",
            id=id,
        )

    if deleted_ctx:
        return {"message": f"Context {id!r} deleted successfully"}
    return {"message": f"Credentials {id!r} deleted successfully"}
