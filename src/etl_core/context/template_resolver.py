from __future__ import annotations

import re
from typing import Any, Dict, Optional

from etl_core.context.context import Context
from etl_core.context.context_parameter import ContextParameter
from etl_core.context.credentials_mapping_context import CredentialsMappingContext
from etl_core.context.environment import normalize_environment
from etl_core.context.secrets.secret_utils import create_secret_provider
from etl_core.errors import ContextResolutionError, TemplateResolutionError
from etl_core.persistence.handlers.credentials_handler import CredentialsHandler

_FULL_TOKEN_RE = re.compile(r"^\$\{ctx\.([A-Za-z0-9_]+)\}$")
_TOKEN_RE = re.compile(r"\$\{ctx\.([A-Za-z0-9_]+)\}")

STRUCTURAL_FIELDS = {
    "context_id",
    "routes",
    "out_port_schemas",
    "in_port_schemas",
    "layout",
    "metadata_",
    "extra_output_ports",
    "extra_input_ports",
}


def _normalize_env_str(environment: Optional[str]) -> Optional[str]:
    if environment is None:
        return None
    return normalize_environment(environment)


def _resolve_context_parameter_values(
    *,
    context_id: str,
    ctx_obj: Context,
) -> Dict[str, Any]:
    params = getattr(ctx_obj, "parameters", {}) or {}
    if not isinstance(params, dict):
        return {}

    values: Dict[str, Any] = {}
    secret_store = create_secret_provider()
    for key, item in params.items():
        if isinstance(item, ContextParameter):
            if item.is_secure:
                secret_key = f"{context_id}/{key}"
                try:
                    values[key] = secret_store.get(secret_key)
                except Exception as exc:  # noqa: BLE001
                    raise ContextResolutionError(
                        f"Missing secure context value for key '{key}' in context "
                        f"'{context_id}'.",
                        code="CONTEXT_SECURE_VALUE_MISSING",
                        context={"context_id": context_id, "key": key},
                    ) from exc
            else:
                values[key] = item.value
            continue

        # Graceful fallback for plain dict/values in legacy tests.
        if isinstance(item, dict):
            values[key] = item.get("value")
        else:
            values[key] = item
    return values


def _context_value_lookup(
    *,
    context_id: str,
    ctx_obj: Context,
    environment: Optional[str] = None,
    creds_handler: Optional[CredentialsHandler] = None,
) -> Dict[str, Any]:
    values = _resolve_context_parameter_values(context_id=context_id, ctx_obj=ctx_obj)
    if not isinstance(ctx_obj, CredentialsMappingContext):
        return values

    env_override = _normalize_env_str(environment)
    creds, _cred_id = ctx_obj.resolve_active_credentials(
        override_env=env_override,
        repo=creds_handler,
    )
    values.update(
        {
            "host": creds.host,
            "port": creds.port,
            "database": creds.database,
            "user": creds.user,
            "password": creds.decrypted_password,
            "pool_max_size": creds.pool_max_size,
            "pool_timeout_s": creds.pool_timeout_s,
        }
    )
    return values


def _resolve_string_template(value: str, lookup: Dict[str, Any]) -> Any:
    full_match = _FULL_TOKEN_RE.match(value)
    if full_match:
        key = full_match.group(1)
        if key not in lookup:
            raise TemplateResolutionError(
                f"Unknown context placeholder key: '{key}'.",
                code="TEMPLATE_UNKNOWN_KEY",
                context={"key": key},
            )
        return lookup[key]

    if "${ctx." not in value:
        return value

    def _replace(match: re.Match[str]) -> str:
        key = match.group(1)
        if key not in lookup:
            raise TemplateResolutionError(
                f"Unknown context placeholder key: '{key}'.",
                code="TEMPLATE_UNKNOWN_KEY",
                context={"key": key},
            )
        replacement = lookup[key]
        return "" if replacement is None else str(replacement)

    return _TOKEN_RE.sub(_replace, value)


def _resolve_value(value: Any, lookup: Dict[str, Any]) -> Any:
    if isinstance(value, str):
        return _resolve_string_template(value, lookup)
    if isinstance(value, list):
        return [_resolve_value(item, lookup) for item in value]
    if isinstance(value, dict):
        return {k: _resolve_value(v, lookup) for k, v in value.items()}
    return value


def apply_context_templates_to_component(
    component: Any,
    *,
    environment: Optional[str] = None,
    creds_handler: Optional[CredentialsHandler] = None,
) -> int:
    """
    Resolve ${ctx.key} placeholders in component fields in-place.

    Returns number of component fields that changed.
    """
    context_id = getattr(component, "context_id", None)
    if not isinstance(context_id, str) or context_id.strip() == "":
        return 0

    resolver = getattr(component, "get_resolved_context", None)
    if not callable(resolver):
        return 0
    ctx_obj = resolver()
    if not isinstance(ctx_obj, Context):
        return 0

    lookup = _context_value_lookup(
        context_id=context_id,
        ctx_obj=ctx_obj,
        environment=environment,
        creds_handler=creds_handler,
    )

    fields = getattr(component.__class__, "model_fields", {}) or {}
    changed = 0
    for field_name in fields.keys():
        if field_name in STRUCTURAL_FIELDS:
            continue
        current_value = getattr(component, field_name, None)
        resolved_value = _resolve_value(current_value, lookup)
        if resolved_value != current_value:
            setattr(component, field_name, resolved_value)
            changed += 1
    return changed
