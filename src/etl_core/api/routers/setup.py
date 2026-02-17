from __future__ import annotations

import os
from typing import List, Literal

from fastapi import APIRouter, Depends, status
from pydantic import BaseModel, Field

from etl_core.components.data_operations.filter.comparison_rule import (
    RULE_LOGICAL_OPERATORS,
    RULE_OPERATORS,
)
from etl_core.components.wiring.column_definition import DataType
from etl_core.context.environment import (
    DEFAULT_ENVIRONMENTS,
    ENVIRONMENT_ICONS,
    ENVIRONMENT_LABELS,
)
from etl_core.api.dependencies import get_context_handler
from etl_core.persistence.handlers.context_handler import ContextHandler

router = APIRouter(prefix="/setup", tags=["setup"])

CONTRACT_VERSION = "core-studio-v1"
CORE_VERSION = "0.1.0"
_SETUP_KEY_ENV = "ETL_SETUP_ACCESS_KEY"

# Default icon and label for environments not in the built-in map.
_DEFAULT_ICON = "fa-solid fa-globe"
_DEFAULT_LABEL_PREFIX = ""


class SetupValidationConfig(BaseModel):
    mode: Literal["none", "shared_key"]
    required: bool
    endpoint: str = "/setup/validate"
    key_env_var: str = _SETUP_KEY_ENV


class EnvironmentCapability(BaseModel):
    value: str
    label: str
    icon: str


class SetupCapabilitiesResponse(BaseModel):
    contract_version: str = CONTRACT_VERSION
    core_version: str = CORE_VERSION
    setup_validation: SetupValidationConfig
    environments: List[EnvironmentCapability]
    rule_operators: List[str]
    rule_logical_operators: List[str]
    data_types: List[str]


class SetupValidateRequest(BaseModel):
    key: str = Field(min_length=1)


class SetupValidateResponse(BaseModel):
    valid: bool


def _configured_setup_key() -> str | None:
    raw = os.getenv(_SETUP_KEY_ENV)
    if raw is None:
        return None
    stripped = raw.strip()
    return stripped or None


def _validation_config() -> SetupValidationConfig:
    configured = _configured_setup_key()
    if configured is None:
        return SetupValidationConfig(mode="none", required=False)
    return SetupValidationConfig(mode="shared_key", required=True)


def _environment_capabilities(
    ctx_handler: ContextHandler,
) -> List[EnvironmentCapability]:
    """Return built-in environments merged with any custom ones from contexts."""
    seen: set[str] = set()
    items: List[EnvironmentCapability] = []

    # Built-in defaults first (preserves order).
    for env_value in DEFAULT_ENVIRONMENTS:
        seen.add(env_value)
        items.append(
            EnvironmentCapability(
                value=env_value,
                label=ENVIRONMENT_LABELS.get(env_value, env_value.title()),
                icon=ENVIRONMENT_ICONS.get(env_value, _DEFAULT_ICON),
            )
        )

    # Custom environments discovered from persisted contexts.
    try:
        context_envs = ctx_handler.list_distinct_environments()
    except Exception:  # noqa: BLE001
        context_envs = []

    for env_value in context_envs:
        if env_value in seen:
            continue
        seen.add(env_value)
        items.append(
            EnvironmentCapability(
                value=env_value,
                label=ENVIRONMENT_LABELS.get(env_value, env_value.title()),
                icon=ENVIRONMENT_ICONS.get(env_value, _DEFAULT_ICON),
            )
        )

    return items


@router.get(
    "/capabilities",
    response_model=SetupCapabilitiesResponse,
    status_code=status.HTTP_200_OK,
    summary="Get studio integration capabilities",
)
def setup_capabilities(
    ctx_handler: ContextHandler = Depends(get_context_handler),
) -> SetupCapabilitiesResponse:
    return SetupCapabilitiesResponse(
        setup_validation=_validation_config(),
        environments=_environment_capabilities(ctx_handler),
        rule_operators=list(RULE_OPERATORS),
        rule_logical_operators=list(RULE_LOGICAL_OPERATORS),
        data_types=[item.value for item in DataType],
    )


@router.post(
    "/validate",
    response_model=SetupValidateResponse,
    status_code=status.HTTP_200_OK,
    summary="Validate setup access key",
)
def validate_setup_access(request: SetupValidateRequest) -> SetupValidateResponse:
    configured = _configured_setup_key()
    if configured is None:
        return SetupValidateResponse(valid=True)
    return SetupValidateResponse(valid=request.key == configured)
