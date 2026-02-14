from __future__ import annotations

import os
from typing import List, Literal

from fastapi import APIRouter, status
from pydantic import BaseModel, Field

from etl_core.components.data_operations.filter.comparison_rule import (
    RULE_LOGICAL_OPERATORS,
    RULE_OPERATORS,
)
from etl_core.components.wiring.column_definition import DataType
from etl_core.context.environment import Environment

router = APIRouter(prefix="/setup", tags=["setup"])

CONTRACT_VERSION = "core-studio-v1"
CORE_VERSION = "0.1.0"
_SETUP_KEY_ENV = "ETL_SETUP_ACCESS_KEY"

_ENV_LABELS = {
    Environment.DEV: "Development",
    Environment.TEST: "Test",
    Environment.PROD: "Production",
}
_ENV_ICONS = {
    Environment.DEV: "fa-solid fa-bug",
    Environment.TEST: "fa-solid fa-flask-vial",
    Environment.PROD: "fa-solid fa-shield",
}


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


def _environment_capabilities() -> List[EnvironmentCapability]:
    items: List[EnvironmentCapability] = []
    for env in Environment:
        items.append(
            EnvironmentCapability(
                value=env.value,
                label=_ENV_LABELS[env],
                icon=_ENV_ICONS[env],
            )
        )
    return items


@router.get(
    "/capabilities",
    response_model=SetupCapabilitiesResponse,
    status_code=status.HTTP_200_OK,
    summary="Get studio integration capabilities",
)
def setup_capabilities() -> SetupCapabilitiesResponse:
    return SetupCapabilitiesResponse(
        setup_validation=_validation_config(),
        environments=_environment_capabilities(),
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
