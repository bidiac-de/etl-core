"""
Integration tests for SQL Server credentials via mapping-context resolution.

Flow:
- Persist Credentials (provider_id == credentials_id)
- Create mapping Context (env -> credentials_id)
- Components receive context_id and resolve creds via ContextHandler map
"""

from __future__ import annotations

import os
from typing import Tuple
from uuid import uuid4
from unittest.mock import Mock, patch

import pytest

from etl_core.components.databases.pool_args import build_sql_engine_kwargs
from etl_core.components.databases.sqlserver.sqlserver_read import SQLServerRead
from etl_core.components.databases.sqlserver.sqlserver_write import SQLServerWrite
from etl_core.components.wiring.column_definition import DataType, FieldDef
from etl_core.components.wiring.schema import Schema
from etl_core.context.credentials import Credentials
from etl_core.context.environment import Environment
from etl_core.persistance.handlers.credentials_handler import CredentialsHandler


def _mk_schema() -> Schema:
    return Schema(
        fields=[
            FieldDef(name="id", data_type=DataType.INTEGER),
            FieldDef(name="name", data_type=DataType.STRING),
            FieldDef(name="email", data_type=DataType.STRING),
        ]
    )


@pytest.fixture(autouse=True)
def _set_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("COMP_ENV", Environment.TEST.value)
    monkeypatch.setenv("SECRET_BACKEND", "memory")


@pytest.fixture
def test_creds() -> Tuple[str, str]:
    return os.environ["APP_TEST_USER"], os.environ["APP_TEST_PASSWORD"]

def test_credentials_creation(
    persisted_credentials: Credentials, test_creds: Tuple[str, str]
) -> None:
    user, password = test_creds
    assert isinstance(persisted_credentials.credentials_id, str)
    assert persisted_credentials.name == "mssql_test_creds"
    assert persisted_credentials.user == user
    assert persisted_credentials.decrypted_password == password
    assert persisted_credentials.pool_max_size == 10
    assert persisted_credentials.pool_timeout_s == 30


def test_credentials_get_parameter(
    persisted_credentials: Credentials, test_creds: Tuple[str, str]
) -> None:
    user, _ = test_creds
    assert persisted_credentials.get_parameter("user") == user
    assert persisted_credentials.get_parameter("database") == "testdb"
    assert persisted_credentials.get_parameter("pool_max_size") == 10
    assert persisted_credentials.get_parameter("pool_timeout_s") == 30
    with pytest.raises(KeyError, match="Unknown parameter key: invalid_key"):
        persisted_credentials.get_parameter("invalid_key")


def test_credentials_pool_parameters(persisted_credentials: Credentials) -> None:
    assert persisted_credentials.get_parameter("pool_max_size") == 10
    assert persisted_credentials.get_parameter("pool_timeout_s") == 30
    engine_kwargs = build_sql_engine_kwargs(persisted_credentials)
    assert engine_kwargs["pool_size"] == 10
    assert engine_kwargs["pool_timeout"] == 30


def test_credentials_without_pool_settings(test_creds: Tuple[str, str]) -> None:
    _, password = test_creds
    creds = Credentials(
        credentials_id=str(uuid4()),
        name="mssql_min_creds",
        user="minuser",
        host="localhost",
        port=1433,
        database="mindb",
        password=password,
    )
    CredentialsHandler().upsert(provider_id=creds.credentials_id, creds=creds)
    assert creds.pool_max_size is None
    assert creds.pool_timeout_s is None
    engine_kwargs = build_sql_engine_kwargs(creds)
    assert engine_kwargs == {}


def test_credentials_password_handling() -> None:
    creds_no_pass = Credentials(
        credentials_id=str(uuid4()),
        name="mssql_nopass",
        user="nopassuser",
        host="localhost",
        port=1433,
        database="nopassdb",
    )
    assert creds_no_pass.decrypted_password is None


@patch("etl_core.components.databases.sql_connection_handler.SQLConnectionHandler")
def test_sqlserver_read_component_with_real_credentials(
    mock_handler_class: Mock,
    persisted_credentials: Credentials,
    persisted_mapping_context_id: str,
    test_creds: Tuple[str, str],
) -> None:
    mock_handler_class.return_value = Mock()
    comp = SQLServerRead(
        name="mssql_read",
        description="Test read component",
        comp_type="read_sqlserver",
        entity_name="users",
        query="SELECT * FROM users",
        context_id=persisted_mapping_context_id,
    )
    creds = comp._get_credentials()
    user, password = test_creds
    assert creds["user"] == user
    assert creds["password"] == password
    assert creds["database"] == "testdb"
    assert creds["__credentials_id__"] == persisted_credentials.credentials_id


@patch("etl_core.components.databases.sql_connection_handler.SQLConnectionHandler")
def test_sqlserver_write_component_with_real_credentials(
    mock_handler_class: Mock,
    persisted_credentials: Credentials,
    persisted_mapping_context_id: str,
    test_creds: Tuple[str, str],
) -> None:
    mock_handler_class.return_value = Mock()
    comp = SQLServerWrite(
        name="mssql_write",
        description="Test write component",
        comp_type="write_sqlserver",
        entity_name="users",
        in_port_schemas={"in": _mk_schema()},
        context_id=persisted_mapping_context_id,
    )
    creds = comp._get_credentials()
    user, _ = test_creds
    assert creds["user"] == user
    assert creds["database"] == "testdb"
    assert creds["__credentials_id__"] == persisted_credentials.credentials_id
