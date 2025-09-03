"""
Integration tests for PostgreSQL credentials (direct resolution via credentials_id).

Context objects can still exist in the domain, but credential resolution no longer
uses them. These tests ensure Credentials and engine kwargs behave as expected,
and components resolve credentials directly via CredentialsHandler.
"""

from __future__ import annotations

import os
from typing import Tuple
from uuid import uuid4
from unittest.mock import Mock, patch

import pytest

from etl_core.components.databases.pool_args import build_sql_engine_kwargs
from etl_core.components.databases.postgresql.postgresql_read import PostgreSQLRead
from etl_core.components.databases.postgresql.postgresql_write import PostgreSQLWrite
from etl_core.components.wiring.column_definition import DataType, FieldDef
from etl_core.components.wiring.schema import Schema
from etl_core.context.context import Context
from etl_core.context.context_parameter import ContextParameter
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


@pytest.fixture
def test_creds() -> Tuple[str, str]:
    return os.environ["APP_TEST_USER"], os.environ["APP_TEST_PASSWORD"]


@pytest.fixture
def persisted_credentials(test_creds: Tuple[str, str]) -> Credentials:
    """
    Persist credentials so components can resolve by credentials_id
    (directly via CredentialsHandler).
    """
    user, password = test_creds
    creds = Credentials(
        credentials_id=str(uuid4()),
        name="pg_test_creds",
        user=user,
        host="localhost",
        port=5432,
        database="testdb",
        password=password,
        pool_max_size=10,
        pool_timeout_s=30,
    )
    CredentialsHandler().upsert(provider_id=str(uuid4()), creds=creds)
    return creds


@pytest.fixture
def sample_context() -> Context:
    """
    Context remains useful for non-credential parameters; it is no longer
    consulted for credential lookup.
    """
    return Context(
        id=str(uuid4()),
        name="test_env",
        environment=Environment.TEST,
        parameters={
            "db_host": ContextParameter(
                id=1,
                key="db_host",
                value="localhost",
                type="string",
                is_secure=False,
            ),
            "db_port": ContextParameter(
                id=2,
                key="db_port",
                value="5432",
                type="string",
                is_secure=False,
            ),
        },
    )


def test_credentials_creation(
    persisted_credentials: Credentials, test_creds: Tuple[str, str]
) -> None:
    user, password = test_creds
    assert isinstance(persisted_credentials.credentials_id, str)
    assert persisted_credentials.name == "pg_test_creds"
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


def test_context_parameter_retrieval(sample_context: Context) -> None:
    """Context parameters still work, just not for creds."""
    assert sample_context.get_parameter("db_host") == "localhost"
    assert sample_context.get_parameter("db_port") == "5432"
    with pytest.raises(KeyError, match="Parameter with key 'invalid_param' not found"):
        sample_context.get_parameter("invalid_param")


@patch("etl_core.components.databases.sql_connection_handler.SQLConnectionHandler")
def test_postgresql_read_component_with_real_credentials(
    mock_handler_class: Mock,
    persisted_credentials: Credentials,
    test_creds: Tuple[str, str],
) -> None:
    mock_handler_class.return_value = Mock()
    read_comp = PostgreSQLRead(
        name="pg_read",
        description="Test read component",
        comp_type="read_postgresql",
        entity_name="users",
        query="SELECT * FROM users",
        credentials_id=persisted_credentials.credentials_id,
    )
    creds = read_comp._get_credentials()
    user, password = test_creds
    assert creds["user"] == user
    assert creds["password"] == password
    assert creds["database"] == "testdb"


@patch("etl_core.components.databases.sql_connection_handler.SQLConnectionHandler")
def test_postgresql_write_component_with_real_credentials(
    mock_handler_class: Mock,
    persisted_credentials: Credentials,
    test_creds: Tuple[str, str],
) -> None:
    mock_handler_class.return_value = Mock()
    write_comp = PostgreSQLWrite(
        name="pg_write",
        description="Test write component",
        comp_type="write_postgresql",
        entity_name="users",
        in_port_schemas={"in": _mk_schema()},
        credentials_id=persisted_credentials.credentials_id,
    )
    creds = write_comp._get_credentials()
    user, _ = test_creds
    assert creds["user"] == user
    assert creds["database"] == "testdb"


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
        name="minimal_creds",
        user="minuser",
        host="localhost",
        port=5432,
        database="mindb",
        password=password,
    )
    CredentialsHandler().upsert(provider_id=str(uuid4()), creds=creds)
    assert creds.pool_max_size is None
    assert creds.pool_timeout_s is None
    engine_kwargs = build_sql_engine_kwargs(creds)
    assert engine_kwargs == {}


def test_credentials_password_handling() -> None:
    creds_no_pass = Credentials(
        credentials_id=str(uuid4()),
        name="nopass_creds",
        user="nopassuser",
        host="localhost",
        port=5432,
        database="nopassdb",
    )
    assert creds_no_pass.decrypted_password is None


@patch("etl_core.components.databases.sql_connection_handler.SQLConnectionHandler")
def test_credentials_in_postgresql_component_integration(
    mock_handler_class: Mock,
    persisted_credentials: Credentials,
    test_creds: Tuple[str, str],
) -> None:
    mock_handler_class.return_value = Mock()
    read_comp = PostgreSQLRead(
        name="integration_test",
        description="Integration test component",
        comp_type="read_postgresql",
        entity_name="users",
        query="SELECT * FROM users WHERE id = %(id)s",
        params={"id": 1},
        credentials_id=persisted_credentials.credentials_id,
    )
    creds = read_comp._get_credentials()
    user, password = test_creds
    assert creds["user"] == user
    assert creds["password"] == password
    assert creds["database"] == "testdb"
