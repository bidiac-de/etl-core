# tests/components/databases/mariadb/test_credentials_integration.py
import hashlib
import os
from typing import Tuple
from uuid import uuid4

import pytest
from unittest.mock import Mock, patch

from etl_core.context.context import Context
from etl_core.context.environment import Environment
from etl_core.context.credentials import Credentials
from etl_core.context.context_parameter import ContextParameter
from etl_core.components.databases.pool_args import build_sql_engine_kwargs
from etl_core.components.databases.mariadb.mariadb_read import MariaDBRead
from etl_core.components.databases.mariadb.mariadb_write import MariaDBWrite
from etl_core.components.wiring.schema import Schema
from etl_core.components.wiring.column_definition import FieldDef, DataType
from etl_core.persistance.handlers.credentials_handler import CredentialsHandler


def derive_test_password(base_pw: str, purpose: str) -> str:
    digest = hashlib.blake2b(
        f"{purpose}:{base_pw}".encode("utf-8"), digest_size=6
    ).hexdigest()
    return f"{base_pw}_{digest}"


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
def sample_context() -> Context:
    return Context(
        id=str(uuid4()),
        name="test_context",
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
                id=2, key="db_port", value="3306", type="string", is_secure=False
            ),
        },
    )


@pytest.fixture
def persisted_credentials(test_creds: Tuple[str, str]) -> Credentials:
    """
    Create domain credentials, persist via handler, and return the model.

    IMPORTANT:
    We use provider_id == credentials_id so that ContextCredentialsMapTable,
    whose FK references CredentialsTable.provider_id, can be populated with
    the model's credentials_id without violating the FK.
    """
    user, password = test_creds
    creds = Credentials(
        credentials_id=str(uuid4()),
        name="test_db_creds",
        user=user,
        host="localhost",
        port=3306,
        database="testdb",
        password=password,
        pool_max_size=10,
        pool_timeout_s=30,
    )
    # Align provider_id with the domain model's credentials_id
    CredentialsHandler().upsert(provider_id=creds.credentials_id, creds=creds)
    return creds


@pytest.fixture(autouse=True)
def _set_test_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Ensure env-based credential selection is deterministic across tests.
    """
    monkeypatch.setenv("COMP_ENV", Environment.TEST.value)
    monkeypatch.setenv("SECRET_BACKEND", "memory")


@pytest.fixture
def mariadb_read_component(
    persisted_mapping_context_id: str,  # provided by conftest.py
) -> MariaDBRead:
    # Patch during construction because SQLDatabaseComponent connects in model validator
    with patch(
        "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
    ):
        return MariaDBRead(
            name="test_read",
            description="Test read component",
            comp_type="read_mariadb",
            entity_name="users",
            query="SELECT * FROM users",
            context_id=persisted_mapping_context_id,
        )


@pytest.fixture
def mariadb_write_component(
    persisted_mapping_context_id: str,  # provided by conftest.py
) -> MariaDBWrite:
    with patch(
        "etl_core.components.databases.sql_connection_handler.SQLConnectionHandler"
    ):
        return MariaDBWrite(
            name="test_write",
            description="Test write component",
            comp_type="write_mariadb",
            entity_name="users",
            in_port_schemas={"in": _mk_schema()},
            context_id=persisted_mapping_context_id,
        )


def test_credentials_creation(
    persisted_credentials: Credentials, test_creds: Tuple[str, str]
) -> None:
    user, password = test_creds
    assert isinstance(persisted_credentials.credentials_id, str)
    assert persisted_credentials.name == "test_db_creds"
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
    assert sample_context.get_parameter("db_host") == "localhost"
    assert sample_context.get_parameter("db_port") == "3306"
    with pytest.raises(KeyError, match="Parameter with key 'invalid_param' not found"):
        sample_context.get_parameter("invalid_param")


@patch("etl_core.components.databases.sql_connection_handler.SQLConnectionHandler")
def test_mariadb_read_component_with_real_credentials(
    mock_handler_class, mariadb_read_component: MariaDBRead, test_creds
) -> None:
    # Handler is mocked because we just assert resolved credentials mapping
    mock_handler = Mock()
    mock_handler_class.return_value = mock_handler

    creds = mariadb_read_component._get_credentials()
    user, password = test_creds
    assert creds["user"] == user
    assert creds["password"] == password
    assert creds["database"] == "testdb"


@patch("etl_core.components.databases.sql_connection_handler.SQLConnectionHandler")
def test_mariadb_write_component_with_real_credentials(
    mock_handler_class, mariadb_write_component: MariaDBWrite, test_creds
) -> None:
    mock_handler = Mock()
    mock_handler_class.return_value = mock_handler

    creds = mariadb_write_component._get_credentials()
    user, _ = test_creds
    assert creds["user"] == user
    assert creds["database"] == "testdb"


def test_credentials_pool_parameters(persisted_credentials: Credentials) -> None:
    assert persisted_credentials.get_parameter("pool_max_size") == 10
    assert persisted_credentials.get_parameter("pool_timeout_s") == 30
    engine_kwargs = build_sql_engine_kwargs(persisted_credentials)
    assert engine_kwargs["pool_size"] == 10
    assert engine_kwargs["pool_timeout"] == 30


def test_credentials_without_pool_settings(test_creds) -> None:
    _, password = test_creds
    creds = Credentials(
        credentials_id=str(uuid4()),
        name="minimal_creds",
        user="minuser",
        host="localhost",
        port=3306,
        database="mindb",
        password=password,
    )
    # Keep provider_id aligned for consistency across tests
    CredentialsHandler().upsert(provider_id=creds.credentials_id, creds=creds)
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
        port=3306,
        database="nopassdb",
    )
    CredentialsHandler().upsert(provider_id=creds_no_pass.credentials_id, creds=creds_no_pass)
    assert creds_no_pass.decrypted_password is None


def test_credentials_validation(test_creds: Tuple[str, str]) -> None:
    _, base_pw = test_creds
    valid_creds = Credentials(
        credentials_id=str(uuid4()),
        name="valid_creds",
        user="validuser",
        host="localhost",
        port=3306,
        database="validdb",
        password=derive_test_password(base_pw, "valid"),
    )

    special_creds = Credentials(
        credentials_id=str(uuid4()),
        name="special_creds_2024",
        user="user@domain",
        host="localhost",
        port=3306,
        database="test-db_123",
        password=derive_test_password(base_pw, "special_chars"),
    )
    assert isinstance(valid_creds.credentials_id, str)
    assert valid_creds.name == "valid_creds"
    assert special_creds.user == "user@domain"
    assert special_creds.database == "test-db_123"
    assert isinstance(special_creds.decrypted_password, str)


def test_credentials_pool_configuration_validation(test_creds) -> None:
    _, base_pw = test_creds
    valid_pool_creds = Credentials(
        credentials_id=str(uuid4()),
        name="pool_creds",
        user="pooluser",
        host="localhost",
        port=3306,
        database="pooldb",
        password=derive_test_password(base_pw, "pool_ok"),
        pool_max_size=50,
        pool_timeout_s=60,
    )

    min_pool_creds = Credentials(
        credentials_id=str(uuid4()),
        name="min_pool_creds",
        user="minpooluser",
        host="localhost",
        port=3306,
        database="minpooldb",
        password=derive_test_password(base_pw, "pool_min"),
        pool_max_size=1,
        pool_timeout_s=1,
    )
    assert valid_pool_creds.pool_max_size == 50
    assert valid_pool_creds.pool_timeout_s == 60
    assert min_pool_creds.pool_max_size == 1
    assert min_pool_creds.pool_timeout_s == 1


@patch("etl_core.components.databases.sql_connection_handler.SQLConnectionHandler")
def test_mariadb_write_bulk_operations(
    mock_handler_class,
    mariadb_write_component: MariaDBWrite,
    test_creds,
) -> None:
    mock_handler = Mock()
    mock_handler_class.return_value = mock_handler

    creds = mariadb_write_component._get_credentials()
    user, _ = test_creds
    assert creds["user"] == user
    assert creds["database"] == "testdb"


@patch("etl_core.components.databases.sql_connection_handler.SQLConnectionHandler")
def test_mariadb_read_query_operations(
    mock_handler_class,
    persisted_mapping_context_id: str,  # provided by conftest.py
    test_creds,
) -> None:
    mock_handler = Mock()
    mock_handler_class.return_value = mock_handler

    read_comp = MariaDBRead(
        name="test_read_query",
        description="Test read query component",
        comp_type="read_mariadb",
        entity_name="users",
        query="SELECT * FROM users WHERE active = %(id)s",
        params={"active": True},
        context_id=persisted_mapping_context_id,
    )

    creds = read_comp._get_credentials()
    user, _ = test_creds
    assert creds["user"] == user
    assert creds["database"] == "testdb"
