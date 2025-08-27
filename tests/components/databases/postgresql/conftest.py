"""
Test fixtures for PostgreSQL database component tests.

This module provides fixtures that can be used across all PostgreSQL tests.
"""

import pytest
import pandas as pd
import dask.dataframe as dd

from etl_core.context.context import Context
from etl_core.context.environment import Environment
from etl_core.context.credentials import Credentials
from etl_core.context.context_parameter import ContextParameter
from etl_core.components.databases.postgresql.postgresql_read import PostgreSQLRead
from etl_core.components.databases.postgresql.postgresql_write import PostgreSQLWrite


@pytest.fixture
def sample_credentials():
    """Create sample credentials for testing."""
    return Credentials(
        credentials_id=1,
        name="test_db_creds",
        user="testuser",
        host="localhost",
        port=5432,  # PostgreSQL default port
        database="testdb",
        password="testpass123",
        pool_max_size=10,
        pool_timeout_s=30,
    )


@pytest.fixture
def sample_context(sample_credentials):
    """Create a sample context with credentials and parameters."""
    context = Context(
        id=1,
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
                id=2,
                key="db_port",
                value="5432",  # PostgreSQL default port
                type="string",
                is_secure=False,
            ),
            "api_key": ContextParameter(
                id=3,
                key="api_key",
                value="test_api_key",
                type="string",
                is_secure=True,
            ),
            "timeout": ContextParameter(
                id=4,
                key="timeout",
                value="30",
                type="string",
                is_secure=False,
            ),
            "max_retries": ContextParameter(
                id=5,
                key="max_retries",
                value="3",
                type="string",
                is_secure=False,
            ),
        },
    )
    context.add_credentials(sample_credentials)
    return context


@pytest.fixture
def multiple_credentials():
    """Create multiple credentials for testing different scenarios."""
    return {
        "minimal": Credentials(
            credentials_id=10,
            name="minimal_creds",
            user="minuser",
            host="localhost",
            port=5432,  # PostgreSQL default port
            database="mindb",
            password="minpass",
        ),
        "with_pool": Credentials(
            credentials_id=11,
            name="pool_creds",
            user="pooluser",
            host="localhost",
            port=5432,  # PostgreSQL default port
            database="pooldb",
            password="poolpass",
            pool_max_size=50,
            pool_timeout_s=60,
        ),
        "special_chars": Credentials(
            credentials_id=12,
            name="special_creds",
            user="user@domain",
            host="localhost",
            port=5432,  # PostgreSQL default port
            database="test-db_123",
            password="pass@word#123",
        ),
        "no_password": Credentials(
            credentials_id=13,
            name="nopass_creds",
            user="nopassuser",
            host="localhost",
            port=5432,  # PostgreSQL default port
            database="nopassdb",
        ),
    }


@pytest.fixture
def context_with_multiple_credentials(multiple_credentials):
    """Create a context with multiple credentials."""
    context = Context(
        id=2,
        name="multi_creds_context",
        environment=Environment.TEST,
        parameters={
            "db_host": ContextParameter(
                id=6,
                key="db_host",
                value="localhost",
                type="string",
                is_secure=False,
            ),
            "db_port": ContextParameter(
                id=7,
                key="db_port",
                value="5432",
                type="string",
                is_secure=False,
            ),
        },
    )
    for creds in multiple_credentials.values():
        context.add_credentials(creds)
    return context


@pytest.fixture
def postgresql_read_component(sample_context):
    """Create a PostgreSQL read component for testing."""
    component = PostgreSQLRead(
        name="test_read",
        description="Test read component",
        comp_type="read_postgresql",
        database="testdb",
        entity_name="users",
        query="SELECT * FROM users",
        credentials_id=1,
    )
    component.context = sample_context
    return component


@pytest.fixture
def postgresql_write_component(sample_context):
    """Create a PostgreSQL write component for testing."""
    component = PostgreSQLWrite(
        name="test_write",
        description="Test write component",
        comp_type="write_postgresql",
        database="testdb",
        entity_name="users",
        credentials_id=1,
    )
    component.context = sample_context
    return component


@pytest.fixture
def sample_dataframe():
    """Create a sample pandas DataFrame for testing."""
    return pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "name": ["John", "Jane", "Bob", "Alice", "Charlie"],
            "email": [
                "john@example.com",
                "jane@example.com",
                "bob@example.com",
                "alice@example.com",
                "charlie@example.com",
            ],
            "age": [25, 30, 35, 28, 32],
            "active": [True, True, False, True, False],
        }
    )


@pytest.fixture
def sample_dask_dataframe(sample_dataframe):
    """Create a sample Dask DataFrame for testing."""
    return dd.from_pandas(sample_dataframe, npartitions=2)


@pytest.fixture
def sample_sql_queries():
    """Provide sample SQL queries for testing."""
    return {
        "simple_select": "SELECT * FROM users",
        "parameterized": "SELECT * FROM users WHERE id = %(id)s AND active = %(active)s",
        "complex_join": """
            SELECT u.id, u.name, u.email, p.phone, a.street, a.city
            FROM users u
            LEFT JOIN profiles p ON u.id = p.user_id
            LEFT JOIN addresses a ON u.id = a.user_id
            WHERE u.active = %(active)s
        """,
        "aggregation": """
            SELECT 
                city,
                COUNT(*) as user_count,
                AVG(age) as avg_age
            FROM users u
            JOIN addresses a ON u.id = a.user_id
            WHERE u.active = %(active)s
            GROUP BY city
            HAVING COUNT(*) > %(min_users)s
            ORDER BY user_count DESC
        """,
        "insert": "INSERT INTO users (name, email, age) VALUES (%(name)s, %(email)s, %(age)s)",
        "update": "UPDATE users SET active = %(active)s WHERE id = %(id)s",
        "delete": "DELETE FROM users WHERE id = %(id)s",
    }


@pytest.fixture
def sample_query_params():
    """Provide sample query parameters for testing."""
    return {
        "simple": {"id": 1},
        "user_lookup": {"id": 1, "active": True},
        "bulk_insert": [
            {"name": "John", "email": "john@example.com", "age": 25},
            {"name": "Jane", "email": "jane@example.com", "age": 30},
            {"name": "Bob", "email": "bob@example.com", "age": 35},
        ],
        "filter": {"active": True, "min_age": 18, "max_age": 65},
        "pagination": {"offset": 0, "limit": 10, "sort_by": "name", "sort_order": "ASC"},
    }


@pytest.fixture
def sample_data():
    """Create sample data for testing."""
    return [
        {"id": 1, "name": "John", "email": "john@example.com", "age": 25, "active": True},
        {"id": 2, "name": "Jane", "email": "jane@example.com", "age": 30, "active": True},
        {"id": 3, "name": "Bob", "email": "bob@example.com", "age": 35, "active": False},
        {"id": 4, "name": "Alice", "email": "alice@example.com", "age": 28, "active": True},
        {"id": 5, "name": "Charlie", "email": "charlie@example.com", "age": 32, "active": False},
    ]


@pytest.fixture
def large_dataset():
    """Create a large dataset for performance testing."""
    data = []
    for i in range(1000):
        data.append({
            "id": i + 1,
            "name": f"User{i}",
            "email": f"user{i}@example.com",
            "age": 20 + (i % 50),
            "active": i % 3 != 0,
            "created_at": f"2024-01-{(i % 30) + 1:02d}",
        })
    return data


@pytest.fixture
def large_dataframe(large_dataset):
    """Create a large pandas DataFrame for performance testing."""
    return pd.DataFrame(large_dataset)


@pytest.fixture
def large_dask_dataframe(large_dataframe):
    """Create a large Dask DataFrame for performance testing."""
    return dd.from_pandas(large_dataframe, npartitions=4)


@pytest.fixture
def postgresql_connection_string():
    """Provide PostgreSQL connection string for testing."""
    return "postgresql://testuser:testpass123@localhost:5432/testdb"


@pytest.fixture
def postgresql_connection_params():
    """Provide PostgreSQL connection parameters for testing."""
    return {
        "user": "testuser",
        "password": "testpass123",
        "host": "localhost",
        "port": 5432,
        "database": "testdb",
        "charset": "utf8",
        "collation": "en_US.UTF-8",
    }


@pytest.fixture
def postgresql_session_variables():
    """Provide PostgreSQL session variables for testing."""
    return {
        "client_encoding": "utf8",
        "lc_collate": "en_US.UTF-8",
        "timezone": "UTC",
        "datestyle": "ISO",
    }


@pytest.fixture
def postgresql_table_schema():
    """Provide PostgreSQL table schema for testing."""
    return {
        "users": {
            "columns": [
                {"name": "id", "type": "SERIAL PRIMARY KEY"},
                {"name": "name", "type": "VARCHAR(100) NOT NULL"},
                {"name": "email", "type": "VARCHAR(255) UNIQUE NOT NULL"},
                {"name": "age", "type": "INTEGER"},
                {"name": "active", "type": "BOOLEAN DEFAULT TRUE"},
                {"name": "created_at", "type": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"},
            ],
            "indexes": [
                {"name": "idx_users_email", "columns": ["email"]},
                {"name": "idx_users_active", "columns": ["active"]},
                {"name": "idx_users_created_at", "columns": ["created_at"]},
            ],
        },
        "profiles": {
            "columns": [
                {"name": "id", "type": "SERIAL PRIMARY KEY"},
                {"name": "user_id", "type": "INTEGER REFERENCES users(id)"},
                {"name": "phone", "type": "VARCHAR(20)"},
                {"name": "bio", "type": "TEXT"},
                {"name": "avatar_url", "type": "VARCHAR(500)"},
            ],
            "indexes": [
                {"name": "idx_profiles_user_id", "columns": ["user_id"]},
            ],
        },
        "addresses": {
            "columns": [
                {"name": "id", "type": "SERIAL PRIMARY KEY"},
                {"name": "user_id", "type": "INTEGER REFERENCES users(id)"},
                {"name": "street", "type": "VARCHAR(200)"},
                {"name": "city", "type": "VARCHAR(100)"},
                {"name": "state", "type": "VARCHAR(50)"},
                {"name": "postal_code", "type": "VARCHAR(20)"},
                {"name": "country", "type": "VARCHAR(100) DEFAULT 'USA'"},
            ],
            "indexes": [
                {"name": "idx_addresses_user_id", "columns": ["user_id"]},
                {"name": "idx_addresses_city", "columns": ["city"]},
            ],
        },
    }


@pytest.fixture
def postgresql_upsert_query():
    """Provide PostgreSQL UPSERT query for testing."""
    return """
        INSERT INTO users (name, email, age, active)
        VALUES (%(name)s, %(email)s, %(age)s, %(active)s)
        ON CONFLICT (email) 
        DO UPDATE SET
            name = EXCLUDED.name,
            age = EXCLUDED.age,
            active = EXCLUDED.active,
            updated_at = CURRENT_TIMESTAMP
    """


@pytest.fixture
def postgresql_bulk_insert_query():
    """Provide PostgreSQL bulk insert query for testing."""
    return """
        INSERT INTO users (name, email, age, active)
        VALUES (%(name)s, %(email)s, %(age)s, %(active)s)
    """


@pytest.fixture
def postgresql_transaction_queries():
    """Provide PostgreSQL transaction queries for testing."""
    return {
        "begin": "BEGIN",
        "commit": "COMMIT",
        "rollback": "ROLLBACK",
        "savepoint": "SAVEPOINT test_savepoint",
        "rollback_to_savepoint": "ROLLBACK TO SAVEPOINT test_savepoint",
        "release_savepoint": "RELEASE SAVEPOINT test_savepoint",
    }


@pytest.fixture
def postgresql_error_scenarios():
    """Provide PostgreSQL error scenarios for testing."""
    return {
        "connection_timeout": "Connection timeout",
        "authentication_failed": "Authentication failed",
        "permission_denied": "Permission denied",
        "table_not_found": "Table 'nonexistent_table' doesn't exist",
        "column_not_found": "Column 'nonexistent_column' doesn't exist",
        "syntax_error": "Syntax error in SQL statement",
        "constraint_violation": "Constraint violation",
        "deadlock": "Deadlock detected",
        "query_timeout": "Query timeout",
        "insufficient_privileges": "Insufficient privileges",
    }


@pytest.fixture
def postgresql_performance_metrics():
    """Provide PostgreSQL performance metrics for testing."""
    return {
        "query_execution_time": 0.125,
        "rows_affected": 1000,
        "connection_pool_size": 10,
        "active_connections": 3,
        "idle_connections": 7,
        "total_queries": 150,
        "slow_queries": 5,
        "failed_queries": 2,
        "cache_hit_ratio": 0.85,
        "index_usage": 0.92,
    }


@pytest.fixture
def postgresql_logging_config():
    """Provide PostgreSQL logging configuration for testing."""
    return {
        "log_level": "INFO",
        "log_file": "/var/log/postgresql/postgresql.log",
        "log_format": "%t [%p]: [%l-1] user=%u,db=%d,app=%a,client=%h",
        "log_statement": "all",
        "log_min_duration_statement": 1000,
        "log_checkpoints": True,
        "log_connections": True,
        "log_disconnections": True,
        "log_lock_waits": True,
        "log_temp_files": 0,
    }


@pytest.fixture
def postgresql_backup_config():
    """Provide PostgreSQL backup configuration for testing."""
    return {
        "backup_method": "pg_dump",
        "backup_directory": "/var/backups/postgresql",
        "backup_retention_days": 30,
        "backup_compression": True,
        "backup_encryption": False,
        "backup_schedule": "0 2 * * *",  # Daily at 2 AM
        "backup_verification": True,
        "backup_notification": True,
    }


@pytest.fixture
def postgresql_replication_config():
    """Provide PostgreSQL replication configuration for testing."""
    return {
        "replication_mode": "master_slave",
        "replication_slots": 5,
        "wal_level": "replica",
        "max_wal_senders": 10,
        "max_replication_slots": 10,
        "hot_standby": True,
        "archive_mode": True,
        "archive_command": "test ! -f /var/lib/postgresql/archive/%f && cp %p /var/lib/postgresql/archive/%f",
    }


@pytest.fixture
def postgresql_security_config():
    """Provide PostgreSQL security configuration for testing."""
    return {
        "ssl_mode": "require",
        "ssl_cert_file": "/etc/ssl/certs/postgresql.crt",
        "ssl_key_file": "/etc/ssl/private/postgresql.key",
        "ssl_ca_file": "/etc/ssl/certs/ca-certificates.crt",
        "password_encryption": "scram-sha-256",
        "max_connections": 100,
        "shared_preload_libraries": "pg_stat_statements",
        "track_activities": True,
        "track_counts": True,
        "track_io_timing": True,
    }


@pytest.fixture
def postgresql_monitoring_queries():
    """Provide PostgreSQL monitoring queries for testing."""
    return {
        "active_connections": "SELECT count(*) FROM pg_stat_activity WHERE state = 'active'",
        "database_size": "SELECT pg_size_pretty(pg_database_size(current_database()))",
        "table_sizes": """
            SELECT 
                schemaname,
                tablename,
                pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
            FROM pg_tables 
            ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC
        """,
        "slow_queries": """
            SELECT 
                query,
                calls,
                total_time,
                mean_time,
                rows
            FROM pg_stat_statements 
            ORDER BY mean_time DESC 
            LIMIT 10
        """,
        "index_usage": """
            SELECT 
                schemaname,
                tablename,
                indexname,
                idx_scan,
                idx_tup_read,
                idx_tup_fetch
            FROM pg_stat_user_indexes 
            ORDER BY idx_scan DESC
        """,
        "locks": """
            SELECT 
                locktype,
                database,
                relation,
                page,
                tuple,
                virtualxid,
                transactionid,
                classid,
                objid,
                objsubid,
                virtualtransaction,
                pid,
                mode,
                granted
            FROM pg_locks 
            WHERE NOT granted
        """,
    }


@pytest.fixture
def postgresql_maintenance_queries():
    """Provide PostgreSQL maintenance queries for testing."""
    return {
        "vacuum": "VACUUM ANALYZE",
        "reindex": "REINDEX TABLE users",
        "analyze": "ANALYZE users",
        "cluster": "CLUSTER users USING idx_users_id",
        "check_table": "SELECT * FROM users LIMIT 1",
        "update_statistics": "ANALYZE",
        "cleanup_logs": "DELETE FROM log_table WHERE created_at < NOW() - INTERVAL '30 days'",
    }


@pytest.fixture
def postgresql_test_data():
    """Provide test data for PostgreSQL operations."""
    return {
        "users": [
            {"name": "Test User 1", "email": "test1@example.com", "age": 25, "active": True},
            {"name": "Test User 2", "email": "test2@example.com", "age": 30, "active": True},
            {"name": "Test User 3", "email": "test3@example.com", "age": 35, "active": False},
        ],
        "profiles": [
            {"user_id": 1, "phone": "+1234567890", "bio": "Test bio 1"},
            {"user_id": 2, "phone": "+0987654321", "bio": "Test bio 2"},
            {"user_id": 3, "phone": "+1122334455", "bio": "Test bio 3"},
        ],
        "addresses": [
            {"user_id": 1, "street": "123 Test St", "city": "Test City", "state": "TS", "postal_code": "12345"},
            {"user_id": 2, "street": "456 Test Ave", "city": "Test Town", "state": "TS", "postal_code": "67890"},
            {"user_id": 3, "street": "789 Test Blvd", "city": "Test Village", "state": "TS", "postal_code": "11111"},
        ],
    }


@pytest.fixture
def postgresql_cleanup_queries():
    """Provide cleanup queries for PostgreSQL tests."""
    return {
        "drop_users": "DROP TABLE IF EXISTS users CASCADE",
        "drop_profiles": "DROP TABLE IF EXISTS profiles CASCADE",
        "drop_addresses": "DROP TABLE IF EXISTS addresses CASCADE",
        "drop_logs": "DROP TABLE IF EXISTS log_table CASCADE",
        "reset_sequences": "ALTER SEQUENCE users_id_seq RESTART WITH 1",
        "clear_statistics": "SELECT pg_stat_reset()",
    }
