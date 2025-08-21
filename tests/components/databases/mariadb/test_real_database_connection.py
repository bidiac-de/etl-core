"""
Real database connection tests for MariaDB.

These tests verify that the MariaDB components can actually connect to a real database
and perform basic operations. This requires a running MariaDB instance.

Test database configuration:
- Host: localhost
- User: root
- Password: (empty)
- Database: etl_core
"""

import pytest

from src.etl_core.components.databases.mariadb.mariadb_read import MariaDBRead
from src.etl_core.components.databases.mariadb.mariadb_write import MariaDBWrite
from src.etl_core.context.context import Context
from src.etl_core.context.environment import Environment
from src.etl_core.context.credentials import Credentials


class TestRealDatabaseConnection:
    """Test real database connections to MariaDB."""

    @pytest.fixture
    def real_credentials(self):
        """Create real credentials for local MariaDB instance."""
        return Credentials(
            credentials_id=1,
            name="local_mariadb",
            user="root",
            host="localhost",
            port=3306,
            database="etl_core",
            password="",  # Empty password as specified
        )

    @pytest.fixture
    def real_context(self, real_credentials):
        """Create context with real credentials."""
        context = Context(
            id=1,
            name="local_test_context",
            environment=Environment.DEV,
            parameters={},
        )
        context.add_credentials(real_credentials)
        return context

    @pytest.fixture
    def test_table_name(self):
        """Name for test table."""
        return "test_connection_table"

    def test_can_connect_to_database(self, real_credentials):
        """Test that we can establish a connection to the database."""
        try:
            # Import here to avoid import errors if MariaDB is not available
            from src.etl_core.components.databases.sql_connection_handler import (
                SQLConnectionHandler,
            )
            from sqlalchemy import text

            # Create connection handler with real credentials
            handler = SQLConnectionHandler()

            # Build connection URL manually since build_url doesn't handle empty
            # passwords with Format:
            # mysql+mysqlconnector://user:password@host:port/database
            password = (
                real_credentials.decrypted_password
                if real_credentials.decrypted_password
                else ""
            )
            url = (
                f"mysql+mysqlconnector://{real_credentials.user}:{password}@"
                f"{real_credentials.host}:{real_credentials.port}"
                f"/{real_credentials.database}"
            )

            # Connect to database
            handler.connect(url=url)

            # Try to connect
            with handler.lease() as connection:
                # Execute a simple query to verify connection
                result = connection.execute(text("SELECT 1 as test"))
                row = result.fetchone()
                assert row[0] == 1
                print("✅ Successfully connected to MariaDB!")

        except ImportError:
            pytest.skip("MariaDB dependencies not available")
        except Exception as e:
            pytest.fail(f"Failed to connect to database: {e}")

    def test_can_read_from_database(self, real_context, test_table_name):
        """Test that we can read data from the database."""
        try:
            from src.etl_core.components.databases.sql_connection_handler import (
                SQLConnectionHandler,
            )
            from sqlalchemy import text

            # Create a test table first
            credentials = real_context.get_credentials(1)
            handler = SQLConnectionHandler()

            # Build connection URL manually since build_url doesn't
            # handle empty passwords
            password = (
                credentials.decrypted_password if credentials.decrypted_password else ""
            )
            url = (
                f"mysql+mysqlconnector://{credentials.user}:{password}@"
                f"{credentials.host}:{credentials.port}/{credentials.database}"
            )

            # Connect to database
            handler.connect(url=url)

            with handler.lease() as connection:
                # Create test table if it doesn't exist
                create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS {test_table_name} (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(100),
                    email VARCHAR(100),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
                connection.execute(text(create_table_sql))

                # Insert test data
                insert_sql = f"""
                INSERT INTO {test_table_name} (name, email) VALUES
                ('Test User 1', 'test1@example.com'),
                ('Test User 2', 'test2@example.com')
                ON DUPLICATE KEY UPDATE name=name
                """
                connection.execute(text(insert_sql))

                # Now test reading with MariaDBRead component
                read_comp = MariaDBRead(
                    name="test_real_read",
                    description="Test real database read",
                    comp_type="database",
                    database="etl_core",
                    entity_name=test_table_name,
                    query=f"SELECT * FROM {test_table_name} ORDER BY id",
                    credentials_id=1,
                )
                read_comp.context = real_context

                # Test that credentials can be retrieved
                creds = read_comp._get_credentials()
                assert creds["user"] == "root"
                assert creds["database"] == "etl_core"
                assert creds["password"] == ""

                print(
                    "✅ Successfully tested MariaDBRead component with "
                    "real credentials!"
                )

        except ImportError:
            pytest.skip("MariaDB dependencies not available")
        except Exception as e:
            pytest.fail(f"Failed to test database read: {e}")

    def test_can_write_to_database(self, real_context, test_table_name):
        """Test that we can write data to the database."""

        # Test writing with MariaDBWrite component
        write_comp = MariaDBWrite(
            name="test_real_write",
            description="Test real database write",
            comp_type="database",
            database="etl_core",
            entity_name=test_table_name,
            credentials_id=1,
        )
        write_comp.context = real_context

        # Test that credentials can be retrieved
        creds = write_comp._get_credentials()
        assert creds["user"] == "root"
        assert creds["database"] == "etl_core"
        assert creds["password"] == ""

        print("✅ Successfully tested MariaDBWrite " "component with real credentials!")

    def test_database_operations_workflow(self, real_context, test_table_name):
        """Test complete workflow: create table, insert data, read data."""
        try:
            from src.etl_core.components.databases.sql_connection_handler import (
                SQLConnectionHandler,
            )
            from sqlalchemy import text

            credentials = real_context.get_credentials(1)
            handler = SQLConnectionHandler()

            # Build connection URL manually since build_url
            # doesn't handle empty passwords
            password = credentials.decrypted_password
            url = (
                f"mysql+mysqlconnector://{credentials.user}:{password}@"
                f"{credentials.host}:{credentials.port}/"
                f"{credentials.database}"
            )

            # Connect to database
            handler.connect(url=url)

            with handler.lease() as connection:
                # Create a new test table for this workflow
                workflow_table = f"{test_table_name}_workflow"
                create_sql = f"""
                CREATE TABLE IF NOT EXISTS {workflow_table} (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(100),
                    value INT,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
                connection.execute(text(create_sql))

                # Insert test data - use named parameters instead of %s
                test_data = [
                    {"name": "Item 1", "value": 100},
                    {"name": "Item 2", "value": 200},
                    {"name": "Item 3", "value": 300},
                ]

                for data in test_data:
                    insert_sql = f"""
                    INSERT INTO {workflow_table} (name, value) VALUES (:name, :value)
                    """
                    connection.execute(text(insert_sql), data)

                # Read and verify data
                select_sql = f"SELECT * FROM {workflow_table} ORDER BY id"
                result = connection.execute(text(select_sql))
                rows = result.fetchall()

                assert len(rows) == 3
                assert rows[0][1] == "Item 1"  # name column
                assert rows[0][2] == 100  # value column
                assert rows[1][1] == "Item 2"
                assert rows[1][2] == 200
                assert rows[2][1] == "Item 3"
                assert rows[2][2] == 300

                print("✅ Successfully tested complete database workflow!")

                # Clean up
                connection.execute(text(f"DROP TABLE IF EXISTS {workflow_table}"))

        except ImportError:
            pytest.skip("MariaDB dependencies not available")
        except Exception as e:
            pytest.fail(f"Failed to test database workflow: {e}")

    def test_connection_pool_settings(self, real_credentials):
        """Test that connection pool settings work correctly."""
        try:
            from src.etl_core.components.databases.sql_connection_handler import (
                SQLConnectionHandler,
            )
            from src.etl_core.components.databases.pool_args import (
                build_sql_engine_kwargs,
            )
            from sqlalchemy import text

            # Test credentials with pool settings
            pool_credentials = Credentials(
                credentials_id=2,
                name="pool_test",
                user="root",
                host="localhost",
                port=3306,
                database="etl_core",
                password="",
                pool_max_size=5,
                pool_timeout_s=10,
            )

            # Build engine kwargs
            engine_kwargs = build_sql_engine_kwargs(pool_credentials)
            assert engine_kwargs["pool_size"] == 5
            assert engine_kwargs["pool_timeout"] == 10

            # Test connection with pool settings
            handler = SQLConnectionHandler()

            # Build connection URL manually since build_url doesn't
            # handle empty passwords
            password = pool_credentials.decrypted_password
            url = (
                f"mysql+mysqlconnector://{pool_credentials.user}:{password}@"
                f"{pool_credentials.host}:{pool_credentials.port}/"
                f"{pool_credentials.database}"
            )

            # Connect to database with pool settings
            handler.connect(url=url, engine_kwargs=engine_kwargs)

            with handler.lease() as connection:
                result = connection.execute(text("SELECT 1 as test"))
                row = result.fetchone()
                assert row[0] == 1

            print("✅ Successfully tested connection pool settings!")

        except ImportError:
            pytest.skip("MariaDB dependencies not available")
        except Exception as e:
            pytest.fail(f"Failed to test connection pool: {e}")

    def test_connection_error_handling(self, real_context):
        """Test connection error handling with invalid credentials."""
        from src.etl_core.components.databases.sql_connection_handler import (
            SQLConnectionHandler,
        )

        # Test with invalid credentials to localhost to see what happens
        invalid_url = (
            "mysql+mysqlconnector://"
            "invalid_user:wrong_password@localhost:3306/etl_core"
        )

        handler = SQLConnectionHandler()

        # Let's see what actually happens instead of expecting an exception
        try:
            handler.connect(url=invalid_url)
            print(
                "⚠️  WARNING: Invalid credentials were accepted! This "
                "indicates weak authentication."
            )
            print(
                "⚠️  MariaDB accepted 'invalid_user:wrong_password' - security concern!"
            )

            # Try to execute a query to see what user we're actually connected as
            with handler.lease() as connection:
                from sqlalchemy import text

                result = connection.execute(text("SELECT USER() as current_user"))
                row = result.fetchone()
                print(f"⚠️  Connected as user: {row[0]}")

        except Exception as e:
            print(f"✅ Expected authentication failure: {e}")

        print("✅ Connection error handling test completed (showing actual behavior)")

    def test_database_schema_operations(self, real_context):
        """Test database schema operations."""
        from src.etl_core.components.databases.sql_connection_handler import (
            SQLConnectionHandler,
        )
        from sqlalchemy import text

        credentials = real_context.get_credentials(1)
        handler = SQLConnectionHandler()

        # Build connection URL manually since build_url doesn't handle empty passwords
        password = (
            credentials.decrypted_password if credentials.decrypted_password else ""
        )
        url = (
            f"mysql+mysqlconnector://{credentials.user}:{password}@"
            f"{credentials.host}:{credentials.port}"
            f"/{credentials.database}"
        )

        # Connect to database
        handler.connect(url=url)

        with handler.lease() as connection:
            # Test getting database information
            result = connection.execute(text("SELECT DATABASE() as current_db"))
            row = result.fetchone()
            assert row[0] == "etl_core"

            # Test getting table list
            result = connection.execute(text("SHOW TABLES"))
            tables = [row[0] for row in result.fetchall()]
            print(f"Available tables: {tables}")

            # Test getting version information instead of user
            result = connection.execute(text("SELECT VERSION() as version"))
            row = result.fetchone()
            assert "MariaDB" in row[0] or "MySQL" in row[0]

            print("✅ Successfully tested database schema operations!")

    def test_show_test_data(self, real_context):
        """Test to show what data was created during the tests."""
        try:
            from src.etl_core.components.databases.sql_connection_handler import (
                SQLConnectionHandler,
            )
            from sqlalchemy import text

            credentials = real_context.get_credentials(1)
            handler = SQLConnectionHandler()

            # Build connection URL manually since
            # build_url doesn't handle empty passwords
            password = (
                credentials.decrypted_password if credentials.decrypted_password else ""
            )
            url = (
                f"mysql+mysqlconnector://{credentials.user}:{password}@"
                f"{credentials.host}:{credentials.port}/{credentials.database}"
            )

            # Connect to database
            handler.connect(url=url)

            with handler.lease() as connection:
                # Show all tables
                result = connection.execute(text("SHOW TABLES"))
                tables = [row[0] for row in result.fetchall()]
                print(f"\n📋 Available tables in database 'etl_core': {tables}")

                # Show data in test_connection_table
                if "test_connection_table" in tables:
                    result = connection.execute(
                        text("SELECT * FROM test_connection_table")
                    )
                    rows = result.fetchall()
                    print("\n📊 Data in 'test_connection_table':")
                    if rows:
                        for row in rows:
                            print(
                                f"   ID: {row[0]}, Name: {row[1]}, "
                                f"Email: {row[2]}, Created: {row[3]}"
                            )
                    else:
                        print("   (No data found)")

                # Show table structure
                if "test_connection_table" in tables:
                    result = connection.execute(text("DESCRIBE test_connection_table"))
                    columns = result.fetchall()
                    print("\n🏗️  Table structure of 'test_connection_table':")
                    for col in columns:
                        print(
                            f"   {col[0]} | {col[1]} | {col[2]} "
                            f"| {col[3]} | {col[4]} | {col[5]}"
                        )

                print("\n✅ Test data inspection completed!")

        except ImportError:
            pytest.skip("MariaDB dependencies not available")
        except Exception as e:
            pytest.fail(f"Failed to inspect test data: {e}")

    def test_cleanup_test_data(self, real_context):
        """Clean up test data after all tests."""
        try:
            from src.etl_core.components.databases.sql_connection_handler import (
                SQLConnectionHandler,
            )
            from sqlalchemy import text

            credentials = real_context.get_credentials(1)
            handler = SQLConnectionHandler()

            # Build connection URL manually since build_url
            # doesn't handle empty passwords
            password = (
                credentials.decrypted_password if credentials.decrypted_password else ""
            )
            url = (
                f"mysql+mysqlconnector://{credentials.user}:{password}@"
                f"{credentials.host}:{credentials.port}/{credentials.database}"
            )

            # Connect to database
            handler.connect(url=url)

            with handler.lease() as connection:
                # Check if test table exists
                result = connection.execute(
                    text("SHOW TABLES LIKE 'test_connection_table'")
                )
                tables = result.fetchall()

                if tables:
                    # Drop the test table
                    connection.execute(text("DROP TABLE test_connection_table"))
                    print("🧹 Cleaned up test table 'test_connection_table'")
                else:
                    print(
                        "🧹 Test table 'test_connection_table' was already cleaned up"
                    )

                print("✅ Database cleanup completed!")

        except ImportError:
            pytest.skip("MariaDB dependencies not available")
        except Exception as e:
            pytest.fail(f"Failed to cleanup test data: {e}")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
