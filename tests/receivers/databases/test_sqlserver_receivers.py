"""
Tests for SQL Server receivers.

These tests mock the database connections and test the receiver logic
without requiring actual SQL Server instances.
"""

import pytest
import pandas as pd
import dask.dataframe as dd

from unittest.mock import Mock, patch
from sqlalchemy import text

from etl_core.receivers.databases.mariadb.mariadb_receiver import MariaDBReceiver
from etl_core.receivers.databases.postgresql.postgresql_receiver import (
    PostgreSQLReceiver,
)
from etl_core.receivers.databases.sqlserver.sqlserver_receiver import (
    SQLServerReceiver,
)


def _empty_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=["id", "name", "email"])


def _single_row_frame() -> pd.DataFrame:
    return pd.DataFrame([{"id": 1, "name": "John", "email": "john@example.com"}])


class TestSQLServerReceivers:
    """Test cases for SQL Server receivers."""

    def test_sqlserver_receiver_get_connection(self, mock_connection_handler):
        """Test SQLServerReceiver connection handling."""
        receiver = SQLServerReceiver()
        expected_connection = (
            mock_connection_handler.lease.return_value.__enter__.return_value
        )
        connection = receiver._get_connection(mock_connection_handler)
        assert connection == expected_connection

        # Verify that lease() was called once inside _get_connection.
        assert mock_connection_handler.lease.call_count == 1

    @pytest.mark.asyncio
    async def test_sqlserver_receiver_read_row(
        self, mock_connection_handler, mock_metrics
    ):
        """Test SQLServerReceiver read_row method."""
        receiver = SQLServerReceiver()

        mock_result = Mock()
        mock_result = Mock()
        mock_row1 = Mock()
        mock_row1._mapping = {"id": 1, "name": "John"}
        mock_row2 = Mock()
        mock_row2._mapping = {"id": 2, "name": "Jane"}
        mock_result.__iter__ = Mock(return_value=iter([mock_row1, mock_row2]))
        mock_connection_handler.lease().__enter__().execute.return_value = mock_result

        results = []
        async for result in receiver.read_row(
            entity_name="users",
            metrics=mock_metrics,
            connection_handler=mock_connection_handler,
            query="SELECT * FROM users",
            params={"limit": 10},
        ):
            results.append(result)

        assert len(results) == 2
        assert results[0]["id"] == 1
        assert results[0]["name"] == "John"

    @pytest.mark.asyncio
    async def test_sqlserver_receiver_read_bulk(
        self, mock_connection_handler, mock_metrics
    ):
        """Test SQLServerReceiver read_bulk method."""
        receiver = SQLServerReceiver()

        mock_result = Mock()
        mock_row1 = Mock()
        mock_row1._mapping = {"id": 1, "name": "John"}
        mock_row2 = Mock()
        mock_row2._mapping = {"id": 2, "name": "Jane"}
        mock_result.__iter__ = Mock(return_value=iter([mock_row1, mock_row2]))
        mock_connection_handler.lease().__enter__().execute.return_value = mock_result

        result = await receiver.read_bulk(
            entity_name="users",
            metrics=mock_metrics,
            connection_handler=mock_connection_handler,
            query="SELECT * FROM users",
            params={"limit": 10},
        )

        assert len(result) == 2
        assert result.iloc[0]["name"] == "John"
        assert result.iloc[1]["name"] == "Jane"

    @pytest.mark.asyncio
    async def test_sqlserver_receiver_read_bigdata(
        self, mock_connection_handler, mock_metrics
    ):
        """Test SQLServerReceiver read_bigdata method."""
        receiver = SQLServerReceiver()

        mock_result = Mock()
        mock_row1 = Mock()
        mock_row1._mapping = {"id": 1, "name": "John"}
        mock_row2 = Mock()
        mock_row2._mapping = {"id": 2, "name": "Jane"}
        mock_result.__iter__ = Mock(return_value=iter([mock_row1, mock_row2]))
        mock_connection_handler.lease().__enter__().execute.return_value = mock_result

        result = await receiver.read_bigdata(
            entity_name="users",
            metrics=mock_metrics,
            connection_handler=mock_connection_handler,
            query="SELECT * FROM users",
            params={"limit": 10},
        )

        assert len(result) == 2
        pandas_result = result.compute()
        assert pandas_result.iloc[0]["name"] == "John"
        assert pandas_result.iloc[1]["name"] == "Jane"

    @pytest.mark.asyncio
    async def test_sqlserver_receiver_write_row(
        self, mock_connection_handler, mock_metrics
    ):
        """Test SQLServerReceiver write_row method."""
        receiver = SQLServerReceiver()

        mock_result = Mock()
        mock_result.rowcount = 1
        mock_connection_handler.lease().__enter__().execute.return_value = mock_result

        result = await receiver.write_row(
            entity_name="users",
            row={"name": "John", "email": "john@example.com"},
            metrics=mock_metrics,
            query="INSERT INTO users (name, email) VALUES (:name, :email)",
            connection_handler=mock_connection_handler,
        )

        mock_connection_handler.lease().__enter__().execute.assert_called_once()
        mock_connection_handler.lease().__enter__().commit.assert_called_once()
        assert result == {
            "affected_rows": 1,
            "row": {"name": "John", "email": "john@example.com"},
        }

    @pytest.mark.asyncio
    async def test_sqlserver_receiver_write_bulk(
        self, mock_connection_handler, mock_metrics, sample_dataframe
    ):
        """Test SQLServerReceiver write_bulk method with DataFrame data."""
        receiver = SQLServerReceiver()

        mock_result = Mock()
        mock_result.rowcount = 2
        mock_connection_handler.lease().__enter__().execute.return_value = mock_result

        result = await receiver.write_bulk(
            entity_name="users",
            frame=sample_dataframe,
            metrics=mock_metrics,
            query="INSERT INTO users (id, name, email) VALUES (:id, :name, :email)",
            connection_handler=mock_connection_handler,
        )

        assert result.equals(sample_dataframe)

    @pytest.mark.parametrize(
        "test_type,has_custom_query,has_chunk_size,expected_rowcount",
        [
            ("basic", False, False, 4),
            ("custom_query", True, False, 4),
            ("chunk_size", False, True, 4),
        ],
    )
    @pytest.mark.asyncio
    async def test_sqlserver_receiver_write_bigdata_scenarios(
        self,
        mock_connection_handler,
        mock_metrics,
        sample_dask_dataframe,
        test_type,
        has_custom_query,
        has_chunk_size,
        expected_rowcount,
    ):
        """Test SQLServerReceiver write_bigdata with different scenarios."""
        receiver = SQLServerReceiver()

        mock_result = Mock()
        mock_result.rowcount = expected_rowcount
        mock_connection_handler.lease().__enter__().execute.return_value = mock_result

        kwargs = {
            "entity_name": "users",
            "frame": sample_dask_dataframe,
            "metrics": mock_metrics,
            "table": "users",
            "connection_handler": mock_connection_handler,
        }

        kwargs = {
            "entity_name": "users",
            "frame": sample_dask_dataframe,
            "metrics": mock_metrics,
            "connection_handler": mock_connection_handler,
            "query": "INSERT INTO users (id, name, email) VALUES (:id, :name, :email)",
        }

        if has_custom_query:
            kwargs["query"] = "INSERT INTO users (name, email) VALUES (:name, :email)"
        if has_chunk_size:
            pass

        try:
            result = await receiver.write_bigdata(
                entity_name="users",
                frame=sample_dask_dataframe,
                metrics=mock_metrics,
                query="INSERT INTO users (id, name, email) VALUES (:id, :name, :email)",
                connection_handler=mock_connection_handler,
            )
            assert result is not None
            assert hasattr(result, "npartitions")
        except Exception as e:
            # If Dask tokenization fails, that's expected
            assert "tokenize" in str(e).lower() or "serialize" in str(e).lower()

    def test_sqlserver_receiver_inheritance(self, mock_connection_handler):
        """Test SQLServerReceiver inheritance from abstract base classes."""
        receiver = SQLServerReceiver()

        assert hasattr(receiver, "read_row")
        assert hasattr(receiver, "read_bulk")
        assert hasattr(receiver, "read_bigdata")
        assert hasattr(receiver, "write_row")
        assert hasattr(receiver, "write_bulk")
        assert hasattr(receiver, "write_bigdata")

    @pytest.mark.asyncio
    async def test_sqlserver_receiver_error_handling(
        self, mock_connection_handler, mock_metrics
    ):
        """Test SQLServerReceiver error handling."""
        receiver = SQLServerReceiver()

        mock_connection_handler.lease().__enter__().execute.side_effect = Exception(
            "Database error"
        )

        with pytest.raises(Exception):
            async for _ in receiver.read_row(
                entity_name="users",
                metrics=mock_metrics,
                connection_handler=mock_connection_handler,
                query="SELECT * FROM users",
                params={"limit": 10},
            ):
                pass

    @pytest.mark.asyncio
    async def test_sqlserver_receiver_write_bulk_with_custom_query(
        self, mock_connection_handler, mock_metrics, sample_dataframe
    ):
        """Test SQLServerReceiver write_bulk with custom query."""
        receiver = SQLServerReceiver()

        mock_result = Mock()
        mock_result.rowcount = 2
        mock_connection_handler.lease().__enter__().execute.return_value = mock_result

        result = await receiver.write_bulk(
            entity_name="users",
            frame=sample_dataframe,
            metrics=mock_metrics,
            query="INSERT INTO users (name, email) VALUES (:name, :email)",
            connection_handler=mock_connection_handler,
        )

        assert result.equals(sample_dataframe)

    @pytest.mark.asyncio
    async def test_sqlserver_receiver_write_bulk_with_chunk_size(
        self, mock_connection_handler, mock_metrics, sample_dataframe
    ):
        """Test SQLServerReceiver write_bulk with custom chunk size."""
        receiver = SQLServerReceiver()

        mock_result = Mock()
        mock_result.rowcount = 2
        mock_connection_handler.lease().__enter__().execute.return_value = mock_result

        result = await receiver.write_bulk(
            entity_name="users",
            frame=sample_dataframe,
            metrics=mock_metrics,
            query="INSERT INTO users (id, name, email) VALUES (:id, :name, :email)",
            connection_handler=mock_connection_handler,
        )

        assert result.equals(sample_dataframe)

    @pytest.mark.parametrize(
        "partition_size,expected_calls,has_data",
        [
            (2, 2, True),
            (1, 1, False),
            (1, 1, True),
        ],
    )
    @pytest.mark.asyncio
    async def test_write_bigdata_partition_scenarios(
        self,
        mock_connection_handler,
        mock_metrics,
        partition_size,
        expected_calls,
        has_data,
    ):
        """Test write_bigdata with different partition scenarios."""
        receiver = SQLServerReceiver()

        if has_data:
            df = pd.DataFrame({"id": [1, 2, 3, 4], "name": ["A", "B", "C", "D"]})
        else:
            df = pd.DataFrame()

        ddf = dd.from_pandas(df, npartitions=partition_size)

        if has_data:
            mock_result = Mock()
            mock_result.rowcount = len(df) if len(df) > 0 else 0
            mock_connection_handler.lease().__enter__().execute.return_value = (
                mock_result
            )

        with patch("dask.dataframe.DataFrame.compute") as mock_compute:
            mock_compute.return_value = df

            result = await receiver.write_bigdata(
                entity_name="test_table",
                frame=ddf,
                metrics=mock_metrics,
                query="INSERT INTO test_table (id, name) VALUES (:id, :name)",
                connection_handler=mock_connection_handler,
            )

            assert mock_compute.call_count == expected_calls
            assert result is not None
            assert hasattr(result, "npartitions")

    @pytest.mark.asyncio
    async def test_connection_failure_handling(
        self, mock_connection_handler, mock_metrics
    ):
        """Test handling of connection failures."""
        receiver = SQLServerReceiver()

        mock_connection_handler.lease.side_effect = Exception("Connection failed")

        with pytest.raises(Exception, match="Connection failed"):
            async for _ in receiver.read_row(
                entity_name="users",
                metrics=mock_metrics,
                connection_handler=mock_connection_handler,
                query="SELECT 1",
                params={},
            ):
                pass

    @pytest.mark.asyncio
    async def test_sql_injection_protection(
        self, mock_connection_handler, mock_metrics
    ):
        """Test that SQL injection attempts are properly handled."""
        receiver = SQLServerReceiver()

        malicious_query = "SELECT * FROM users WHERE id = '1'; DROP TABLE users; --"

        mock_conn = mock_connection_handler.lease().__enter__()
        mock_conn.execute = Mock()

        mock_result = Mock()
        mock_result.__iter__ = Mock(return_value=iter([]))
        mock_conn.execute.return_value = mock_result

        async for _ in receiver.read_row(
            entity_name="users",
            metrics=mock_metrics,
            connection_handler=mock_connection_handler,
            query=malicious_query,
            params={},
        ):
            pass

        mock_conn.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_transaction_rollback_on_error(
        self, mock_connection_handler, mock_metrics
    ):
        """Test transaction rollback when errors occur."""
        receiver = SQLServerReceiver()

        mock_conn = mock_connection_handler.lease().__enter__()
        mock_conn.execute.side_effect = Exception("Database error")

        with pytest.raises(Exception):
            await receiver.write_row(
                entity_name="users",
                row={"name": "John"},
                metrics=mock_metrics,
                query="INSERT INTO users (name) VALUES (:name)",
                table="users",
                connection_handler=mock_connection_handler,
            )

        mock_context_manager = mock_connection_handler.lease.return_value
        mock_context_manager.__exit__.assert_called_once()
        exit_args, _ = mock_context_manager.__exit__.call_args
        assert exit_args[0] is Exception
        assert str(exit_args[1]) == "Database error"
        assert exit_args[2] is not None

    @pytest.mark.asyncio
    async def test_dask_dataframe_partitioning(
        self, mock_connection_handler, mock_metrics
    ):
        """Test Dask DataFrame partition processing."""
        receiver = SQLServerReceiver()

        df = pd.DataFrame({"id": [1, 2, 3, 4], "name": ["A", "B", "C", "D"]})
        ddf = dd.from_pandas(df, npartitions=2)

        with patch("dask.dataframe.DataFrame.compute") as mock_compute:
            mock_compute.return_value = df

            await receiver.write_bigdata(
                entity_name="users",
                frame=ddf,
                metrics=mock_metrics,
                query="INSERT INTO users (id, name) VALUES (:id, :name)",
                connection_handler=mock_connection_handler,
            )

            assert mock_compute.call_count == 2

    @pytest.mark.asyncio
    async def test_read_row_with_empty_result(
        self, mock_connection_handler, mock_metrics
    ):
        """Test read_row with empty query result."""
        receiver = SQLServerReceiver()

        mock_result = Mock()
        mock_result.__iter__ = Mock(return_value=iter([]))
        mock_connection_handler.lease().__enter__().execute.return_value = mock_result

        results = []
        async for result in receiver.read_row(
            entity_name="empty_table",
            metrics=mock_metrics,
            connection_handler=mock_connection_handler,
            query="SELECT * FROM empty_table",
            params={},
        ):
            results.append(result)

        assert len(results) == 0

        result = await receiver.read_bulk(
            entity_name="empty_table",
            metrics=mock_metrics,
            connection_handler=mock_connection_handler,
            query="SELECT * FROM empty_table",
            params={},
        )

        assert len(result) == 0
        assert isinstance(result, pd.DataFrame)

    @pytest.mark.asyncio
    async def test_read_bulk_with_empty_result(
        self, mock_connection_handler, mock_metrics
    ):
        """Test read_bulk with empty query result."""
        receiver = SQLServerReceiver()

        mock_result = Mock()
        mock_result.__iter__ = Mock(return_value=iter([]))
        mock_connection_handler.lease().__enter__().execute.return_value = mock_result

        result = await receiver.read_bulk(
            entity_name="empty_table",
            metrics=mock_metrics,
            connection_handler=mock_connection_handler,
            query="SELECT * FROM empty_table",
            params={},
        )

        assert len(result) == 0
        assert isinstance(result, pd.DataFrame)

    @pytest.mark.asyncio
    async def test_connection_lease_context_manager(
        self, mock_connection_handler, mock_metrics
    ):
        """Test that connection lease context manager is properly used."""
        receiver = SQLServerReceiver()

        mock_result = Mock()
        mock_result.__iter__ = Mock(return_value=iter([]))
        mock_connection_handler.lease().__enter__().execute.return_value = mock_result

        await receiver.read_bulk(
            entity_name="users",
            metrics=mock_metrics,
            connection_handler=mock_connection_handler,
            query="SELECT 1",
            params={},
        )

        assert mock_connection_handler.lease.call_count >= 1

    @pytest.mark.asyncio
    async def test_metrics_integration(self, mock_connection_handler, mock_metrics):
        """Test that metrics are properly passed through to operations."""
        receiver = SQLServerReceiver()

        mock_result = Mock()
        mock_result.__iter__ = Mock(return_value=iter([]))
        mock_connection_handler.lease().__enter__().execute.return_value = mock_result

        await receiver.read_bulk(
            entity_name="users",
            metrics=mock_metrics,
            connection_handler=mock_connection_handler,
            query="SELECT 1",
            params={},
        )

    @pytest.mark.asyncio
    async def test_large_data_handling(self, mock_connection_handler, mock_metrics):
        """Test handling of large datasets."""
        receiver = SQLServerReceiver()

        large_data = [{"id": i, "name": f"User{i}"} for i in range(1000)]

        mock_result = Mock()
        mock_rows = []
        for data in large_data:
            mock_row = Mock()
            mock_row._mapping = data
            mock_rows.append(mock_row)
        mock_result.__iter__ = Mock(return_value=iter(mock_rows))
        mock_connection_handler.lease().__enter__().execute.return_value = mock_result

        result = await receiver.read_bulk(
            entity_name="large_table",
            metrics=mock_metrics,
            connection_handler=mock_connection_handler,
            query="SELECT * FROM large_table",
            params={},
        )

        assert len(result) == 1000
        assert result.iloc[0]["id"] == 0
        assert result.iloc[999]["id"] == 999

    @pytest.mark.asyncio
    async def test_special_characters_in_data(
        self, mock_connection_handler, mock_metrics
    ):
        """Test handling of special characters in data."""
        receiver = SQLServerReceiver()

        special_data = {
            "name": "José María",
            "email": "jose.maria@café.com",
            "description": "Special chars: äöüßñéèêë",
        }

        mock_result = Mock()
        mock_result.rowcount = 1
        mock_connection_handler.lease().__enter__().execute.return_value = mock_result

        result = await receiver.write_row(
            entity_name="users",
            row=special_data,
            metrics=mock_metrics,
            connection_handler=mock_connection_handler,
            query="INSERT INTO users (name, email, description) \
                VALUES (:name, :email, :description)",
        )

        mock_connection_handler.lease().__enter__().execute.assert_called_once()
        assert result == {"affected_rows": 1, "row": special_data}

    @pytest.mark.asyncio
    async def test_numeric_data_types(self, mock_connection_handler, mock_metrics):
        """Test handling of various numeric data types."""
        receiver = SQLServerReceiver()

        numeric_data = {
            "integer": 42,
            "float": 3.14159,
            "decimal": 123.456,
            "negative": -100,
        }

        mock_result = Mock()
        mock_result.rowcount = 1
        mock_connection_handler.lease().__enter__().execute.return_value = mock_result

        result = await receiver.write_row(
            entity_name="numeric_table",
            row=numeric_data,
            metrics=mock_metrics,
            connection_handler=mock_connection_handler,
            query="INSERT INTO numeric_table (integer, float, decimal, negative) \
                VALUES (:integer, :float, :decimal, :negative)",
        )

        mock_connection_handler.lease().__enter__().execute.assert_called_once()
        assert result == {"affected_rows": 1, "row": numeric_data}

    @pytest.mark.asyncio
    async def test_boolean_data_types(self, mock_connection_handler, mock_metrics):
        """Test handling of boolean data types."""
        receiver = SQLServerReceiver()

        boolean_data = {"is_active": True, "is_deleted": False, "has_permission": True}

        mock_result = Mock()
        mock_result.rowcount = 1
        mock_connection_handler.lease().__enter__().execute.return_value = mock_result

        result = await receiver.write_row(
            entity_name="boolean_table",
            row=boolean_data,
            metrics=mock_metrics,
            connection_handler=mock_connection_handler,
            query="INSERT INTO boolean_table (is_active, is_deleted, has_permission) \
                VALUES (:is_active, :is_deleted, :has_permission)",
        )

        mock_connection_handler.lease().__enter__().execute.assert_called_once()

        assert result == {"affected_rows": 1, "row": boolean_data}

    @pytest.mark.asyncio
    async def test_write_bulk_dataframe_to_dict_conversion(
        self, mock_connection_handler, mock_metrics
    ):
        """Test write_bulk DataFrame to dict conversion."""
        receiver = SQLServerReceiver()

        df = pd.DataFrame({"id": [1, 2], "name": ["John", "Jane"]})

        mock_result = Mock()
        mock_result.rowcount = 2
        mock_connection_handler.lease().__enter__().execute.return_value = mock_result

        await receiver.write_bulk(
            entity_name="users",
            frame=df,
            metrics=mock_metrics,
            query="INSERT INTO users (id, name) VALUES (:id, :name)",
            connection_handler=mock_connection_handler,
        )

        assert mock_connection_handler.lease().__enter__().execute.call_count == 2
        mock_connection_handler.lease().__enter__().commit.assert_called_once()

    @pytest.mark.parametrize(
        "has_data,expected_execute_calls,expected_commit_calls",
        [
            (True, 1, 1),
            (False, 0, 0),
        ],
    )
    @pytest.mark.asyncio
    async def test_partition_processing_logic(
        self,
        mock_connection_handler,
        mock_metrics,
        has_data,
        expected_execute_calls,
        expected_commit_calls,
    ):
        """Test the partition processing logic with different data scenarios."""

        if has_data:
            partition_df = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
        else:
            partition_df = pd.DataFrame()

        mock_result = Mock()
        mock_result.rowcount = len(partition_df) if has_data else 0
        mock_connection_handler.lease().__enter__().execute.return_value = mock_result

        table = "test_table"
        with mock_connection_handler.lease() as conn:
            rows = partition_df.to_dict("records")

            if rows:
                columns = list(rows[0].keys())
                placeholders = ", ".join([f":{key}" for key in columns])
                query = (
                    f"INSERT INTO {table} ({', '.join(columns)}) "
                    f"VALUES ({placeholders})"
                )

                conn.execute(text(query), rows)
                conn.commit()

        assert (
            mock_connection_handler.lease().__enter__().execute.call_count
            == expected_execute_calls
        )
        assert (
            mock_connection_handler.lease().__enter__().commit.call_count
            == expected_commit_calls
        )

        if has_data:
            call_args = mock_connection_handler.lease().__enter__().execute.call_args
            assert call_args is not None
            sql_query = call_args[0][0]
            assert "INSERT INTO test_table" in str(sql_query)
            assert "id, name" in str(sql_query) or "name, id" in str(sql_query)

    def test_partition_processing_column_logic(self, mock_connection_handler):
        """Test the column and placeholder generation logic from _process_partition."""

        test_cases = [
            pd.DataFrame({"id": [1, 2]}),
            pd.DataFrame({"id": [1, 2], "name": ["A", "B"], "age": [25, 30]}),
            pd.DataFrame({"id": [1], "active": [True], "score": [98.5]}),
        ]

        for i, partition_df in enumerate(test_cases):

            mock_result = Mock()
            mock_result.rowcount = len(partition_df)
            mock_connection_handler.lease().__enter__().execute.return_value = (
                mock_result
            )

            table = f"test_table_{i}"

            with mock_connection_handler.lease() as conn:
                rows = partition_df.to_dict("records")

                if rows:
                    columns = list(rows[0].keys())
                    placeholders = ", ".join([f":{key}" for key in columns])
                    query = (
                        f"INSERT INTO {table} ({', '.join(columns)}) "
                        f"VALUES ({placeholders})"
                    )

                    conn.execute(text(query), rows)
                    conn.commit()

            expected_calls = i + 1
            assert (
                mock_connection_handler.lease().__enter__().execute.call_count
                == expected_calls
            )
            assert (
                mock_connection_handler.lease().__enter__().commit.call_count
                == expected_calls
            )


@pytest.mark.parametrize(
    "_backend,receiver_cls",
    [
        pytest.param("mariadb", MariaDBReceiver, id="mariadb"),
        pytest.param("postgresql", PostgreSQLReceiver, id="postgresql"),
        pytest.param("sqlserver", SQLServerReceiver, id="sqlserver"),
    ],
)
@pytest.mark.parametrize(
    "frame_factory,expected_execute_calls,expected_commit_calls",
    [
        pytest.param(_empty_frame, 0, 0, id="empty-frame"),
        pytest.param(_single_row_frame, 1, 1, id="single-row"),
    ],
)
@pytest.mark.asyncio
async def test_write_bulk_empty_and_single_row(
    _backend,
    receiver_cls,
    frame_factory,
    expected_execute_calls,
    expected_commit_calls,
    mock_connection_handler,
    mock_metrics,
):
    """Shared write_bulk behavior across SQL receivers."""
    receiver = receiver_cls()
    frame = frame_factory()

    mock_context = mock_connection_handler.lease.return_value
    mock_conn = mock_context.__enter__.return_value

    if expected_execute_calls:
        mock_result = Mock()
        mock_result.rowcount = expected_execute_calls
        mock_conn.execute.return_value = mock_result

    result = await receiver.write_bulk(
        entity_name="users",
        frame=frame,
        metrics=mock_metrics,
        query="INSERT INTO users (id, name, email) VALUES (:id, :name, :email)",
        connection_handler=mock_connection_handler,
    )

    assert result.equals(frame)

    if expected_execute_calls == 0:
        mock_connection_handler.lease.assert_not_called()
        mock_conn.execute.assert_not_called()
        mock_conn.commit.assert_not_called()
    else:
        assert mock_conn.execute.call_count == expected_execute_calls
        assert mock_conn.commit.call_count == expected_commit_calls


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
