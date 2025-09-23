"""Tests for database operation mixin functionality."""

import pytest
from pydantic import BaseModel, Field
from etl_core.components.databases.database_operation_mixin import DatabaseOperationMixin
from etl_core.components.databases.if_exists_strategy import DatabaseOperation


class DatabaseOperationMixinComponent(BaseModel, DatabaseOperationMixin):
    """Test component class that uses the DatabaseOperationMixin."""
    
    name: str = Field(default="test-component")


class TestDatabaseOperationMixin:
    """Tests for the DatabaseOperationMixin class."""

    def test_mixin_default_values(self):
        """Test that mixin provides correct default values."""
        component = DatabaseOperationMixinComponent()
        
        assert component.where_conditions == []
        assert component.operation == DatabaseOperation.INSERT

    def test_mixin_custom_values(self):
        """Test setting custom values for mixin fields."""
        component = DatabaseOperationMixinComponent(
            where_conditions=["id = 1", "status = 'active'"],
            operation=DatabaseOperation.UPDATE,
        )
        
        assert component.where_conditions == ["id = 1", "status = 'active'"]
        assert component.operation == DatabaseOperation.UPDATE

    def test_build_query_insert(self):
        """Test _build_query for INSERT operation."""
        component = DatabaseOperationMixinComponent(
            operation=DatabaseOperation.INSERT
        )
        
        table = "test_table"
        columns = ["id", "name", "status"]
        
        query = component._build_query(table, columns, DatabaseOperation.INSERT)
        expected = "INSERT INTO test_table (id, name, status) VALUES (:id, :name, :status)"
        
        assert query == expected

    def test_build_query_insert_with_different_columns(self):
        """Test _build_query for INSERT with different columns."""
        component = DatabaseOperationMixinComponent()
        
        table = "users"
        columns = ["user_id", "email", "created_at"]
        
        query = component._build_query(table, columns, DatabaseOperation.INSERT)
        expected = "INSERT INTO users (user_id, email, created_at) VALUES (:user_id, :email, :created_at)"
        
        assert query == expected

    def test_build_query_upsert(self):
        """Test _build_query for UPSERT operation."""
        component = DatabaseOperationMixinComponent(
            operation=DatabaseOperation.UPSERT
        )
        
        table = "test_table"
        columns = ["id", "name", "status"]
        
        query = component._build_query(table, columns, DatabaseOperation.UPSERT)
        # Note: Default implementation falls back to INSERT for UPSERT
        expected = "INSERT INTO test_table (id, name, status) VALUES (:id, :name, :status)"
        
        assert query == expected

    def test_build_query_truncate(self):
        """Test _build_query for TRUNCATE operation."""
        component = DatabaseOperationMixinComponent(
            operation=DatabaseOperation.TRUNCATE
        )
        
        table = "test_table"
        columns = ["id", "name", "status"]
        
        query = component._build_query(table, columns, DatabaseOperation.TRUNCATE)
        expected = "TRUNCATE TABLE test_table; INSERT INTO test_table             (id, name, status) VALUES (:id, :name, :status)"
        
        assert query == expected

    def test_build_query_update_with_conditions(self):
        """Test _build_query for UPDATE operation with conditions."""
        component = DatabaseOperationMixinComponent(
            operation=DatabaseOperation.UPDATE,
            where_conditions=["id = :id_filter", "status = 'active'"]
        )
        
        table = "test_table"
        columns = ["name", "status"]
        
        query = component._build_query(table, columns, DatabaseOperation.UPDATE)
        expected = "UPDATE test_table SET name = :name, status = :status WHERE id = :id_filter AND status = 'active'"
        
        assert query == expected

    def test_build_query_update_without_conditions(self):
        """Test _build_query for UPDATE operation without conditions raises error."""
        component = DatabaseOperationMixinComponent(
            operation=DatabaseOperation.UPDATE,
            where_conditions=[]  # Empty conditions
        )
        
        table = "test_table"
        columns = ["name", "status"]
        
        with pytest.raises(ValueError, match="UPDATE operation requires where_conditions"):
            component._build_query(table, columns, DatabaseOperation.UPDATE)

    def test_build_query_update_single_condition(self):
        """Test _build_query for UPDATE with single condition."""
        component = DatabaseOperationMixinComponent(
            operation=DatabaseOperation.UPDATE,
            where_conditions=["id = :target_id"]
        )
        
        table = "users"
        columns = ["email", "updated_at"]
        
        query = component._build_query(table, columns, DatabaseOperation.UPDATE)
        expected = "UPDATE users SET email = :email, updated_at = :updated_at WHERE id = :target_id"
        
        assert query == expected

    def test_build_query_update_multiple_conditions(self):
        """Test _build_query for UPDATE with multiple conditions."""
        component = DatabaseOperationMixinComponent(
            operation=DatabaseOperation.UPDATE,
            where_conditions=["tenant_id = :tenant", "status = 'draft'", "created_at > :date_filter"]
        )
        
        table = "documents"
        columns = ["title", "content", "status"]
        
        query = component._build_query(table, columns, DatabaseOperation.UPDATE)
        expected = (
            "UPDATE documents SET title = :title, content = :content, status = :status "
            "WHERE tenant_id = :tenant AND status = 'draft' AND created_at > :date_filter"
        )
        
        assert query == expected

    def test_build_query_with_kwargs(self):
        """Test _build_query with additional kwargs."""
        component = DatabaseOperationMixinComponent()
        
        table = "test_table"
        columns = ["id", "name"]
        
        # kwargs should not affect the basic implementation
        query = component._build_query(
            table, columns, DatabaseOperation.INSERT,
            extra_param="value", another_param=123
        )
        expected = "INSERT INTO test_table (id, name) VALUES (:id, :name)"
        
        assert query == expected

    def test_build_query_empty_columns(self):
        """Test _build_query with empty columns list."""
        component = DatabaseOperationMixinComponent()
        
        table = "test_table"
        columns = []
        
        query = component._build_query(table, columns, DatabaseOperation.INSERT)
        expected = "INSERT INTO test_table () VALUES ()"
        
        assert query == expected

    def test_build_query_single_column(self):
        """Test _build_query with single column."""
        component = DatabaseOperationMixinComponent()
        
        table = "counters"
        columns = ["count"]
        
        query = component._build_query(table, columns, DatabaseOperation.INSERT)
        expected = "INSERT INTO counters (count) VALUES (:count)"
        
        assert query == expected

    def test_where_conditions_default_factory(self):
        """Test that where_conditions uses default factory correctly."""
        component1 = DatabaseOperationMixinComponent()
        component2 = DatabaseOperationMixinComponent()
        
        # Both should have separate empty lists
        assert component1.where_conditions == []
        assert component2.where_conditions == []
        assert component1.where_conditions is not component2.where_conditions

    def test_mixin_field_descriptions(self):
        """Test that mixin fields have proper descriptions."""
        component = DatabaseOperationMixinComponent()
        
        # Check that fields exist with expected descriptions
        fields = component.model_fields
        
        assert "where_conditions" in fields
        assert "operation" in fields
        
        where_desc = fields["where_conditions"].description
        op_desc = fields["operation"].description
        
        # Handle case where description might be a tuple
        where_desc_str = str(where_desc) if isinstance(where_desc, tuple) else where_desc
        op_desc_str = str(op_desc) if isinstance(op_desc, tuple) else op_desc
        
        assert "WHERE conditions for UPDATE operations" in where_desc_str
        assert "Database operation type" in op_desc_str


class TestDatabaseOperationIntegration:
    """Integration tests for DatabaseOperationMixin."""

    def test_all_database_operations(self):
        """Test that all database operations can be built successfully."""
        operations_to_test = [
            (DatabaseOperation.INSERT, []),
            (DatabaseOperation.UPSERT, []),
            (DatabaseOperation.TRUNCATE, []),
            (DatabaseOperation.UPDATE, ["id = 1"]),  # UPDATE needs conditions
        ]
        
        table = "test_table"
        columns = ["id", "name", "value"]
        
        for operation, conditions in operations_to_test:
            component = DatabaseOperationMixinComponent(
                operation=operation,
                where_conditions=conditions
            )
            
            # Should not raise any exceptions
            query = component._build_query(table, columns, operation)
            assert isinstance(query, str)
            assert len(query) > 0
            assert table in query

    def test_operation_enum_coverage(self):
        """Test that all DatabaseOperation enum values are handled."""
        component = DatabaseOperationMixinComponent(
            where_conditions=["id = 1"]  # For UPDATE case
        )
        
        table = "test_table"
        columns = ["id", "name"]
        
        # Test each enum value
        for operation in DatabaseOperation:
            try:
                query = component._build_query(table, columns, operation)
                assert isinstance(query, str)
                assert len(query) > 0
            except ValueError as e:
                # Only UPDATE without conditions should raise ValueError
                if operation == DatabaseOperation.UPDATE:
                    # Reset conditions and try again
                    component.where_conditions = ["id = 1"]
                    query = component._build_query(table, columns, operation)
                    assert isinstance(query, str)
                else:
                    raise e

    def test_query_parameter_consistency(self):
        """Test that generated queries use consistent parameter naming."""
        component = DatabaseOperationMixinComponent()
        
        table = "test_table"
        columns = ["user_id", "email_address", "created_at"]
        
        for operation in [DatabaseOperation.INSERT, DatabaseOperation.UPSERT, DatabaseOperation.TRUNCATE]:
            query = component._build_query(table, columns, operation)
            
            # All columns should appear as parameters
            for col in columns:
                assert f":{col}" in query

    def test_update_query_structure(self):
        """Test UPDATE query structure and parameter usage."""
        component = DatabaseOperationMixinComponent(
            where_conditions=["id = :id_param", "tenant = :tenant_param"]
        )
        
        table = "users"
        columns = ["name", "email", "updated_at"]
        
        query = component._build_query(table, columns, DatabaseOperation.UPDATE)
        
        # Should contain SET clause with all columns
        for col in columns:
            assert f"{col} = :{col}" in query
        
        # Should contain WHERE clause with all conditions
        for condition in component.where_conditions:
            assert condition in query
        
        # Should have proper structure
        assert "UPDATE" in query
        assert "SET" in query
        assert "WHERE" in query
        assert "AND" in query  # Multiple conditions
