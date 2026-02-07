"""Tests for BaseHandler class providing shared handler functionality."""

import pytest
from unittest.mock import MagicMock, patch
from typing import Optional
from sqlmodel import SQLModel, Field

from etl_core.persistence.handlers.base_handler import BaseHandler


class DummyTable(SQLModel, table=True):
    """Dummy table for testing generic CRUD operations."""
    __tablename__ = "dummy_test_table"
    id: Optional[str] = Field(default=None, primary_key=True)


class TestBaseHandlerInit:
    """Test BaseHandler initialization."""

    @patch("etl_core.persistence.handlers.base_handler.ensure_schema")
    def test_init_calls_ensure_schema(self, mock_ensure_schema):
        """ensure_schema is called during initialization."""
        mock_engine = MagicMock()
        handler = BaseHandler(engine_=mock_engine)

        mock_ensure_schema.assert_called_once()
        assert handler.engine == mock_engine

    @patch("etl_core.persistence.handlers.base_handler.ensure_schema")
    def test_init_uses_default_engine(self, mock_ensure_schema):
        """Default engine is used when none provided."""
        # The default engine parameter is evaluated at function definition time,
        # so we need to check that the handler uses the module's engine when
        # no engine_ is explicitly passed. We verify this by checking the type.
        from etl_core.persistence.db import engine as default_engine
        handler = BaseHandler()
        # Should use the actual default engine from the db module
        assert handler.engine is default_engine


class TestBaseHandlerSession:
    """Test _session context manager."""

    @patch("etl_core.persistence.handlers.base_handler.ensure_schema")
    @patch("etl_core.persistence.handlers.base_handler.Session")
    def test_session_context_manager(self, mock_session_cls, mock_ensure_schema):
        """_session yields a session and properly closes it."""
        mock_engine = MagicMock()
        mock_session = MagicMock()
        mock_session_cls.return_value.__enter__.return_value = mock_session

        handler = BaseHandler(engine_=mock_engine)

        with handler._session() as session:
            assert session == mock_session

        mock_session_cls.assert_called_once_with(mock_engine)


class TestBaseHandlerGenericCrud:
    """Test generic CRUD helper methods."""

    @patch("etl_core.persistence.handlers.base_handler.ensure_schema")
    def test_get_by_id_without_table_raises(self, mock_ensure_schema):
        """_get_by_id raises NotImplementedError when no table specified."""
        handler = BaseHandler(engine_=MagicMock())

        with pytest.raises(NotImplementedError, match="No table class specified"):
            handler._get_by_id("some-id")

    @patch("etl_core.persistence.handlers.base_handler.ensure_schema")
    def test_list_all_without_table_raises(self, mock_ensure_schema):
        """_list_all raises NotImplementedError when no table specified."""
        handler = BaseHandler(engine_=MagicMock())

        with pytest.raises(NotImplementedError, match="No table class specified"):
            handler._list_all()

    @patch("etl_core.persistence.handlers.base_handler.ensure_schema")
    def test_delete_by_id_without_table_raises(self, mock_ensure_schema):
        """_delete_by_id raises NotImplementedError when no table specified."""
        handler = BaseHandler(engine_=MagicMock())

        with pytest.raises(NotImplementedError, match="No table class specified"):
            handler._delete_by_id("some-id")

    @patch("etl_core.persistence.handlers.base_handler.ensure_schema")
    @patch("etl_core.persistence.handlers.base_handler.Session")
    def test_get_by_id_with_table_cls(self, mock_session_cls, mock_ensure_schema):
        """_get_by_id fetches row using provided table class."""
        mock_session = MagicMock()
        mock_session_cls.return_value.__enter__.return_value = mock_session
        mock_row = MagicMock()
        mock_session.get.return_value = mock_row

        handler = BaseHandler(engine_=MagicMock())
        result = handler._get_by_id("test-id", table_cls=DummyTable)

        mock_session.get.assert_called_once_with(DummyTable, "test-id")
        assert result == mock_row

    @patch("etl_core.persistence.handlers.base_handler.ensure_schema")
    @patch("etl_core.persistence.handlers.base_handler.Session")
    def test_get_by_id_returns_none_when_not_found(self, mock_session_cls, mock_ensure_schema):
        """_get_by_id returns None when row not found."""
        mock_session = MagicMock()
        mock_session_cls.return_value.__enter__.return_value = mock_session
        mock_session.get.return_value = None

        handler = BaseHandler(engine_=MagicMock())
        result = handler._get_by_id("missing-id", table_cls=DummyTable)

        assert result is None

    @patch("etl_core.persistence.handlers.base_handler.ensure_schema")
    @patch("etl_core.persistence.handlers.base_handler.Session")
    def test_list_all_with_table_cls(self, mock_session_cls, mock_ensure_schema):
        """_list_all returns all rows from provided table."""
        mock_session = MagicMock()
        mock_session_cls.return_value.__enter__.return_value = mock_session
        mock_rows = [MagicMock(), MagicMock()]
        mock_session.exec.return_value.all.return_value = mock_rows

        handler = BaseHandler(engine_=MagicMock())
        result = handler._list_all(table_cls=DummyTable)

        assert result == mock_rows

    @patch("etl_core.persistence.handlers.base_handler.ensure_schema")
    @patch("etl_core.persistence.handlers.base_handler.Session")
    def test_delete_by_id_success(self, mock_session_cls, mock_ensure_schema):
        """_delete_by_id returns True when row deleted."""
        mock_session = MagicMock()
        mock_session_cls.return_value.__enter__.return_value = mock_session
        mock_row = MagicMock()
        mock_session.get.return_value = mock_row

        handler = BaseHandler(engine_=MagicMock())
        result = handler._delete_by_id("test-id", table_cls=DummyTable)

        mock_session.delete.assert_called_once_with(mock_row)
        mock_session.commit.assert_called_once()
        assert result is True

    @patch("etl_core.persistence.handlers.base_handler.ensure_schema")
    @patch("etl_core.persistence.handlers.base_handler.Session")
    def test_delete_by_id_not_found_returns_false(self, mock_session_cls, mock_ensure_schema):
        """_delete_by_id returns False when row not found."""
        mock_session = MagicMock()
        mock_session_cls.return_value.__enter__.return_value = mock_session
        mock_session.get.return_value = None

        handler = BaseHandler(engine_=MagicMock())
        result = handler._delete_by_id("missing-id", table_cls=DummyTable)

        mock_session.delete.assert_not_called()
        assert result is False

    @patch("etl_core.persistence.handlers.base_handler.ensure_schema")
    @patch("etl_core.persistence.handlers.base_handler.Session")
    def test_delete_by_id_raises_custom_error(self, mock_session_cls, mock_ensure_schema):
        """_delete_by_id raises custom error when specified."""
        mock_session = MagicMock()
        mock_session_cls.return_value.__enter__.return_value = mock_session
        mock_session.get.return_value = None

        class CustomNotFoundError(Exception):
            pass

        handler = BaseHandler(engine_=MagicMock())

        with pytest.raises(CustomNotFoundError):
            handler._delete_by_id(
                "missing-id",
                table_cls=DummyTable,
                not_found_error=CustomNotFoundError,
            )


class TestBaseHandlerWithTableAttribute:
    """Test handlers that set _table class attribute."""

    @patch("etl_core.persistence.handlers.base_handler.ensure_schema")
    @patch("etl_core.persistence.handlers.base_handler.Session")
    def test_get_by_id_uses_table_attribute(self, mock_session_cls, mock_ensure_schema):
        """_get_by_id uses _table attribute when table_cls not provided."""
        mock_session = MagicMock()
        mock_session_cls.return_value.__enter__.return_value = mock_session
        mock_row = MagicMock()
        mock_session.get.return_value = mock_row

        class ConcreteHandler(BaseHandler):
            _table = DummyTable

        handler = ConcreteHandler(engine_=MagicMock())
        result = handler._get_by_id("test-id")

        mock_session.get.assert_called_once_with(DummyTable, "test-id")
        assert result == mock_row

    @patch("etl_core.persistence.handlers.base_handler.ensure_schema")
    @patch("etl_core.persistence.handlers.base_handler.Session")
    def test_list_all_uses_table_attribute(self, mock_session_cls, mock_ensure_schema):
        """_list_all uses _table attribute when table_cls not provided."""
        mock_session = MagicMock()
        mock_session_cls.return_value.__enter__.return_value = mock_session
        mock_rows = [MagicMock()]
        mock_session.exec.return_value.all.return_value = mock_rows

        class ConcreteHandler(BaseHandler):
            _table = DummyTable

        handler = ConcreteHandler(engine_=MagicMock())
        result = handler._list_all()

        assert result == mock_rows

    @patch("etl_core.persistence.handlers.base_handler.ensure_schema")
    @patch("etl_core.persistence.handlers.base_handler.Session")
    def test_delete_by_id_uses_table_attribute(self, mock_session_cls, mock_ensure_schema):
        """_delete_by_id uses _table attribute when table_cls not provided."""
        mock_session = MagicMock()
        mock_session_cls.return_value.__enter__.return_value = mock_session
        mock_row = MagicMock()
        mock_session.get.return_value = mock_row

        class ConcreteHandler(BaseHandler):
            _table = DummyTable

        handler = ConcreteHandler(engine_=MagicMock())
        result = handler._delete_by_id("test-id")

        mock_session.get.assert_called_once_with(DummyTable, "test-id")
        mock_session.delete.assert_called_once_with(mock_row)
        assert result is True


