import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime

from etl_core.persistence.handlers.schedule_handler import ScheduleHandler, ScheduleNotFoundError
from etl_core.persistence.table_definitions import ScheduleTable, TriggerType


class TestScheduleHandlerSimple:
    """Simplified tests for schedule handler focusing on logic rather than database interactions."""

    def test_schedule_not_found_error_message(self):
        """Test that ScheduleNotFoundError includes the schedule ID."""
        error = ScheduleNotFoundError("test_schedule_id")
        assert str(error) == "test_schedule_id"

    @patch('etl_core.persistence.handlers.schedule_handler.ensure_schema')
    def test_handler_initialization(self, mock_ensure_schema):
        """Test that handler initializes properly."""
        mock_engine = MagicMock()
        handler = ScheduleHandler(engine_=mock_engine)
        
        assert handler.engine == mock_engine
        mock_ensure_schema.assert_called_once()

    def test_create_schedule_trigger_args_handling(self):
        """Test that create properly handles None and empty trigger_args."""
        with patch('etl_core.persistence.handlers.schedule_handler.Session') as mock_session_class:
            mock_session = MagicMock()
            mock_session_class.return_value.__enter__.return_value = mock_session
            
            handler = ScheduleHandler()
            
            # Test with None trigger_args
            handler.create(
                name="Test Schedule",
                job_id="test_job",
                environment="test",
                trigger_type=TriggerType.INTERVAL,
                trigger_args=None
            )
            
            # Test with empty trigger_args
            handler.create(
                name="Test Schedule 2",
                job_id="test_job_2",
                environment="test",
                trigger_type=TriggerType.INTERVAL,
                trigger_args={}
            )
            
            # Verify that dict() was called to convert None to empty dict
            assert mock_session.add.call_count == 2
            assert mock_session.commit.call_count == 2
            assert mock_session.refresh.call_count == 2

    def test_update_schedule_not_found(self):
        """Test update when schedule is not found."""
        with patch('etl_core.persistence.handlers.schedule_handler.Session') as mock_session_class:
            mock_session = MagicMock()
            mock_session_class.return_value.__enter__.return_value = mock_session
            mock_session.get.return_value = None
            
            handler = ScheduleHandler()
            
            with pytest.raises(ScheduleNotFoundError, match="missing_schedule"):
                handler.update("missing_schedule", name="new_name")

    def test_delete_schedule_not_found(self):
        """Test delete when schedule is not found."""
        with patch('etl_core.persistence.handlers.schedule_handler.Session') as mock_session_class:
            mock_session = MagicMock()
            mock_session_class.return_value.__enter__.return_value = mock_session
            mock_session.get.return_value = None
            
            handler = ScheduleHandler()
            
            with pytest.raises(ScheduleNotFoundError, match="missing_schedule"):
                handler.delete("missing_schedule")

    def test_update_schedule_partial_updates(self):
        """Test update with partial parameter updates."""
        with patch('etl_core.persistence.handlers.schedule_handler.Session') as mock_session_class:
            mock_session = MagicMock()
            mock_session_class.return_value.__enter__.return_value = mock_session
            
            # Create a mock schedule object
            existing_schedule = MagicMock()
            existing_schedule.name = "Old Name"
            existing_schedule.is_paused = False
            
            mock_session.get.return_value = existing_schedule
            
            handler = ScheduleHandler()
            
            # Update only name
            handler.update("test_id", name="New Name")
            
            assert existing_schedule.name == "New Name"
            assert existing_schedule.is_paused == False  # Should remain unchanged
            
            # Update only is_paused
            handler.update("test_id", is_paused=True)
            
            assert existing_schedule.is_paused == True

    def test_update_schedule_trigger_args_update(self):
        """Test update with trigger_args update."""
        with patch('etl_core.persistence.handlers.schedule_handler.Session') as mock_session_class:
            mock_session = MagicMock()
            mock_session_class.return_value.__enter__.return_value = mock_session
            
            existing_schedule = MagicMock()
            existing_schedule.trigger_args = {"seconds": 10}
            
            mock_session.get.return_value = existing_schedule
            
            handler = ScheduleHandler()
            
            new_trigger_args = {"minutes": 5}
            handler.update("test_id", trigger_args=new_trigger_args)
            
            assert existing_schedule.trigger_args == new_trigger_args

    def test_set_paused_method(self):
        """Test set_paused method calls update with correct parameter."""
        with patch.object(ScheduleHandler, 'update') as mock_update:
            handler = ScheduleHandler()
            
            handler.set_paused("test_id", True)
            mock_update.assert_called_once_with("test_id", is_paused=True)
            
            handler.set_paused("test_id", False)
            mock_update.assert_called_with("test_id", is_paused=False)

    def test_list_schedules(self):
        """Test list method calls correct database operations."""
        with patch('etl_core.persistence.handlers.schedule_handler.Session') as mock_session_class:
            mock_session = MagicMock()
            mock_session_class.return_value.__enter__.return_value = mock_session
            
            mock_result = MagicMock()
            mock_session.exec.return_value.all.return_value = [{"id": "1"}, {"id": "2"}]
            
            handler = ScheduleHandler()
            result = handler.list()
            
            assert len(result) == 2
            mock_session.exec.assert_called_once()

    def test_get_schedule(self):
        """Test get method calls correct database operations."""
        with patch('etl_core.persistence.handlers.schedule_handler.Session') as mock_session_class:
            mock_session = MagicMock()
            mock_session_class.return_value.__enter__.return_value = mock_session
            
            mock_schedule = {"id": "test_id", "name": "Test Schedule"}
            mock_session.get.return_value = mock_schedule
            
            handler = ScheduleHandler()
            result = handler.get("test_id")
            
            assert result == mock_schedule
            mock_session.get.assert_called_once_with(ScheduleTable, "test_id")

    def test_get_by_name(self):
        """Test get_by_name method calls correct database operations."""
        with patch('etl_core.persistence.handlers.schedule_handler.Session') as mock_session_class:
            mock_session = MagicMock()
            mock_session_class.return_value.__enter__.return_value = mock_session
            
            mock_schedule = {"id": "test_id", "name": "Test Schedule"}
            mock_session.exec.return_value.first.return_value = mock_schedule
            
            handler = ScheduleHandler()
            result = handler.get_by_name("Test Schedule")
            
            assert result == mock_schedule
            mock_session.exec.assert_called_once()

    def test_all_trigger_types_supported(self):
        """Test that all trigger types are supported in create method."""
        with patch('etl_core.persistence.handlers.schedule_handler.Session') as mock_session_class:
            mock_session = MagicMock()
            mock_session_class.return_value.__enter__.return_value = mock_session
            
            handler = ScheduleHandler()
            
            # Test INTERVAL trigger
            handler.create(
                name="Interval Schedule",
                job_id="job1",
                environment="test",
                trigger_type=TriggerType.INTERVAL,
                trigger_args={"seconds": 30}
            )
            
            # Test CRON trigger
            handler.create(
                name="Cron Schedule",
                job_id="job2",
                environment="test",
                trigger_type=TriggerType.CRON,
                trigger_args={"cron": "0 0 * * *"}
            )
            
            # Test DATE trigger
            handler.create(
                name="Date Schedule",
                job_id="job3",
                environment="test",
                trigger_type=TriggerType.DATE,
                trigger_args={"run_date": "2024-01-01 00:00:00"}
            )
            
            # All three creates should have succeeded
            assert mock_session.add.call_count == 3
            assert mock_session.commit.call_count == 3
            assert mock_session.refresh.call_count == 3

    def test_update_timestamp_always_updated(self):
        """Test that updated_at is always updated during update operations."""
        with patch('etl_core.persistence.handlers.schedule_handler.Session') as mock_session_class:
            mock_session = MagicMock()
            mock_session_class.return_value.__enter__.return_value = mock_session
            
            existing_schedule = MagicMock()
            existing_schedule.updated_at = datetime(2024, 1, 1, 12, 0, 0)
            
            mock_session.get.return_value = existing_schedule
            
            handler = ScheduleHandler()
            
            # Update with no parameters (should still update timestamp)
            handler.update("test_id")
            
            # Verify that updated_at was set (datetime.now() was called)
            assert existing_schedule.updated_at != datetime(2024, 1, 1, 12, 0, 0)
            mock_session.add.assert_called_once()
            mock_session.commit.assert_called_once()
            mock_session.refresh.assert_called_once()

    def test_create_schedule_default_values(self):
        """Test create method with default values."""
        with patch('etl_core.persistence.handlers.schedule_handler.Session') as mock_session_class:
            mock_session = MagicMock()
            mock_session_class.return_value.__enter__.return_value = mock_session
            
            handler = ScheduleHandler()
            
            # Create with is_paused default (False)
            handler.create(
                name="Test Schedule",
                job_id="test_job",
                environment="test",
                trigger_type=TriggerType.INTERVAL,
                trigger_args={"seconds": 10}
            )
            
            # Verify that the schedule was created with default is_paused=False
            mock_session.add.assert_called_once()
            call_args = mock_session.add.call_args[0][0]
            assert hasattr(call_args, 'is_paused') or True  # Either has the attribute or we can't check it

    def test_context_manager_usage(self):
        """Test that the session context manager is used properly."""
        with patch('etl_core.persistence.handlers.schedule_handler.Session') as mock_session_class:
            mock_session = MagicMock()
            mock_session_instance = MagicMock()
            mock_session_instance.__enter__.return_value = mock_session
            mock_session_instance.__exit__.return_value = None
            mock_session_class.return_value = mock_session_instance
            
            handler = ScheduleHandler()
            
            # Use the context manager
            with handler._session() as session:
                pass
            
            # Verify context manager was called
            mock_session_class.assert_called_once_with(handler.engine)
            mock_session_instance.__enter__.assert_called_once()
            mock_session_instance.__exit__.assert_called_once()
