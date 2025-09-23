import json
import pytest
from unittest.mock import Mock, patch, MagicMock
import typer
import requests

from etl_core.api.cli.commands.schedules import (
    schedules_app,
    create,
    list_cmd,
    get_cmd,
    update_cmd,
    delete_cmd,
    pause_cmd,
    resume_cmd,
    run_now_cmd,
    _parse_json,
)
from etl_core.persistence.table_definitions import TriggerType


class TestParseJson:
    """Test the _parse_json helper function."""
    
    def test_parse_json_empty_string(self):
        """Test parsing empty string returns empty dict."""
        result = _parse_json("")
        assert result == {}
    
    def test_parse_json_none(self):
        """Test parsing None returns empty dict."""
        result = _parse_json(None)
        assert result == {}
    
    def test_parse_json_valid_string(self):
        """Test parsing valid JSON string."""
        json_str = '{"key": "value", "number": 42}'
        result = _parse_json(json_str)
        assert result == {"key": "value", "number": 42}
    
    def test_parse_json_invalid_json(self):
        """Test parsing invalid JSON raises exception."""
        with pytest.raises(json.JSONDecodeError):
            _parse_json('{"invalid": json}')


class TestCreateCommand:
    """Test the create command."""
    
    @patch('etl_core.api.cli.commands.schedules.api_base_url')
    @patch('requests.post')
    def test_create_with_api_url(self, mock_post, mock_api_base_url):
        """Test create command when API base URL is available."""
        mock_api_base_url.return_value = "http://test-api.com"
        mock_response = Mock()
        mock_response.json.return_value = {"id": "test-id", "name": "test-schedule"}
        mock_post.return_value = mock_response
        
        with patch('typer.echo') as mock_echo:
            create(
                name="test-schedule",
                job_id="job-123",
                environment="DEV",
                trigger_type=TriggerType.INTERVAL,
                trigger_args='{"seconds": 30}',
                paused=False
            )
        
        mock_post.assert_called_once_with(
            "http://test-api.com/schedules/",
            json={
                "name": "test-schedule",
                "job_id": "job-123",
                "environment": "DEV",
                "trigger_type": "interval",
                "trigger_args": {"seconds": 30},
                "paused": False,
            }
        )
        mock_response.raise_for_status.assert_called_once()
        mock_echo.assert_called_once_with({"id": "test-id", "name": "test-schedule"})
    
    @patch('etl_core.api.cli.commands.schedules.api_base_url')
    @patch('etl_core.scheduling.commands.CreateScheduleCommand')
    def test_create_without_api_url(self, mock_command_class, mock_api_base_url):
        """Test create command when API base URL is not available."""
        mock_api_base_url.return_value = None
        mock_command = Mock()
        mock_command.execute.return_value = Mock(id="created-schedule-id")
        mock_command_class.return_value = mock_command
        
        with patch('typer.echo') as mock_echo:
            create(
                name="test-schedule",
                job_id="job-123",
                environment="TEST",
                trigger_type=TriggerType.CRON,
                trigger_args='{"cron": "0 12 * * *"}',
                paused=True
            )
        
        mock_command_class.assert_called_once_with(
            name="test-schedule",
            job_id="job-123",
            environment="TEST",
            trigger_type=TriggerType.CRON,
            trigger_args={"cron": "0 12 * * *"},
            paused=True,
        )
        mock_command.execute.assert_called_once()
        mock_echo.assert_called_once_with("created-schedule-id")
    
    @patch('etl_core.api.cli.commands.schedules.api_base_url')
    @patch('etl_core.scheduling.commands.CreateScheduleCommand')
    def test_create_without_trigger_args(self, mock_command_class, mock_api_base_url):
        """Test create command without trigger_args parameter."""
        mock_api_base_url.return_value = None
        mock_command = Mock()
        mock_command.execute.return_value = Mock(id="created-schedule-id")
        mock_command_class.return_value = mock_command
        
        with patch('typer.echo') as mock_echo:
            create(
                name="simple-schedule",
                job_id="job-456",
                environment="PROD",
                trigger_type=TriggerType.DATE,
                trigger_args=None,
                paused=False
            )
        
        mock_command_class.assert_called_once_with(
            name="simple-schedule",
            job_id="job-456",
            environment="PROD",
            trigger_type=TriggerType.DATE,
            trigger_args={},
            paused=False,
        )


class TestListCommand:
    """Test the list command."""
    
    @patch('etl_core.api.cli.commands.schedules.api_base_url')
    @patch('requests.get')
    def test_list_with_api_url(self, mock_get, mock_api_base_url):
        """Test list command when API base URL is available."""
        mock_api_base_url.return_value = "http://test-api.com"
        mock_response = Mock()
        mock_response.json.return_value = [
            {
                "id": "schedule-1",
                "name": "Schedule 1",
                "job_id": "job-1",
                "environment": "DEV",
                "trigger_type": "interval",
                "is_paused": False,
            },
            {
                "id": "schedule-2",
                "name": "Schedule 2",
                "job_id": "job-2",
                "environment": "PROD",
                "trigger_type": "cron",
                "is_paused": True,
            }
        ]
        mock_get.return_value = mock_response
        
        with patch('typer.echo') as mock_echo:
            list_cmd()
        
        mock_get.assert_called_once_with("http://test-api.com/schedules/")
        mock_response.raise_for_status.assert_called_once()
        
        expected_calls = [
            (("schedule-1\tSchedule 1\tjob-1\tDEV\tinterval\tpaused=False",),),
            (("schedule-2\tSchedule 2\tjob-2\tPROD\tcron\tpaused=True",),),
        ]
        assert mock_echo.call_count == 2
        mock_echo.assert_has_calls(expected_calls)
    
    @patch('etl_core.api.cli.commands.schedules.api_base_url')
    @patch('etl_core.api.cli.commands.schedules._schedule_handler_singleton')
    def test_list_without_api_url(self, mock_handler_singleton, mock_api_base_url):
        """Test list command when API base URL is not available."""
        mock_api_base_url.return_value = None
        
        # Mock schedule handler
        mock_handler = Mock()
        mock_schedule_1 = Mock()
        mock_schedule_1.id = "local-schedule-1"
        mock_schedule_1.name = "Local Schedule 1"
        mock_schedule_1.job_id = "local-job-1"
        mock_schedule_1.environment = "TEST"
        mock_schedule_1.trigger_type = "date"
        mock_schedule_1.is_paused = False
        
        mock_schedule_2 = Mock()
        mock_schedule_2.id = "local-schedule-2"
        mock_schedule_2.name = "Local Schedule 2"
        mock_schedule_2.job_id = "local-job-2"
        mock_schedule_2.environment = "DEV"
        mock_schedule_2.trigger_type = "interval"
        mock_schedule_2.is_paused = True
        
        mock_handler.list.return_value = [mock_schedule_1, mock_schedule_2]
        mock_handler_singleton.return_value = mock_handler
        
        with patch('typer.echo') as mock_echo:
            list_cmd()
        
        mock_handler.list.assert_called_once()
        expected_calls = [
            (("local-schedule-1\tLocal Schedule 1\tlocal-job-1\tTEST\tdate\tpaused=False",),),
            (("local-schedule-2\tLocal Schedule 2\tlocal-job-2\tDEV\tinterval\tpaused=True",),),
        ]
        assert mock_echo.call_count == 2
        mock_echo.assert_has_calls(expected_calls)


class TestGetCommand:
    """Test the get command."""
    
    @patch('etl_core.api.cli.commands.schedules.api_base_url')
    @patch('requests.get')
    def test_get_with_api_url_success(self, mock_get, mock_api_base_url):
        """Test get command with API URL when schedule exists."""
        mock_api_base_url.return_value = "http://test-api.com"
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "id": "schedule-123",
            "name": "Test Schedule",
            "job_id": "job-456",
            "environment": "DEV",
            "trigger_type": "interval",
            "trigger_args": {"seconds": 30},
            "is_paused": False,
        }
        mock_get.return_value = mock_response
        
        with patch('typer.echo') as mock_echo:
            get_cmd("schedule-123")
        
        mock_get.assert_called_once_with("http://test-api.com/schedules/schedule-123")
        mock_response.raise_for_status.assert_called_once()
        
        # Verify the JSON output
        args, _ = mock_echo.call_args
        output_json = json.loads(args[0])
        assert output_json["id"] == "schedule-123"
        assert output_json["name"] == "Test Schedule"
    
    @patch('etl_core.api.cli.commands.schedules.api_base_url')
    @patch('requests.get')
    def test_get_with_api_url_not_found(self, mock_get, mock_api_base_url):
        """Test get command with API URL when schedule not found."""
        mock_api_base_url.return_value = "http://test-api.com"
        mock_response = Mock()
        mock_response.status_code = 404
        mock_get.return_value = mock_response
        
        with pytest.raises(typer.Exit) as exc_info:
            get_cmd("nonexistent-schedule")
        
        assert exc_info.value.exit_code == 1
        mock_get.assert_called_once_with("http://test-api.com/schedules/nonexistent-schedule")
    
    @patch('etl_core.api.cli.commands.schedules.api_base_url')
    @patch('etl_core.api.cli.commands.schedules._schedule_handler_singleton')
    def test_get_without_api_url_success(self, mock_handler_singleton, mock_api_base_url):
        """Test get command without API URL when schedule exists."""
        mock_api_base_url.return_value = None
        
        # Mock schedule handler and schedule
        mock_handler = Mock()
        mock_schedule = Mock()
        mock_schedule.id = "local-schedule-123"
        mock_schedule.name = "Local Test Schedule"
        mock_schedule.job_id = "local-job-456"
        mock_schedule.environment = "PROD"
        mock_schedule.trigger_type = "cron"
        mock_schedule.trigger_args = {"cron": "0 9 * * MON-FRI"}
        mock_schedule.is_paused = True
        
        mock_handler.get.return_value = mock_schedule
        mock_handler_singleton.return_value = mock_handler
        
        with patch('typer.echo') as mock_echo:
            get_cmd("local-schedule-123")
        
        mock_handler.get.assert_called_once_with("local-schedule-123")
        
        # Verify the JSON output
        args, _ = mock_echo.call_args
        output_json = json.loads(args[0])
        assert output_json["id"] == "local-schedule-123"
        assert output_json["name"] == "Local Test Schedule"
        assert output_json["trigger_args"] == {"cron": "0 9 * * MON-FRI"}
    
    @patch('etl_core.api.cli.commands.schedules.api_base_url')
    @patch('etl_core.api.cli.commands.schedules._schedule_handler_singleton')
    def test_get_without_api_url_not_found(self, mock_handler_singleton, mock_api_base_url):
        """Test get command without API URL when schedule not found."""
        mock_api_base_url.return_value = None
        
        mock_handler = Mock()
        mock_handler.get.return_value = None
        mock_handler_singleton.return_value = mock_handler
        
        with pytest.raises(typer.Exit) as exc_info:
            get_cmd("nonexistent-local-schedule")
        
        assert exc_info.value.exit_code == 1
        mock_handler.get.assert_called_once_with("nonexistent-local-schedule")


class TestUpdateCommand:
    """Test the update command."""
    
    @patch('etl_core.api.cli.commands.schedules.api_base_url')
    @patch('requests.put')
    def test_update_with_api_url_success(self, mock_put, mock_api_base_url):
        """Test update command with API URL."""
        mock_api_base_url.return_value = "http://test-api.com"
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"id": "updated-schedule", "name": "Updated Name"}
        mock_put.return_value = mock_response
        
        with patch('typer.echo') as mock_echo:
            update_cmd(
                schedule_id="schedule-123",
                name="Updated Name",
                job_id=None,
                environment="PROD",
                trigger_type=TriggerType.CRON,
                trigger_args='{"cron": "0 */6 * * *"}',
                paused=True
            )
        
        expected_payload = {
            "name": "Updated Name",
            "environment": "PROD",
            "trigger_type": "cron",
            "trigger_args": {"cron": "0 */6 * * *"},
            "paused": True,
        }
        mock_put.assert_called_once_with(
            "http://test-api.com/schedules/schedule-123",
            json=expected_payload
        )
        mock_response.raise_for_status.assert_called_once()
        mock_echo.assert_called_once_with({"id": "updated-schedule", "name": "Updated Name"})
    
    @patch('etl_core.api.cli.commands.schedules.api_base_url')
    @patch('requests.put')
    def test_update_with_api_url_not_found(self, mock_put, mock_api_base_url):
        """Test update command with API URL when schedule not found."""
        mock_api_base_url.return_value = "http://test-api.com"
        mock_response = Mock()
        mock_response.status_code = 404
        mock_put.return_value = mock_response
        
        with pytest.raises(typer.Exit) as exc_info:
            update_cmd(
                schedule_id="nonexistent-schedule",
                name="New Name",
                job_id=None,
                environment=None,
                trigger_type=None,
                trigger_args=None,
                paused=None
            )
        
        assert exc_info.value.exit_code == 1
    
    @patch('etl_core.api.cli.commands.schedules.api_base_url')
    @patch('etl_core.scheduling.commands.UpdateScheduleCommand')
    def test_update_without_api_url(self, mock_command_class, mock_api_base_url):
        """Test update command without API URL."""
        mock_api_base_url.return_value = None
        mock_command = Mock()
        mock_command.execute.return_value = Mock(id="updated-schedule-id")
        mock_command_class.return_value = mock_command
        
        with patch('typer.echo') as mock_echo:
            update_cmd(
                schedule_id="local-schedule-123",
                name=None,
                job_id="new-job-id",
                environment=None,
                trigger_type=TriggerType.INTERVAL,
                trigger_args='{"minutes": 15}',
                paused=False
            )
        
        mock_command_class.assert_called_once_with(
            schedule_id="local-schedule-123",
            name=None,
            job_id="new-job-id",
            environment=None,
            trigger_type=TriggerType.INTERVAL,
            trigger_args={"minutes": 15},
            paused=False,
        )
        mock_command.execute.assert_called_once()
        mock_echo.assert_called_once_with("updated-schedule-id")


class TestDeleteCommand:
    """Test the delete command."""
    
    @patch('etl_core.api.cli.commands.schedules.api_base_url')
    @patch('requests.delete')
    def test_delete_with_api_url_success(self, mock_delete, mock_api_base_url):
        """Test delete command with API URL."""
        mock_api_base_url.return_value = "http://test-api.com"
        mock_response = Mock()
        mock_response.status_code = 200
        mock_delete.return_value = mock_response
        
        with patch('typer.echo') as mock_echo:
            delete_cmd("schedule-to-delete")
        
        mock_delete.assert_called_once_with("http://test-api.com/schedules/schedule-to-delete")
        mock_response.raise_for_status.assert_called_once()
        mock_echo.assert_called_once_with("OK")
    
    @patch('etl_core.api.cli.commands.schedules.api_base_url')
    @patch('requests.delete')
    def test_delete_with_api_url_not_found(self, mock_delete, mock_api_base_url):
        """Test delete command with API URL when schedule not found."""
        mock_api_base_url.return_value = "http://test-api.com"
        mock_response = Mock()
        mock_response.status_code = 404
        mock_delete.return_value = mock_response
        
        with pytest.raises(typer.Exit) as exc_info:
            delete_cmd("nonexistent-schedule")
        
        assert exc_info.value.exit_code == 1
    
    @patch('etl_core.api.cli.commands.schedules.api_base_url')
    @patch('etl_core.scheduling.commands.DeleteScheduleCommand')
    def test_delete_without_api_url(self, mock_command_class, mock_api_base_url):
        """Test delete command without API URL."""
        mock_api_base_url.return_value = None
        mock_command = Mock()
        mock_command_class.return_value = mock_command
        
        with patch('typer.echo') as mock_echo:
            delete_cmd("local-schedule-to-delete")
        
        mock_command_class.assert_called_once_with("local-schedule-to-delete")
        mock_command.execute.assert_called_once()
        mock_echo.assert_called_once_with("OK")


class TestPauseCommand:
    """Test the pause command."""
    
    @patch('etl_core.api.cli.commands.schedules.api_base_url')
    @patch('requests.post')
    def test_pause_with_api_url_success(self, mock_post, mock_api_base_url):
        """Test pause command with API URL."""
        mock_api_base_url.return_value = "http://test-api.com"
        mock_response = Mock()
        mock_response.status_code = 200
        mock_post.return_value = mock_response
        
        with patch('typer.echo') as mock_echo:
            pause_cmd("schedule-to-pause")
        
        mock_post.assert_called_once_with("http://test-api.com/schedules/schedule-to-pause/pause")
        mock_response.raise_for_status.assert_called_once()
        mock_echo.assert_called_once_with("OK")
    
    @patch('etl_core.api.cli.commands.schedules.api_base_url')
    @patch('requests.post')
    def test_pause_with_api_url_not_found(self, mock_post, mock_api_base_url):
        """Test pause command with API URL when schedule not found."""
        mock_api_base_url.return_value = "http://test-api.com"
        mock_response = Mock()
        mock_response.status_code = 404
        mock_post.return_value = mock_response
        
        with pytest.raises(typer.Exit) as exc_info:
            pause_cmd("nonexistent-schedule")
        
        assert exc_info.value.exit_code == 1
    
    @patch('etl_core.api.cli.commands.schedules.api_base_url')
    @patch('etl_core.scheduling.commands.PauseScheduleCommand')
    def test_pause_without_api_url(self, mock_command_class, mock_api_base_url):
        """Test pause command without API URL."""
        mock_api_base_url.return_value = None
        mock_command = Mock()
        mock_command_class.return_value = mock_command
        
        with patch('typer.echo') as mock_echo:
            pause_cmd("local-schedule-to-pause")
        
        mock_command_class.assert_called_once_with("local-schedule-to-pause")
        mock_command.execute.assert_called_once()
        mock_echo.assert_called_once_with("OK")


class TestResumeCommand:
    """Test the resume command."""
    
    @patch('etl_core.api.cli.commands.schedules.api_base_url')
    @patch('requests.post')
    def test_resume_with_api_url_success(self, mock_post, mock_api_base_url):
        """Test resume command with API URL."""
        mock_api_base_url.return_value = "http://test-api.com"
        mock_response = Mock()
        mock_response.status_code = 200
        mock_post.return_value = mock_response
        
        with patch('typer.echo') as mock_echo:
            resume_cmd("schedule-to-resume")
        
        mock_post.assert_called_once_with("http://test-api.com/schedules/schedule-to-resume/resume")
        mock_response.raise_for_status.assert_called_once()
        mock_echo.assert_called_once_with("OK")
    
    @patch('etl_core.api.cli.commands.schedules.api_base_url')
    @patch('requests.post')
    def test_resume_with_api_url_not_found(self, mock_post, mock_api_base_url):
        """Test resume command with API URL when schedule not found."""
        mock_api_base_url.return_value = "http://test-api.com"
        mock_response = Mock()
        mock_response.status_code = 404
        mock_post.return_value = mock_response
        
        with pytest.raises(typer.Exit) as exc_info:
            resume_cmd("nonexistent-schedule")
        
        assert exc_info.value.exit_code == 1
    
    @patch('etl_core.api.cli.commands.schedules.api_base_url')
    @patch('etl_core.scheduling.commands.ResumeScheduleCommand')
    def test_resume_without_api_url(self, mock_command_class, mock_api_base_url):
        """Test resume command without API URL."""
        mock_api_base_url.return_value = None
        mock_command = Mock()
        mock_command_class.return_value = mock_command
        
        with patch('typer.echo') as mock_echo:
            resume_cmd("local-schedule-to-resume")
        
        mock_command_class.assert_called_once_with("local-schedule-to-resume")
        mock_command.execute.assert_called_once()
        mock_echo.assert_called_once_with("OK")


class TestRunNowCommand:
    """Test the run-now command."""
    
    @patch('etl_core.api.cli.commands.schedules.api_base_url')
    @patch('requests.post')
    def test_run_now_with_api_url_success(self, mock_post, mock_api_base_url):
        """Test run-now command with API URL."""
        mock_api_base_url.return_value = "http://test-api.com"
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"execution_id": "exec-123", "status": "started"}
        mock_post.return_value = mock_response
        
        with patch('typer.echo') as mock_echo:
            run_now_cmd("schedule-to-run")
        
        mock_post.assert_called_once_with("http://test-api.com/schedules/schedule-to-run/run-now")
        mock_response.raise_for_status.assert_called_once()
        
        # Verify JSON output
        args, _ = mock_echo.call_args
        output_json = json.loads(args[0])
        assert output_json["execution_id"] == "exec-123"
        assert output_json["status"] == "started"
    
    @patch('etl_core.api.cli.commands.schedules.api_base_url')
    @patch('requests.post')
    def test_run_now_with_api_url_not_found(self, mock_post, mock_api_base_url):
        """Test run-now command with API URL when schedule not found."""
        mock_api_base_url.return_value = "http://test-api.com"
        mock_response = Mock()
        mock_response.status_code = 404
        mock_post.return_value = mock_response
        
        with pytest.raises(typer.Exit) as exc_info:
            run_now_cmd("nonexistent-schedule")
        
        assert exc_info.value.exit_code == 1
    
    @patch('etl_core.api.cli.commands.schedules.api_base_url')
    @patch('etl_core.scheduling.commands.RunNowScheduleCommand')
    @patch('asyncio.run')
    def test_run_now_without_api_url(self, mock_asyncio_run, mock_command_class, mock_api_base_url):
        """Test run-now command without API URL."""
        mock_api_base_url.return_value = None
        mock_command = Mock()
        mock_command_class.return_value = mock_command
        
        run_now_cmd("local-schedule-to-run")
        
        mock_command_class.assert_called_once_with("local-schedule-to-run")
        mock_asyncio_run.assert_called_once_with(mock_command.execute())


class TestSchedulesApp:
    """Test the schedules_app Typer application."""
    
    def test_schedules_app_creation(self):
        """Test that schedules_app is properly created."""
        assert schedules_app is not None
        assert hasattr(schedules_app, 'commands')