import asyncio
import logging
import os
from types import SimpleNamespace
from typing import Dict, Any, List
from unittest.mock import patch, MagicMock, AsyncMock

import pytest
from apscheduler.jobstores.base import JobLookupError

from etl_core.scheduling import scheduler_service
from etl_core.persistence.table_definitions import ScheduleTable, TriggerType
from etl_core.job_execution.job_execution_handler import ExecutionAlreadyRunning


class TestSchedulerServiceErrorHandling:
    """Test error handling in scheduler service."""

    def test_start_scheduler_creation_failure(self, monkeypatch):
        """Test start() when scheduler creation fails."""
        def failing_scheduler(*args, **kwargs):
            raise RuntimeError("Scheduler creation failed")
        
        monkeypatch.setattr(scheduler_service, "AsyncIOScheduler", failing_scheduler)
        
        deps = scheduler_service._Deps(
            schedules=MagicMock(),
            jobs=MagicMock(),
            executor=MagicMock()
        )
        service = scheduler_service.SchedulerService(deps=deps)
        
        with pytest.raises(RuntimeError, match="Scheduler creation failed"):
            service.start()
        
        assert not service._started

    def test_start_sync_job_scheduling_failure(self, monkeypatch):
        """Test start() when sync job scheduling fails."""
        fake_scheduler = MagicMock()
        fake_scheduler.start = MagicMock()
        fake_scheduler.add_job.side_effect = RuntimeError("Sync job failed")
        
        monkeypatch.setattr(scheduler_service, "AsyncIOScheduler", lambda *args, **kwargs: fake_scheduler)
        monkeypatch.setattr(scheduler_service, "IntervalTrigger", MagicMock)
        
        deps = scheduler_service._Deps(
            schedules=MagicMock(),
            jobs=MagicMock(),
            executor=MagicMock()
        )
        service = scheduler_service.SchedulerService(deps=deps)
        
        with pytest.raises(RuntimeError, match="Sync job failed"):
            service.start()
        
        # The service is started before the sync job scheduling, so _started will be True
        # but the sync job scheduling failure causes the exception
        assert service._started

    def test_pause_all_job_lookup_error(self, monkeypatch):
        """Test pause_all() with JobLookupError."""
        fake_scheduler = MagicMock()
        fake_job = MagicMock()
        fake_job.id = "test_job"
        fake_scheduler.get_jobs.return_value = [fake_job]
        fake_scheduler.pause_job.side_effect = JobLookupError("test_job")
        
        monkeypatch.setattr(scheduler_service, "AsyncIOScheduler", lambda *args, **kwargs: fake_scheduler)
        
        deps = scheduler_service._Deps(
            schedules=MagicMock(),
            jobs=MagicMock(),
            executor=MagicMock()
        )
        service = scheduler_service.SchedulerService(deps=deps)
        service._scheduler = fake_scheduler
        
        # Should not raise, should log warning
        with patch.object(service._log, 'warning') as mock_warning:
            service.pause_all()
            mock_warning.assert_called()

    def test_pause_all_general_exception(self, monkeypatch):
        """Test pause_all() with general exception."""
        fake_scheduler = MagicMock()
        fake_job = MagicMock()
        fake_job.id = "test_job"
        fake_scheduler.get_jobs.return_value = [fake_job]
        fake_scheduler.pause_job.side_effect = RuntimeError("Pause failed")
        
        monkeypatch.setattr(scheduler_service, "AsyncIOScheduler", lambda *args, **kwargs: fake_scheduler)
        
        deps = scheduler_service._Deps(
            schedules=MagicMock(),
            jobs=MagicMock(),
            executor=MagicMock()
        )
        service = scheduler_service.SchedulerService(deps=deps)
        service._scheduler = fake_scheduler
        
        # Should not raise, should log exception
        with patch.object(service._log, 'exception') as mock_exception:
            service.pause_all()
            mock_exception.assert_called()


class TestSchedulerServiceSyncOperations:
    """Test sync operations and their error handling."""

    def test_sync_from_db_remove_stale_job_lookup_error(self, monkeypatch):
        """Test sync_from_db with JobLookupError during stale job removal."""
        fake_scheduler = MagicMock()
        fake_job = MagicMock()
        fake_job.id = "stale_job"
        fake_scheduler.get_jobs.return_value = [fake_job]
        fake_scheduler.remove_job.side_effect = JobLookupError("stale_job")
        
        monkeypatch.setattr(scheduler_service, "AsyncIOScheduler", lambda *args, **kwargs: fake_scheduler)
        
        deps = scheduler_service._Deps(
            schedules=MagicMock(),
            jobs=MagicMock(),
            executor=MagicMock()
        )
        service = scheduler_service.SchedulerService(deps=deps)
        service._scheduler = fake_scheduler
        
        with patch.object(service._log, 'debug') as mock_debug:
            service.sync_from_db()
            mock_debug.assert_called()

    def test_sync_from_db_remove_stale_job_general_error(self, monkeypatch):
        """Test sync_from_db with general error during stale job removal."""
        fake_scheduler = MagicMock()
        fake_job = MagicMock()
        fake_job.id = "stale_job"
        fake_scheduler.get_jobs.return_value = [fake_job]
        fake_scheduler.remove_job.side_effect = RuntimeError("Remove failed")
        
        monkeypatch.setattr(scheduler_service, "AsyncIOScheduler", lambda *args, **kwargs: fake_scheduler)
        
        deps = scheduler_service._Deps(
            schedules=MagicMock(),
            jobs=MagicMock(),
            executor=MagicMock()
        )
        service = scheduler_service.SchedulerService(deps=deps)
        service._scheduler = fake_scheduler
        
        with patch.object(service._log, 'exception') as mock_exception:
            service.sync_from_db()
            mock_exception.assert_called()

    def test_upsert_aps_job_invalid_trigger(self, monkeypatch):
        """Test _upsert_aps_job with invalid trigger."""
        fake_scheduler = MagicMock()
        monkeypatch.setattr(scheduler_service, "AsyncIOScheduler", lambda *args, **kwargs: fake_scheduler)
        
        deps = scheduler_service._Deps(
            schedules=MagicMock(),
            jobs=MagicMock(),
            executor=MagicMock()
        )
        service = scheduler_service.SchedulerService(deps=deps)
        service._scheduler = fake_scheduler
        
        # Mock _to_trigger to raise an exception
        with patch.object(service, '_to_trigger') as mock_trigger:
            mock_trigger.side_effect = ValueError("Invalid trigger")
            
            schedule = SimpleNamespace(
                id="test_schedule",
                name="Test Schedule",
                job_id="test_job",
                trigger_type=TriggerType.INTERVAL,
                trigger_args={"seconds": 10},
                is_paused=False
            )
            
            with patch.object(service._log, 'exception') as mock_exception:
                service._upsert_aps_job(schedule)
                mock_exception.assert_called()

    def test_upsert_aps_job_creation_failure(self, monkeypatch):
        """Test _upsert_aps_job when job creation fails."""
        fake_scheduler = MagicMock()
        fake_scheduler.add_job.side_effect = RuntimeError("Job creation failed")
        monkeypatch.setattr(scheduler_service, "AsyncIOScheduler", lambda *args, **kwargs: fake_scheduler)
        
        deps = scheduler_service._Deps(
            schedules=MagicMock(),
            jobs=MagicMock(),
            executor=MagicMock()
        )
        service = scheduler_service.SchedulerService(deps=deps)
        service._scheduler = fake_scheduler
        
        schedule = SimpleNamespace(
            id="test_schedule",
            name="Test Schedule",
            job_id="test_job",
            trigger_type=TriggerType.INTERVAL,
            trigger_args={"seconds": 10},
            is_paused=False
        )
        
        with patch.object(service._log, 'exception') as mock_exception:
            service._upsert_aps_job(schedule)
            mock_exception.assert_called()

    def test_upsert_aps_job_pause_failure(self, monkeypatch):
        """Test _upsert_aps_job when pausing fails."""
        fake_scheduler = MagicMock()
        fake_scheduler.pause_job.side_effect = JobLookupError("test_schedule")
        monkeypatch.setattr(scheduler_service, "AsyncIOScheduler", lambda *args, **kwargs: fake_scheduler)
        
        deps = scheduler_service._Deps(
            schedules=MagicMock(),
            jobs=MagicMock(),
            executor=MagicMock()
        )
        service = scheduler_service.SchedulerService(deps=deps)
        service._scheduler = fake_scheduler
        
        schedule = SimpleNamespace(
            id="test_schedule",
            name="Test Schedule",
            job_id="test_job",
            trigger_type=TriggerType.INTERVAL,
            trigger_args={"seconds": 10},
            is_paused=True
        )
        
        with patch.object(service._log, 'warning') as mock_warning:
            service._upsert_aps_job(schedule)
            mock_warning.assert_called()

    def test_to_trigger_unsupported_type(self):
        """Test _to_trigger with unsupported trigger type."""
        deps = scheduler_service._Deps(
            schedules=MagicMock(),
            jobs=MagicMock(),
            executor=MagicMock()
        )
        service = scheduler_service.SchedulerService(deps=deps)
        
        with pytest.raises(ValueError, match="Unsupported trigger type"):
            service._to_trigger("UNSUPPORTED", {})


class TestSchedulerServiceRunSchedule:
    """Test _run_schedule method and its error handling."""

    @pytest.mark.asyncio
    async def test_run_schedule_missing_schedule(self, monkeypatch):
        """Test _run_schedule when schedule is not found."""
        fake_scheduler = MagicMock()
        monkeypatch.setattr(scheduler_service, "AsyncIOScheduler", lambda *args, **kwargs: fake_scheduler)
        
        deps = scheduler_service._Deps(
            schedules=MagicMock(),
            jobs=MagicMock(),
            executor=MagicMock()
        )
        deps.schedules.get = MagicMock(return_value=None)
        
        service = scheduler_service.SchedulerService(deps=deps)
        service._scheduler = fake_scheduler
        
        # Mock asyncio.to_thread to execute synchronously
        async def mock_to_thread(func, *args, **kwargs):
            return func(*args, **kwargs)
        
        monkeypatch.setattr(scheduler_service.asyncio, "to_thread", mock_to_thread)
        
        with patch.object(service._log, 'warning') as mock_warning:
            await service._run_schedule("missing_schedule")
            mock_warning.assert_called()

    @pytest.mark.asyncio
    async def test_run_schedule_job_load_failure(self, monkeypatch):
        """Test _run_schedule when job loading fails."""
        fake_scheduler = MagicMock()
        monkeypatch.setattr(scheduler_service, "AsyncIOScheduler", lambda *args, **kwargs: fake_scheduler)
        
        schedule = SimpleNamespace(
            id="test_schedule",
            job_id="test_job",
            environment="test"
        )
        
        deps = scheduler_service._Deps(
            schedules=MagicMock(),
            jobs=MagicMock(),
            executor=MagicMock()
        )
        deps.schedules.get = MagicMock(return_value=schedule)
        deps.jobs.load_runtime_job = MagicMock(side_effect=RuntimeError("Job load failed"))
        
        service = scheduler_service.SchedulerService(deps=deps)
        service._scheduler = fake_scheduler
        
        # Mock asyncio.to_thread to execute synchronously
        async def mock_to_thread(func, *args, **kwargs):
            return func(*args, **kwargs)
        
        monkeypatch.setattr(scheduler_service.asyncio, "to_thread", mock_to_thread)
        
        with patch.object(service._log, 'exception') as mock_exception:
            await service._run_schedule("test_schedule")
            mock_exception.assert_called()

    @pytest.mark.asyncio
    async def test_run_schedule_execution_failure(self, monkeypatch):
        """Test _run_schedule when execution fails."""
        fake_scheduler = MagicMock()
        monkeypatch.setattr(scheduler_service, "AsyncIOScheduler", lambda *args, **kwargs: fake_scheduler)
        
        schedule = SimpleNamespace(
            id="test_schedule",
            job_id="test_job",
            environment="test"
        )
        runtime_job = SimpleNamespace(id="test_job")
        
        deps = scheduler_service._Deps(
            schedules=MagicMock(),
            jobs=MagicMock(),
            executor=MagicMock()
        )
        deps.schedules.get = MagicMock(return_value=schedule)
        deps.jobs.load_runtime_job = MagicMock(return_value=runtime_job)
        deps.executor.execute_job_async = AsyncMock(side_effect=RuntimeError("Execution failed"))
        
        service = scheduler_service.SchedulerService(deps=deps)
        service._scheduler = fake_scheduler
        
        # Mock asyncio.to_thread to execute synchronously
        async def mock_to_thread(func, *args, **kwargs):
            return func(*args, **kwargs)
        
        monkeypatch.setattr(scheduler_service.asyncio, "to_thread", mock_to_thread)
        
        with patch.object(service._log, 'exception') as mock_exception:
            await service._run_schedule("test_schedule")
            mock_exception.assert_called()

    @pytest.mark.asyncio
    async def test_run_schedule_success(self, monkeypatch):
        """Test _run_schedule success case."""
        fake_scheduler = MagicMock()
        monkeypatch.setattr(scheduler_service, "AsyncIOScheduler", lambda *args, **kwargs: fake_scheduler)
        
        schedule = SimpleNamespace(
            id="test_schedule",
            job_id="test_job",
            environment="test"
        )
        runtime_job = SimpleNamespace(id="test_job")
        
        deps = scheduler_service._Deps(
            schedules=MagicMock(),
            jobs=MagicMock(),
            executor=MagicMock()
        )
        deps.schedules.get = MagicMock(return_value=schedule)
        deps.jobs.load_runtime_job = MagicMock(return_value=runtime_job)
        deps.executor.execute_job_async = AsyncMock()
        
        service = scheduler_service.SchedulerService(deps=deps)
        service._scheduler = fake_scheduler
        
        # Mock asyncio.to_thread to execute synchronously
        async def mock_to_thread(func, *args, **kwargs):
            return func(*args, **kwargs)
        
        monkeypatch.setattr(scheduler_service.asyncio, "to_thread", mock_to_thread)
        
        with patch.object(service._log, 'info') as mock_info:
            await service._run_schedule("test_schedule")
            mock_info.assert_called()


class TestSchedulerServiceScheduleOperations:
    """Test schedule operations (remove, pause, resume)."""

    def test_remove_schedule_lookup_error(self, monkeypatch):
        """Test remove_schedule with JobLookupError."""
        fake_scheduler = MagicMock()
        fake_scheduler.remove_job.side_effect = JobLookupError("test_schedule")
        monkeypatch.setattr(scheduler_service, "AsyncIOScheduler", lambda *args, **kwargs: fake_scheduler)
        
        deps = scheduler_service._Deps(
            schedules=MagicMock(),
            jobs=MagicMock(),
            executor=MagicMock()
        )
        service = scheduler_service.SchedulerService(deps=deps)
        service._scheduler = fake_scheduler
        
        with patch.object(service._log, 'debug') as mock_debug:
            service.remove_schedule("test_schedule")
            mock_debug.assert_called()

    def test_remove_schedule_general_error(self, monkeypatch):
        """Test remove_schedule with general error."""
        fake_scheduler = MagicMock()
        fake_scheduler.remove_job.side_effect = RuntimeError("Remove failed")
        monkeypatch.setattr(scheduler_service, "AsyncIOScheduler", lambda *args, **kwargs: fake_scheduler)
        
        deps = scheduler_service._Deps(
            schedules=MagicMock(),
            jobs=MagicMock(),
            executor=MagicMock()
        )
        service = scheduler_service.SchedulerService(deps=deps)
        service._scheduler = fake_scheduler
        
        with patch.object(service._log, 'exception') as mock_exception:
            service.remove_schedule("test_schedule")
            mock_exception.assert_called()

    def test_pause_schedule_lookup_error(self, monkeypatch):
        """Test pause_schedule with JobLookupError."""
        fake_scheduler = MagicMock()
        fake_scheduler.pause_job.side_effect = JobLookupError("test_schedule")
        monkeypatch.setattr(scheduler_service, "AsyncIOScheduler", lambda *args, **kwargs: fake_scheduler)
        
        deps = scheduler_service._Deps(
            schedules=MagicMock(),
            jobs=MagicMock(),
            executor=MagicMock()
        )
        service = scheduler_service.SchedulerService(deps=deps)
        service._scheduler = fake_scheduler
        
        with patch.object(service._log, 'debug') as mock_debug:
            service.pause_schedule("test_schedule")
            mock_debug.assert_called()

    def test_resume_schedule_lookup_error(self, monkeypatch):
        """Test resume_schedule with JobLookupError."""
        fake_scheduler = MagicMock()
        fake_scheduler.resume_job.side_effect = JobLookupError("test_schedule")
        monkeypatch.setattr(scheduler_service, "AsyncIOScheduler", lambda *args, **kwargs: fake_scheduler)
        
        deps = scheduler_service._Deps(
            schedules=MagicMock(),
            jobs=MagicMock(),
            executor=MagicMock()
        )
        service = scheduler_service.SchedulerService(deps=deps)
        service._scheduler = fake_scheduler
        
        with patch.object(service._log, 'debug') as mock_debug:
            service.resume_schedule("test_schedule")
            mock_debug.assert_called()

    def test_schedule_operations_no_scheduler(self):
        """Test schedule operations when scheduler is None."""
        deps = scheduler_service._Deps(
            schedules=MagicMock(),
            jobs=MagicMock(),
            executor=MagicMock()
        )
        service = scheduler_service.SchedulerService(deps=deps)
        service._scheduler = None
        
        # These should return without error
        service.remove_schedule("test")
        service.pause_schedule("test")
        service.resume_schedule("test")


class TestSchedulerServiceUtilityMethods:
    """Test utility methods."""

    def test_context_for_log_various_inputs(self):
        """Test _context_for_log with various inputs."""
        # None input
        result = scheduler_service.SchedulerService._context_for_log(None)
        assert result == "<none>"
        
        # Empty string
        result = scheduler_service.SchedulerService._context_for_log("")
        assert result == "<none>"
        
        # Common environment values
        result = scheduler_service.SchedulerService._context_for_log("dev")
        assert result == "DEV"
        
        result = scheduler_service.SchedulerService._context_for_log("test")
        assert result == "TEST"
        
        result = scheduler_service.SchedulerService._context_for_log("prod")
        assert result == "PROD"
        
        # Valid format
        result = scheduler_service.SchedulerService._context_for_log("valid_env_123")
        assert result == "valid_env_123"
        
        # Invalid format (should be masked)
        result = scheduler_service.SchedulerService._context_for_log("invalid@env#with$special%chars")
        assert result == "<masked>"

    def test_ensure_scheduler_closed_loop(self, monkeypatch):
        """Test _ensure_scheduler with closed event loop."""
        fake_scheduler = MagicMock()
        fake_scheduler._eventloop = MagicMock()
        fake_scheduler._eventloop.is_closed.return_value = True
        
        deps = scheduler_service._Deps(
            schedules=MagicMock(),
            jobs=MagicMock(),
            executor=MagicMock()
        )
        service = scheduler_service.SchedulerService(deps=deps)
        service._scheduler = fake_scheduler
        
        # Mock AsyncIOScheduler
        new_scheduler = MagicMock()
        monkeypatch.setattr(scheduler_service, "AsyncIOScheduler", lambda *args, **kwargs: new_scheduler)
        
        result = service._ensure_scheduler()
        assert result == new_scheduler
        assert service._scheduler == new_scheduler

    def test_ensure_scheduler_no_running_loop(self, monkeypatch):
        """Test _ensure_scheduler when no event loop is running."""
        deps = scheduler_service._Deps(
            schedules=MagicMock(),
            jobs=MagicMock(),
            executor=MagicMock()
        )
        service = scheduler_service.SchedulerService(deps=deps)
        service._scheduler = None
        
        # Mock asyncio.get_running_loop to raise RuntimeError
        monkeypatch.setattr(scheduler_service.asyncio, "get_running_loop", MagicMock(side_effect=RuntimeError()))
        
        new_scheduler = MagicMock()
        monkeypatch.setattr(scheduler_service, "AsyncIOScheduler", lambda *args, **kwargs: new_scheduler)
        
        result = service._ensure_scheduler()
        assert result == new_scheduler
        assert service._scheduler == new_scheduler

    def test_add_internal_job(self, monkeypatch):
        """Test add_internal_job method."""
        fake_scheduler = MagicMock()
        monkeypatch.setattr(scheduler_service, "AsyncIOScheduler", lambda *args, **kwargs: fake_scheduler)
        
        deps = scheduler_service._Deps(
            schedules=MagicMock(),
            jobs=MagicMock(),
            executor=MagicMock()
        )
        service = scheduler_service.SchedulerService(deps=deps)
        service._scheduler = fake_scheduler
        
        def test_func():
            pass
        
        service.add_internal_job(
            job_id="internal_test",
            func=test_func,
            trigger=MagicMock()
        )
        
        fake_scheduler.add_job.assert_called_once()
        assert "internal_test" in scheduler_service._INTERNAL_JOB_IDS


class TestParseSyncInterval:
    """Test parse_sync_interval function."""

    def test_parse_sync_interval_none(self):
        """Test parse_sync_interval with None input."""
        result = scheduler_service.parse_sync_interval(None)
        assert result == 30  # Default value

    def test_parse_sync_interval_disable_sentinels(self):
        """Test parse_sync_interval with disable sentinel values."""
        disable_values = ["0", "off", "false", "disable", "disabled", "none", "no"]
        for value in disable_values:
            result = scheduler_service.parse_sync_interval(value)
            assert result is None

    def test_parse_sync_interval_invalid_string(self):
        """Test parse_sync_interval with invalid string."""
        result = scheduler_service.parse_sync_interval("invalid_string")
        assert result == 30  # Default value

    def test_parse_sync_interval_negative_int(self):
        """Test parse_sync_interval with negative integer."""
        result = scheduler_service.parse_sync_interval(-5)
        assert result is None

    def test_parse_sync_interval_positive_int(self):
        """Test parse_sync_interval with positive integer."""
        result = scheduler_service.parse_sync_interval(60)
        assert result == 60

    def test_parse_sync_interval_zero_int(self):
        """Test parse_sync_interval with zero integer."""
        result = scheduler_service.parse_sync_interval(0)
        assert result is None

    def test_parse_sync_interval_empty_string(self):
        """Test parse_sync_interval with empty string."""
        result = scheduler_service.parse_sync_interval("")
        assert result == 30  # Default value

    def test_parse_sync_interval_whitespace_string(self):
        """Test parse_sync_interval with whitespace string."""
        result = scheduler_service.parse_sync_interval("   ")
        assert result == 30  # Default value

    def test_resolve_sync_interval_seconds_env_var(self, monkeypatch):
        """Test _resolve_sync_interval_seconds with environment variable."""
        monkeypatch.setenv("ETL_SCHEDULES_SYNC_SECONDS", "45")
        result = scheduler_service._resolve_sync_interval_seconds()
        assert result == 45

    def test_resolve_sync_interval_seconds_explicit(self):
        """Test _resolve_sync_interval_seconds with explicit value."""
        result = scheduler_service._resolve_sync_interval_seconds("60")
        assert result == 60
