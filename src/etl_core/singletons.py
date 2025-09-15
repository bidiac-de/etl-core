from __future__ import annotations

from typing import Optional

from etl_core.persistence.handlers.execution_records_handler import (
    ExecutionRecordsHandler,
)
from etl_core.persistence.handlers.job_handler import JobHandler
from etl_core.job_execution.job_execution_handler import JobExecutionHandler
from etl_core.persistence.handlers.context_handler import ContextHandler
from etl_core.persistence.handlers.credentials_handler import CredentialsHandler
from etl_core.scheduling.scheduler_service import SchedulerService


_job_handler_singleton: Optional[JobHandler] = None
_execution_records_handler_singleton: Optional[ExecutionRecordsHandler] = None
_execution_handler_singleton: Optional[JobExecutionHandler] = None
_context_handler_singleton: Optional[ContextHandler] = None
_credentials_handler_singleton: Optional[CredentialsHandler] = None
_scheduler_handler_singleton: Optional[SchedulerService] = None


def execution_records_handler() -> ExecutionRecordsHandler:
    global _execution_records_handler_singleton
    if _execution_records_handler_singleton is None:
        _execution_records_handler_singleton = ExecutionRecordsHandler()
    return _execution_records_handler_singleton


def job_handler() -> JobHandler:
    global _job_handler_singleton
    if _job_handler_singleton is None:
        _job_handler_singleton = JobHandler()
    return _job_handler_singleton


def execution_handler() -> JobExecutionHandler:
    global _execution_handler_singleton
    if _execution_handler_singleton is None:
        _execution_handler_singleton = JobExecutionHandler()
    return _execution_handler_singleton


def context_handler() -> ContextHandler:
    global _context_handler_singleton
    if _context_handler_singleton is None:
        _context_handler_singleton = ContextHandler()
    return _context_handler_singleton


def credentials_handler() -> CredentialsHandler:
    global _credentials_handler_singleton
    if _credentials_handler_singleton is None:
        _credentials_handler_singleton = CredentialsHandler()
    return _credentials_handler_singleton


def scheduler_handler() -> SchedulerService:
    global _scheduler_handler_singleton
    if _scheduler_handler_singleton is None:
        _scheduler_handler_singleton = SchedulerService.instance()
    return _scheduler_handler_singleton
