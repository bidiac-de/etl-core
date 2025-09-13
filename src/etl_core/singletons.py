from __future__ import annotations

from typing import Optional

from etl_core.persistance.handlers.execution_records_handler import (
    ExecutionRecordsHandler,
)
from etl_core.persistance.handlers.job_handler import JobHandler
from etl_core.job_execution.job_execution_handler import JobExecutionHandler


_job_handler_singleton: Optional[JobHandler] = None
_execution_records_handler_singleton: Optional[ExecutionRecordsHandler] = None
_execution_handler_singleton: Optional[JobExecutionHandler] = None


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
