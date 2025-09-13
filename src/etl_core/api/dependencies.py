from fastapi import Request

from etl_core.singletons import (
    job_handler as _jh_singleton,
    execution_handler as _eh_singleton,
    execution_records_handler as _erh_singleton,
    context_handler as _ch_singleton,
    credentials_handler as _crh_singleton,
)
from etl_core.persistance.handlers.job_handler import JobHandler
from etl_core.job_execution.job_execution_handler import JobExecutionHandler
from etl_core.persistance.handlers.execution_records_handler import (
    ExecutionRecordsHandler,
)


def get_job_handler(_: Request) -> JobHandler:
    return _jh_singleton()


def get_execution_handler(_: Request) -> JobExecutionHandler:
    return _eh_singleton()


def get_execution_records_handler(_: Request) -> ExecutionRecordsHandler:
    return _erh_singleton()


def get_context_handler(_: Request):
    return _ch_singleton()


def get_credentials_handler(_: Request):
    return _crh_singleton()
