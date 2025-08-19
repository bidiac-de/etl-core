from fastapi import Request

from src.singletons import (
    job_handler as _jh_singleton,
    execution_handler as _eh_singleton,
)
from src.persistance.handlers.job_handler import JobHandler
from src.job_execution.job_execution_handler import JobExecutionHandler


def get_job_handler(_: Request) -> JobHandler:
    return _jh_singleton()


def get_execution_handler(_: Request) -> JobExecutionHandler:
    return _eh_singleton()
