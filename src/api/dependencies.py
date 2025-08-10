from fastapi import Request

from src.persistance.handlers.job_handler import JobHandler
from src.job_execution.job_execution_handler import JobExecutionHandler


def get_job_handler(request: Request) -> JobHandler:
    return request.app.state.job_handler


def get_execution_handler(request: Request) -> JobExecutionHandler:
    return request.app.state.execution_handler
