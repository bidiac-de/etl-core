from __future__ import annotations

from typing import Tuple

from etl_core.api.cli.adapters import (
    HttpContextsClient,
    HttpExecutionClient,
    HttpJobsClient,
    LocalContextsClient,
    LocalExecutionClient,
    LocalJobsClient,
)
from etl_core.api.cli.ports import ContextsPort, ExecutionPort, JobsPort


def pick_clients(
    remote: bool, base_url: str
) -> Tuple[JobsPort, ExecutionPort, ContextsPort]:
    if remote:
        return (
            HttpJobsClient(base_url),
            HttpExecutionClient(base_url),
            HttpContextsClient(base_url),
        )
    return LocalJobsClient(), LocalExecutionClient(), LocalContextsClient()
