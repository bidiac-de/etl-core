from __future__ import annotations

from typing import Tuple

from etl_core.api.cli.adapters import (
    LocalContextsClient,
    LocalExecutionClient,
    LocalJobsClient,
    RemoteContextsClient,
    RemoteExecutionClient,
    RemoteJobsClient,
    api_base_url,
)
from etl_core.api.cli.ports import ContextsPort, ExecutionPort, JobsPort
from etl_core.persistence.db import ensure_schema


def pick_clients() -> Tuple[JobsPort, ExecutionPort, ContextsPort]:
    base = api_base_url()
    if base:
        return (
            RemoteJobsClient(base),
            RemoteExecutionClient(base),
            RemoteContextsClient(base),
        )
    ensure_schema()
    return LocalJobsClient(), LocalExecutionClient(), LocalContextsClient()
