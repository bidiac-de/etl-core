from __future__ import annotations

from typing import Tuple

from etl_core.api.cli.adapters import (
    LocalContextsClient,
    LocalExecutionClient,
    LocalJobsClient,
)
from etl_core.api.cli.ports import ContextsPort, ExecutionPort, JobsPort


def pick_clients() -> Tuple[JobsPort, ExecutionPort, ContextsPort]:
    return LocalJobsClient(), LocalExecutionClient(), LocalContextsClient()
