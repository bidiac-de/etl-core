from __future__ import annotations

from typing import Any, Dict, AsyncIterator, Tuple
from copy import deepcopy

import dask.dataframe as dd
import pandas as pd

from etl_core.metrics.component_metrics.data_operations_metrics.data_operations_metrics import (  # noqa e501
    DataOperationsMetrics,
)
from etl_core.components.wiring.ports import OutPortSpec
from etl_core.receivers.data_operations_receivers.data_operations_receiver import (
    DataOperationsReceiver,
)


class SplitReceiver(DataOperationsReceiver):
    """Duplicates payloads across OUTPUT_PORTS."""

    async def process_row(
        self,
        *,
        row: Dict[str, Any],
        branches: Tuple[OutPortSpec, ...],
        metrics: DataOperationsMetrics,
    ) -> AsyncIterator[Tuple[OutPortSpec, Dict[str, Any]]]:
        metrics.lines_received += 1
        fanout = len(branches)
        for port in branches:
            out_payload = deepcopy(row) if fanout > 1 else row
            metrics.lines_processed += 1
            metrics.lines_forwarded += 1
            yield port, out_payload

    async def process_bulk(
        self,
        *,
        dataframe: pd.DataFrame,
        branches: Tuple[OutPortSpec, ...],
        metrics: DataOperationsMetrics,
    ) -> AsyncIterator[Tuple[OutPortSpec, pd.DataFrame]]:
        rows = int(dataframe.shape[0])
        metrics.lines_received += rows
        copy_needed = len(branches) > 1

        for port in branches:
            out_df = dataframe.copy() if copy_needed else dataframe
            metrics.lines_processed += rows
            metrics.lines_forwarded += rows
            yield port, out_df

    async def process_bigdata(
        self,
        *,
        ddf: dd.DataFrame,
        branches: Tuple[OutPortSpec, ...],
    ) -> AsyncIterator[Tuple[OutPortSpec, dd.DataFrame]]:

        # Could change since Dask DataFrames are lazy.
        for port in branches:
            yield port, ddf
