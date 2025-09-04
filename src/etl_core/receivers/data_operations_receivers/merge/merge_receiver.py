from __future__ import annotations

from typing import Any, AsyncIterator, Dict, Tuple

import dask.dataframe as dd
import pandas as pd

from etl_core.components.wiring.ports import OutPortSpec
from etl_core.metrics.component_metrics.data_operations_metrics.data_operations_metrics import (  # noqa E501
    DataOperationsMetrics,
)
from etl_core.receivers.data_operations_receivers.data_operations_receiver import (
    DataOperationsReceiver,
)


class MergeReceiver(DataOperationsReceiver):
    """
    Receiver that routes incoming payloads from multiple inputs to a single output port.
    It does not combine payloads; it forwards each input payload to the output.
    """

    async def process_row(
        self,
        *,
        out_port: OutPortSpec,
        row: Dict[str, Any],
        metrics: DataOperationsMetrics,
    ) -> AsyncIterator[Tuple[OutPortSpec, Dict[str, Any]]]:
        metrics.lines_received += 1
        metrics.lines_processed += 1
        metrics.lines_forwarded += 1
        yield out_port, row

    async def process_bulk(
        self,
        *,
        out_port: OutPortSpec,
        dataframe: pd.DataFrame,
        metrics: DataOperationsMetrics,
    ) -> AsyncIterator[Tuple[OutPortSpec, pd.DataFrame]]:
        rows = int(dataframe.shape[0])
        metrics.lines_received += rows
        out_df = dataframe.copy()
        metrics.lines_processed += rows
        metrics.lines_forwarded += rows
        yield out_port, out_df

    async def process_bigdata(
        self,
        *,
        out_port: OutPortSpec,
        ddf: dd.DataFrame,
        metrics: DataOperationsMetrics,
    ) -> AsyncIterator[Tuple[OutPortSpec, dd.DataFrame]]:
        try:
            count = int(ddf.map_partitions(len).sum().compute())
        except Exception:
            count = 0
        metrics.lines_received += count
        metrics.lines_processed += count
        metrics.lines_forwarded += count
        yield out_port, ddf
