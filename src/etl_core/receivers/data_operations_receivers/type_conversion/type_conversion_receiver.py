from __future__ import annotations

from typing import Any, AsyncGenerator, Dict, Sequence

import pandas as pd
import dask.dataframe as dd

from src.etl_core.receivers.base_receiver import Receiver
from src.etl_core.metrics.component_metrics.component_metrics import ComponentMetrics
from src.etl_core.receivers.data_operations_receivers.type_conversion.type_conversion_helper import (
    TypeConversionRule,
    convert_row_nested,
    convert_frame_top_level,
    convert_dask_top_level,
)


class TypeConversionReceiver(Receiver):
    """Apply type-cast rules for row/bulk/bigdata; streaming yields only."""

    async def process_row(
            self,
            row: Dict[str, Any],
            rules: Sequence[TypeConversionRule],
            metrics: ComponentMetrics,
    ) -> AsyncGenerator[Dict[str, Any], None]:
        metrics.lines_received += 1
        converted, errors, dropped = convert_row_nested(row, rules)
        if dropped:
            for _ in ():
                yield {}
            return
        metrics.lines_forwarded += 1
        yield {"__payload__": converted, "__errors__": errors}

    async def process_bulk(
            self,
            dataframe: pd.DataFrame,
            rules: Sequence[TypeConversionRule],
            metrics: ComponentMetrics,
    ) -> AsyncGenerator[pd.DataFrame, None]:
        total_received = int(len(dataframe))
        out = convert_frame_top_level(dataframe, rules)
        total_forwarded = int(len(out))
        metrics.lines_received += total_received
        metrics.lines_forwarded += total_forwarded
        yield out

    async def process_bigdata(
            self,
            ddf: dd.DataFrame,
            rules: Sequence[TypeConversionRule],
            metrics: ComponentMetrics,
    ) -> AsyncGenerator[dd.DataFrame, None]:
        out = convert_dask_top_level(ddf, rules)
        try:
            metrics.lines_received += int(ddf.map_partitions(len).sum().compute())
            metrics.lines_forwarded += int(out.map_partitions(len).sum().compute())
        except Exception:
            pass
        yield out