from __future__ import annotations

from typing import Any, AsyncIterator, Dict, Sequence, Tuple

import dask.dataframe as dd
import pandas as pd

from etl_core.metrics.component_metrics.component_metrics import ComponentMetrics
from etl_core.receivers.base_receiver import Receiver
from etl_core.receivers.data_operations_receivers.type_conversion.type_conversion_helper import (
    Schema,
    TypeConversionRule,
    convert_dask_top_level,
    convert_frame_top_level,
    convert_row_nested,
    validate_frame_against_schema,
)


class TypeConversionReceiver(Receiver):
    """Receiver for type conversion with row, pandas and dask paths."""

    async def process_row(
            self,
            *,
            row: Dict[str, Any],
            rules: Sequence[TypeConversionRule],
            metrics: ComponentMetrics,
    ) -> AsyncIterator[Tuple[str, Any]]:
        """Convert a single nested row; yield ('out', payload)."""
        metrics.lines_received += 1
        keep, new_row = convert_row_nested(row, rules)
        if keep:
            metrics.lines_forwarded += 1
            yield "out", new_row

    async def process_bulk(
            self,
            *,
            dataframe: pd.DataFrame,
            rules: Sequence[TypeConversionRule],
            metrics: ComponentMetrics,
            out_schema: Schema | None = None,
    ) -> AsyncIterator[Tuple[str, Any]]:
        """Convert a pandas DataFrame; optionally validate schema."""
        try:
            metrics.lines_received += int(len(dataframe))
        except Exception:
            pass

        converted = convert_frame_top_level(dataframe, rules)

        if out_schema is not None:
            validate_frame_against_schema(converted, out_schema)

        try:
            metrics.lines_forwarded += int(len(converted))
        except Exception:
            pass

        yield "out", converted

    async def process_bigdata(
            self,
            *,
            ddf: dd.DataFrame,
            rules: Sequence[TypeConversionRule],
            metrics: ComponentMetrics,
    ) -> AsyncIterator[Tuple[str, Any]]:
        """
        Convert a Dask DataFrame lazily.
        """
        converted = convert_dask_top_level(ddf, rules)
        yield "out", converted