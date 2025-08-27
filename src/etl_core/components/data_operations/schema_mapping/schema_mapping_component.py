from __future__ import annotations

from typing import Any, AsyncIterator, Dict, List, Tuple

import dask.dataframe as dd
import pandas as pd
from pydantic import Field, PrivateAttr, model_validator

from etl_core.components.data_operations.data_operations import (
    DataOperationsComponent,
)
from etl_core.components.data_operations.schema_mapping.mapping_rule import (
    FieldMapping,
)
from etl_core.components.envelopes import Out
from etl_core.components.component_registry import register_component
from etl_core.metrics.component_metrics.data_operations_metrics.data_operations_metrics import (  # noqa: E501
    DataOperationsMetrics,
)
from etl_core.receivers.data_operations_receivers.schema_mapping.schema_mapping_receiver import (  # noqa: E501
    SchemaMappingReceiver,
    _Path,
)
from etl_core.components.wiring.column_definition import DataType, FieldDef
from etl_core.components.wiring.schema import Schema
from etl_core.components.wiring.validation import get_leaf_field_map

CompiledRule = Tuple[str, _Path, str, _Path]


@register_component("schema_mapping")
class SchemaMappingComponent(DataOperationsComponent):
    rules: List[FieldMapping] = Field(default_factory=list)

    _compiled_row_rules: List[CompiledRule] = PrivateAttr(default=[])
    _string_rules: List[Tuple[str, str, str, str]] = PrivateAttr(default=[])

    @model_validator(mode="after")
    def _build_objects(self) -> "SchemaMappingComponent":
        self._receiver = SchemaMappingReceiver()
        self._validate_and_prepare_rules()
        return self

    async def process_row(
        self,
        row: Dict[str, Any],
        *,
        metrics: DataOperationsMetrics,
    ) -> AsyncIterator[Out]:
        async for port, payload in self._receiver.process_row(
            row, metrics=metrics, rules=self._compiled_row_rules
        ):
            yield Out(port=port, payload=payload)

    async def process_bulk(
        self,
        dataframe: pd.DataFrame,
        *,
        metrics: DataOperationsMetrics,
    ) -> AsyncIterator[Out]:
        async for port, payload in self._receiver.process_bulk(
            dataframe, metrics=metrics, rules=self._string_rules
        ):
            yield Out(port=port, payload=payload)

    async def process_bigdata(
        self,
        ddf: dd.DataFrame,
        *,
        metrics: DataOperationsMetrics,
    ) -> AsyncIterator[Out]:
        async for port, payload in self._receiver.process_bigdata(
            ddf, metrics=metrics, rules=self._string_rules
        ):
            yield Out(port=port, payload=payload)

    def _validate_and_prepare_rules(self) -> None:
        """
        1) Collision detection: (dst_port, dst_path) must be unique.
        2) Validate src/dst paths exist in the corresponding port schemas.
        3) Type compatibility (treat PATH as STRING).
        4) Prepare compiled rules for row/bulk/bigdata.
        """
        if not self.rules:
            self._compiled_row_rules = []
            self._string_rules = []
            return

        # detect collisions
        seen: set[tuple[str, str]] = set()
        for r in self.rules:
            key = (r.dst_port, r.dst_path)
            if key in seen:
                raise ValueError(
                    f"{self.name}: duplicate mapping to destination "
                    f"{r.dst_port!r}:{r.dst_path!r}"
                )
            seen.add(key)

        # path presence and type compatibility
        for r in self.rules:
            src_schema = self.schema_for_in_port(r.src_port)
            if not isinstance(src_schema, Schema):
                raise ValueError(
                    f"{self.name}: no schema for input port {r.src_port!r}"
                )
            dst_schema = self.schema_for_out_port(r.dst_port)
            if not isinstance(dst_schema, Schema):
                raise ValueError(
                    f"{self.name}: no schema for output port {r.dst_port!r}"
                )

            src_map = get_leaf_field_map(src_schema, path_separator=".")
            dst_map = get_leaf_field_map(dst_schema, path_separator=".")

            src_fd = src_map.get(r.src_path)
            if src_fd is None:
                raise ValueError(
                    f"{self.name}: unknown source path {r.src_port!r}:"
                    f"{r.src_path!r}"
                )

            dst_fd = dst_map.get(r.dst_path)
            if dst_fd is None:
                raise ValueError(
                    f"{self.name}: unknown destination path {r.dst_port!r}:"
                    f"{r.dst_path!r}"
                )

            if not self._types_compatible(src_fd, dst_fd):
                raise ValueError(
                    f"{self.name}: type mismatch {r.src_port}:{r.src_path} "
                    f"({src_fd.data_type}) -> {r.dst_port}:{r.dst_path} "
                    f"({dst_fd.data_type})"
                )

        # prepare cached rules
        self._compiled_row_rules = [
            (r.src_port, _Path.parse(r.src_path), r.dst_port, _Path.parse(r.dst_path))
            for r in self.rules
        ]
        self._string_rules = [
            (r.src_port, r.src_path, r.dst_port, r.dst_path) for r in self.rules
        ]

    @staticmethod
    def _types_compatible(src: FieldDef, dst: FieldDef) -> bool:
        """
        Keep it strict but practical:
        - exact match is OK
        - treat PATH like STRING (row validators do the same)
        """

        def norm(dt: DataType) -> DataType:
            return DataType.STRING if dt == DataType.PATH else dt

        return norm(src.data_type) == norm(dst.data_type)
