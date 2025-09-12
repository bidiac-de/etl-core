from __future__ import annotations
from typing import Any, Dict, AsyncGenerator, ClassVar
import pandas as pd
from pydantic import model_validator

from etl_core.components.file_components.xml.xml_component import XML
from etl_core.components.component_registry import register_component
from etl_core.receivers.files.xml.xml_receiver import XMLReceiver
from etl_core.metrics.component_metrics.component_metrics import ComponentMetrics
from etl_core.components.envelopes import Out
from etl_core.components.wiring.ports import OutPortSpec


@register_component("read_xml")
class ReadXML(XML):
    """XML reader supporting row, bulk, and bigdata with async *streaming* semantics.
    - process_row yields individual nested dicts
    - process_bulk and process_bigdata yield pandas DataFrame *chunks*
    """

    ALLOW_NO_INPUTS: ClassVar[bool] = True
    OUTPUT_PORTS = (OutPortSpec(name="out", required=True, fanout="many"),)

    @model_validator(mode="after")
    def _build_objects(self):
        self._receiver = XMLReceiver()
        return self

    async def process_row(
        self, row: Dict[str, Any], metrics: ComponentMetrics
    ) -> AsyncGenerator[Out, None]:
        async for rec in self._receiver.read_row(
            self.filepath, metrics=metrics, record_tag=self.record_tag
        ):
            yield Out(port="out", payload=rec)

    async def process_bulk(
        self, dataframe: pd.DataFrame, metrics: ComponentMetrics
    ) -> AsyncGenerator[Out, None]:
        async for df in self._receiver.read_bulk(
            self.filepath, metrics=metrics, record_tag=self.record_tag
        ):
            yield Out(port="out", payload=df)

    async def process_bigdata(
        self, dataframe: pd.DataFrame, metrics: ComponentMetrics
    ) -> AsyncGenerator[Out, None]:
        async for ddf in self._receiver.read_bigdata(
            self.filepath, metrics=metrics, record_tag=self.record_tag
        ):
            yield Out(port="out", payload=ddf)
