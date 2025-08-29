from typing import Any, Dict, AsyncGenerator
import pandas as pd
import dask.dataframe as dd
from pydantic import model_validator

from etl_core.components.file_components.xml.xml_component import XML
from etl_core.components.component_registry import register_component
from etl_core.metrics.component_metrics.component_metrics import ComponentMetrics
from etl_core.receivers.files.xml.xml_receiver import XMLReceiver
from etl_core.components.envelopes import Out
from etl_core.components.wiring.ports import InPortSpec, OutPortSpec


@register_component("write_xml")
class WriteXML(XML):
    """
    XML writer supporting row, bulk, and bigdata modes with port routing.
    """

    INPUT_PORTS = (InPortSpec(name="in", required=True, fanin="many"),)
    OUTPUT_PORTS = (OutPortSpec(name="out", required=False, fanout="many"),)

    @model_validator(mode="after")
    def _build_objects(self):
        self._receiver = XMLReceiver()
        return self

    async def process_row(
            self, row: Dict[str, Any], metrics: ComponentMetrics
    ) -> AsyncGenerator[Out, None]:
        """Write a single record (dict), emit the written row on 'out'."""
        await self._receiver.write_row(
            self.filepath, metrics=metrics, row=row, root_tag=self.root_tag, record_tag=self.record_tag
        )
        yield Out(port="out", payload=row)

    async def process_bulk(
            self, dataframe: pd.DataFrame, metrics: ComponentMetrics
    ) -> AsyncGenerator[Out, None]:
        """Write full pandas DataFrame as XML, emit it on 'out'."""
        await self._receiver.write_bulk(
            self.filepath, metrics=metrics, data=dataframe, root_tag=self.root_tag, record_tag=self.record_tag
        )
        yield Out(port="out", payload=dataframe)

    async def process_bigdata(
            self, dataframe: dd.DataFrame, metrics: ComponentMetrics
    ) -> AsyncGenerator[Out, None]:
        """Write Dask DataFrame partition-wise to part-*.xml, emit ddf on 'out'."""
        await self._receiver.write_bigdata(
            self.filepath, metrics=metrics, data=dataframe, root_tag=self.root_tag, record_tag=self.record_tag
        )
        yield Out(port="out", payload=dataframe)

