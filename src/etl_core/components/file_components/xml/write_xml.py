from typing import Literal, Dict, Any, AsyncGenerator, List

import pandas as pd
from pydantic import model_validator

from etl_core.components.file_components.xml.xml_component import XML
from etl_core.receivers.files.xml.xml_receiver import XMLReceiver


class WriteXML(XML):
    type: Literal["write_xml"] = "write_xml"

    @model_validator(mode="after")
    def _build_objects(self):
        self._receiver = XMLReceiver(schema=self.schema)
        return self

    async def process_row(
            self, row: Dict[str, Any], metrics: Any
    ) -> AsyncGenerator[Dict[str, Any], None]:
        await self._receiver.write_row(
            self.filepath, metrics=metrics, row=row, root_tag=self.root_tag, record_tag=self.record_tag
        )
        yield row

    async def process_bulk(
            self, data: List[Dict[str, Any]] | pd.DataFrame, metrics: Any
    ) -> AsyncGenerator[List[Dict[str, Any]] | pd.DataFrame, None]:
        await self._receiver.write_bulk(
            self.filepath, metrics=metrics, data=data, root_tag=self.root_tag, record_tag=self.record_tag
        )
        yield data

    async def process_bigdata(self, chunk_iterable: dd.DataFrame, metrics: Any) -> AsyncGenerator[Any, None]:
        await self._receiver.write_bigdata(
            self.filepath, metrics=metrics, data=chunk_iterable, record_tag=self.record_tag, root_tag=self.root_tag
        )
        yield chunk_iterable
