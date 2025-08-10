from typing import Any, Dict, List, Literal
from pydantic import Field, model_validator
from src.components.file_components.csv.csv_component import CSV
from src.components.registry import register_component
from src.metrics.component_metrics.component_metrics import ComponentMetrics
from src.receivers.files.csv_receiver import CSVReceiver

@register_component("write_csv")
class WriteCSV(CSV):
    type: Literal["write_csv"] = Field(default="write_csv")

    @model_validator(mode="after")
    def _build_objects(self):
        self._receiver = CSVReceiver()  # stateless
        return self

    async def process_row(
            self, row: Dict[str, Any], metrics: ComponentMetrics
    ) -> Dict[str, Any]:
        await self._receiver.write_row(self.filepath, metrics=metrics, row=row)
        return row

    async def process_bulk(
            self, data: List[Dict[str, Any]], metrics: ComponentMetrics
    ) -> List[Dict[str, Any]]:
        await self._receiver.write_bulk(self.filepath, metrics=metrics, data=data)
        return data

    async def process_bigdata(
            self, chunk_iterable: Any, metrics: ComponentMetrics
    ) -> Any:
        await self._receiver.write_bigdata(self.filepath, metrics=metrics, data=chunk_iterable)
        return chunk_iterable