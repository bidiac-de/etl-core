from src.components.databases.mongodb.mongodb import MongoDBComponent
from src.metrics.component_metrics.component_metrics import ComponentMetrics
from src.receivers.databases.mongodb_receiver import MongoDBReceiver
from typing import Dict, List, Optional, Any
import pandas as pd
from pydantic import model_validator


class ReadMongoDB(MongoDBComponent):
    """
    MongoDB reader supporting row, bulk, and bigdata modes.
    """

    @model_validator(mode="after")
    def _build_objects(self):
        self._receiver = MongoDBReceiver()
        return self

    async def process_row(
        self, row: Dict[str, Any], metrics: ComponentMetrics
    ) -> Dict[str, Any]:
        """
        Read rows one-by-one (streaming).
        """
        async for result in self._receiver.read_row(
            self.collection_name, row, metrics=metrics
        ):
            yield result

    async def process_bulk(
        self, data: Optional[pd.DataFrame], metrics: ComponentMetrics
    ) -> List[Dict[str, Any]]:
        """
        Read whole MongoDB collection as a pandas DataFrame.
        """
        df = await self._receiver.read_bulk(self.collection_name, data, metrics=metrics)
        yield df

    async def process_bigdata(
        self, chunk_iterable: Any, metrics: ComponentMetrics
    ) -> Any:
        """
        Read large MongoDB collection with a framework
        suitable for big data operations.
        """
        ddf = await self._receiver.read_bigdata(
            self.collection_name, chunk_iterable, metrics=metrics
        )
        yield ddf
