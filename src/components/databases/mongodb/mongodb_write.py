from src.components.databases.mongodb.mongodb import MongoDBComponent
from src.metrics.component_metrics.component_metrics import ComponentMetrics
from src.receivers.databases.mongodb_receiver import MongoDBReceiver
from typing import Any
from pydantic import model_validator


class WriteMongoDB(MongoDBComponent):
    """
    MongoDB writer supporting row, bulk, and bigdata modes.
    """

    @model_validator(mode="after")
    def _build_objects(self):
        self._receiver = MongoDBReceiver()
        return self

    async def process_row(self, row: dict, metrics: ComponentMetrics) -> dict:
        """
        Write a single row to MongoDB.
        """
        await self._receiver.write_row(self.collection_name, row, metrics=metrics)
        return row

    async def process_bulk(
        self, data: list[dict], metrics: ComponentMetrics
    ) -> list[dict]:
        """
        Write a bulk of data to MongoDB.
        """
        await self._receiver.write_bulk(self.collection_name, data, metrics=metrics)
        return data

    async def process_bigdata(
        self, chunk_iterable: Any, metrics: ComponentMetrics
    ) -> Any:
        """
        Write large data to MongoDB using a suitable framework
        for big data operations.
        """
        await self._receiver.write_bigdata(
            self.collection_name, chunk_iterable, metrics=metrics
        )
        return chunk_iterable
