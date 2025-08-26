from __future__ import annotations

from typing import Any, AsyncIterator, Dict, ClassVar

import dask.dataframe as dd
import pandas as pd
from pydantic import Field, model_validator

from src.etl_core.components.component_registry import register_component
from src.etl_core.components.databases.mariadb.mariadb import MariaDBComponent
from src.etl_core.metrics.component_metrics.component_metrics import ComponentMetrics
from etl_core.components.envelopes import Out
from etl_core.components.wiring.ports import OutPortSpec
from etl_core.receivers.databases.mariadb.mariadb_receiver import MariaDBReceiver


@register_component("read_mariadb")
class MariaDBRead(MariaDBComponent):
    """
    MariaDB reader with ports + schema.

    - INPUT_PORTS: none (source)
    - OUTPUT_PORTS:
        - 'out' (required): rows/frames read from the DB
    """

    ALLOW_NO_INPUTS: ClassVar[bool] = True
    OUTPUT_PORTS = (OutPortSpec(name="out", required=True, fanout="many"),)

    query: str = Field(..., description="SQL query to execute")
    params: Dict[str, Any] = Field(default_factory=dict, description="Query parameters")
    strategy_type: str = Field(default="bulk", description="Execution strategy type")

    @model_validator(mode="after")
    def _build_objects(self) -> "MariaDBRead":
        self._receiver = MariaDBReceiver()
        return self

    async def process_row(
        self, payload: Any, metrics: ComponentMetrics
    ) -> AsyncIterator[Out]:
        """Stream rows one-by-one and envelope them for routing."""
        async for row in self._receiver.read_row(
            entity_name=self.entity_name,
            metrics=metrics,
            query=self.query,
            params=self.params,
            connection_handler=self.connection_handler,
        ):
            yield Out(port="out", payload=row)

    async def process_bulk(
        self, payload: Any, metrics: ComponentMetrics
    ) -> AsyncIterator[Out]:
        """Read full result as a pandas DataFrame and envelope it."""
        frame: pd.DataFrame = await self._receiver.read_bulk(
            entity_name=self.entity_name,
            metrics=metrics,
            query=self.query,
            params=self.params,
            connection_handler=self.connection_handler,
        )
        yield Out(port="out", payload=frame)

    async def process_bigdata(
        self, payload: Any, metrics: ComponentMetrics
    ) -> AsyncIterator[Out]:
        """Read result as a Dask DataFrame and envelope it."""
        ddf: dd.DataFrame = await self._receiver.read_bigdata(
            entity_name=self.entity_name,
            metrics=metrics,
            query=self.query,
            params=self.params,
            connection_handler=self.connection_handler,
        )
        yield Out(port="out", payload=ddf)
