from typing import ClassVar

from etl_core.components.databases.sql_reader_base import SQLReaderBase

from etl_core.components.databases.postgresql.postgresql import PostgreSQLComponent
from etl_core.components.component_registry import register_component
from etl_core.receivers.databases.postgresql.postgresql_receiver import (
    PostgreSQLReceiver,
)
from etl_core.components.wiring.ports import OutPortSpec


@register_component("read_postgresql")
class PostgreSQLRead(SQLReaderBase, PostgreSQLComponent):
    """PostgreSQL reader supporting row, bulk, and bigdata modes."""

    OUTPUT_PORTS = (OutPortSpec(name="out", required=True, fanout="many"),)

    ALLOW_NO_INPUTS = True

    receiver_class: ClassVar[type] = PostgreSQLReceiver
